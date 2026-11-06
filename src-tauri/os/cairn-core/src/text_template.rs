//! Single-pass placeholders shared by Response prompts and route sink bodies.
use serde_json::{Map, Value};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Token<'a> {
    Literal(&'a str),
    Placeholder { name: &'a str, source: &'a str },
}

pub(crate) fn is_response_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub(crate) fn tokens(template: &str) -> Vec<Token<'_>> {
    static SCANNER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"(?P<escape>\\\{\{)|\{\{\s*(?P<name>[A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z0-9_-]+)*)\s*\}\}",
        ).unwrap()
    });
    let mut result = Vec::new();
    let mut cursor = 0;
    for captures in SCANNER.captures_iter(template) {
        let matched = captures.get(0).unwrap();
        if matched.start() > cursor {
            result.push(Token::Literal(&template[cursor..matched.start()]));
        }
        if let Some(name) = captures.name("name") {
            result.push(Token::Placeholder {
                name: name.as_str(),
                source: matched.as_str(),
            });
        } else {
            result.push(Token::Literal(
                &template[matched.start() + 1..matched.end()],
            ));
        }
        cursor = matched.end();
    }
    if cursor < template.len() {
        result.push(Token::Literal(&template[cursor..]));
    }
    result
}

pub(crate) fn format_number(number: f64) -> String {
    let rounded = format!("{number:.2}");
    let text = rounded.trim_end_matches('0').trim_end_matches('.');
    if text == "-0" {
        "0".into()
    } else {
        text.to_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnboundName {
    pub name: String,
}

impl std::fmt::Display for UnboundName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}
impl std::error::Error for UnboundName {}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => format_number(number.as_f64().unwrap()),
        Value::Array(values) => values.iter().map(value_text).collect::<Vec<_>>().join(", "),
        other => other.to_string(),
    }
}

pub(crate) fn render(template: &str, fields: &Map<String, Value>) -> Result<String, UnboundName> {
    let mut result = String::new();
    for token in tokens(template) {
        match token {
            Token::Literal(text) => result.push_str(text),
            Token::Placeholder { name, .. } => {
                let value = fields
                    .get(name)
                    .filter(|v| !v.as_array().is_some_and(Vec::is_empty))
                    .ok_or_else(|| UnboundName {
                        name: name.to_owned(),
                    })?;
                result.push_str(&value_text(value));
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(crate) fn fixture_cases() -> Vec<Value> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../src/test/fixtures/text-template.json");
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    pub(crate) fn segments(source: &str, response: bool) -> Value {
        let mut segments: Vec<Value> = Vec::new();
        for token in tokens(source) {
            let item = match token {
                Token::Literal(text) => json!({"literal": text}),
                Token::Placeholder { name, source } if response && !is_response_name(name) => {
                    json!({"literal": source})
                }
                Token::Placeholder { name, source } => json!({"name": name, "source": source}),
            };
            if let (Some(previous), Some(text)) = (
                segments.last_mut(),
                item.get("literal").and_then(Value::as_str),
            ) {
                if let Some(previous_text) = previous.get("literal").and_then(Value::as_str) {
                    *previous = json!({"literal": format!("{previous_text}{text}")});
                    continue;
                }
            }
            segments.push(item);
        }
        json!(segments)
    }

    #[test]
    fn shared_grammar_fixture() {
        for case in fixture_cases() {
            let source = case["source"].as_str().unwrap();
            for (consumer, response) in [("sink", false), ("response", true)] {
                assert_eq!(
                    segments(source, response),
                    case[consumer]["segments"],
                    "{} {consumer}",
                    case["name"]
                );
                let names: Vec<_> = tokens(source)
                    .into_iter()
                    .filter_map(|token| match token {
                        Token::Placeholder { name, .. } if !response || is_response_name(name) => {
                            Some(name)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    json!(names),
                    case[consumer]["names"],
                    "{} {consumer}",
                    case["name"]
                );
            }
            let fields: Map<String, Value> = case["sink"]["names"]
                .as_array()
                .unwrap()
                .iter()
                .map(|name| {
                    let name = name.as_str().unwrap();
                    (name.to_owned(), json!(format!("<{name}>")))
                })
                .collect();
            let expected: String = case["sink"]["segments"]
                .as_array()
                .unwrap()
                .iter()
                .map(|segment| {
                    segment
                        .get("literal")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("<{}>", segment["name"].as_str().unwrap()))
                })
                .collect();
            assert_eq!(
                render(source, &fields).unwrap(),
                expected,
                "{}",
                case["name"]
            );
        }
    }

    #[test]
    fn rendering_scalars_lists_and_single_pass() {
        let fields = json!({"s":"{{other}}","empty":"","yes":true,"n":12,"round":0.826,"trim":1.20,"zero":-0.001,"list":["A","B"],"object":{"x":1},"null":null});
        assert_eq!(render("{{s}}|{{empty}}|{{yes}}|{{n}}|{{round}}|{{trim}}|{{zero}}|{{list}}|{{object}}|{{null}}", fields.as_object().unwrap()).unwrap(),
            "{{other}}||true|12|0.83|1.2|0|A, B|{\"x\":1}|null");
        assert_eq!(
            render(r"\{{missing}}", fields.as_object().unwrap()).unwrap(),
            "{{missing}}"
        );
    }

    #[test]
    fn missing_and_empty_lists_are_typed_errors() {
        let fields = json!({"empty":[],"blank":""});
        for name in ["missing", "empty"] {
            assert_eq!(
                render(
                    &format!("before {{{{{name}}}}} after"),
                    fields.as_object().unwrap()
                ),
                Err(UnboundName { name: name.into() })
            );
        }
        assert_eq!(
            render("{{blank}}", fields.as_object().unwrap()).unwrap(),
            ""
        );
    }

    #[test]
    fn number_format_is_shared_and_normalizes_zero() {
        for (number, expected) in [
            (12.0, "12"),
            (0.826, "0.83"),
            (1.20, "1.2"),
            (-0.001, "0"),
            (-1.236, "-1.24"),
        ] {
            assert_eq!(format_number(number), expected);
        }
    }
}
