//! Whether a file still parses: the gate a replay or a landing passes before any
//! tree reaches a branch tip.
//!
//! A line merge cannot see syntax. Two branches that each append to the same
//! bracketed list can merge "cleanly" into a file that closes the list twice,
//! and nothing downstream of the merge notices until a build fails on the
//! default branch. This module answers the one question a merge cannot: does the
//! merged text still parse in its own language?
//!
//! Parsing is in process and exact. Rust goes through `syn`, which implements
//! the real grammar; JSON, YAML, and TOML go through their serde parsers. A path
//! in any other language is not gated, which is a statement about what this
//! instrument can see, not a verdict that the file is fine.
//!
//! Formatting is deliberately out of scope: a merge that leaves a file parseable
//! but unformatted is the advisory `rust-fmt` lane's business, and refusing a
//! replay over whitespace would be a gate that stops work it cannot justify.

use std::fmt;

/// One file that does not parse, located at the parser's first error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseFailure {
    pub path: String,
    /// 1-based line of the first error.
    pub line: usize,
    /// 1-based column of the first error.
    pub column: usize,
    pub message: String,
}

impl fmt::Display for ParseFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}:{}: {}",
            self.path, self.line, self.column, self.message
        )
    }
}

/// The languages the gate can parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Language {
    Rust,
    Json,
    Yaml,
    Toml,
}

fn language_of(path: &str) -> Option<Language> {
    let extension = path.rsplit_once('.')?.1;
    match extension {
        "rs" => Some(Language::Rust),
        "json" => Some(Language::Json),
        "yaml" | "yml" => Some(Language::Yaml),
        "toml" => Some(Language::Toml),
        _ => None,
    }
}

/// Whether the gate can say anything about this path at all.
pub fn is_gated(path: &str) -> bool {
    language_of(path).is_some()
}

/// The first parse error in `text`, read as the language `path` names. `None`
/// when it parses, and also when the path's language is not gated.
pub fn parse_failure(path: &str, text: &str) -> Option<ParseFailure> {
    let (line, column, message) = match language_of(path)? {
        Language::Rust => {
            let error = syn::parse_file(text).err()?;
            let start = error.span().start();
            (start.line, start.column + 1, rust_message(&error))
        }
        Language::Json => {
            let json = jsonc_as_json(text);
            let error = serde_json::from_str::<serde::de::IgnoredAny>(&json).err()?;
            (error.line(), error.column(), error.to_string())
        }
        Language::Yaml => yaml_failure(text)?,
        Language::Toml => {
            let error = text.parse::<toml::Table>().err()?;
            let (line, column) = error
                .span()
                .map(|span| line_and_column(text, span.start))
                .unwrap_or((0, 0));
            (line, column, error.message().to_string())
        }
    };
    Some(ParseFailure {
        path: path.to_string(),
        line,
        column,
        message: first_line(&message),
    })
}

/// `syn` lexes before it parses, and a lex failure carries proc-macro2's
/// generic wording. Its span still points at the offending token, so the
/// message is the only thing worth rewording: a doubled or missing bracket is
/// by far the likeliest cause after a merge.
fn rust_message(error: &syn::Error) -> String {
    let message = error.to_string();
    if message == "cannot parse string into token stream" {
        "the file does not lex: an unbalanced delimiter or an unterminated literal".to_string()
    } else {
        message
    }
}

/// JSON with comments and trailing commas (a `tsconfig.json`, editor settings)
/// read as the JSON it denotes. Comments and trailing commas become spaces, so
/// every line and column stays where it was; strict JSON is unchanged. A merge
/// that splices a bracket is still caught, and a branch that adds a comment to
/// such a file is not refused for it.
fn jsonc_as_json(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = bytes.to_vec();
    let mut in_string = false;
    let mut last_comma: Option<usize> = None;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if in_string {
            match byte {
                b'\\' => i += 1,
                b'"' => in_string = false,
                _ => {}
            }
            i += 1;
            continue;
        }
        match (byte, bytes.get(i + 1)) {
            (b'/', Some(b'/')) => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    out[i] = b' ';
                    i += 1;
                }
                continue;
            }
            (b'/', Some(b'*')) => {
                let start = i;
                i += 2;
                while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                let end = (i + 2).min(bytes.len());
                for blank in &mut out[start..end] {
                    if *blank != b'\n' {
                        *blank = b' ';
                    }
                }
                i = end;
                continue;
            }
            (b'"', _) => {
                in_string = true;
                last_comma = None;
            }
            (b',', _) => last_comma = Some(i),
            (b'}' | b']', _) => {
                if let Some(comma) = last_comma.take() {
                    out[comma] = b' ';
                }
            }
            (other, _) if other.is_ascii_whitespace() => {}
            _ => last_comma = None,
        }
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_string())
}

/// YAML may carry several `---` documents; each must parse.
fn yaml_failure(text: &str) -> Option<(usize, usize, String)> {
    use serde::Deserialize as _;
    for document in serde_yaml::Deserializer::from_str(text) {
        if let Err(error) = serde_yaml::Value::deserialize(document) {
            let (line, column) = error
                .location()
                .map(|location| (location.line(), location.column()))
                .unwrap_or((0, 0));
            return Some((line, column, error.to_string()));
        }
    }
    None
}

/// 1-based line and column of a byte offset.
fn line_and_column(text: &str, offset: usize) -> (usize, usize) {
    let before = &text[..offset.min(text.len())];
    let line = before.matches('\n').count() + 1;
    let column = before
        .rsplit_once('\n')
        .map_or(before, |(_, tail)| tail)
        .chars()
        .count()
        + 1;
    (line, column)
}

/// Parser messages can run to several lines; a refusal names the first.
fn first_line(message: &str) -> String {
    message
        .lines()
        .next()
        .unwrap_or_default()
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_with_comments_and_trailing_commas_parses_and_a_spliced_bracket_does_not() {
        let tsconfig = "{\n  // why\n  \"a\": [1, 2,], /* inline */\n  \"s\": \"// not a comment, \\\" ]\",\n}\n";
        assert_eq!(parse_failure("tsconfig.json", tsconfig), None);
        let spliced = "{\n  // why\n  \"a\": [1, 2],\n  3],\n}\n";
        let failure = parse_failure("tsconfig.json", spliced).expect("a spliced bracket");
        assert_eq!(failure.line, 4, "{failure}");
    }

    #[test]
    fn a_doubled_array_closing_is_located_where_the_parser_stops() {
        let text =
            "fn pins() {\n    check(&[\n        \"a\",\n    ],\n        \"b\",\n    ]);\n}\n";
        let failure =
            parse_failure("src/lib.rs", text).expect("the doubled closing must not parse");
        assert_eq!(failure.path, "src/lib.rs");
        // The lexer stops at the closing bracket with nothing left to close.
        assert_eq!((failure.line, failure.column), (6, 5), "{failure}");
        assert!(
            failure.message.contains("unbalanced delimiter"),
            "{failure}"
        );
        assert!(failure.to_string().starts_with("src/lib.rs:6:5: "));
    }

    #[test]
    fn valid_rust_passes() {
        assert_eq!(
            parse_failure(
                "a.rs",
                "#!/usr/bin/env rust\nfn main() { let _ = [1, 2,]; }\n"
            ),
            None
        );
    }

    #[test]
    fn json_yaml_and_toml_are_gated_with_their_own_parsers() {
        assert_eq!(parse_failure("a.json", "{\"a\": [1, 2]}"), None);
        let json = parse_failure("a.json", "{\"a\": [1, 2]]}").unwrap();
        assert_eq!(json.line, 1);

        assert_eq!(parse_failure("a.yaml", "a: 1\n---\nb: [1, 2]\n"), None);
        let yaml = parse_failure("a.yml", "a: 1\nb: [1, 2\n").unwrap();
        assert!(yaml.line >= 2, "{yaml}");

        assert_eq!(parse_failure("Cargo.toml", "[a]\nb = 1\n"), None);
        let toml = parse_failure("Cargo.toml", "[a]\nb = 1\nb = 2\n").unwrap();
        assert_eq!(toml.line, 3, "{toml}");
    }

    #[test]
    fn ungated_languages_say_nothing() {
        assert!(!is_gated("README.md"));
        assert!(!is_gated("Makefile"));
        assert_eq!(parse_failure("notes.md", "]]]"), None);
    }
}
