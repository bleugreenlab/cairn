//! Commands derived from a resource's advertised affordances.
//!
//! Every read returns the actions its resource accepts — mode, target template,
//! required and optional keys. That is already a command set; this module only
//! names it. Nothing here knows what an issue or a terminal is, so a resource
//! family that ships tomorrow is drivable from the shell the day its contract
//! entry lands.
//!
//! Key lists are used for naming and for help. They are deliberately *not* used
//! to validate a payload: the write dispatcher gates on the same contract data
//! and produces a better rejection than a client could, and a second check here
//! would be exactly the drift the structured projection exists to prevent.

use cairn_common::read::{ActionSpec, AffordanceSpec, KeyInfo};
use serde_json::{Map, Value};

/// The canonical verb for an action: its label, slugified.
///
/// `append comment` becomes `append-comment`, which is always accepted.
pub(crate) fn verb_slug(label: &str) -> String {
    label
        .split_whitespace()
        .map(|word| {
            word.chars()
                .filter(|c| c.is_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect::<String>()
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// The words of a label that may stand in for it on their own.
///
/// Short words carry no distinguishing weight and would let `:a` mean something,
/// so a candidate alias is at least three characters. Ambiguity is resolved by
/// the caller, not here: a word shared by two of a resource's actions names
/// neither.
fn alias_words(label: &str) -> Vec<String> {
    label
        .split_whitespace()
        .map(|word| {
            word.chars()
                .filter(|c| c.is_alphanumeric())
                .flat_map(char::to_lowercase)
                .collect::<String>()
        })
        .filter(|word| word.chars().count() >= 3)
        .collect()
}

/// Every verb this resource answers to, canonical slugs first, for help and
/// completion.
pub(crate) fn verbs(spec: &AffordanceSpec) -> Vec<String> {
    spec.actions.iter().map(|a| verb_slug(&a.label)).collect()
}

/// Resolve a typed verb against the actions the current resource advertises.
///
/// A canonical slug always wins. Otherwise a single unambiguous word of one
/// label selects it, which is what makes `:comment` reach `append comment`
/// without the shell ever having heard of comments.
pub(crate) fn resolve_verb<'a>(
    spec: &'a AffordanceSpec,
    verb: &str,
) -> Result<&'a ActionSpec, String> {
    let verb = verb.trim().to_lowercase();
    if let Some(action) = spec
        .actions
        .iter()
        .find(|action| verb_slug(&action.label) == verb)
    {
        return Ok(action);
    }

    let matched: Vec<&ActionSpec> = spec
        .actions
        .iter()
        .filter(|action| alias_words(&action.label).contains(&verb))
        .collect();
    match matched.len() {
        1 => Ok(matched[0]),
        0 => Err(format!(
            "no action `{verb}` here. {}",
            available(spec, "this resource advertises")
        )),
        _ => Err(format!(
            "`{verb}` is ambiguous here: {}",
            matched
                .iter()
                .map(|action| format!(":{}", verb_slug(&action.label)))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

fn available(spec: &AffordanceSpec, lead: &str) -> String {
    if spec.actions.is_empty() {
        format!("{lead} no actions")
    } else {
        format!(
            "{lead}: {}",
            verbs(spec)
                .iter()
                .map(|verb| format!(":{verb}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

/// The canonical spelling of `candidate` among an action's keys, honoring the
/// aliases the contract advertises. `None` when the action does not name it.
fn canonical_key<'a>(action: &'a ActionSpec, candidate: &str) -> Option<&'a str> {
    action
        .required
        .iter()
        .chain(action.optional.iter())
        .find(|key| key.key == candidate || key.aliases.iter().any(|alias| alias == candidate))
        .map(|key| key.key.as_str())
}

fn key_info<'a>(action: &'a ActionSpec, key: &str) -> Option<&'a KeyInfo> {
    action
        .required
        .iter()
        .chain(action.optional.iter())
        .find(|info| info.key == key)
}

/// Split `token` into a canonical key and its value, but only when the head
/// actually names a key of this action.
///
/// Requiring the head to be a known key is what keeps `:comment shipped x=y in
/// the end` a comment rather than a malformed keyed payload: prose that happens
/// to contain `=` is still prose.
fn split_key<'a>(action: &'a ActionSpec, token: &'a str) -> Option<(&'a str, &'a str)> {
    let (head, value) = token.split_once('=')?;
    canonical_key(action, head).map(|key| (key, value))
}

/// Build the payload for `action` from the text typed after its verb.
///
/// Three shapes, in order of how people actually type: nothing at all, a bare
/// value for an action that needs exactly one thing, and explicit `key=value`
/// pairs. The single-required-key rule is what makes `:comment ship it` work,
/// and it falls out of the contract's key list rather than being written down
/// for comments specifically.
pub(crate) fn build_payload(action: &ActionSpec, args: &str) -> Result<Value, String> {
    let args = args.trim();
    let mut payload = Map::new();

    if args.is_empty() {
        if !action.required.is_empty() {
            return Err(format!(
                "`{}` needs {}",
                verb_slug(&action.label),
                key_list(&action.required)
            ));
        }
        return Ok(Value::Object(payload));
    }

    let tokens = tokenize(args)?;
    let keyed = tokens
        .iter()
        .any(|token| split_key(action, token).is_some());

    if !keyed {
        return match action.required.as_slice() {
            [only] => {
                payload.insert(only.key.clone(), coerce(&only.ty, args));
                Ok(Value::Object(payload))
            }
            _ => Err(format!(
                "`{}` takes {}; write them as key=value (quote values containing spaces)",
                verb_slug(&action.label),
                key_list(&action.required)
            )),
        };
    }

    for token in &tokens {
        let Some((key, value)) = split_key(action, token) else {
            return Err(format!(
                "expected key=value, got `{token}`. `{}` accepts {}",
                verb_slug(&action.label),
                key_list_all(action)
            ));
        };
        let ty = key_info(action, key)
            .map(|info| info.ty.clone())
            .unwrap_or_else(|| "str".to_string());
        payload.insert(key.to_string(), coerce(&ty, value));
    }
    Ok(Value::Object(payload))
}

fn key_list(keys: &[KeyInfo]) -> String {
    if keys.is_empty() {
        return "no keys".to_string();
    }
    keys.iter()
        .map(|key| format!("{}({})", key.key, key.ty))
        .collect::<Vec<_>>()
        .join(", ")
}

fn key_list_all(action: &ActionSpec) -> String {
    let all: Vec<KeyInfo> = action
        .required
        .iter()
        .chain(action.optional.iter())
        .cloned()
        .collect();
    key_list(&all)
}

/// Coerce a typed value to the JSON shape the contract declares.
///
/// An unparseable value is passed through as a string rather than rejected: the
/// write gate reports a type mismatch against the same contract, and one clear
/// rejection from the authority beats two competing ones.
fn coerce(ty: &str, value: &str) -> Value {
    let trimmed = value.trim();
    match ty {
        "bool" => match trimmed {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            other => Value::String(other.to_string()),
        },
        "int" => trimmed
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(trimmed.to_string())),
        "float" => trimmed
            .parse::<f64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::String(trimmed.to_string())),
        "array" => {
            if trimmed.is_empty() {
                return Value::Array(Vec::new());
            }
            Value::Array(
                trimmed
                    .split(',')
                    .map(|part| Value::String(part.trim().to_string()))
                    .collect(),
            )
        }
        "object" => {
            serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(trimmed.to_string()))
        }
        // `any JSON value`: take JSON when it is JSON, text otherwise.
        "any JSON value" => {
            serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(trimmed.to_string()))
        }
        _ => Value::String(value.to_string()),
    }
}

/// Split a line into whitespace-separated words, honoring single and double
/// quotes so a value may contain spaces.
///
/// This is deliberately not POSIX word splitting: the shell's argument grammar
/// is `key=value` pairs and free text, and importing globbing, variable
/// expansion, and `$` semantics would promise a language this prompt does not
/// speak. Quotes group, a backslash escapes the next character, and that is all.
fn tokenize(input: &str) -> Result<Vec<String>, String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut started = false;
    let mut quote: Option<char> = None;
    let mut chars = input.chars();

    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                started = true;
                match chars.next() {
                    Some(escaped) => current.push(escaped),
                    None => return Err("line ends with a dangling backslash".to_string()),
                }
            }
            '\'' | '"' if quote.is_none() => {
                started = true;
                quote = Some(c);
            }
            c if Some(c) == quote => quote = None,
            c if c.is_whitespace() && quote.is_none() => {
                if started {
                    tokens.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            c => {
                started = true;
                current.push(c);
            }
        }
    }
    if quote.is_some() {
        return Err("unterminated quote".to_string());
    }
    if started {
        tokens.push(current);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str, ty: &str) -> KeyInfo {
        KeyInfo {
            key: name.to_string(),
            ty: ty.to_string(),
            note: String::new(),
            aliases: Vec::new(),
        }
    }

    fn action(
        label: &str,
        mode: &str,
        required: Vec<KeyInfo>,
        optional: Vec<KeyInfo>,
    ) -> ActionSpec {
        ActionSpec {
            label: label.to_string(),
            mode: mode.to_string(),
            uri_template: "cairn://p/{project}/{number}".to_string(),
            uri: Some("cairn://p/cairn/4279".to_string()),
            required,
            optional,
            example: String::new(),
            guidance: None,
        }
    }

    /// The issue affordance, as a read actually returns it.
    fn issue_spec() -> AffordanceSpec {
        AffordanceSpec {
            kind: "issue".to_string(),
            name: "Issue".to_string(),
            links: Vec::new(),
            filters: Vec::new(),
            actions: vec![
                action(
                    "patch issue",
                    "patch",
                    Vec::new(),
                    vec![key("title", "str"), key("status", "str")],
                ),
                action(
                    "append comment",
                    "append",
                    vec![key("content", "str")],
                    Vec::new(),
                ),
                action("delete issue", "delete", Vec::new(), Vec::new()),
                action(
                    "append message",
                    "append",
                    vec![key("content", "str")],
                    Vec::new(),
                ),
            ],
        }
    }

    #[test]
    fn a_canonical_slug_always_names_its_action() {
        let spec = issue_spec();
        assert_eq!(
            resolve_verb(&spec, "append-comment").unwrap().label,
            "append comment"
        );
    }

    #[test]
    fn an_unambiguous_word_stands_in_for_the_whole_label() {
        // The sketch types `:comment` and `:patch`; neither word is written
        // anywhere in the shell.
        let spec = issue_spec();
        assert_eq!(
            resolve_verb(&spec, "comment").unwrap().label,
            "append comment"
        );
        assert_eq!(resolve_verb(&spec, "patch").unwrap().label, "patch issue");
        assert_eq!(
            resolve_verb(&spec, "message").unwrap().label,
            "append message"
        );
    }

    #[test]
    fn a_word_two_actions_share_names_neither_and_says_so() {
        let spec = issue_spec();
        // `append` heads both `append comment` and `append message`; `issue`
        // trails both `patch issue` and `delete issue`.
        let error = resolve_verb(&spec, "append").unwrap_err();
        assert!(error.contains("ambiguous"), "{error}");
        assert!(error.contains(":append-comment"), "{error}");
        assert!(error.contains(":append-message"), "{error}");
        assert!(resolve_verb(&spec, "issue")
            .unwrap_err()
            .contains("ambiguous"));
    }

    #[test]
    fn an_unknown_verb_lists_what_this_resource_does_offer() {
        let error = resolve_verb(&issue_spec(), "frobnicate").unwrap_err();
        assert!(error.contains(":append-comment"), "{error}");
        assert!(error.contains(":delete-issue"), "{error}");
    }

    #[test]
    fn one_required_key_absorbs_the_whole_remainder() {
        let spec = issue_spec();
        let comment = resolve_verb(&spec, "comment").unwrap();
        assert_eq!(
            build_payload(comment, "ship it, the tests are green").unwrap(),
            serde_json::json!({"content": "ship it, the tests are green"})
        );
    }

    #[test]
    fn prose_containing_an_equals_sign_stays_prose() {
        // `status=closed` is not a key of `append comment`, so the line is a
        // comment body rather than a malformed keyed payload.
        let spec = issue_spec();
        let comment = resolve_verb(&spec, "comment").unwrap();
        assert_eq!(
            build_payload(comment, "set status=closed once this lands").unwrap(),
            serde_json::json!({"content": "set status=closed once this lands"})
        );
    }

    #[test]
    fn key_value_pairs_are_parsed_and_quoted_values_keep_their_spaces() {
        let spec = issue_spec();
        let patch = resolve_verb(&spec, "patch").unwrap();
        assert_eq!(
            build_payload(patch, "status=closed title=\"a longer title\"").unwrap(),
            serde_json::json!({"status": "closed", "title": "a longer title"})
        );
    }

    #[test]
    fn an_action_with_no_required_keys_accepts_a_bare_verb() {
        let spec = issue_spec();
        let delete = resolve_verb(&spec, "delete").unwrap();
        assert_eq!(build_payload(delete, "").unwrap(), serde_json::json!({}));
    }

    #[test]
    fn a_missing_required_key_is_reported_before_anything_is_sent() {
        let spec = issue_spec();
        let comment = resolve_verb(&spec, "comment").unwrap();
        let error = build_payload(comment, "").unwrap_err();
        assert!(error.contains("content(str)"), "{error}");
    }

    #[test]
    fn several_required_keys_with_no_key_tokens_ask_for_the_keyed_form() {
        let create = action(
            "create issue",
            "create",
            vec![key("title", "str"), key("description", "str")],
            Vec::new(),
        );
        let error = build_payload(&create, "just some words").unwrap_err();
        assert!(error.contains("key=value"), "{error}");
        assert!(error.contains("title(str)"), "{error}");
    }

    #[test]
    fn values_are_coerced_to_the_type_the_contract_declares() {
        let typed = action(
            "patch todos",
            "patch",
            Vec::new(),
            vec![
                key("escalate", "bool"),
                key("limit", "int"),
                key("labels", "array"),
                key("payload", "object"),
            ],
        );
        assert_eq!(
            build_payload(
                &typed,
                "escalate=true limit=5 labels=bug,ui payload={\"a\":1}"
            )
            .unwrap(),
            serde_json::json!({
                "escalate": true,
                "limit": 5,
                "labels": ["bug", "ui"],
                "payload": {"a": 1}
            })
        );
    }

    #[test]
    fn an_uncoercible_value_is_forwarded_for_the_write_gate_to_judge() {
        let typed = action("x", "patch", Vec::new(), vec![key("limit", "int")]);
        assert_eq!(
            build_payload(&typed, "limit=soon").unwrap(),
            serde_json::json!({"limit": "soon"})
        );
    }

    #[test]
    fn an_advertised_alias_resolves_to_its_canonical_key() {
        let mut aliased = key("dependsOn", "array");
        aliased.aliases = vec!["depends_on".to_string()];
        let patch = action("patch issue", "patch", Vec::new(), vec![aliased]);
        assert_eq!(
            build_payload(&patch, "depends_on=cairn://p/cairn/1").unwrap(),
            serde_json::json!({"dependsOn": ["cairn://p/cairn/1"]})
        );
    }

    #[test]
    fn tokenizing_reports_an_unterminated_quote_instead_of_guessing() {
        assert!(tokenize("title=\"unfinished").is_err());
        assert_eq!(tokenize("a  b\t c").unwrap(), ["a", "b", "c"]);
        assert_eq!(tokenize("k=''").unwrap(), ["k="]);
        assert_eq!(tokenize("a\\ b").unwrap(), ["a b"]);
    }
}
