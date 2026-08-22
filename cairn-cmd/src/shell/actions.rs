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

/// The shortest readable spelling that selects exactly this action.
///
/// Help must only advertise a shorthand the dispatcher will accept. Builtins
/// are excluded because colon dispatch resolves them before resource actions.
pub(crate) fn preferred_verb(
    spec: &AffordanceSpec,
    action: &ActionSpec,
    builtins: &[&str],
) -> String {
    alias_words(&action.label)
        .into_iter()
        .find(|alias| {
            !builtins.contains(&alias.as_str())
                && spec
                    .actions
                    .iter()
                    .filter(|candidate| alias_words(&candidate.label).contains(alias))
                    .count()
                    == 1
        })
        .unwrap_or_else(|| verb_slug(&action.label))
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

/// Split `word` into a canonical key and its value, but only when the head
/// actually names a key of this action.
///
/// Requiring the head to be a known key is what keeps `:comment shipped x=y in
/// the end` a comment rather than a malformed keyed payload: prose that happens
/// to contain `=` is still prose.
fn split_key<'a>(action: &'a ActionSpec, word: &'a str) -> Option<(&'a str, &'a str)> {
    let (head, value) = word.split_once('=')?;
    canonical_key(action, head).map(|key| (key, value))
}

/// Whether the text after a verb is written in the keyed `key=value` form.
///
/// Decided on raw whitespace-separated words, *before* any quoting grammar is
/// applied. Quotes exist so a value may contain spaces, so they mean something
/// only once the line is known to be carrying values; a bare comment body is not
/// a value. Reading free text through the quoting grammar is what made
/// `:comment it's green` die on an apostrophe, and no amount of care inside the
/// tokenizer fixes that — prose simply must not reach it.
fn is_keyed(action: &ActionSpec, args: &str) -> bool {
    args.split_whitespace()
        .any(|word| split_key(action, word).is_some())
}

/// Build the payload for `action` from the text typed after its verb.
///
/// Three shapes, in order of how people actually type: nothing at all, a bare
/// value for an action that needs exactly one thing, and explicit `key=value`
/// pairs. The single-required-key rule is what makes `:comment ship it` work,
/// and it falls out of the contract's key list rather than being written down
/// for comments specifically.
///
/// Keyed-ness is settled first and free text is taken verbatim, so a line is
/// parsed by exactly one grammar rather than passing through a shared one on its
/// way to the right answer.
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

    if !is_keyed(action, args) {
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

    for token in &tokenize(args)? {
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
            // A bracketed literal arrives intact, so read it as the JSON it
            // plainly is. The comma list is the shorthand for the common case,
            // not the only form an array may be written in.
            if trimmed.starts_with('[') {
                if let Ok(value @ Value::Array(_)) = serde_json::from_str::<Value>(trimmed) {
                    return value;
                }
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

/// Split a **keyed** line into whitespace-separated words, honoring single and
/// double quotes so a value may contain spaces.
///
/// This is deliberately not POSIX word splitting: the shell's argument grammar
/// is `key=value` pairs and free text, and importing globbing, variable
/// expansion, and `$` semantics would promise a language this prompt does not
/// speak. Quotes group, a backslash escapes the next character, a `{` or `[`
/// directly after `=` opens a JSON literal, and that is all.
///
/// Only a line [`is_keyed`] has already accepted arrives here, so every quote,
/// backslash, and brace this reads was typed to delimit a value. That is what
/// makes an unterminated quote a real error rather than an apostrophe in prose.
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
            // A brace or bracket directly after `=` is the head of a JSON value,
            // so the run it opens is taken verbatim. Anchoring the rule to `=`
            // is what keeps a lone bracket in prose ordinary text.
            '{' | '[' if quote.is_none() && current.ends_with('=') => {
                started = true;
                current.push(c);
                take_json_literal(c, &mut chars, &mut current)?;
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

/// Consume a brace- or bracket-delimited run verbatim, its opener already
/// emitted, so the literal reaches [`coerce`] as the JSON it was typed as.
///
/// Inside the literal the shell's grouping quotes do not apply: `"` is JSON's
/// own string delimiter, and taking it as grouping is what silently turned
/// `{"a":1}` into `{a:1}` before it ever reached a parser. Nesting is tracked so
/// an object inside an object ends where it should, and whitespace is kept, so
/// `payload={"a": 1}` needs no outer quoting.
fn take_json_literal(
    open: char,
    chars: &mut std::str::Chars<'_>,
    out: &mut String,
) -> Result<(), String> {
    let close = if open == '{' { '}' } else { ']' };
    let mut depth = 1usize;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        out.push(c);
        if in_string {
            match c {
                // JSON owns its escapes; the backslash and whatever it guards
                // ride through untouched so `{"a":"x\"y"}` ends where it should.
                '\\' => match chars.next() {
                    Some(escaped) => out.push(escaped),
                    None => return Err("line ends with a dangling backslash".to_string()),
                },
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            c if c == open => depth += 1,
            c if c == close => {
                depth -= 1;
                if depth == 0 {
                    return Ok(());
                }
            }
            _ => {}
        }
    }
    Err(format!("unterminated `{open}`"))
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
    fn help_prefers_the_leading_safe_alias_over_an_internal_label() {
        let action = action(
            "patch project rename hide attach remote",
            "patch",
            Vec::new(),
            Vec::new(),
        );
        let spec = AffordanceSpec {
            kind: "project".into(),
            name: "Project".into(),
            links: Vec::new(),
            filters: Vec::new(),
            actions: vec![action],
        };
        assert_eq!(preferred_verb(&spec, &spec.actions[0], &["help"]), "patch");
        assert_eq!(resolve_verb(&spec, "patch").unwrap(), &spec.actions[0]);
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
    fn free_text_never_passes_through_the_quoting_grammar() {
        // An apostrophe is the common case and the one that made this a bug:
        // prose is not a value, so nothing here opens a quote.
        let spec = issue_spec();
        let comment = resolve_verb(&spec, "comment").unwrap();
        assert_eq!(
            build_payload(comment, "it's green now").unwrap(),
            serde_json::json!({"content": "it's green now"})
        );
        // The tokenizer's other two rejections are just as absent, and its
        // escapes just as inert: a body reaches the resource byte for byte.
        assert_eq!(
            build_payload(comment, "escape it with a trailing \\").unwrap(),
            serde_json::json!({"content": "escape it with a trailing \\"})
        );
        assert_eq!(
            build_payload(comment, "the payload={\"a\": 1 was wrong").unwrap(),
            serde_json::json!({"content": "the payload={\"a\": 1 was wrong"})
        );
    }

    #[test]
    fn a_malformed_keyed_line_is_still_rejected_where_quoting_means_something() {
        let spec = issue_spec();
        let patch = resolve_verb(&spec, "patch").unwrap();
        // `title=` names a key, so this line is carrying values and its quote
        // was typed to delimit one.
        let error = build_payload(patch, "title=\"unfinished").unwrap_err();
        assert!(error.contains("unterminated quote"), "{error}");

        let typed = action(
            "patch todos",
            "patch",
            Vec::new(),
            vec![key("payload", "object")],
        );
        let error = build_payload(&typed, "payload={\"a\": 1").unwrap_err();
        assert!(error.contains("unterminated"), "{error}");

        // And the keyed form keeps its own way to carry an apostrophe.
        assert_eq!(
            build_payload(patch, "title=\"it's done\"").unwrap(),
            serde_json::json!({"title": "it's done"})
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
    fn an_array_may_be_written_as_a_json_literal_or_a_comma_list() {
        let typed = action("x", "patch", Vec::new(), vec![key("labels", "array")]);
        assert_eq!(
            build_payload(&typed, "labels=[\"bug\", \"ui\"]").unwrap(),
            serde_json::json!({"labels": ["bug", "ui"]})
        );
        assert_eq!(
            build_payload(&typed, "labels=bug,ui").unwrap(),
            serde_json::json!({"labels": ["bug", "ui"]})
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

    #[test]
    fn a_json_literal_after_an_equals_keeps_its_own_quotes_and_spaces() {
        // Grouping quotes would eat JSON's string delimiters, so the literal is
        // taken verbatim from its opening brace to the one that balances it.
        assert_eq!(
            tokenize("payload={\"a\": 1, \"b\": [2, 3]}").unwrap(),
            ["payload={\"a\": 1, \"b\": [2, 3]}"]
        );
        // Nesting closes at its own depth, and a delimiter inside a JSON string
        // is text rather than a closer.
        assert_eq!(
            tokenize("a={\"x\": {\"y\": \"}\"}} b=1").unwrap(),
            ["a={\"x\": {\"y\": \"}\"}}", "b=1"]
        );
        // The rule is anchored to `=`, so a bracket in prose stays prose.
        assert_eq!(tokenize("see [the fix").unwrap(), ["see", "[the", "fix"]);
        assert!(tokenize("payload={\"a\": 1").is_err());
    }
}
