//! The `run` verb's key grammar, stated once for every surface that accepts a
//! run call.
//!
//! A key the grammar does not define used to be dropped without a word. That is
//! how an `executor` written inside a command item (rather than beside
//! `commands`) placed batches on whatever machine the job already held while the
//! caller believed it had pinned one, and how a camelCase `commitMsg` would drop
//! the commit with it. The MCP tool validator and cairn-core's `handle_run` both
//! ask [`run_grammar_refusal`] before anything executes, so the SDK, the CLI and
//! the MCP tool refuse the same call with the same words.
//!
//! The check walks the raw JSON rather than leaning on the deserializer, so it
//! can name every offending key at once with its location, suggest the key the
//! caller most likely meant, and teach the corrected shape. Callers that only
//! need a batch's identity keep parsing leniently; this is the front door, not
//! the type.

use serde_json::{Map, Value};

/// Keys a `run` call accepts beside `commands`.
pub const RUN_CALL_KEYS: &[&str] = &[
    "commands",
    "sequential",
    "stop_on_error",
    "commit_msg",
    "branch",
    "executor",
    "conflict_markers_reason",
];

/// Keys one item of `commands` accepts.
pub const RUN_ITEM_KEYS: &[&str] = &[
    "command",
    "description",
    "timeout",
    "target",
    "payload",
    "code",
    "interpreter",
    "repl",
    "background",
    "waitFor",
];

/// Keys an item's `payload` accepts.
pub const RUN_ITEM_PAYLOAD_KEYS: &[&str] = &["args", "args_json"];

/// The placement key `executor` replaced. Refused by name so a caller who still
/// writes it learns the key that took its place.
const RETIRED_PLACEMENT_KEY: &str = "constraints";

/// The refusal for a `run` call carrying keys outside the grammar, or `None`
/// when every key is one the grammar defines.
///
/// The whole call is refused: a batch with one refused item and the rest run is
/// worse than either outcome. A misplaced key is never moved for the caller,
/// because two items naming different machines have no correct hoist; the
/// refusal teaches the one accepted shape instead.
pub fn run_grammar_refusal(raw: &Value) -> Option<String> {
    let Value::Object(call) = raw else {
        return None;
    };
    let mut findings = Vec::new();
    for (key, value) in call {
        if !RUN_CALL_KEYS.contains(&key.as_str()) {
            findings.push(call_key_finding(key, value));
        }
    }
    let mut misplaced_executors: Vec<&Value> = Vec::new();
    if let Some(Value::Array(items)) = call.get("commands") {
        for (index, item) in items.iter().enumerate() {
            let Value::Object(item) = item else {
                continue;
            };
            for (key, value) in item {
                if RUN_ITEM_KEYS.contains(&key.as_str()) {
                    continue;
                }
                if key == "executor" {
                    misplaced_executors.push(value);
                }
                findings.push(item_key_finding(index, key, value));
            }
            if let Some(Value::Object(payload)) = item.get("payload") {
                findings.extend(payload_findings(index, payload));
            }
        }
    }
    if findings.is_empty() {
        return None;
    }
    let mut text = if findings.len() == 1 {
        format!("This `run` call was refused: {}", findings[0])
    } else {
        let mut text = format!(
            "This `run` call was refused: {} keys are outside the run grammar.",
            findings.len()
        );
        for finding in &findings {
            text.push_str("\n- ");
            text.push_str(finding);
        }
        text
    };
    if misplaced_executors
        .windows(2)
        .any(|pair| pair[0] != pair[1])
    {
        text.push_str(
            "\nThe items name different machines, and one batch runs on one machine: split them into one run call per machine.",
        );
    }
    text.push_str("\nNothing ran.");
    Some(text)
}

fn call_key_finding(key: &str, value: &Value) -> String {
    if key == RETIRED_PLACEMENT_KEY {
        return retired_placement_finding("constraints");
    }
    if RUN_ITEM_KEYS.contains(&key) {
        return format!(
            "`{key}` is an item option: it belongs inside each item of `commands`. Move it there: `run({{commands:[{{command:\"...\", {key}:{}}}]}})`.",
            compact(value)
        );
    }
    if let Some(meant) = nearest(key, RUN_CALL_KEYS) {
        return format!("unknown key `{key}`; did you mean `{meant}`?");
    }
    if let Some(meant) = nearest(key, RUN_ITEM_KEYS) {
        return format!(
            "unknown key `{key}`; did you mean `{meant}`? It is an item option, so it belongs inside each item of `commands`."
        );
    }
    format!(
        "unknown key `{key}`. A run call takes {}.",
        key_list(RUN_CALL_KEYS)
    )
}

fn item_key_finding(index: usize, key: &str, value: &Value) -> String {
    let location = format!("commands[{index}].{key}");
    if key == RETIRED_PLACEMENT_KEY {
        return retired_placement_finding(&location);
    }
    if RUN_CALL_KEYS.contains(&key) {
        return format!(
            "`{location}` is a call-level option: {}. Move it beside `commands`: `run({{commands:[...], {key}:{}}})`.",
            call_level_reason(key),
            compact(value)
        );
    }
    if RUN_ITEM_PAYLOAD_KEYS.contains(&key) {
        return format!(
            "`{location}` belongs under the item's `payload`: `{{target:\"...\", payload:{{{key}:{}}}}}`.",
            compact(value)
        );
    }
    if let Some(meant) = nearest(key, RUN_ITEM_KEYS) {
        return format!("unknown key `{location}`; did you mean `{meant}`?");
    }
    if let Some(meant) = nearest(key, RUN_CALL_KEYS) {
        return format!(
            "unknown key `{location}`; did you mean `{meant}`? It is a call-level option: {}, so it belongs beside `commands`, not inside an item.",
            call_level_reason(meant)
        );
    }
    format!(
        "unknown key `{location}`. A run item takes {}.",
        key_list(RUN_ITEM_KEYS)
    )
}

fn payload_findings(index: usize, payload: &Map<String, Value>) -> Vec<String> {
    payload
        .keys()
        .filter(|key| !RUN_ITEM_PAYLOAD_KEYS.contains(&key.as_str()))
        .map(|key| {
            let location = format!("commands[{index}].payload.{key}");
            match nearest(key, RUN_ITEM_PAYLOAD_KEYS) {
                Some(meant) => format!("unknown key `{location}`; did you mean `{meant}`?"),
                None => format!(
                    "unknown key `{location}`. An item's payload takes `args` (a skill script's positional arguments) or `args_json` (an MCP tool's named arguments)."
                ),
            }
        })
        .collect()
}

/// Why a call-level key cannot ride on one item.
fn call_level_reason(key: &str) -> &'static str {
    match key {
        "executor" => "one batch runs on one machine",
        "commit_msg" | "conflict_markers_reason" => "one batch commits once",
        "branch" => "one batch runs against one revision",
        _ => "it applies to the whole batch",
    }
}

fn retired_placement_finding(location: &str) -> String {
    format!(
        "`{location}` was replaced by `executor`, a call-level option beside `commands`: state {{name:\"<public-name>\"}} for one machine or {{os:\"linux\"}} for any machine on a platform, optionally with requiredToolchains. Read cairn://executors for the names available."
    )
}

fn key_list(keys: &[&str]) -> String {
    keys.iter()
        .map(|key| format!("`{key}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A value rendered for a copy-paste example, shortened when it is long enough
/// to bury the shape it illustrates.
fn compact(value: &Value) -> String {
    const MAX: usize = 60;
    let rendered = value.to_string();
    if rendered.chars().count() <= MAX {
        return rendered;
    }
    match value {
        Value::String(_) => "\"...\"".to_string(),
        Value::Array(_) => "[...]".to_string(),
        Value::Object(_) => "{...}".to_string(),
        _ => rendered,
    }
}

/// The valid key the caller most plausibly meant: the same key in another case
/// convention (`commitMsg` for `commit_msg`), or one within two edits. A
/// suggestion further away than that teaches the wrong key, so none is made.
fn nearest<'a>(key: &str, valid: &[&'a str]) -> Option<&'a str> {
    let wanted = fold(key);
    let mut best: Option<(usize, &'a str)> = None;
    for &candidate in valid {
        let distance = edit_distance(&wanted, &fold(candidate));
        let close = distance == 0 || (distance <= 2 && wanted.len() >= 4);
        if close && best.is_none_or(|(seen, _)| distance < seen) {
            best = Some((distance, candidate));
        }
    }
    best.map(|(_, candidate)| candidate)
}

fn fold(key: &str) -> String {
    key.chars()
        .filter(|c| *c != '_' && *c != '-')
        .flat_map(char::to_lowercase)
        .collect()
}

fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    for (i, l) in left.chars().enumerate() {
        let mut current = vec![i + 1; right.len() + 1];
        for (j, r) in right.iter().enumerate() {
            let substitution = previous[j] + usize::from(l != *r);
            current[j + 1] = substitution.min(previous[j + 1] + 1).min(current[j] + 1);
        }
        previous = current;
    }
    previous[right.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_well_formed_call_is_not_refused() {
        let call = json!({
            "commands": [
                {"command": "hostname", "description": "probe", "timeout": 1000},
                {"target": "cairn://skills/x/scripts/y", "payload": {"args": ["a"]}},
                {"code": "print(1)", "interpreter": "python"},
                {"target": "cairn://workflows/w", "background": true}
            ],
            "sequential": true,
            "stop_on_error": false,
            "commit_msg": "m",
            "executor": {"name": "fedora"},
            "conflict_markers_reason": "fixture"
        });
        assert_eq!(run_grammar_refusal(&call), None);
        let wait = json!({"commands": [{"waitFor": {"duration": "3m"}}]});
        assert_eq!(run_grammar_refusal(&wait), None);
    }

    /// The rs/778 specimen: the selector rode inside the item, was dropped, and
    /// the batch ran on the job's residency machine.
    #[test]
    fn an_item_level_executor_is_refused_with_the_call_level_shape() {
        let call = json!({"commands": [{"command": "hostname", "executor": {"name": "fedora"}}]});
        let refusal = run_grammar_refusal(&call).expect("refused");
        assert!(
            refusal.contains("`commands[0].executor` is a call-level option: one batch runs on one machine. Move it beside `commands`: `run({commands:[...], executor:{\"name\":\"fedora\"}})`."),
            "{refusal}"
        );
        assert!(refusal.ends_with("Nothing ran."), "{refusal}");
    }

    #[test]
    fn every_offending_key_is_named_at_once_with_its_location() {
        let call = json!({
            "commitMsg": "m",
            "commands": [
                {"command": "a"},
                {"command": "b", "timout": 5, "zzqq": 1}
            ]
        });
        let refusal = run_grammar_refusal(&call).expect("refused");
        assert!(
            refusal.contains("3 keys are outside the run grammar"),
            "{refusal}"
        );
        assert!(
            refusal.contains("unknown key `commitMsg`; did you mean `commit_msg`?"),
            "{refusal}"
        );
        assert!(
            refusal.contains("unknown key `commands[1].timout`; did you mean `timeout`?"),
            "{refusal}"
        );
        assert!(
            refusal.contains("unknown key `commands[1].zzqq`. A run item takes `command`"),
            "{refusal}"
        );
        assert!(refusal.ends_with("Nothing ran."), "{refusal}");
    }

    #[test]
    fn items_naming_different_machines_are_told_to_split() {
        let call = json!({"commands": [
            {"command": "a", "executor": {"name": "fedora"}},
            {"command": "b", "executor": {"os": "linux"}}
        ]});
        let refusal = run_grammar_refusal(&call).expect("refused");
        assert!(
            refusal.contains("split them into one run call per machine"),
            "{refusal}"
        );
        let same = json!({"commands": [
            {"command": "a", "executor": {"name": "fedora"}},
            {"command": "b", "executor": {"name": "fedora"}}
        ]});
        assert!(!run_grammar_refusal(&same)
            .expect("refused")
            .contains("split them"));
    }

    #[test]
    fn keys_on_the_wrong_level_say_where_they_belong() {
        let call =
            json!({"commands": [{"command": "a", "commitMsg": "m", "args": ["x"]}], "timeout": 5});
        let refusal = run_grammar_refusal(&call).expect("refused");
        assert!(
            refusal
                .contains("`timeout` is an item option: it belongs inside each item of `commands`"),
            "{refusal}"
        );
        assert!(
            refusal.contains("unknown key `commands[0].commitMsg`; did you mean `commit_msg`? It is a call-level option: one batch commits once"),
            "{refusal}"
        );
        assert!(
            refusal.contains("`commands[0].args` belongs under the item's `payload`"),
            "{refusal}"
        );
    }

    #[test]
    fn the_retired_placement_key_names_its_replacement() {
        let call = json!({"commands": [{"command": "a"}], "constraints": {"os": "linux"}});
        let refusal = run_grammar_refusal(&call).expect("refused");
        assert!(
            refusal.contains("`constraints` was replaced by `executor`"),
            "{refusal}"
        );
        assert!(refusal.contains("cairn://executors"), "{refusal}");
    }

    #[test]
    fn unknown_payload_keys_are_refused() {
        let call =
            json!({"commands": [{"target": "cairn://mcp/s/t", "payload": {"argsJson": {}}}]});
        let refusal = run_grammar_refusal(&call).expect("refused");
        assert!(
            refusal
                .contains("unknown key `commands[0].payload.argsJson`; did you mean `args_json`?"),
            "{refusal}"
        );
    }

    #[test]
    fn suggestions_stay_close() {
        assert_eq!(nearest("stopOnError", RUN_CALL_KEYS), Some("stop_on_error"));
        assert_eq!(nearest("wait_for", RUN_ITEM_KEYS), Some("waitFor"));
        assert_eq!(nearest("cmd", RUN_ITEM_KEYS), None);
        assert_eq!(nearest("verbose", RUN_CALL_KEYS), None);
    }
}
