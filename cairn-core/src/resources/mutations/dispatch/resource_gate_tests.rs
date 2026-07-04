use super::todos::parse_todo_write_items;
use super::wakes::{parse_wake_filter, terminal_slug_from_ref};
use super::*;

fn item(target: &str, mode: ChangeMode, payload: Option<serde_json::Value>) -> ChangeItem {
    ChangeItem {
        target: target.to_string(),
        mode,
        payload,
    }
}

fn gate(item: &ChangeItem) -> ResourceMutationResult<&'static MutationSpec> {
    let resource = cairn_common::uri::parse_uri(&item.target).unwrap();
    gate_resource_change(0, item, &resource)
}

#[test]
fn gate_rejects_apply_mode_with_enumeration() {
    let it = item(
        "cairn://p/CAIRN/1/1/builder/chat/1/2",
        ChangeMode::Apply,
        None,
    );
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("Unsupported resource mutation"));
}

#[test]
fn gate_rejects_unsupported_mode_and_lists_valid_ones() {
    // Issue supports patch/append/delete, not replace.
    let it = item("cairn://p/CAIRN/1", ChangeMode::Replace, None);
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("Unsupported resource mutation"));
    assert!(failure.error.contains("patch issue"));
    assert!(failure.error.contains("append comment"));
    assert!(failure.error.contains("delete issue"));
}

#[test]
fn gate_marks_read_only_resource() {
    let it = item("cairn://p/CAIRN/1/1/builder/chat", ChangeMode::Append, None);
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("read-only"));
}

#[test]
fn gate_names_missing_required_keys_with_example() {
    // Terminal create requires `command`.
    let it = item(
        "cairn://p/CAIRN/1/1/builder/terminal/dev",
        ChangeMode::Create,
        Some(serde_json::json!({ "description": "d" })),
    );
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("Missing required payload key"));
    assert!(failure.error.contains("command"));
    assert!(failure.error.contains("Example:"));
}

#[test]
fn todo_append_mis_keyed_item_enumerates_accepted_keys() {
    // A todo item keyed `title` (the message/artifact spelling) instead of
    // `content` clears the top-level `todos` gate but fails item
    // deserialization. The rejection must name the accepted item keys so the
    // agent self-corrects without a discovery round-trip (CAIRN #164).
    let payload = serde_json::json!({ "todos": [{ "title": "do the thing" }] });
    let it = item("cairn:~/todos", ChangeMode::Append, Some(payload.clone()));
    let failure = parse_todo_write_items(0, &it, &payload).unwrap_err();
    assert!(
        failure.error.contains("content"),
        "rejection must name the canonical `content` key: {}",
        failure.error
    );
    assert!(failure.error.contains("status"));
    assert!(failure.error.contains("Each item accepts:"));
}

#[test]
fn gate_accepts_alias_for_required_key() {
    // tasks append requires subagentType; the snake_case alias must satisfy it.
    let it = item(
        "cairn://p/CAIRN/1/1/builder/tasks",
        ChangeMode::Append,
        Some(serde_json::json!({ "subagent_type": "Explore", "description": "map parser flow" })),
    );
    assert!(gate(&it).is_ok());
}

#[test]
fn gate_requires_task_description() {
    // tasks append requires both subagentType and description.
    let it = item(
        "cairn://p/CAIRN/1/1/builder/tasks",
        ChangeMode::Append,
        Some(serde_json::json!({ "subagentType": "Explore", "prompt": "do the thing" })),
    );
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("Missing required payload key"));
    assert!(failure.error.contains("description"));
}

#[test]
fn gate_accepts_supported_mutation() {
    // The issues collection creates via `append`, not `create`.
    let it = item(
        "cairn://p/CAIRN/issues",
        ChangeMode::Append,
        Some(serde_json::json!({ "title": "hi" })),
    );
    let spec = gate(&it).unwrap();
    assert_eq!(spec.label, "create issue");
}

#[test]
fn terminal_slug_from_ref_strips_prefixes() {
    assert_eq!(terminal_slug_from_ref("run-1"), "run-1");
    assert_eq!(terminal_slug_from_ref("cairn:~/terminal/run-1"), "run-1");
    assert_eq!(
        terminal_slug_from_ref("cairn://p/CAIRN/1/1/builder/terminal/run-1"),
        "run-1"
    );
    assert_eq!(terminal_slug_from_ref("  dev  "), "dev");
}

#[test]
fn return_content_flag_parses_off_the_target_query() {
    assert!(wants_return_content("cairn:~/browser?return_content=true"));
    assert!(wants_return_content("cairn:~/browser?return_content=1"));
    assert!(wants_return_content("cairn:~/browser?return_content"));
    assert!(wants_return_content(
        "cairn:~/browser?format=markdown&return_content=true"
    ));
    // Absent, false, or a different key does not enable it.
    assert!(!wants_return_content("cairn:~/browser"));
    assert!(!wants_return_content(
        "cairn:~/browser?return_content=false"
    ));
    assert!(!wants_return_content("cairn:~/browser?other=true"));
}

#[test]
fn parse_wake_filter_reads_terminal_output_on_and_phrase() {
    let it = item(
        "cairn://p/CAIRN/1/1/builder/wakes",
        ChangeMode::Append,
        None,
    );
    let value = serde_json::json!({
        "kind": "terminal",
        "ref": "cairn:~/terminal/dev",
        "on": "output",
        "phrase": "ready",
    });
    let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
    assert_eq!(filter.kind, "terminal");
    assert_eq!(filter.reference.as_deref(), Some("cairn:~/terminal/dev"));
    assert_eq!(filter.on.as_deref(), Some("output"));
    assert_eq!(filter.phrase.as_deref(), Some("ready"));
}

#[test]
fn parse_wake_filter_defaults_output_fields_to_none() {
    let it = item(
        "cairn://p/CAIRN/1/1/builder/wakes",
        ChangeMode::Append,
        None,
    );
    let value = serde_json::json!({ "kind": "terminal", "ref": "cairn:~/terminal/dev" });
    let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
    assert!(
        filter.on.is_none(),
        "on defaults to exit semantics downstream"
    );
    assert!(filter.phrase.is_none());
}

#[test]
fn gate_allows_payloadless_mutations() {
    // Issue patch has no required keys, but the arm still wants a payload;
    // the gate itself must not reject the missing payload.
    let it = item("cairn://p/CAIRN/1", ChangeMode::Patch, None);
    assert!(gate(&it).is_ok());
    // Terminal delete takes no payload and has no required keys.
    let it = item(
        "cairn://p/CAIRN/1/1/builder/terminal/dev",
        ChangeMode::Delete,
        None,
    );
    assert!(gate(&it).is_ok());
}
