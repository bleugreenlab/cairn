use super::feed::ack_token;
use super::todos::parse_todo_write_items;
use super::wakes::{
    normalize_posts_filter, parse_schedule_create, parse_schedule_reference, parse_wake_filter,
    terminal_slug_from_ref,
};
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

/// The replay action is the only sanctioned way a branch's ancestry moves, so it
/// has to actually reach dispatch. A contract entry nothing routes to is a
/// documented action that silently does nothing.
#[test]
fn gate_accepts_the_rebase_replay_action() {
    let it = item(
        "cairn://p/cairn/1/1/builder/rebase",
        ChangeMode::Patch,
        Some(serde_json::json!({
            "action": "replay",
            "resolution": "take-committed-tip"
        })),
    );
    let spec = gate(&it).expect("the replay mutation is gated in");
    assert_eq!(
        spec.label,
        "ask the store to replay this branch onto its base"
    );
}

/// A rebase session is read and replayed, never created or deleted by hand.
#[test]
fn gate_rejects_creating_or_deleting_a_rebase_session() {
    for mode in [ChangeMode::Create, ChangeMode::Delete, ChangeMode::Append] {
        let it = item("cairn://p/cairn/1/1/builder/rebase", mode, None);
        let failure = gate(&it).unwrap_err();
        assert!(
            failure.error.contains("Unsupported resource mutation"),
            "{mode:?}: {}",
            failure.error
        );
    }
}

#[test]
fn gate_rejects_apply_mode_with_enumeration() {
    let it = item(
        "cairn://p/cairn/1/1/builder/chat/1/2",
        ChangeMode::Apply,
        None,
    );
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("Unsupported resource mutation"));
}

#[test]
fn gate_rejects_unsupported_mode_and_lists_valid_ones() {
    // Issue supports patch/append/delete, not replace.
    let it = item("cairn://p/cairn/1", ChangeMode::Replace, None);
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("Unsupported resource mutation"));
    assert!(failure.error.contains("patch issue"));
    assert!(failure.error.contains("append comment"));
    assert!(failure.error.contains("delete issue"));
}

#[test]
fn gate_marks_read_only_resource() {
    let it = item("cairn://p/cairn/1/1/builder/chat", ChangeMode::Append, None);
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("read-only"));
}

#[test]
fn gate_names_missing_required_keys_with_example() {
    // Terminal create requires `command`.
    let it = item(
        "cairn://p/cairn/1/1/builder/terminal/dev",
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

/// The incident this gate exists for (CAIRN #4136): a thread following a broken
/// reply instruction wrote `{content, to:"external"}` to its own messages
/// resource. `to` was dropped, the append landed on the thread's own stream, and
/// the result read "Sent direct message" — a caller error turned into phantom
/// success, invisible until an unrelated wake echoed the misdelivery back.
#[test]
fn gate_rejects_the_unknown_key_that_caused_the_misdelivery() {
    let it = item(
        "cairn://p/cairn/1/1/builder/messages",
        ChangeMode::Append,
        Some(serde_json::json!({ "content": "hi", "to": "external" })),
    );
    let failure = gate(&it).unwrap_err();
    assert!(
        failure.error.contains("Unknown payload key"),
        "{}",
        failure.error
    );
    assert!(failure.error.contains("`to`"), "{}", failure.error);
    assert!(
        failure.error.contains("content"),
        "the rejection enumerates what the resource accepts: {}",
        failure.error
    );
    assert!(failure.error.contains("Example:"), "{}", failure.error);
}

/// The same append without the stray key still delivers, and the widened
/// `escalate` key rides along — closing the gate must not cost a real feature.
#[test]
fn gate_accepts_node_message_content_and_escalate() {
    for payload in [
        serde_json::json!({ "content": "hi" }),
        serde_json::json!({ "content": "hi", "escalate": true }),
    ] {
        let it = item(
            "cairn://p/cairn/1/1/builder/messages",
            ChangeMode::Append,
            Some(payload.clone()),
        );
        assert!(gate(&it).is_ok(), "{payload} should gate in");
    }
}

/// Widening is per-kind, not global. A project channel append ignores
/// `escalate`, so accepting it there would advertise a promise that surface does
/// not keep — the same class of bug as the incident.
#[test]
fn gate_rejects_escalate_on_a_channel_that_ignores_it() {
    let it = item(
        "cairn://p/cairn/messages",
        ChangeMode::Append,
        Some(serde_json::json!({ "content": "hi", "escalate": true })),
    );
    let failure = gate(&it).unwrap_err();
    assert!(
        failure.error.contains("Unknown payload key"),
        "{}",
        failure.error
    );
    assert!(failure.error.contains("`escalate`"), "{}", failure.error);
}

/// A declared alias is a promise: widening a key by alias would be worthless if
/// the gate then refused the spelling it was widened for.
#[test]
fn gate_accepts_a_declared_alias_as_a_known_key() {
    let it = item(
        "cairn://p/cairn/1/1/builder/memories/1",
        ChangeMode::Patch,
        Some(serde_json::json!({
            "action": "defer",
            "reason": "needs a wider scope",
            "new_scope": { "scope": "project", "value": "cairn" },
        })),
    );
    assert!(gate(&it).is_ok(), "snake_case alias must gate in");
}

/// One mode can declare SEVERAL payload shapes. A node memory patch is either an
/// edit or a triage; checking only the first-listed shape would reject the other
/// outright, so both have to clear the gate.
#[test]
fn gate_accepts_every_payload_shape_a_mode_declares() {
    for payload in [
        serde_json::json!({ "content": "revised", "status": "draft" }),
        serde_json::json!({ "action": "promote", "reason": "durable canon" }),
    ] {
        let it = item(
            "cairn://p/cairn/1/1/builder/memories/1",
            ChangeMode::Patch,
            Some(payload.clone()),
        );
        assert!(gate(&it).is_ok(), "{payload} is a declared shape");
    }

    // A key belonging to neither shape is still rejected, and the rejection
    // points at the alternative rather than silently quoting one shape.
    let it = item(
        "cairn://p/cairn/1/1/builder/memories/1",
        ChangeMode::Patch,
        Some(serde_json::json!({ "content": "x", "nonsense": true })),
    );
    let failure = gate(&it).unwrap_err();
    assert!(failure.error.contains("`nonsense`"), "{}", failure.error);
    assert!(
        failure.error.contains("different payload shape"),
        "{}",
        failure.error
    );
}

/// Artifact payloads are the addressed artifact's schema, resolved at write
/// time, so the contract cannot enumerate them. They must clear this gate and be
/// closed by the artifact handler against the real schema instead.
#[test]
fn gate_leaves_schema_resolved_artifact_payloads_open() {
    let it = item(
        "cairn://p/cairn/1/1/builder/plan",
        ChangeMode::Create,
        Some(serde_json::json!({ "anyFieldTheSchemaDeclares": "x" })),
    );
    assert!(gate(&it).is_ok(), "artifact writes stay schema-resolved");
}

/// Missing beats unknown: a caller short a required key is told that first,
/// because it is the more fundamental error.
#[test]
fn gate_reports_a_missing_key_ahead_of_an_unknown_one() {
    let it = item(
        "cairn://p/cairn/1/1/builder/terminal/dev",
        ChangeMode::Create,
        Some(serde_json::json!({ "nonsense": true })),
    );
    let failure = gate(&it).unwrap_err();
    assert!(
        failure.error.contains("Missing required payload key"),
        "{}",
        failure.error
    );
    assert!(failure.error.contains("command"), "{}", failure.error);
}

#[test]
fn gate_accepts_alias_for_required_key() {
    // tasks append requires subagentType; the snake_case alias must satisfy it.
    let it = item(
        "cairn://p/cairn/1/1/builder/tasks",
        ChangeMode::Append,
        Some(serde_json::json!({ "subagent_type": "Explore", "description": "map parser flow" })),
    );
    assert!(gate(&it).is_ok());
}

#[test]
fn gate_requires_task_description() {
    // tasks append requires both subagentType and description.
    let it = item(
        "cairn://p/cairn/1/1/builder/tasks",
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
        "cairn://p/cairn/issues",
        ChangeMode::Append,
        Some(serde_json::json!({ "title": "hi" })),
    );
    let spec = gate(&it).unwrap();
    assert_eq!(spec.label, "create issue");
}

/// A feed is read and acknowledged, and that is all. The gate has to let the
/// acknowledgement through at every coordinate a home is addressed at, and let
/// nothing else through at any of them.
#[test]
fn gate_accepts_only_the_feed_acknowledgement() {
    for target in [
        "cairn://p/cairn/1/1/builder/feed",
        "cairn://p/cairn/1/1/builder/task/probe/feed",
    ] {
        let ack = item(
            target,
            ChangeMode::Patch,
            Some(serde_json::json!({ "ack": "tok" })),
        );
        assert_eq!(
            gate(&ack).unwrap().label,
            "acknowledge the feed page just read",
            "{target}"
        );

        // A patch that names no token is missing the only key there is.
        let bare = item(target, ChangeMode::Patch, Some(serde_json::json!({})));
        let failure = gate(&bare).unwrap_err();
        assert!(failure.error.contains("ack"), "{}", failure.error);

        for mode in [ChangeMode::Append, ChangeMode::Replace, ChangeMode::Delete] {
            let it = item(target, mode, Some(serde_json::json!({ "ack": "tok" })));
            let failure = gate(&it).unwrap_err();
            assert!(
                failure.error.contains("Unsupported resource mutation"),
                "{target} {mode:?}: {}",
                failure.error
            );
            assert!(
                failure
                    .error
                    .contains("acknowledge the feed page just read"),
                "a rejection enumerates what the feed does accept: {}",
                failure.error
            );
        }
    }
}

/// The acknowledgement payload is one opaque token. Anything that could name a
/// position instead is refused by name rather than ignored.
#[test]
fn a_feed_acknowledgement_carries_a_token_and_nothing_else() {
    let it = item("cairn:~/feed", ChangeMode::Patch, None);
    assert_eq!(
        ack_token(0, &it, &serde_json::json!({ "ack": "  tok  " })).unwrap(),
        "tok"
    );
    for payload in [
        serde_json::json!({ "ack": "tok", "through": 42 }),
        serde_json::json!({ "position": 42 }),
        serde_json::json!({ "ack": "" }),
        serde_json::json!({ "ack": 42 }),
        serde_json::json!("tok"),
    ] {
        assert!(
            ack_token(0, &it, &payload).is_err(),
            "payload must be refused: {payload}"
        );
    }
}

#[test]
fn terminal_slug_from_ref_strips_prefixes() {
    assert_eq!(terminal_slug_from_ref("run-1"), "run-1");
    assert_eq!(terminal_slug_from_ref("cairn:~/terminal/run-1"), "run-1");
    assert_eq!(
        terminal_slug_from_ref("cairn://p/cairn/1/1/builder/terminal/run-1"),
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
fn parses_schedule_creation_payload() {
    let it = item(
        "cairn://p/cairn/1/1/builder/wakes",
        ChangeMode::Append,
        None,
    );
    let schedule = parse_schedule_create(
        0,
        &it,
        &serde_json::json!({ "every": "6h", "reason": "Review progress" }),
    )
    .unwrap();
    assert_eq!(schedule.every_ms, 21_600_000);
    assert_eq!(schedule.reason, "Review progress");
}

#[test]
fn schedule_lifecycle_is_intercepted_only_for_schedule_kind() {
    let it = item("cairn://p/cairn/1/1/builder/wakes", ChangeMode::Patch, None);
    assert_eq!(
        parse_schedule_reference(
            0,
            &it,
            &serde_json::json!({ "kind": "schedule", "ref": "schedule-id" }),
            "unmute",
        )
        .unwrap()
        .as_deref(),
        Some("schedule-id")
    );
    assert!(parse_schedule_reference(
        0,
        &it,
        &serde_json::json!({ "kind": "terminal", "ref": "dev" }),
        "unmute",
    )
    .unwrap()
    .is_none());
    assert!(
        parse_schedule_reference(0, &it, &serde_json::json!({ "kind": "schedule" }), "unmute",)
            .is_err()
    );
}

#[test]
fn parse_wake_filter_reads_terminal_output_on_and_phrase() {
    let it = item(
        "cairn://p/cairn/1/1/builder/wakes",
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
        "cairn://p/cairn/1/1/builder/wakes",
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

/// `kind:"posts"` is what an agent writes; `post` is what the wake schema
/// stores. Normalizing at the mutation boundary is what keeps the two from
/// becoming two vocabularies, so the alias and the stored kind must both land on
/// the same row.
#[test]
fn posts_wake_filter_normalizes_the_agent_facing_alias() {
    let it = item(
        "cairn://p/cairn/1/1/builder/wakes",
        ChangeMode::Append,
        None,
    );
    for written in ["posts", "post"] {
        let value = serde_json::json!({ "kind": written });
        let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
        let filter = normalize_posts_filter(0, &it, filter).unwrap();
        assert_eq!(filter.kind, "post");
        assert!(filter.reference.is_none());
    }
}

/// A Posts watch governs the whole corpus and decides its reach from the post's
/// scope at delivery. A caller-supplied `ref` would either silently do nothing
/// or look like a way to name a destination the subscription does not govern, so
/// it is refused rather than ignored.
#[test]
fn a_posts_wake_filter_refuses_a_caller_supplied_ref() {
    let it = item(
        "cairn://p/cairn/1/1/builder/wakes",
        ChangeMode::Append,
        None,
    );
    let value = serde_json::json!({ "kind": "posts", "ref": "cairn://posts/7" });
    let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
    assert!(normalize_posts_filter(0, &it, filter).is_err());
}

/// Normalization is scoped to the Posts alias: every other subscribe vocabulary
/// passes through untouched, `ref` included.
#[test]
fn posts_normalization_leaves_other_wake_kinds_alone() {
    let it = item(
        "cairn://p/cairn/1/1/builder/wakes",
        ChangeMode::Append,
        None,
    );
    let value = serde_json::json!({ "kind": "terminal", "ref": "cairn:~/terminal/dev" });
    let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
    let filter = normalize_posts_filter(0, &it, filter).unwrap();
    assert_eq!(filter.kind, "terminal");
    assert_eq!(filter.reference.as_deref(), Some("cairn:~/terminal/dev"));
}

#[test]
fn gate_allows_payloadless_mutations() {
    // Issue patch has no required keys, but the arm still wants a payload;
    // the gate itself must not reject the missing payload.
    let it = item("cairn://p/cairn/1", ChangeMode::Patch, None);
    assert!(gate(&it).is_ok());
    // Terminal delete takes no payload and has no required keys.
    let it = item(
        "cairn://p/cairn/1/1/builder/terminal/dev",
        ChangeMode::Delete,
        None,
    );
    assert!(gate(&it).is_ok());
}

/// The fleet's management surface is the resource graph, so the gate has to let
/// exactly the three advertised operations through — and no more, because a
/// mode that reaches dispatch without a contract entry is an undocumented
/// capability.
#[test]
fn gate_accepts_enrollment_configuration_and_removal_only() {
    let enroll = item(
        "cairn://executors",
        ChangeMode::Create,
        Some(serde_json::json!({ "host": "bglab-ub.local", "sshUser": "mitch" })),
    );
    assert_eq!(gate(&enroll).unwrap().label, "enroll a machine");

    let configure = item(
        "cairn://executors/bglab-ub",
        ChangeMode::Patch,
        Some(serde_json::json!({ "draining": true, "expectedGeneration": 7 })),
    );
    assert_eq!(
        gate(&configure).unwrap().label,
        "configure an enrolled machine"
    );

    let remove = item("cairn://executors/bglab-ub", ChangeMode::Delete, None);
    assert!(gate(&remove).is_ok());

    for mode in [ChangeMode::Append, ChangeMode::Replace, ChangeMode::Patch] {
        let it = item("cairn://executors", mode, Some(serde_json::json!({})));
        let failure = gate(&it).unwrap_err();
        assert!(
            failure.error.contains("Unsupported resource mutation"),
            "{mode:?}: {}",
            failure.error
        );
        assert!(
            failure.error.contains("enroll a machine"),
            "a rejection enumerates what the fleet does accept: {}",
            failure.error
        );
    }
}

/// Enrollment asks for the two facts a person actually has. Everything else —
/// identity, paths, tunnel port, project membership — is derived, so requiring
/// any of it would be asking an operator to invent runner internals.
#[test]
fn enrollment_requires_only_the_host_and_the_ssh_user() {
    let missing = item(
        "cairn://executors",
        ChangeMode::Create,
        Some(serde_json::json!({ "host": "bglab-ub.local" })),
    );
    let failure = gate(&missing).unwrap_err();
    assert!(failure.error.contains("Missing required payload key"));
    assert!(failure.error.contains("sshUser"));

    // A snake_case alias satisfies the gate, matching the advertised aliases.
    let aliased = item(
        "cairn://executors",
        ChangeMode::Create,
        Some(serde_json::json!({ "host": "bglab-ub.local", "ssh_user": "mitch" })),
    );
    assert!(gate(&aliased).is_ok());

    // No project selection at all is a complete request: omitted means every
    // project, which is eligibility rather than an unanswered question.
    let all_projects = item(
        "cairn://executors",
        ChangeMode::Create,
        Some(serde_json::json!({ "host": "bglab-ub.local", "sshUser": "mitch" })),
    );
    assert!(gate(&all_projects).is_ok());
}
