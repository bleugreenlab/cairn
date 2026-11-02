//! The `run` verb's time contract, stated once for every crate that must agree
//! about it.
//!
//! Three layers once derived a run item's budget independently — the cairn-cmd
//! HTTP callback socket, the executor batch item, and the host-local fallback —
//! and disagreed by a factor of six. The socket was the smallest, so it died
//! first and discarded every byte a full test suite had produced while the suite
//! itself kept running. These constants exist so "how long may this take" has
//! exactly one answer: cairn-cmd sizes its socket from the grace window and
//! advertises the ceiling in the `run` tool contract, cairn-core enforces both.
//!
//! [`declared_need`] is the third shared answer: how long a batch whose items
//! all declared a timeout said it is worth. A `run` call stays open on its
//! socket for that long (the grace window at least) before it parks; see
//! `callback_contract::host_answer_bound`.

use std::time::Duration;

use crate::uri::{parse_uri, CairnResource};

/// How long a `run` call stays synchronous. A batch that settles inside this
/// window returns from the original call; past it the call suspends durably and
/// the agent resumes with the same completed result. Grace governs the SHAPE of
/// the call and nothing else — it never bounds an item.
pub const RUN_GRACE_WINDOW_MS: u64 = 120_000;

/// The absolute bound on one `run` batch, and therefore on any single item in
/// it. An item that omits `timeout` is bounded only by this, so a real build or
/// test suite runs to completion; an explicit `timeout` is honored up to it.
///
/// There is deliberately no second, smaller bound underneath. A quieter
/// ten-minute cap once sat here and killed a no-timeout `bun run test:rust`
/// halfway through with no way for the agent to tell that from a real failure.
/// A batch that genuinely reaches six hours is a process that should have been a
/// terminal, and it fails loudly saying exactly that.
pub const RUN_BATCH_CEILING_MS: u32 = 6 * 60 * 60 * 1_000;

/// [`RUN_BATCH_CEILING_MS`] in whole hours, for the prose that advertises it to
/// agents. The advertised text is generated from this so the documented maximum
/// and the enforced one cannot drift apart.
pub const RUN_BATCH_CEILING_HOURS: u32 = RUN_BATCH_CEILING_MS / (60 * 60 * 1_000);

/// One `run` item as the declared-need formula sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredItem {
    /// The item's own `timeout`, in milliseconds, or `None` when it declared
    /// none (run to completion).
    pub timeout_ms: Option<u64>,
    /// The item runs on the host and cannot suspend past the grace window (a
    /// REPL send, a decide, an executor action), so its timeout is clamped to
    /// grace wherever it runs.
    pub host_clamped: bool,
}

/// How long a batch said it is worth: the longest of its items when they run in
/// parallel, their sum when they run in order, capped at the batch ceiling.
///
/// `None` unless EVERY item declared a timeout. An omitted `timeout` means
/// "run to completion", and one such item makes the whole batch undeclared.
/// Host-clamped items contribute `min(timeout, grace)`, since that is the most
/// they can take.
///
/// The host sizes two things from this one number: how long a call stays
/// open on its socket before it parks, and how long a fully declared batch may
/// wait for room on a busy machine. The client sizes its socket from the same
/// number, so both sides of the wire agree without saying it.
pub fn declared_need(
    items: impl IntoIterator<Item = DeclaredItem>,
    sequential: bool,
) -> Option<Duration> {
    let ceiling = u64::from(RUN_BATCH_CEILING_MS);
    let mut any = false;
    let mut total: u64 = 0;
    for item in items {
        any = true;
        let mut ms = item.timeout_ms?.min(ceiling);
        if item.host_clamped {
            ms = ms.min(RUN_GRACE_WINDOW_MS);
        }
        total = if sequential {
            total.saturating_add(ms)
        } else {
            total.max(ms)
        };
    }
    any.then(|| Duration::from_millis(total.min(ceiling)))
}

/// [`declared_need`] read straight off a raw `run` payload, the way both the
/// client (before it sends) and the host (when it answers) see it.
///
/// The raw payload is the authority on purpose: a procedure invocation expands
/// on the host into a stored batch the client never sees, so both sides judge
/// the call by what was actually sent (a procedure invocation declares no
/// timeout, so it is undeclared).
pub fn declared_need_of_payload(payload: &serde_json::Value) -> Option<Duration> {
    let commands = payload.get("commands")?.as_array()?;
    let sequential = payload
        .get("sequential")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let items: Option<Vec<DeclaredItem>> = commands
        .iter()
        .map(|item| {
            let timeout_ms = match item.get("timeout") {
                None | Some(serde_json::Value::Null) => None,
                // A timeout the host cannot parse as one is not a declaration.
                Some(value) => Some(value.as_u64()?),
            };
            Some(DeclaredItem {
                timeout_ms,
                host_clamped: is_host_clamped_item(
                    item.get("repl").is_some_and(|repl| !repl.is_null()),
                    item.get("target").and_then(serde_json::Value::as_str),
                ),
            })
        })
        .collect();
    declared_need(items?, sequential)
}

/// True for a batch that is host control flow rather than execution: a
/// `waitFor`, or a workflow run target. Its answer window is the grace window
/// whatever it declares, because it either answers at once or suspends.
pub fn is_control_flow_batch(payload: &serde_json::Value) -> bool {
    let Some(commands) = payload
        .get("commands")
        .and_then(serde_json::Value::as_array)
    else {
        return false;
    };
    commands.iter().any(|item| {
        item.get("waitFor").is_some_and(|wait| !wait.is_null())
            || matches!(
                item_target(item),
                Some(CairnResource::Workflow { .. } | CairnResource::ProjectWorkflow { .. })
            )
    })
}

fn item_target(item: &serde_json::Value) -> Option<CairnResource> {
    item.get("target")
        .and_then(serde_json::Value::as_str)
        .and_then(parse_uri)
}

/// A cairn-models facet verb reached through `run` at `cairn://models/<verb>`:
/// an item the host answers itself by calling the facet with the model the
/// caller names, never a process. Every door shares one run-dispatch arm; what
/// differs between them is stated here, so the classifiers that need it (the
/// grace window, the host-item cap) read one answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelDoor {
    Decide,
    Embed,
    Complete,
    Image,
    Speak,
}

impl ModelDoor {
    /// The door `resource` addresses, if it addresses one.
    pub fn for_resource(resource: &CairnResource) -> Option<Self> {
        match resource {
            CairnResource::ModelsDecide => Some(Self::Decide),
            CairnResource::ModelsEmbed => Some(Self::Embed),
            CairnResource::ModelsComplete => Some(Self::Complete),
            CairnResource::ModelsImage => Some(Self::Image),
            CairnResource::ModelsSpeak => Some(Self::Speak),
            _ => None,
        }
    }

    /// The URI the door is addressed by.
    pub fn uri(self) -> &'static str {
        match self {
            Self::Decide => "cairn://models/decide",
            Self::Embed => "cairn://models/embed",
            Self::Complete => "cairn://models/complete",
            Self::Image => "cairn://models/image",
            Self::Speak => "cairn://models/speak",
        }
    }

    /// Whether an item may run past the grace window, suspending its batch as a
    /// response item does. A door that always answers inside its own short
    /// per-request bounds keeps the host-item cap instead, and never makes its
    /// batch suspendable.
    pub fn outlives_grace(self) -> bool {
        match self {
            // Each provider request has its own short bound, and a corpus too
            // large for the window is sent in several calls.
            Self::Decide | Self::Embed => false,
            // It is the verb a response item calls, and a reasoning model's
            // answer can take minutes.
            Self::Complete => true,
            // Generation is slow (the facet allows an image 180 s), and a
            // long clip speaks for as long as it takes to say.
            Self::Image | Self::Speak => true,
        }
    }

    /// Whether the door's reply rides the run result's text whole, outside the
    /// cap that keeps command output readable. A reply is data a caller parses
    /// (vectors, a structured answer, a media reference), and a reply cut to
    /// fit reads as a valid shorter one, so no door's is ever cut or summarised.
    pub fn exempt_from_text_cap(self) -> bool {
        match self {
            Self::Decide | Self::Embed | Self::Complete | Self::Image | Self::Speak => true,
        }
    }
}

/// True for an item that runs on the host and cannot suspend past the grace
/// window: a REPL send, an executor action, or a model door that keeps the
/// host-item cap ([`ModelDoor::outlives_grace`]).
pub fn is_host_clamped_item(repl: bool, target: Option<&str>) -> bool {
    repl || match target.and_then(parse_uri) {
        Some(CairnResource::ExecutorAction { .. }) => true,
        Some(resource) => {
            ModelDoor::for_resource(&resource).is_some_and(|door| !door.outlives_grace())
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn need(payload: serde_json::Value) -> Option<Duration> {
        declared_need_of_payload(&payload)
    }

    #[test]
    fn parallel_items_are_worth_the_longest_and_sequential_the_sum() {
        let items = json!([
            {"command": "a", "timeout": 1000},
            {"command": "b", "timeout": 4000}
        ]);
        assert_eq!(
            need(json!({"commands": items})),
            Some(Duration::from_millis(4000))
        );
        assert_eq!(
            need(json!({"commands": items, "sequential": true})),
            Some(Duration::from_millis(5000))
        );
    }

    #[test]
    fn one_undeclared_item_makes_the_batch_undeclared() {
        assert_eq!(need(json!({"commands": [{"command": "bun test"}]})), None);
        assert_eq!(
            need(json!({"commands": [
                {"command": "a", "timeout": 1000},
                {"command": "b"}
            ]})),
            None
        );
        assert_eq!(need(json!({"commands": []})), None);
        assert_eq!(need(json!({})), None);
        // A timeout the host could not parse is not a declaration.
        assert_eq!(
            need(json!({"commands": [{"command": "a", "timeout": "soon"}]})),
            None
        );
    }

    #[test]
    fn the_need_is_capped_at_the_batch_ceiling() {
        let ceiling = Duration::from_millis(u64::from(RUN_BATCH_CEILING_MS));
        assert_eq!(
            need(json!({"commands": [{"command": "a", "timeout": 99_000_000u64}]})),
            Some(ceiling)
        );
        assert_eq!(
            need(json!({"sequential": true, "commands": [
                {"command": "a", "timeout": RUN_BATCH_CEILING_MS},
                {"command": "b", "timeout": RUN_BATCH_CEILING_MS}
            ]})),
            Some(ceiling)
        );
    }

    #[test]
    fn host_items_contribute_at_most_the_grace_window() {
        let grace = Duration::from_millis(RUN_GRACE_WINDOW_MS);
        for item in [
            json!({"code": "1", "interpreter": "python", "repl": "s", "timeout": 600_000}),
            json!({"target": "cairn://models/decide", "timeout": 600_000}),
            json!({"target": "cairn://models/embed", "timeout": 600_000}),
        ] {
            assert_eq!(
                need(json!({"commands": [item.clone()]})),
                Some(grace),
                "{item}"
            );
        }
        assert_eq!(
            need(json!({"commands": [
                {"target": "cairn://models/decide", "timeout": 600_000},
                {"command": "a", "timeout": 300_000}
            ]})),
            Some(Duration::from_millis(300_000))
        );
    }

    #[test]
    fn every_model_door_is_addressed_by_its_own_uri_and_only_complete_suspends() {
        for door in [ModelDoor::Decide, ModelDoor::Embed, ModelDoor::Complete] {
            let resource = parse_uri(door.uri()).expect("a door's URI parses");
            assert_eq!(ModelDoor::for_resource(&resource), Some(door));
            assert_eq!(
                is_host_clamped_item(false, Some(door.uri())),
                !door.outlives_grace(),
                "{door:?}"
            );
        }
        assert!(ModelDoor::Complete.outlives_grace());
        assert!(!is_host_clamped_item(false, Some("cairn://models/catalog")));
    }

    #[test]
    fn waits_and_workflows_are_control_flow() {
        assert!(is_control_flow_batch(
            &json!({"commands": [{"waitFor": {"duration": "3m"}}]})
        ));
        assert!(is_control_flow_batch(
            &json!({"commands": [{"target": "cairn://workflows/review", "timeout": 900_000}]})
        ));
        assert!(!is_control_flow_batch(
            &json!({"commands": [{"command": "x", "timeout": 900_000}]})
        ));
    }
}
