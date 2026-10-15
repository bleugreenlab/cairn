//! How long every callback layer may wait for the host, stated once for every
//! crate that wraps a host wait.
//!
//! A callback travels through up to three layers before the host answers it,
//! and each layer has its own timeout. The rule this module exists to hold is
//! that **every layer's bound sits strictly above the host wait it wraps**. A
//! wrapper that fires first hands the agent a transport error for work the host
//! is still doing, and the agent's natural next move (retry) duplicates it.
//!
//! The paths that wrap a host wait, and what each must exceed:
//!
//! - **The cairn-cmd HTTP socket** (colocated, and inside an executor cell)
//!   uses [`socket_timeout`]. Colocated it wraps the host directly; in a cell
//!   it wraps the executor's relay, so it is [`relay_timeout`] plus
//!   [`RELAY_HOP_MARGIN`].
//! - **The executor's in-cell MCP relay** (`McpRelay` in cairn-exec) uses
//!   [`relay_timeout`]: the host's answer bound for that verb
//!   ([`host_answer_bound`]) plus [`CALLBACK_TIMEOUT_MARGIN`].
//! - **The fence round trip** (`RunnerCallback::SandboxDenied`, sent by
//!   cairn-exec and awaited by the runner's permission wait) uses
//!   [`runner_callback_timeout`]: [`RELAYED_FENCE_WAIT_BUDGET`] plus
//!   [`CALLBACK_TIMEOUT_MARGIN`]. It once used a flat 30 s, under the runner's
//!   45 s inline wait, so the agent was told "runner callback timed out" for a
//!   permission that was still pending and then had its turn parked anyway.
//!   Executors built before this contract still use that flat 30 s
//!   ([`LEGACY_EXECUTOR_CALLBACK_TIMEOUT`]), which is why the relayed fence
//!   wait itself is held below it.
//!
//! The host-side inline waits themselves (a permission, a question, a task
//! spawn) all use [`INLINE_DURABLE_WAIT_BUDGET`]; past it the host durably
//! suspends and answers with a marker, so the wait it exposes to its callers
//! never exceeds the budget.
//!
//! The ordering is enforced at compile time by the `const` assertions below
//! and again, per request shape, by this module's tests.
use std::time::Duration;

use crate::executor_protocol::RunnerCallback;
use crate::protocol::CallbackRequest;
use crate::run_contract::RUN_GRACE_WINDOW_MS;
use crate::uri::{parse_uri, CairnResource};
use crate::write_contract::{WRITE_CHECK_INLINE_WAIT_MS, WRITE_STORE_LOCK_WAIT_MS};

/// How long the host waits inline for a permission answer, a question answer,
/// or a spawned task before durably suspending the run (no auto-deny): long
/// enough that a prompt answer keeps the agent's process warm, and short
/// enough that every callback layer wrapping it outlives it with margin.
pub const INLINE_DURABLE_WAIT_BUDGET: Duration = Duration::from_secs(45);

/// Margin added above a host wait so the layer wrapping it always outlives the
/// host's own answer, and the host's result wins the race.
pub const CALLBACK_TIMEOUT_MARGIN: Duration = Duration::from_secs(60);

/// Margin the cairn-cmd socket adds above the executor relay it talks through
/// inside a cell, so the relay's own timeout (which it reports) fires first.
pub const RELAY_HOP_MARGIN: Duration = Duration::from_secs(15);

/// Bound for verbs that block on an unbounded external event with no host-side
/// timeout below them: a blocking `write` append to a node's tasks/questions
/// collection (a sub-agent task or user question that may legitimately run far
/// longer than any `run` batch). The host owns these awaits; no wrapper may
/// undercut them. Six days is effectively "no ceiling" while still bounding a
/// wedged socket, and the socket built on it stays strictly below the spawned
/// agent's MCP tool timeout (`MCP_TOOL_TIMEOUT` / Codex `tool_timeout_sec`, set
/// to 7 days) so the agent never abandons cairn-cmd mid-await.
pub const UNBOUNDED_CALLBACK_TIMEOUT: Duration = Duration::from_secs(6 * 24 * 60 * 60);

/// Bound for runner callbacks that only record bookkeeping (a cache checkpoint,
/// process events, item start/completion) and wait on no inline host budget.
pub const BOOKKEEPING_CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);

/// The flat bound every executor built before this contract applies to EVERY
/// runner callback, the fence round trip included. Those executors stay in the
/// fleet until their next published artifact, and nothing on the wire says
/// which ones they are, so the runner has to fit inside it.
pub const LEGACY_EXECUTOR_CALLBACK_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the runner waits inline on a fence crossing relayed from an
/// executor batch before durably suspending.
///
/// Shorter than [`INLINE_DURABLE_WAIT_BUDGET`] on purpose. The runner cannot
/// see the executor's callback waiter: the batch context it does see stays
/// open while a sibling item runs, long after one item's callback was
/// abandoned. Answering inside [`LEGACY_EXECUTOR_CALLBACK_TIMEOUT`] means even
/// an older executor is still listening when the suspend marker arrives, so
/// the park never interrupts a turn whose tool call was told something else.
pub const RELAYED_FENCE_WAIT_BUDGET: Duration = Duration::from_secs(20);

/// What the relayed fence wait leaves between its answer and a legacy
/// executor's give-up: the runner's own bookkeeping after the budget, and the
/// frame's trip back over the link.
const LEGACY_HANDOFF_MARGIN: Duration = Duration::from_secs(5);

/// The fence round trip: the runner may hold a `SandboxDenied` callback for the
/// whole relayed budget before answering with a suspend marker.
pub const FENCE_CALLBACK_TIMEOUT: Duration =
    add(RELAYED_FENCE_WAIT_BUDGET, CALLBACK_TIMEOUT_MARGIN);

/// The host's grace window for a `run` batch. It answers within it either way:
/// synchronously if the batch settled, with a suspend marker if not.
const RUN_HOST_BOUND: Duration = Duration::from_millis(RUN_GRACE_WINDOW_MS);

/// The longest bounded host wait for every other verb: a file-target `write`
/// queued on the project store lock, then waiting inline for the when:write
/// checks of the commit it sealed. A `watch` long-poll (290 s) and a `read`
/// both finish well inside it.
const DEFAULT_HOST_BOUND: Duration =
    Duration::from_millis(WRITE_STORE_LOCK_WAIT_MS + WRITE_CHECK_INLINE_WAIT_MS);

const fn add(a: Duration, b: Duration) -> Duration {
    match a.checked_add(b) {
        Some(sum) => sum,
        None => panic!("callback bound overflow"),
    }
}

const fn above(outer: Duration, inner: Duration) -> bool {
    outer.as_millis() > inner.as_millis()
}

const _: () = {
    // The fence round trip outlives the inline wait it carries.
    assert!(above(FENCE_CALLBACK_TIMEOUT, RELAYED_FENCE_WAIT_BUDGET));
    // And the relayed wait answers while an older executor still listens.
    assert!(above(
        LEGACY_EXECUTOR_CALLBACK_TIMEOUT,
        add(RELAYED_FENCE_WAIT_BUDGET, LEGACY_HANDOFF_MARGIN)
    ));
    // Bookkeeping callbacks never carry an inline wait, but keep their bound
    // honest: it must not be mistaken for one that can.
    assert!(above(FENCE_CALLBACK_TIMEOUT, BOOKKEEPING_CALLBACK_TIMEOUT));
    // Every relay bound outlives its host bound, and every socket its relay.
    assert!(above(
        add(RUN_HOST_BOUND, CALLBACK_TIMEOUT_MARGIN),
        RUN_HOST_BOUND
    ));
    assert!(above(
        add(DEFAULT_HOST_BOUND, CALLBACK_TIMEOUT_MARGIN),
        DEFAULT_HOST_BOUND
    ));
    assert!(above(
        UNBOUNDED_CALLBACK_TIMEOUT,
        add(DEFAULT_HOST_BOUND, CALLBACK_TIMEOUT_MARGIN)
    ));
    assert!(above(
        UNBOUNDED_CALLBACK_TIMEOUT,
        add(RUN_HOST_BOUND, CALLBACK_TIMEOUT_MARGIN)
    ));
    // The host's inline waits fit inside the shortest request it can arrive on.
    assert!(above(RUN_HOST_BOUND, INLINE_DURABLE_WAIT_BUDGET));
};

/// How long the host may take to answer a request before it must have replied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostAnswerBound {
    Bounded(Duration),
    /// The host awaits an external event (a sub-agent task, a user's answer)
    /// with no timeout of its own.
    Unbounded,
}

/// True if a `write` payload contains a blocking append to a node's
/// tasks/questions collection, an await with no host-side timeout below it
/// (the host blocks until the sub-agent task completes or the user answers).
pub fn change_has_blocking_append(payload: &serde_json::Value) -> bool {
    let Some(changes) = payload.get("changes").and_then(|c| c.as_array()) else {
        return false;
    };
    changes.iter().any(|item| {
        let is_append = item.get("mode").and_then(|m| m.as_str()) == Some("append");
        is_append
            && item
                .get("target")
                .and_then(|t| t.as_str())
                .and_then(parse_uri)
                .is_some_and(|r| {
                    matches!(
                        r,
                        CairnResource::NodeTasks { .. } | CairnResource::NodeQuestions { .. }
                    )
                })
    })
}

/// The host's answer bound for a request. `run` always answers within its
/// grace window; a blocking tasks/questions append awaits an unbounded external
/// event; everything else answers within the store-lock wait plus the inline
/// write-check wait.
///
/// A `run` item's own `timeout` is deliberately absent: it decides when that
/// item is killed, never when the host answers. Deriving the bound per item is
/// what once let a no-timeout command fail the agent at a 180-second socket
/// while the host ran on for ten minutes.
pub fn host_answer_bound(request: &CallbackRequest) -> HostAnswerBound {
    match request.tool.as_str() {
        "run" => HostAnswerBound::Bounded(RUN_HOST_BOUND),
        "write" if change_has_blocking_append(&request.payload) => HostAnswerBound::Unbounded,
        _ => HostAnswerBound::Bounded(DEFAULT_HOST_BOUND),
    }
}

/// The executor relay's bound for a request forwarded from inside a cell.
pub fn relay_timeout(request: &CallbackRequest) -> Duration {
    match host_answer_bound(request) {
        HostAnswerBound::Bounded(bound) => add(bound, CALLBACK_TIMEOUT_MARGIN),
        HostAnswerBound::Unbounded => UNBOUNDED_CALLBACK_TIMEOUT,
    }
}

/// The cairn-cmd HTTP socket's bound for a request. Inside a cell it wraps the
/// relay; colocated it wraps the host directly and simply has a little more
/// room than it needs.
pub fn socket_timeout(request: &CallbackRequest) -> Duration {
    add(relay_timeout(request), RELAY_HOP_MARGIN)
}

/// The executor's bound for one runner callback round trip.
pub fn runner_callback_timeout(callback: &RunnerCallback) -> Duration {
    match callback {
        RunnerCallback::SandboxDenied { .. } => FENCE_CALLBACK_TIMEOUT,
        RunnerCallback::CacheCheckpoint { .. }
        | RunnerCallback::ProcessEvent { .. }
        | RunnerCallback::ProcessItemStarted { .. }
        | RunnerCallback::ProcessItemCompleted { .. } => BOOKKEEPING_CALLBACK_TIMEOUT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor_protocol::SandboxDenial;

    /// The spawned agent's MCP tool timeout: `MCP_TOOL_TIMEOUT=604800000` in
    /// the Claude backends and `tool_timeout_sec=604800` for Codex.
    const AGENT_MCP_TOOL_TIMEOUT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

    fn callback_request(tool: &str, payload: serde_json::Value) -> CallbackRequest {
        CallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: tool.to_string(),
            payload,
            tool_use_id: None,
        }
    }

    fn blocking_append(collection: &str) -> CallbackRequest {
        callback_request(
            "write",
            serde_json::json!({
                "changes": [{
                    "target": format!("cairn://p/CAIRN/1621/1/builder/{collection}"),
                    "mode": "append",
                    "payload": { "prompt": "do a thing" }
                }]
            }),
        )
    }

    fn sample_requests() -> Vec<CallbackRequest> {
        vec![
            callback_request(
                "run",
                serde_json::json!({ "commands": [{ "command": "x" }] }),
            ),
            callback_request(
                "write",
                serde_json::json!({ "changes": [{ "target": "file:a", "mode": "create" }] }),
            ),
            callback_request("read_batch", serde_json::json!({ "paths": ["file:x"] })),
            callback_request("watch", serde_json::json!({})),
            blocking_append("tasks"),
            blocking_append("questions"),
        ]
    }

    fn sandbox_denied() -> RunnerCallback {
        RunnerCallback::SandboxDenied {
            runner_context_id: "ctx".into(),
            command: "git push".into(),
            cwd: "/w".into(),
            denial: SandboxDenial::Command,
        }
    }

    #[test]
    fn every_bound_sits_strictly_above_the_host_budget_it_wraps() {
        for request in sample_requests() {
            let relay = relay_timeout(&request);
            let socket = socket_timeout(&request);
            match host_answer_bound(&request) {
                HostAnswerBound::Bounded(host) => assert!(
                    relay > host,
                    "{}: relay {relay:?} must outlive host {host:?}",
                    request.tool
                ),
                HostAnswerBound::Unbounded => {
                    assert_eq!(relay, UNBOUNDED_CALLBACK_TIMEOUT);
                    assert!(relay > relay_timeout(&sample_requests()[1]));
                }
            }
            assert!(
                socket > relay,
                "{}: socket {socket:?} must outlive relay {relay:?}",
                request.tool
            );
            // The host's inline waits ride inside every bounded request.
            assert!(relay > INLINE_DURABLE_WAIT_BUDGET);
        }
    }

    #[test]
    fn the_fence_round_trip_outlives_the_inline_wait() {
        let bound = runner_callback_timeout(&sandbox_denied());
        assert!(
            bound > RELAYED_FENCE_WAIT_BUDGET,
            "the executor must wait for the runner's suspend marker, not give up first: \
             {bound:?} vs inline {RELAYED_FENCE_WAIT_BUDGET:?}"
        );
        assert_eq!(bound, RELAYED_FENCE_WAIT_BUDGET + CALLBACK_TIMEOUT_MARGIN);
    }

    /// The runner cannot tell an older executor from a newer one, and an older
    /// one gives up on every callback at a flat 30 s. The relayed fence wait
    /// must answer inside that, with room for the frame to travel back.
    #[test]
    fn a_relayed_fence_wait_answers_before_any_legacy_executor_gives_up() {
        assert!(
            RELAYED_FENCE_WAIT_BUDGET + LEGACY_HANDOFF_MARGIN < LEGACY_EXECUTOR_CALLBACK_TIMEOUT
        );
        assert!(RELAYED_FENCE_WAIT_BUDGET <= INLINE_DURABLE_WAIT_BUDGET);
    }

    #[test]
    fn bookkeeping_callbacks_keep_the_short_bound() {
        let callbacks = [
            RunnerCallback::CacheCheckpoint {
                runner_context_id: "ctx".into(),
                command: "c".into(),
                cwd: "/w".into(),
                exit_code: Some(0),
            },
            RunnerCallback::ProcessEvent {
                runner_context_id: "ctx".into(),
                stream_id: "s".into(),
                payload: "p".into(),
            },
            RunnerCallback::ProcessItemStarted {
                runner_context_id: "ctx".into(),
                stream_id: "s".into(),
            },
            RunnerCallback::ProcessItemCompleted {
                runner_context_id: "ctx".into(),
                stream_id: "s".into(),
                succeeded: true,
                exit_code: Some(0),
                timed_out: false,
                duration_ms: 1,
            },
        ];
        for callback in callbacks {
            assert_eq!(
                runner_callback_timeout(&callback),
                BOOKKEEPING_CALLBACK_TIMEOUT,
                "{}",
                callback.kind()
            );
        }
    }

    #[test]
    fn the_unbounded_socket_stays_below_the_agent_mcp_tool_timeout() {
        for collection in ["tasks", "questions"] {
            let socket = socket_timeout(&blocking_append(collection));
            assert!(
                socket < AGENT_MCP_TOOL_TIMEOUT,
                "the agent must not abandon cairn-cmd mid-await: {socket:?}"
            );
        }
    }

    /// The socket bound is a property of the host's grace window, not of the
    /// batch: an item's `timeout` must not move it.
    #[test]
    fn the_run_bound_is_constant_across_item_timeouts() {
        let batches = [
            serde_json::json!({ "commands": [{ "command": "echo hi" }] }),
            serde_json::json!({
                "commands": [{ "command": "sleep 1", "timeout": 5_000_000u32 }]
            }),
            serde_json::json!({
                "commands": [
                    { "command": "sleep 150", "timeout": 300_000u32 },
                    { "command": "sleep 150", "timeout": 300_000u32 }
                ],
                "sequential": true
            }),
        ];
        for payload in batches {
            assert_eq!(
                host_answer_bound(&callback_request("run", payload.clone())),
                HostAnswerBound::Bounded(RUN_HOST_BOUND),
                "item timeouts must not move the bound: {payload}"
            );
        }
        assert_eq!(
            relay_timeout(&callback_request("run", serde_json::json!({}))),
            Duration::from_millis(RUN_GRACE_WINDOW_MS) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn a_blocking_tasks_or_questions_append_is_unbounded() {
        for collection in ["tasks", "questions"] {
            assert_eq!(
                host_answer_bound(&blocking_append(collection)),
                HostAnswerBound::Unbounded,
                "{collection} append should not be undercut"
            );
        }
    }

    #[test]
    fn a_plain_write_a_non_append_and_a_read_use_the_write_bound() {
        let requests = [
            callback_request(
                "write",
                serde_json::json!({
                    "changes": [{ "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } }]
                }),
            ),
            callback_request(
                "write",
                serde_json::json!({
                    "changes": [{ "target": "cairn://p/CAIRN/1621/1/builder/tasks", "mode": "patch", "payload": {} }]
                }),
            ),
            callback_request("read_batch", serde_json::json!({ "paths": ["file:x"] })),
        ];
        for request in requests {
            assert_eq!(
                host_answer_bound(&request),
                HostAnswerBound::Bounded(Duration::from_millis(
                    WRITE_STORE_LOCK_WAIT_MS + WRITE_CHECK_INLINE_WAIT_MS
                ))
            );
        }
    }

    /// A committing write may wait on the store lock and then on its own
    /// when:write checks. Both waits ride inside the host bound, so the socket
    /// that carries the write always outlives the host's answer and the agent
    /// receives the commit instead of a transport error.
    #[test]
    fn the_write_socket_outlives_the_lock_wait_plus_the_inline_check_wait() {
        let write = callback_request(
            "write",
            serde_json::json!({ "changes": [{ "target": "file:a", "mode": "create" }] }),
        );
        let host_waits = Duration::from_millis(WRITE_STORE_LOCK_WAIT_MS)
            + Duration::from_millis(WRITE_CHECK_INLINE_WAIT_MS);
        assert!(relay_timeout(&write) > host_waits);
        assert!(socket_timeout(&write) > relay_timeout(&write));
    }
}
