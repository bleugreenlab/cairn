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
//!   ([`host_answer_bound`]), or a longer bound a `run` sender stated, plus
//!   [`CALLBACK_TIMEOUT_MARGIN`].
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
//! ## A `run` call answers in its turn
//!
//! A `run` call stays open for its declared item timeouts and answers in the
//! same turn. Its work window is the grace window, or the batch's
//! [`declared_need`](crate::run_contract::declared_need) when every item
//! declared a timeout and that is longer, capped at the batch ceiling. Parking
//! (durably suspending the turn and answering with a marker) is the fallback,
//! used only when the work outlives that window: an undeclared batch past
//! grace, a declared one past its need, or a host that cannot hold the call.
//! Whatever window applies, the host answers before the socket closes: it
//! keeps [`ANSWER_RESERVE`] after the work window to park the call and send
//! the reply, so its answer bound is the work window plus that reserve.
//!
//! The sender states how long it listens in
//! [`CallbackRequest::answer_bound_ms`], and the host answers within
//! [`effective_answer_bound`]. `cairn-cmd` states the host's own bound, which
//! its socket was sized from. The SDK states its own timeout, or that it
//! listens as long as the host takes; it has no tool call to park on, so a
//! long batch it runs is waited for rather than parked. A request without the
//! field (an older client, or one relayed by an older executor that dropped
//! it) gets [`LEGACY_RUN_ANSWER_BOUND`], which fits inside every bound such a
//! sender ever applied.
//!
//! The ordering is enforced at compile time by the `const` assertions below
//! and again, per request shape, by this module's tests.
use std::time::Duration;

use crate::executor_protocol::RunnerCallback;
use crate::protocol::CallbackRequest;
use crate::run_contract::{
    declared_need_of_payload, is_control_flow_batch, RUN_BATCH_CEILING_MS, RUN_GRACE_WINDOW_MS,
};
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

/// The `run` grace window: the shortest work window any `run` call gets.
const RUN_GRACE: Duration = Duration::from_millis(RUN_GRACE_WINDOW_MS);

/// The longest work window any `run` call gets: the batch ceiling.
const RUN_CEILING: Duration = Duration::from_millis(RUN_BATCH_CEILING_MS as u64);

/// The room the host keeps after a `run` call's work window: past the work
/// deadline it still has to park the call durably (a store-lock bracket and
/// one row) and send the reply, and both must land before the sender gives up.
pub const ANSWER_RESERVE: Duration = Duration::from_secs(20);

/// The host's answer bound for a `run` request that did not state its own: the
/// grace window plus the reserve, ignoring any declared need. Every sender
/// built before senders stated a bound waited at least the grace window plus
/// [`CALLBACK_TIMEOUT_MARGIN`], so this always answers while it still listens.
pub const LEGACY_RUN_ANSWER_BOUND: Duration = add(RUN_GRACE, ANSWER_RESERVE);

/// The longest a `run` call is ever answered within: a batch that runs to the
/// ceiling, plus the reserve. A stated bound above it is held to it.
pub const LONGEST_RUN_ANSWER_BOUND: Duration = add(RUN_CEILING, ANSWER_RESERVE);

/// The bound Cairn hands every agent CLI for one MCP tool call, hard and idle
/// alike: Claude's `MCP_TOOL_TIMEOUT` and `CLAUDE_CODE_MCP_TOOL_IDLE_TIMEOUT`,
/// and Codex's per-server `tool_timeout_sec`. Seven days is an effective
/// "never" that still ends a wedged socket, so Cairn's own bounds, not the
/// client's, decide when a call ends. Every socket must stay below it so the
/// agent never abandons cairn-cmd mid-await.
pub const AGENT_MCP_TOOL_TIMEOUT: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The most text, in characters, Cairn has every agent CLI carry inline from
/// one `run` result: Claude's per-tool `anthropic/maxResultSizeChars` and
/// Codex's `tool_output_token_limit`. Cairn's own composer, not the client,
/// decides what a run result shows. It keeps command output well under this,
/// and a model door's reply is data that must arrive whole. Without it Claude
/// moves any result past about 50,000 characters to a file the agent cannot
/// open, and Codex cuts every tool output to 10,000 tokens. 500,000 is the most
/// Claude accepts for the annotation, so a reply longer than this still leaves
/// an agent's conversation, and nothing on Cairn's side can lift that.
pub const AGENT_TOOL_RESULT_CHAR_CEILING: usize = 500_000;

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
    // The longest `run` socket (a batch that declared the whole ceiling) stays
    // below the agent's own tool timeout.
    assert!(above(
        AGENT_MCP_TOOL_TIMEOUT,
        add(
            add(add(RUN_CEILING, ANSWER_RESERVE), CALLBACK_TIMEOUT_MARGIN),
            RELAY_HOP_MARGIN
        )
    ));
    // The legacy bound answers while an older relay (grace + margin) and an
    // older socket (that + the hop margin) still listen.
    assert!(above(
        add(RUN_GRACE, CALLBACK_TIMEOUT_MARGIN),
        LEGACY_RUN_ANSWER_BOUND
    ));
    // Every relay bound outlives its host bound, and every socket its relay.
    assert!(above(
        add(LEGACY_RUN_ANSWER_BOUND, CALLBACK_TIMEOUT_MARGIN),
        LEGACY_RUN_ANSWER_BOUND
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
        add(add(RUN_CEILING, ANSWER_RESERVE), CALLBACK_TIMEOUT_MARGIN)
    ));
    assert!(above(
        AGENT_MCP_TOOL_TIMEOUT,
        add(
            add(UNBOUNDED_CALLBACK_TIMEOUT, CALLBACK_TIMEOUT_MARGIN),
            RELAY_HOP_MARGIN
        )
    ));
    // The host's inline waits fit inside the shortest request it can arrive on.
    assert!(above(RUN_GRACE, INLINE_DURABLE_WAIT_BUDGET));
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

/// How long the host may spend on a `run` call's work before it must park it:
/// the grace window, or the batch's declared need when every item declared a
/// timeout and that is longer, capped at the batch ceiling. A `waitFor` or a
/// workflow target is control flow that answers or suspends at once, so it
/// gets grace whatever it declares.
pub fn run_work_window(payload: &serde_json::Value) -> Duration {
    if is_control_flow_batch(payload) {
        return RUN_GRACE;
    }
    declared_need_of_payload(payload)
        .map_or(RUN_GRACE, |need| need.max(RUN_GRACE))
        .min(RUN_CEILING)
}

/// The host's answer bound for a request. `run` answers within its work window
/// plus [`ANSWER_RESERVE`] (see the module doc); a blocking tasks/questions
/// append awaits an unbounded external event; everything else answers within
/// the store-lock wait plus the inline write-check wait.
///
/// Only a batch whose items ALL declared a timeout moves the `run` bound. An
/// item with no timeout runs to completion, which no socket should be sized
/// for, so such a batch parks past grace and resumes durably instead.
pub fn host_answer_bound(request: &CallbackRequest) -> HostAnswerBound {
    match request.tool.as_str() {
        "run" => HostAnswerBound::Bounded(add(run_work_window(&request.payload), ANSWER_RESERVE)),
        "write" if change_has_blocking_append(&request.payload) => HostAnswerBound::Unbounded,
        _ => HostAnswerBound::Bounded(DEFAULT_HOST_BOUND),
    }
}

/// The bound the host actually answers a request within.
///
/// A `run` sender's stated [`CallbackRequest::answer_bound_ms`] is believed up
/// to [`LONGEST_RUN_ANSWER_BOUND`]: it is how long that sender listens, and
/// every relay it passed waits at least as long ([`relay_timeout`]). The host
/// still parks a call it can park once its work window ends; the extra time
/// only lets a call it cannot park (an SDK call, which has no tool call of its
/// own) wait for its batch instead of being cancelled. A `run` request that
/// stated nothing gets [`LEGACY_RUN_ANSWER_BOUND`], since its sender may wait
/// no longer than that. Any other verb is never answered later than
/// [`host_answer_bound`].
pub fn effective_answer_bound(request: &CallbackRequest) -> HostAnswerBound {
    let own = host_answer_bound(request);
    match (request.answer_bound_ms.map(Duration::from_millis), own) {
        (Some(stated), _) if request.tool == "run" => {
            HostAnswerBound::Bounded(stated.min(LONGEST_RUN_ANSWER_BOUND))
        }
        (Some(stated), HostAnswerBound::Bounded(bound)) => {
            HostAnswerBound::Bounded(bound.min(stated))
        }
        (Some(stated), HostAnswerBound::Unbounded) => {
            HostAnswerBound::Bounded(stated.min(UNBOUNDED_CALLBACK_TIMEOUT))
        }
        (None, _) if request.tool == "run" => HostAnswerBound::Bounded(LEGACY_RUN_ANSWER_BOUND),
        (None, own) => own,
    }
}

/// The bound `cairn-cmd` states in [`CallbackRequest::answer_bound_ms`]: the
/// host's own bound for the request, which its socket was sized from, or
/// `None` for an unbounded one (whose socket is the unbounded callback
/// timeout, and which states nothing).
pub fn stated_answer_bound_ms(request: &CallbackRequest) -> Option<u64> {
    match host_answer_bound(request) {
        HostAnswerBound::Bounded(bound) => Some(bound.as_millis() as u64),
        HostAnswerBound::Unbounded => None,
    }
}

/// The executor relay's bound for a request forwarded from inside a cell: the
/// host's own bound, or the longer bound a `run` sender stated (so an SDK
/// call inside a cell is waited for as long as it listens), plus the margin.
/// The relay never answers a request before the host must have.
pub fn relay_timeout(request: &CallbackRequest) -> Duration {
    match (host_answer_bound(request), effective_answer_bound(request)) {
        (HostAnswerBound::Bounded(own), HostAnswerBound::Bounded(effective)) => {
            add(own.max(effective), CALLBACK_TIMEOUT_MARGIN)
        }
        _ => UNBOUNDED_CALLBACK_TIMEOUT,
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

    fn callback_request(tool: &str, payload: serde_json::Value) -> CallbackRequest {
        CallbackRequest {
            cwd: "/tmp".to_string(),
            tool: tool.to_string(),
            payload,
            ..Default::default()
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
            callback_request(
                "run",
                serde_json::json!({ "commands": [{ "command": "x", "timeout": 600_000 }] }),
            ),
            callback_request(
                "run",
                serde_json::json!({ "commands": [{ "command": "x", "timeout": 99_000_000u64 }] }),
            ),
            callback_request(
                "run",
                serde_json::json!({ "commands": [{ "waitFor": { "duration": "3m" } }] }),
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
            reported_denial: None,
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

    /// Tenet 1297: a `run` call stays open for its declared item timeouts. A
    /// batch whose items ALL declared one moves the bound to its need plus the
    /// reserve; a batch with any undeclared item stays at grace plus the
    /// reserve and parks past grace.
    #[test]
    fn a_fully_declared_run_holds_the_socket_for_its_need() {
        let ten_minutes = Duration::from_secs(600);
        let bound =
            |payload: serde_json::Value| host_answer_bound(&callback_request("run", payload));
        assert_eq!(
            bound(serde_json::json!({
                "commands": [{ "command": "cargo test", "timeout": 600_000 }]
            })),
            HostAnswerBound::Bounded(ten_minutes + ANSWER_RESERVE)
        );
        assert_eq!(
            bound(serde_json::json!({
                "commands": [
                    { "command": "a", "timeout": 300_000 },
                    { "command": "b", "timeout": 300_000 }
                ],
                "sequential": true
            })),
            HostAnswerBound::Bounded(ten_minutes + ANSWER_RESERVE)
        );
        // Partly declared: the undeclared item runs to completion, so the
        // batch keeps the grace window.
        assert_eq!(
            bound(serde_json::json!({
                "commands": [
                    { "command": "a", "timeout": 600_000 },
                    { "command": "b" }
                ]
            })),
            HostAnswerBound::Bounded(LEGACY_RUN_ANSWER_BOUND)
        );
        // A short declaration never shrinks the window below grace.
        assert_eq!(
            bound(serde_json::json!({ "commands": [{ "command": "a", "timeout": 1_000 }] })),
            HostAnswerBound::Bounded(LEGACY_RUN_ANSWER_BOUND)
        );
        // Control flow answers or suspends at once, whatever it declares.
        assert_eq!(
            bound(serde_json::json!({
                "commands": [{ "target": "cairn://workflows/review", "timeout": 600_000 }]
            })),
            HostAnswerBound::Bounded(LEGACY_RUN_ANSWER_BOUND)
        );
        assert_eq!(
            relay_timeout(&callback_request("run", serde_json::json!({}))),
            LEGACY_RUN_ANSWER_BOUND + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn the_ceiling_run_socket_stays_below_the_agent_mcp_tool_timeout() {
        let request = callback_request(
            "run",
            serde_json::json!({ "commands": [{ "command": "x", "timeout": 99_000_000u64 }] }),
        );
        assert_eq!(
            host_answer_bound(&request),
            HostAnswerBound::Bounded(RUN_CEILING + ANSWER_RESERVE)
        );
        assert!(socket_timeout(&request) < AGENT_MCP_TOOL_TIMEOUT);
    }

    /// A request that states no bound came from a sender that may wait only
    /// the old grace-sized relay (180 s) or socket (195 s), so the host answers
    /// within grace plus the reserve, ignoring the declared need.
    #[test]
    fn an_unstated_run_bound_falls_back_to_the_legacy_bound() {
        let mut request = callback_request(
            "run",
            serde_json::json!({ "commands": [{ "command": "x", "timeout": 600_000 }] }),
        );
        assert_eq!(
            effective_answer_bound(&request),
            HostAnswerBound::Bounded(Duration::from_secs(140))
        );
        assert!(LEGACY_RUN_ANSWER_BOUND < RUN_GRACE + CALLBACK_TIMEOUT_MARGIN);

        request.answer_bound_ms = stated_answer_bound_ms(&request);
        assert_eq!(
            effective_answer_bound(&request),
            HostAnswerBound::Bounded(Duration::from_secs(600) + ANSWER_RESERVE)
        );
        // A sender that states less than the host would allow is believed.
        request.answer_bound_ms = Some(150_000);
        assert_eq!(
            effective_answer_bound(&request),
            HostAnswerBound::Bounded(Duration::from_secs(150))
        );
        // Other verbs are never answered later than the host's own bound.
        let mut read = callback_request("read_batch", serde_json::json!({ "paths": ["file:x"] }));
        assert_eq!(effective_answer_bound(&read), host_answer_bound(&read));
        read.answer_bound_ms = Some(u64::MAX);
        assert_eq!(effective_answer_bound(&read), host_answer_bound(&read));
    }

    /// The SDK has no tool call to park on, so the host waits for a batch it
    /// runs for as long as the SDK listens, which the SDK states. A stated
    /// bound longer than the host's own is believed up to the ceiling, even
    /// for an undeclared batch, and every relay it crosses waits that long.
    #[test]
    fn a_run_sender_that_listens_longer_is_waited_for_up_to_the_ceiling() {
        let mut request = callback_request(
            "run",
            serde_json::json!({ "commands": [{ "command": "x" }] }),
        );
        request.answer_bound_ms = Some(3_600_000);
        assert_eq!(
            effective_answer_bound(&request),
            HostAnswerBound::Bounded(Duration::from_secs(3_600))
        );
        assert_eq!(
            relay_timeout(&request),
            Duration::from_secs(3_600) + CALLBACK_TIMEOUT_MARGIN
        );
        request.answer_bound_ms = Some(u64::MAX);
        assert_eq!(
            effective_answer_bound(&request),
            HostAnswerBound::Bounded(LONGEST_RUN_ANSWER_BOUND)
        );
        assert!(socket_timeout(&request) < AGENT_MCP_TOOL_TIMEOUT);
        // cairn-cmd states the host's own bound, so its relay is unchanged.
        request.answer_bound_ms = stated_answer_bound_ms(&request);
        assert_eq!(
            relay_timeout(&request),
            LEGACY_RUN_ANSWER_BOUND + CALLBACK_TIMEOUT_MARGIN
        );
    }

    /// An executor built before the field existed deserializes the request
    /// into its own `CallbackRequest` and re-serializes it to forward, which
    /// drops the field, so the host falls back to the legacy bound.
    #[test]
    fn an_older_relay_drops_the_stated_bound_on_the_way_through() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct OlderCallbackRequest {
            cwd: String,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            run_id: Option<String>,
            tool: String,
            payload: serde_json::Value,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            tool_use_id: Option<String>,
            #[serde(default, skip_serializing_if = "Option::is_none")]
            thread_id: Option<String>,
        }
        let mut request = callback_request(
            "run",
            serde_json::json!({ "commands": [{ "command": "x", "timeout": 600_000 }] }),
        );
        request.answer_bound_ms = stated_answer_bound_ms(&request);
        let wire = serde_json::to_string(&request).unwrap();
        assert!(wire.contains("answer_bound_ms"));
        let older: OlderCallbackRequest = serde_json::from_str(&wire).unwrap();
        let forwarded = serde_json::to_string(&older).unwrap();
        let arrived: CallbackRequest = serde_json::from_str(&forwarded).unwrap();
        assert_eq!(arrived.answer_bound_ms, None);
        assert_eq!(
            effective_answer_bound(&arrived),
            HostAnswerBound::Bounded(LEGACY_RUN_ANSWER_BOUND)
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
