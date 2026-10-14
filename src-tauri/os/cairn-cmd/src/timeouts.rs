//! HTTP callback timeout derivation for the cairn-cmd verbs. Every outer
//! ceiling is sized strictly above the host budget it wraps.
use std::time::Duration;

use cairn_common::protocol::CallbackRequest;
use cairn_common::uri::{parse_uri as parse_cairn_uri, CairnResource};

// ---------------------------------------------------------------------------
// cairn-cmd -> host HTTP callback timeout
//
// The host owns execution. Every outer layer's ceiling is derived to sit
// strictly *above* the layer below it, so the HTTP socket never fires before
// the host's own timeout returns a (partial) result. See
// `mcp/handlers/run.rs` for the host per-item budget this mirrors.
// ---------------------------------------------------------------------------

/// The host's grace window for a `run` batch. A batch that settles inside it
/// returns synchronously; past it the host returns a suspend marker and the
/// agent resumes durably. Either way the host answers within this window, so an
/// item's own `timeout` no longer bears on how long the socket stays open.
///
/// It is the shared constant rather than a mirror of one: the socket sizing here
/// and the host's own window are the same fact, and a mirror is a place for them
/// to drift.
const HOST_RUN_GRACE_MS: u64 = cairn_common::run_contract::RUN_GRACE_WINDOW_MS;

/// Margin added above the host's grace window so the HTTP socket always
/// outlives the host's own answer and the host's result wins the race.
const CALLBACK_TIMEOUT_MARGIN: Duration = Duration::from_secs(60);

/// HTTP callback ceiling for a `run` batch. It is a constant, not a function of
/// the batch: an item's `timeout` decides when that item is killed, never when
/// the host answers. Deriving it per item is what used to let a no-timeout
/// command fail the agent at a 180-second socket while the host ran on for ten
/// minutes.
const RUN_CALLBACK_TIMEOUT: Duration =
    Duration::from_millis(HOST_RUN_GRACE_MS + CALLBACK_TIMEOUT_MARGIN.as_millis() as u64);

/// The longest bounded host wait beneath this module's default ceiling: how long
/// a file-target `write` may queue on the project store lock behind another
/// writer. Like [`HOST_RUN_GRACE_MS`] it is the shared constant rather than a
/// mirror of one.
const HOST_WRITE_STORE_LOCK_WAIT_MS: u64 = cairn_common::write_contract::WRITE_STORE_LOCK_WAIT_MS;

/// HTTP callback ceiling for verbs whose host work is bounded and short: a
/// `read`, a non-blocking `write`, a resource read, or a `watch` long-poll (the
/// host returns its `pending` sentinel at 290s, comfortably under this).
///
/// Derived, not stated. This was an independent 600s that happened to EQUAL the
/// host's store-lock wait rather than sit above it — the one arrangement this
/// module's doctrine forbids. A write that queued on the lock behind a base
/// advance could have its socket fire while the host went on to land the commit,
/// handing the agent a transport error for a write that had in fact succeeded,
/// whose natural next move is to re-issue an already-applied batch (CAIRN-3264).
const DEFAULT_CALLBACK_TIMEOUT: Duration = Duration::from_millis(
    HOST_WRITE_STORE_LOCK_WAIT_MS + CALLBACK_TIMEOUT_MARGIN.as_millis() as u64,
);

/// HTTP callback ceiling for verbs that block on an unbounded external event
/// with no host-side timeout below them: a blocking `write` append to a node's
/// tasks/questions collection (a sub-agent task or user question that may
/// legitimately run far longer than any `run` batch). The host owns these
/// awaits; the socket must not undercut
/// them. Six days is effectively "no ceiling" while still bounding a wedged
/// socket, and sits strictly below the spawned agent's MCP tool timeout
/// (`MCP_TOOL_TIMEOUT` / Codex `tool_timeout_sec`, set to 7 days) so the agent
/// never abandons cairn-cmd mid-await.
const UNBOUNDED_CALLBACK_TIMEOUT: Duration = Duration::from_secs(6 * 24 * 60 * 60);

/// True if a `write` payload contains a blocking append to a node's
/// tasks/questions collection — an await with no host-side timeout below it
/// (the host blocks until the sub-agent task completes or the user answers).
fn change_has_blocking_append(payload: &serde_json::Value) -> bool {
    let Some(changes) = payload.get("changes").and_then(|c| c.as_array()) else {
        return false;
    };
    changes.iter().any(|item| {
        let is_append = item.get("mode").and_then(|m| m.as_str()) == Some("append");
        is_append
            && item
                .get("target")
                .and_then(|t| t.as_str())
                .and_then(parse_cairn_uri)
                .is_some_and(|r| {
                    matches!(
                        r,
                        CairnResource::NodeTasks { .. } | CairnResource::NodeQuestions { .. }
                    )
                })
    })
}

/// HTTP callback timeout for a request. The host owns execution; this ceiling
/// must sit strictly above whatever the host can legally take so the HTTP layer
/// never undercuts the host's own timeout. `run` always answers within its grace
/// window; blocking `write` appends await an unbounded external event; everything
/// else uses the short default.
pub(crate) fn callback_timeout(request: &CallbackRequest) -> Duration {
    match request.tool.as_str() {
        "run" => RUN_CALLBACK_TIMEOUT,
        "write" if change_has_blocking_append(&request.payload) => UNBOUNDED_CALLBACK_TIMEOUT,
        _ => DEFAULT_CALLBACK_TIMEOUT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- callback timeout derivation -------------------------------------

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

    /// The socket ceiling is a property of the host's grace window, not of the
    /// batch. An item's `timeout` decides when that item is killed; it must not
    /// move when the host answers, or a long item fails the agent at the socket
    /// while the host is still working.
    #[test]
    fn callback_timeout_for_run_is_constant_across_item_timeouts() {
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
            serde_json::json!({
                "commands": [{ "command": "a" }, { "command": "b" }]
            }),
        ];
        for payload in batches {
            assert_eq!(
                callback_timeout(&callback_request("run", payload.clone())),
                RUN_CALLBACK_TIMEOUT,
                "item timeouts must not move the socket ceiling: {payload}"
            );
        }
        assert_eq!(
            RUN_CALLBACK_TIMEOUT,
            Duration::from_millis(HOST_RUN_GRACE_MS) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn callback_timeout_blocking_write_append_is_unbounded() {
        for collection in ["tasks", "questions"] {
            let payload = serde_json::json!({
                "changes": [{
                    "target": format!("cairn://p/CAIRN/1621/1/builder/{collection}"),
                    "mode": "append",
                    "payload": { "prompt": "do a thing" }
                }]
            });
            let request = callback_request("write", payload);
            assert_eq!(
                callback_timeout(&request),
                UNBOUNDED_CALLBACK_TIMEOUT,
                "{collection} append should not be undercut"
            );
        }
    }

    #[test]
    fn callback_timeout_plain_write_uses_default() {
        // A file edit is bounded host work: the short default applies.
        let payload = serde_json::json!({
            "changes": [{ "target": "file:src/lib.rs", "mode": "create", "payload": { "content": "x" } }]
        });
        let request = callback_request("write", payload);
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }

    #[test]
    fn callback_timeout_non_append_to_tasks_uses_default() {
        // Only `mode=append` to tasks/questions blocks; a read/patch does not.
        let payload = serde_json::json!({
            "changes": [{ "target": "cairn://p/CAIRN/1621/1/builder/tasks", "mode": "patch", "payload": {} }]
        });
        let request = callback_request("write", payload);
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }

    /// The property the whole module exists to hold, for the one ceiling that
    /// did not hold it. Equality is not enough: the socket and the host budget
    /// must be ordered, or a wait that runs its full length races the socket
    /// that wraps it and the loser is whichever fires first.
    #[test]
    fn the_default_ceiling_sits_strictly_above_the_host_wait_it_wraps() {
        assert!(
            DEFAULT_CALLBACK_TIMEOUT > Duration::from_millis(HOST_WRITE_STORE_LOCK_WAIT_MS),
            "the callback socket must outlive the store-lock wait beneath it, \
             or a write can fail at the socket while the host goes on to land it: \
             ceiling {DEFAULT_CALLBACK_TIMEOUT:?} vs wait {HOST_WRITE_STORE_LOCK_WAIT_MS}ms"
        );
        assert_eq!(
            DEFAULT_CALLBACK_TIMEOUT,
            Duration::from_millis(HOST_WRITE_STORE_LOCK_WAIT_MS) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    /// Every ceiling this module hands out is ordered above the host budget it
    /// wraps, so a socket never decides an answer the host was still computing.
    #[test]
    fn every_ceiling_sits_above_its_host_budget() {
        assert!(RUN_CALLBACK_TIMEOUT > Duration::from_millis(HOST_RUN_GRACE_MS));
        assert!(DEFAULT_CALLBACK_TIMEOUT > Duration::from_millis(HOST_WRITE_STORE_LOCK_WAIT_MS));
        // A blocking append awaits an unbounded external event with no host
        // timeout below it, so its ceiling must outrun every bounded one.
        assert!(UNBOUNDED_CALLBACK_TIMEOUT > DEFAULT_CALLBACK_TIMEOUT);
        assert!(UNBOUNDED_CALLBACK_TIMEOUT > RUN_CALLBACK_TIMEOUT);
    }

    #[test]
    fn callback_timeout_read_uses_default() {
        let request = callback_request("read_batch", serde_json::json!({ "paths": ["file:x"] }));
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }
}
