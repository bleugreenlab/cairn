//! HTTP callback timeout derivation for the cairn-cmd verbs. Every outer
//! ceiling is sized strictly above the host budget it wraps.
use std::time::Duration;

use cairn_common::protocol::CallbackRequest;
use cairn_common::uri::{parse_uri as parse_cairn_uri, CairnResource};

use crate::schemas::{RunInput, RunItemInput};

// ---------------------------------------------------------------------------
// cairn-cmd -> host HTTP callback timeout
//
// The host owns execution. Every outer layer's ceiling is derived to sit
// strictly *above* the layer below it, so the HTTP socket never fires before
// the host's own timeout returns a (partial) result. See
// `mcp/handlers/run.rs` for the host per-item budget this mirrors.
// ---------------------------------------------------------------------------

/// Host per-item execution budget for `run` items, mirroring
/// `mcp/handlers/run.rs` (`timeout.unwrap_or(120_000).min(600_000)` ms). The
/// host is the only layer that returns partial output on expiry.
const HOST_ITEM_TIMEOUT_DEFAULT_MS: u64 = 120_000;
const HOST_ITEM_TIMEOUT_MAX_MS: u64 = 600_000;

/// Margin added above a `run` batch's host budget so the HTTP socket always
/// outlives the host's own per-item timeout and the host's (partial) result
/// wins the race.
const CALLBACK_TIMEOUT_MARGIN: Duration = Duration::from_secs(60);

/// HTTP callback ceiling for verbs whose host work is bounded and short: a
/// `read`, a non-blocking `write`, a resource read, or a `watch` long-poll (the
/// host returns its `pending` sentinel at 290s, comfortably under this).
const DEFAULT_CALLBACK_TIMEOUT: Duration = Duration::from_secs(600);

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

/// Effective host timeout for one `run` item, in milliseconds (mirrors
/// `run.rs`: caller value or the 120s default, clamped to the 600s ceiling).
fn effective_run_item_timeout_ms(item: &RunItemInput) -> u64 {
    item.timeout
        .map(u64::from)
        .unwrap_or(HOST_ITEM_TIMEOUT_DEFAULT_MS)
        .min(HOST_ITEM_TIMEOUT_MAX_MS)
}

/// HTTP callback timeout for a `run` batch. The host runs items sequentially
/// (sum of per-item budgets) or in parallel (max of per-item budgets), plus a
/// margin so the socket outlives the host's own per-item timeout. An empty
/// batch falls back to the single-item default.
fn run_callback_timeout(input: &RunInput) -> Duration {
    let sequential = input.sequential.unwrap_or(false);
    let budget_ms: u64 = if sequential {
        input
            .commands
            .iter()
            .map(effective_run_item_timeout_ms)
            .sum()
    } else {
        input
            .commands
            .iter()
            .map(effective_run_item_timeout_ms)
            .max()
            .unwrap_or(HOST_ITEM_TIMEOUT_DEFAULT_MS)
    };
    Duration::from_millis(budget_ms) + CALLBACK_TIMEOUT_MARGIN
}

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
/// never undercuts the host's own timeout. `run` derives its budget from the
/// batch; blocking `write` appends await an unbounded external event; everything
/// else uses the short default.
pub(crate) fn callback_timeout(request: &CallbackRequest) -> Duration {
    match request.tool.as_str() {
        "run" => serde_json::from_value::<RunInput>(request.payload.clone())
            .map(|input| run_callback_timeout(&input))
            .unwrap_or(DEFAULT_CALLBACK_TIMEOUT),
        "write" if change_has_blocking_append(&request.payload) => UNBOUNDED_CALLBACK_TIMEOUT,
        _ => DEFAULT_CALLBACK_TIMEOUT,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::run_input;

    // ---- callback timeout derivation -------------------------------------

    fn callback_request(tool: &str, payload: serde_json::Value) -> CallbackRequest {
        CallbackRequest {
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: tool.to_string(),
            payload,
            tool_use_id: None,
        }
    }

    #[test]
    fn effective_item_timeout_defaults_to_120s() {
        let input = run_input(serde_json::json!({ "commands": [{ "command": "echo hi" }] }));
        assert_eq!(
            effective_run_item_timeout_ms(&input.commands[0]),
            HOST_ITEM_TIMEOUT_DEFAULT_MS
        );
    }

    #[test]
    fn effective_item_timeout_clamps_to_host_ceiling() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "sleep 1", "timeout": 5_000_000u32 }]
        }));
        assert_eq!(
            effective_run_item_timeout_ms(&input.commands[0]),
            HOST_ITEM_TIMEOUT_MAX_MS
        );
    }

    #[test]
    fn run_timeout_sequential_sums_item_budgets_plus_margin() {
        // Two items at 300s each, run in order: host budget is the sum.
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "sleep 150", "timeout": 300_000u32 },
                { "command": "sleep 150", "timeout": 300_000u32 }
            ],
            "sequential": true
        }));
        assert_eq!(
            run_callback_timeout(&input),
            Duration::from_millis(600_000) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn run_timeout_parallel_takes_max_item_budget_plus_margin() {
        // Default (parallel): host runs concurrently, so the budget is the max.
        let input = run_input(serde_json::json!({
            "commands": [
                { "command": "sleep 100", "timeout": 200_000u32 },
                { "command": "sleep 300", "timeout": 500_000u32 }
            ]
        }));
        assert_eq!(
            run_callback_timeout(&input),
            Duration::from_millis(500_000) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn run_timeout_uses_defaults_when_items_omit_timeout() {
        let input = run_input(serde_json::json!({
            "commands": [{ "command": "a" }, { "command": "b" }],
            "sequential": true
        }));
        assert_eq!(
            run_callback_timeout(&input),
            Duration::from_millis(2 * HOST_ITEM_TIMEOUT_DEFAULT_MS) + CALLBACK_TIMEOUT_MARGIN
        );
    }

    #[test]
    fn callback_timeout_run_derives_from_batch() {
        let payload = serde_json::json!({
            "commands": [
                { "command": "sleep 150", "timeout": 300_000u32 },
                { "command": "sleep 150", "timeout": 300_000u32 }
            ],
            "sequential": true
        });
        let request = callback_request("run", payload);
        assert_eq!(
            callback_timeout(&request),
            Duration::from_millis(600_000) + CALLBACK_TIMEOUT_MARGIN
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

    #[test]
    fn callback_timeout_read_uses_default() {
        let request = callback_request("read_batch", serde_json::json!({ "paths": ["file:x"] }));
        assert_eq!(callback_timeout(&request), DEFAULT_CALLBACK_TIMEOUT);
    }
}
