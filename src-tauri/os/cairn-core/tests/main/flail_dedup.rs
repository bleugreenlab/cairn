//! CAIRN-1230 / CAIRN-1271: turn-scoped content-aware dedup at the MCP dispatch
//! boundary.
//!
//! These tests drive [`dispatch_tool`] with a real [`Orchestrator`] and a run
//! registered with an active turn, exercising the read-family flail dedup: a
//! repeated identical call whose content is *unchanged* short-circuits to a
//! stub, while a repeat whose underlying content *changed* re-fetches fresh.
//! Mutations, different slices, turn boundaries, and untracked runs are never
//! deduped.

use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_common::protocol::{stamp_callback_origin, MODEL_FACING_ORIGIN};
use cairn_core::internal::agent_process::process::RunHandle;
use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;

/// Register a run and put it into a `ServingTurn` so dedup has a turn id to
/// scope against. Mirrors the in-app path where a turn begins on stream parse
/// before any tool call dispatches.
fn register_run(orch: &Orchestrator, run_id: &str) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let child = Arc::new(Mutex::new(None));
    let stdin = Arc::new(Mutex::new(None));
    let handle = RunHandle::new(child, stdin, Some(format!("sess-{run_id}")), None);
    processes.register(run_id.to_string(), handle);
}

fn register_run_with_turn(orch: &Orchestrator, run_id: &str, turn_id: &str) {
    register_run(orch, run_id);
    orch.process_state.begin_turn(run_id, turn_id);
}

async fn active_turn_fixture() -> (
    tempfile::TempDir,
    Orchestrator,
    Mutex<HashMap<String, usize>>,
) {
    let (temp, orch) = common::test_orchestrator().await;
    register_run_with_turn(&orch, "run-1", "turn-1");
    (temp, orch, Mutex::new(HashMap::new()))
}

/// A read as the agent's own `read` tool sends it.
fn read_request(run_id: &str, path: &str) -> McpCallbackRequest {
    let mut payload = serde_json::json!({ "path": path });
    stamp_callback_origin(&mut payload, MODEL_FACING_ORIGIN);
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "read".to_string(),
        payload,
        tool_use_id: None,
        origin: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_never_dedups_write() {
    let (_temp, orch, cursors) = active_turn_fixture().await;

    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({ "changes": [] }),
        tool_use_id: None,
        origin: None,
    };

    let first = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    let second = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    assert!(!first.starts_with("[duplicate call]"));
    assert!(
        !second.starts_with("[duplicate call]"),
        "write must always execute, never stub: {second}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_refetches_when_content_changed() {
    // The core CAIRN-1271 behavior: a repeated identical read whose underlying
    // content changed within the turn must return the fresh content, not the
    // stale duplicate stub. An unchanged repeat still dedups.
    let (_temp, orch, cursors) = active_turn_fixture().await;

    // A real file we can mutate between reads.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("content.txt");
    std::fs::write(&path, "before").unwrap();
    let request = read_request("run-1", &format!("file:{}", path.display()));

    let first = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    assert!(!first.starts_with("[duplicate call]"));
    assert!(
        first.contains("before"),
        "first read returns content: {first}"
    );

    // Identical call, file unchanged -> duplicate stub.
    let unchanged = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    assert!(
        unchanged.starts_with("[duplicate call]"),
        "unchanged content must dedup: {unchanged}"
    );

    // Mutate the file; the same call must now re-fetch the fresh content.
    std::fs::write(&path, "after").unwrap();
    let changed = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    assert!(
        !changed.starts_with("[duplicate call]"),
        "changed content must re-fetch, not stub: {changed}"
    );
    assert!(
        changed.contains("after"),
        "re-fetch must return the new content: {changed}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_never_dedups_read_resource_terminal_poll() {
    // read_resource is reserved for terminal output in the CLI/server routing,
    // so it is excluded from the read-family entirely. Repeated identical calls
    // still reach the handler (and observe new cursor output) rather than a stub.
    let (_temp, orch, cursors) = active_turn_fixture().await;
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "read_resource".to_string(),
        payload: serde_json::json!({ "uri": "cairn://p/cairn/42/1/builder/terminal/dev-server" }),
        tool_use_id: None,
        origin: None,
    };

    let first = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    let second = dispatch_tool(&orch, &request, &cursors)
        .await
        .into_inner()
        .content;
    assert!(!first.starts_with("[duplicate call]"));
    assert!(
        !second.starts_with("[duplicate call]"),
        "read_resource is the terminal cursor-poll path and must never be deduped: {second}"
    );
}
