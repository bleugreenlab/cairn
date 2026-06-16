//! CAIRN-1230 / CAIRN-1271: turn-scoped content-aware dedup at the MCP dispatch
//! boundary.
//!
//! These tests drive [`dispatch_tool`] with a real [`Orchestrator`] and a run
//! registered with an active turn, exercising the read-family flail dedup: a
//! repeated identical call whose content is *unchanged* short-circuits to a
//! stub, while a repeat whose underlying content *changed* re-fetches fresh.
//! Mutations, different slices, turn boundaries, and untracked runs are never
//! deduped.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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

fn read_request(run_id: &str, path: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "read".to_string(),
        payload: serde_json::json!({ "path": path }),
        tool_use_id: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_dedups_identical_read_within_turn() {
    let (_temp, orch, cursors) = active_turn_fixture().await;
    let request = read_request("run-1", "file:/tmp/cairn-dedup-nonexistent-xyz");

    // First call executes the handler (not a stub).
    let first = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(
        !first.starts_with("[duplicate call]"),
        "first call must execute, got stub: {first}"
    );

    // Identical second call short-circuits to the stub.
    let second = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(
        second.starts_with("[duplicate call]"),
        "duplicate read must return the stub, got: {second}"
    );
    assert!(
        second.contains("call #2"),
        "stub should name the count: {second}"
    );
    assert!(
        second.contains("file:/tmp/cairn-dedup-nonexistent-xyz"),
        "stub should name the target: {second}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_does_not_dedup_different_slice() {
    let (_temp, orch, cursors) = active_turn_fixture().await;

    let base = read_request("run-1", "file:/tmp/cairn-dedup-nonexistent?offset=0");
    let other = read_request("run-1", "file:/tmp/cairn-dedup-nonexistent?offset=100");

    let first = dispatch_tool(&orch, &base, &cursors).await.content;
    assert!(!first.starts_with("[duplicate call]"));
    // A different slice is a different call -> executes, never stubbed.
    let second = dispatch_tool(&orch, &other, &cursors).await.content;
    assert!(
        !second.starts_with("[duplicate call]"),
        "a different slice must not be deduped, got: {second}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_never_dedups_write() {
    let (_temp, orch, cursors) = active_turn_fixture().await;

    let request = McpCallbackRequest {
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({ "changes": [] }),
        tool_use_id: None,
    };

    let first = dispatch_tool(&orch, &request, &cursors).await.content;
    let second = dispatch_tool(&orch, &request, &cursors).await.content;
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

    let first = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(!first.starts_with("[duplicate call]"));
    assert!(
        first.contains("before"),
        "first read returns content: {first}"
    );

    // Identical call, file unchanged -> duplicate stub.
    let unchanged = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(
        unchanged.starts_with("[duplicate call]"),
        "unchanged content must dedup: {unchanged}"
    );

    // Mutate the file; the same call must now re-fetch the fresh content.
    std::fs::write(&path, "after").unwrap();
    let changed = dispatch_tool(&orch, &request, &cursors).await.content;
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
        cwd: "/tmp".to_string(),
        run_id: Some("run-1".to_string()),
        tool: "read_resource".to_string(),
        payload: serde_json::json!({ "uri": "cairn://p/CAIRN/42/1/builder/terminal/dev-server" }),
        tool_use_id: None,
    };

    let first = dispatch_tool(&orch, &request, &cursors).await.content;
    let second = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(!first.starts_with("[duplicate call]"));
    assert!(
        !second.starts_with("[duplicate call]"),
        "read_resource is the terminal cursor-poll path and must never be deduped: {second}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_passes_through_read_without_active_turn() {
    let (_temp, orch) = common::test_orchestrator().await;
    // Run is registered but Idle (no turn begun) -> PassThrough, never stubbed.
    register_run(&orch, "run-1");
    let cursors = Mutex::new(HashMap::new());
    let request = read_request("run-1", "file:/tmp/cairn-dedup-nonexistent");

    let first = dispatch_tool(&orch, &request, &cursors).await.content;
    let second = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(!first.starts_with("[duplicate call]"));
    assert!(
        !second.starts_with("[duplicate call]"),
        "an Idle run (no active turn) must never be deduped: {second}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_resets_dedup_on_turn_change() {
    let (_temp, orch, cursors) = active_turn_fixture().await;
    let request = read_request("run-1", "file:/tmp/cairn-dedup-nonexistent");

    let _first = dispatch_tool(&orch, &request, &cursors).await.content;
    let dup = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(
        dup.starts_with("[duplicate call]"),
        "second call should stub: {dup}"
    );

    // A new turn wipes the seen-set: the same read executes again.
    orch.process_state.begin_turn("run-1", "turn-2");
    let after = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(
        !after.starts_with("[duplicate call]"),
        "a new turn must reset the seen-set: {after}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_nudges_on_third_identical_read() {
    let (_temp, orch, cursors) = active_turn_fixture().await;
    let request = read_request("run-1", "file:/tmp/cairn-dedup-nonexistent");

    let _first = dispatch_tool(&orch, &request, &cursors).await.content;
    let second = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(second.contains("call #2"));
    assert!(
        !second.contains("retry loop"),
        "no nudge below threshold: {second}"
    );

    let third = dispatch_tool(&orch, &request, &cursors).await.content;
    assert!(third.contains("call #3"));
    assert!(
        third.contains("retry loop"),
        "nudge should appear on the third identical call: {third}"
    );
}
