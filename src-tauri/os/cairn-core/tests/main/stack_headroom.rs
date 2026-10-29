//! A served tool call fits in half a default thread stack.
//!
//! In an unoptimized build an async poll frame costs about twice the sum of
//! every child future it awaits, and a tool call crosses a dozen such frames
//! between `dispatch_tool` and its handler. When that chain outgrew the 2 MiB a
//! libtest thread and a tokio worker get, the overflow surfaced as an abort in
//! whichever unrelated test happened to run deepest, on whichever merge tipped
//! it over, and read as flake. These tests run one served call on a thread with
//! half that stack, so lost headroom fails here, by name, while there is still
//! room to find what grew. `cairn_platform::stack` describes the remedy.
//!
//! Both calls enter through `dispatch_tool`, the one function every
//! authenticated tool call passes through, so everything from the ownership
//! fence down to the resource handler is inside the budget.

use crate::common;

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;
use serde_json::{json, Value};
use tempfile::TempDir;

/// Half of the 2 MiB that std gives a spawned thread and tokio gives a worker.
const BUDGET: usize = 1024 * 1024;

/// Drive `make`'s future to completion on a current-thread runtime, pinned on
/// the stack of a thread that has only [`BUDGET`] bytes: the shape
/// `#[tokio::test]` gives a test body, with less room. An overflow aborts the
/// process with a message naming this thread.
fn within_budget<F: Future<Output = ()> + 'static>(make: impl FnOnce() -> F + Send + 'static) {
    let joined = std::thread::Builder::new()
        .name(format!("served tool call within a {BUDGET}-byte stack"))
        .stack_size(BUDGET)
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let body = std::pin::pin!(make());
            runtime.block_on(body);
        })
        .unwrap()
        .join();
    if let Err(panic) = joined {
        std::panic::resume_unwind(panic);
    }
}

async fn issue_fixture() -> (TempDir, Arc<LocalDb>, Orchestrator) {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    let project_id = common::create_project(&db, "stk").await;
    db.execute(
        "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
         VALUES ('issue-1', ?1, 1, 'headroom probe', 'active', 1, 1)",
        params![project_id.as_str()],
    )
    .await
    .unwrap();
    let orch = common::orchestrator(&temp, db.clone());
    (temp, db, orch)
}

async fn dispatch(orch: &Orchestrator, tool: &str, payload: Value) -> String {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: tool.to_string(),
        payload,
        tool_use_id: None,
    };
    let cursors = Mutex::new(HashMap::new());
    dispatch_tool(orch, &request, &cursors)
        .await
        .into_inner()
        .content
}

#[test]
fn a_served_resource_write_fits_in_half_a_default_stack() {
    within_budget(|| async {
        let (_temp, db, orch) = issue_fixture().await;
        let result = dispatch(
            &orch,
            "write",
            json!({ "changes": [{
                "target": "cairn://p/stk/1",
                "mode": "append",
                "payload": { "content": "a comment only a served write could make" },
            }]}),
        )
        .await;
        assert_eq!(
            common::query_i64(&db, "SELECT COUNT(*) FROM comments")
                .await
                .unwrap(),
            1,
            "the write was not served: {result}"
        );
    });
}

#[test]
fn a_served_resource_read_fits_in_half_a_default_stack() {
    within_budget(|| async {
        let (_temp, _db, orch) = issue_fixture().await;
        let result = dispatch(
            &orch,
            "read_issue_resource",
            json!({ "uri": "cairn://p/stk/1" }),
        )
        .await;
        assert!(
            result.contains("headroom probe"),
            "the read was not served: {result}"
        );
    });
}
