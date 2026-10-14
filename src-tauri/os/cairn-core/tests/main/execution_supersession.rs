//! One canonical execution lineage per issue (CAIRN-4665).
//!
//! On cairn/4658 an execution failed, an operator started a successor twenty-one
//! seconds later, and twenty minutes after that a delayed turn-end checks push
//! woke the dead execution's builder. Two agents then corrected the same compile
//! failure on two near-identical branches until a human chose one.
//!
//! Every test here follows the discipline `run_ownership_fence.rs` establishes
//! for this class of fence: **assert the absence of the effect, not the presence
//! of a refusal.** A fence that refuses politely and then acts anyway passes a
//! text-only assertion while leaving the defect exactly where it was. The
//! effects counted here are the ones the incident actually produced -- a run
//! row, a turn row, an issue comment, a transcript event.

use crate::common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::execution::canonical::{self, Fence, SupersessionActor};
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

const COMMENT: &str = "a comment only an unfenced write could have made";

/// The cairn/4658 shape: one issue, execution 1 with a builder, a review node
/// and a delegated sub-task, all live. Execution 2 does not exist yet -- the
/// tests that need it create it the way the system does, through the phase-1
/// writer, so what they exercise is the real transaction rather than a
/// hand-arranged imitation of its result.
async fn fixture() -> (tempfile::TempDir, Arc<LocalDb>, Orchestrator) {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    db.execute_script(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
           VALUES ('proj-1', 'default', 'Cairn', 'lineage', '/tmp/repo', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('issue-1', 'proj-1', 4658, 'Contended issue', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq,
                                 lifecycle)
           VALUES ('exec-1', 'build', 'issue-1', 'proj-1', 'running', 1, 1, 'canonical');
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name,
                           uri_segment, status, current_session_id, branch, created_at, updated_at)
           VALUES ('builder-1', 'exec-1', 'builder', 'issue-1', 'proj-1', 'Builder',
                   'builder', 'running', 'session-b1', 'agent/lineage-4658-builder-0', 1, 1);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name,
                           uri_segment, status, current_session_id, created_at, updated_at)
           VALUES ('review-1', 'exec-1', 'review', 'issue-1', 'proj-1', 'Review',
                   'review', 'pending', 'session-r1', 1, 1);
         -- A delegated sub-agent task carries its PARENT's execution_id, which is
         -- what lets the cascade reach descendants without recursion.
         INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, node_name,
                           uri_segment, status, current_session_id, created_at, updated_at)
           VALUES ('task-1', 'exec-1', 'builder-1', 'issue-1', 'proj-1', 'explore',
                   'explore', 'running', 'session-t1', 1, 1);
         INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
           VALUES ('session-b1', 'builder-1', 'claude', 'open', 1, 1, 1),
                  ('session-r1', 'review-1', 'claude', 'open', 1, 1, 1),
                  ('session-t1', 'task-1', 'claude', 'open', 1, 1, 1);
         INSERT INTO runs (id, project_id, issue_id, job_id, session_id, status, created_at,
                           updated_at)
           VALUES ('run-b1', 'proj-1', 'issue-1', 'builder-1', 'session-b1', 'live', 1, 1);
         INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                            created_at, updated_at)
           VALUES ('turn-b1', 'session-b1', 'run-b1', 'builder-1', 1, 'running', 'follow_up', 1, 1);
         UPDATE jobs SET current_turn_id = 'turn-b1' WHERE id = 'builder-1';",
    )
    .await
    .unwrap();
    let orch = common::orchestrator(&temp, db.clone());
    (temp, db, orch)
}

/// Start execution 2 the way the system does: one transaction that publishes the
/// fence, cascades suspension, and inserts the successor.
async fn start_successor(db: &LocalDb) -> Vec<canonical::Supersession> {
    db.write(|conn| {
        Box::pin(async move {
            let seq = canonical::next_seq_conn(conn, "issue-1").await?;
            let superseded = canonical::supersede_canonical_conn(
                conn,
                "issue-1",
                "exec-2",
                &SupersessionActor::operator_ui(),
                1000,
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at,
                                         seq, lifecycle)
                 VALUES ('exec-2', 'build', 'issue-1', 'proj-1', 'running', 1000, ?1,
                         'canonical')",
                params![seq],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id,
                                   node_name, uri_segment, status, current_session_id,
                                   created_at, updated_at)
                 VALUES ('builder-2', 'exec-2', 'builder', 'issue-1', 'proj-1', 'Builder',
                         'builder', 'running', 'session-b2', 1000, 1000)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('session-b2', 'builder-2', 'claude', 'open', 1, 1000, 1000)",
                (),
            )
            .await?;
            Ok(superseded)
        })
    })
    .await
    .unwrap()
}

async fn count(db: &LocalDb, sql: &'static str) -> i64 {
    common::query_i64(db, sql).await.unwrap()
}

fn commenting_write(run_id: &str) -> McpCallbackRequest {
    McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: Some(run_id.to_string()),
        tool: "write".to_string(),
        payload: serde_json::json!({
            "changes": [{
                "target": "cairn://p/lineage/4658",
                "mode": "append",
                "payload": { "content": COMMENT },
            }]
        }),
        tool_use_id: None,
    }
}

async fn dispatch(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let cursors = Mutex::new(HashMap::new());
    dispatch_tool(orch, request, &cursors)
        .await
        .into_inner()
        .content
}

// == Phase 1: what starting a successor does ==============================

/// Acceptance 2. One transaction fences the prior execution, cancels its
/// builder, its review node AND its delegated sub-task, closes their sessions,
/// terminalizes their live turns, and freezes exactly that job set for phase 2.
#[tokio::test]
async fn starting_a_successor_freezes_the_whole_prior_execution_in_one_transaction() {
    let (_temp, db, _orch) = fixture().await;

    let superseded = start_successor(&db).await;

    assert_eq!(
        superseded.len(),
        1,
        "exactly one prior execution was fenced"
    );
    let mut targets = superseded[0].stop_targets.clone();
    targets.sort();
    assert_eq!(
        targets,
        vec![
            "builder-1".to_string(),
            "review-1".to_string(),
            "task-1".to_string()
        ],
        "the frozen set must reach the delegated sub-task too: it carries its parent's \
         execution_id, which is what makes the cascade reach descendants"
    );

    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM jobs WHERE execution_id = 'exec-1' AND status <> 'cancelled'"
        )
        .await,
        0,
        "every job of the superseded execution is cancelled"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM sessions WHERE job_id IN ('builder-1','review-1','task-1') \
             AND status <> 'closed'"
        )
        .await,
        0,
        "their sessions are closed"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM turns WHERE job_id = 'builder-1' \
             AND state IN ('pending','running','yielded')"
        )
        .await,
        0,
        "the live turn is terminalized rather than left running"
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT end_reason FROM turns WHERE id = ?1", "turn-b1")
            .await
            .as_deref(),
        Some("superseded"),
        "the turn says WHY it ended"
    );
    assert_eq!(
        count(
            &db,
            "SELECT COUNT(*) FROM execution_supersessions WHERE reason = 'successor_created'"
        )
        .await,
        1,
        "one audit row records who stopped owning the attempt, when, and on whose authority"
    );
}

/// The invariant, structurally. Nothing can produce a second canonical execution
/// for one issue -- not a racing launch, not a hand-written repair.
#[tokio::test]
async fn a_second_canonical_execution_for_one_issue_is_rejected_by_the_store() {
    let (_temp, db, _orch) = fixture().await;

    let refused = db
        .execute(
            "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-rogue', 'build', 'issue-1', 'proj-1', 'running', 2, 2)",
            (),
        )
        .await;

    assert!(
        refused.is_err(),
        "a second canonical execution for one issue must be impossible, not merely unlikely"
    );
}

// == The fence ============================================================

/// Acceptance 3. The one place a resume is admitted refuses, and inserts no run.
#[tokio::test]
async fn a_superseded_builder_cannot_resume_and_inserts_no_run() {
    let (_temp, db, orch) = fixture().await;
    start_successor(&db).await;
    let runs_before = count(&db, "SELECT COUNT(*) FROM runs").await;

    let refusal = cairn_core::internal::execution::jobs::continue_job_impl(
        &orch,
        "builder-1",
        None,
        None,
        None,
    )
    .expect_err("a superseded builder must not take a turn");

    assert!(
        refusal.contains("no longer owns this issue's attempt"),
        "the refusal should name the reason: {refusal}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM runs").await,
        runs_before,
        "the refusal must cost no run row -- the absence of the effect is the whole point"
    );
}

/// Acceptance 4. Every file write, artifact publication and check trigger is a
/// tool call, so refusing at `dispatch_tool` refuses all of them.
#[tokio::test]
async fn a_superseded_runs_tool_call_is_refused_and_changes_nothing() {
    let (_temp, db, orch) = fixture().await;
    start_successor(&db).await;

    let result = dispatch(&orch, &commenting_write("run-b1")).await;

    assert!(
        result.contains("Stop here"),
        "expected the canonical refusal, got: {result}"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM comments").await,
        0,
        "a fenced write must not reach the handler"
    );
}

/// The guard that gives every refusal test above its teeth: the canonical
/// successor's own run is served and its effect lands. Without this, a fence
/// that refused everything would look correct.
#[tokio::test]
async fn the_canonical_successors_run_is_served_and_its_change_lands() {
    let (_temp, db, orch) = fixture().await;
    start_successor(&db).await;
    db.execute(
        "INSERT INTO runs (id, project_id, issue_id, job_id, session_id, status, started_at,
                           created_at, updated_at)
         VALUES ('run-b2', 'proj-1', 'issue-1', 'builder-2', 'session-b2', 'live', 2000, 2000,
                 2000)",
        (),
    )
    .await
    .unwrap();

    let result = dispatch(&orch, &commenting_write("run-b2")).await;

    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM comments").await,
        1,
        "the canonical execution must keep working: {result}"
    );
}

/// A run with no job, and a job with no execution, are the thread and chat
/// runs. Fencing them would have fenced the entire thread system on migration
/// day, so the predicate must answer Clear for both.
#[tokio::test]
async fn a_run_with_no_execution_is_never_fenced() {
    let (_temp, db, _orch) = fixture().await;
    db.execute_script(
        "INSERT INTO runs (id, status, created_at, updated_at) VALUES ('run-chat', 'live', 1, 1);
         INSERT INTO jobs (id, project_id, status, node_name, uri_segment, created_at, updated_at)
           VALUES ('thread-job', 'proj-1', 'running', 'thread', 'thread', 1, 1);
         INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-thread', 'proj-1', 'thread-job', 'live', 1, 1);",
    )
    .await
    .unwrap();

    for run_id in ["run-chat", "run-thread"] {
        assert_eq!(
            canonical::fence_for_run(&db, run_id).await.unwrap(),
            Fence::Clear,
            "{run_id} belongs to no issue attempt and must never be fenced"
        );
    }
    assert_eq!(
        canonical::fence_for_job(&db, "thread-job").await.unwrap(),
        Fence::Clear,
    );
}

/// Acceptance: an intentional parallel alternative runs freely and is never
/// mistaken for the issue's implementation.
#[tokio::test]
async fn a_declared_parallel_execution_is_never_fenced_by_a_successor() {
    let (_temp, db, _orch) = fixture().await;
    db.execute_script(
        "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq,
                                 lifecycle)
           VALUES ('exec-alt', 'build', 'issue-1', 'proj-1', 'running', 5, 9, 'parallel');
         INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment,
                           status, current_session_id, created_at, updated_at)
           VALUES ('builder-alt', 'exec-alt', 'issue-1', 'proj-1', 'Builder', 'builder',
                   'running', 'session-alt', 5, 5);",
    )
    .await
    .unwrap();

    start_successor(&db).await;

    assert_eq!(
        canonical::fence_for_job(&db, "builder-alt").await.unwrap(),
        Fence::Clear,
        "a declared alternative is not competing for the issue's attempt, so a new attempt \
         does not fence it"
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM jobs WHERE id = ?1", "builder-alt")
            .await
            .as_deref(),
        Some("running"),
        "and its work is not cancelled"
    );
}

/// Nobody chose an automatic trigger, so it has no authority to take an issue's
/// attempt away from work that is running right now.
///
/// The dangerous reading is that an execution carrying an `issue_id` is
/// therefore an attempt at that issue. It is not: a `job_ended` trigger, a skill
/// call, a webhook or a schedule firing on an active issue would then cancel its
/// builder, its review node and its delegated sub-tasks as a side effect, with
/// no operator having decided anything.
#[tokio::test]
async fn an_automatic_trigger_cannot_displace_live_canonical_work() {
    let (_temp, db, _orch) = fixture().await;

    let holds = db
        .read(|conn| {
            Box::pin(async move { canonical::issue_has_canonical_execution(conn, "issue-1").await })
        })
        .await
        .unwrap();

    assert!(
        holds,
        "precondition: execution 1 owns this issue's attempt and its builder is live"
    );

    // The predicate an automatic launch consults. Holding true here is what makes
    // it insert `parallel` and supersede nothing; the operator door is the only
    // one that fences.
    assert_eq!(
        canonical::fence_for_job(&db, "builder-1").await.unwrap(),
        Fence::Clear,
        "an automatic trigger must leave the running builder alone"
    );
    assert_eq!(
        common::scalar_text_by_id(&db, "SELECT status FROM jobs WHERE id = ?1", "builder-1")
            .await
            .as_deref(),
        Some("running"),
    );
}

/// The complement, so the rule above is not "automatic triggers never own
/// anything": on an issue nothing is attempting, an automatic trigger's
/// execution IS the attempt.
#[tokio::test]
async fn an_automatic_trigger_takes_the_attempt_when_nothing_holds_it() {
    let (_temp, db, _orch) = fixture().await;
    db.execute(
        "UPDATE executions SET lifecycle = 'superseded' WHERE id = 'exec-1'",
        (),
    )
    .await
    .unwrap();

    let holds = db
        .read(|conn| {
            Box::pin(async move { canonical::issue_has_canonical_execution(conn, "issue-1").await })
        })
        .await
        .unwrap();

    assert!(
        !holds,
        "with no canonical execution, an automatic trigger's launch becomes the attempt"
    );
}
