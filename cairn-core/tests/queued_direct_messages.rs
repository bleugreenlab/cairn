//! CAIRN-1196: queued mid-turn direct message delivery.
//!
//! These tests pin the storage and dispatch-augmentation halves of the
//! queue-on-active behavior. The active-turn message handler path is covered
//! indirectly through the dispatch test below — if the recipient has a
//! running head turn, the message must remain queued (delivered_at IS NULL)
//! until a prompt boundary fires.

mod common;

use std::collections::HashMap;
use std::sync::Mutex;

use cairn_core::internal::dispatch::dispatch_tool;
use cairn_core::internal::mcp::handlers::files::handle_change;
use cairn_core::internal::mcp::handlers::issue_resources::handle_read_issue_resource;
use cairn_core::internal::mcp::handlers::messages::append_direct_message;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::wakes::{
    self, WakeDelivery, WakeEvent, WakeRouteAction, WakeScope, WakeSource, WakeSubscriptionState,
};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::messages::{db as msg_db, queued::DeliveryUrgency};
use cairn_core::models::ChannelType;
use turso::params;

fn insert_pending_direct(orch: &Orchestrator, recipient: &str, content: &str) -> String {
    let msg = msg_db::insert_message(
        &orch.db.local,
        &ChannelType::Direct,
        None,
        Some("sender-run"),
        "planner",
        Some(recipient),
        content,
    )
    .unwrap();
    assert_eq!(
        msg.delivered_at, None,
        "new direct should start with delivered_at IS NULL"
    );
    msg.id
}

fn callback_request(tool: &str, run_id: Option<&str>) -> McpCallbackRequest {
    McpCallbackRequest {
        cwd: "/tmp".to_string(),
        run_id: run_id.map(str::to_string),
        tool: tool.to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_tool_augments_result_with_queued_direct_messages() {
    let (_temp, orch) = common::test_orchestrator().await;
    let recipient_run = "recipient-run";
    let _ = insert_pending_direct(&orch, recipient_run, "hello mid-turn");

    let request = callback_request("bogus_tool", Some(recipient_run));
    let cursors = Mutex::new(HashMap::new());
    let result = dispatch_tool(&orch, &request, &cursors).await;

    assert!(
        !result.content.contains("<system-reminder>"),
        "augmentation must keep reminders out of handler content: {}",
        result.content
    );
    assert!(
        result
            .reminders
            .iter()
            .any(|r| r.contains("[Direct message from planner] hello mid-turn")),
        "augmentation should collect the rendered DM as a reminder: {:?}",
        result.reminders
    );

    // Message is now marked delivered — a second dispatch does not re-deliver.
    let again = dispatch_tool(&orch, &request, &cursors).await;
    assert!(
        again.reminders.is_empty(),
        "claim_pending_directs_for_run should be idempotent across dispatch calls: {:?}",
        again.reminders
    );
}

/// Insert the minimum DB shape `append_direct_message` needs to resolve a
/// direct-message target: a project with the given key, an issue with the
/// given number, an execution at seq=1, a top-level job at uri_segment
/// `builder`, and a run whose head turn sits in `turn_state` (`pending` or
/// `running` to exercise the queue path).
async fn insert_dm_recipient(db: &LocalDb, project_key: &str, turn_state: &str) {
    let project_key = project_key.to_string();
    let turn_state = turn_state.to_string();
    db.write(|conn| {
        let project_key = project_key.clone();
        let turn_state = turn_state.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('proj-1', 'default', 'Test Project', ?1, '/tmp/test-repo', 1, 1)",
                params![project_key.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                 VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 'session-1', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, project_id, job_id, chat_id, status, session_id, backend, created_at, updated_at, start_mode)
                 VALUES ('run-1', 'proj-1', 'job-1', NULL, 'live', 'session-1', 'codex', 1, 1, 'resume')",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, ?1, 1, 1)",
                params![turn_state.as_str()],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// COUNT(*) FROM turns for the recipient job. The queue path must not create
/// a new turn — only the pre-existing one (sequence 1) should be there after
/// `append_direct_message` returns.
async fn turn_count(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM turns WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.expect("count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

/// Find the most recently inserted direct message addressed to the recipient.
async fn latest_direct_to(db: &LocalDb, recipient_run_id: &str) -> Option<(String, Option<i64>)> {
    let recipient_run_id = recipient_run_id.to_string();
    db.read(|conn| {
        let recipient_run_id = recipient_run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, delivered_at FROM messages
                     WHERE channel_type = 'direct' AND recipient_run_id = ?1
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                    params![recipient_run_id.as_str()],
                )
                .await?;
            rows.next()
                .await?
                .map(|row| {
                    use cairn_core::internal::storage::RowExt;
                    Ok::<_, cairn_core::internal::storage::DbError>((row.text(0)?, row.opt_i64(1)?))
                })
                .transpose()
        })
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn append_direct_message_queues_when_head_turn_is_running() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_dm_recipient(&orch.db.local, "PROJ", "running").await;

    let before_turns = turn_count(&orch.db.local, "job-1").await;

    // External sender (run_id = None) — keeps the test out of the
    // sender-side lookups while still exercising the recipient-side queue.
    let request = callback_request("write", None);

    let result = append_direct_message(
        &orch, &request, "PROJ", 42, 1, "builder", None, "ping", false,
    )
    .await
    .unwrap();

    assert!(
        result.starts_with("Queued for delivery to builder"),
        "queue path should return a 'Queued for delivery' string, got: {result}"
    );

    // No new turn created (no continue_job_impl on the active path).
    assert_eq!(
        turn_count(&orch.db.local, "job-1").await,
        before_turns,
        "queue path must not create a successor turn"
    );

    // Message persisted with delivered_at = NULL so the next prompt-boundary
    // injection path (hook additionalContext or dispatch augmentation) can
    // claim it.
    let (_, delivered_at) = latest_direct_to(&orch.db.local, "run-1")
        .await
        .expect("queued message should be inserted");
    assert_eq!(
        delivered_at, None,
        "queue path must leave delivered_at NULL until an injection path claims it"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn append_direct_message_queues_when_head_turn_is_pending() {
    // Pending counts as mid-turn for the active-turn predicate — the same
    // active-turn guard inside the turn-creation paths refuses both pending
    // and running, so the queue path must treat them the same.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_dm_recipient(&orch.db.local, "PEND", "pending").await;

    let request = callback_request("write", None);

    let result = append_direct_message(
        &orch, &request, "PEND", 42, 1, "builder", None, "ping", false,
    )
    .await
    .unwrap();
    assert!(
        result.starts_with("Queued for delivery to builder"),
        "{}",
        result
    );

    let (_, delivered_at) = latest_direct_to(&orch.db.local, "run-1")
        .await
        .expect("queued message should be inserted");
    assert_eq!(delivered_at, None);
}

/// Seed a top-level builder job and a `review` sub-task nested under it.
/// Both jobs have runs whose head turn is `running`, so DMs go through the
/// queue path rather than continue_job_impl (which would need a real worktree
/// and process). Returns nothing — fixed test IDs make assertions readable.
async fn insert_node_and_subtask(db: &LocalDb, project_key: &str) {
    let project_key = project_key.to_string();
    db.write(|conn| {
        let project_key = project_key.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('proj-1', 'default', 'Test Project', ?1, '/tmp/test-repo', 1, 1)",
                params![project_key.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                 VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                (),
            )
            .await?;
            // Top-level builder job.
            conn.execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
                 VALUES ('job-builder', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 'session-builder', 1, 1)",
                (),
            )
            .await?;
            // Sub-task `review` nested under builder.
            conn.execute(
                "INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
                 VALUES ('job-review', 'exec-1', 'job-builder', 'issue-1', 'proj-1', 'review', 'review', 'running', 'session-review', 1, 1)",
                (),
            )
            .await?;
            for session in ["session-builder", "session-review"] {
                let owner = if session == "session-builder" { "job-builder" } else { "job-review" };
                conn.execute(
                    "INSERT INTO sessions (id, job_id, status, created_at, updated_at)
                     VALUES (?1, ?2, 'active', 1, 1)",
                    params![session, owner],
                )
                .await?;
            }
            // issue_id on runs is required so lookup_run_by_id returns a
            // non-null issue_number — the sender-name path branches on that.
            for (run_id, job_id, session_id) in [
                ("run-builder", "job-builder", "session-builder"),
                ("run-review", "job-review", "session-review"),
            ] {
                conn.execute(
                    "INSERT INTO runs (id, project_id, job_id, issue_id, chat_id, status, session_id, backend, created_at, updated_at, start_mode)
                     VALUES (?1, 'proj-1', ?2, 'issue-1', NULL, 'live', ?3, 'codex', 1, 1, 'resume')",
                    params![run_id, job_id, session_id],
                )
                .await?;
            }
            for (turn_id, session_id, run_id, job_id) in [
                ("turn-builder", "session-builder", "run-builder", "job-builder"),
                ("turn-review", "session-review", "run-review", "job-review"),
            ] {
                conn.execute(
                    "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, ?4, 1, 'running', 1, 1)",
                    params![turn_id, session_id, run_id, job_id],
                )
                .await?;
            }
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-builder' WHERE id = 'job-builder'",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id = 'turn-review' WHERE id = 'job-review'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn append_direct_message_resolves_subtask_addressed_as_node_slash_task() {
    // CAIRN-1200: the canonical address for a sub-task is
    // cairn://p/PROJ/N/SEQ/<parent>/task/<segment>. parse_message_target
    // turns that into (node_name=<parent>, task_name=Some(<segment>)).
    // Before the fix, the bug was upstream (the URI advertised in the
    // prompt's Active Peers list was wrong), not in this resolver — so
    // this is the regression pin that the sub-task arm of find_recipient_job
    // continues to resolve the right job when the caller addresses the
    // canonical URI.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_node_and_subtask(&orch.db.local, "PROJ").await;

    let request = callback_request("write", None);

    let result = append_direct_message(
        &orch,
        &request,
        "PROJ",
        42,
        1,
        "builder",
        Some("review"),
        "please re-check",
        false,
    )
    .await
    .expect("sub-task DM by canonical URI must resolve");

    assert!(
        result.starts_with("Queued for delivery to builder/review"),
        "sub-task DM should queue against the review job, got: {result}"
    );

    // The message was persisted addressed to the review run, not the builder run.
    let (_, delivered_at) = latest_direct_to(&orch.db.local, "run-review")
        .await
        .expect("sub-task DM should be inserted with recipient_run_id=run-review");
    assert_eq!(
        delivered_at, None,
        "queue path leaves delivered_at NULL until an injection path claims it"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn append_direct_message_subtask_addressed_as_top_level_surfaces_canonical_uri() {
    // The producer bug that prompted this issue: an agent reads a peer
    // listing or a lifecycle channel message that names the sub-task with
    // the wrong URI shape — cairn://p/PROJ/42/1/review instead of
    // cairn://p/PROJ/42/1/builder/task/review — and DMs that broken URI.
    // The resolver should still correctly reject it (no top-level `review`
    // node exists) but the error must now echo the addressed URI verbatim
    // and explain which scope was searched, so the bug is one-glance
    // diagnosable instead of a debug round trip.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_node_and_subtask(&orch.db.local, "PROJ").await;

    let request = callback_request("write", None);

    let err = append_direct_message(
        &orch, &request, "PROJ", 42, 1,
        "review", // wrong: review is a sub-task, not a top-level node
        None, "hello", false,
    )
    .await
    .expect_err("top-level lookup of a sub-task uri_segment must miss");

    assert!(
        err.contains("cairn://p/PROJ/42/1/review"),
        "error must echo the addressed URI verbatim, got: {err}"
    );
    assert!(
        err.contains("top-level node with uri_segment 'review'"),
        "error must explain which scope was searched, got: {err}"
    );
    assert!(
        !err.contains("No agent found: PROJ/42"),
        "old broken format must not reappear, got: {err}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn append_direct_message_subtask_with_wrong_parent_names_parent_in_error() {
    // Bonus from the fix: a sub-task lookup that misses because the parent
    // is wrong should mention the parent so the caller can fix the URI.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_node_and_subtask(&orch.db.local, "PROJ").await;

    let request = callback_request("write", None);

    let err = append_direct_message(
        &orch,
        &request,
        "PROJ",
        42,
        1,
        "planner", // wrong parent
        Some("review"),
        "hello",
        false,
    )
    .await
    .expect_err("sub-task under a non-existent parent must miss");

    assert!(
        err.contains("cairn://p/PROJ/42/1/planner/task/review"),
        "error must echo the addressed sub-task URI, got: {err}"
    );
    assert!(
        err.contains("sub-task with uri_segment 'review' under parent 'planner'"),
        "error must name both segment and parent so the URI typo is visible, got: {err}"
    );
}

/// Fetch the sender_name stamped on the most-recent direct message addressed
/// to the given recipient. Used to pin that sub-task senders advertise their
/// canonical URI rather than the broken top-level shape.
async fn latest_direct_sender(db: &LocalDb, recipient_run_id: &str) -> Option<String> {
    let recipient_run_id = recipient_run_id.to_string();
    db.read(|conn| {
        let recipient_run_id = recipient_run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT sender_name FROM messages
                     WHERE channel_type = 'direct' AND recipient_run_id = ?1
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                    params![recipient_run_id.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| row.text(0)).transpose()
        })
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn sender_name_for_subtask_sender_is_canonical_task_uri() {
    // CAIRN-1200 follow-up to PR review Finding 1: sub-task senders feed
    // their identity through sender_name_for_run, which stamps every
    // outbound message's sender_name and surfaces in the reply-to hint a
    // DM recipient receives. Before the fix this was the broken top-level
    // shape cairn://p/PROJ/42/1/review — every reply to a sub-task then
    // hit `No agent found` because the addressed URI was unreachable.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_node_and_subtask(&orch.db.local, "PROJ").await;

    // Sender is the review sub-task; recipient is the builder. Both jobs
    // exist and both runs have `issue_id` so lookup_run resolves the
    // sender's RunContext with issue_number populated.
    let request = callback_request("write", Some("run-review"));

    let result = append_direct_message(
        &orch, &request, "PROJ", 42, 1, "builder", None, "ack", false,
    )
    .await
    .expect("sub-task should be able to DM its sibling builder");
    assert!(
        result.starts_with("Queued for delivery to builder"),
        "DM should queue (recipient turn is running), got: {result}"
    );

    let sender_name = latest_direct_sender(&orch.db.local, "run-builder")
        .await
        .expect("DM should be inserted with sender_name set");
    assert_eq!(
        sender_name, "cairn://p/PROJ/42/1/builder/task/review",
        "sub-task sender_name must be the canonical /task/ URI, not the broken cairn://p/PROJ/42/1/review (which the reply-to hint would echo and fail to resolve)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn sender_name_for_top_level_sender_unchanged() {
    // Regression pin alongside the sub-task fix: top-level node senders
    // continue to record cairn://p/PROJ/N/SEQ/<segment>.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_node_and_subtask(&orch.db.local, "PROJ").await;

    let request = callback_request("write", Some("run-builder"));

    let _ = append_direct_message(
        &orch,
        &request,
        "PROJ",
        42,
        1,
        "builder",
        Some("review"),
        "ack back",
        false,
    )
    .await
    .expect("builder DM to review sub-task should resolve and queue");

    let sender_name = latest_direct_sender(&orch.db.local, "run-review")
        .await
        .expect("DM should be inserted with sender_name set");
    assert_eq!(
        sender_name, "cairn://p/PROJ/42/1/builder",
        "top-level sender_name shape must not regress"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dispatch_tool_no_augmentation_without_run_id() {
    let (_temp, orch) = common::test_orchestrator().await;
    // No run_id on the request — e.g. external CLI caller. We can't claim DMs
    // without a recipient run, so the result is returned verbatim.
    let request = callback_request("bogus_tool", None);
    let cursors = Mutex::new(HashMap::new());
    let result = dispatch_tool(&orch, &request, &cursors).await;
    assert!(
        result.reminders.is_empty(),
        "no run_id means no augmentation: {:?}",
        result.reminders
    );
}

#[tokio::test(flavor = "current_thread")]
async fn flush_pending_directs_on_idle_is_noop_without_pending() {
    // CAIRN-1297: the end-of-turn flush must not resume a healthy idle run when
    // nothing is queued for it — guards against spurious wakes / resume loops.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_dm_recipient(&orch.db.local, "PROJ", "running").await;

    let before = turn_count(&orch.db.local, "job-1").await;
    cairn_core::messages::delivery::flush_pending_directs_on_idle(&orch, "run-1");
    assert_eq!(
        turn_count(&orch.db.local, "job-1").await,
        before,
        "no pending directs/notices -> no resume -> no successor turn"
    );
}

/// Seed a recipient job with **no** `current_session_id` (and no session row),
/// so `continue_job_impl` errors out ("Job has no current session to resume")
/// before it ever tries to spawn an agent. Lets a test drive the flush's resume
/// path to its failure branch without a live process.
async fn insert_sessionless_recipient(db: &LocalDb, project_key: &str) {
    let project_key = project_key.to_string();
    db.write(|conn| {
        let project_key = project_key.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('proj-1', 'default', 'Test Project', ?1, '/tmp/test-repo', 1, 1)",
                params![project_key.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                 VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                (),
            )
            .await?;
            // No current_session_id -> continue_job_impl bails before spawn.
            conn.execute(
                "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                 VALUES ('run-1', 'proj-1', 'job-1', 'live', 1, 1)",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn node_messages_append_routes_like_bare_node() {
    // CAIRN-1329: appending to the canonical `.../NODE/messages` resource must
    // deliver identically to the bare-node append path. Recipient turn is running,
    // so the message queues (delivered_at IS NULL) just like the bare-node path.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_dm_recipient(&orch.db.local, "PROJ", "running").await;

    let request = McpCallbackRequest {
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "write".to_string(),
        payload: serde_json::json!({
            "changes": [{
                "target": "cairn://p/PROJ/42/1/builder/messages",
                "mode": "append",
                "payload": { "content": "ping via /messages" }
            }]
        }),
        tool_use_id: None,
    };

    let result = handle_change(&orch, &request).await;
    assert!(
        result.contains("builder")
            && !result.to_lowercase().contains("invalid")
            && !result.contains("Unsupported"),
        "append to /messages should route to direct delivery, got: {result}"
    );

    // The direct message was persisted against the builder run, pending.
    let (_, delivered_at) = latest_direct_to(&orch.db.local, "run-1")
        .await
        .expect("append to /messages should insert a direct message");
    assert_eq!(
        delivered_at, None,
        "queued direct from /messages must leave delivered_at NULL"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn node_messages_read_returns_directs() {
    // CAIRN-1329: read/append symmetry — reading `.../NODE/messages` returns the
    // node's direct-message stream.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_dm_recipient(&orch.db.local, "PROJ", "running").await;
    let _ = insert_pending_direct(&orch, "run-1", "hello builder");

    let request = McpCallbackRequest {
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "read".to_string(),
        payload: serde_json::json!({ "uri": "cairn://p/PROJ/42/1/builder/messages" }),
        tool_use_id: None,
    };

    let result = handle_read_issue_resource(&orch, &request).await;
    assert!(
        result.contains("hello builder"),
        "node messages read should include the direct message content, got: {result}"
    );
    assert!(
        result.contains("planner"),
        "node messages read should show the sender, got: {result}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_message_resume_failure_keeps_suppressed_digest_and_mute_pending() {
    let (_temp, orch) = common::test_orchestrator().await;
    insert_sessionless_recipient(&orch.db.local, "PROJ").await;

    let child_source = WakeSource::Issue {
        reference: "cairn://p/PROJ/99".to_string(),
    };
    let muted = wakes::mute_scope(
        &orch.db.local,
        "job-1",
        &WakeScope::new(child_source.clone(), None),
        None,
        "agent",
    )
    .await
    .expect("mute should create the scoped wake subscription");

    let action = wakes::route_wake(
        &orch,
        WakeEvent {
            source: child_source,
            fact_kind: "pr_state_change".to_string(),
            detail_uri: Some("cairn://p/PROJ/99/1/pr".to_string()),
            delivery: WakeDelivery::Targeted {
                subscriber_job_id: "job-1".to_string(),
                message: "child PR changed".to_string(),
            },
            urgency: DeliveryUrgency::Queue,
        },
    )
    .await
    .expect("muted child wake should route through the digest");
    assert_eq!(action, WakeRouteAction::Suppressed);
    assert_eq!(
        wakes::peek_pending_suppressed_for_job(&orch.db.local, "job-1")
            .await
            .unwrap()
            .len(),
        1
    );

    let request = callback_request("write", None);
    let result = append_direct_message(
        &orch,
        &request,
        "PROJ",
        42,
        1,
        "builder",
        None,
        "live user re-engagement",
        false,
    )
    .await
    .expect("direct message should queue after resume failure");
    assert!(
        result.starts_with("Queued for delivery to builder"),
        "resume failure should leave the direct queued, got: {result}"
    );

    let (_, delivered_at) = latest_direct_to(&orch.db.local, "run-1")
        .await
        .expect("direct message should still be persisted");
    assert_eq!(
        delivered_at, None,
        "failed direct resume must not stamp the live message delivered"
    );
    assert_eq!(
        wakes::peek_pending_suppressed_for_job(&orch.db.local, "job-1")
            .await
            .unwrap()
            .len(),
        1,
        "failed direct resume must not claim the rendered digest"
    );
    let subscription = wakes::list_subscriptions_for_job(&orch.db.local, "job-1")
        .await
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.id == muted.id)
        .expect("muted subscription should still exist");
    assert_eq!(
        subscription.state,
        WakeSubscriptionState::Muted,
        "failed direct resume must not lift the muted child subscription"
    );

    let claimed = wakes::claim_pending_suppressed_for_job_with_live_source(
        &orch.db.local,
        "job-1",
        Some(&WakeSource::User),
    )
    .await
    .unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "a later successful live wake can still claim it"
    );
    let lifted = wakes::list_subscriptions_for_job(&orch.db.local, "job-1")
        .await
        .unwrap()
        .into_iter()
        .find(|candidate| candidate.id == muted.id)
        .unwrap();
    assert_eq!(lifted.state, WakeSubscriptionState::Active);
}

#[tokio::test(flavor = "current_thread")]
async fn flush_pending_directs_on_idle_keeps_direct_pending_when_resume_fails() {
    // CAIRN-1297: peek-then-resume-then-stamp. When continue_job_impl fails (here
    // because the job has no session to resume), the flush must NOT stamp the
    // direct delivered, so a later prompt boundary can still pick it up. This
    // drives flush_pending_directs_on_idle through the real continue_job_impl
    // call to its Err branch.
    let (_temp, orch) = common::test_orchestrator().await;
    insert_sessionless_recipient(&orch.db.local, "PROJ").await;
    let id = insert_pending_direct(&orch, "run-1", "child is blocked on a question");

    let before = turn_count(&orch.db.local, "job-1").await;
    cairn_core::messages::delivery::flush_pending_directs_on_idle(&orch, "run-1");

    // Resume failed -> no successor turn created.
    assert_eq!(
        turn_count(&orch.db.local, "job-1").await,
        before,
        "a failed resume must not create a successor turn"
    );

    // ...and the direct is NOT stamped -> still claimable at the next boundary.
    let still_pending = msg_db::claim_pending_directs_for_run(&orch.db.local, "run-1").unwrap();
    assert_eq!(
        still_pending.len(),
        1,
        "a failed resume must leave the direct pending (not stamped)"
    );
    assert_eq!(still_pending[0].id, id);
}
