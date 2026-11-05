use crate::common;

use std::path::Path;
use std::sync::Arc;

use crate::common::{orchestrator, resource_orchestrator_fixture};
use cairn_core::internal::mcp::handlers::fence::{raise_fence, Crossing, FenceDecision};
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::models::Fence;
use cairn_db::turso::params;
use serde_json::json;

/// A fence-crossing `tool_input` an agent may answer. `descriptor` is the
/// session-grant key; `request` is the originating verb, embedded for resume, a
/// `read` so that re-dispatching it here is harmless.
fn crossing_tool_input() -> String {
    json!({
        "kind": "shell_path_crossing",
        "verb": "read",
        "descriptor": "/etc/hosts",
        "summary": "the sandbox refused a write to /etc/hosts",
        "request": {
            "cwd": "/wt",
            "run_id": "run-1",
            "tool": "read",
            "payload": { "path": "file:/etc/hosts" },
            "tool_use_id": "toolu-perm"
        }
    })
    .to_string()
}

/// Insert a node fixture with one pending fence permission request. The request
/// has no `turn_id`, so resolving it does not create a successor turn or resume
/// a (nonexistent) process — keeping the test to the resolution semantics.
async fn insert_permission_fixture(db: &LocalDb) {
    let project_id = common::create_project(db, "tpa").await;
    let tool_input = crossing_tool_input();
    db.write(|conn| {
        let project_id = project_id.clone();
        let tool_input = tool_input.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
                 VALUES ('issue-1', ?1, 1, 'Permission answer test', 'active', 'needs_input', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, '{}', 'manual')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'builder', 'running', 'session-1', 'builder', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-1', ?1, 'issue-1', 'job-1', 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
                 VALUES ('perm-row-1', 'run-1', 'job-1', 'toolu-perm', 'read', ?1, 'pending', 1, 'perm-1')",
                params![tool_input.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The node fixture plus a sub-agent task `write-scripts` beneath `builder`,
/// whose own run holds a pending crossing at `perm-1` — the same segment the
/// node's request carries, so an answer that resolved the wrong owner would
/// show up as the node's request changing instead.
async fn insert_task_permission_fixture(db: &LocalDb) {
    insert_permission_fixture(db).await;
    let tool_input = crossing_tool_input();
    db.write(|conn| {
        let tool_input = tool_input.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, parent_job_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 SELECT 'job-task', 'exec-1', 'issue-1', project_id, 'job-1', 'Build', 'running', 'session-task', 'write-scripts', 2, 2
                 FROM jobs WHERE id = 'job-1'",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('session-task', 'job-task', 'active', 2, 2)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 SELECT 'run-task', project_id, 'issue-1', 'job-task', 'live', 'session-task', 2, 2, 'resume'
                 FROM jobs WHERE id = 'job-1'",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
                 VALUES ('perm-row-task', 'run-task', 'job-task', 'toolu-task', 'read', ?1, 'pending', 2, 'perm-1')",
                params![tool_input.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// A thread `design` that has actually run (session job with no issue, the
/// reserved `thread` segment, no parent) with a task `probe` beneath its
/// session, whose run holds a pending crossing at `perm-1`.
async fn insert_thread_task_permission_fixture(db: &LocalDb) {
    let project_id = common::create_project(db, "tpt").await;
    let tool_input = crossing_tool_input();
    db.write(|conn| {
        let project_id = project_id.clone();
        let tool_input = tool_input.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
                 VALUES ('thread-1', ?1, 'design', 'active', 'needs_input', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, project_id, status, started_at)
                 VALUES ('host-exec-1', 'host-thread-job-1', ?1, 'running', 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, thread_id, project_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('thread-job-1', 'host-exec-1', 'thread-1', ?1, 'Thread', 'running', 'thread-session-1', 'thread', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('thread-session-1', 'thread-job-1', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-t1', ?1, 'thread-job-1', 'live', 'thread-session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('thread-task-job', 'host-exec-1', ?1, 'thread-job-1', 'Explore', 'running', 'thread-task-session', 'probe', 2, 2)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('thread-task-session', 'thread-task-job', 'active', 2, 2)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-probe', ?1, 'thread-task-job', 'live', 'thread-task-session', 2, 2, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
                 VALUES ('perm-row-thread-task', 'run-probe', 'thread-task-job', 'toolu-probe', 'read', ?1, 'pending', 2, 'perm-1')",
                params![tool_input.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

fn change_request(payload: serde_json::Value) -> McpCallbackRequest {
    change_request_as("run-1", payload)
}

fn change_request_as(run_id: &str, payload: serde_json::Value) -> McpCallbackRequest {
    McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: String::new(),
        run_id: Some(run_id.to_string()),
        tool: "write".to_string(),
        payload,
        tool_use_id: None,
        origin: None,
    }
}

async fn perm_status(db: &LocalDb) -> Option<String> {
    perm_status_of(db, "perm-row-1").await
}

async fn perm_status_of(db: &LocalDb, id: &str) -> Option<String> {
    let id = id.to_string();
    db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT status FROM permission_requests WHERE id = ?1",
                    params![id.as_str()],
                )
                .await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

async fn permission_request_count(db: &LocalDb) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT COUNT(*) FROM permission_requests", ())
                .await?;
            Ok(rows
                .next()
                .await?
                .and_then(|row| row.i64(0).ok())
                .unwrap_or(0))
        })
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn node_permission_patch_allow_session_resolves_and_grants() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let target = "cairn://p/tpa/1/1/builder/permissions/perm-1";
    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{"target": target, "mode": "patch", "payload": {"decision": "allow", "scope": "session"}}]
        })),
    )
    .await;
    assert!(
        result.contains("Answered permission perm-1"),
        "unexpected result: {result}"
    );
    assert_eq!(perm_status(&db).await.as_deref(), Some("allowed"));
    // scope:session records the crossing descriptor so an identical crossing
    // auto-allows without suspending again.
    assert!(
        orch.session_allowed_crossings
            .lock()
            .unwrap()
            .contains("/etc/hosts"),
        "session grant not recorded"
    );

    let duplicate = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{"target": target, "mode": "patch", "payload": {"decision": "allow", "scope": "session"}}]
        })),
    )
    .await;
    assert!(
        duplicate.contains("already answered"),
        "unexpected duplicate result: {duplicate}"
    );
}

#[tokio::test]
async fn node_permission_patch_deny_resolves() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let target = "cairn://p/tpa/1/1/builder/permissions/perm-1";
    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{"target": target, "mode": "patch", "payload": {"decision": "deny"}}]
        })),
    )
    .await;
    assert!(
        result.contains("Answered permission perm-1"),
        "unexpected result: {result}"
    );
    assert_eq!(perm_status(&db).await.as_deref(), Some("denied"));
    assert!(
        !orch
            .session_allowed_crossings
            .lock()
            .unwrap()
            .contains("/etc/hosts"),
        "deny must not record a grant"
    );
}

/// A sub-agent task's job has a parent, so it is not addressable as a node: its
/// permission must resolve through the parent node AND the task. Answering by
/// the task segment alone failed with `node tpa/1/1/write-scripts not found`,
/// leaving the task's batch blocked on a prompt nobody could answer.
#[tokio::test]
async fn task_permission_patch_allow_resolves_the_tasks_request() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_task_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{
                "target": "cairn://p/tpa/1/1/builder/task/write-scripts/permissions/perm-1",
                "mode": "patch",
                "payload": {"decision": "allow", "scope": "session"}
            }]
        })),
    )
    .await;
    assert!(
        result.contains("Answered permission perm-1"),
        "unexpected result: {result}"
    );
    assert_eq!(
        perm_status_of(&db, "perm-row-task").await.as_deref(),
        Some("allowed")
    );
    assert_eq!(
        perm_status(&db).await.as_deref(),
        Some("pending"),
        "answering the task's perm-1 must not touch the parent node's perm-1"
    );
}

#[tokio::test]
async fn task_permission_patch_deny_resolves_the_tasks_request() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_task_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{
                "target": "cairn://p/tpa/1/1/builder/task/write-scripts/permissions/perm-1",
                "mode": "patch",
                "payload": {"decision": "deny"}
            }]
        })),
    )
    .await;
    assert!(
        result.contains("Answered permission perm-1"),
        "unexpected result: {result}"
    );
    assert_eq!(
        perm_status_of(&db, "perm-row-task").await.as_deref(),
        Some("denied")
    );
    assert_eq!(perm_status(&db).await.as_deref(), Some("pending"));
}

/// The node address of a task permission is still the parent node: a task
/// name that is not beneath it names nothing, and says which task it looked
/// for rather than reporting a missing node.
#[tokio::test]
async fn task_permission_patch_for_an_unknown_task_names_the_task() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_task_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{
                "target": "cairn://p/tpa/1/1/builder/task/no-such-task/permissions/perm-1",
                "mode": "patch",
                "payload": {"decision": "deny"}
            }]
        })),
    )
    .await;
    assert!(
        result.contains("task tpa/1/1/builder/task/no-such-task not found"),
        "unexpected result: {result}"
    );
    assert_eq!(
        perm_status_of(&db, "perm-row-task").await.as_deref(),
        Some("pending")
    );
}

/// A thread-hosted task's permission URI normalizes onto the reserved
/// `(0, 0, thread)` coordinate; it resolves through the thread's session to the
/// task beneath it, exactly as an issue-hosted task resolves through its node.
#[tokio::test]
async fn thread_task_permission_patch_resolves_the_tasks_request() {
    for (decision, expected) in [("allow", "allowed"), ("deny", "denied")] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        insert_thread_task_permission_fixture(&db).await;
        let orch = orchestrator(&temp, db.clone());

        let result = handle_write(
            &orch,
            &change_request_as(
                "run-t1",
                json!({
                    "changes": [{
                        "target": "cairn://p/tpt/design/task/probe/permissions/perm-1",
                        "mode": "patch",
                        "payload": {"decision": decision}
                    }]
                }),
            ),
        )
        .await;
        assert!(
            result.contains("Answered permission perm-1"),
            "{decision}: unexpected result: {result}"
        );
        assert_eq!(
            perm_status_of(&db, "perm-row-thread-task").await.as_deref(),
            Some(expected),
            "{decision}"
        );
    }
}

#[tokio::test]
async fn case_colliding_project_keys_make_permission_identity_ambiguous() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_permission_fixture(&db).await;
    common::create_project(&db, "TPA").await;
    let orch = orchestrator(&temp, db.clone());

    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{
                "target": "cairn://p/tpa/1/1/builder/permissions/perm-1",
                "mode": "patch",
                "payload": {"decision": "deny"}
            }]
        })),
    )
    .await;

    assert!(
        result.contains("project key tpa is ambiguous"),
        "expected project identity ambiguity, got: {result}"
    );
    assert_eq!(
        perm_status(&db).await.as_deref(),
        Some("pending"),
        "an ambiguous project identity must not answer either project's permission"
    );
}

#[tokio::test]
async fn pending_permission_read_shows_answer_action() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db);

    let output = handle_read_file(
        &orch,
        &McpCallbackRequest {
            answer_bound_ms: None,
            thread_id: None,
            cwd: String::new(),
            run_id: None,
            tool: "read".to_string(),
            payload: json!({"path": "cairn://p/tpa/1/1/builder/permissions/perm-1"}),
            tool_use_id: None,
            origin: None,
        },
    )
    .await;
    assert!(output.contains("## actions"), "missing actions: {output}");
    assert!(
        output.contains("answer permission"),
        "missing answer action: {output}"
    );
    assert!(
        output.contains("mode:\"patch\"") || output.contains("mode=\"patch\""),
        "missing patch example: {output}"
    );
}

#[tokio::test]
async fn raise_fence_honors_policy_and_session_grant() {
    let (_temp, db, orch) = resource_orchestrator_fixture().await;

    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/wt".to_string(),
        run_id: Some("run-x".to_string()),
        tool: "read".to_string(),
        payload: json!({"path": "file:/outside"}),
        tool_use_id: Some("toolu-x".to_string()),
        origin: None,
    };

    // Allow / Deny short-circuit without touching the DB.
    assert!(matches!(
        raise_fence(
            &orch,
            "run-x",
            Fence::Allow,
            &request,
            Crossing::read_denied(Path::new("/outside"))
        )
        .await,
        FenceDecision::Allow
    ));
    assert!(matches!(
        raise_fence(
            &orch,
            "run-x",
            Fence::Deny,
            &request,
            Crossing::read_denied(Path::new("/outside"))
        )
        .await,
        FenceDecision::Deny(_)
    ));

    // A session grant for the descriptor short-circuits Ask to Allow with no
    // permission_requests row inserted (no suspend).
    orch.session_allowed_crossings
        .lock()
        .unwrap()
        .insert(Crossing::read_denied(Path::new("/outside")).descriptor);
    assert!(matches!(
        raise_fence(
            &orch,
            "run-x",
            Fence::Ask,
            &request,
            Crossing::read_denied(Path::new("/outside"))
        )
        .await,
        FenceDecision::Allow
    ));
    assert_eq!(
        permission_request_count(&db).await,
        0,
        "a granted crossing must not insert a permission request"
    );
}

/// Bug 2: a shell path crossing keys its session grant on the *resolved path*,
/// so a grant for one command short-circuits a *different* command that touches
/// the same out-of-worktree path. Proven at the keying level: two distinct shell
/// commands produce the same `/etc/hosts` descriptor, and a session grant on the
/// path auto-allows the second without inserting a new permission request.
#[tokio::test]
async fn shell_path_crossing_session_grant_generalizes_across_commands() {
    let (_temp, db, orch) = resource_orchestrator_fixture().await;

    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/wt".to_string(),
        run_id: Some("run-x".to_string()),
        tool: "run".to_string(),
        payload: json!({"command": "grep needle /etc/hosts"}),
        tool_use_id: Some("toolu-x".to_string()),
        origin: None,
    };

    // Two different commands, same resolved out-of-worktree path -> same
    // descriptor.
    let first = Crossing::shell_path(Path::new("/etc/hosts"), "cat /etc/hosts");
    let second = Crossing::shell_path(Path::new("/etc/hosts"), "grep needle /etc/hosts");
    assert_eq!(first.descriptor, "/etc/hosts");
    assert_eq!(first.descriptor, second.descriptor);

    // Grant a session crossing for the path (as resolve_permission_request would
    // on allow + session).
    orch.session_allowed_crossings
        .lock()
        .unwrap()
        .insert("/etc/hosts".to_string());

    // A different command touching the same path short-circuits to Allow with no
    // new permission request.
    assert!(matches!(
        raise_fence(&orch, "run-x", Fence::Ask, &request, second).await,
        FenceDecision::Allow
    ));
    assert_eq!(
        permission_request_count(&db).await,
        0,
        "a path-keyed session grant must short-circuit without a new request"
    );
}

// ============================================================================
// Authority prompts are not agent-answerable
// ============================================================================

/// An authority `tool_input`, as `raise_authority` stores it: a normalized
/// scope, the concrete mutation, and the resultant configuration identity. It
/// carries no `descriptor`, which is what keeps it from ever parsing as a fence
/// crossing.
fn authority_tool_input() -> String {
    json!({
        "kind": "authority_request",
        "verb": "write",
        "authority": {
            "scope": {
                "place": {
                    "place": "tool",
                    "workspace_id": "default",
                    "kind": "mcp_server",
                    "canonical_name": "linear"
                },
                "action": "write"
            },
            "mutation": "create",
            "summary": "install workspace MCP server 'linear'",
            "facts": {
                "mcp_config": {
                    "algorithm": "sha256",
                    "encoding_version": 1,
                    "digest": "fixture-digest"
                }
            }
        },
        "reason": "workspace_tool_capability",
        "principal": {
            "node_uri": "cairn://p/tpa/1/1/builder",
            "run_id": "run-1",
            "agent_id": "build"
        },
        "audience": {"workspace_id": "default"},
        "request": {
            "cwd": "/wt",
            "run_id": "run-1",
            "tool": "write",
            "payload": {},
            "tool_use_id": "toolu-perm"
        }
    })
    .to_string()
}

async fn make_request_an_authority_prompt(db: &LocalDb) {
    let tool_input = authority_tool_input();
    db.write(|conn| {
        let tool_input = tool_input.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE permission_requests SET tool_input = ?1, tool_name = 'write' \
                 WHERE id = 'perm-row-1'",
                params![tool_input.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn grant_count(db: &LocalDb) -> i64 {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query("SELECT COUNT(*) FROM authority_grants", ())
                .await?;
            Ok(rows
                .next()
                .await?
                .and_then(|row| row.i64(0).ok())
                .unwrap_or(0))
        })
    })
    .await
    .unwrap()
}

/// The residual CAIRN-3803 shipped: an agent could answer its own authority
/// prompt through the `permissions` resource, including with `standing` — the
/// one lifetime that crosses runs. Every lifetime must now be refused, and the
/// refusal must leave the prompt pending for the operator rather than resolving
/// it into a denial the agent chose.
#[tokio::test]
async fn an_agent_cannot_approve_an_authority_prompt_at_any_lifetime() {
    for lifetime in ["once", "turn", "session", "standing"] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        insert_permission_fixture(&db).await;
        make_request_an_authority_prompt(&db).await;
        let orch = orchestrator(&temp, db.clone());

        let result = handle_write(
            &orch,
            &change_request(json!({
                "changes": [{
                    "target": "cairn://p/tpa/1/1/builder/permissions/perm-1",
                    "mode": "patch",
                    "payload": {"decision": "allow", "lifetime": lifetime}
                }]
            })),
        )
        .await;

        assert!(
            result.contains("only an authenticated operator"),
            "lifetime {lifetime}: expected a self-approval refusal, got: {result}"
        );
        assert_eq!(
            perm_status(&db).await.as_deref(),
            Some("pending"),
            "lifetime {lifetime}: a refused self-approval must leave the prompt pending for the \
             operator, not resolve it"
        );
        assert_eq!(
            grant_count(&db).await,
            0,
            "lifetime {lifetime}: no grant may be minted"
        );
    }
}

/// Denial stays open to every surface. An agent recognizing that it should not
/// be doing something is behavior worth keeping, and removing it would be a
/// regression dressed up as hardening.
#[tokio::test]
async fn an_agent_can_still_deny_an_authority_prompt() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_permission_fixture(&db).await;
    make_request_an_authority_prompt(&db).await;
    let orch = orchestrator(&temp, db.clone());

    let result = handle_write(
        &orch,
        &change_request(json!({
            "changes": [{
                "target": "cairn://p/tpa/1/1/builder/permissions/perm-1",
                "mode": "patch",
                "payload": {"decision": "deny"}
            }]
        })),
    )
    .await;

    assert!(
        result.contains("Answered permission perm-1"),
        "unexpected result: {result}"
    );
    assert_eq!(perm_status(&db).await.as_deref(), Some("denied"));
    assert_eq!(grant_count(&db).await, 0);
}
