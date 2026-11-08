use crate::common;

use std::path::Path;
use std::sync::Arc;

use crate::common::{orchestrator, resource_orchestrator_fixture};
use cairn_core::internal::mcp::handlers::fence::{raise_fence, Crossing, FenceDecision};
use cairn_core::internal::mcp::handlers::read::handle_read_file;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::storage::{LocalDb, RowExt};
use cairn_core::models::{ExecutionSnapshot, Fence};
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

/// Ask requires an existing exact session grant; it does not itself authorize
/// a parent to allow its child's crossing.
fn permission_snapshot(project_id: &str, issue_id: Option<&str>) -> String {
    let snapshot: ExecutionSnapshot = serde_json::from_value(json!({
        "recipe": {
            "id": "recipe-1",
            "name": "Permission answer test",
            "trigger": "manual",
            "nodes": [],
            "edges": []
        },
        "agents": {
            "permission-agent": {
                "id": "permission-agent",
                "name": "Permission test agent",
                "description": "",
                "prompt": "",
                "tools": [],
                "models": [],
                "fence": "ask"
            }
        },
        "skills": {},
        "triggerContext": {
            "projectId": project_id,
            "issueId": issue_id,
            "triggerType": "manual"
        },
        "createdAt": 1
    }))
    .unwrap();
    snapshot.to_json().unwrap()
}

/// Insert a node fixture with one pending fence permission request. The request
/// has no `turn_id`, so resolving it does not create a successor turn or resume
/// a (nonexistent) process — keeping the test to the resolution semantics.
async fn insert_permission_fixture(db: &LocalDb) {
    let project_id = common::create_project(db, "tpa").await;
    insert_builder_node(db, &project_id, Filing::default()).await;
}

/// How `issue-1` was filed. Its parent edge and creating job are written in the
/// issue's own INSERT, as a real filing writes them: a later UPDATE of a
/// parent column is an adoption, which the database revokes for good, so a
/// fixture that wants creator-proven authority must never establish it that
/// way. The parent rows must therefore exist first.
#[derive(Clone, Copy, Default)]
struct Filing {
    parent_issue: Option<&'static str>,
    parent_thread: Option<&'static str>,
    creator_job: Option<&'static str>,
}

fn sql_text(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_string(), |value| format!("'{value}'"))
}

/// `issue-1` as filed by `filing`, with its running `builder` node and one
/// pending crossing at `perm-1`.
async fn insert_builder_node(db: &LocalDb, project_id: &str, filing: Filing) {
    let project_id = project_id.to_string();
    let snapshot = permission_snapshot(&project_id, Some("issue-1"));
    let tool_input = crossing_tool_input();
    let insert_issue = format!(
        "INSERT INTO issues(id, project_id, number, title, status, attention, parent_issue_id, parent_thread_id, parent_job_id, created_at, updated_at)
         VALUES ('issue-1', ?1, 1, 'Permission answer test', 'active', 'needs_input', {}, {}, {}, 1, 1)",
        sql_text(filing.parent_issue),
        sql_text(filing.parent_thread),
        sql_text(filing.creator_job),
    );
    db.write(|conn| {
        let project_id = project_id.clone();
        let snapshot = snapshot.clone();
        let tool_input = tool_input.clone();
        let insert_issue = insert_issue.clone();
        Box::pin(async move {
            conn.execute(insert_issue.as_str(), params![project_id.as_str()])
                .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('exec-1', 'recipe-1', 'issue-1', ?1, 'running', 1, 1, ?2, 'manual')",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'permission-agent', 'builder', 'running', 'session-1', 'builder', 1, 1)",
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
    let snapshot = permission_snapshot(&project_id, None);
    let tool_input = crossing_tool_input();
    db.write(|conn| {
        let project_id = project_id.clone();
        let snapshot = snapshot.clone();
        let tool_input = tool_input.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
                 VALUES ('thread-1', ?1, 'design', 'active', 'needs_input', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, project_id, status, started_at, snapshot)
                 VALUES ('host-exec-1', 'host-thread-job-1', ?1, 'running', 1, ?2)",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, thread_id, project_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('thread-job-1', 'host-exec-1', 'thread-1', ?1, 'permission-agent', 'Thread', 'running', 'thread-session-1', 'thread', 1, 1)",
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
async fn node_permission_patch_self_allow_is_refused() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());
    let result = handle_write(
        &orch,
        &change_request(permission_patch(
            "cairn://p/tpa/1/1/builder/permissions/perm-1",
            json!({"decision": "allow", "scope": "session"}),
        )),
    )
    .await;
    assert!(!result.contains("Answered permission"), "{result}");
    assert_pending_without_resolution(&db, "perm-row-1").await;
    assert!(orch.session_allowed_crossings.lock().unwrap().is_empty());
}

#[tokio::test]
async fn node_permission_patch_self_deny_is_refused() {
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
    assert!(!result.contains("Answered permission"), "{result}");
    assert_pending_without_resolution(&db, "perm-row-1").await;
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
    orch.session_allowed_crossings
        .lock()
        .unwrap()
        .insert("/etc/hosts".to_string());

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
        orch.session_allowed_crossings
            .lock()
            .unwrap()
            .insert("/etc/hosts".to_string());

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

/// Even a legitimate ancestor cannot mint authority grants at any lifetime.
/// The refusal must leave the prompt pending for the operator.
#[tokio::test]
async fn an_agent_cannot_approve_an_authority_prompt_at_any_lifetime() {
    for lifetime in ["once", "turn", "session", "standing"] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        // Filed by the thread, so the thread is a legitimate ancestor and the
        // refusal is about authority issuance, not lineage.
        insert_filed_permission_fixture(&db, None, THREAD_FILED).await;
        make_request_an_authority_prompt(&db).await;
        let orch = orchestrator(&temp, db.clone());

        let result = handle_write(
            &orch,
            &change_request_as(
                "run-other",
                permission_patch(
                    "cairn://p/tpa/1/1/builder/permissions/perm-1",
                    json!({"decision": "allow", "lifetime": lifetime}),
                ),
            ),
        )
        .await;

        assert!(
            result.contains("only an authenticated operator"),
            "lifetime {lifetime}: expected an authority-issuance refusal, got: {result}"
        );
        assert_pending_without_resolution(&db, "perm-row-1").await;
        assert_eq!(
            grant_count(&db).await,
            0,
            "lifetime {lifetime}: no grant may be minted"
        );
    }
}

/// Self-denial is not authority to resolve a request: it remains pending for
/// an authenticated ancestor or the operator.
#[tokio::test]
async fn an_agent_cannot_resolve_its_own_authority_prompt_by_denying() {
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

    assert!(!result.contains("Answered permission"), "{result}");
    assert_pending_without_resolution(&db, "perm-row-1").await;
    assert_eq!(grant_count(&db).await, 0);
}

fn permission_patch(target: &str, payload: serde_json::Value) -> serde_json::Value {
    json!({"changes": [{"target": target, "mode": "patch", "payload": payload}]})
}

async fn fixture_sql(db: &LocalDb, sql: &str) {
    let sql = sql.to_string();
    db.write(move |conn| {
        let sql = sql.clone();
        Box::pin(async move {
            for statement in sql.split(';').filter(|s| !s.trim().is_empty()) {
                conn.execute(statement, ()).await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn assert_pending_without_resolution(db: &LocalDb, request_id: &str) {
    assert_eq!(
        perm_status_of(db, request_id).await.as_deref(),
        Some("pending")
    );
    assert_eq!(
        db.query_opt_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM channel_ask_resolution WHERE action_ref = ?1",
            params![request_id],
        )
        .await
        .unwrap()
        .as_deref(),
        Some("0"),
        "a refused answer must not claim the ask-resolution gate",
    );
    assert_eq!(
        db.query_opt_text(
            "SELECT resolution_actor FROM permission_requests WHERE id = ?1",
            params![request_id],
        )
        .await
        .unwrap(),
        None,
        "a refused answer must not leave resolution attribution",
    );
}

async fn assert_resolution_actor(db: &LocalDb, request_id: &str, node: &str, run_id: &str) {
    let actor = db
        .query_opt_text(
            "SELECT resolution_actor FROM permission_requests WHERE id = ?1",
            params![request_id],
        )
        .await
        .unwrap()
        .expect("resolved permission must have an actor");
    let principal: cairn_common::identity::PrincipalRef = serde_json::from_str(&actor).unwrap();
    assert_eq!(
        principal,
        cairn_common::identity::PrincipalRef::Agent {
            node: node.to_string(),
            run_id: Some(run_id.to_string()),
        }
    );
    let winner = db
        .query_opt_text(
            "SELECT winner_actor FROM channel_ask_resolution WHERE action_ref = ?1",
            params![request_id],
        )
        .await
        .unwrap();
    assert_eq!(winner.as_deref(), Some(actor.as_str()));
}

/// A separate thread session in the same project does not inherit any authority
/// over the builder or its tasks.
async fn insert_unrelated_thread(db: &LocalDb) {
    let project_id = db
        .query_opt_text("SELECT project_id FROM issues WHERE id = 'issue-1'", ())
        .await
        .unwrap()
        .unwrap();
    insert_unrelated_thread_in(db, &project_id).await;
}

/// Thread `other` (home `cairn://p/tpa/other`) with its session job in its own
/// issue-less host execution, so it can exist before `issue-1` and be named as
/// `issue-1`'s filing parent. Its agent holds an `ask` fence.
async fn insert_unrelated_thread_in(db: &LocalDb, project_id: &str) {
    let project_id = project_id.to_string();
    let snapshot = permission_snapshot(&project_id, None);
    db.write(|conn| {
        let project_id = project_id.clone();
        let snapshot = snapshot.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
                 VALUES ('other-thread', ?1, 'other', 'active', 'idle', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, project_id, status, started_at, snapshot)
                 VALUES ('other-exec', 'host-other-job', ?1, 'running', 1, ?2)",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, project_id, thread_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('other-job', 'other-exec', ?1, 'other-thread', 'permission-agent', 'Thread', 'running', 'other-session', 'thread', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('other-session', 'other-job', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-other', ?1, 'other-job', 'live', 'other-session', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Issue `tpa/2` (`parent-issue`), routed under thread `other`, with a running
/// builder `parent-job` / `run-parent` (home `cairn://p/tpa/2/1/builder`).
/// `thread_filed` records, at INSERT, that the thread filed it.
async fn insert_parent_issue(db: &LocalDb, project_id: &str, thread_filed: bool) {
    let project_id = project_id.to_string();
    let snapshot = permission_snapshot(&project_id, Some("parent-issue"));
    let insert_issue = format!(
        "INSERT INTO issues(id, project_id, number, title, status, attention, parent_thread_id, parent_job_id, created_at, updated_at)
         VALUES ('parent-issue', ?1, 2, 'Routed parent', 'active', 'idle', 'other-thread', {}, 1, 1)",
        sql_text(thread_filed.then_some("other-job")),
    );
    db.write(|conn| {
        let project_id = project_id.clone();
        let snapshot = snapshot.clone();
        let insert_issue = insert_issue.clone();
        Box::pin(async move {
            conn.execute(insert_issue.as_str(), params![project_id.as_str()])
                .await?;
            conn.execute(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
                 VALUES ('parent-exec', 'recipe-1', 'parent-issue', ?1, 'running', 1, 1, ?2, 'manual')",
                params![project_id.as_str(), snapshot.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
                 VALUES ('parent-job', 'parent-exec', 'parent-issue', ?1, 'permission-agent', 'builder', 'running', 'parent-session', 'builder', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                 VALUES ('parent-session', 'parent-job', 'active', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
                 VALUES ('run-parent', ?1, 'parent-issue', 'parent-job', 'live', 'parent-session', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Project `tpa` with thread `other` and, optionally, `parent-issue`, then
/// `issue-1` filed as `filing` says.
async fn insert_filed_permission_fixture(
    db: &LocalDb,
    with_parent_issue: Option<bool>,
    filing: Filing,
) {
    let project_id = common::create_project(db, "tpa").await;
    insert_unrelated_thread_in(db, &project_id).await;
    if let Some(thread_filed) = with_parent_issue {
        insert_parent_issue(db, &project_id, thread_filed).await;
    }
    insert_builder_node(db, &project_id, filing).await;
}

/// `issue-1` genuinely filed by thread `other`.
const THREAD_FILED: Filing = Filing {
    parent_issue: None,
    parent_thread: Some("other-thread"),
    creator_job: Some("other-job"),
};

#[tokio::test]
async fn unrelated_thread_cannot_answer_even_with_forged_parent_payload_then_parent_can_deny() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_task_permission_fixture(&db).await;
    insert_unrelated_thread(&db).await;
    let orch = orchestrator(&temp, db.clone());
    let target = "cairn://p/tpa/1/1/builder/task/write-scripts/permissions/perm-1";
    // Even an already-held crossing grant cannot make an unrelated agent an ancestor.
    orch.session_allowed_crossings
        .lock()
        .unwrap()
        .insert("/etc/hosts".to_string());
    for decision in ["deny", "allow"] {
        let result = handle_write(
            &orch,
            &change_request_as(
                "run-other",
                permission_patch(target, json!({"decision": decision})),
            ),
        )
        .await;
        assert!(result.contains("ancestor lineage"), "{decision}: {result}");
        assert_pending_without_resolution(&db, "perm-row-task").await;
    }
    assert_eq!(
        db.query_one(
            "SELECT COUNT(*) FROM authorization_events WHERE request_uri = 'perm-row-task' AND reason = 'permission_answer_authority' AND outcome = 'forbidden'",
            (),
            |row| row.i64(0),
        ).await.unwrap(),
        2,
        "both validly shaped foreign answers must reach and be journaled by the shared guard",
    );
    for decision in ["deny", "allow"] {
        let result = handle_write(&orch, &change_request_as("run-other", permission_patch(
            target,
            json!({
                "decision": decision,
                "actor": "cairn://p/tpa/1/1/builder",
                "run_id": "run-1",
                "parent_job_id": "job-1",
                "lineage": ["job-1"],
                "resolution_actor": {"kind": "agent", "node": "cairn://p/tpa/1/1/builder", "run_id": "run-1"}
            }),
        ))).await;
        assert!(
            !result.contains("Answered permission"),
            "{decision}: {result}"
        );
        assert_pending_without_resolution(&db, "perm-row-task").await;
    }

    let result = handle_write(
        &orch,
        &change_request(permission_patch(target, json!({"decision": "deny"}))),
    )
    .await;
    assert!(result.contains("Answered permission perm-1"), "{result}");
    assert_eq!(
        perm_status_of(&db, "perm-row-task").await.as_deref(),
        Some("denied")
    );
    assert_resolution_actor(&db, "perm-row-task", "cairn://p/tpa/1/1/builder", "run-1").await;
    assert_pending_without_resolution(&db, "perm-row-1").await;
}

#[tokio::test]
async fn sibling_cannot_resolve_another_tasks_permission() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_task_permission_fixture(&db).await;
    fixture_sql(&db, "
        INSERT INTO jobs(id, execution_id, issue_id, project_id, parent_job_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
        SELECT 'sibling-job', execution_id, issue_id, project_id, parent_job_id, 'Build', 'running', 'sibling-session', 'sibling', 2, 2 FROM jobs WHERE id = 'job-task';
        INSERT INTO sessions(id, job_id, status, created_at, updated_at) VALUES ('sibling-session', 'sibling-job', 'active', 2, 2);
        INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
        SELECT 'run-sibling', project_id, issue_id, 'sibling-job', 'live', 'sibling-session', 2, 2, 'resume' FROM jobs WHERE id = 'job-task'
    ").await;
    let orch = orchestrator(&temp, db.clone());
    for decision in ["allow", "deny"] {
        let result = handle_write(
            &orch,
            &change_request_as(
                "run-sibling",
                permission_patch(
                    "cairn://p/tpa/1/1/builder/task/write-scripts/permissions/perm-1",
                    json!({"decision": decision}),
                ),
            ),
        )
        .await;
        assert!(
            !result.contains("Answered permission"),
            "{decision}: {result}"
        );
        assert_pending_without_resolution(&db, "perm-row-task").await;
    }
}

#[tokio::test]
async fn ancestor_cannot_allow_a_crossing_outside_its_effective_grants() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_task_permission_fixture(&db).await;
    let orch = orchestrator(&temp, db.clone());
    // A different descriptor is not authority for /etc/hosts.
    orch.session_allowed_crossings
        .lock()
        .unwrap()
        .insert("/etc/resolv.conf".to_string());
    let result = handle_write(
        &orch,
        &change_request(permission_patch(
            "cairn://p/tpa/1/1/builder/task/write-scripts/permissions/perm-1",
            json!({"decision": "allow", "scope": "session"}),
        )),
    )
    .await;
    assert!(!result.contains("Answered permission"), "{result}");
    assert_pending_without_resolution(&db, "perm-row-task").await;
    assert!(!orch
        .session_allowed_crossings
        .lock()
        .unwrap()
        .contains("/etc/hosts"));
}

/// An issue→issue or issue→thread edge confers answering authority only when
/// its `parent_job_id` proves the parent filed the child. The grandparent
/// thread answers only when BOTH edges on the way up carry that proof; a
/// missing proof on either edge is attention routing and nothing more.
#[tokio::test]
async fn grandparent_thread_can_answer_only_through_creator_proven_issue_parent_edges() {
    for (child_proof, parent_proof) in [(false, false), (true, false), (false, true), (true, true)]
    {
        let case = format!("child_proof={child_proof} parent_proof={parent_proof}");
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        insert_filed_permission_fixture(
            &db,
            Some(parent_proof),
            Filing {
                parent_issue: Some("parent-issue"),
                parent_thread: None,
                creator_job: child_proof.then_some("parent-job"),
            },
        )
        .await;
        let orch = orchestrator(&temp, db.clone());
        // The grandparent already holds the crossing grant, so a refused allow
        // is about lineage, not about missing effect grants.
        orch.session_allowed_crossings
            .lock()
            .unwrap()
            .insert("/etc/hosts".to_string());
        let target = "cairn://p/tpa/1/1/builder/permissions/perm-1";
        if child_proof && parent_proof {
            let result = handle_write(
                &orch,
                &change_request_as(
                    "run-other",
                    permission_patch(target, json!({"decision": "deny"})),
                ),
            )
            .await;
            assert!(
                result.contains("Answered permission perm-1"),
                "{case}: {result}"
            );
            assert_eq!(perm_status(&db).await.as_deref(), Some("denied"), "{case}");
            assert_resolution_actor(&db, "perm-row-1", "cairn://p/tpa/other", "run-other").await;
        } else {
            let mut refusals = 0;
            assert_lineage_refuses_both(
                &orch,
                &db,
                "run-other",
                target,
                "perm-row-1",
                &mut refusals,
                &case,
            )
            .await;
            operator_deny(&orch, &db, "perm-row-1").await;
        }
    }
}

/// `issue-1` genuinely filed by `parent-issue`'s builder, which thread `other`
/// genuinely filed: both edges carry creator proof from INSERT. Thread
/// `adopter` exists but has never filed anything.
async fn insert_issue_parent_lineage(db: &LocalDb) {
    insert_filed_permission_fixture(
        db,
        Some(true),
        Filing {
            parent_issue: Some("parent-issue"),
            parent_thread: None,
            creator_job: Some("parent-job"),
        },
    )
    .await;
    fixture_sql(db, "
        INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
        SELECT 'new-thread', project_id, 'adopter', 'active', 'idle', 1, 1 FROM jobs WHERE id = 'job-1';
        INSERT INTO executions(id, recipe_id, project_id, status, started_at, snapshot)
        SELECT 'new-exec', 'host-new-job', project_id, status, started_at, snapshot FROM executions WHERE id = 'other-exec';
        INSERT INTO jobs(id, execution_id, project_id, thread_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
        SELECT 'new-job', 'new-exec', project_id, 'new-thread', 'permission-agent', 'Thread', 'running', 'new-session', 'thread', 1, 1 FROM jobs WHERE id = 'job-1';
        INSERT INTO sessions(id, job_id, status, created_at, updated_at)
        VALUES ('new-session', 'new-job', 'active', 1, 1);
        INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at, start_mode)
        SELECT 'run-new', project_id, 'new-job', 'live', 'new-session', 1, 1, 'resume' FROM jobs WHERE id = 'job-1';
        INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
        SELECT 'perm-row-2', run_id, job_id, 'toolu-perm-2', tool_name, tool_input, 'pending', 1, 'perm-2' FROM permission_requests WHERE id = 'perm-row-1';
        INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
        SELECT 'perm-row-3', run_id, job_id, 'toolu-perm-3', tool_name, tool_input, 'pending', 1, 'perm-3' FROM permission_requests WHERE id = 'perm-row-1'
    ").await;
}

async fn refusal_journal_count(db: &LocalDb, request_id: &str) -> i64 {
    db.query_one(
        "SELECT COUNT(*) FROM authorization_events WHERE request_uri = ?1 AND reason = 'permission_answer_authority' AND outcome = 'forbidden'",
        params![request_id],
        |row| row.i64(0),
    ).await.unwrap()
}

#[tokio::test]
async fn real_issue_reparent_revokes_former_parent_and_grandparent_permission_authority() {
    for orphan_first in [false, true] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        insert_issue_parent_lineage(&db).await;
        let orch = orchestrator(&temp, db.clone());
        // Both former ancestors already hold the crossing grant. Their refusal
        // must therefore be about the active lineage, not missing effect grants.
        orch.session_allowed_crossings
            .lock()
            .unwrap()
            .insert("/etc/hosts".to_string());
        // Positive controls: before the edge changes, the creator-proven parent
        // and grandparent genuinely answer, so the refusals below are revocation.
        for (run_id, home, segment, row) in [
            (
                "run-parent",
                "cairn://p/tpa/2/1/builder",
                "perm-2",
                "perm-row-2",
            ),
            ("run-other", "cairn://p/tpa/other", "perm-3", "perm-row-3"),
        ] {
            let result = handle_write(
                &orch,
                &change_request_as(
                    run_id,
                    permission_patch(
                        &format!("cairn://p/tpa/1/1/builder/permissions/{segment}"),
                        json!({"decision": "deny"}),
                    ),
                ),
            )
            .await;
            assert!(
                result.contains(&format!("Answered permission {segment}")),
                "{run_id}: {result}"
            );
            assert_resolution_actor(&db, row, home, run_id).await;
        }
        let target = "cairn://p/tpa/1/1/builder/permissions/perm-1";
        let parents = if orphan_first {
            vec![serde_json::Value::Null, json!("cairn://p/tpa/adopter")]
        } else {
            vec![json!("cairn://p/tpa/adopter")]
        };
        let mut refusals = 0;
        for parent in parents {
            let result = handle_write(
                &orch,
                &change_request_as(
                    "run-new",
                    permission_patch("cairn://p/tpa/1", json!({"parent": parent})),
                ),
            )
            .await;
            assert_eq!(
                db.query_opt_text(
                    "SELECT parent_issue_id FROM issues WHERE id = 'issue-1'",
                    ()
                )
                .await
                .unwrap(),
                None,
                "the real issue write must clear the former parent edge: {result}",
            );
            assert_eq!(
                db.query_opt_text(
                    "SELECT parent_thread_id FROM issues WHERE id = 'issue-1'",
                    ()
                )
                .await
                .unwrap(),
                parent.as_str().map(|_| "new-thread".to_string()),
                "the real issue write must persist the requested adoption: {result}",
            );
            assert_eq!(
                db.query_opt_text("SELECT parent_job_id FROM issues WHERE id = 'issue-1'", ())
                    .await
                    .unwrap()
                    .as_deref(),
                Some("parent-job"),
                "creator provenance deliberately survives reparenting",
            );
            assert_revocation_is_monotonic(&db, "issue-1", &format!("orphan_first={orphan_first}"))
                .await;
            for run_id in ["run-parent", "run-other"] {
                for decision in ["allow", "deny"] {
                    let result = handle_write(
                        &orch,
                        &change_request_as(
                            run_id,
                            permission_patch(target, json!({"decision": decision})),
                        ),
                    )
                    .await;
                    assert!(
                        result.contains("not an ancestor"),
                        "{run_id} {decision}: {result}"
                    );
                    assert_pending_without_resolution(&db, "perm-row-1").await;
                    refusals += 1;
                    assert_eq!(refusal_journal_count(&db, "perm-row-1").await, refusals);
                }
            }
        }
        // Adoption is attention routing only: the adopting thread never filed
        // the child, so it cannot answer either. The operator still can.
        assert_lineage_refuses_both(
            &orch,
            &db,
            "run-new",
            target,
            "perm-row-1",
            &mut refusals,
            &format!("adopter orphan_first={orphan_first}"),
        )
        .await;
        operator_deny(&orch, &db, "perm-row-1").await;
    }
}

// ============================================================================
// Mutable parent edges never create answering authority
// ============================================================================

/// Every allow/deny from `run_id` is refused on lineage, journaled once each,
/// and leaves the request pending with no resolution claim. `refusals` is the
/// running journal count for `request_id`.
async fn assert_lineage_refuses_both(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    db: &LocalDb,
    run_id: &str,
    target: &str,
    request_id: &str,
    refusals: &mut i64,
    case: &str,
) {
    for decision in ["allow", "deny"] {
        let result = handle_write(
            orch,
            &change_request_as(
                run_id,
                permission_patch(target, json!({"decision": decision})),
            ),
        )
        .await;
        assert!(
            result.contains("not an ancestor"),
            "{case}: {run_id} {decision}: {result}"
        );
        assert_pending_without_resolution(db, request_id).await;
        *refusals += 1;
        assert_eq!(
            refusal_journal_count(db, request_id).await,
            *refusals,
            "{case}: {run_id} {decision} must be journaled"
        );
    }
}

async fn assert_answers(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    db: &LocalDb,
    adopter: Adopter,
    target: &str,
    request_id: &str,
    decision: &str,
    case: &str,
) {
    let result = handle_write(
        orch,
        &change_request_as(
            adopter.run,
            permission_patch(target, json!({"decision": decision})),
        ),
    )
    .await;
    assert!(
        result.contains("Answered permission"),
        "{case}: {decision}: {result}"
    );
    let expected = if decision == "allow" {
        "allowed"
    } else {
        "denied"
    };
    assert_eq!(
        perm_status_of(db, request_id).await.as_deref(),
        Some(expected),
        "{case}"
    );
    assert_resolution_actor(db, request_id, adopter.home, adopter.run).await;
}

/// The authenticated operator answers a prompt no agent may: refusal leaves the
/// prompt answerable, it does not strand it.
async fn operator_deny(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    db: &LocalDb,
    request_id: &str,
) {
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, CredentialRef,
        PrincipalRef, VerificationMethod, VerificationRecord, VerificationStatus,
        VerificationStrength,
    };
    use cairn_core::internal::mcp::handlers::permission::{
        resolve_permission_request, OperatorApproval, PermissionAnswer, PermissionDecision,
    };

    let actor = PrincipalRef::Machine {
        device_id: "test-device".to_string(),
    };
    let verification = VerificationRecord::new(
        VerificationMethod::DesktopCredential,
        VerificationStatus::Verified,
        None,
        None,
        None,
        Some(CredentialRef::new("desktop_operator_credential").unwrap()),
        VerificationStrength::new("local_shared_secret").unwrap(),
        1,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::AuthenticatedDesktop,
        Address::Desktop {
            device_id: "test-device".to_string(),
        },
        verification,
        1,
        None,
    )
    .unwrap();
    let appearance = AppearanceSnapshot::new(actor.clone(), evidence, vec![], None).unwrap();
    let approval = OperatorApproval::authenticated(actor, appearance).unwrap();
    resolve_permission_request(
        orch,
        request_id,
        PermissionAnswer::from_operator(PermissionDecision::Deny, approval),
    )
    .await
    .expect("the operator answers a prompt its agents may not");
    assert_eq!(
        perm_status_of(db, request_id).await.as_deref(),
        Some("denied")
    );
}

/// An agent that can write a parent edge: an issue node (`tpa/2`) or a thread
/// (`tpa/other`), each holding the `/etc/hosts` crossing grant under an `ask`
/// fence so its refused allows prove lineage rather than missing effect grants.
#[derive(Clone, Copy)]
struct Adopter {
    label: &'static str,
    run: &'static str,
    job: &'static str,
    home: &'static str,
    parent_uri: &'static str,
    issue_parent: Option<&'static str>,
    thread_parent: Option<&'static str>,
}

const ISSUE_ADOPTER: Adopter = Adopter {
    label: "issue",
    run: "run-adopter",
    job: "adopter-job",
    home: "cairn://p/tpa/2/1/builder",
    parent_uri: "cairn://p/tpa/2",
    issue_parent: Some("adopter-issue"),
    thread_parent: None,
};

const THREAD_ADOPTER: Adopter = Adopter {
    label: "thread",
    run: "run-other",
    job: "other-job",
    home: "cairn://p/tpa/other",
    parent_uri: "cairn://p/tpa/other",
    issue_parent: None,
    thread_parent: Some("other-thread"),
};

/// The builder fixture (`tpa/1`, its pending `perm-1`), the unrelated thread,
/// and an unrelated issue `tpa/2` with its own running builder. Issues filed
/// through the real write path number from 10 so they never collide.
async fn seed_adopters(db: &LocalDb) {
    insert_permission_fixture(db).await;
    insert_unrelated_thread(db).await;
    fixture_sql(db, "
        UPDATE projects SET next_issue_number = 10 WHERE id = (SELECT project_id FROM issues WHERE id = 'issue-1');
        INSERT INTO issues(id, project_id, number, title, status, attention, created_at, updated_at)
        SELECT 'adopter-issue', project_id, 2, 'Unrelated adopter', 'active', 'idle', 1, 1 FROM issues WHERE id = 'issue-1';
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
        SELECT 'adopter-exec', recipe_id, 'adopter-issue', project_id, 'running', 1, 1, snapshot, 'manual' FROM executions WHERE id = 'exec-1';
        INSERT INTO jobs(id, execution_id, issue_id, project_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
        SELECT 'adopter-job', 'adopter-exec', 'adopter-issue', project_id, 'permission-agent', 'builder', 'running', 'adopter-session', 'builder', 1, 1 FROM jobs WHERE id = 'job-1';
        INSERT INTO sessions(id, job_id, status, created_at, updated_at)
        VALUES ('adopter-session', 'adopter-job', 'active', 1, 1);
        INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
        SELECT 'run-adopter', project_id, 'adopter-issue', 'adopter-job', 'live', 'adopter-session', 1, 1, 'resume' FROM jobs WHERE id = 'job-1'
    ").await;
}

fn grant_hosts(orch: &cairn_core::internal::orchestrator::Orchestrator) {
    orch.session_allowed_crossings
        .lock()
        .unwrap()
        .insert("/etc/hosts".to_string());
}

async fn patch_parent(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    run_id: &str,
    number: i64,
    parent: serde_json::Value,
) -> String {
    handle_write(
        orch,
        &change_request_as(
            run_id,
            permission_patch(
                &format!("cairn://p/tpa/{number}"),
                json!({ "parent": parent }),
            ),
        ),
    )
    .await
}

/// The stored `(parent_issue_id, parent_thread_id, parent_job_id)` of an issue.
async fn parent_columns(
    db: &LocalDb,
    issue_id: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let mut columns = Vec::new();
    for column in ["parent_issue_id", "parent_thread_id", "parent_job_id"] {
        columns.push(
            db.query_opt_text(
                &format!("SELECT {column} FROM issues WHERE id = ?1"),
                params![issue_id],
            )
            .await
            .unwrap(),
        );
    }
    let parent_job = columns.pop().unwrap();
    let thread = columns.pop().unwrap();
    let issue = columns.pop().unwrap();
    (issue, thread, parent_job)
}

/// The child's filing-edge revocation stamp, as text so the assertion does not
/// depend on the column's storage type.
async fn filing_revoked_at(db: &LocalDb, issue_id: &str) -> Option<String> {
    db.query_opt_text(
        "SELECT CAST(filing_parent_revoked_at AS TEXT) FROM issues WHERE id = ?1",
        params![issue_id],
    )
    .await
    .unwrap()
}

/// Once set, revocation is permanent in the database itself: no direct UPDATE
/// may clear or move it, and the row cannot be REPLACEd back to an unrevoked
/// one. Each attempt must fail and leave the stamp and the edge untouched.
async fn assert_revocation_is_monotonic(db: &LocalDb, issue_id: &str, case: &str) {
    let stamp = filing_revoked_at(db, issue_id).await;
    assert!(stamp.is_some(), "{case}: the edge change must have revoked");
    let edge = parent_columns(db, issue_id).await;
    for (attempt, sql) in [
        (
            "clear",
            "UPDATE issues SET filing_parent_revoked_at = NULL WHERE id = ?1",
        ),
        (
            "move",
            "UPDATE issues SET filing_parent_revoked_at = filing_parent_revoked_at + 1 WHERE id = ?1",
        ),
        (
            "replace",
            "REPLACE INTO issues(id, project_id, number, title, status, attention, parent_issue_id, parent_thread_id, parent_job_id, created_at, updated_at)
             SELECT id, project_id, number, title, status, attention, parent_issue_id, parent_thread_id, parent_job_id, created_at, updated_at
             FROM issues WHERE id = ?1",
        ),
    ] {
        let outcome = db.execute(sql, params![issue_id]).await;
        assert!(
            outcome.is_err(),
            "{case}: {attempt} of a revoked filing edge must be refused by the database"
        );
        assert_eq!(filing_revoked_at(db, issue_id).await, stamp, "{case}: {attempt}");
        assert_eq!(parent_columns(db, issue_id).await, edge, "{case}: {attempt}");
    }
}

/// An unrevoked filed row cannot be REPLACEd into a different edge or creator
/// with no revocation stamp: that would mint creator proof by SQL rather than
/// by filing. Each attempt must fail and leave the row as filed.
async fn assert_unrevoked_replace_with_new_edge_is_refused(
    db: &LocalDb,
    issue_id: &str,
    case: &str,
) {
    let edge = parent_columns(db, issue_id).await;
    assert_eq!(filing_revoked_at(db, issue_id).await, None, "{case}");
    for (changed, columns) in [
        ("parent edge", "'issue-1', NULL, parent_job_id"),
        (
            "creator proof",
            "parent_issue_id, parent_thread_id, 'job-1'",
        ),
    ] {
        let sql = format!(
            "REPLACE INTO issues(id, project_id, number, title, status, attention, parent_issue_id, parent_thread_id, parent_job_id, created_at, updated_at, filing_parent_revoked_at)
             SELECT id, project_id, number, title, status, attention, {columns}, created_at, updated_at, NULL
             FROM issues WHERE id = ?1"
        );
        let outcome = db.execute(sql.as_str(), params![issue_id]).await;
        assert!(
            outcome.is_err(),
            "{case}: REPLACE with a changed {changed} and no stamp must be refused"
        );
        assert_eq!(
            parent_columns(db, issue_id).await,
            edge,
            "{case}: {changed}"
        );
        assert_eq!(
            filing_revoked_at(db, issue_id).await,
            None,
            "{case}: {changed}"
        );
    }
}

fn current_edge(adopter: Adopter) -> (Option<String>, Option<String>) {
    (
        adopter.issue_parent.map(str::to_string),
        adopter.thread_parent.map(str::to_string),
    )
}

/// File a child through the real `issues` append as `run_id`, then give it a
/// running builder holding three pending crossings `perm-a`..`perm-c`.
/// Returns the child's `(id, number)`.
async fn file_child_with_pending_prompts(
    orch: &cairn_core::internal::orchestrator::Orchestrator,
    db: &LocalDb,
    run_id: &str,
    title: &str,
    parent: Option<&str>,
) -> (String, i64) {
    let mut payload = json!({ "title": title });
    if let Some(parent) = parent {
        payload["parent"] = json!(parent);
    }
    let result = handle_write(
        orch,
        &change_request_as(
            run_id,
            json!({"changes": [{"target": "cairn://p/tpa/issues", "mode": "append", "payload": payload}]}),
        ),
    )
    .await;
    assert!(result.contains("Created issue tpa/"), "{result}");
    let id = db
        .query_opt_text("SELECT id FROM issues WHERE title = ?1", params![title])
        .await
        .unwrap()
        .expect("the filed child exists");
    let number = db
        .query_one(
            "SELECT number FROM issues WHERE id = ?1",
            params![id.as_str()],
            |row| row.i64(0),
        )
        .await
        .unwrap();
    let mut sql = format!("
        INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot, triggered_by)
        SELECT 'child-exec', recipe_id, '{id}', project_id, 'running', 1, 1, snapshot, 'manual' FROM executions WHERE id = 'exec-1';
        INSERT INTO jobs(id, execution_id, issue_id, project_id, agent_config_id, node_name, status, current_session_id, uri_segment, created_at, updated_at)
        SELECT 'child-job', 'child-exec', '{id}', project_id, 'permission-agent', 'builder', 'running', 'child-session', 'builder', 1, 1 FROM jobs WHERE id = 'job-1';
        INSERT INTO sessions(id, job_id, status, created_at, updated_at)
        VALUES ('child-session', 'child-job', 'active', 1, 1);
        INSERT INTO runs(id, project_id, issue_id, job_id, status, session_id, created_at, updated_at, start_mode)
        SELECT 'run-child', project_id, '{id}', 'child-job', 'live', 'child-session', 1, 1, 'resume' FROM jobs WHERE id = 'job-1'
    ");
    for segment in ["perm-a", "perm-b", "perm-c"] {
        sql.push_str(&format!(";
            INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, created_at, uri_segment)
            SELECT 'child-{segment}', 'run-child', 'child-job', 'toolu-{segment}', tool_name, tool_input, 'pending', 1, '{segment}' FROM permission_requests WHERE id = 'perm-row-1'
        "));
    }
    fixture_sql(db, &sql).await;
    (id, number)
}

/// Security ruling on PR #5037, cases (1) and (2): an agent that adopts an
/// unrelated issue gains attention routing only. Neither its deny nor its
/// allow (within a grant it holds) is accepted; each refusal is journaled and
/// the prompt stays pending for the operator.
#[tokio::test]
async fn adopting_an_unrelated_issue_confers_no_answer_authority() {
    for adopter in [ISSUE_ADOPTER, THREAD_ADOPTER] {
        let case = format!("{} adopter", adopter.label);
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        seed_adopters(&db).await;
        let orch = orchestrator(&temp, db.clone());
        grant_hosts(&orch);

        let result = patch_parent(&orch, adopter.run, 1, json!(adopter.parent_uri)).await;
        let (issue, thread, parent_job) = parent_columns(&db, "issue-1").await;
        assert_eq!(
            (issue, thread),
            current_edge(adopter),
            "{case}: the adoption must persist: {result}"
        );
        assert_eq!(
            parent_job, None,
            "{case}: adoption must never mint creator proof"
        );
        assert_revocation_is_monotonic(&db, "issue-1", &case).await;

        let mut refusals = 0;
        assert_lineage_refuses_both(
            &orch,
            &db,
            adopter.run,
            "cairn://p/tpa/1/1/builder/permissions/perm-1",
            "perm-row-1",
            &mut refusals,
            &case,
        )
        .await;
        operator_deny(&orch, &db, "perm-row-1").await;
    }
}

/// Security ruling cases (3)/(4) and the same-parent no-op, on the real write
/// path: a child A files really is A's to answer, and naming the current
/// parent again revokes nothing. The first real edge change (orphan) revokes
/// it, and re-adoption by the very same A never restores it.
#[tokio::test]
async fn a_filed_child_orphaned_then_readopted_by_its_filer_loses_answer_authority_for_good() {
    // The issue filer names its own issue; the thread filer relies on the
    // ordinary inferred parent a thread's filing gets.
    for (adopter, explicit_parent) in [
        (ISSUE_ADOPTER, Some(ISSUE_ADOPTER.parent_uri)),
        (THREAD_ADOPTER, None),
    ] {
        let case = format!("{} filer", adopter.label);
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        seed_adopters(&db).await;
        let orch = orchestrator(&temp, db.clone());
        grant_hosts(&orch);

        let (child, number) = file_child_with_pending_prompts(
            &orch,
            &db,
            adopter.run,
            "Genuine child",
            explicit_parent,
        )
        .await;
        let target =
            |segment: &str| format!("cairn://p/tpa/{number}/1/builder/permissions/{segment}");
        let filed = parent_columns(&db, &child).await;
        assert_eq!(
            filed,
            {
                let (issue, thread) = current_edge(adopter);
                (issue, thread, Some(adopter.job.to_string()))
            },
            "{case}: filing must store the edge and the authenticated creating job",
        );

        assert_eq!(
            filing_revoked_at(&db, &child).await,
            None,
            "{case}: the filing INSERT itself must not revoke its own edge"
        );

        // Positive control: the filer answers its genuine child.
        assert_answers(
            &orch,
            &db,
            adopter,
            &target("perm-a"),
            "child-perm-a",
            "deny",
            &case,
        )
        .await;

        // Naming the current parent again is not a change and revokes nothing.
        let result = patch_parent(&orch, adopter.run, number, json!(adopter.parent_uri)).await;
        assert_eq!(
            parent_columns(&db, &child).await,
            filed,
            "{case}: no-op patch: {result}"
        );
        assert_answers(
            &orch,
            &db,
            adopter,
            &target("perm-b"),
            "child-perm-b",
            "allow",
            &case,
        )
        .await;

        assert_eq!(
            filing_revoked_at(&db, &child).await,
            None,
            "{case}: naming the current parent again must not revoke"
        );
        assert_unrevoked_replace_with_new_edge_is_refused(&db, &child, &case).await;
        assert_eq!(
            perm_status_of(&db, "child-perm-c").await.as_deref(),
            Some("pending"),
            "{case}: a refused REPLACE leaves the child's prompt untouched"
        );

        // Orphan: the edge is gone; creator provenance deliberately survives.
        let result = patch_parent(&orch, adopter.run, number, serde_json::Value::Null).await;
        assert_eq!(
            parent_columns(&db, &child).await,
            (None, None, Some(adopter.job.to_string())),
            "{case}: orphan: {result}"
        );
        let mut refusals = 0;
        assert_lineage_refuses_both(
            &orch,
            &db,
            adopter.run,
            &target("perm-c"),
            "child-perm-c",
            &mut refusals,
            &format!("{case} orphaned"),
        )
        .await;

        let orphan_stamp = filing_revoked_at(&db, &child).await;
        assert!(orphan_stamp.is_some(), "{case}: orphaning must revoke");

        // Re-adoption by the same filer restores the identical stored edge, but
        // the revocation is monotonic.
        let result = patch_parent(&orch, adopter.run, number, json!(adopter.parent_uri)).await;
        assert_eq!(
            parent_columns(&db, &child).await,
            filed,
            "{case}: re-adopt: {result}"
        );
        assert_lineage_refuses_both(
            &orch,
            &db,
            adopter.run,
            &target("perm-c"),
            "child-perm-c",
            &mut refusals,
            &format!("{case} re-adopted"),
        )
        .await;
        assert_eq!(
            filing_revoked_at(&db, &child).await,
            orphan_stamp,
            "{case}: re-adoption must neither clear nor restamp the revocation"
        );
        assert_revocation_is_monotonic(&db, &child, &case).await;
        operator_deny(&orch, &db, "child-perm-c").await;
    }
}

/// A child A filed under some other issue records A's job as its creator, but
/// moving it under A afterwards is an adoption, not a filing: even though the
/// stored `parent_job_id` now names a job of the current parent, the edge
/// change revoked it.
#[tokio::test]
async fn a_child_filed_elsewhere_then_moved_under_its_filer_gains_no_answer_authority() {
    for adopter in [ISSUE_ADOPTER, THREAD_ADOPTER] {
        let case = format!("{} filer", adopter.label);
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        seed_adopters(&db).await;
        let orch = orchestrator(&temp, db.clone());
        grant_hosts(&orch);

        let (child, number) = file_child_with_pending_prompts(
            &orch,
            &db,
            adopter.run,
            "Filed elsewhere",
            Some("cairn://p/tpa/1"),
        )
        .await;
        assert_eq!(
            parent_columns(&db, &child).await,
            (
                Some("issue-1".to_string()),
                None,
                Some(adopter.job.to_string())
            ),
            "{case}: filing under tpa/1 records the filer as creator"
        );
        assert_eq!(filing_revoked_at(&db, &child).await, None, "{case}");
        let result = patch_parent(&orch, adopter.run, number, json!(adopter.parent_uri)).await;
        let (issue, thread) = current_edge(adopter);
        assert_eq!(
            parent_columns(&db, &child).await,
            (issue, thread, Some(adopter.job.to_string())),
            "{case}: move: {result}"
        );
        let mut refusals = 0;
        assert_lineage_refuses_both(
            &orch,
            &db,
            adopter.run,
            &format!("cairn://p/tpa/{number}/1/builder/permissions/perm-a"),
            "child-perm-a",
            &mut refusals,
            &case,
        )
        .await;
        assert_revocation_is_monotonic(&db, &child, &case).await;
        operator_deny(&orch, &db, "child-perm-a").await;
    }
}

/// Security's final ruling: an issue that already had a parent edge before the
/// revocation migration fails closed, even with a creator `parent_job_id` that
/// matches its current parent. No lifetime reparent journal exists, so a
/// matching pair cannot prove the edge was never adopted back. This runs the
/// real upgrade: a database migrated up to just before the revocation
/// migration holds a genuine legacy lineage, then the full set is applied.
#[tokio::test]
async fn legacy_parent_edges_fail_closed_after_the_revocation_upgrade() {
    use cairn_db::storage::{MigrationRunner, TURSO_MIGRATIONS};

    let temp = tempfile::tempdir().unwrap();
    let db = Arc::new(
        LocalDb::open(temp.path().join("legacy.turso.db"))
            .await
            .unwrap(),
    );
    // The pre-upgrade era is the prefix before the revocation migration: later
    // migrations may depend on its column, so they are never applied without it.
    let index = TURSO_MIGRATIONS
        .iter()
        .position(|migration| migration.name() == "issue_filing_parent_revocation")
        .expect("the revocation migration is registered");
    MigrationRunner::new(TURSO_MIGRATIONS[..index].to_vec())
        .run(&db)
        .await
        .unwrap();

    // Every edge here is a genuine filing with its matching creator: issue-1
    // by parent-issue's builder, parent-issue by thread `other`.
    insert_filed_permission_fixture(
        &db,
        Some(true),
        Filing {
            parent_issue: Some("parent-issue"),
            parent_thread: None,
            creator_job: Some("parent-job"),
        },
    )
    .await;

    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    for issue_id in ["issue-1", "parent-issue"] {
        assert!(
            filing_revoked_at(&db, issue_id).await.is_some(),
            "{issue_id}: a pre-existing parent edge must be revoked by the upgrade"
        );
    }
    assert_eq!(
        parent_columns(&db, "issue-1").await,
        (
            Some("parent-issue".to_string()),
            None,
            Some("parent-job".to_string())
        ),
        "the upgrade keeps the edge and creator provenance; it revokes, never clears"
    );
    assert_revocation_is_monotonic(&db, "issue-1", "legacy").await;

    let orch = orchestrator(&temp, db.clone());
    grant_hosts(&orch);
    let mut refusals = 0;
    for run_id in ["run-parent", "run-other"] {
        assert_lineage_refuses_both(
            &orch,
            &db,
            run_id,
            "cairn://p/tpa/1/1/builder/permissions/perm-1",
            "perm-row-1",
            &mut refusals,
            "legacy",
        )
        .await;
    }
    operator_deny(&orch, &db, "perm-row-1").await;
}

/// Creator proof is part of the filing edge: rewriting `parent_job_id` with
/// ordinary SQL revokes the edge just as moving it does, and writing the
/// original filer back does not un-revoke it.
#[tokio::test]
async fn a_raw_creator_proof_rewrite_revokes_the_filing_edge() {
    let (temp, db) = common::migrated_db().await;
    let db = Arc::new(db);
    insert_filed_permission_fixture(&db, None, THREAD_FILED).await;
    assert_eq!(
        filing_revoked_at(&db, "issue-1").await,
        None,
        "a filing INSERT does not revoke its own edge"
    );
    fixture_sql(
        &db,
        "UPDATE issues SET parent_job_id = 'job-1' WHERE id = 'issue-1'",
    )
    .await;
    let stamp = filing_revoked_at(&db, "issue-1").await;
    assert!(stamp.is_some(), "rewriting the creator must revoke");
    fixture_sql(
        &db,
        "UPDATE issues SET parent_job_id = 'other-job' WHERE id = 'issue-1'",
    )
    .await;
    assert_eq!(filing_revoked_at(&db, "issue-1").await, stamp);
    assert_eq!(
        parent_columns(&db, "issue-1").await,
        (
            None,
            Some("other-thread".to_string()),
            Some("other-job".to_string())
        ),
        "the row looks exactly as filed, but its proof is spent"
    );
    assert_revocation_is_monotonic(&db, "issue-1", "proof rewrite").await;

    let orch = orchestrator(&temp, db.clone());
    grant_hosts(&orch);
    let mut refusals = 0;
    assert_lineage_refuses_both(
        &orch,
        &db,
        "run-other",
        "cairn://p/tpa/1/1/builder/permissions/perm-1",
        "perm-row-1",
        &mut refusals,
        "proof rewrite",
    )
    .await;
    operator_deny(&orch, &db, "perm-row-1").await;
}

async fn check_claude_ancestor_core_tool_grants(mcp_prompt: bool) {
    for excluded in [false, true] {
        let (temp, db) = common::migrated_db().await;
        let db = Arc::new(db);
        insert_filed_permission_fixture(&db, None, THREAD_FILED).await;
        fixture_sql(
            &db,
            "UPDATE sessions SET backend = 'claude' WHERE id = 'other-session'",
        )
        .await;
        let snapshot = db
            .query_opt_text("SELECT snapshot FROM executions WHERE id = 'exec-1'", ())
            .await
            .unwrap()
            .unwrap();
        let mut snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        snapshot["agents"]["permission-agent"]["disallowedTools"] = if excluded {
            json!(["mcp__cairn__run"])
        } else {
            json!([])
        };
        let typed: ExecutionSnapshot = serde_json::from_value(snapshot).unwrap();
        assert_eq!(
            typed.agents["permission-agent"]
                .disallowed_tools
                .clone()
                .unwrap(),
            if excluded {
                vec!["mcp__cairn__run".to_string()]
            } else {
                vec![]
            }
        );
        let snapshot = typed.to_json().unwrap();
        let input = if mcp_prompt {
                json!({"serverName": "cairn", "toolName": "run", "_meta": {"codex_approval_kind": "mcp_tool_call"}})
            } else {
                json!({"commands": [{"command": "echo permission-test"}]})
            }.to_string();
        db.write(move |conn| {
                let snapshot = snapshot.clone();
                let input = input.clone();
                Box::pin(async move {
                    // The answering thread's agent config lives in its own host execution.
                    conn.execute("UPDATE executions SET snapshot = ?1 WHERE id IN ('exec-1', 'other-exec')", params![snapshot]).await?;
                    conn.execute("UPDATE permission_requests SET tool_name = 'mcp__cairn__run', tool_input = ?1 WHERE id = 'perm-row-1'", params![input]).await?;
                    Ok(())
                })
            }).await.unwrap();
        let orch = orchestrator(&temp, db.clone());
        let result = handle_write(
            &orch,
            &change_request_as(
                "run-other",
                permission_patch(
                    "cairn://p/tpa/1/1/builder/permissions/perm-1",
                    json!({"decision": "allow", "scope": "session"}),
                ),
            ),
        )
        .await;
        if excluded {
            assert!(
                result.contains("own effective grants"),
                "mcp={mcp_prompt}: {result}"
            );
            assert_pending_without_resolution(&db, "perm-row-1").await;
            assert_eq!(refusal_journal_count(&db, "perm-row-1").await, 1);
        } else {
            assert!(
                result.contains("Answered permission perm-1"),
                "mcp={mcp_prompt}: {result}"
            );
            assert_eq!(perm_status(&db).await.as_deref(), Some("allowed"));
            assert_resolution_actor(&db, "perm-row-1", "cairn://p/tpa/other", "run-other").await;
        }
    }
}

#[tokio::test]
async fn claude_ancestor_cannot_delegate_an_excluded_core_tool_from_legacy_prompt() {
    check_claude_ancestor_core_tool_grants(false).await;
}

#[tokio::test]
async fn claude_ancestor_cannot_delegate_an_excluded_core_tool_from_mcp_prompt() {
    check_claude_ancestor_core_tool_grants(true).await;
}
