//! Programmatic execution start via an issue's executions collection.
//!
//! Appending `{recipe?, backend?}` to `cairn://p/PROJECT/NUMBER/executions`
//! starts a new execution. This is the external-driver entry point: it shares
//! the same start sequence as the user-facing Tauri command
//! (`start_recipe_execution_and_advance`) and stamps `initiated_via="external"`
//! attribution on the snapshot. Local-secret auth (which already gates every
//! `write`) is the authorization — there is no separate user-only guard.

use cairn_common::protocol::CallbackRequest;
use cairn_db::turso::params;

use crate::models::AgentSnapshot;
#[cfg(test)]
use crate::models::ExecutionSnapshot;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};

/// Resolve `(project_id, issue_id)` for a project key + issue number.
async fn resolve_project_and_issue(
    db: &LocalDb,
    project_key: &str,
    number: i32,
) -> Result<(String, String), String> {
    let key = project_key.to_uppercase();
    let resolved = db
        .read(|conn| {
            let key = key.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT p.id, i.id
                         FROM projects p
                         JOIN issues i ON i.project_id = p.id
                         WHERE p.key = ?1 AND i.number = ?2
                         LIMIT 1",
                        params![key.as_str(), number],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((row.text(0)?, row.text(1)?))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    resolved.ok_or_else(|| format!("Issue {}-{} not found", key, number))
}

/// Start a new execution for an issue from an append to its executions collection.
pub async fn start_execution_from_collection(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    recipe: Option<&str>,
    backend: Option<&str>,
) -> Result<String, String> {
    // Route the issue lookup to the database that OWNS the project: a team
    // project's issue rows live wholly in its synced replica (CAIRN-2181), so
    // resolving against the private DB would never find a team issue and the
    // execution would never start — the create-then-start half of team memory
    // triage (CAIRN-2587). `for_project` is a strict no-op for a local project.
    let owning_db = orch.db.for_project(project_key).await;
    let (project_id, issue_id) = resolve_project_and_issue(&owning_db, project_key, number).await?;

    let execution = crate::execution::recipe::start_recipe_execution_and_advance(
        orch,
        &issue_id,
        recipe,
        &project_id,
        backend,
        Some("external"),
        crate::models::TriggerType::Manual,
    )?;

    // Wake any in-flight `watch` so the driving loop re-derives state promptly.
    orch.wake_for_issue(&issue_id).await;

    let key = project_key.to_uppercase();
    let issue_uri = cairn_common::uri::build_issue_uri(&key, number);
    let seq = execution.seq.unwrap_or(0);
    Ok(format!(
        "Started execution {} for {}-{} (recipe '{}'). Watch {} for attention.",
        seq, key, number, execution.recipe_id, issue_uri
    ))
}

/// Edit one agent snapshot within an execution from a `patch` on
/// `cairn://p/PROJECT/NUMBER/executions/{seq}`. Resolves the execution, enforces
/// the self-edit privilege guard (resource path only), then routes through the
/// canonical [`crate::execution::snapshot_edit::update_execution_agent`] — the
/// exact pipeline the UI command uses, so a URI edit and a UI edit are
/// byte-for-byte the same operation.
#[allow(clippy::too_many_arguments)]
pub async fn edit_execution_agent(
    orch: &Orchestrator,
    request: &CallbackRequest,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    agent_id: &str,
    snapshot_patch: serde_json::Value,
    dry_run: bool,
) -> Result<String, String> {
    let execution_id = resolve_execution_id(&orch.db.local, project_key, number, exec_seq).await?;

    // Merge the patch over the stored agent snapshot so callers can send only the
    // fields they want to change. A full snapshot is just a merge that touches
    // every field, so full-replace callers (the UI command) are unaffected.
    let agent_snapshot =
        merge_agent_snapshot(&orch.db.local, &execution_id, agent_id, snapshot_patch).await?;

    // Guard against the merged/effective snapshot: an omitted `fence` keeps the
    // stored value (no delta), so a non-fence sibling edit stays allowed.
    enforce_self_edit_guard(orch, request, &execution_id, agent_id, &agent_snapshot).await?;

    if dry_run {
        return Ok(format!(
            "Would edit agent '{agent_id}' snapshot in {project_key}-{number}/{exec_seq}"
        ));
    }

    crate::execution::snapshot_edit::update_execution_agent(
        orch,
        &execution_id,
        agent_id,
        agent_snapshot,
    )
    .await?;

    Ok(format!(
        "Edited agent '{agent_id}' snapshot in {project_key}-{number}/{exec_seq}"
    ))
}

/// Shallow-merge a partial agent-snapshot patch over the stored snapshot for one
/// agent, then validate the result as a full `AgentSnapshot`. The stored base is
/// taken through `config::snapshot_migrate::load` (migrate-on-read), so the patch
/// merges onto the current-form representation. When the agent does not yet exist
/// in the snapshot, the patch must itself be a complete snapshot.
async fn merge_agent_snapshot(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
    patch: serde_json::Value,
) -> Result<AgentSnapshot, String> {
    let patch = match patch {
        serde_json::Value::Object(map) => map,
        _ => return Err("payload.snapshot must be an object".to_string()),
    };

    let merged = match load_agent_snapshot_value(db, execution_id, agent_id).await? {
        Some(serde_json::Value::Object(mut base)) => {
            for (key, value) in patch {
                base.insert(key, value);
            }
            serde_json::Value::Object(base)
        }
        // New agent (or no base): the patch must itself be a complete snapshot.
        _ => serde_json::Value::Object(patch),
    };

    serde_json::from_value::<AgentSnapshot>(merged)
        .map_err(|e| format!("Invalid agent snapshot after merge: {e}"))
}

/// Load the stored snapshot for one agent as a JSON object (current-form, after
/// migrate-on-read), or `None` when the execution/agent is absent.
async fn load_agent_snapshot_value(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) -> Result<Option<serde_json::Value>, String> {
    let json = db
        .query_opt_text(
            "SELECT snapshot FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await
        .map_err(|e| format!("Failed to load execution snapshot: {e}"))?;
    let Some(json) = json else {
        return Ok(None);
    };
    let snapshot = crate::config::snapshot_migrate::load(&json)?;
    match snapshot.agents.get(agent_id) {
        Some(agent) => Ok(Some(
            serde_json::to_value(agent).map_err(|e| e.to_string())?,
        )),
        None => Ok(None),
    }
}

/// Resolve `execution_id` for `(project_key, number, exec_seq)`.
async fn resolve_execution_id(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
) -> Result<String, String> {
    let key = project_key.to_uppercase();
    let resolved = db
        .read(|conn| {
            let key = key.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT e.id
                         FROM executions e
                         JOIN issues i ON e.issue_id = i.id
                         JOIN projects p ON i.project_id = p.id
                         WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3
                         LIMIT 1",
                        params![key.as_str(), number, exec_seq],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some(row.text(0)?)),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    resolved.ok_or_else(|| format!("Execution {key}-{number}/{exec_seq} not found"))
}

/// Privilege guard for the resource edit path: an agent must not be able to edit
/// its OWN snapshot (especially `fence`), nor change the `fence` of ANY agent in
/// its OWN execution (sibling-fence weakening). Non-fence sibling edits and all
/// cross-execution edits pass. A caller with no resolvable run/job (a user-driven
/// external caller; the UI command never reaches this path) has no
/// self-execution to protect and is allowed.
async fn enforce_self_edit_guard(
    orch: &Orchestrator,
    request: &CallbackRequest,
    target_execution_id: &str,
    agent_id: &str,
    agent_snapshot: &AgentSnapshot,
) -> Result<(), String> {
    let run = match crate::mcp::handlers::run_context::lookup_run(&orch.db.local, request).await {
        Ok(run) => run,
        Err(_) => return Ok(()),
    };
    let Some((caller_execution_id, caller_agent_config_id)) =
        caller_execution_and_agent(&orch.db.local, &run.job_id).await?
    else {
        return Ok(());
    };

    // The guard only governs edits to the caller's own execution.
    if caller_execution_id != target_execution_id {
        return Ok(());
    }

    let stored_fence =
        load_stored_agent_fence(&orch.db.local, target_execution_id, agent_id).await?;
    let fence_changed = stored_fence != agent_snapshot.fence;

    // Floor: an agent may never edit its own snapshot.
    if caller_agent_config_id.as_deref() == Some(agent_id) {
        let note = if fence_changed {
            " (a fence change here would self-escalate out of the sandbox)"
        } else {
            ""
        };
        return Err(format!(
            "Refused: an agent cannot edit its own execution snapshot ('{agent_id}'){note}"
        ));
    }

    // Own-execution fence lock: block any fence change to a sibling agent in the
    // caller's own execution.
    if fence_changed {
        return Err(format!(
            "Refused: an agent cannot change the fence of any agent ('{agent_id}') in its own execution"
        ));
    }

    Ok(())
}

/// Resolve the caller's `(execution_id, agent_config_id)` from its job row.
/// Returns `None` when the job has no execution (project-level run) or no row.
async fn caller_execution_and_agent(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<(String, Option<String>)>, String> {
    if job_id.is_empty() {
        return Ok(None);
    }
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT execution_id, agent_config_id FROM jobs WHERE id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => {
                    let exec = row.opt_text(0)?;
                    let agent = row.opt_text(1)?;
                    Ok::<_, DbError>(exec.map(|e| (e, agent)))
                }
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Load the stored `fence` for one agent in an execution snapshot (migrate-on-read
/// applied), so a fence delta can be detected against the incoming edit.
async fn load_stored_agent_fence(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) -> Result<Option<crate::models::Fence>, String> {
    let json = db
        .query_opt_text(
            "SELECT snapshot FROM executions WHERE id = ?1",
            params![execution_id],
        )
        .await
        .map_err(|e| format!("Failed to load execution snapshot: {e}"))?;
    let Some(json) = json else {
        return Ok(None);
    };
    let snapshot = crate::config::snapshot_migrate::load(&json)?;
    Ok(snapshot.agents.get(agent_id).and_then(|a| a.fence))
}

#[cfg(test)]
mod snapshot_edit_guard_tests {
    use super::*;
    use crate::db::DbState;
    use crate::models::{
        Fence, Model, ModelSelection, RecipeSnapshot, RecipeTrigger, TriggerContext, TriggerType,
    };
    use crate::orchestrator::{Orchestrator, OrchestratorBuilder};
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::collections::HashMap;
    use std::sync::Arc;

    async fn exec(db: &LocalDb, sql: &'static str) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn seeded_orch() -> Orchestrator {
        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    fn agent(id: &str, prompt: &str, fence: Fence) -> serde_json::Value {
        serde_json::to_value(agent_struct(id, prompt, fence)).unwrap()
    }

    fn agent_struct(id: &str, prompt: &str, fence: Fence) -> AgentSnapshot {
        AgentSnapshot {
            id: id.to_string(),
            name: id.to_string(),
            description: String::new(),
            prompt: prompt.to_string(),
            tools: vec![],
            tier: None,
            backend_preference: None,
            // Concrete selection so the canonical pipeline stores verbatim and
            // skips preset resolution (no config files needed in tests).
            selection: Some(ModelSelection {
                backend: "claude".to_string(),
                model: Model::new(Model::SONNET),
            }),
            disallowed_tools: None,
            skills: None,
            fence: Some(fence),
            sandbox: None,
            on_escape: None,
            model: None,
            resolved_backend: None,
            extras: None,
        }
    }

    fn snapshot_json() -> String {
        let recipe = RecipeSnapshot {
            id: "r".to_string(),
            name: "R".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![],
            edges: vec![],
        };
        let mut agents = HashMap::new();
        agents.insert(
            "builder".to_string(),
            agent_struct("builder", "prompt for builder", Fence::Ask),
        );
        agents.insert(
            "planner".to_string(),
            agent_struct("planner", "prompt for planner", Fence::Ask),
        );
        let snap = ExecutionSnapshot::new(
            recipe,
            agents,
            HashMap::new(),
            TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        );
        snap.to_json().unwrap()
    }

    async fn set_snapshot(db: &LocalDb, exec_id: &str, json: &str) {
        let exec_id = exec_id.to_string();
        let json = json.to_string();
        db.write(|conn| {
            let exec_id = exec_id.clone();
            let json = json.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                    params![json.as_str(), exec_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Project CAIRN / issue 1 with two executions. exec-1 (seq 1) holds the
    /// edited snapshot and two running, started agent jobs (builder, planner)
    /// with their runs. exec-2 (seq 2) is an unrelated execution whose run lets
    /// us exercise the cross-execution (allowed) path.
    async fn seed(orch: &Orchestrator) {
        let db = &orch.db.local;
        exec(
            db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1','default','Cairn','CAIRN','/tmp/repo',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1','proj-1',1,'T','active',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1','r','issue-1','proj-1','running',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-2','r','issue-1','proj-1','running',1,2)",
        )
        .await;
        exec(
            db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, agent_config_id, status, started_at, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-builder','exec-1','issue-1','proj-1','Builder','builder','running',1,1,1,'builder','/tmp/repo-builder')",
        )
        .await;
        exec(
            db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, agent_config_id, status, started_at, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-planner','exec-1','issue-1','proj-1','Planner','planner','running',1,1,1,'planner','/tmp/repo-planner')",
        )
        .await;
        exec(
            db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, agent_config_id, status, started_at, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-other','exec-2','issue-1','proj-1','Builder','builder','running',1,1,1,'builder','/tmp/repo-other')",
        )
        .await;
        exec(
            db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-builder','job-builder','issue-1',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-planner','job-planner','issue-1',1,1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-other','job-other','issue-1',1,1)",
        )
        .await;
        let json = snapshot_json();
        set_snapshot(db, "exec-1", &json).await;
        set_snapshot(db, "exec-2", &json).await;
    }

    fn request(run_id: Option<&str>) -> CallbackRequest {
        CallbackRequest {
            thread_id: None,
            cwd: "/tmp/no-such-worktree".to_string(),
            run_id: run_id.map(str::to_string),
            tool: "change".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        }
    }

    async fn load_snapshot(db: &LocalDb, exec_id: &str) -> ExecutionSnapshot {
        let json = db
            .query_opt_text(
                "SELECT snapshot FROM executions WHERE id = ?1",
                params![exec_id],
            )
            .await
            .unwrap()
            .unwrap();
        crate::config::snapshot_migrate::load(&json).unwrap()
    }

    async fn needs_fresh_session(db: &LocalDb, job_id: &str) -> bool {
        let job_id = job_id.to_string();
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT needs_fresh_session FROM jobs WHERE id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                Ok(row.i64(0)? != 0)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn self_edit_is_rejected() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // builder's own run editing builder's own snapshot — even with no fence
        // delta — is refused outright.
        let err = edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "builder",
            agent("builder", "prompt for builder", Fence::Ask),
            false,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("cannot edit its own execution snapshot"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn self_edit_fence_change_is_called_out() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        let err = edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "builder",
            agent("builder", "prompt for builder", Fence::Allow),
            false,
        )
        .await
        .unwrap_err();
        assert!(err.contains("self-escalate"), "got: {err}");
        // The stored snapshot is untouched (fence stays Ask).
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        assert_eq!(snap.agents["builder"].fence, Some(Fence::Ask));
    }

    #[tokio::test]
    async fn own_exec_sibling_fence_change_is_rejected() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // builder editing sibling planner's fence within its own execution —
        // blocked (sibling-fence weakening).
        let err = edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "planner",
            agent("planner", "prompt for planner", Fence::Allow),
            false,
        )
        .await
        .unwrap_err();
        assert!(err.contains("cannot change the fence"), "got: {err}");
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        assert_eq!(snap.agents["planner"].fence, Some(Fence::Ask));
    }

    #[tokio::test]
    async fn own_exec_sibling_non_fence_edit_is_allowed_and_marks_fresh_session() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // builder editing planner's prompt (fence unchanged) within its own
        // execution is allowed; the prompt change flags planner's started job
        // for a fresh session.
        edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "planner",
            agent("planner", "new planner prompt", Fence::Ask),
            false,
        )
        .await
        .unwrap();
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        assert_eq!(snap.agents["planner"].prompt, "new planner prompt");
        assert!(needs_fresh_session(&orch.db.local, "job-planner").await);
    }

    #[tokio::test]
    async fn cross_execution_edit_is_allowed() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // A caller in exec-2 editing builder's fence in exec-1 is a different
        // execution — allowed.
        edit_execution_agent(
            &orch,
            &request(Some("run-other")),
            "CAIRN",
            1,
            1,
            "builder",
            agent("builder", "prompt for builder", Fence::Allow),
            false,
        )
        .await
        .unwrap();
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        assert_eq!(snap.agents["builder"].fence, Some(Fence::Allow));
    }

    #[tokio::test]
    async fn user_caller_without_run_is_allowed() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // No resolvable run (user-driven / external caller): no self-execution to
        // protect, so even a self-named fence change goes through.
        edit_execution_agent(
            &orch,
            &request(None),
            "CAIRN",
            1,
            1,
            "builder",
            agent("builder", "prompt for builder", Fence::Allow),
            false,
        )
        .await
        .unwrap();
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        assert_eq!(snap.agents["builder"].fence, Some(Fence::Allow));
    }

    #[tokio::test]
    async fn dry_run_does_not_mutate() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        let summary = edit_execution_agent(
            &orch,
            &request(None),
            "CAIRN",
            1,
            1,
            "builder",
            agent("builder", "changed", Fence::Allow),
            true,
        )
        .await
        .unwrap();
        assert!(summary.starts_with("Would edit"), "got: {summary}");
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        assert_eq!(snap.agents["builder"].prompt, "prompt for builder");
        assert_eq!(snap.agents["builder"].fence, Some(Fence::Ask));
    }

    #[tokio::test]
    async fn partial_self_edit_is_rejected() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // A partial prompt-only patch on the caller's own agent is still blocked
        // by the floor (independent of any fence delta).
        let err = edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "builder",
            serde_json::json!({"prompt": "x"}),
            false,
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("cannot edit its own execution snapshot"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn partial_sibling_fence_change_is_rejected() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // A fence-only partial patch on a sibling: the delta is computed from the
        // merge, so the own-execution fence lock still fires.
        let err = edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "planner",
            serde_json::json!({"fence": "allow"}),
            false,
        )
        .await
        .unwrap_err();
        assert!(err.contains("cannot change the fence"), "got: {err}");
    }

    #[tokio::test]
    async fn partial_sibling_prompt_edit_merges_and_preserves_fields() {
        let orch = seeded_orch().await;
        seed(&orch).await;
        // A prompt-only partial patch on a sibling: fence omitted means unchanged
        // (no delta, allowed), and untouched fields survive the merge.
        edit_execution_agent(
            &orch,
            &request(Some("run-builder")),
            "CAIRN",
            1,
            1,
            "planner",
            serde_json::json!({"prompt": "merged prompt"}),
            false,
        )
        .await
        .unwrap();
        let snap = load_snapshot(&orch.db.local, "exec-1").await;
        let planner = &snap.agents["planner"];
        assert_eq!(planner.prompt, "merged prompt");
        // Omitted fields are preserved from the stored snapshot.
        assert_eq!(planner.fence, Some(Fence::Ask));
        let selection = planner.selection.as_ref().unwrap();
        assert_eq!(selection.backend, "claude");
        assert_eq!(selection.model.as_str(), "sonnet");
        // The prompt change still flags the started job for a fresh session.
        assert!(needs_fresh_session(&orch.db.local, "job-planner").await);
    }
}
