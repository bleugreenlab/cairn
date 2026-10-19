//! Canonical execution-snapshot agent-edit pipeline.
//!
//! Both the Tauri `update_execution_agent` command and the
//! `cairn://p/PROJECT/NUMBER/executions/{seq}` resource patch call
//! [`update_execution_agent`], so a URI edit and a UI edit are byte-for-byte the
//! same operation: load the snapshot, resolve-early normalize the agent against
//! current presets, write it back (plus the model on non-terminal jobs),
//! and propagate fence/model to any live session. The honest timing model: fence
//! reaches a live session immediately, model and prompt on the next turn.
//!
//! An edited prompt needs nothing propagated. Every backend reassembles the whole
//! instruction stack on every process spawn, resume included — Claude writes it to
//! the file it passes as `--system-prompt-file` next to `--resume`, Codex puts it
//! in `thread/resume`'s own `baseInstructions` — so the next turn of the existing
//! session runs under the edit. This used to flag those jobs to rotate to a fresh
//! session instead, which discarded a live conversation to deliver something the
//! resume was already carrying (CAIRN-4618).

use std::path::PathBuf;

use cairn_db::turso::params;

use crate::config::agents::FileAgent;
use crate::config::presets::{load_effective_presets, resolve_agent_snapshot};
use crate::models::AgentSnapshot;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Apply an edited agent snapshot to a stored execution snapshot and propagate
/// the change. The single source of truth shared by the Tauri command and the
/// execution-snapshot resource patch.
pub async fn update_execution_agent(
    orch: &Orchestrator,
    execution_id: &str,
    agent_id: &str,
    agent_snapshot: AgentSnapshot,
) -> Result<(), String> {
    let owning = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id).await?;
    let db: &LocalDb = owning.as_ref();

    // Serialize this snapshot read-modify-write with every other writer of the
    // execution's snapshot: delegated-packet persistence and settlement, and
    // advancement's DAG reduction. Held through the write only — provider
    // changes and live-session propagation below run outside it.
    let snapshot_lock = orch.execution_lock(execution_id);
    let snapshot_guard = snapshot_lock.lock().await;

    let snapshot_json = load_execution_snapshot_json(db, execution_id)
        .await?
        .ok_or("Execution has no snapshot")?;

    let mut snapshot = crate::config::snapshot_migrate::load(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {e}"))?;
    let project_path = project_repo_path(db, &snapshot.trigger_context.project_id).await?;

    // Resolve-early against current presets. Composer output that already
    // carries a concrete selection is stored verbatim; otherwise resolve
    // once (loud).
    let mut normalized_snapshot = if agent_snapshot.selection.is_some() {
        agent_snapshot.clone()
    } else {
        let presets = load_effective_presets(&orch.config_dir, project_path.as_deref());
        resolve_agent_snapshot(
            &FileAgent {
                id: agent_snapshot.id.clone(),
                name: agent_snapshot.name.clone(),
                description: agent_snapshot.description.clone(),
                prompt: agent_snapshot.prompt.clone(),
                tools: agent_snapshot.tools.clone(),
                tier: agent_snapshot
                    .tier
                    .clone()
                    .or_else(|| agent_snapshot.selection.as_ref().map(|s| s.model.clone())),
                fence: agent_snapshot.fence,
                disallowed_tools: agent_snapshot.disallowed_tools.clone(),
                skills: agent_snapshot.skills.clone(),
                hooks: None,
                backend_preference: agent_snapshot.backend_preference.clone(),
                icon: None,
                bundles: Vec::new(),
                is_project_scoped: project_path.is_some(),
                file_path: PathBuf::new(),
                seed_budget_tokens: agent_snapshot.seed_budget_tokens,
                seed_eras: agent_snapshot.seed_eras,
            },
            None,
            &presets,
        )?
    };
    // An agent definition carries no account policy, so re-resolution must not
    // be allowed to drop the one the edit chose.
    normalized_snapshot.provider_account = agent_snapshot.provider_account.clone();

    // This is the one door an agent snapshot is edited through, so it is the one
    // place that can honestly say an edit happened. The stamp is what tells a
    // long-lived host (a thread session) that this snapshot is now customized
    // and must stop being re-resolved from its authored definition each turn.
    // Set server-side from the edit itself, never from the incoming payload:
    // `merge_agent_patch` strips the field so a caller cannot assert it.
    normalized_snapshot.edited_at = Some(chrono::Utc::now().timestamp());

    snapshot
        .agents
        .insert(agent_id.to_string(), normalized_snapshot.clone());
    let updated_json = snapshot.to_json()?;
    let model = normalized_snapshot
        .selection
        .as_ref()
        .map(|s| s.model.to_string());

    let job_ids = load_agent_job_ids(db, execution_id, agent_id).await?;

    update_agent_snapshot(db, execution_id, &updated_json).await?;
    drop(snapshot_guard);

    if let Some(selection) = normalized_snapshot.selection.as_ref() {
        for job_id in job_ids {
            crate::execution::provider_change::apply_for_job(
                orch,
                db,
                &job_id,
                &selection.backend,
                model.as_deref(),
            )
            .await?;
        }
    }

    propagate_agent_changes_to_processes(orch, execution_id, agent_id, &normalized_snapshot).await;

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "executions", "action": "update"}),
    );

    Ok(())
}

/// The jobs an edit of this agent applies to: every job still working, plus any
/// job whose current session is still open. A job can reach a terminal status
/// while its session stays warm and resumable, and that session must launch
/// under the edited model rather than the one it was created with.
async fn load_agent_job_ids(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) -> Result<Vec<String>, String> {
    let execution_id = execution_id.to_string();
    let agent_id = agent_id.to_string();
    let jobs: Vec<(String, String, Option<String>)> = db
        .read(|conn| {
            let execution_id = execution_id.clone();
            let agent_id = agent_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, status, current_session_id FROM jobs
                             WHERE execution_id = ?1 AND agent_config_id = ?2",
                        params![execution_id.as_str(), agent_id.as_str()],
                    )
                    .await?;
                let mut result = Vec::new();
                while let Some(row) = rows.next().await? {
                    result.push((row.text(0)?, row.text(1)?, row.opt_text(2)?));
                }
                Ok(result)
            })
        })
        .await
        .map_err(|error| format!("Failed to load jobs for provider change: {error}"))?;

    let mut applicable = Vec::new();
    for (job_id, status, session_id) in jobs {
        if status != "complete" && status != "failed" {
            applicable.push(job_id);
            continue;
        }
        let Some(session_id) = session_id else {
            continue;
        };
        let session_status = db
            .query_text("SELECT status FROM sessions WHERE id = ?1", (session_id,))
            .await
            .map_err(|error| format!("Failed to read a job's session status: {error}"))?;
        if session_status.as_deref() == Some("open") {
            applicable.push(job_id);
        }
    }
    Ok(applicable)
}

/// Push fence/model changes to any live session running this agent. Fence lands
/// immediately (`set_permission_mode`); model rotates onto the next turn
/// (`set_model`). Best-effort: a missing or dead session is simply skipped.
async fn propagate_agent_changes_to_processes(
    orch: &Orchestrator,
    execution_id: &str,
    agent_id: &str,
    agent_snapshot: &AgentSnapshot,
) {
    if agent_snapshot.selection.is_none() && agent_snapshot.fence.is_none() {
        return;
    }

    let owning = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id)
        .await
        .unwrap_or_else(|_| orch.db.local.clone());
    let job_sessions = match load_agent_job_sessions(&owning, execution_id, agent_id).await {
        Ok(rows) => rows,
        Err(e) => {
            log::warn!("Failed to query jobs for agent propagation: {e}");
            return;
        }
    };
    let process_state = &orch.process_state;

    for (job_id, session_id) in job_sessions {
        let Some(session_id) = session_id else {
            continue;
        };
        let Some(run_id) = process_state.find_process_by_session(&session_id) else {
            continue;
        };

        if let Some(selection) = &agent_snapshot.selection {
            let model_str = selection.model.to_string();
            if let Err(e) =
                crate::backends::stdin::send_set_model(process_state, &run_id, &model_str)
            {
                log::warn!(
                    "Failed to propagate model to job {}: {}",
                    &job_id[..job_id.len().min(8)],
                    e
                );
            }
        }

        let perms =
            crate::backends::AgentPermissions::new(agent_snapshot.fence.unwrap_or_default());
        let mode = perms.to_legacy_str();
        if let Err(e) =
            crate::backends::stdin::send_set_permission_mode(process_state, &run_id, mode)
        {
            log::warn!(
                "Failed to propagate permissions to job {}: {}",
                &job_id[..job_id.len().min(8)],
                e
            );
        }
    }
}

async fn load_execution_snapshot_json(
    db: &LocalDb,
    execution_id: &str,
) -> Result<Option<String>, String> {
    db.query_opt_text(
        "SELECT snapshot FROM executions WHERE id = ?1",
        params![execution_id],
    )
    .await
    .map_err(|e| format!("Failed to load execution: {e}"))
}

async fn project_repo_path(db: &LocalDb, project_id: &str) -> Result<Option<PathBuf>, String> {
    db.query_opt_text(
        "SELECT repo_path FROM projects WHERE id = ?1",
        params![project_id],
    )
    .await
    .map(|path| path.map(PathBuf::from))
    .map_err(|e| format!("Failed to load project path: {e}"))
}

/// Persist the edited snapshot itself.
///
/// The per-job model deliberately does NOT travel with this write. Updating
/// every job's model up front and rotating afterwards is what let a snapshot
/// edit strand a prefix of its jobs on the new model while their sessions still
/// ran the old provider; each job's model now commits with that job's own
/// transition, so a failure part-way leaves the remaining jobs untouched rather
/// than half-changed.
async fn update_agent_snapshot(
    db: &LocalDb,
    execution_id: &str,
    snapshot_json: &str,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let snapshot_json = snapshot_json.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        let snapshot_json = snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                params![snapshot_json.as_str(), execution_id.as_str()],
            )
            .await?;

            Ok(())
        })
    })
    .await
    .map_err(|e| format!("Failed to update snapshot: {e}"))
}

async fn load_agent_job_sessions(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) -> Result<Vec<(String, Option<String>)>, String> {
    let execution_id = execution_id.to_string();
    let agent_id = agent_id.to_string();
    db.read(|conn| {
        let execution_id = execution_id.clone();
        let agent_id = agent_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, current_session_id
                     FROM jobs
                     WHERE execution_id = ?1
                       AND agent_config_id = ?2
                       AND status NOT IN ('complete', 'failed')",
                    params![execution_id.as_str(), agent_id.as_str()],
                )
                .await?;
            let mut jobs = Vec::new();
            while let Some(row) = rows.next().await? {
                jobs.push((row.text(0)?, row.opt_text(1)?));
            }
            Ok(jobs)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::models::{
        ExecutionSnapshot, Fence, Model, ModelSelection, RecipeSnapshot, RecipeTrigger,
        TriggerContext, TriggerType,
    };
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::collections::HashMap;
    use std::sync::Arc;

    async fn orchestrator() -> Orchestrator {
        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    fn builder(model: &str) -> AgentSnapshot {
        AgentSnapshot {
            edited_at: None,
            id: "builder".to_string(),
            name: "builder".to_string(),
            description: String::new(),
            prompt: "prompt".to_string(),
            tools: vec![],
            tier: None,
            backend_preference: None,
            selection: Some(ModelSelection {
                backend: "claude".to_string(),
                model: Model::new(model),
            }),
            provider_account: None,
            disallowed_tools: None,
            skills: None,
            fence: Some(Fence::Ask),
            sandbox: None,
            on_escape: None,
            model: None,
            resolved_backend: None,
            extras: None,
            seed_budget_tokens: None,
            seed_eras: None,
        }
    }

    fn snapshot_json(agent: AgentSnapshot) -> String {
        let recipe = RecipeSnapshot {
            id: "r".to_string(),
            name: "R".to_string(),
            description: None,
            trigger: RecipeTrigger::Manual,
            nodes: vec![],
            edges: vec![],
        };
        ExecutionSnapshot::new(
            recipe,
            HashMap::from([("builder".to_string(), agent)]),
            HashMap::new(),
            TriggerContext {
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
                trigger_type: TriggerType::Manual,
                event_payload: None,
                initiated_via: None,
            },
        )
        .to_json()
        .unwrap()
    }

    /// A job that has not opened a session yet carries a NULL
    /// `current_session_id`. A model-carrying edit must treat that as "no session
    /// to rotate": write the job's model and succeed. Reading the column as
    /// required TEXT failed the edit after the snapshot had already been written.
    #[tokio::test]
    async fn a_model_edit_lands_on_a_job_that_has_no_session_yet() {
        let orch = orchestrator().await;
        let db = &orch.db.local;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj-1','default','Cairn','cairn','/tmp/repo',1,1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1','proj-1',1,'T','active',1,1);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES ('exec-1','r','issue-1','proj-1','running',1,1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, agent_config_id,
                               status, model, current_session_id, created_at, updated_at, uri_segment)
               VALUES ('job-1','exec-1','issue-1','proj-1','Builder','builder',
                       'pending','claude-old',NULL,1,1,'builder');",
        )
        .await
        .unwrap();
        db.execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = 'exec-1'",
            params![snapshot_json(builder("claude-old"))],
        )
        .await
        .unwrap();

        update_execution_agent(&orch, "exec-1", "builder", builder(Model::SONNET))
            .await
            .expect("a model edit to a session-less job succeeds");

        let stored = db
            .query_opt_text("SELECT snapshot FROM executions WHERE id = 'exec-1'", ())
            .await
            .unwrap()
            .unwrap();
        let stored = crate::config::snapshot_migrate::load(&stored).unwrap();
        let edited = &stored.agents["builder"];
        assert_eq!(
            edited.selection.as_ref().map(|s| s.model.to_string()),
            Some(Model::new(Model::SONNET).to_string()),
            "the snapshot carries the edited model"
        );
        assert!(edited.edited_at.is_some(), "the edit is stamped");
        assert_eq!(
            db.query_opt_text("SELECT model FROM jobs WHERE id = 'job-1'", ())
                .await
                .unwrap(),
            Some(Model::new(Model::SONNET).to_string()),
            "the job's model follows the edit"
        );
        assert_eq!(
            db.query_opt_text("SELECT current_session_id FROM jobs WHERE id = 'job-1'", ())
                .await
                .unwrap(),
            None,
            "no session is invented for a job that had none"
        );
    }
}
