//! Canonical execution-snapshot agent-edit pipeline.
//!
//! Both the Tauri `update_execution_agent` command and the
//! `cairn://p/PROJECT/NUMBER/executions/{seq}` resource patch call
//! [`update_execution_agent`], so a URI edit and a UI edit are byte-for-byte the
//! same operation: load the snapshot, resolve-early normalize the agent against
//! current presets, write it back (plus the model on non-terminal jobs),
//! propagate fence/model to any live session, and flag started jobs for a fresh
//! session when the prompt changed. The honest timing model: fence reaches a
//! live session immediately, model on the next turn, prompt on the next session.

use std::path::PathBuf;

use turso::params;

use crate::config::agents::FileAgent;
use crate::config::presets::{load_effective_presets, resolve_agent_snapshot};
use crate::models::{AgentSnapshot, ExecutionSnapshot};
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

    let snapshot_json = load_execution_snapshot_json(db, execution_id)
        .await?
        .ok_or("Execution has no snapshot")?;

    let mut snapshot = ExecutionSnapshot::from_json(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {e}"))?;
    let project_path = project_repo_path(db, &snapshot.trigger_context.project_id).await?;

    // Resolve-early against current presets. Composer output that already
    // carries a concrete selection is stored verbatim; otherwise resolve
    // once (loud).
    let normalized_snapshot = if agent_snapshot.selection.is_some() {
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
                is_project_scoped: project_path.is_some(),
                file_path: PathBuf::new(),
            },
            None,
            &presets,
        )?
    };

    // Detect a prompt edit before overwriting the agent. The system prompt is
    // fixed at session spawn, so a prompt change can't reach the live session
    // in place; flag affected started jobs so the continue path rotates them
    // to a fresh session on their next turn.
    let prompt_changed = snapshot
        .agents
        .get(agent_id)
        .map(|old| old.prompt != normalized_snapshot.prompt)
        .unwrap_or(false);

    snapshot
        .agents
        .insert(agent_id.to_string(), normalized_snapshot.clone());
    let updated_json = snapshot.to_json()?;
    let model = normalized_snapshot
        .selection
        .as_ref()
        .map(|s| s.model.to_string());

    update_agent_snapshot(db, execution_id, agent_id, &updated_json, model.as_deref()).await?;

    propagate_agent_changes_to_processes(orch, execution_id, agent_id, &normalized_snapshot).await;

    if prompt_changed {
        mark_agent_jobs_need_fresh_session(db, execution_id, agent_id).await?;
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "executions", "action": "update"}),
    );

    Ok(())
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

async fn update_agent_snapshot(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
    snapshot_json: &str,
    model: Option<&str>,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let agent_id = agent_id.to_string();
    let snapshot_json = snapshot_json.to_string();
    let model = model.map(|s| s.to_string());
    db.write(|conn| {
        let execution_id = execution_id.clone();
        let agent_id = agent_id.clone();
        let snapshot_json = snapshot_json.clone();
        let model = model.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                params![snapshot_json.as_str(), execution_id.as_str()],
            )
            .await?;

            if let Some(model) = model {
                let now = chrono::Utc::now().timestamp() as i32;
                conn.execute(
                    "UPDATE jobs
                     SET model = ?1, updated_at = ?2
                     WHERE execution_id = ?3
                       AND agent_config_id = ?4
                       AND status NOT IN ('complete', 'failed')",
                    params![
                        model.as_str(),
                        now,
                        execution_id.as_str(),
                        agent_id.as_str()
                    ],
                )
                .await?;
            }

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

/// Flag the started, non-terminal jobs running an agent so their next turn
/// rotates to a fresh session (applying an edited system prompt). Never-started
/// jobs are skipped: they build the current prompt on first start.
async fn mark_agent_jobs_need_fresh_session(
    db: &LocalDb,
    execution_id: &str,
    agent_id: &str,
) -> Result<(), String> {
    db.execute(
        "UPDATE jobs SET needs_fresh_session = 1
         WHERE execution_id = ?1 AND agent_config_id = ?2
           AND started_at IS NOT NULL
           AND status NOT IN ('complete', 'failed', 'cancelled')",
        params![execution_id, agent_id],
    )
    .await
    .map(|_| ())
    .map_err(|e| format!("Failed to flag jobs for fresh session: {e}"))
}
