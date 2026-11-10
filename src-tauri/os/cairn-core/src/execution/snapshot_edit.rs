//! Canonical execution-snapshot agent-edit pipeline.
//!
//! Both the Tauri `update_execution_agent` command and the
//! `cairn://p/PROJECT/NUMBER/executions/{seq}` resource patch call
//! [`update_execution_agent`], so a URI edit and a UI edit are byte-for-byte the
//! same operation: load the snapshot, store the edited agent, write it back
//! (plus a pinned model on non-terminal jobs),
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

use cairn_db::turso::params;

use crate::models::AgentSnapshot;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Apply an edited agent snapshot to a stored execution snapshot and propagate
/// the change. The single source of truth shared by the Tauri command and the
/// execution-snapshot resource patch.
///
/// An edit that changes anything is recorded in `execution_agent_edits`, in
/// the same write as the snapshot, so a live override of a step's allotment is
/// a fact the recipe-resolution records can be joined to. `actor` is the
/// editing node or thread, `operator` for the app, `None` for the CLI.
pub async fn update_execution_agent(
    orch: &Orchestrator,
    execution_id: &str,
    agent_id: &str,
    agent_snapshot: AgentSnapshot,
    actor: Option<&str>,
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

    // The edit is stored as made: a pin runs verbatim on the agent's jobs, and
    // an allotment is what a node that has not launched yet will select
    // within. A job that already launched keeps the selection it stored.
    let mut normalized_snapshot = agent_snapshot;

    // A pin the edit changes is stored as the catalog spells it, which is what
    // its jobs will be launched with. One the edit keeps is not a model edit,
    // and is not resolved again: a prompt edit must not be refused because the
    // agent's provider has since stopped listing its model.
    let stored_selection = snapshot
        .agents
        .get(agent_id)
        .and_then(|agent| agent.selection.as_ref());
    let edited_pin = normalized_snapshot
        .selection
        .as_ref()
        .filter(|selection| stored_selection != Some(*selection));
    if let Some(selection) = edited_pin {
        let resolved = crate::execution::provider_change::resolve_model_edit(
            orch,
            Some(snapshot.trigger_context.project_id.as_str()),
            Some(selection.backend.as_str()),
            selection.model.as_str(),
        )?;
        normalized_snapshot.selection = Some(resolved);
    }

    // This is the one door an agent snapshot is edited through, so it is the one
    // place that can honestly say an edit happened. The stamp is what tells a
    // long-lived host (a thread session) that this snapshot is now customized
    // and must stop being re-resolved from its authored definition each turn.
    // Set server-side from the edit itself, never from the incoming payload:
    // `merge_agent_patch` strips the field so a caller cannot assert it.
    normalized_snapshot.edited_at = Some(chrono::Utc::now().timestamp());

    let edit = AgentEdit::between(
        execution_id,
        agent_id,
        snapshot.agents.get(agent_id),
        &normalized_snapshot,
        actor,
    );
    snapshot
        .agents
        .insert(agent_id.to_string(), normalized_snapshot.clone());
    let updated_json = snapshot.to_json()?;
    let model = normalized_snapshot
        .selection
        .as_ref()
        .map(|s| s.model.to_string());

    let job_ids = load_agent_job_ids(db, execution_id, agent_id).await?;

    update_agent_snapshot(db, execution_id, &updated_json, edit).await?;
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

    let change = crate::notify::execution_db_change_for_id(db, execution_id, "update").await;
    let _ = orch.services.emitter.emit("db-change", change);

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

/// One recorded edit: which snapshot keys changed, and the agent's allotment
/// and pin before and after.
#[derive(Clone)]
struct AgentEdit {
    id: String,
    execution_id: String,
    agent_id: String,
    actor: Option<String>,
    changed: String,
    before: String,
    after: String,
}

impl AgentEdit {
    /// The edit from `before` to `after`, or `None` when nothing changed.
    fn between(
        execution_id: &str,
        agent_id: &str,
        before: Option<&AgentSnapshot>,
        after: &AgentSnapshot,
        actor: Option<&str>,
    ) -> Option<Self> {
        let changed = changed_keys(before, after);
        if changed.is_empty() {
            return None;
        }
        let models = |agent: Option<&AgentSnapshot>| {
            serde_json::json!({
                "models": agent.map(|agent| &agent.models),
                "selection": agent.and_then(|agent| agent.selection.as_ref()),
            })
            .to_string()
        };
        Some(Self {
            id: cairn_common::ids::mint_child(execution_id),
            execution_id: execution_id.to_string(),
            agent_id: agent_id.to_string(),
            actor: actor.map(str::to_string),
            changed: serde_json::to_string(&changed).unwrap_or_else(|_| "[]".to_string()),
            before: models(before),
            after: models(Some(after)),
        })
    }
}

/// The snapshot keys whose values differ, the server's edit stamp aside.
fn changed_keys(before: Option<&AgentSnapshot>, after: &AgentSnapshot) -> Vec<String> {
    let object = |agent: Option<&AgentSnapshot>| {
        agent
            .and_then(|agent| serde_json::to_value(agent).ok())
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default()
    };
    let (before, after) = (object(before), object(Some(after)));
    let keys: std::collections::BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    keys.into_iter()
        .filter(|key| key.as_str() != "editedAt" && before.get(*key) != after.get(*key))
        .cloned()
        .collect()
}

/// Persist the edited snapshot itself, and the record of the edit.
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
    edit: Option<AgentEdit>,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let snapshot_json = snapshot_json.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        let snapshot_json = snapshot_json.clone();
        let edit = edit.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                params![snapshot_json.as_str(), execution_id.as_str()],
            )
            .await?;
            if let Some(edit) = edit {
                conn.execute(
                    "INSERT INTO execution_agent_edits
                       (id, execution_id, agent_id, created_at, actor, changed, before, after)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        edit.id.as_str(),
                        edit.execution_id.as_str(),
                        edit.agent_id.as_str(),
                        chrono::Utc::now().timestamp(),
                        edit.actor.as_deref(),
                        edit.changed.as_str(),
                        edit.before.as_str(),
                        edit.after.as_str()
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
