use super::*;

/// Load agent config for a job — tries execution snapshot first, falls back to
/// config files — with the model the job launched on.
///
/// A pinned agent carries its pin. Otherwise the model is the selection the
/// job stored at its launch (`jobs.selection`), with its effort: every turn of
/// a job runs on the one choice it made, never a fresh one.
pub(super) fn load_agent_config(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: Option<&Path>,
) -> Result<Option<AgentConfig>, String> {
    let Some(mut agent) = load_agent_definition(orch, job, project_path)? else {
        return Ok(None);
    };
    // The stored selection supplies the model when the agent names none, and
    // its effort and options when the agent's model is the one it chose (a
    // host snapshot derives its selection from `jobs.model`, which carries no
    // effort of its own).
    if let Some(record) = orch.job_selection(&job.id)? {
        match &agent.selection {
            None => {
                agent.selection = Some(record.chosen);
                agent.extras = Some(record.extras);
            }
            Some(selection) if selection.model == record.chosen.model => {
                agent.selection = Some(record.chosen);
                if agent.extras.is_none() {
                    agent.extras = Some(record.extras);
                }
            }
            Some(_) => {}
        }
    }
    Ok(Some(agent))
}

fn load_agent_definition(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: Option<&Path>,
) -> Result<Option<AgentConfig>, String> {
    let Some(aid) = &job.agent_config_id else {
        return Ok(None);
    };

    // Try snapshot first (ensures reproducibility for execution-based jobs)
    let mut snapshot_models = None;
    if let Some(exec_id) = &job.execution_id {
        log::info!(
            "load_agent_config: job {} has execution_id={}, trying snapshot for agent '{}'",
            job.id,
            exec_id,
            aid
        );
        let exec_db = run_db({
            let dbs = orch.db.clone();
            let exec_id = exec_id.clone();
            async move {
                crate::execution::routing::owning_db_for_execution(&dbs, &exec_id)
                    .await
                    .map_err(|e| e.to_string())
            }
        })?;
        let snapshot_data = run_db(load_agent_snapshot_data(
            exec_db,
            exec_id.clone(),
            aid.clone(),
        ))?;
        if let Some(agent) = snapshot_data.agent {
            if !agent.prompt.is_empty() {
                log::info!(
                    "load_agent_config: loaded agent '{}' from snapshot (prompt len={}, tools={:?})",
                    aid, agent.prompt.len(), agent.tools
                );
                return Ok(Some(agent));
            }
            // Snapshot agent has empty prompt — likely a placeholder from executor
            // expansion. Fall through to config files, but keep its allotment.
            snapshot_models = Some(agent.models.clone()).filter(|models| !models.is_empty());
            log::info!(
                "load_agent_config: snapshot agent '{}' has empty prompt, falling through to config files",
                aid
            );
        } else {
            log::info!(
                "load_agent_config: agent '{}' not found in snapshot for execution {}",
                aid,
                exec_id
            );
        }
    } else {
        log::info!(
            "load_agent_config: job {} has no execution_id, using file-based config for '{}'",
            job.id,
            aid
        );
    }

    // Fall back to config files.
    let project_id = &job.project_id;
    let agent = config::get_config_dir().ok().and_then(|cd| {
        config_agents::get_agent(&cd, aid, project_path)
            .ok()
            .flatten()
            .map(|fa| {
                let snapshot = fa.snapshot();
                AgentConfig {
                    id: snapshot.id,
                    name: snapshot.name,
                    description: snapshot.description,
                    prompt: snapshot.prompt,
                    tools: snapshot.tools,
                    models: snapshot_models.clone().unwrap_or(snapshot.models),
                    workspace_id: if fa.is_project_scoped {
                        None
                    } else {
                        Some("workspace".to_string())
                    },
                    project_id: if fa.is_project_scoped {
                        Some(project_id.clone())
                    } else {
                        None
                    },
                    created_at: 0,
                    updated_at: 0,
                    disallowed_tools: snapshot.disallowed_tools,
                    skills: snapshot.skills,
                    fence: snapshot.fence,
                    icon: fa.icon.clone(),
                    selection: snapshot.selection,
                    extras: snapshot.extras,
                }
            })
    });

    Ok(agent)
}

pub(super) const SESSION_COLUMNS: &str =
    "id, job_id, chat_id, backend, account_id, status, parent_session_id,
    replaced_by_id, terminal_reason, sequence, created_at, closed_at, updated_at, backend_id";
