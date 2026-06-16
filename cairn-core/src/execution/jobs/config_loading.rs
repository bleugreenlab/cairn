use super::*;

/// Load agent config for a job — tries execution snapshot first, falls back to config files.
pub(super) fn load_agent_config(
    orch: &Orchestrator,
    job: &DbJob,
    project_path: Option<&Path>,
) -> Result<Option<AgentConfig>, String> {
    let Some(aid) = &job.agent_config_id else {
        return Ok(None);
    };

    // Try snapshot first (ensures reproducibility for execution-based jobs)
    let mut snapshot_tier_override: Option<Model> = None;
    let mut snapshot_backend_preference: Option<String> = None;
    if let Some(exec_id) = &job.execution_id {
        log::info!(
            "load_agent_config: job {} has execution_id={}, trying snapshot for agent '{}'",
            job.id,
            exec_id,
            aid
        );
        let snapshot_data = run_db(load_agent_snapshot_data(
            orch.db.local.clone(),
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
            // expansion. Fall through to config files, but remember overrides.
            snapshot_tier_override = agent.tier.clone();
            snapshot_backend_preference = agent.backend_preference.clone();
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

    // Fall back to config files. Resolve-early: resolve once, against current
    // effective presets, into a concrete AgentConfig (loud on failure).
    let project_id = &job.project_id;
    let agent = config::get_config_dir().ok().and_then(|cd| {
        let presets = load_effective_presets(&cd, project_path);
        config_agents::get_agent(&cd, aid, project_path)
            .ok()
            .flatten()
            .and_then(|mut fa| {
                if snapshot_backend_preference.is_some() {
                    fa.backend_preference = snapshot_backend_preference.clone();
                }
                let override_sel = snapshot_tier_override
                    .as_ref()
                    .map(|m| LaunchSelectionOverride::Tier(m.as_str().to_string()));
                let snapshot = resolve_agent_snapshot(&fa, override_sel.as_ref(), &presets).ok()?;

                Some(AgentConfig {
                    id: snapshot.id,
                    name: snapshot.name,
                    description: snapshot.description,
                    prompt: snapshot.prompt,
                    tools: snapshot.tools,
                    tier: snapshot.tier,
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
                    backend_preference: snapshot.backend_preference,
                    selection: snapshot.selection,
                    extras: snapshot.extras,
                })
            })
    });

    Ok(agent)
}

pub(super) const RUN_COLUMNS: &str =
    "id, issue_id, project_id, job_id, status, session_id, error_message,
    started_at, exited_at, created_at, updated_at, chat_id, backend, exit_reason, start_mode";

pub(super) const SESSION_COLUMNS: &str = "id, job_id, chat_id, backend, status, parent_session_id,
    replaced_by_id, terminal_reason, sequence, created_at, closed_at, updated_at, backend_id";
