//! Single trigger event dispatcher.
//!
//! Subscribes to `TriggerEvent` values from the broadcast channel,
//! enriches them into full typed events, applies anti-recursion guards,
//! matches recipes, and dispatches.

use crate::execution::triggers::{
    dispatch_event_recipes, find_recipes_for_job_ended, find_recipes_for_skill_called,
    is_event_triggered,
};
use crate::execution::Initiator;
use crate::models::{
    ExecutionSnapshot, JobEndedEvent, SkillCalledEvent, TriggerEvent, TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::schema::{executions, issues, jobs, projects};
use diesel::prelude::*;

/// Process a single trigger event: enrich, guard, match, dispatch.
///
/// Called from each host's subscriber loop (Tauri and cairn-server)
/// inside `spawn_blocking` since it does synchronous DB work.
pub fn process_trigger_event(orch: &Orchestrator, event: TriggerEvent) {
    log::info!("trigger-dispatch: received {:?}", &event);

    match event {
        TriggerEvent::JobEnded {
            job_id,
            status,
            execution_id,
            issue_id,
            project_id,
        } => {
            process_job_ended(orch, &job_id, &status, execution_id, issue_id, &project_id);
        }
        TriggerEvent::SkillCalled {
            skill_id,
            skill_name,
            run_id,
            job_id,
            execution_id,
            issue_id,
            project_id,
            project_key,
            issue_number,
            exec_seq,
            node_name,
        } => {
            process_skill_called(
                orch,
                &skill_id,
                &skill_name,
                &run_id,
                &job_id,
                execution_id,
                issue_id,
                &project_id,
                &project_key,
                issue_number,
                exec_seq,
                node_name,
            );
        }
    }
}

fn process_job_ended(
    orch: &Orchestrator,
    job_id: &str,
    status: &str,
    execution_id: Option<String>,
    issue_id: Option<String>,
    project_id: &str,
) {
    let Ok(mut conn) = orch.db.conn.lock() else {
        log::warn!("trigger-dispatch: failed to acquire DB lock for job_ended");
        return;
    };

    // Anti-recursion: if execution was event-triggered, skip
    if let Some(ref exec_id) = execution_id {
        let snapshot_json: Option<String> = executions::table
            .find(exec_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if let Some(ref json) = snapshot_json {
            if let Ok(snapshot) = serde_json::from_str::<ExecutionSnapshot>(json) {
                if is_event_triggered(&snapshot) {
                    log::info!(
                        "trigger-dispatch: skipping job_ended for event-triggered execution {}",
                        &exec_id[..exec_id.len().min(8)]
                    );
                    return;
                }
            }
        }
    }

    // Enrich: look up human-readable identifiers
    let project_key: String = projects::table
        .find(project_id)
        .select(projects::key)
        .first(&mut *conn)
        .unwrap_or_else(|_| project_id.to_string());

    let agent_config_id: Option<String> = jobs::table
        .find(job_id)
        .select(jobs::agent_config_id)
        .first::<Option<String>>(&mut *conn)
        .ok()
        .flatten();

    let node_name: Option<String> = jobs::table
        .find(job_id)
        .select(jobs::node_name)
        .first::<Option<String>>(&mut *conn)
        .ok()
        .flatten();

    let issue_number: Option<i32> = issue_id.as_ref().and_then(|iid| {
        issues::table
            .find(iid)
            .select(issues::number)
            .first(&mut *conn)
            .ok()
    });

    let exec_seq: Option<i32> = execution_id.as_ref().and_then(|eid| {
        executions::table
            .find(eid)
            .select(executions::seq)
            .first::<Option<i32>>(&mut *conn)
            .ok()
            .flatten()
    });

    // Build transcript URI
    let transcript_uri = match (&issue_number, &exec_seq, &node_name) {
        (Some(num), Some(seq), Some(name)) => Some(format!(
            "cairn://{}/{}/{}/{}/chat",
            project_key, num, seq, name
        )),
        _ => None,
    };

    // Extract initiator
    let initiator: Option<Initiator> = execution_id.as_ref().and_then(|exec_id| {
        let init_row: Option<(Option<String>, Option<String>, Option<String>)> = executions::table
            .find(exec_id)
            .select((
                executions::initiator_sub,
                executions::initiator_auth_mode,
                executions::initiator_org_id,
            ))
            .first(&mut *conn)
            .ok();

        init_row.and_then(|(sub, auth_mode, org_id)| {
            Some(Initiator {
                sub: sub?,
                auth_mode: auth_mode?,
                org_id: org_id.unwrap_or_default(),
            })
        })
    });

    let enriched = JobEndedEvent {
        event_id: job_id.to_string(),
        source_job_id: job_id.to_string(),
        project_key,
        issue_number,
        execution_seq: exec_seq,
        agent_config_id,
        node_name,
        status: status.to_string(),
        completed_at: chrono::Utc::now().timestamp(),
        transcript_uri,
    };

    // Drop lock before recipe operations (they also acquire locks)
    drop(conn);

    let recipes = match find_recipes_for_job_ended(orch, project_id, &enriched) {
        Ok(r) => r,
        Err(e) => {
            log::warn!(
                "trigger-dispatch: failed to find recipes for job_ended: {}",
                e
            );
            return;
        }
    };

    if recipes.is_empty() {
        log::debug!(
            "trigger-dispatch: no recipes match job_ended for job {} in project {}",
            &job_id[..job_id.len().min(8)],
            project_id,
        );
        return;
    }

    log::info!(
        "trigger-dispatch: {} recipe(s) match job_ended for job {}",
        recipes.len(),
        &job_id[..job_id.len().min(8)]
    );

    let payload = match serde_json::to_value(&enriched) {
        Ok(v) => v,
        Err(e) => {
            log::error!("trigger-dispatch: failed to serialize JobEndedEvent: {}", e);
            return;
        }
    };

    dispatch_event_recipes(
        orch,
        recipes,
        TriggerType::JobEnded,
        payload,
        issue_id.as_deref(),
        project_id,
        initiator,
    );
}

#[allow(clippy::too_many_arguments)]
fn process_skill_called(
    orch: &Orchestrator,
    skill_id: &str,
    skill_name: &str,
    _run_id: &str,
    job_id: &str,
    execution_id: Option<String>,
    issue_id: Option<String>,
    project_id: &str,
    project_key: &str,
    issue_number: Option<i32>,
    exec_seq: Option<i32>,
    node_name: Option<String>,
) {
    // Anti-recursion: if execution was event-triggered, skip
    if let Some(ref exec_id) = execution_id {
        let Ok(mut conn) = orch.db.conn.lock() else {
            return;
        };
        let snapshot_json: Option<String> = executions::table
            .find(exec_id)
            .select(executions::snapshot)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if let Some(ref json) = snapshot_json {
            if let Ok(snapshot) = serde_json::from_str::<ExecutionSnapshot>(json) {
                if is_event_triggered(&snapshot) {
                    log::info!(
                        "trigger-dispatch: skipping skill_called for event-triggered execution {}",
                        &exec_id[..exec_id.len().min(8)]
                    );
                    return;
                }
            }
        }
    }

    // Build transcript URI
    let transcript_uri = match (issue_number, exec_seq, &node_name) {
        (Some(num), Some(seq), Some(name)) => Some(format!(
            "cairn://{}/{}/{}/{}/chat",
            project_key, num, seq, name
        )),
        _ => None,
    };

    let enriched = SkillCalledEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        source_job_id: job_id.to_string(),
        skill_id: skill_id.to_string(),
        skill_name: skill_name.to_string(),
        project_key: project_key.to_string(),
        issue_number,
        execution_seq: exec_seq,
        node_name,
        transcript_uri,
    };

    // Log all recipes visible in this project context
    match orch.list_recipes_for_context(project_id) {
        Ok(all) => {
            let details: Vec<String> = all
                .iter()
                .map(|r| format!("{}(trigger={}, id={})", r.name, r.trigger, r.id))
                .collect();
            log::info!(
                "trigger-dispatch: {} recipe(s) in project {}: [{}]",
                all.len(),
                project_id,
                details.join(", "),
            );
        }
        Err(e) => {
            log::warn!(
                "trigger-dispatch: failed to list recipes for context: {}",
                e
            );
        }
    }

    let recipes = match find_recipes_for_skill_called(orch, project_id, &enriched) {
        Ok(r) => r,
        Err(e) => {
            log::warn!(
                "trigger-dispatch: failed to find recipes for skill_called: {}",
                e
            );
            return;
        }
    };

    log::info!(
        "trigger-dispatch: {} recipe(s) match skill_called for skill '{}' (project {})",
        recipes.len(),
        skill_id,
        project_id,
    );

    if recipes.is_empty() {
        return;
    }

    // Extract initiator
    let initiator: Option<Initiator> = execution_id.as_ref().and_then(|exec_id| {
        let Ok(mut conn) = orch.db.conn.lock() else {
            return None;
        };
        let init_row: Option<(Option<String>, Option<String>, Option<String>)> = executions::table
            .find(exec_id)
            .select((
                executions::initiator_sub,
                executions::initiator_auth_mode,
                executions::initiator_org_id,
            ))
            .first(&mut *conn)
            .ok();

        init_row.and_then(|(sub, auth_mode, org_id)| {
            Some(Initiator {
                sub: sub?,
                auth_mode: auth_mode?,
                org_id: org_id.unwrap_or_default(),
            })
        })
    });

    log::info!(
        "trigger-dispatch: {} recipe(s) match skill_called for skill '{}'",
        recipes.len(),
        skill_id
    );

    let payload = match serde_json::to_value(&enriched) {
        Ok(v) => v,
        Err(e) => {
            log::error!(
                "trigger-dispatch: failed to serialize SkillCalledEvent: {}",
                e
            );
            return;
        }
    };

    dispatch_event_recipes(
        orch,
        recipes,
        TriggerType::SkillCalled,
        payload,
        issue_id.as_deref(),
        project_id,
        initiator,
    );
}
