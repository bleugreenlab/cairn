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
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::ids;
use std::future::Future;
use std::sync::Arc;
use turso::params;

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
    let owning = match crate::storage::run_db_blocking({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        move || async move {
            crate::execution::routing::owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    }) {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "trigger-dispatch: failed to resolve owning db for job_ended: {}",
                error
            );
            return;
        }
    };
    let context = match load_job_ended_dispatch_context(
        owning,
        execution_id.as_deref(),
        issue_id.as_deref(),
        project_id,
        job_id,
    ) {
        Ok(context) => context,
        Err(error) => {
            log::warn!("trigger-dispatch: failed to enrich job_ended: {}", error);
            return;
        }
    };

    if context.skip_event_triggered {
        if let Some(ref exec_id) = execution_id {
            log::info!(
                "trigger-dispatch: skipping job_ended for event-triggered execution {}",
                &exec_id[..exec_id.len().min(8)]
            );
        }
        return;
    }

    // Build transcript URI
    let transcript_uri = match (&context.issue_number, &context.exec_seq, &context.node_name) {
        (Some(num), Some(seq), Some(name)) => Some(cairn_common::uri::build_node_chat_uri(
            &context.project_key,
            *num,
            *seq,
            name,
        )),
        _ => None,
    };

    let enriched = JobEndedEvent {
        event_id: job_id.to_string(),
        source_job_id: job_id.to_string(),
        project_key: context.project_key,
        issue_number: context.issue_number,
        execution_seq: context.exec_seq,
        agent_config_id: context.agent_config_id,
        node_name: context.node_name,
        status: status.to_string(),
        completed_at: chrono::Utc::now().timestamp(),
        transcript_uri,
    };

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
        context.initiator,
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
    let execution_context = match execution_id.as_ref() {
        Some(exec_id) => {
            let owning = match crate::storage::run_db_blocking({
                let dbs = orch.db.clone();
                let exec_id = exec_id.to_string();
                move || async move {
                    crate::execution::routing::owning_db_for_execution(&dbs, &exec_id)
                        .await
                        .map_err(|e| e.to_string())
                }
            }) {
                Ok(db) => db,
                Err(error) => {
                    log::warn!(
                        "trigger-dispatch: failed to resolve owning db for skill_called: {}",
                        error
                    );
                    return;
                }
            };
            match load_execution_dispatch_context(owning, exec_id.as_str()) {
                Ok(context) => Some(context),
                Err(error) => {
                    log::warn!(
                        "trigger-dispatch: failed to load execution context for skill_called: {}",
                        error
                    );
                    None
                }
            }
        }
        None => None,
    };

    // Anti-recursion: if execution was event-triggered, skip
    if execution_context
        .as_ref()
        .is_some_and(|context| context.skip_event_triggered)
    {
        if let Some(ref exec_id) = execution_id {
            log::info!(
                "trigger-dispatch: skipping skill_called for event-triggered execution {}",
                &exec_id[..exec_id.len().min(8)]
            );
        }
        return;
    }

    // Build transcript URI
    let transcript_uri = match (issue_number, exec_seq, &node_name) {
        (Some(num), Some(seq), Some(name)) => Some(cairn_common::uri::build_node_chat_uri(
            project_key,
            num,
            seq,
            name,
        )),
        _ => None,
    };

    let enriched = SkillCalledEvent {
        event_id: ids::mint_child(job_id),
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

    let initiator = execution_context.and_then(|context| context.initiator);

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

struct ExecutionDispatchContext {
    skip_event_triggered: bool,
    exec_seq: Option<i32>,
    initiator: Option<Initiator>,
}

struct JobEndedDispatchContext {
    skip_event_triggered: bool,
    project_key: String,
    agent_config_id: Option<String>,
    node_name: Option<String>,
    issue_number: Option<i32>,
    exec_seq: Option<i32>,
    initiator: Option<Initiator>,
}

fn load_job_ended_dispatch_context(
    db: Arc<LocalDb>,
    execution_id: Option<&str>,
    issue_id: Option<&str>,
    project_id: &str,
    job_id: &str,
) -> Result<JobEndedDispatchContext, String> {
    let execution_id = execution_id.map(str::to_string);
    let issue_id = issue_id.map(str::to_string);
    let project_id = project_id.to_string();
    let job_id = job_id.to_string();
    run_dispatch_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            let issue_id = issue_id.clone();
            let project_id = project_id.clone();
            let job_id = job_id.clone();
            Box::pin(async move {
                let execution_context = match execution_id.as_deref() {
                    Some(exec_id) => {
                        Some(load_execution_dispatch_context_conn(conn, exec_id).await?)
                    }
                    None => None,
                };

                let project_key = match query_one_text(
                    conn,
                    "SELECT key FROM projects WHERE id = ?1",
                    project_id.as_str(),
                )
                .await?
                {
                    Some(key) => key,
                    None => project_id.clone(),
                };

                let (agent_config_id, node_name) = load_job_event_context_conn(conn, &job_id)
                    .await?
                    .unwrap_or((None, None));

                let issue_number = match issue_id.as_deref() {
                    Some(issue_id) => {
                        query_one_i64(conn, "SELECT number FROM issues WHERE id = ?1", issue_id)
                            .await?
                            .map(|value| value as i32)
                    }
                    None => None,
                };

                Ok(JobEndedDispatchContext {
                    skip_event_triggered: execution_context
                        .as_ref()
                        .is_some_and(|context| context.skip_event_triggered),
                    project_key,
                    agent_config_id,
                    node_name,
                    issue_number,
                    exec_seq: execution_context
                        .as_ref()
                        .and_then(|context| context.exec_seq),
                    initiator: execution_context.and_then(|context| context.initiator),
                })
            })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

fn load_execution_dispatch_context(
    db: Arc<LocalDb>,
    execution_id: &str,
) -> Result<ExecutionDispatchContext, String> {
    let execution_id = execution_id.to_string();
    run_dispatch_db(async move {
        db.read(|conn| {
            let execution_id = execution_id.clone();
            Box::pin(async move { load_execution_dispatch_context_conn(conn, &execution_id).await })
        })
        .await
        .map_err(|error| error.to_string())
    })
}

async fn load_execution_dispatch_context_conn(
    conn: &turso::Connection,
    execution_id: &str,
) -> DbResult<ExecutionDispatchContext> {
    let mut rows = conn
        .query(
            "SELECT snapshot, seq, initiator_sub, initiator_auth_mode, initiator_org_id
             FROM executions
             WHERE id = ?1",
            params![execution_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(ExecutionDispatchContext {
            skip_event_triggered: false,
            exec_seq: None,
            initiator: None,
        });
    };

    let skip_event_triggered = row
        .opt_text(0)?
        .as_deref()
        .and_then(|json| serde_json::from_str::<ExecutionSnapshot>(json).ok())
        .is_some_and(|snapshot| is_event_triggered(&snapshot));
    let exec_seq = row.opt_i64(1)?.map(|value| value as i32);
    let sub = row.opt_text(2)?;
    let auth_mode = row.opt_text(3)?;
    let org_id = row.opt_text(4)?;
    let initiator = match (sub, auth_mode) {
        (Some(sub), Some(auth_mode)) => Some(Initiator {
            sub,
            auth_mode,
            org_id: org_id.unwrap_or_default(),
        }),
        _ => None,
    };

    Ok(ExecutionDispatchContext {
        skip_event_triggered,
        exec_seq,
        initiator,
    })
}

async fn load_job_event_context_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<(Option<String>, Option<String>)>> {
    let mut rows = conn
        .query(
            "SELECT agent_config_id, node_name
             FROM jobs
             WHERE id = ?1",
            params![job_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    Ok(Some((row.opt_text(0)?, row.opt_text(1)?)))
}

async fn query_one_text(
    conn: &turso::Connection,
    sql: &'static str,
    id: &str,
) -> DbResult<Option<String>> {
    let mut rows = conn.query(sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => row.opt_text(0),
        None => Ok(None),
    }
}

async fn query_one_i64(
    conn: &turso::Connection,
    sql: &'static str,
    id: &str,
) -> DbResult<Option<i64>> {
    let mut rows = conn.query(sql, params![id]).await?;
    match rows.next().await? {
        Some(row) => row.opt_i64(0),
        None => Ok(None),
    }
}

fn run_dispatch_db<T, Fut>(future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_dispatch_db_future(future))
            .join()
            .map_err(|_| "Trigger dispatch database task panicked".to_string())?
    } else {
        run_dispatch_db_future(future)
    }
}

fn run_dispatch_db_future<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("Failed to create trigger dispatch database runtime: {error}"))?
        .block_on(future)
}
