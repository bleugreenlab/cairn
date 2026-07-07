use crate::effects::types::WorkflowEffect;
use crate::models::{
    AgentConfig, DelegatedStatus, ExecutionSnapshot, RecipeSnapshot, RecipeTrigger, TriggerContext,
    TriggerType,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_common::ids;

use crate::execution::delegation::{
    create_call_packet, create_or_reuse_task_packet, CreateCallPacketInput,
    CreateDelegatedPacketInput, DelegatedTaskPayload,
};

/// Parent run context needed to materialize and resume delegated tasks.
pub(super) struct ParentRunContext {
    pub(super) run_id: String,
    pub(super) job_id: String,
    pub(super) execution_id: Option<String>,
    pub(super) exec_seq: Option<i32>,
    pub(super) issue_id: Option<String>,
    pub(super) issue_number: Option<i32>,
    pub(super) project_id: String,
    pub(super) project_key: String,
}

pub(super) fn block_on<T>(
    fut: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    fn run<T>(fut: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(fut)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run(fut))
            .join()
            .map_err(|_| "database helper thread panicked".to_string())?
    } else {
        run(fut)
    }
}

pub(super) fn block_on_value<T>(
    fut: impl std::future::Future<Output = T> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    block_on(async move { Ok(fut.await) })
}

/// Resolve the turn a new delegated packet should anchor its resume on.
///
/// When the parent is already suspended on an in-flight delegated wait, its
/// `current_turn_id` points at the *pending* `dependency_unblock` successor that
/// suspend created — a placeholder the agent never executed. A second
/// `change(task)` call in the same parent turn would otherwise capture that
/// placeholder and chain a fresh successor onto it (T1 -> S1 -> S2), stranding
/// the parent: each batch then anchors a different turn, so no child completion
/// satisfies the resume gate. Walking back through any pending dependency-unblock
/// successors to the real executing turn makes concurrent task spawns coalesce
/// onto one shared wait (all anchored to T1, sharing successor S1).
pub(super) async fn resolve_delegated_wait_anchor(
    db: &LocalDb,
    current_turn_id: &str,
) -> Option<String> {
    let mut turn_id = current_turn_id.to_string();
    // Bounded walk: with coalescing, chains never form, but guard regardless
    // (also collapses any chain left by an older build).
    for _ in 0..16 {
        let next = db
            .read({
                let turn_id = turn_id.clone();
                move |conn| {
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT state, start_reason, predecessor_id
                                 FROM turns WHERE id = ?1 LIMIT 1",
                                (turn_id.as_str(),),
                            )
                            .await?;
                        rows.next()
                            .await?
                            .map(|row| {
                                Ok::<_, crate::storage::DbError>((
                                    row.text(0)?,
                                    row.text(1)?,
                                    row.opt_text(2)?,
                                ))
                            })
                            .transpose()
                    })
                }
            })
            .await
            .ok()
            .flatten();
        match next {
            Some((state, start_reason, Some(pred)))
                if state == "pending" && start_reason == "dependency_unblock" =>
            {
                turn_id = pred;
            }
            _ => break,
        }
    }
    Some(turn_id)
}

pub(super) async fn select_optional_text(
    db: &LocalDb,
    sql: &'static str,
    id: &str,
) -> DbResult<Option<String>> {
    let id = id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, (id.as_str(),)).await?;
            rows.next()
                .await?
                .map(|row| row.opt_text(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
}

pub(super) async fn select_optional_i64(
    db: &LocalDb,
    sql: &'static str,
    id: &str,
) -> DbResult<Option<i64>> {
    let id = id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, (id.as_str(),)).await?;
            rows.next()
                .await?
                .map(|row| row.opt_i64(0))
                .transpose()
                .map(Option::flatten)
        })
    })
    .await
}

pub(super) async fn project_repo_path(db: &LocalDb, project_id: &str) -> Option<String> {
    select_optional_text(
        db,
        "SELECT repo_path FROM projects WHERE id = ?1",
        project_id,
    )
    .await
    .ok()
    .flatten()
}

/// Resolve just the caller's node job_id, using the same run-id-or-cwd
/// resolution as the self task-spawn pipeline. Exposed at crate visibility so
/// the blocking-append router can compare the caller against each task append's
/// addressed node when deciding self vs cross-node routing.
pub(crate) async fn lookup_caller_job_id(
    db: &LocalDb,
    run_id: Option<&str>,
    cwd: &str,
) -> Result<String, String> {
    lookup_run_context(db, run_id, cwd)
        .await
        .map(|ctx| ctx.job_id)
}

pub(super) async fn lookup_run_context(
    db: &LocalDb,
    run_id: Option<&str>,
    cwd: &str,
) -> Result<ParentRunContext, String> {
    if let Some(run_id) = run_id {
        let run_id = run_id.to_string();
        db.read(|conn| {
            Box::pin(async move {
                lookup_run_context_by_id(conn, &run_id)
                    .await?
                    .ok_or_else(|| {
                        crate::storage::DbError::Row(format!("No run found with id '{}'", run_id))
                    })
            })
        })
        .await
        .map_err(|e| e.to_string())
    } else {
        lookup_run_context_by_cwd(db, cwd).await
    }
}

async fn lookup_run_context_by_cwd(db: &LocalDb, cwd: &str) -> Result<ParentRunContext, String> {
    let cwd = cwd.to_string();
    db.read(|conn| {
        Box::pin(async move {
            if let Some(ctx) = lookup_run_context_by_cwd_worktree(conn, &cwd).await? {
                return Ok(ctx);
            }
            lookup_run_context_by_cwd_project(conn, &cwd)
                .await?
                .ok_or_else(|| {
                    crate::storage::DbError::Row(format!("No active run found for path '{}'", cwd))
                })
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn lookup_run_context_by_id(
    conn: &cairn_db::turso::Connection,
    run_id: &str,
) -> DbResult<Option<ParentRunContext>> {
    let mut rows = conn
        .query(
            "
            SELECT r.id, r.job_id, j.execution_id, r.issue_id, i.number,
                   j.project_id, p.key, j.node_name, e.seq
            FROM runs r
            JOIN jobs j ON r.job_id = j.id
            LEFT JOIN issues i ON r.issue_id = i.id
            JOIN projects p ON j.project_id = p.id
            LEFT JOIN executions e ON j.execution_id = e.id
            WHERE r.id = ?1
            LIMIT 1
            ",
            (run_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| run_context_from_row(&row, "implementation"))
        .transpose()
}

async fn lookup_run_context_by_cwd_worktree(
    conn: &cairn_db::turso::Connection,
    cwd: &str,
) -> DbResult<Option<ParentRunContext>> {
    let mut rows = conn
        .query(
            "
            SELECT r.id, r.job_id, j.execution_id, r.issue_id, i.number,
                   i.project_id, p.key, j.node_name, e.seq
            FROM runs r
            JOIN jobs j ON r.job_id = j.id
            JOIN issues i ON r.issue_id = i.id
            JOIN projects p ON i.project_id = p.id
            LEFT JOIN executions e ON j.execution_id = e.id
            WHERE j.worktree_path = ?1
              AND r.status IN ('starting', 'live')
            ORDER BY r.created_at DESC
            LIMIT 1
            ",
            (cwd,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| run_context_from_row(&row, "implementation"))
        .transpose()
}

async fn lookup_run_context_by_cwd_project(
    conn: &cairn_db::turso::Connection,
    cwd: &str,
) -> DbResult<Option<ParentRunContext>> {
    let mut rows = conn
        .query(
            "
            SELECT r.id, r.job_id, j.execution_id, r.issue_id, NULL,
                   j.project_id, p.key, j.node_name, e.seq
            FROM runs r
            JOIN jobs j ON r.job_id = j.id
            JOIN projects p ON j.project_id = p.id
            LEFT JOIN executions e ON j.execution_id = e.id
            WHERE p.repo_path = ?1
              AND r.status IN ('starting', 'live')
              AND j.issue_id IS NULL
            ORDER BY r.created_at DESC
            LIMIT 1
            ",
            (cwd,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| run_context_from_row(&row, "project"))
        .transpose()
}

fn run_context_from_row(row: &cairn_db::turso::Row, _job_type: &str) -> DbResult<ParentRunContext> {
    Ok(ParentRunContext {
        run_id: row.text(0)?,
        job_id: row.text(1)?,
        execution_id: row.opt_text(2)?,
        issue_id: row.opt_text(3)?,
        issue_number: row.opt_i64(4)?.map(|value| value as i32),
        project_id: row.text(5)?,
        project_key: row.text(6)?,
        exec_seq: row.opt_i64(8)?.map(|value| value as i32),
    })
}

pub(super) async fn ensure_task_execution_context(
    orch: &Orchestrator,
    parent_ctx: &ParentRunContext,
) -> Result<String, String> {
    if let Some(execution_id) = &parent_ctx.execution_id {
        return Ok(execution_id.clone());
    }

    let db = crate::execution::routing::owning_db_for_job(&orch.db, &parent_ctx.job_id).await?;
    let existing_execution_id = select_optional_text(
        &db,
        "SELECT execution_id FROM jobs WHERE id = ?1",
        &parent_ctx.job_id,
    )
    .await
    .map_err(|e| format!("Failed to load parent job execution context: {}", e))?;
    if let Some(execution_id) = existing_execution_id {
        return Ok(execution_id);
    }

    let now = chrono::Utc::now().timestamp() as i32;

    let snapshot = ExecutionSnapshot {
        recipe: RecipeSnapshot {
            id: format!("delegation-{}", parent_ctx.job_id),
            name: "Delegated Work".to_string(),
            description: Some("Synthetic execution for task delegation".to_string()),
            trigger: RecipeTrigger::Manual,
            nodes: vec![],
            edges: vec![],
        },
        agents: std::collections::HashMap::new(),
        skills: std::collections::HashMap::new(),
        trigger_context: TriggerContext {
            issue_id: parent_ctx.issue_id.clone(),
            project_id: parent_ctx.project_id.clone(),
            trigger_type: TriggerType::Manual,
            event_payload: None,
            initiated_via: None,
        },
        presets: None,
        delegated_packets: vec![],
        created_at: now as i64,
    };
    let snapshot_json = snapshot.to_json()?;

    let seq = match parent_ctx.issue_id.as_deref() {
        Some(issue_id) => Some(next_execution_seq(&db, issue_id).await?),
        None => None,
    };

    let execution_id = ids::mint_child(&parent_ctx.job_id);
    insert_synthetic_execution(
        &db,
        &execution_id,
        &snapshot.recipe.id,
        parent_ctx,
        now,
        &snapshot_json,
        seq,
    )
    .await?;

    Ok(execution_id)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_task_packet(
    orch: &Orchestrator,
    execution_id: &str,
    parent_ctx: &ParentRunContext,
    payload: &DelegatedTaskPayload,
    agent_config: &AgentConfig,
    cwd: &str,
    parent_tool_use_id: Option<&str>,
    background: bool,
) -> Result<crate::models::DelegatedWorkPacket, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id).await?;
    // Serialize snapshot read-modify-write per execution to prevent concurrent
    // batch_tasks calls from overwriting each other's packets.
    let lock = orch.execution_lock(execution_id);
    let _guard = lock.lock().await;

    let snapshot_json = select_optional_text(
        &db,
        "SELECT snapshot FROM executions WHERE id = ?1",
        execution_id,
    )
    .await
    .map_err(|e| format!("Failed to load execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot = crate::config::snapshot_migrate::load(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;

    let parent_turn_id = match select_optional_text(
        &db,
        "SELECT current_turn_id FROM jobs WHERE id = ?1",
        &parent_ctx.job_id,
    )
    .await
    .ok()
    .flatten()
    {
        // Anchor the packet on the parent's real executing turn so concurrent
        // task spawns coalesce onto one delegated wait instead of chaining.
        Some(current) => resolve_delegated_wait_anchor(&db, &current).await,
        None => None,
    };
    let parent_backend = select_optional_text(
        &db,
        "SELECT model FROM jobs WHERE id = ?1",
        &parent_ctx.job_id,
    )
    .await
    .ok()
    .flatten()
    .as_deref()
    .and_then(crate::backends::backend_for_model);
    let session = payload.session.into();

    let output_contract = crate::execution::delegation::resolve_delegated_output_contract(
        payload.output_schema.as_ref(),
    )?;

    let packet = create_or_reuse_task_packet(
        &mut snapshot,
        CreateDelegatedPacketInput {
            parent_job_id: &parent_ctx.job_id,
            parent_turn_id: parent_turn_id.as_deref(),
            parent_tool_use_id,
            title: &payload.description,
            problem_statement: &payload.prompt,
            agent_config_id: &agent_config.id,
            cwd,
            fence: agent_config.fence,
            acceptance: vec!["Return the delegated result with the return tool".to_string()],
            output_contract,
            session,
            task_index: payload.task_index,
            tier_override: payload.tier.as_deref(),
            backend_preference: payload.backend_preference.as_deref().or(parent_backend),
            background,
        },
    );

    let snapshot_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize execution snapshot: {}", e))?;
    update_execution_snapshot(&db, execution_id, snapshot_json)
        .await
        .map_err(|e| format!("Failed to persist delegated packet: {}", e))?;

    Ok(packet)
}

/// Persist a pre-materialized call packet into the execution snapshot
/// (CAIRN-2481). The call sibling of [`persist_task_packet`]: it takes the same
/// per-execution snapshot lock and anchor resolution, but writes a `Materialized`
/// `CallTool` packet whose `result_artifact_job_id` is the already-created call
/// job, so the DAG never expands it into a node.
#[allow(clippy::too_many_arguments)]
pub(super) async fn persist_call_packet(
    orch: &Orchestrator,
    execution_id: &str,
    parent_ctx: &ParentRunContext,
    agent_config_id: &str,
    title: &str,
    problem_statement: &str,
    cwd: &str,
    output_contract: crate::models::DelegatedOutputContract,
    result_artifact_job_id: &str,
    parent_tool_use_id: Option<&str>,
    tier_override: Option<&str>,
    task_index: Option<i32>,
    background: bool,
) -> Result<crate::models::DelegatedWorkPacket, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id).await?;
    // Serialize the snapshot read-modify-write per execution so a batch of calls
    // never overwrites each other's packets.
    let lock = orch.execution_lock(execution_id);
    let _guard = lock.lock().await;

    let snapshot_json = select_optional_text(
        &db,
        "SELECT snapshot FROM executions WHERE id = ?1",
        execution_id,
    )
    .await
    .map_err(|e| format!("Failed to load execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot = crate::config::snapshot_migrate::load(&snapshot_json)
        .map_err(|e| format!("Failed to parse execution snapshot: {}", e))?;

    let parent_turn_id = match select_optional_text(
        &db,
        "SELECT current_turn_id FROM jobs WHERE id = ?1",
        &parent_ctx.job_id,
    )
    .await
    .ok()
    .flatten()
    {
        Some(current) => resolve_delegated_wait_anchor(&db, &current).await,
        None => None,
    };
    let parent_backend = select_optional_text(
        &db,
        "SELECT model FROM jobs WHERE id = ?1",
        &parent_ctx.job_id,
    )
    .await
    .ok()
    .flatten()
    .as_deref()
    .and_then(crate::backends::backend_for_model);

    let packet = create_call_packet(
        &mut snapshot,
        CreateCallPacketInput {
            parent_job_id: &parent_ctx.job_id,
            parent_turn_id: parent_turn_id.as_deref(),
            parent_tool_use_id,
            title,
            problem_statement,
            agent_config_id,
            cwd,
            output_contract,
            result_artifact_job_id,
            task_index,
            tier_override,
            backend_preference: parent_backend,
            background,
        },
    );

    let snapshot_json = serde_json::to_string(&snapshot)
        .map_err(|e| format!("Failed to serialize execution snapshot: {}", e))?;
    update_execution_snapshot(&db, execution_id, snapshot_json)
        .await
        .map_err(|e| format!("Failed to persist call packet: {}", e))?;

    Ok(packet)
}

async fn lookup_packet_run(
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
) -> Result<Option<(crate::models::DelegatedWorkPacket, String, String)>, String> {
    let packet = match refresh_packet_state(db, execution_id, packet_id).await? {
        Some(packet) => packet,
        None => return Ok(None),
    };
    let Some(job_id) = packet.result_artifact_job_id.clone() else {
        return Ok(None);
    };
    let Some(run_id) = latest_run_for_job(db, &job_id).await? else {
        return Ok(None);
    };
    Ok(Some((packet, job_id, run_id)))
}

async fn next_execution_seq(db: &LocalDb, issue_id: &str) -> Result<i32, String> {
    let value = select_optional_i64(
        db,
        "SELECT MAX(seq) FROM executions WHERE issue_id = ?1",
        issue_id,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(value.map(|value| value as i32 + 1).unwrap_or(1))
}

async fn insert_synthetic_execution(
    db: &LocalDb,
    execution_id: &str,
    recipe_id: &str,
    parent_ctx: &ParentRunContext,
    now: i32,
    snapshot_json: &str,
    seq: Option<i32>,
) -> Result<(), String> {
    let execution_id = execution_id.to_string();
    let recipe_id = recipe_id.to_string();
    let issue_id = parent_ctx.issue_id.clone();
    let project_id = if parent_ctx.issue_id.is_none() {
        Some(parent_ctx.project_id.clone())
    } else {
        None
    };
    let parent_job_id = parent_ctx.job_id.clone();
    let snapshot_json = snapshot_json.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        let recipe_id = recipe_id.clone();
        let issue_id = issue_id.clone();
        let project_id = project_id.clone();
        let parent_job_id = parent_job_id.clone();
        let snapshot_json = snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO executions (
                    id, recipe_id, issue_id, project_id, status, started_at,
                    completed_at, snapshot, seq, initiator_sub,
                    initiator_org_id, triggered_by
                )
                VALUES (?1, ?2, ?3, ?4, 'running', ?5, NULL, ?6, ?7, NULL, NULL, 'manual')
                ",
                (
                    execution_id.as_str(),
                    recipe_id.as_str(),
                    issue_id.as_deref(),
                    project_id.as_deref(),
                    now,
                    snapshot_json.as_str(),
                    seq,
                ),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET execution_id = ?1 WHERE id = ?2",
                (execution_id.as_str(), parent_job_id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn update_execution_snapshot(
    db: &LocalDb,
    execution_id: &str,
    snapshot_json: String,
) -> DbResult<()> {
    let execution_id = execution_id.to_string();
    db.write(|conn| {
        let execution_id = execution_id.clone();
        let snapshot_json = snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
                (snapshot_json.as_str(), execution_id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await
}

async fn latest_run_for_job(db: &LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.query_text(
        "
        SELECT id FROM runs
        WHERE job_id = ?1
        ORDER BY created_at DESC
        LIMIT 1
        ",
        (job_id,),
    )
    .await
    .map_err(|e| format!("Failed to load delegated run: {}", e))
}

pub(super) async fn latest_run_for_job_arc(
    db: std::sync::Arc<LocalDb>,
    job_id: String,
) -> Result<Option<String>, String> {
    latest_run_for_job(&db, &job_id).await
}

pub(super) async fn refresh_packet_state(
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
) -> Result<Option<crate::models::DelegatedWorkPacket>, String> {
    let snapshot_json = select_optional_text(
        db,
        "SELECT snapshot FROM executions WHERE id = ?1",
        execution_id,
    )
    .await
    .map_err(|e| format!("Failed to load execution: {}", e))?;
    let snapshot_json = snapshot_json.ok_or("Execution has no snapshot")?;
    let mut snapshot = crate::config::snapshot_migrate::load(&snapshot_json)
        .map_err(|e| format!("Failed to parse snapshot: {}", e))?;

    let mut changed = false;
    let packet = snapshot
        .delegated_packets
        .iter_mut()
        .find(|packet| packet.id == packet_id);
    let Some(packet) = packet else {
        return Ok(None);
    };

    if let Some(job_id) = &packet.result_artifact_job_id {
        let job_status = select_optional_text(db, "SELECT status FROM jobs WHERE id = ?1", job_id)
            .await
            .map_err(|e| format!("Failed to load delegated job: {}", e))?;
        let run_status = match latest_run_for_job(db, job_id).await? {
            Some(run_id) => {
                select_optional_text(db, "SELECT status FROM runs WHERE id = ?1", &run_id)
                    .await
                    .ok()
                    .flatten()
            }
            None => None,
        };

        let next_status = match job_status.as_deref() {
            Some("complete") => DelegatedStatus::Completed,
            Some("failed") => DelegatedStatus::Failed,
            // A cancelled child is terminal; without this it would stay at its
            // prior non-terminal packet status and never satisfy the resume gate
            // or the background completion wake.
            Some("cancelled") => DelegatedStatus::Cancelled,
            Some("running") => DelegatedStatus::Running,
            Some("pending") => DelegatedStatus::Materialized,
            _ if matches!(run_status.as_deref(), Some("live") | Some("starting")) => {
                DelegatedStatus::Running
            }
            _ => packet.status.clone(),
        };

        if packet.status != next_status {
            packet.status = next_status;
            changed = true;
        }
    }

    let result = packet.clone();
    if changed {
        let snapshot_json = serde_json::to_string(&snapshot)
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))?;
        update_execution_snapshot(db, execution_id, snapshot_json)
            .await
            .map_err(|e| format!("Failed to update delegated packet status: {}", e))?;
    }

    Ok(Some(result))
}

pub(super) async fn wait_for_packet_run_materialization(
    orch: &Orchestrator,
    execution_id: &str,
    packet_id: &str,
) -> Result<(crate::models::DelegatedWorkPacket, String, String), String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        let lookup = lookup_packet_run(&db, execution_id, packet_id).await?;

        if let Some(result) = lookup {
            return Ok(result);
        }

        if tokio::time::Instant::now() >= deadline {
            return Err("Delegated task did not materialize a child job".to_string());
        }

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(super) async fn dispatch_ready_agent_jobs(
    orch: &Orchestrator,
    ready_jobs: Vec<crate::models::Job>,
) -> Result<(), String> {
    if ready_jobs.is_empty() {
        return Ok(());
    }

    let executor = orch.executor.get().ok_or_else(|| {
        format!(
            "No executor configured for {} ready delegated jobs",
            ready_jobs.len()
        )
    })?;

    executor
        .execute(orch, WorkflowEffect::StartAgentJobs(ready_jobs))
        .await
        .map(|_| ())
}

// ============================================================================
// Handler
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn migrated_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("anchor-test.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn insert_turn(
        db: &LocalDb,
        id: &str,
        seq: i64,
        state: &str,
        start_reason: &str,
        predecessor: Option<&str>,
    ) {
        let id = id.to_string();
        let state = state.to_string();
        let start_reason = start_reason.to_string();
        let predecessor = predecessor.map(str::to_string);
        db.write(move |conn| {
            let id = id.clone();
            let state = state.clone();
            let start_reason = start_reason.clone();
            let predecessor = predecessor.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO turns(id, session_id, sequence, state, start_reason, \
                     predecessor_id, created_at, updated_at) \
                     VALUES (?1, 'sess', ?2, ?3, ?4, ?5, 0, 0)",
                    (
                        id.as_str(),
                        seq,
                        state.as_str(),
                        start_reason.as_str(),
                        predecessor.as_deref(),
                    ),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// A real executing turn resolves to itself.
    #[tokio::test]
    async fn anchor_of_executing_turn_is_itself() {
        let db = migrated_db().await;
        insert_turn(&db, "T1", 1, "running", "initial", None).await;
        assert_eq!(
            resolve_delegated_wait_anchor(&db, "T1").await.as_deref(),
            Some("T1")
        );
    }

    /// A pending dependency-unblock successor resolves back to its predecessor,
    /// so a second concurrent task spawn anchors on the real turn (coalescing).
    #[tokio::test]
    async fn anchor_walks_back_from_pending_dependency_successor() {
        let db = migrated_db().await;
        insert_turn(&db, "T1", 1, "yielded", "initial", None).await;
        insert_turn(&db, "S1", 2, "pending", "dependency_unblock", Some("T1")).await;
        assert_eq!(
            resolve_delegated_wait_anchor(&db, "S1").await.as_deref(),
            Some("T1")
        );
    }

    /// A pre-existing chain (S1 -> S2) collapses fully to the real turn.
    #[tokio::test]
    async fn anchor_collapses_existing_chain() {
        let db = migrated_db().await;
        insert_turn(&db, "T1", 1, "yielded", "initial", None).await;
        insert_turn(&db, "S1", 2, "pending", "dependency_unblock", Some("T1")).await;
        insert_turn(&db, "S2", 3, "pending", "dependency_unblock", Some("S1")).await;
        assert_eq!(
            resolve_delegated_wait_anchor(&db, "S2").await.as_deref(),
            Some("T1")
        );
    }
}
