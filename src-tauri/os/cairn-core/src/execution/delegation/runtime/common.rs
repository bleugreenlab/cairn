use crate::effects::types::WorkflowEffect;
use crate::models::{AgentConfig, DelegatedStatus};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};
use cairn_workflow::delegated_packets as packets;

use crate::execution::delegation::{
    create_call_packet, create_or_reuse_task_packet, CreateCallPacketInput,
    CreateDelegatedPacketInput, DelegatedTaskPayload,
};

/// Parent run context needed to materialize and resume delegated tasks.
pub(super) struct ParentRunContext {
    pub(super) run_id: String,
    pub(super) job_id: String,
    pub(super) execution_id: Option<String>,
    pub(super) project_id: String,
    pub(super) project_key: String,
}

pub(super) fn block_on<T>(
    fut: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    crate::storage::run_db_blocking(move || fut)
}

/// Resolve the turn a new delegated packet should anchor its resume on.
///
/// New waits keep the yielded predecessor as `current_turn_id` until their final
/// child settles. The bounded walk remains as recovery for pending successors
/// written by older builds, ensuring a second spawn still coalesces onto the real
/// executing turn rather than extending a legacy placeholder chain.
async fn resolve_delegated_wait_anchor(db: &LocalDb, current_turn_id: &str) -> Option<String> {
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

/// Resolve just the caller's node job_id from authenticated run identity.
/// Exposed at crate visibility so
/// the blocking-append router can compare the caller against each task append's
/// addressed node when deciding self vs cross-node routing.
pub(crate) async fn lookup_caller_job_id(db: &LocalDb, run_id: &str) -> Result<String, String> {
    lookup_run_context(db, run_id).await.map(|ctx| ctx.job_id)
}

pub(super) async fn lookup_run_context(
    db: &LocalDb,
    run_id: &str,
) -> Result<ParentRunContext, String> {
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
}

async fn lookup_run_context_by_id(
    conn: &crate::storage::TrackedConnection,
    run_id: &str,
) -> DbResult<Option<ParentRunContext>> {
    // The parent's own identity and project, nothing about an issue coordinate:
    // a delegating parent may be an issue node or a thread session, and every
    // URI a delegated task needs is built from the CHILD's canonical home.
    let mut rows = conn
        .query(
            "
            SELECT r.id, r.job_id, j.execution_id, j.project_id, p.key
            FROM runs r
            JOIN jobs j ON r.job_id = j.id
            JOIN projects p ON j.project_id = p.id
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

fn run_context_from_row(row: &cairn_db::turso::Row, _job_type: &str) -> DbResult<ParentRunContext> {
    Ok(ParentRunContext {
        run_id: row.text(0)?,
        job_id: row.text(1)?,
        execution_id: row.opt_text(2)?,
        project_id: row.text(3)?,
        project_key: row.text(4)?,
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
    let job = db
        .read({
            let job_id = parent_ctx.job_id.clone();
            move |conn| {
                let job_id = job_id.clone();
                Box::pin(async move { crate::execution::jobs::load_job_conn(conn, &job_id).await })
            }
        })
        .await
        .map_err(|e| format!("Failed to load parent job execution context: {}", e))?
        .ok_or_else(|| format!("Parent job not found: {}", parent_ctx.job_id))?;
    if let Some(execution_id) = job.execution_id.clone() {
        return Ok(execution_id);
    }

    // One creation path. The host execution a delegating job needs and the one a
    // job's own turn needs are the same object created at different moments, and
    // it is seeded with the host's OWN agent — so the fence walk never lands on a
    // snapshot missing the agent that is running. Delegated agents join this same
    // map later through the entry API and will not clobber it.
    let project_path = project_repo_path(&db, &job.project_id)
        .await
        .map(std::path::PathBuf::from);
    crate::execution::jobs::ensure_host_agent_snapshot(
        orch,
        &job,
        project_path.as_deref(),
        crate::execution::jobs::SnapshotFreshness::for_job(&job),
    )
    .await?
    .ok_or_else(|| {
        format!(
            "Failed to create host execution for job {}",
            parent_ctx.job_id
        )
    })
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
    // Serialize packet creation per execution, so two spawns of the same retried
    // task cannot both miss each other's packet and create two, and so
    // advancement's DAG reduction (which holds this lock) never expands a packet
    // whose spawn is still in flight.
    let lock = orch.execution_lock(execution_id);
    let _guard = lock.lock().await;

    let existing = packets::list_db(&db, execution_id)
        .await
        .map_err(|e| format!("Failed to load delegated packets: {}", e))?;

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
    let session = payload.session.into();

    let output_contract = crate::execution::delegation::resolve_delegated_output_contract(
        payload.output_schema.as_ref(),
    )?;

    let (packet, created) = create_or_reuse_task_packet(
        &existing,
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
            models: payload.models.as_deref(),
            background,
        },
    );

    if created {
        packets::insert_db(&db, execution_id, &packet)
            .await
            .map_err(|e| format!("Failed to persist delegated packet: {}", e))?;
    }

    Ok(packet)
}

/// Persist a pre-materialized call packet (CAIRN-2481). The call sibling of [`persist_task_packet`]: it takes the same
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
    models: Option<&[cairn_config::allotment::AllotmentEntry]>,
    task_index: Option<i32>,
    background: bool,
) -> Result<crate::models::DelegatedWorkPacket, String> {
    let db = crate::execution::routing::owning_db_for_execution(&orch.db, execution_id).await?;
    // Serialize packet creation per execution, as `persist_task_packet` does.
    let lock = orch.execution_lock(execution_id);
    let _guard = lock.lock().await;

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

    let packet = create_call_packet(CreateCallPacketInput {
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
        models,
        background,
    });

    packets::insert_db(&db, execution_id, &packet)
        .await
        .map_err(|e| format!("Failed to persist call packet: {}", e))?;

    Ok(packet)
}

pub(super) async fn latest_run_for_job(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<String>, String> {
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

pub(super) async fn refresh_packet_state(
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
) -> Result<Option<crate::models::DelegatedWorkPacket>, String> {
    let Some(mut packet) = packets::get_db(db, execution_id, packet_id)
        .await
        .map_err(|e| format!("Failed to load delegated packet: {}", e))?
    else {
        return Ok(None);
    };
    let mut changed = false;

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

    if changed {
        // Status alone, field by field: packet state is written by callers
        // that hold no common lock (the spawn, the resume gate a child's
        // completion fires, a start-failure wake), and a whole-packet write
        // from this earlier read would undo another caller's change, bringing
        // a retired packet back `Pending` or unlinking its job.
        let change = packets::PacketChange {
            status: Some(packet.status.clone()),
            ..Default::default()
        };
        packets::update_db(db, execution_id, packet_id, &change)
            .await
            .map_err(|e| format!("Failed to update delegated packet status: {}", e))?;
    }

    Ok(Some(packet))
}

/// Settle a packet that advancement did not link to a child job: link it to the
/// job its agent node did produce, or retire it so it can never produce one.
///
/// Advancement writes `result_artifact_job_id` in a second snapshot pass after
/// creating the node's job, so an advancement that errored between the two can
/// leave a real job its packet does not name. Such a job is found by its agent
/// node and linked, which is what lets the resume gate and the spawn report see
/// it. A packet with no job is marked failed: advancement expands every
/// `Pending` packet it finds, so leaving one pending would create an orphan child
/// on some later, unrelated advancement — after its spawn was already reported
/// as refused — and a `Materialized` one left non-terminal would hold its batch
/// siblings' resume gate open forever.
///
/// This runs under the execution's snapshot lock, which advancement's DAG
/// reduction also holds while it expands packets (see
/// `execution::advancement::advance_execution_with_actions`). So a packet seen
/// without a job is never one whose expansion is still in flight: its creation
/// either finished, linking a job found here, or failed for good.
///
/// Returns the child job id when one exists.
pub(super) async fn link_or_retire_unmaterialized_packet(
    orch: &Orchestrator,
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
) -> Result<Option<String>, String> {
    let lock = orch.execution_lock(execution_id);
    let _guard = lock.lock().await;

    let Some(packet) = packets::get_db(db, execution_id, packet_id)
        .await
        .map_err(|e| format!("Failed to load delegated packet: {}", e))?
    else {
        return Ok(None);
    };
    if let Some(job_id) = packet.result_artifact_job_id.clone() {
        return Ok(Some(job_id));
    }

    let agent_node_id = packet
        .materialized_node_ids
        .iter()
        .find(|node_id| node_id.ends_with("-agent"))
        .cloned()
        .unwrap_or_else(|| format!("delegated-{}-agent", packet.id));
    let execution_owned = execution_id.to_string();
    let node_job = db
        .query_opt_text(
            "SELECT id FROM jobs
             WHERE execution_id = ?1 AND recipe_node_id = ?2 AND status <> 'cancelled'
             ORDER BY created_at DESC LIMIT 1",
            (execution_owned, agent_node_id),
        )
        .await
        .map_err(|e| format!("Failed to look up delegated job: {}", e))?;

    let change = match &node_job {
        Some(job_id) => packets::PacketChange {
            result_artifact_job_id: Some(job_id.clone()),
            ..Default::default()
        },
        None if matches!(
            packet.status,
            DelegatedStatus::Pending | DelegatedStatus::Materialized
        ) =>
        {
            packets::PacketChange {
                status: Some(DelegatedStatus::Failed),
                ..Default::default()
            }
        }
        None => return Ok(None),
    };
    packets::update_db(db, execution_id, packet_id, &change)
        .await
        .map_err(|e| format!("Failed to settle delegated packet: {}", e))?;
    Ok(node_job)
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

    async fn migrated_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.keep().join("anchor-test.db"))
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
