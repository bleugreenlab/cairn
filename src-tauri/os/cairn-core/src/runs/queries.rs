//! Application projections around session-owned run and transcript persistence.
use crate::{
    error::CairnError,
    models::{RunTodos, TodoItem},
    storage::LocalDb,
};
pub use cairn_session::runs::queries::*;
use std::sync::Arc;
pub fn get_job_todos(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<TodoItem>, CairnError> {
    let job_id = job_id.to_string();
    run_query_db(async move {
        crate::todos::get_todos_for_job(&db, &job_id)
            .await
            .map_err(CairnError::Internal)
    })
}

pub fn get_job_todos_by_run(db: Arc<LocalDb>, job_id: &str) -> Result<Vec<RunTodos>, CairnError> {
    let todos = get_job_todos(db, job_id)?;
    if todos.is_empty() {
        return Ok(vec![]);
    }
    Ok(vec![RunTodos {
        run_id: String::new(),
        run_number: 1,
        todos,
    }])
}

/// Reconcile runs left non-terminal by a host that is no longer running, and stop
/// any of their processes that outlived it.
///
/// `boot_at` is this host's [`crate::orchestrator::Orchestrator::boot_at`]. It is
/// required, not inferred: the transport is already serving when startup recovery
/// runs, so "every non-terminal run is stale" is false, and acting on it would let
/// this sweep signal an agent this host had just spawned.
pub fn reconcile_stale_runs(
    orch: &crate::orchestrator::Orchestrator,
    emitter: &dyn crate::services::EventEmitter,
    boot_at: i64,
) {
    // Every `leased`/`starting` row is about to be requeued by ordinary
    // admission recovery, so all three pending states are relaunchable. A run
    // whose row is `active` had a process that died with the predecessor: it is
    // settled here, and recovery withdraws its requeued row.
    let admitted = run_query_db({
        let local = orch.db.local.clone();
        async move {
            crate::storage::pending_launch_run_ids(&local)
                .await
                .map_err(|error| CairnError::Internal(error.to_string()))
        }
    });
    let admitted = match admitted {
        Ok(admitted) => admitted,
        Err(error) => {
            // Without the durable claims a queued launch is indistinguishable
            // from a dead one; leave every run for the in-session reaper.
            log::warn!("startup reconcile could not read admission claims; skipping: {error}");
            return;
        }
    };
    let result = reconcile_stale_runs_inner(
        orch.db.local.clone(),
        emitter,
        boot_at,
        &crate::agent_process::orphan::OsProcessTable,
        &orch.claimed_launch_job_ids(),
        &admitted,
    );
    if let Ok(result) = result {
        // `recompute_job` sweeps a job's whole execution, and the repair
        // candidates cluster by execution (a resolved issue contributes every
        // job it left unsettled), so sweep each execution once.
        let mut swept = std::collections::HashSet::new();
        for job_id in result.reproject_job_ids {
            let execution_id = crate::storage::run_db_blocking({
                let db = orch.db.local.clone();
                let job_id = job_id.clone();
                move || async move {
                    db.query_opt_text("SELECT execution_id FROM jobs WHERE id = ?1", (job_id,))
                        .await
                        .map_err(|error| error.to_string())
                }
            })
            .ok()
            .flatten();
            let outcome = match execution_id {
                Some(execution_id) if !swept.insert(execution_id.clone()) => continue,
                Some(execution_id) => {
                    crate::execution::advancement::recompute_execution_jobs(orch, &execution_id)
                }
                None => crate::execution::advancement::recompute_job(orch, &job_id),
            };
            if let Err(error) = outcome {
                log::warn!("Failed to re-derive job status for {job_id}: {error}");
            }
        }
    }
}
