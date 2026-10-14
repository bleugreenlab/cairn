use crate::models::RunStatus;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};
use cairn_db::turso::params;
pub use cairn_session::backends::run_state::*;
use std::sync::Arc;

/// Resolve the run's owning database ONCE at the backend session-launch
/// boundary (CAIRN-2208). A team run's `runs` row — and the whole streaming
/// transcript entangled with it — lives wholly in that team's synced replica,
/// so the backend's stream/event/run-state writes must target THAT database,
/// not the private one, or the `message_streams → runs` foreign key fails. This
/// is the expensive async scan ([`crate::execution::routing::owning_db_for_run`]
/// probes every open DB); callers resolve it once here and thread the returned
/// handle through the reader/streaming hot path rather than re-resolving per
/// event. Fail-closed: a team run whose replica is not open errors rather than
/// silently writing the transcript into the private database. For a local run
/// the private DB always carries the row, so this is a strict no-op.
pub(in crate::backends) fn resolve_run_db(
    backend_name: &str,
    orch: &Orchestrator,
    run_id: &str,
) -> Result<Arc<LocalDb>, String> {
    let dbs = orch.db.clone();
    let run_id = run_id.to_string();
    run_backend_db(backend_name, async move {
        crate::execution::routing::owning_db_for_run(&dbs, &run_id)
            .await
            .map_err(|e| e.to_string())
    })
}

pub(in crate::backends) fn transition_run_to_live(
    backend_name: &str,
    orch: &Orchestrator,
    db: &Arc<LocalDb>,
    run_id: &str,
) -> Result<(), String> {
    let db = db.clone();
    let emit_db = db.clone();
    let run_id = run_id.to_string();
    let emit_run_id = run_id.clone();
    let _job_id = run_backend_db(backend_name, async move {
        db.write(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status, job_id
                         FROM runs
                         WHERE id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal(format!("run {} not found", run_id)))?;
                let current = row
                    .opt_text(0)?
                    .ok_or_else(|| DbError::internal(format!("run {} has no status", run_id)))?;
                let job_id = row.opt_text(1)?;
                let from: RunStatus = current.parse().map_err(|_| {
                    DbError::internal(format!(
                        "unparseable run status for {}: {}",
                        run_id, current
                    ))
                })?;
                if !matches!(from, RunStatus::Starting) {
                    return Err(DbError::internal(format!(
                        "run transition not allowed: {} -> {}",
                        from,
                        RunStatus::Live
                    )));
                }
                let now = chrono::Utc::now().timestamp() as i32;
                let live = RunStatus::Live.to_string();
                conn.execute(
                    "UPDATE runs
                     SET status = ?1,
                         started_at = ?2,
                         updated_at = ?2
                     WHERE id = ?3",
                    params![live.as_str(), now, run_id.as_str()],
                )
                .await?;
                Ok(job_id)
            })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    let change = run_backend_db(backend_name, async move {
        Ok(crate::notify::run_db_change_for_id(&emit_db, &emit_run_id, "update").await)
    })?;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(())
}
