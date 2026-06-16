use crate::models::RunStatus;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, RowExt};
use std::future::Future;
use turso::params;

pub(in crate::backends) fn run_backend_db<T, Fut>(
    backend_name: &'static str,
    future: Fut,
) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run_backend_db_future(backend_name, future))
            .join()
            .map_err(|_| format!("{backend_name} backend database task panicked"))?
    } else {
        run_backend_db_future(backend_name, future)
    }
}

fn run_backend_db_future<T>(
    backend_name: &'static str,
    future: impl Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| {
            format!("Failed to create {backend_name} backend database runtime: {error}")
        })?
        .block_on(future)
}

pub(in crate::backends) fn transition_run_to_live(
    backend_name: &'static str,
    orch: &Orchestrator,
    run_id: &str,
) -> Result<(), String> {
    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    let emit_run_id = run_id.clone();
    let job_id = run_backend_db(backend_name, async move {
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
    let _ = orch.services.emitter.emit(
        "db-change",
        crate::notify::run_db_change_ids("update", &emit_run_id, job_id.as_deref()),
    );
    Ok(())
}

pub(in crate::backends) fn set_session_backend_id(
    backend_name: &'static str,
    orch: &Orchestrator,
    session_id: &str,
    backend_id: &str,
) -> Result<(), String> {
    let db = orch.db.local.clone();
    let session_id = session_id.to_string();
    let backend_id = backend_id.to_string();
    run_backend_db(backend_name, async move {
        db.write(|conn| {
            let session_id = session_id.clone();
            let backend_id = backend_id.clone();
            Box::pin(async move {
                let now = chrono::Utc::now().timestamp() as i32;
                conn.execute(
                    "UPDATE sessions
                     SET backend_id = ?1,
                         updated_at = ?2
                     WHERE id = ?3",
                    params![backend_id.as_str(), now, session_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub(in crate::backends) fn run_job_id(
    backend_name: &'static str,
    orch: &Orchestrator,
    run_id: &str,
) -> Option<String> {
    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    run_backend_db(backend_name, async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT job_id
                         FROM runs
                         WHERE id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()?
                    .flatten())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
}

pub(in crate::backends) fn run_status(
    backend_name: &'static str,
    orch: &Orchestrator,
    run_id: &str,
) -> Option<String> {
    let db = orch.db.local.clone();
    let run_id = run_id.to_string();
    run_backend_db(backend_name, async move {
        db.read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT status
                         FROM runs
                         WHERE id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()?
                    .flatten())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .ok()
    .flatten()
}

pub(in crate::backends) fn is_task_spawned_run(
    backend_name: &'static str,
    orch: &Orchestrator,
    run_id: &str,
) -> bool {
    let db = orch.db.local.clone();
    let Some(job_id) = run_job_id(backend_name, orch, run_id) else {
        return false;
    };

    run_backend_db(backend_name, async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT parent_job_id
                         FROM jobs
                         WHERE id = ?1",
                        params![job_id.as_str()],
                    )
                    .await?;
                Ok(rows
                    .next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()?
                    .flatten()
                    .is_some())
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
    .unwrap_or(false)
}
