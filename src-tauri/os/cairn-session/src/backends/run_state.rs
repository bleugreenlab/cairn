use cairn_db::storage::{LocalDb, RowExt};
use cairn_db::turso::params;
use std::{future::Future, sync::Arc};
pub fn run_backend_db<T, Fut>(backend_name: &str, future: Fut) -> Result<T, String>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    // Copied because the name crosses into the spawned thread below. Taking a
    // borrow rather than a `'static` literal is what lets a run be attributed
    // to a provider its operator named.
    let backend_name = backend_name.to_string();
    if tokio::runtime::Handle::try_current().is_ok() {
        let panicked = format!("{backend_name} backend database task panicked");
        std::thread::spawn(move || run_backend_db_future(&backend_name, future))
            .join()
            .map_err(|_| panicked)?
    } else {
        run_backend_db_future(&backend_name, future)
    }
}

fn run_backend_db_future<T>(
    backend_name: &str,
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

pub fn set_session_backend_id(
    backend_name: &str,
    db: &Arc<LocalDb>,
    session_id: &str,
    backend_id: &str,
) -> Result<(), String> {
    let db = db.clone();
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

pub fn run_job_id(backend_name: &str, db: &Arc<LocalDb>, run_id: &str) -> Option<String> {
    let db = db.clone();
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

pub fn run_status(backend_name: &str, db: &Arc<LocalDb>, run_id: &str) -> Option<String> {
    let db = db.clone();
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

pub fn is_task_spawned_run(backend_name: &str, db: &Arc<LocalDb>, run_id: &str) -> bool {
    let Some(job_id) = run_job_id(backend_name, db, run_id) else {
        return false;
    };
    let db = db.clone();

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
