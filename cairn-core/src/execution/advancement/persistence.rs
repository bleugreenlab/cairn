use super::*;

pub(crate) fn run_advancement_db<T>(
    future: impl Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    fn run<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(future)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run(future))
            .join()
            .map_err(|_| "Advancement DB runtime thread panicked".to_string())?
    } else {
        run(future)
    }
}

pub(super) fn db_internal(message: impl Into<String>) -> DbError {
    DbError::internal(message.into())
}

pub(super) async fn load_job_by_id_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Option<DbJob>> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1 LIMIT 1");
    let mut rows = conn.query(&sql, (job_id,)).await?;
    rows.next()
        .await?
        .map(|row| db_job_from_row(&row))
        .transpose()
}

pub(crate) fn load_job(db: Arc<LocalDb>, job_id: &str) -> Result<Option<DbJob>, String> {
    let job_id = job_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move { load_job_by_id_conn(conn, &job_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

pub(crate) fn load_project_repo_path(
    db: Arc<LocalDb>,
    project_id: &str,
) -> Result<Option<String>, String> {
    let project_id = project_id.to_string();
    run_advancement_db(async move {
        db.read(|conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT repo_path FROM projects WHERE id = ?1",
                        (project_id.as_str(),),
                    )
                    .await?;
                crate::storage::next_text(&mut rows, 0).await
            })
        })
        .await
        .map_err(|e| format!("Failed to load project path: {e}"))
    })
}
