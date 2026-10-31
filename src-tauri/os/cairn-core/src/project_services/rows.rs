//! The durable identity of a service: one `jobs` row per (project, owner,
//! service), and one `runs` row per incarnation and per delegated call.
//!
//! The job's home is `cairn://p/<project>/services/<id>`, or for a thread's
//! service `cairn://p/<project>/<thread>/services/<name>` (see
//! `cairn_db::lineage::home_uri_for_job_conn`), so every run under it
//! authenticates as `PrincipalRef::Agent { node: <that home>, run_id }`. The
//! job has no issue, thread, or execution: a thread's service names its owner
//! in `service_thread_id`, never `thread_id`, which would make the job one of
//! the thread's own. Its status is `service` and its runs' start mode is
//! `service`, so neither agent recovery nor the stale-run reaper mistakes them
//! for an agent whose process went missing.

use cairn_common::ids;
use cairn_db::turso::params;

use super::{ServiceOwner, SERVICE_AGENT_CONFIG, SERVICE_START_MODE};
use crate::storage::{LocalDb, RowExt};

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// The service's job, created on first use.
pub(crate) async fn ensure_service_job(
    db: &LocalDb,
    project_id: &str,
    owner: &ServiceOwner,
    service: &str,
) -> Result<String, String> {
    let project_id = project_id.to_string();
    let owner_thread = owner.thread_id().map(str::to_string);
    let service = service.to_string();
    db.write(move |conn| {
        let (project_id, owner_thread, service) =
            (project_id.clone(), owner_thread.clone(), service.clone());
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM jobs
                     WHERE project_id = ?1 AND agent_config_id = ?2 AND uri_segment = ?3
                       AND COALESCE(service_thread_id, '') = ?4
                       AND issue_id IS NULL AND parent_job_id IS NULL AND thread_id IS NULL
                     LIMIT 1",
                    params![
                        project_id.as_str(),
                        SERVICE_AGENT_CONFIG,
                        service.as_str(),
                        owner_thread.as_deref().unwrap_or("")
                    ],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                return row.text(0);
            }
            drop(rows);
            let id = ids::mint_child(&project_id);
            let at = now();
            let description = match owner_thread {
                None => format!("Project service `{service}`"),
                Some(_) => format!("Thread service `{service}`"),
            };
            conn.execute(
                "INSERT INTO jobs (id, project_id, agent_config_id, uri_segment, node_name, status,
                                   task_description, service_thread_id, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?4, 'service', ?5, ?6, ?7, ?7)",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    SERVICE_AGENT_CONFIG,
                    service.as_str(),
                    description,
                    owner_thread.as_deref(),
                    at
                ],
            )
            .await?;
            Ok(id)
        })
    })
    .await
    .map_err(|error| format!("Failed to ensure the service job: {error}"))
}

/// A new live run under the service's job.
pub(crate) async fn mint_run(
    db: &LocalDb,
    project_id: &str,
    job_id: &str,
) -> Result<String, String> {
    let run_id = ids::mint_child(job_id);
    let (project_id, job_id, id) = (project_id.to_string(), job_id.to_string(), run_id.clone());
    db.write(move |conn| {
        let (project_id, job_id, id) = (project_id.clone(), job_id.clone(), id.clone());
        Box::pin(async move {
            let at = now();
            conn.execute(
                "INSERT INTO runs (id, project_id, job_id, status, start_mode,
                                   started_at, created_at, updated_at)
                 VALUES (?1, ?2, ?3, 'live', ?4, ?5, ?5, ?5)",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    job_id.as_str(),
                    SERVICE_START_MODE,
                    at
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to mint a service run: {error}"))?;
    Ok(run_id)
}

/// Finalize a service run. Idempotent: an already-final run is left alone.
pub(crate) async fn finalize_run(
    db: &LocalDb,
    run_id: &str,
    exit_reason: &str,
) -> Result<(), String> {
    let (run_id, reason) = (run_id.to_string(), exit_reason.to_string());
    db.write(move |conn| {
        let (run_id, reason) = (run_id.clone(), reason.clone());
        Box::pin(async move {
            let at = now();
            conn.execute(
                "UPDATE runs SET status = 'exited', exit_reason = ?2, exited_at = ?3, updated_at = ?3
                 WHERE id = ?1 AND status IN ('starting', 'live')",
                params![run_id.as_str(), reason.as_str(), at],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to finalize a service run: {error}"))
}

/// Finalize every service run a predecessor runner left live. Supervisor
/// state is not persisted, so at startup none of them has a supervisor.
pub(crate) async fn finalize_orphaned_runs(db: &LocalDb) -> Result<u64, String> {
    db.write(move |conn| {
        Box::pin(async move {
            let at = now();
            let changed = conn
                .execute(
                    "UPDATE runs SET status = 'exited', exit_reason = 'runner restarted',
                                     exited_at = ?1, updated_at = ?1
                     WHERE start_mode = ?2 AND status IN ('starting', 'live')",
                    params![at, SERVICE_START_MODE],
                )
                .await?;
            Ok(changed)
        })
    })
    .await
    .map_err(|error| format!("Failed to finalize orphaned service runs: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn db() -> (tempfile::TempDir, LocalDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(dir.path().join("cairn.turso.db"))
            .await
            .unwrap();
        crate::project_services::testing::seed_project(&db).await;
        (dir, db)
    }

    /// A thread's service is a job of its own, beside a project service of the
    /// same name, whose home sits under the thread. It is not one of the
    /// thread's jobs: `thread_id` stays empty, so it never inherits what a
    /// thread job may do or appears in the thread's task list.
    #[tokio::test]
    async fn a_thread_service_job_is_homed_under_its_thread_without_being_the_threads() {
        let (_dir, db) = db().await;
        db.execute(
            "INSERT INTO threads (id, project_id, name, created_at, updated_at)
             VALUES ('t-1', 'p', 'spanish', 1, 1)",
            (),
        )
        .await
        .unwrap();
        let owner = ServiceOwner::Thread {
            id: "t-1".into(),
            name: "spanish".into(),
        };
        let project = ensure_service_job(&db, "p", &ServiceOwner::Project, "tutor")
            .await
            .unwrap();
        let thread = ensure_service_job(&db, "p", &owner, "tutor").await.unwrap();
        assert_ne!(project, thread);
        assert_eq!(
            ensure_service_job(&db, "p", &owner, "tutor").await.unwrap(),
            thread
        );
        assert_eq!(
            cairn_db::lineage::home_uri_for_job(&db, &thread)
                .await
                .unwrap(),
            Some("cairn://p/cairn/spanish/services/tutor".to_string())
        );
        assert_eq!(
            cairn_db::lineage::home_uri_for_job(&db, &project)
                .await
                .unwrap(),
            Some("cairn://p/cairn/services/tutor".to_string())
        );
        let thread_id = db
            .query_opt_text(
                "SELECT thread_id FROM jobs WHERE id = ?1",
                params![thread.as_str()],
            )
            .await
            .unwrap();
        assert_eq!(thread_id, None);
    }

    #[tokio::test]
    async fn a_service_job_is_one_row_and_its_home_is_the_service_uri() {
        let (_dir, db) = db().await;
        let project = ServiceOwner::Project;
        let job = ensure_service_job(&db, "p", &project, "echo")
            .await
            .unwrap();
        assert_eq!(
            ensure_service_job(&db, "p", &project, "echo")
                .await
                .unwrap(),
            job
        );
        assert_ne!(
            ensure_service_job(&db, "p", &project, "judge")
                .await
                .unwrap(),
            job
        );
        assert_eq!(
            cairn_db::lineage::home_uri_for_job(&db, &job)
                .await
                .unwrap(),
            Some("cairn://p/cairn/services/echo".to_string())
        );

        let run = mint_run(&db, "p", &job).await.unwrap();
        let status = db
            .query_opt_text(
                "SELECT status FROM runs WHERE id = ?1",
                params![run.as_str()],
            )
            .await
            .unwrap();
        assert_eq!(status.as_deref(), Some("live"));
        finalize_run(&db, &run, "stopped").await.unwrap();
        let status = db
            .query_opt_text(
                "SELECT status FROM runs WHERE id = ?1",
                params![run.as_str()],
            )
            .await
            .unwrap();
        assert_eq!(status.as_deref(), Some("exited"));

        let orphan = mint_run(&db, "p", &job).await.unwrap();
        assert_eq!(finalize_orphaned_runs(&db).await.unwrap(), 1);
        let reason = db
            .query_opt_text(
                "SELECT exit_reason FROM runs WHERE id = ?1",
                params![orphan.as_str()],
            )
            .await
            .unwrap();
        assert_eq!(reason.as_deref(), Some("runner restarted"));
    }
}
