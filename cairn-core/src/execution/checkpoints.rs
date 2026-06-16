//! Checkpoint approval flow for recipe execution.
//!
//! Human-in-the-loop checkpoints where agents wait for user approval
//! before continuing execution.
//!
//! These are internal helpers called from advancement logic.
//! The Tauri layer wraps these as `#[tauri::command]` functions.

use uuid::Uuid;

use crate::db_records::{db_job_from_row, DbJob, JOB_COLUMNS};
use crate::models::{Artifact, Job};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb};
use turso::params;

/// Approve a blocked checkpoint job.
/// Marks the job as complete, then advances the DAG.
/// Returns the list of newly ready agent jobs after advancement.
pub async fn approve_job_inner(orch: &Orchestrator, job_id: &str) -> Result<Vec<Job>, String> {
    validate_confirmable(orch.db.local.as_ref(), job_id).await?;

    // Record the resolution fact (artifact.confirmed), then let the projection
    // derive Complete and advance the DAG. Agent jobs from the resulting
    // AdvanceDag are started by the executor directly.
    let db = orch.db.local.clone();
    let job_id_owned = job_id.to_string();
    let artifact = db
        .write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move { confirm_latest_artifact_conn(conn, &job_id).await })
        })
        .await
        .map_err(|e| e.to_string())?;
    orch.notifier.artifact(&artifact);
    crate::execution::advancement::recompute_job(orch, job_id)?;
    Ok(vec![])
}

/// Confirm a job's resolution (artifact.confirmed) and recompute its status.
/// Public orchestrator-level entry for hosts that approve/complete a job.
pub fn confirm_job(orch: &Orchestrator, job_id: &str) -> Result<(), String> {
    let db = orch.db.local.clone();
    let job_id_owned = job_id.to_string();
    let artifact = crate::execution::advancement::run_advancement_db(async move {
        db.write(|conn| {
            let job_id = job_id_owned.clone();
            Box::pin(async move { confirm_latest_artifact_conn(conn, &job_id).await })
        })
        .await
        .map_err(|e| e.to_string())
    })?;
    orch.notifier.artifact(&artifact);
    crate::execution::advancement::recompute_job(orch, job_id)
}

/// Validate that a job's resolution is confirmable. An artifact is confirmable
/// whenever it exists and is not yet confirmed — regardless of whether the
/// producing agent is still running (e.g. during the post-completion memory
/// review). A Blocked job with no artifact yet (a command checkpoint, or a
/// missing-output soft-lock) is also confirmable as an override. Confirmation is
/// therefore decoupled from `job.status === blocked` (CAIRN-1576).
async fn validate_confirmable(db: &LocalDb, job_id: &str) -> Result<DbJob, String> {
    let job_id = job_id.to_string();

    db.write(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let job = load_job_by_id_conn(conn, &job_id)
                .await?
                .ok_or_else(|| DbError::internal(format!("Job not found: {job_id}")))?;

            let has_unconfirmed_artifact = {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM artifacts WHERE job_id = ?1 AND confirmed = 0 LIMIT 1",
                        (job_id.as_str(),),
                    )
                    .await?;
                rows.next().await?.is_some()
            };

            if !has_unconfirmed_artifact && job.status != "blocked" {
                return Err(DbError::internal(format!(
                    "Job has no unconfirmed artifact to confirm (current status: {})",
                    job.status
                )));
            }

            Ok(job)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

async fn load_job_by_id_conn(conn: &turso::Connection, job_id: &str) -> DbResult<Option<DbJob>> {
    let mut rows = conn
        .query(
            &format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1"),
            (job_id,),
        )
        .await?;
    rows.next()
        .await?
        .map(|row| db_job_from_row(&row))
        .transpose()
}

/// Confirm a job's latest artifact (the approve resolution fact). Creates a
/// minimal `checkpoint` artifact if the job has none (a standalone checkpoint
/// node approved without ever producing an agent artifact).
pub(crate) async fn confirm_latest_artifact_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<Artifact> {
    let now = chrono::Utc::now().timestamp() as i32;
    let existing = {
        let mut rows = conn
            .query(
                format!(
                    "SELECT {} FROM artifacts WHERE job_id = ?1 ORDER BY version DESC LIMIT 1",
                    crate::artifacts::queries::ARTIFACT_COLUMNS
                ),
                (job_id,),
            )
            .await?;
        rows.next()
            .await?
            .map(|row| crate::artifacts::queries::artifact_from_row(&row))
            .transpose()?
    };
    match existing {
        Some(mut artifact) => {
            conn.execute(
                "UPDATE artifacts SET confirmed = 1, updated_at = ?1 WHERE id = ?2",
                params![now, artifact.id.as_str()],
            )
            .await?;
            artifact.confirmed = true;
            artifact.updated_at = now as i64;
            Ok(artifact)
        }
        None => {
            let artifact_id = Uuid::new_v4().to_string();
            conn.execute(
                "INSERT INTO artifacts (id, job_id, artifact_type, schema_version, data, version,
                                        created_at, updated_at, confirmed)
                 VALUES (?1, ?2, 'checkpoint', 1, '{}', 1, ?3, ?3, 1)",
                params![artifact_id.as_str(), job_id, now],
            )
            .await?;
            Ok(Artifact {
                id: artifact_id,
                job_id: Some(job_id.to_string()),
                artifact_type: "checkpoint".to_string(),
                schema_version: 1,
                data: serde_json::json!({}),
                version: 1,
                parent_version_id: None,
                output_name: None,
                created_at: now as i64,
                updated_at: now as i64,
                seen_at: None,
                confirmed: true,
            })
        }
    }
}

/// Ensure a blockable checkpoint job has an (unconfirmed) artifact whose
/// `confirmed` flag is the gate the projection reads. Used when a standalone
/// checkpoint node arms, so approve has a row to flip.
pub(crate) async fn ensure_checkpoint_artifact_conn(
    conn: &turso::Connection,
    job_id: &str,
) -> DbResult<()> {
    let exists = {
        let mut rows = conn
            .query(
                "SELECT 1 FROM artifacts WHERE job_id = ?1 LIMIT 1",
                (job_id,),
            )
            .await?;
        rows.next().await?.is_some()
    };
    if !exists {
        let now = chrono::Utc::now().timestamp() as i32;
        let artifact_id = Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO artifacts (id, job_id, artifact_type, schema_version, data, version,
                                    created_at, updated_at, confirmed)
             VALUES (?1, ?2, 'checkpoint', 1, '{}', 1, ?3, ?3, 0)",
            params![artifact_id.as_str(), job_id, now],
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod confirmable_tests {
    use super::*;

    async fn seed_job(db: &LocalDb, status: &str) {
        db.execute_script(&format!(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues (id,project_id,number,title,status,attention,created_at,updated_at) VALUES ('i','p',1,'T','active','none',1,1);
             INSERT INTO jobs (id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at) VALUES ('j','i','p','{status}','b','b',1,1);"
        ))
        .await
        .unwrap();
    }

    async fn insert_artifact(db: &LocalDb, confirmed: i64) {
        db.execute(
            "INSERT INTO artifacts (id,job_id,artifact_type,schema_version,data,version,created_at,updated_at,confirmed)
             VALUES ('a','j','plan',1,'{}',1,1,1,?1)",
            (confirmed,),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn running_job_with_unconfirmed_artifact_is_confirmable() {
        // The key CAIRN-1576 guarantee: an unconfirmed artifact is confirmable
        // even while the producing agent is Running (e.g. mid memory review).
        let db = crate::storage::migrated_test_db("confirmable-running.db").await;
        seed_job(&db, "running").await;
        insert_artifact(&db, 0).await;
        assert!(validate_confirmable(&db, "j").await.is_ok());
    }

    #[tokio::test]
    async fn running_job_with_only_confirmed_artifact_is_not_confirmable() {
        let db = crate::storage::migrated_test_db("confirmable-confirmed.db").await;
        seed_job(&db, "running").await;
        insert_artifact(&db, 1).await;
        assert!(validate_confirmable(&db, "j").await.is_err());
    }

    #[tokio::test]
    async fn blocked_job_without_artifact_is_confirmable() {
        // Command checkpoint / missing-output override: still confirmable.
        let db = crate::storage::migrated_test_db("confirmable-blocked.db").await;
        seed_job(&db, "blocked").await;
        assert!(validate_confirmable(&db, "j").await.is_ok());
    }
}
