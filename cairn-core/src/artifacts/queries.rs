//! Artifact query operations.

use serde_json::Value;
use turso::params;

use crate::models::Artifact;
use crate::storage::{DbError, LocalDb, RowExt};

pub(crate) const ARTIFACT_COLUMNS: &str =
    "id, job_id, artifact_type, schema_version, data, version,
    parent_version_id, output_name, created_at, updated_at, seen_at, confirmed";

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

pub(crate) fn artifact_from_row(row: &turso::Row) -> Result<Artifact, DbError> {
    let data: Value = serde_json::from_str(&row.text(4)?)
        .map_err(|e| DbError::internal(format!("Invalid artifact data JSON: {e}")))?;

    Ok(Artifact {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        artifact_type: row.text(2)?,
        schema_version: row.i64(3)? as i32,
        data,
        version: row.i64(5)? as i32,
        parent_version_id: row.opt_text(6)?,
        output_name: row.opt_text(7)?,
        created_at: row.i64(8)?,
        updated_at: row.i64(9)?,
        seen_at: row.opt_i64(10)?,
        confirmed: row.i64(11)? != 0,
    })
}

pub async fn get(db: &LocalDb, artifact_id: &str) -> Result<Artifact, String> {
    let artifact_id = artifact_id.to_string();
    db.query_opt(
        format!("SELECT {ARTIFACT_COLUMNS} FROM artifacts WHERE id = ?1"),
        params![artifact_id.as_str()],
        artifact_from_row,
    )
    .await
    .and_then(|artifact| {
        artifact.ok_or_else(|| DbError::internal(format!("Artifact not found: {artifact_id}")))
    })
    .map_err(|e| db_error("Failed to get artifact", e))
}

pub async fn list(db: &LocalDb, job_id: &str) -> Result<Vec<Artifact>, String> {
    let job_id = job_id.to_string();
    db.query_all(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM artifacts
             WHERE job_id = ?1
             ORDER BY version DESC"
        ),
        params![job_id.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to list artifacts", e))
}

pub async fn get_latest(db: &LocalDb, job_id: &str) -> Result<Option<Artifact>, String> {
    let job_id = job_id.to_string();
    db.query_opt(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM artifacts
             WHERE job_id = ?1
             ORDER BY version DESC
             LIMIT 1"
        ),
        params![job_id.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to get latest artifact", e))
}

pub async fn mark_seen(db: &LocalDb, artifact_id: &str) -> Result<(), String> {
    let seen_at = chrono::Utc::now().timestamp();
    mark_seen_at(db, artifact_id, seen_at).await
}

pub async fn mark_seen_at(db: &LocalDb, artifact_id: &str, seen_at: i64) -> Result<(), String> {
    let artifact_id = artifact_id.to_string();
    db.execute(
        "UPDATE artifacts SET seen_at = ?1 WHERE id = ?2",
        params![seen_at, artifact_id.as_str()],
    )
    .await
    .map(|_| ())
    .map_err(|e| db_error("Failed to mark artifact seen", e))
}

pub async fn list_for_issue(db: &LocalDb, issue_id: &str) -> Result<Vec<Artifact>, String> {
    let issue_id = issue_id.to_string();
    db.query_all(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM (
                SELECT
                    a.id, a.job_id, a.artifact_type, a.schema_version, a.data,
                    a.version, a.parent_version_id, a.output_name, a.created_at,
                    a.updated_at, a.seen_at, a.confirmed,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.job_id
                        ORDER BY a.version DESC
                    ) AS artifact_rank
                FROM artifacts a
                INNER JOIN jobs j ON j.id = a.job_id
                WHERE j.issue_id = ?1
             ) ranked_artifacts
             WHERE artifact_rank = 1"
        ),
        params![issue_id.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to list artifacts for issue", e))
}
