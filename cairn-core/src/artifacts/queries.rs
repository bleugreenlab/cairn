//! Read-only artifact queries.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::diesel_models::DbArtifact;
use crate::models::Artifact;
use crate::schema::{artifacts, jobs};

/// Get a specific artifact by ID.
pub fn get(conn: &mut SqliteConnection, artifact_id: &str) -> Result<Artifact, String> {
    let db_artifact: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Artifact not found: {}", e))?;

    Artifact::try_from(db_artifact).map_err(|e| format!("Failed to parse artifact: {}", e))
}

/// Get all artifacts for a job, ordered by version (newest first).
pub fn list(conn: &mut SqliteConnection, job_id: &str) -> Result<Vec<Artifact>, String> {
    let db_artifacts: Vec<DbArtifact> = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .order(artifacts::version.desc())
        .load(conn)
        .map_err(|e| format!("Failed to query artifacts: {}", e))?;

    db_artifacts
        .into_iter()
        .map(|db| Artifact::try_from(db).map_err(|e| format!("Failed to parse artifact: {}", e)))
        .collect()
}

/// Get the latest artifact for a job.
pub fn get_latest(conn: &mut SqliteConnection, job_id: &str) -> Result<Option<Artifact>, String> {
    let db_artifact: Option<DbArtifact> = artifacts::table
        .filter(artifacts::job_id.eq(job_id))
        .order(artifacts::version.desc())
        .first(conn)
        .optional()
        .map_err(|e| format!("Failed to query artifact: {}", e))?;

    match db_artifact {
        Some(db) => Artifact::try_from(db)
            .map(Some)
            .map_err(|e| format!("Failed to parse artifact: {}", e)),
        None => Ok(None),
    }
}

/// Get the latest artifact for each job in an issue.
pub fn list_for_issue(
    conn: &mut SqliteConnection,
    issue_id: &str,
) -> Result<Vec<Artifact>, String> {
    let job_ids: Vec<String> = jobs::table
        .filter(jobs::issue_id.eq(issue_id))
        .select(jobs::id)
        .load(conn)
        .map_err(|e| format!("Failed to query jobs: {}", e))?;

    let mut result = Vec::new();
    for job_id in job_ids {
        let db_artifact: Option<DbArtifact> = artifacts::table
            .filter(artifacts::job_id.eq(&job_id))
            .order(artifacts::version.desc())
            .first(conn)
            .optional()
            .map_err(|e| format!("Failed to query artifact: {}", e))?;

        if let Some(db) = db_artifact {
            let artifact =
                Artifact::try_from(db).map_err(|e| format!("Failed to parse artifact: {}", e))?;
            result.push(artifact);
        }
    }

    Ok(result)
}
