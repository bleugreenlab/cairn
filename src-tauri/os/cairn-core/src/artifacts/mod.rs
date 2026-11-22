//! Artifact query operations.

pub mod queries;

use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, RowExt};

/// Resolve the producing job's logical bookmark, never a resident checkout's HEAD.
/// Team repositories use this install's local route, not a synced foreign path.
/// Provenance is diagnostic: a branchless job or a failed lookup stores unknown
/// provenance, never blocking the artifact write itself.
pub(crate) async fn written_at_commit(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> Option<String> {
    match resolve_written_at_commit(orch, db, job_id).await {
        Ok(commit) => commit,
        Err(error) => {
            log::warn!("Artifact provenance unavailable for job {job_id}: {error}");
            None
        }
    }
}

async fn resolve_written_at_commit(
    orch: &Orchestrator,
    db: &crate::storage::LocalDb,
    job_id: &str,
) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    let (branch, project_id) = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT branch, project_id FROM jobs WHERE id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::internal(format!("Artifact job {job_id} not found")))?;
                Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .map_err(|e| format!("Failed to resolve artifact provenance: {e}"))?;
    let Some(branch) = branch.filter(|branch| !branch.trim().is_empty()) else {
        return Ok(None);
    };
    let project_id =
        project_id.ok_or_else(|| "Artifact job has a branch but no project".to_string())?;
    let (repository, _) =
        crate::projects::crud::resolve_local_repo_path_and_key(&orch.db, &project_id)
            .await
            .map_err(|e| format!("Failed to resolve artifact repository: {e}"))?;
    let repository = repository
        .filter(|path| !path.trim().is_empty())
        .ok_or_else(|| "Artifact job has a branch but no local project repository".to_string())?;
    crate::fleet::residency::resolve_logical_commit(
        orch,
        std::path::Path::new(&repository),
        &branch,
    )
    .await
    .map(Some)
}
