//! Checkpoint cache query operations.
//!
//! Caching system for command results at checkpoint nodes to avoid
//! re-executing expensive operations.

use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// Result of querying the checkpoint command cache for a job.
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointCacheResult {
    pub command: String,
    pub exit_code: i32,
    pub commit_sha: String,
    pub is_valid: bool,
    pub ran_at: i32,
}

/// Normalize a shell command string for stable cache key comparison.
pub(crate) fn normalize_command(cmd: &str) -> String {
    cmd.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn get_current_head_sha(worktree_path: &str) -> Result<String, String> {
    let output = crate::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("git rev-parse failed: {}", e))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        Err("Failed to get HEAD".to_string())
    }
}

pub(crate) fn is_worktree_dirty(worktree_path: &str) -> Result<bool, String> {
    let output = crate::env::git()
        .args(["status", "--porcelain"])
        .current_dir(worktree_path)
        .output()
        .map_err(|e| format!("git status failed: {}", e))?;

    Ok(!output.stdout.is_empty())
}

/// Get the checkpoint cache result for a job.
/// Returns the cached CI/checkpoint command result if one exists.
pub fn get_checkpoint_cache_result_impl(
    orch: &Orchestrator,
    job_id: &str,
) -> Result<Option<CheckpointCacheResult>, String> {
    let db = orch.db.local.clone();
    let job_id = job_id.to_string();
    run_checkpoint_cache_db(async move {
        db.read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "
                        SELECT c.command, c.exit_code, c.commit_sha, c.is_dirty,
                               c.ran_at, j.worktree_path
                        FROM checkpoint_command_cache c
                        JOIN jobs j ON j.id = c.job_id
                        WHERE c.job_id = ?1
                        ORDER BY c.ran_at DESC
                        LIMIT 1
                        ",
                        (job_id.as_str(),),
                    )
                    .await?;

                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };

                let command = row.text(0)?;
                let exit_code = row.i64(1)? as i32;
                let commit_sha = row.text(2)?;
                let is_dirty = row.i64(3)?;
                let ran_at = row.i64(4)? as i32;
                let worktree_path = row.opt_text(5)?;

                let is_valid = if let Some(worktree) = &worktree_path {
                    let current_sha = get_current_head_sha(worktree).unwrap_or_default();
                    let currently_dirty = is_worktree_dirty(worktree).unwrap_or(true);
                    commit_sha == current_sha && is_dirty == 0 && !currently_dirty
                } else {
                    false
                };

                Ok(Some(CheckpointCacheResult {
                    command,
                    exit_code,
                    commit_sha: commit_sha[..7.min(commit_sha.len())].to_string(),
                    is_valid,
                    ran_at,
                }))
            })
        })
        .await
        .map_err(|e| e.to_string())
    })
}

fn run_checkpoint_cache_db<T>(
    future: impl std::future::Future<Output = Result<T, String>> + Send + 'static,
) -> Result<T, String>
where
    T: Send + 'static,
{
    fn run<T>(future: impl std::future::Future<Output = Result<T, String>>) -> Result<T, String> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?
            .block_on(future)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(move || run(future))
            .join()
            .map_err(|_| "Checkpoint cache DB runtime thread panicked".to_string())?
    } else {
        run(future)
    }
}
