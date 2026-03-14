//! Checkpoint cache query operations.
//!
//! Caching system for command results at checkpoint nodes to avoid
//! re-executing expensive operations.

use crate::diesel_models::{DbCheckpointCommandCache, DbJob};
use crate::orchestrator::Orchestrator;
use crate::schema::{checkpoint_command_cache, jobs};
use diesel::prelude::*;

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
    let mut conn = orch
        .db
        .conn
        .lock()
        .map_err(|e| format!("DB lock error: {}", e))?;

    // Get cached result for this job
    let cached: Option<DbCheckpointCommandCache> = checkpoint_command_cache::table
        .filter(checkpoint_command_cache::job_id.eq(job_id))
        .order(checkpoint_command_cache::ran_at.desc())
        .first(&mut *conn)
        .ok();

    let Some(cached) = cached else {
        return Ok(None);
    };

    // Get job's worktree to check current validity
    let job: DbJob = jobs::table
        .find(job_id)
        .first(&mut *conn)
        .map_err(|e| format!("Job not found: {}", e))?;

    let is_valid = if let Some(worktree) = &job.worktree_path {
        let current_sha = get_current_head_sha(worktree).unwrap_or_default();
        let currently_dirty = is_worktree_dirty(worktree).unwrap_or(true);
        cached.commit_sha == current_sha && cached.is_dirty == 0 && !currently_dirty
    } else {
        false
    };

    Ok(Some(CheckpointCacheResult {
        command: cached.command,
        exit_code: cached.exit_code,
        commit_sha: cached.commit_sha[..7.min(cached.commit_sha.len())].to_string(),
        is_valid,
        ran_at: cached.ran_at,
    }))
}
