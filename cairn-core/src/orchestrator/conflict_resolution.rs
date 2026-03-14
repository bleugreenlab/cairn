//! Automatic conflict resolution for sibling PRs.
//!
//! When a PR is merged, sibling PRs in the same project may develop merge
//! conflicts. This module detects conflicting PRs and auto-resumes their
//! parent builder jobs to rebase and push updated branches.

use crate::schema::{action_runs, jobs, pr_data, projects, runs};
use diesel::prelude::*;

use super::Orchestrator;

/// Check if a PR is conflicting and resume the parent builder if idle.
///
/// Returns `Ok(Some(job_id))` if the builder was resumed, `Ok(None)` if
/// not needed (PR not conflicting, no parent job, or builder already running).
pub fn try_resume_for_conflict(
    orch: &Orchestrator,
    pr_data_id: &str,
) -> Result<Option<String>, String> {
    let (parent_job_id, pr_number, default_branch) = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        // Verify PR is open and conflicting
        let pr_info: Option<(String, String, i32)> = pr_data::table
            .inner_join(
                action_runs::table.on(pr_data::action_run_id.eq(action_runs::id.nullable())),
            )
            .filter(pr_data::id.eq(pr_data_id))
            .filter(pr_data::pr_status.eq("open"))
            .filter(pr_data::mergeable.eq(Some("CONFLICTING")))
            .select((
                action_runs::parent_job_id.assume_not_null(),
                action_runs::project_id,
                pr_data::pr_number,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| e.to_string())?;

        let Some((parent_job_id, project_id, pr_number)) = pr_info else {
            return Ok(None); // PR not open+conflicting or no parent job
        };

        // Get the project's default branch for the rebase message
        let default_branch: String = projects::table
            .find(&project_id)
            .select(projects::default_branch)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
            .unwrap_or_else(|| "main".to_string());

        // Verify the parent job has a claude_session_id
        let has_session: bool = jobs::table
            .find(&parent_job_id)
            .select(jobs::claude_session_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
            .is_some();

        if !has_session {
            log::info!(
                "Skipping conflict auto-resume for job {}: no claude_session_id",
                &parent_job_id[..parent_job_id.len().min(8)]
            );
            return Ok(None);
        }

        // Guard: check no run is currently "running" for this job
        let running_count: i64 = runs::table
            .filter(runs::job_id.eq(&parent_job_id))
            .filter(runs::status.eq("running"))
            .count()
            .first(&mut *conn)
            .unwrap_or(0);

        if running_count > 0 {
            log::info!(
                "Skipping conflict auto-resume for job {}: already has a running run",
                &parent_job_id[..parent_job_id.len().min(8)]
            );
            return Ok(None);
        }

        (parent_job_id, pr_number, default_branch)
    };

    let conflict_message = format!(
        "Your PR has merge conflicts with the base branch. Please rebase on the latest {branch}, \
         resolve any conflicts, and push the updated branch.\n\
         \n\
         Steps:\n\
         1. git fetch origin {branch}\n\
         2. git rebase origin/{branch}\n\
         3. Resolve any merge conflicts\n\
         4. git push --force-with-lease\n\
         \n\
         After resolving, verify the code still builds and tests pass.",
        branch = default_branch,
    );

    let result =
        crate::execution::jobs::continue_job_impl(orch, &parent_job_id, Some(&conflict_message));

    match result {
        Ok(_run) => {
            log::info!(
                "Auto-resumed builder {} for PR #{} conflict resolution",
                &parent_job_id[..parent_job_id.len().min(8)],
                pr_number,
            );
            Ok(Some(parent_job_id))
        }
        Err(e) => {
            log::warn!(
                "Failed to auto-resume builder {} for PR #{}: {}",
                &parent_job_id[..parent_job_id.len().min(8)],
                pr_number,
                e
            );
            Err(e)
        }
    }
}

/// Find all open tracked PRs for a project, excluding one (e.g. the just-merged PR).
///
/// Returns `(pr_data_id, action_run_id)` pairs.
pub fn find_open_prs_for_project(
    orch: &Orchestrator,
    project_id: &str,
    exclude_pr_data_id: Option<&str>,
) -> Result<Vec<(String, String)>, String> {
    let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

    let mut query = pr_data::table
        .inner_join(action_runs::table.on(pr_data::action_run_id.eq(action_runs::id.nullable())))
        .filter(action_runs::project_id.eq(project_id))
        .filter(pr_data::pr_status.eq("open"))
        .select((pr_data::id, pr_data::action_run_id.assume_not_null()))
        .into_boxed();

    if let Some(exclude_id) = exclude_pr_data_id {
        query = query.filter(pr_data::id.ne(exclude_id));
    }

    query.load(&mut *conn).map_err(|e| e.to_string())
}
