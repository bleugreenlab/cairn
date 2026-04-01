//! Automatic conflict resolution for sibling PRs.
//!
//! When a PR is merged, sibling PRs in the same project may develop merge
//! conflicts. This module detects conflicting PRs and auto-resumes their
//! parent builder jobs to rebase and push updated branches.

use crate::schema::{issues, jobs, merge_requests, projects, runs};
use diesel::prelude::*;

use super::Orchestrator;

/// Check if a PR is conflicting and resume the parent builder if idle.
///
/// Returns `Ok(Some(job_id))` if the builder was resumed, `Ok(None)` if
/// not needed (PR not conflicting, no parent job, or builder already running).
pub fn try_resume_for_conflict(orch: &Orchestrator, mr_id: &str) -> Result<Option<String>, String> {
    let (parent_job_id, pr_number, default_branch) = {
        let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

        // Verify PR is open and conflicting
        let mr_info = merge_requests::table
            .filter(merge_requests::id.eq(mr_id))
            .filter(merge_requests::status.eq("open"))
            .filter(merge_requests::github_mergeable.eq(Some("CONFLICTING")))
            .select((
                merge_requests::job_id,
                merge_requests::project_id,
                merge_requests::github_pr_number,
                merge_requests::issue_id,
            ))
            .first::<(String, String, Option<i32>, Option<String>)>(&mut *conn)
            .optional()
            .map_err(|e| e.to_string())?;

        let Some((job_id, project_id, pr_number_opt, issue_id)) = mr_info else {
            return Ok(None); // PR not open+conflicting or no parent job
        };

        let parent_job_id = job_id;

        // Skip auto-resume for backlog issues — the user shelved it intentionally
        if let Some(ref issue_id) = issue_id {
            let issue_status: Option<String> = issues::table
                .find(issue_id)
                .select(issues::status)
                .first::<String>(&mut *conn)
                .ok();
            if issue_status.as_deref() == Some("backlog") {
                log::info!(
                    "Skipping conflict auto-resume for job {}: issue is in backlog",
                    &parent_job_id[..parent_job_id.len().min(8)]
                );
                return Ok(None);
            }
        }

        // Get the project's default branch for the rebase message
        let default_branch: String = projects::table
            .find(&project_id)
            .select(projects::default_branch)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
            .unwrap_or_else(|| "main".to_string());

        // Verify the parent job has a current_session_id
        let has_session: bool = jobs::table
            .find(&parent_job_id)
            .select(jobs::current_session_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten()
            .is_some();

        if !has_session {
            log::info!(
                "Skipping conflict auto-resume for job {}: no current_session_id",
                &parent_job_id[..parent_job_id.len().min(8)]
            );
            return Ok(None);
        }

        // Guard: check no run is currently "running" for this job
        let running_count: i64 = runs::table
            .filter(runs::job_id.eq(&parent_job_id))
            .filter(runs::status.eq_any(&["starting", "live"]))
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

        (parent_job_id, pr_number_opt.unwrap_or(0), default_branch)
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

    let result = crate::execution::jobs::continue_job_impl(
        orch,
        &parent_job_id,
        Some(&conflict_message),
        None,
    );

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

/// Check if a PR's issue is in backlog status.
///
/// Returns `true` if the issue exists and has status "backlog".
/// Used by tests to verify the backlog guard without needing a full Orchestrator.
#[cfg(test)]
pub fn is_issue_backlog(conn: &mut diesel::sqlite::SqliteConnection, mr_id: &str) -> bool {
    use diesel::prelude::*;

    let issue_id: Option<String> = merge_requests::table
        .filter(merge_requests::id.eq(mr_id))
        .select(merge_requests::issue_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    if let Some(ref issue_id) = issue_id {
        let status: Option<String> = issues::table
            .find(issue_id)
            .select(issues::status)
            .first::<String>(conn)
            .ok();
        status.as_deref() == Some("backlog")
    } else {
        false
    }
}

/// Find all open tracked PRs for a project, excluding one (e.g. the just-merged PR).
///
/// Returns `(mr_id, job_id)` pairs.
pub fn find_open_mrs_for_project(
    orch: &Orchestrator,
    project_id: &str,
    exclude_mr_id: Option<&str>,
) -> Result<Vec<(String, String)>, String> {
    let mut conn = orch.db.conn.lock().map_err(|e| e.to_string())?;

    let mut query = merge_requests::table
        .filter(merge_requests::project_id.eq(project_id))
        .filter(merge_requests::status.eq("open"))
        .select((merge_requests::id, merge_requests::job_id))
        .into_boxed();

    if let Some(exclude_id) = exclude_mr_id {
        query = query.filter(merge_requests::id.ne(exclude_id));
    }

    query.load(&mut *conn).map_err(|e| e.to_string())
}

/// Deprecated alias for find_open_mrs_for_project.
pub fn find_open_prs_for_project(
    orch: &Orchestrator,
    project_id: &str,
    exclude_pr_data_id: Option<&str>,
) -> Result<Vec<(String, String)>, String> {
    find_open_mrs_for_project(orch, project_id, exclude_pr_data_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewMergeRequest;
    use crate::schema::merge_requests;
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };

    /// Set up the common test scenario: project, issue, job, merge_request.
    /// Returns (mr_id, job_id, issue_id).
    fn setup_conflicting_pr(
        conn: &mut diesel::sqlite::SqliteConnection,
        issue_status: &str,
    ) -> (String, String, String) {
        let project_id = create_test_project(conn, "Test", "TST");
        let issue_id = create_test_issue(conn, &project_id, "Test issue");
        let job_id = create_test_job(conn, &issue_id, &project_id, "build", "completed", None);
        let now = chrono::Utc::now().timestamp() as i32;

        // Set issue status
        diesel::update(issues::table.find(&issue_id))
            .set(issues::status.eq(issue_status))
            .execute(conn)
            .unwrap();

        // Set current_session_id on the job so it passes the session check
        diesel::update(crate::schema::jobs::table.find(&job_id))
            .set(crate::schema::jobs::current_session_id.eq(Some("session-123")))
            .execute(conn)
            .unwrap();

        // Create merge_request: open + conflicting
        let mr_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(merge_requests::table)
            .values(NewMergeRequest {
                id: &mr_id,
                job_id: &job_id,
                project_id: &project_id,
                issue_id: Some(&issue_id),
                manager_id: None,
                title: "Test PR",
                body: None,
                source_branch: "agent/test-branch",
                target_branch: "main",
                status: "open",
                merge_method: "merge",
                additions: None,
                deletions: None,
                changed_files: None,
                commit_count: None,
                checks_json: None,
                checks_status: None,
                opened_at: now,
                updated_at: now,
                github_pr_number: Some(42),
                github_pr_url: Some("https://github.com/test/test/pull/42"),
                github_state: Some("OPEN"),
            })
            .execute(conn)
            .unwrap();
        diesel::update(merge_requests::table.filter(merge_requests::id.eq(&mr_id)))
            .set(merge_requests::github_mergeable.eq(Some("CONFLICTING")))
            .execute(conn)
            .unwrap();

        (mr_id, job_id, issue_id)
    }

    /// Set up a conflicting merge_request with no issue_id.
    /// Returns (mr_id, job_id).
    fn setup_conflicting_pr_no_issue(
        conn: &mut diesel::sqlite::SqliteConnection,
    ) -> (String, String) {
        let project_id = create_test_project(conn, "Test", "TST");
        // We still need a job — create a temporary issue just for the job FK,
        // but set merge_request.issue_id = None to simulate the no-issue path.
        let issue_id = create_test_issue(conn, &project_id, "Temp issue");
        let job_id = create_test_job(conn, &issue_id, &project_id, "build", "completed", None);
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::update(crate::schema::jobs::table.find(&job_id))
            .set(crate::schema::jobs::current_session_id.eq(Some("session-123")))
            .execute(conn)
            .unwrap();

        let mr_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(merge_requests::table)
            .values(NewMergeRequest {
                id: &mr_id,
                job_id: &job_id,
                project_id: &project_id,
                issue_id: None,
                manager_id: None,
                title: "Test PR",
                body: None,
                source_branch: "agent/test-branch",
                target_branch: "main",
                status: "open",
                merge_method: "merge",
                additions: None,
                deletions: None,
                changed_files: None,
                commit_count: None,
                checks_json: None,
                checks_status: None,
                opened_at: now,
                updated_at: now,
                github_pr_number: Some(42),
                github_pr_url: Some("https://github.com/test/test/pull/42"),
                github_state: Some("OPEN"),
            })
            .execute(conn)
            .unwrap();
        diesel::update(merge_requests::table.filter(merge_requests::id.eq(&mr_id)))
            .set(merge_requests::github_mergeable.eq(Some("CONFLICTING")))
            .execute(conn)
            .unwrap();

        (mr_id, job_id)
    }

    #[test]
    fn no_issue_is_not_detected_as_backlog() {
        let mut conn = test_diesel_conn();
        let (mr_id, _job_id) = setup_conflicting_pr_no_issue(&mut conn);

        assert!(!is_issue_backlog(&mut conn, &mr_id));
    }

    #[test]
    fn try_resume_proceeds_past_guard_when_no_issue() {
        // When merge_request has no issue_id, the backlog guard should be skipped.
        // We prove this by showing the function reaches continue_job_impl (returns Err)
        // rather than returning Ok(None) from the backlog guard.
        let mut conn = test_diesel_conn();
        let (mr_id, _job_id) = setup_conflicting_pr_no_issue(&mut conn);

        let db = std::sync::Arc::new(crate::db::DbState {
            conn: std::sync::Mutex::new(conn),
        });

        let mut mock_process = crate::services::testing::MockProcessSpawner::new();
        mock_process
            .expect_spawn()
            .returning(|_| Err("mock: not a real process".to_string()));

        let services = std::sync::Arc::new(
            crate::services::testing::TestServicesBuilder::new()
                .with_process(mock_process)
                .build(),
        );
        let account_manager = std::sync::Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = std::sync::Arc::new(std::sync::Mutex::new(None));
        let orch = Orchestrator {
            db,
            services: services.clone(),
            process_state: std::sync::Arc::new(
                crate::agent_process::process::AgentProcessState::default(),
            ),
            mcp_auth: std::sync::Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: std::sync::Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            identity_store: std::sync::Arc::new(std::sync::Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: std::sync::Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        };

        let result = try_resume_for_conflict(&orch, &mr_id);
        // Should NOT be Ok(None) — that would mean the backlog guard blocked it.
        // Instead it should be Err(...) because continue_job_impl fails in test.
        assert!(
            result.is_err(),
            "expected Err from continue_job_impl, got {:?}",
            result
        );
    }

    #[test]
    fn backlog_issue_is_detected_by_guard() {
        let mut conn = test_diesel_conn();
        let (mr_id, _job_id, _issue_id) = setup_conflicting_pr(&mut conn, "backlog");

        assert!(is_issue_backlog(&mut conn, &mr_id));
    }

    #[test]
    fn active_issue_is_not_detected_as_backlog() {
        let mut conn = test_diesel_conn();
        let (mr_id, _job_id, _issue_id) = setup_conflicting_pr(&mut conn, "active");

        assert!(!is_issue_backlog(&mut conn, &mr_id));
    }

    #[test]
    fn try_resume_skips_backlog_issue() {
        let mut conn = test_diesel_conn();
        let (mr_id, _job_id, _issue_id) = setup_conflicting_pr(&mut conn, "backlog");

        // Build a test orchestrator
        let db = std::sync::Arc::new(crate::db::DbState {
            conn: std::sync::Mutex::new(conn),
        });
        let services =
            std::sync::Arc::new(crate::services::testing::TestServicesBuilder::new().build());
        let account_manager = std::sync::Arc::new(crate::orchestrator::AccountManager::new(
            db.clone(),
            services.emitter.clone(),
        ));
        let sync_tx = std::sync::Arc::new(std::sync::Mutex::new(None));
        let orch = Orchestrator {
            db,
            services: services.clone(),
            process_state: std::sync::Arc::new(
                crate::agent_process::process::AgentProcessState::default(),
            ),
            mcp_auth: std::sync::Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
                "/tmp",
            ))),
            warm_gc: None,
            pty_state: std::sync::Arc::new(crate::services::PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            identity_store: std::sync::Arc::new(std::sync::Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: std::path::PathBuf::from("/tmp"),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: std::sync::Arc::new(std::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            provider_usage_snapshots: Default::default(),
            executor: std::sync::Arc::new(std::sync::OnceLock::new()),
        };

        let result = try_resume_for_conflict(&orch, &mr_id);
        assert!(result.is_ok());
        assert!(
            result.unwrap().is_none(),
            "should not resume for backlog issue"
        );

        // Verify no new run was created
        let run_count: i64 = crate::schema::runs::table
            .count()
            .first(&mut *orch.db.conn.lock().unwrap())
            .unwrap();
        assert_eq!(run_count, 0);
    }
}
