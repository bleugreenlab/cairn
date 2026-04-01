use crate::diesel_models::DbMergeRequest;
use crate::models::{Check, PrCache, PrDataSummary, WebhookEvent};
use crate::schema::{issues, jobs, merge_requests, webhook_events};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// Convert DbMergeRequest to PrCache (the frontend model).
///
/// PrCache remains the frontend-facing type for backwards compatibility.
/// The fields map from merge_requests columns to the existing PrCache shape.
pub fn to_pr_cache(mr: DbMergeRequest) -> PrCache {
    let checks: Vec<Check> = mr
        .checks_json
        .and_then(|json| serde_json::from_str(&json).ok())
        .unwrap_or_default();

    PrCache {
        id: mr.id,
        job_id: Some(mr.job_id),
        pr_number: mr.github_pr_number.unwrap_or(0),
        pr_url: mr.github_pr_url.unwrap_or_default(),
        title: Some(mr.title),
        body: mr.body,
        state: mr
            .github_state
            .and_then(|s| s.parse().ok())
            .unwrap_or(match mr.status.as_str() {
                "merged" => crate::models::PrState::Merged,
                "closed" => crate::models::PrState::Closed,
                _ => crate::models::PrState::Open,
            }),
        is_draft: false,
        review_decision: mr.github_review.and_then(|s| s.parse().ok()),
        mergeable: mr
            .github_mergeable
            .and_then(|s| s.parse().ok())
            .unwrap_or_default(),
        additions: mr.additions,
        deletions: mr.deletions,
        checks_status: mr.checks_status.and_then(|s| s.parse().ok()),
        checks,
        fetched_at: mr.github_fetched_at.map(|t| t as i64).unwrap_or(0),
        updated_at: mr.updated_at as i64,
    }
}

/// Get merge request for a job_id.
pub fn get_by_job_id(conn: &mut SqliteConnection, job_id: &str) -> Result<Option<PrCache>, String> {
    let mr: Option<DbMergeRequest> = merge_requests::table
        .filter(merge_requests::job_id.eq(job_id))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(mr.map(to_pr_cache))
}

/// Get merge request by its ID.
pub fn get_by_id(conn: &mut SqliteConnection, id: &str) -> Result<Option<PrCache>, String> {
    let mr: Option<DbMergeRequest> = merge_requests::table
        .find(id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(mr.map(to_pr_cache))
}

/// Get merge request by github_pr_url.
pub fn get_by_github_url(
    conn: &mut SqliteConnection,
    github_pr_url: &str,
) -> Result<Option<DbMergeRequest>, String> {
    merge_requests::table
        .filter(merge_requests::github_pr_url.eq(github_pr_url))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())
}

/// Get merge request by manager_id (most recent).
pub fn get_by_manager_id(
    conn: &mut SqliteConnection,
    manager_id: &str,
) -> Result<Option<PrCache>, String> {
    let mr: Option<DbMergeRequest> = merge_requests::table
        .filter(merge_requests::manager_id.eq(manager_id))
        .order(merge_requests::updated_at.desc())
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(mr.map(to_pr_cache))
}

/// Get merge request summaries by job IDs directly (internal use).
pub fn get_summaries_for_jobs(
    conn: &mut SqliteConnection,
    job_ids: &[String],
) -> Result<Vec<PrDataSummary>, String> {
    if job_ids.is_empty() {
        return Ok(vec![]);
    }
    let results: Vec<DbMergeRequest> = merge_requests::table
        .filter(merge_requests::job_id.eq_any(job_ids))
        .load(conn)
        .map_err(|e| e.to_string())?;
    Ok(results
        .into_iter()
        .map(|mr| PrDataSummary {
            id: mr.id,
            action_run_id: Some(mr.job_id),
            pr_number: mr.github_pr_number.unwrap_or(0),
            pr_url: mr.github_pr_url.unwrap_or_default(),
            pr_status: mr.status,
        })
        .collect())
}

/// Get merge request summaries for multiple action run IDs (for sidebar display).
///
/// The frontend passes action_run IDs from `builtin:create_pr` action runs.
/// We resolve each to its parent_job_id, find the merge_request by job_id,
/// and return the original action_run_id in the response so the frontend
/// can map results back to the correct timeline node.
pub fn get_summaries_for_action_runs(
    conn: &mut SqliteConnection,
    action_run_ids: &[String],
) -> Result<Vec<PrDataSummary>, String> {
    use crate::schema::action_runs;

    if action_run_ids.is_empty() {
        return Ok(vec![]);
    }

    // Resolve action_run_ids to (action_run_id, parent_job_id) pairs
    let ar_to_job: Vec<(String, Option<String>)> = action_runs::table
        .filter(action_runs::id.eq_any(action_run_ids))
        .select((action_runs::id, action_runs::parent_job_id))
        .load(conn)
        .map_err(|e| e.to_string())?;

    // Collect the job_ids we need to look up
    let job_ids: Vec<&str> = ar_to_job
        .iter()
        .filter_map(|(_, job_id)| job_id.as_deref())
        .collect();

    if job_ids.is_empty() {
        return Ok(vec![]);
    }

    // Load merge_requests by those job_ids
    let mrs: Vec<DbMergeRequest> = merge_requests::table
        .filter(merge_requests::job_id.eq_any(&job_ids))
        .load(conn)
        .map_err(|e| e.to_string())?;

    // Build job_id → merge_request map
    let mr_by_job: std::collections::HashMap<&str, &DbMergeRequest> =
        mrs.iter().map(|mr| (mr.job_id.as_str(), mr)).collect();

    // Map back: for each action_run, find the merge_request via parent_job_id,
    // and return the original action_run_id in the response
    Ok(ar_to_job
        .iter()
        .filter_map(|(ar_id, job_id)| {
            let job_id = job_id.as_deref()?;
            let mr = mr_by_job.get(job_id)?;
            Some(PrDataSummary {
                id: mr.id.clone(),
                action_run_id: Some(ar_id.clone()),
                pr_number: mr.github_pr_number.unwrap_or(0),
                pr_url: mr.github_pr_url.clone().unwrap_or_default(),
                pr_status: mr.status.clone(),
            })
        })
        .collect())
}

/// Get a merge request by action_run_id (resolves through parent_job_id).
///
/// Used by get_pr_details when the caller passes an action_run_id.
pub fn get_by_action_run_id(
    conn: &mut SqliteConnection,
    action_run_id: &str,
) -> Result<Option<PrCache>, String> {
    use crate::schema::action_runs;

    // First try direct job_id match (caller might already be passing a job_id)
    let mr: Option<DbMergeRequest> = merge_requests::table
        .filter(merge_requests::job_id.eq(action_run_id))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if let Some(mr) = mr {
        return Ok(Some(to_pr_cache(mr)));
    }

    // Fall back: look up parent_job_id from action_runs
    let parent_job_id: Option<String> = action_runs::table
        .find(action_run_id)
        .select(action_runs::parent_job_id)
        .first::<Option<String>>(conn)
        .ok()
        .flatten();

    if let Some(job_id) = parent_job_id {
        let mr: Option<DbMergeRequest> = merge_requests::table
            .filter(merge_requests::job_id.eq(&job_id))
            .first(conn)
            .optional()
            .map_err(|e| e.to_string())?;
        return Ok(mr.map(to_pr_cache));
    }

    Ok(None)
}

/// Get all merge requests for a project with provenance.
pub fn get_project_merge_requests(
    conn: &mut SqliteConnection,
    project_id: &str,
) -> Result<Vec<crate::models::ProjectPrEntry>, String> {
    #[allow(clippy::type_complexity)]
    let results: Vec<(
        String,         // mr.id
        String,         // mr.job_id
        String,         // mr.status
        String,         // mr.title
        Option<i32>,    // mr.additions
        Option<i32>,    // mr.deletions
        Option<String>, // mr.checks_status
        Option<String>, // mr.github_review
        Option<i32>,    // mr.github_pr_number
        Option<String>, // mr.github_pr_url
        i32,            // mr.opened_at
        i32,            // mr.updated_at
        Option<String>, // mr.issue_id
        Option<String>, // mr.manager_id
        Option<i32>,    // issues.number
        Option<String>, // issues.title
    )> = merge_requests::table
        .left_join(issues::table.on(merge_requests::issue_id.eq(issues::id.nullable())))
        .filter(merge_requests::project_id.eq(project_id))
        .select((
            merge_requests::id,
            merge_requests::job_id,
            merge_requests::status,
            merge_requests::title,
            merge_requests::additions,
            merge_requests::deletions,
            merge_requests::checks_status,
            merge_requests::github_review,
            merge_requests::github_pr_number,
            merge_requests::github_pr_url,
            merge_requests::opened_at,
            merge_requests::updated_at,
            merge_requests::issue_id,
            merge_requests::manager_id,
            issues::number.nullable(),
            issues::title.nullable(),
        ))
        .order(merge_requests::updated_at.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    // Look up execution_id for each job
    Ok(results
        .into_iter()
        .map(|r| {
            let execution_id: Option<String> = jobs::table
                .find(&r.1)
                .select(jobs::execution_id)
                .first::<Option<String>>(conn)
                .ok()
                .flatten();

            crate::models::ProjectPrEntry {
                id: r.0,
                action_run_id: r.1.clone(), // Using job_id in the action_run_id field for compat
                pr_number: r.8.unwrap_or(0),
                pr_url: r.9.unwrap_or_default(),
                pr_status: r.2,
                title: Some(r.3),
                is_draft: false,
                additions: r.4,
                deletions: r.5,
                checks_status: r.6,
                review_decision: r.7,
                opened_at: Some(r.10 as i64),
                updated_at: r.11 as i64,
                manager_id: r.13,
                execution_id: execution_id.unwrap_or_default(),
                issue_id: r.12,
                issue_number: r.14,
                issue_title: r.15,
            }
        })
        .collect())
}

/// Get webhook events for a PR number.
pub fn get_webhook_events(
    conn: &mut SqliteConnection,
    pr_number: i64,
) -> Result<Vec<WebhookEvent>, String> {
    let events = webhook_events::table
        .filter(webhook_events::pr_number.eq(pr_number as i32))
        .order(webhook_events::processed_at.desc())
        .limit(100)
        .load::<crate::diesel_models::DbWebhookEvent>(conn)
        .map_err(|e| e.to_string())?;
    Ok(events.into_iter().map(db_webhook_to_model).collect())
}

/// Get webhook events for a job (via its merge request's PR number).
pub fn get_webhook_events_for_job(
    conn: &mut SqliteConnection,
    job_id: &str,
) -> Result<Vec<WebhookEvent>, String> {
    let pr_number: Option<i32> = merge_requests::table
        .filter(merge_requests::job_id.eq(job_id))
        .select(merge_requests::github_pr_number)
        .first::<Option<i32>>(conn)
        .ok()
        .flatten();

    match pr_number {
        Some(pr_num) => {
            let events = webhook_events::table
                .filter(webhook_events::pr_number.eq(pr_num))
                .order(webhook_events::processed_at.desc())
                .limit(100)
                .load::<crate::diesel_models::DbWebhookEvent>(conn)
                .map_err(|e| e.to_string())?;
            Ok(events.into_iter().map(db_webhook_to_model).collect())
        }
        None => Ok(vec![]),
    }
}

fn db_webhook_to_model(e: crate::diesel_models::DbWebhookEvent) -> WebhookEvent {
    WebhookEvent {
        id: e.id,
        event_type: e.event_type,
        action: e.action,
        repo_full_name: e.repo_full_name,
        pr_number: e.pr_number.map(|n| n as i64),
        payload_summary: e.payload_summary,
        processed_at: e.processed_at as i64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::NewMergeRequest;
    use crate::models::PrState;
    use crate::test_utils::{
        create_test_issue, create_test_job, create_test_project, test_diesel_conn,
    };

    fn insert_mr<'a>(
        conn: &mut SqliteConnection,
        job_id: &'a str,
        project_id: &'a str,
        issue_id: Option<&'a str>,
        manager_id: Option<&'a str>,
        status: &'a str,
        github_pr_number: Option<i32>,
        github_pr_url: Option<&'a str>,
        github_state: Option<&'a str>,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        let new_mr = NewMergeRequest {
            id: &id,
            job_id,
            project_id,
            issue_id,
            manager_id,
            title: "Test MR",
            body: None,
            source_branch: "feature",
            target_branch: "main",
            status,
            merge_method: "squash",
            additions: Some(10),
            deletions: Some(5),
            changed_files: Some(3),
            commit_count: Some(2),
            checks_json: None,
            checks_status: None,
            opened_at: now,
            updated_at: now,
            github_pr_number,
            github_pr_url,
            github_state,
        };

        diesel::insert_into(merge_requests::table)
            .values(&new_mr)
            .execute(conn)
            .expect("Failed to insert merge_request");

        id
    }

    // === to_pr_cache conversion ===

    #[test]
    fn to_pr_cache_maps_github_state_when_present() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPC1");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(42),
            Some("https://github.com/o/r/pull/42"),
            Some("OPEN"),
        );

        let pr = get_by_id(&mut conn, &mr_id).unwrap().unwrap();
        assert_eq!(pr.state, PrState::Open);
        assert_eq!(pr.pr_number, 42);
        assert_eq!(pr.pr_url, "https://github.com/o/r/pull/42");
    }

    #[test]
    fn to_pr_cache_falls_back_to_status_when_github_state_null() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPC2");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        // status="merged", no github_state → should map to PrState::Merged
        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "merged",
            None,
            None,
            None,
        );

        let pr = get_by_id(&mut conn, &mr_id).unwrap().unwrap();
        assert_eq!(pr.state, PrState::Merged);
        assert_eq!(pr.pr_number, 0, "no github_pr_number → defaults to 0");
        assert!(pr.pr_url.is_empty(), "no github_pr_url → defaults to empty");
    }

    #[test]
    fn to_pr_cache_closed_status_fallback() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPC3");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "closed",
            None,
            None,
            None,
        );

        let pr = get_by_id(&mut conn, &mr_id).unwrap().unwrap();
        assert_eq!(pr.state, PrState::Closed);
    }

    #[test]
    fn to_pr_cache_open_status_fallback() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPC4");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            None,
            None,
            None,
        );

        let pr = get_by_id(&mut conn, &mr_id).unwrap().unwrap();
        assert_eq!(pr.state, PrState::Open);
    }

    #[test]
    fn to_pr_cache_deserializes_checks_json() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPC5");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            None,
            None,
            None,
        );

        // Check struct has: name, state (CheckState enum), description, workflow_name, link
        let checks = r#"[{"name":"ci","state":"SUCCESS","description":null,"workflow_name":null,"link":null}]"#;
        diesel::update(merge_requests::table.filter(merge_requests::id.eq(&mr_id)))
            .set(merge_requests::checks_json.eq(Some(checks)))
            .execute(&mut conn)
            .unwrap();

        let pr = get_by_id(&mut conn, &mr_id).unwrap().unwrap();
        assert_eq!(pr.checks.len(), 1);
        assert_eq!(pr.checks[0].name, "ci");
    }

    #[test]
    fn to_pr_cache_defaults_checks_on_invalid_json() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TPC6");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            None,
            None,
            None,
        );

        // Set invalid checks_json
        diesel::update(merge_requests::table.filter(merge_requests::id.eq(&mr_id)))
            .set(merge_requests::checks_json.eq(Some("not-json")))
            .execute(&mut conn)
            .unwrap();

        let pr = get_by_id(&mut conn, &mr_id).unwrap().unwrap();
        assert!(
            pr.checks.is_empty(),
            "invalid JSON should default to empty vec"
        );
    }

    // === get_by_job_id ===

    #[test]
    fn get_by_job_id_returns_matching_mr() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGBJ");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(1),
            None,
            None,
        );

        let result = get_by_job_id(&mut conn, &job_id).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().job_id, Some(job_id));
    }

    #[test]
    fn get_by_job_id_returns_none_for_unknown() {
        let mut conn = test_diesel_conn();
        let result = get_by_job_id(&mut conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    // === get_by_github_url (key webhook matching mechanism) ===

    #[test]
    fn get_by_github_url_finds_unique_match() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGBU");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        let url = "https://github.com/owner/repo/pull/99";
        let mr_id = insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(99),
            Some(url),
            Some("OPEN"),
        );

        let result = get_by_github_url(&mut conn, url).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().id, mr_id);
    }

    #[test]
    fn get_by_github_url_returns_none_for_unknown() {
        let mut conn = test_diesel_conn();
        let result = get_by_github_url(&mut conn, "https://github.com/x/y/pull/0").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_by_github_url_distinguishes_across_projects() {
        // This is the CAIRN-1021 fix: different repos with the same PR number
        // are distinguished by URL, not number.
        let mut conn = test_diesel_conn();
        let project_a = create_test_project(&mut conn, "Proj A", "PA");
        let project_b = create_test_project(&mut conn, "Proj B", "PB");
        let issue_a = create_test_issue(&mut conn, &project_a, "Issue A");
        let issue_b = create_test_issue(&mut conn, &project_b, "Issue B");
        let job_a = create_test_job(&mut conn, &issue_a, &project_a, "build", "complete", None);
        let job_b = create_test_job(&mut conn, &issue_b, &project_b, "build", "complete", None);

        let url_a = "https://github.com/owner/repo-a/pull/1";
        let url_b = "https://github.com/owner/repo-b/pull/1";

        let mr_id_a = insert_mr(
            &mut conn,
            &job_a,
            &project_a,
            Some(&issue_a),
            None,
            "open",
            Some(1),
            Some(url_a),
            Some("OPEN"),
        );
        let mr_id_b = insert_mr(
            &mut conn,
            &job_b,
            &project_b,
            Some(&issue_b),
            None,
            "open",
            Some(1),
            Some(url_b),
            Some("OPEN"),
        );

        // Same PR number (1), different URLs → different merge requests
        let result_a = get_by_github_url(&mut conn, url_a).unwrap().unwrap();
        let result_b = get_by_github_url(&mut conn, url_b).unwrap().unwrap();
        assert_eq!(result_a.id, mr_id_a);
        assert_eq!(result_b.id, mr_id_b);
        assert_ne!(mr_id_a, mr_id_b);
    }

    // === get_by_manager_id ===

    fn insert_manager(conn: &mut SqliteConnection, id: &str, project_id: &str) {
        diesel::sql_query(
            "INSERT INTO managers (id, project_id, name, branch, created_at, updated_at) \
             VALUES (?, ?, 'Test Manager', 'mgr-branch', 0, 0)",
        )
        .bind::<diesel::sql_types::Text, _>(id)
        .bind::<diesel::sql_types::Text, _>(project_id)
        .execute(conn)
        .expect("Failed to create manager");
    }

    #[test]
    fn get_by_manager_id_returns_most_recent() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGBM");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_1 = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);
        let job_2 = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        // Create a manager for the FK constraint
        insert_manager(&mut conn, "mgr-1", &project_id);

        // Insert two MRs for the same manager, different jobs.
        // First one: set updated_at to an older value explicitly.
        insert_mr(
            &mut conn,
            &job_1,
            &project_id,
            None,
            Some("mgr-1"),
            "closed",
            None,
            None,
            None,
        );
        diesel::update(merge_requests::table.filter(merge_requests::job_id.eq(&job_1)))
            .set(merge_requests::updated_at.eq(1000))
            .execute(&mut conn)
            .unwrap();

        let mr_id_2 = insert_mr(
            &mut conn,
            &job_2,
            &project_id,
            None,
            Some("mgr-1"),
            "open",
            None,
            None,
            None,
        );
        diesel::update(merge_requests::table.filter(merge_requests::job_id.eq(&job_2)))
            .set(merge_requests::updated_at.eq(2000))
            .execute(&mut conn)
            .unwrap();

        let result = get_by_manager_id(&mut conn, "mgr-1").unwrap();
        assert!(result.is_some());
        // Should return the most recently updated one
        assert_eq!(result.unwrap().id, mr_id_2);
    }

    #[test]
    fn get_by_manager_id_returns_none_for_unknown() {
        let mut conn = test_diesel_conn();
        let result = get_by_manager_id(&mut conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }

    // === get_summaries_for_action_runs ===

    #[test]
    fn summaries_for_action_runs_returns_empty_on_empty_input() {
        let mut conn = test_diesel_conn();
        let result = get_summaries_for_action_runs(&mut conn, &[]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn summaries_for_action_runs_resolves_through_parent_job() {
        use crate::schema::action_runs;

        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGSJ");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_1 = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);
        let job_2 = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        // Create action_configs + action_runs pointing to jobs as parent_job_id
        diesel::sql_query(
            "INSERT OR IGNORE INTO action_configs (id, name, description, is_builtin, created_at, updated_at) \
             VALUES ('cfg-1', 'create_pr', 'test', 1, 0, 0)"
        ).execute(&mut conn).unwrap();

        // Create an execution for the action_runs FK
        diesel::sql_query(
            "INSERT INTO executions (id, recipe_id, status, started_at, triggered_by) \
             VALUES ('exec-sum', 'recipe-1', 'complete', 0, 'manual')",
        )
        .execute(&mut conn)
        .unwrap();

        // Action run 1 → parent_job_id = job_1
        diesel::sql_query(
            "INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, project_id, status, parent_job_id, created_at) \
             VALUES ('ar-1', 'exec-sum', 'node-1', 'cfg-1', ?, 'complete', ?, 0)"
        )
        .bind::<diesel::sql_types::Text, _>(&project_id)
        .bind::<diesel::sql_types::Text, _>(&job_1)
        .execute(&mut conn).unwrap();

        // Action run 2 → parent_job_id = job_2
        diesel::sql_query(
            "INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, project_id, status, parent_job_id, created_at) \
             VALUES ('ar-2', 'exec-sum', 'node-2', 'cfg-1', ?, 'complete', ?, 0)"
        )
        .bind::<diesel::sql_types::Text, _>(&project_id)
        .bind::<diesel::sql_types::Text, _>(&job_2)
        .execute(&mut conn).unwrap();

        // Create MRs for job_1 and job_2
        insert_mr(
            &mut conn,
            &job_1,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(1),
            Some("https://github.com/o/r/pull/1"),
            None,
        );
        insert_mr(
            &mut conn,
            &job_2,
            &project_id,
            Some(&issue_id),
            None,
            "merged",
            Some(2),
            Some("https://github.com/o/r/pull/2"),
            None,
        );

        let result =
            get_summaries_for_action_runs(&mut conn, &["ar-1".to_string(), "ar-2".to_string()])
                .unwrap();

        assert_eq!(result.len(), 2);
        // action_run_id should be preserved in the response
        let ar_ids: Vec<&str> = result
            .iter()
            .filter_map(|s| s.action_run_id.as_deref())
            .collect();
        assert!(ar_ids.contains(&"ar-1"));
        assert!(ar_ids.contains(&"ar-2"));
        let statuses: Vec<&str> = result.iter().map(|s| s.pr_status.as_str()).collect();
        assert!(statuses.contains(&"open"));
        assert!(statuses.contains(&"merged"));
    }

    // === get_webhook_events_for_job ===

    #[test]
    fn get_webhook_events_for_job_returns_empty_when_no_mr() {
        let mut conn = test_diesel_conn();
        let result = get_webhook_events_for_job(&mut conn, "nonexistent-job").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn get_webhook_events_for_job_returns_empty_when_mr_has_no_pr_number() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGWE");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        // MR without github_pr_number
        insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            None,
            None,
            None,
        );

        let result = get_webhook_events_for_job(&mut conn, &job_id).unwrap();
        assert!(result.is_empty());
    }

    // === Action run compatibility ===

    /// Helper to create execution + action_config + action_run for testing
    /// the action_run_id → parent_job_id resolution path.
    fn insert_action_run(
        conn: &mut SqliteConnection,
        project_id: &str,
        issue_id: Option<&str>,
        parent_job_id: &str,
    ) -> String {
        use crate::diesel_models::{NewActionRun, NewExecution};
        use crate::schema::{action_runs, executions};

        let exec_id = uuid::Uuid::new_v4().to_string();
        let now = chrono::Utc::now().timestamp() as i32;

        diesel::insert_into(executions::table)
            .values(NewExecution {
                id: &exec_id,
                recipe_id: "default",
                issue_id,
                project_id: Some(project_id),
                status: "completed",
                started_at: now,
                completed_at: Some(now),
                snapshot: None,
                seq: Some(1),
                initiator_sub: None,
                initiator_auth_mode: None,
                initiator_org_id: None,
                triggered_by: "manual",
            })
            .execute(conn)
            .expect("insert execution");

        // Ensure action_config exists
        diesel::sql_query(
            "INSERT OR IGNORE INTO action_configs (id, name, description, is_builtin, project_id, created_at, updated_at)
             VALUES ('builtin:create_pr', 'create_pr', 'Create PR', 1, NULL, 0, 0)"
        )
        .execute(conn).ok();

        let ar_id = uuid::Uuid::new_v4().to_string();
        diesel::insert_into(action_runs::table)
            .values(NewActionRun {
                id: &ar_id,
                execution_id: &exec_id,
                recipe_node_id: "node-1",
                action_config_id: "builtin:create_pr",
                issue_id,
                project_id,
                status: "completed",
                inputs: None,
                output: None,
                error_message: None,
                started_at: Some(now),
                completed_at: Some(now),
                created_at: now,
                parent_job_id: Some(parent_job_id),
            })
            .execute(conn)
            .expect("insert action_run");

        ar_id
    }

    #[test]
    fn get_summaries_for_action_runs_resolves_through_parent_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TSAR");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        // Create merge_request linked to the job
        insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(99),
            Some("https://github.com/o/r/pull/99"),
            Some("OPEN"),
        );

        // Create an action_run that points to this job as parent
        let ar_id = insert_action_run(&mut conn, &project_id, Some(&issue_id), &job_id);

        // Query with the action_run_id — should resolve through parent_job_id
        let results = get_summaries_for_action_runs(&mut conn, &[ar_id.clone()]).unwrap();
        assert_eq!(results.len(), 1);
        // The response should carry the original action_run_id, not the job_id
        assert_eq!(results[0].action_run_id, Some(ar_id));
        assert_eq!(results[0].pr_number, 99);
        assert_eq!(results[0].pr_status, "open");
    }

    #[test]
    fn get_summaries_for_action_runs_returns_empty_for_unknown_ids() {
        let mut conn = test_diesel_conn();
        let results =
            get_summaries_for_action_runs(&mut conn, &["nonexistent".to_string()]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_by_action_run_id_resolves_through_parent_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGAR");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(42),
            Some("https://github.com/o/r/pull/42"),
            Some("OPEN"),
        );

        let ar_id = insert_action_run(&mut conn, &project_id, Some(&issue_id), &job_id);

        // Query with action_run_id — should find the MR via parent_job_id
        let result = get_by_action_run_id(&mut conn, &ar_id).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().pr_number, 42);
    }

    #[test]
    fn get_by_action_run_id_works_with_job_id_directly() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TGJD");
        let issue_id = create_test_issue(&mut conn, &project_id, "Test");
        let job_id = create_test_job(&mut conn, &issue_id, &project_id, "build", "complete", None);

        insert_mr(
            &mut conn,
            &job_id,
            &project_id,
            Some(&issue_id),
            None,
            "open",
            Some(42),
            Some("https://github.com/o/r/pull/42"),
            Some("OPEN"),
        );

        // Query directly with job_id (new callers may pass this)
        let result = get_by_action_run_id(&mut conn, &job_id).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().pr_number, 42);
    }

    #[test]
    fn get_by_action_run_id_returns_none_for_unknown() {
        let mut conn = test_diesel_conn();
        let result = get_by_action_run_id(&mut conn, "nonexistent").unwrap();
        assert!(result.is_none());
    }
}
