//! Manager completion: readiness checks, status transition, and PR description generation.

use crate::models::{IssueStatus, Manager};
use crate::schema::{issues, managers};
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::Serialize;

/// Result of checking whether a manager is ready for completion.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionReadiness {
    pub ready: bool,
    pub open_issues: Vec<OpenIssue>,
    pub total_managed: usize,
    pub completed_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenIssue {
    pub number: i32,
    pub title: String,
    pub status: String,
}

/// Check if a manager is ready for completion.
pub fn check_readiness(
    conn: &mut SqliteConnection,
    manager_id: &str,
) -> Result<CompletionReadiness, String> {
    let managed: Vec<(i32, String, String)> = issues::table
        .filter(issues::manager_id.eq(manager_id))
        .select((issues::number, issues::title, issues::status))
        .load(conn)
        .map_err(|e| format!("Failed to query managed issues: {}", e))?;

    let total_managed = managed.len();

    let mut open_issues = Vec::new();
    let mut completed_count = 0;

    for (number, title, status) in managed {
        let parsed: IssueStatus = status.parse().unwrap_or_default();
        match parsed {
            IssueStatus::Merged | IssueStatus::Closed => {
                completed_count += 1;
            }
            _ => {
                open_issues.push(OpenIssue {
                    number,
                    title,
                    status,
                });
            }
        }
    }

    Ok(CompletionReadiness {
        ready: open_issues.is_empty(),
        open_issues,
        total_managed,
        completed_count,
    })
}

/// Transition a manager to completed status.
pub fn complete(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    manager_id: &str,
) -> Result<Manager, String> {
    let now = clock.now() as i32;

    diesel::update(managers::table.find(manager_id))
        .set((
            managers::status.eq("completed"),
            managers::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| format!("Failed to complete manager: {}", e))?;

    crate::managers::crud::get(conn, manager_id)?
        .ok_or_else(|| "Manager not found after update".to_string())
}

/// Generate PR title and body from managed issues and dashboard content.
pub fn generate_pr_description(
    conn: &mut SqliteConnection,
    manager: &Manager,
    worktree_path: Option<&std::path::Path>,
) -> (String, String) {
    if manager.job_id.is_none() {
        return (manager.name.clone(), manager.description.clone());
    }

    // Load managed issues for the body
    let managed_issues =
        crate::managers::crud::list_managed_issues(conn, &manager.id).unwrap_or_default();

    // Load dashboard content from file
    let meta_content = load_dashboard_content(&manager.name, worktree_path);

    // Title: manager name + first heading from dashboard content
    let title = match &meta_content {
        Some(content) => extract_first_heading(content)
            .map(|h| format!("{}: {}", manager.name, h))
            .unwrap_or_else(|| manager.name.clone()),
        None => manager.name.clone(),
    };

    // Body: managed issues summary + dashboard content
    let mut body = String::new();

    // Managed issues section (derived from DB)
    if !managed_issues.is_empty() {
        let (mut merged, mut other): (Vec<_>, Vec<_>) = managed_issues
            .iter()
            .partition(|i| matches!(i.status, IssueStatus::Merged | IssueStatus::Closed));
        merged.sort_by_key(|i| i.number);
        other.sort_by_key(|i| i.number);

        body.push_str("## Changes\n\n");
        for issue in &merged {
            body.push_str(&format!("- #{} {}\n", issue.number, issue.title));
        }
        if !other.is_empty() {
            body.push('\n');
            body.push_str("### Open\n\n");
            for issue in &other {
                body.push_str(&format!(
                    "- #{} {} ({})\n",
                    issue.number, issue.title, issue.status
                ));
            }
        }
        body.push('\n');
    }

    // Append dashboard content if available
    if let Some(content) = &meta_content {
        if !body.is_empty() {
            body.push_str("---\n\n");
        }
        body.push_str(content);
    }

    if body.is_empty() {
        body = fallback_body(&manager.description);
    }

    (title, body)
}

/// Load the markdown content from the dashboard file on disk.
fn load_dashboard_content(
    manager_name: &str,
    worktree_path: Option<&std::path::Path>,
) -> Option<String> {
    let wt = worktree_path?;
    let path = wt.join(crate::managers::dashboard_path(manager_name));
    let content = std::fs::read_to_string(&path).ok()?;
    if content.trim().is_empty() {
        None
    } else {
        Some(content)
    }
}

/// Extract the first markdown heading (# ...) from content.
fn extract_first_heading(markdown: &str) -> Option<String> {
    markdown
        .lines()
        .find(|line| line.starts_with("# "))
        .map(|line| line.trim_start_matches('#').trim().to_string())
}

fn fallback_body(description: &str) -> String {
    if description.is_empty() {
        "Feature branch merge.".to_string()
    } else {
        description.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CreateManager, ManagerStatus};
    use crate::services::testing::MockClock;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    fn create_test_manager(conn: &mut SqliteConnection, project_id: &str) -> Manager {
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000000);
        crate::managers::crud::create(
            conn,
            &clock,
            CreateManager {
                project_id: project_id.to_string(),
                home_project_id: None,
                scope_kind: None,
                name: "Test Manager".to_string(),
                branch: "feature/test".to_string(),
                description: Some("Test feature".to_string()),
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn test_check_readiness_no_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        let readiness = check_readiness(&mut conn, &manager.id).unwrap();
        assert!(readiness.ready);
        assert_eq!(readiness.total_managed, 0);
        assert_eq!(readiness.completed_count, 0);
        assert!(readiness.open_issues.is_empty());
    }

    #[test]
    fn test_check_readiness_with_open_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        // Create managed issues
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);
        crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Open Issue".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        let readiness = check_readiness(&mut conn, &manager.id).unwrap();
        assert!(!readiness.ready);
        assert_eq!(readiness.total_managed, 1);
        assert_eq!(readiness.completed_count, 0);
        assert_eq!(readiness.open_issues.len(), 1);
        assert_eq!(readiness.open_issues[0].title, "Open Issue");
    }

    #[test]
    fn test_check_readiness_all_merged() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);
        let issue = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Done Issue".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        // Mark as merged
        crate::transitions::resolve_issue(
            &mut conn,
            &issue.id,
            crate::transitions::Resolution::Merged,
            None,
        )
        .unwrap();

        let readiness = check_readiness(&mut conn, &manager.id).unwrap();
        assert!(readiness.ready);
        assert_eq!(readiness.total_managed, 1);
        assert_eq!(readiness.completed_count, 1);
        assert!(readiness.open_issues.is_empty());
    }

    #[test]
    fn test_complete_transitions_status() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        assert_eq!(manager.status, ManagerStatus::Active);

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700002000);
        let completed = complete(&mut conn, &clock, &manager.id).unwrap();

        assert_eq!(completed.status, ManagerStatus::Completed);
        assert_eq!(completed.updated_at, 1700002000);
    }

    #[test]
    fn test_generate_pr_description_no_dashboard_file() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        let (title, body) = generate_pr_description(&mut conn, &manager, None);
        assert_eq!(title, "Test Manager");
        assert_eq!(body, "Test feature");
    }

    #[test]
    fn test_check_readiness_closed_counts_as_completed() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);
        let issue = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Closed Issue".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        crate::transitions::resolve_issue(
            &mut conn,
            &issue.id,
            crate::transitions::Resolution::Closed,
            None,
        )
        .unwrap();

        let readiness = check_readiness(&mut conn, &manager.id).unwrap();
        assert!(readiness.ready);
        assert_eq!(readiness.completed_count, 1);
    }

    #[test]
    fn test_check_readiness_mixed_statuses() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);

        // Create a merged issue
        let merged = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Merged".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();
        crate::transitions::resolve_issue(
            &mut conn,
            &merged.id,
            crate::transitions::Resolution::Merged,
            None,
        )
        .unwrap();

        // Create a closed issue
        let closed = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Closed".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();
        crate::transitions::resolve_issue(
            &mut conn,
            &closed.id,
            crate::transitions::Resolution::Closed,
            None,
        )
        .unwrap();

        // Create an open issue
        crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Still Open".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        let readiness = check_readiness(&mut conn, &manager.id).unwrap();
        assert!(!readiness.ready);
        assert_eq!(readiness.total_managed, 3);
        assert_eq!(readiness.completed_count, 2);
        assert_eq!(readiness.open_issues.len(), 1);
        assert_eq!(readiness.open_issues[0].title, "Still Open");
    }

    #[test]
    fn test_generate_pr_description_no_job_id() {
        // Manager with no job_id should return name/description directly
        let manager = Manager {
            id: "mgr-1".to_string(),
            project_id: "proj-1".to_string(),
            home_project_id: Some("proj-1".to_string()),
            scope_kind: crate::models::ManagerScopeKind::Branch,
            name: "Direct Manager".to_string(),
            description: "Direct description".to_string(),
            branch: "feature/direct".to_string(),
            job_id: None,
            status: ManagerStatus::Active,
            current_session_id: None,
            current_turn_id: None,
            last_wake_at: None,
            last_turn_completed_at: None,
            last_error: None,
            agent_config_id: None,
            tier: None,
            parent_manager_id: None,
            created_at: 1700000000,
            updated_at: 1700000000,
            execution_id: None,
        };

        let mut conn = test_diesel_conn();
        let (title, body) = generate_pr_description(&mut conn, &manager, None);
        assert_eq!(title, "Direct Manager");
        assert_eq!(body, "Direct description");
    }

    /// Helper to create a manager with a dashboard file on disk.
    /// Returns (manager, tempdir) — caller must keep tempdir alive.
    fn create_manager_with_dashboard(
        conn: &mut SqliteConnection,
        project_id: &str,
        dashboard_content: &str,
    ) -> (Manager, tempfile::TempDir) {
        let manager = create_test_manager(conn, project_id);

        let wt_dir = tempfile::tempdir().unwrap();
        let dashboard_rel = crate::managers::dashboard_path(&manager.name);
        let dashboard_abs = wt_dir.path().join(&dashboard_rel);
        std::fs::create_dir_all(dashboard_abs.parent().unwrap()).unwrap();
        std::fs::write(&dashboard_abs, dashboard_content).unwrap();

        (manager, wt_dir)
    }

    #[test]
    fn test_generate_pr_description_with_dashboard_file() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        let (manager, wt_dir) = create_manager_with_dashboard(
            &mut conn,
            &project_id,
            "# Implement Auth System\n\nBuilt login, JWT validation, and session handling.\n\n## Key Decisions\n- Use bcrypt for password hashing\n- Store tokens in httpOnly cookies for XSS protection",
        );

        let (title, body) = generate_pr_description(&mut conn, &manager, Some(wt_dir.path()));

        // Title extracted from first heading
        assert_eq!(title, "Test Manager: Implement Auth System");
        // Body includes dashboard content
        assert!(body.contains("Built login, JWT validation"));
        assert!(body.contains("Use bcrypt for password hashing"));
    }

    #[test]
    fn test_generate_pr_description_with_managed_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        let (manager, wt_dir) = create_manager_with_dashboard(
            &mut conn,
            &project_id,
            "# Auth System\n\nShipped the auth system.",
        );

        // Create managed issues
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);

        let merged_issue = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Add login endpoint".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();
        crate::transitions::resolve_issue(
            &mut conn,
            &merged_issue.id,
            crate::transitions::Resolution::Merged,
            None,
        )
        .unwrap();

        crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Add rate limiting".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        let (title, body) = generate_pr_description(&mut conn, &manager, Some(wt_dir.path()));

        assert_eq!(title, "Test Manager: Auth System");
        assert!(body.contains("## Changes"));
        assert!(body.contains("Add login endpoint"));
        assert!(body.contains("### Open"));
        assert!(body.contains("Add rate limiting"));
        // Dashboard content appended after separator
        assert!(body.contains("---"));
        assert!(body.contains("Shipped the auth system"));
    }

    #[test]
    fn test_generate_pr_description_empty_dashboard_file() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        let (manager, wt_dir) = create_manager_with_dashboard(&mut conn, &project_id, "   \n  ");

        let (title, body) = generate_pr_description(&mut conn, &manager, Some(wt_dir.path()));

        // Empty content → falls back to manager name/description
        assert_eq!(title, "Test Manager");
        assert_eq!(body, "Test feature");
    }

    #[test]
    fn test_extract_first_heading_variants() {
        // Standard heading
        assert_eq!(
            extract_first_heading("# Hello World\n\nBody text"),
            Some("Hello World".to_string())
        );
        // Heading not on first line
        assert_eq!(
            extract_first_heading("Some preamble\n# Actual Heading\nMore text"),
            Some("Actual Heading".to_string())
        );
        // No heading at all
        assert_eq!(extract_first_heading("Just plain text\nNo headings"), None);
        // Only ## headings (not # headings)
        assert_eq!(extract_first_heading("## Sub Heading\n\nBody"), None);
        // Empty string
        assert_eq!(extract_first_heading(""), None);
    }

    #[test]
    fn test_generate_pr_description_issues_only_no_artifact() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");
        let manager = create_test_manager(&mut conn, &project_id);

        // Link a job to the manager (but no artifact)
        let issue_id = crate::test_utils::create_test_issue(&mut conn, &project_id, "Temp");
        let job_id = crate::test_utils::create_test_job(
            &mut conn,
            &issue_id,
            &project_id,
            "manager",
            "running",
            None,
        );
        diesel::update(managers::table.find(&manager.id))
            .set(managers::job_id.eq(Some(&job_id)))
            .execute(&mut conn)
            .unwrap();
        let manager = crate::managers::crud::get(&mut conn, &manager.id)
            .unwrap()
            .unwrap();

        // Create a merged managed issue
        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);
        let merged = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Add login".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();
        crate::transitions::resolve_issue(
            &mut conn,
            &merged.id,
            crate::transitions::Resolution::Merged,
            None,
        )
        .unwrap();

        let (title, body) = generate_pr_description(&mut conn, &manager, None);

        // No artifact → title is just manager name
        assert_eq!(title, "Test Manager");
        // Body has issues section but no separator or dashboard content
        assert!(body.contains("## Changes"));
        assert!(body.contains("Add login"));
        assert!(!body.contains("---"));
    }

    #[test]
    fn test_generate_pr_description_content_without_heading() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        let (manager, wt_dir) = create_manager_with_dashboard(
            &mut conn,
            &project_id,
            "This is a dashboard with no heading.\n\nJust paragraphs.",
        );

        let (title, body) = generate_pr_description(&mut conn, &manager, Some(wt_dir.path()));

        // No # heading → title falls back to manager name
        assert_eq!(title, "Test Manager");
        // Body still includes the content
        assert!(body.contains("This is a dashboard with no heading"));
    }

    #[test]
    fn test_generate_pr_description_closed_issues_in_changes() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        let (manager, wt_dir) = create_manager_with_dashboard(&mut conn, &project_id, "# Summary");

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700001000);

        // Create a Closed issue (should appear in Changes, not Open)
        let closed = crate::issues::crud::create(
            &mut conn,
            &clock,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Removed feature".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();
        crate::transitions::resolve_issue(
            &mut conn,
            &closed.id,
            crate::transitions::Resolution::Closed,
            None,
        )
        .unwrap();

        let (_, body) = generate_pr_description(&mut conn, &manager, Some(wt_dir.path()));

        assert!(body.contains("## Changes"));
        assert!(body.contains("Removed feature"));
        // Closed issues should NOT appear in Open section
        assert!(!body.contains("### Open"));
    }

    #[test]
    fn test_generate_pr_description_empty_description() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test", "TEST");

        let mut clock = MockClock::new();
        clock.expect_now().returning(|| 1700000000);
        let manager = crate::managers::crud::create(
            &mut conn,
            &clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Feature".to_string(),
                branch: "feature/x".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let (title, body) = generate_pr_description(&mut conn, &manager, None);
        assert_eq!(title, "Feature");
        assert_eq!(body, "Feature branch merge.");
    }
}
