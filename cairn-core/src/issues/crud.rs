//! Issue CRUD operations.

use crate::diesel_models::{DbIssue, NewIssue, UpdateIssueChangeset};
use crate::models::{CreateIssue, Issue, IssueAttention, IssueProgress, IssueStatus, UpdateIssue};
use crate::schema::{issues, projects};
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use uuid::Uuid;

/// Convert DbIssue to Issue model.
pub fn db_issue_to_issue(db: DbIssue) -> Issue {
    Issue {
        id: db.id,
        project_id: db.project_id,
        number: db.number,
        title: db.title,
        description: db.description.unwrap_or_default(),
        status: db.status.parse().unwrap_or(IssueStatus::Backlog),
        progress: db.progress.parse().unwrap_or(IssueProgress::Backlog),
        attention: db.attention.parse().unwrap_or(IssueAttention::None),
        priority: db.priority.unwrap_or(0),
        completed_at: db.completed_at.map(|t| t as i64),
        dismissed_at: db.dismissed_at.map(|t| t as i64),
        created_at: db.created_at as i64,
        updated_at: db.updated_at as i64,
        backend_override: db.model,
        merged_at: db.merged_at.map(|t| t as i64),
        closed_at: db.closed_at.map(|t| t as i64),
        manager_id: db.manager_id,
    }
}

/// Create a new issue.
pub fn create(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: CreateIssue,
) -> Result<Issue, String> {
    let now = clock.now() as i32;
    let id = Uuid::new_v4().to_string();

    // Get next issue number
    let next_number: i32 = projects::table
        .find(&input.project_id)
        .select(projects::next_issue_number)
        .first::<Option<i32>>(conn)
        .map_err(|e| format!("Project not found: {}", e))?
        .unwrap_or(1);

    // Increment the issue number
    diesel::update(projects::table.find(&input.project_id))
        .set((
            projects::next_issue_number.eq(next_number + 1),
            projects::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    let description = input.description.as_deref();
    let new_issue = NewIssue {
        id: &id,
        project_id: &input.project_id,
        number: next_number,
        title: &input.title,
        description,
        status: "backlog",
        progress: "backlog",
        attention: "none",
        priority: Some(0),
        created_at: now,
        updated_at: now,
        model: None,
        manager_id: input.manager_id.as_deref(),
    };

    diesel::insert_into(issues::table)
        .values(&new_issue)
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(Issue {
        id,
        project_id: input.project_id,
        number: next_number,
        title: input.title,
        description: input.description.unwrap_or_default(),
        status: IssueStatus::Backlog,
        progress: IssueProgress::Backlog,
        attention: IssueAttention::None,
        priority: 0,
        completed_at: None,
        dismissed_at: None,
        created_at: now as i64,
        updated_at: now as i64,
        backend_override: None,
        merged_at: None,
        closed_at: None,
        manager_id: input.manager_id,
    })
}

/// Get a single issue by ID.
pub fn get(conn: &mut SqliteConnection, id: &str) -> Result<Option<Issue>, String> {
    let db_issue: Option<DbIssue> = issues::table
        .find(id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_issue.map(db_issue_to_issue))
}

/// List all issues for a project, newest first.
pub fn list(conn: &mut SqliteConnection, project_id: &str) -> Result<Vec<Issue>, String> {
    let db_issues: Vec<DbIssue> = issues::table
        .filter(issues::project_id.eq(project_id))
        .order(issues::number.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_issues.into_iter().map(db_issue_to_issue).collect())
}

/// Update an issue's title, description, model, and/or skills.
pub fn update(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: UpdateIssue,
) -> Result<Issue, String> {
    let now = clock.now() as i32;

    let backend_override_update: Option<Option<&str>> = input
        .backend_override
        .as_ref()
        .map(|value| value.as_deref());

    let changeset = UpdateIssueChangeset {
        updated_at: Some(now),
        title: input.title.as_deref(),
        description: input.description.as_ref().map(|d| Some(d.as_str())),
        status: None,
        progress: None,
        attention: None,
        priority: None,
        completed_at: None,
        dismissed_at: None,
        model: backend_override_update,
        merged_at: None,
        closed_at: None,
    };

    diesel::update(issues::table.find(&input.id))
        .set(&changeset)
        .execute(conn)
        .map_err(|e| e.to_string())?;

    get(conn, &input.id)?.ok_or_else(|| "Issue not found".to_string())
}

/// Set dismissed_at on an issue.
pub fn dismiss(conn: &mut SqliteConnection, clock: &dyn Clock, id: &str) -> Result<(), String> {
    let now = clock.now() as i32;

    diesel::update(issues::table.find(id))
        .set((
            issues::dismissed_at.eq(Some(now)),
            issues::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Clear dismissed_at on an issue.
pub fn restore(conn: &mut SqliteConnection, clock: &dyn Clock, id: &str) -> Result<(), String> {
    let now = clock.now() as i32;

    diesel::update(issues::table.find(id))
        .set((
            issues::dismissed_at.eq(None::<i32>),
            issues::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Mark an issue as merged (sets merged_at timestamp and recomputes status).
pub fn complete(conn: &mut SqliteConnection, clock: &dyn Clock, id: &str) -> Result<(), String> {
    crate::transitions::resolve_issue(
        conn,
        id,
        crate::transitions::Resolution::Merged,
        Some(clock),
    )?;
    Ok(())
}

/// Delete issue's DB records (prompts, events, runs, jobs, issue).
/// Does NOT handle worktree cleanup or session kills — caller must do that.
pub fn delete_db(conn: &mut SqliteConnection, issue_id: &str) -> Result<(), String> {
    use crate::schema::{events, jobs, prompts, runs};

    // Get all run IDs for the issue
    let run_ids: Vec<String> = runs::table
        .filter(runs::issue_id.eq(issue_id))
        .select(runs::id)
        .load(conn)
        .map_err(|e| e.to_string())?;

    // Delete prompts for those runs
    for run_id in &run_ids {
        diesel::delete(prompts::table.filter(prompts::run_id.eq(run_id)))
            .execute(conn)
            .map_err(|e| e.to_string())?;
    }

    // Delete events for those runs
    for run_id in &run_ids {
        diesel::delete(events::table.filter(events::run_id.eq(run_id)))
            .execute(conn)
            .map_err(|e| e.to_string())?;
    }

    // Delete all runs
    diesel::delete(runs::table.filter(runs::issue_id.eq(issue_id)))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    // Delete jobs (merge_requests, artifacts will cascade)
    diesel::delete(jobs::table.filter(jobs::issue_id.eq(issue_id)))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    // Delete the issue (comments will cascade via ON DELETE CASCADE)
    diesel::delete(issues::table.find(issue_id))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::DbProject;
    use crate::services::testing::MockClock;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    /// Helper to create a test issue via the create function with real clock.
    fn create_test_issue_via_impl(
        conn: &mut SqliteConnection,
        project_id: &str,
        title: &str,
    ) -> String {
        use crate::services::RealClock;
        create(
            conn,
            &RealClock,
            CreateIssue {
                project_id: project_id.to_string(),
                title: title.to_string(),
                description: None,
                backend_override: None,
                manager_id: None,
            },
        )
        .expect("Failed to create test issue")
        .id
    }

    #[test]
    fn test_create_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        use crate::services::RealClock;
        let issue = create(
            &mut conn,
            &RealClock,
            CreateIssue {
                project_id: project_id.clone(),
                title: "Test Issue".to_string(),
                description: Some("A description".to_string()),
                backend_override: None,
                manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(issue.title, "Test Issue");
        assert_eq!(issue.description, "A description");
        assert_eq!(issue.number, 1);
        assert_eq!(issue.project_id, project_id);
        assert_eq!(issue.status, IssueStatus::Backlog);
        assert!(issue.merged_at.is_none());
        assert!(issue.closed_at.is_none());
    }

    #[test]
    fn test_create_issue_increments_number() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        use crate::services::RealClock;

        let issue1 = create(
            &mut conn,
            &RealClock,
            CreateIssue {
                project_id: project_id.clone(),
                title: "First".to_string(),
                description: None,
                backend_override: None,

                manager_id: None,
            },
        )
        .unwrap();

        let issue2 = create(
            &mut conn,
            &RealClock,
            CreateIssue {
                project_id: project_id.clone(),
                title: "Second".to_string(),
                description: None,
                backend_override: None,

                manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(issue1.number, 1);
        assert_eq!(issue2.number, 2);
    }

    #[test]
    fn test_create_issue_invalid_project() {
        let mut conn = test_diesel_conn();
        use crate::services::RealClock;
        let result = create(
            &mut conn,
            &RealClock,
            CreateIssue {
                project_id: "nonexistent".to_string(),
                title: "Test".to_string(),
                description: None,
                backend_override: None,

                manager_id: None,
            },
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Project not found"));
    }

    #[test]
    fn test_create_issue_with_clock() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let issue = create(
            &mut conn,
            &mock_clock,
            CreateIssue {
                project_id: project_id.clone(),
                title: "Clock Test Issue".to_string(),
                description: Some("Testing clock injection".to_string()),
                backend_override: None,

                manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(issue.created_at, 1700000000);
        assert_eq!(issue.updated_at, 1700000000);

        let db_issue: DbIssue = issues::table.find(&issue.id).first(&mut conn).unwrap();
        assert_eq!(db_issue.created_at, 1700000000);
        assert_eq!(db_issue.updated_at, 1700000000);
    }

    #[test]
    fn test_create_issue_with_clock_updates_project_timestamp() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        create(
            &mut conn,
            &mock_clock,
            CreateIssue {
                project_id: project_id.clone(),
                title: "Test".to_string(),
                description: None,
                backend_override: None,

                manager_id: None,
            },
        )
        .unwrap();

        let db_project: DbProject = projects::table.find(&project_id).first(&mut conn).unwrap();
        assert_eq!(db_project.updated_at, 1700000000);
    }

    #[test]
    fn test_update_issue_title() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Original Title");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000500);

        let updated = update(
            &mut conn,
            &mock_clock,
            UpdateIssue {
                id: issue_id.clone(),
                title: Some("Updated Title".to_string()),
                description: None,
                backend_override: None,
            },
        )
        .unwrap();

        assert_eq!(updated.title, "Updated Title");
        assert_eq!(updated.updated_at, 1700000500);
    }

    #[test]
    fn test_update_issue_description() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Test Issue");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000600);

        let updated = update(
            &mut conn,
            &mock_clock,
            UpdateIssue {
                id: issue_id.clone(),
                title: None,
                description: Some("New description".to_string()),
                backend_override: None,
            },
        )
        .unwrap();

        assert_eq!(updated.description, "New description");
        assert_eq!(updated.updated_at, 1700000600);
    }

    #[test]
    fn test_update_issue_both_fields() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Original");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000700);

        let updated = update(
            &mut conn,
            &mock_clock,
            UpdateIssue {
                id: issue_id,
                title: Some("New Title".to_string()),
                description: Some("New Description".to_string()),
                backend_override: None,
            },
        )
        .unwrap();

        assert_eq!(updated.title, "New Title");
        assert_eq!(updated.description, "New Description");
        assert_eq!(updated.updated_at, 1700000700);
    }

    #[test]
    fn test_update_issue_nonexistent() {
        let mut conn = test_diesel_conn();

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let result = update(
            &mut conn,
            &mock_clock,
            UpdateIssue {
                id: "nonexistent".to_string(),
                title: Some("Title".to_string()),
                description: None,
                backend_override: None,
            },
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Issue not found"));
    }

    #[test]
    fn test_dismiss_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Test Issue");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700001000);

        dismiss(&mut conn, &mock_clock, &issue_id).unwrap();

        let db_issue: DbIssue = issues::table.find(&issue_id).first(&mut conn).unwrap();
        assert_eq!(db_issue.dismissed_at, Some(1700001000));
        assert_eq!(db_issue.updated_at, 1700001000);
    }

    #[test]
    fn test_restore_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Test Issue");

        diesel::update(issues::table.find(&issue_id))
            .set(issues::dismissed_at.eq(Some(1700000000)))
            .execute(&mut conn)
            .unwrap();

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700002000);

        restore(&mut conn, &mock_clock, &issue_id).unwrap();

        let db_issue: DbIssue = issues::table.find(&issue_id).first(&mut conn).unwrap();
        assert!(db_issue.dismissed_at.is_none());
        assert_eq!(db_issue.updated_at, 1700002000);
    }

    #[test]
    fn test_complete_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Test Issue");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700003000);

        complete(&mut conn, &mock_clock, &issue_id).unwrap();

        let db_issue: DbIssue = issues::table.find(&issue_id).first(&mut conn).unwrap();
        assert_eq!(db_issue.status, "merged");
        assert_eq!(db_issue.merged_at, Some(1700003000));
        assert_eq!(db_issue.completed_at, Some(1700003000));
        assert_eq!(db_issue.updated_at, 1700003000);
    }

    #[test]
    fn test_get_issue() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");
        let issue_id = create_test_issue_via_impl(&mut conn, &project_id, "Test Issue");

        let issue = get(&mut conn, &issue_id).unwrap();
        assert!(issue.is_some());
        assert_eq!(issue.unwrap().title, "Test Issue");
    }

    #[test]
    fn test_get_issue_nonexistent() {
        let mut conn = test_diesel_conn();
        let issue = get(&mut conn, "nonexistent").unwrap();
        assert!(issue.is_none());
    }

    #[test]
    fn test_issue_lifecycle() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        // Create at time 1000
        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1000);
        let issue = create(
            &mut conn,
            &mock_clock,
            CreateIssue {
                project_id: project_id.clone(),
                title: "Lifecycle Test".to_string(),
                description: None,
                backend_override: None,

                manager_id: None,
            },
        )
        .unwrap();
        assert_eq!(issue.created_at, 1000);

        // Update at time 2000
        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 2000);
        let updated = update(
            &mut conn,
            &mock_clock,
            UpdateIssue {
                id: issue.id.clone(),
                title: Some("Updated".to_string()),
                description: None,
                backend_override: None,
            },
        )
        .unwrap();
        assert_eq!(updated.updated_at, 2000);

        // Dismiss at time 3000
        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 3000);
        dismiss(&mut conn, &mock_clock, &issue.id).unwrap();

        // Restore at time 4000
        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 4000);
        restore(&mut conn, &mock_clock, &issue.id).unwrap();

        // Complete at time 5000
        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 5000);
        complete(&mut conn, &mock_clock, &issue.id).unwrap();

        let db_issue: DbIssue = issues::table.find(&issue.id).first(&mut conn).unwrap();
        assert_eq!(db_issue.status, "merged");
        assert_eq!(db_issue.merged_at, Some(5000));
        assert_eq!(db_issue.completed_at, Some(5000));
        assert!(db_issue.dismissed_at.is_none());
        assert_eq!(db_issue.updated_at, 5000);
    }
}
