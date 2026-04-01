//! Manager CRUD operations.

use crate::diesel_models::{
    DbManager, NewJob, NewManager, NewManagerScope, UpdateManagerChangeset,
};
use crate::models::{
    CreateManager, Issue, Manager, ManagerScopeKind, ManagerStatus, UpdateManager,
};
use crate::schema::{issues, jobs, manager_scopes, managers};
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use uuid::Uuid;

/// Convert DbManager to Manager model.
fn db_manager_to_manager(db: DbManager) -> Manager {
    Manager {
        id: db.id,
        project_id: db.project_id,
        home_project_id: db.home_project_id,
        scope_kind: db.scope_kind.parse().unwrap_or(ManagerScopeKind::Branch),
        name: db.name,
        description: db.description,
        branch: db.branch.unwrap_or_default(),
        job_id: db.job_id,
        status: db.status.parse().unwrap_or(ManagerStatus::Active),
        current_session_id: db.current_session_id,
        current_turn_id: db.current_turn_id,
        last_wake_at: db.last_wake_at.map(i64::from),
        last_turn_completed_at: db.last_turn_completed_at.map(i64::from),
        last_error: db.last_error,
        agent_config_id: db.agent_config_id,
        tier: db.model.as_ref().and_then(|s| s.parse().ok()),
        parent_manager_id: db.parent_manager_id,
        created_at: db.created_at as i64,
        updated_at: db.updated_at as i64,
        execution_id: db.execution_id,
    }
}

/// Create a new manager with its associated job.
pub fn create(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: CreateManager,
) -> Result<Manager, String> {
    let now = clock.now() as i32;
    let id = Uuid::new_v4().to_string();
    let job_id = Uuid::new_v4().to_string();
    let project_id = input.project_id.clone();
    let home_project_id = input
        .home_project_id
        .clone()
        .unwrap_or_else(|| project_id.clone());
    let model_str: Option<String> = input.tier.as_ref().map(|m| m.to_string());
    let description = input.description.as_deref().unwrap_or("");
    let scope_kind = input.scope_kind.clone().unwrap_or(ManagerScopeKind::Branch);
    let scope_kind_str = scope_kind.to_string();
    let branch = (!input.branch.is_empty()).then_some(input.branch.as_str());

    // Default to "manager" agent config if none specified
    let agent_config_id: Option<&str> = input.agent_config_id.as_deref().or(Some("manager"));

    let new_job = NewJob {
        id: &job_id,
        execution_id: None,
        manager_id: None,
        recipe_node_id: None,
        parent_job_id: None,
        worktree_path: None,
        branch,
        base_commit: None,
        current_session_id: None,
        resume_session_id: None,
        status: "pending",
        agent_config_id,
        issue_id: None,
        project_id: &project_id,
        task_description: None,
        created_at: now,
        updated_at: now,
        completed_at: None,
        parent_tool_use_id: None,
        task_index: None,
        started_at: None,
        model: model_str.as_deref(),
        node_name: Some(input.name.as_str()),
        base_branch: None,
        current_turn_id: None,
    };

    let new_manager = NewManager {
        id: &id,
        project_id: &project_id,
        home_project_id: Some(&home_project_id),
        scope_kind: &scope_kind_str,
        name: &input.name,
        description,
        branch,
        job_id: Some(&job_id),
        status: "active",
        current_session_id: None,
        current_turn_id: None,
        last_wake_at: None,
        last_turn_completed_at: None,
        last_error: None,
        agent_config_id,
        model: model_str.as_deref(),
        parent_manager_id: input.parent_manager_id.as_deref(),
        created_at: now,
        updated_at: now,
        execution_id: None,
    };

    let scope_id = format!("scope-{}", id);
    let new_scope = NewManagerScope {
        id: &scope_id,
        manager_id: &id,
        project_id: Some(&project_id),
        scope_kind: &scope_kind_str,
        branch,
        created_at: now,
    };

    conn.transaction::<(), diesel::result::Error, _>(|conn| {
        diesel::insert_into(jobs::table)
            .values(&new_job)
            .execute(conn)?;
        diesel::insert_into(managers::table)
            .values(&new_manager)
            .execute(conn)?;
        diesel::insert_into(manager_scopes::table)
            .values(&new_scope)
            .execute(conn)?;
        diesel::update(jobs::table.find(&job_id))
            .set(jobs::manager_id.eq(Some(id.as_str())))
            .execute(conn)?;
        Ok(())
    })
    .map_err(|e| format!("Failed to create manager: {}", e))?;

    Ok(Manager {
        id,
        project_id,
        home_project_id: Some(home_project_id),
        scope_kind,
        name: input.name,
        description: description.to_string(),
        branch: input.branch,
        job_id: Some(job_id),
        status: ManagerStatus::Active,
        current_session_id: None,
        current_turn_id: None,
        last_wake_at: None,
        last_turn_completed_at: None,
        last_error: None,
        agent_config_id: agent_config_id.map(|s| s.to_string()),
        tier: input.tier,
        parent_manager_id: input.parent_manager_id,
        created_at: now as i64,
        updated_at: now as i64,
        execution_id: None,
    })
}

/// Ensure a default manager exists for a project on its default branch.
///
/// If no manager exists on `default_branch`, creates one named after the project.
/// Returns `Some(manager)` if one was created, `None` if one already existed.
pub fn ensure_default_manager(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    project_id: &str,
    project_name: &str,
    default_branch: &str,
) -> Result<Option<Manager>, String> {
    // Check if any manager already exists on the default branch
    let existing: Option<DbManager> = managers::table
        .filter(managers::project_id.eq(project_id))
        .filter(managers::branch.eq(default_branch))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if existing.is_some() {
        return Ok(None);
    }

    let manager = create(
        conn,
        clock,
        CreateManager {
            project_id: project_id.to_string(),
            home_project_id: None,
            scope_kind: None,
            name: project_name.to_string(),
            branch: default_branch.to_string(),
            description: None,
            agent_config_id: None,
            tier: None,
            parent_manager_id: None,
        },
    )?;

    Ok(Some(manager))
}

/// Get a manager by ID.
pub fn get(conn: &mut SqliteConnection, id: &str) -> Result<Option<Manager>, String> {
    let db_manager: Option<DbManager> = managers::table
        .find(id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_manager.map(db_manager_to_manager))
}

/// Get a manager by project and branch.
pub fn get_by_branch(
    conn: &mut SqliteConnection,
    project_id: &str,
    branch: &str,
) -> Result<Option<Manager>, String> {
    let db_manager: Option<DbManager> = managers::table
        .filter(managers::project_id.eq(project_id))
        .filter(managers::branch.eq(branch))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_manager.map(db_manager_to_manager))
}

/// Get a manager by a job ID.
///
/// New actor-native jobs resolve through `jobs.manager_id`. The legacy
/// `managers.job_id` path remains as a compatibility fallback for older rows.
pub fn get_by_job_id(conn: &mut SqliteConnection, job_id: &str) -> Result<Option<Manager>, String> {
    let actor_manager_id: Option<Option<String>> = jobs::table
        .find(job_id)
        .select(jobs::manager_id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if let Some(Some(manager_id)) = actor_manager_id {
        return get(conn, &manager_id);
    }

    let db_manager: Option<DbManager> = managers::table
        .filter(managers::job_id.eq(job_id))
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    Ok(db_manager.map(db_manager_to_manager))
}

/// List all managers for a project, newest first.
pub fn list(conn: &mut SqliteConnection, project_id: &str) -> Result<Vec<Manager>, String> {
    let db_managers: Vec<DbManager> = managers::table
        .filter(managers::project_id.eq(project_id))
        .order(managers::created_at.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_managers.into_iter().map(db_manager_to_manager).collect())
}

/// Update a manager.
pub fn update(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: UpdateManager,
) -> Result<Manager, String> {
    let now = clock.now() as i32;

    let model_str: Option<String> = input
        .tier
        .as_ref()
        .and_then(|m| m.as_ref().map(|m| m.to_string()));
    let model_update: Option<Option<&str>> = input
        .tier
        .as_ref()
        .map(|m| m.as_ref().map(|_| model_str.as_deref().unwrap()));

    let agent_config_str: Option<Option<&str>> =
        input.agent_config_id.as_ref().map(|a| a.as_deref());

    let status_str = input.status.as_ref().map(|s| s.to_string());

    let changeset = UpdateManagerChangeset {
        name: input.name.as_deref(),
        description: input.description.as_deref(),
        status: status_str.as_deref(),
        model: model_update,
        agent_config_id: agent_config_str,
        updated_at: Some(now),
        ..Default::default()
    };

    diesel::update(managers::table.find(&input.id))
        .set(&changeset)
        .execute(conn)
        .map_err(|e| e.to_string())?;

    get(conn, &input.id)?.ok_or_else(|| "Manager not found".to_string())
}

/// Delete a manager.
pub fn delete(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    diesel::delete(managers::table.find(id))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// List issues managed by a specific manager.
pub fn list_managed_issues(
    conn: &mut SqliteConnection,
    manager_id: &str,
) -> Result<Vec<Issue>, String> {
    use crate::diesel_models::DbIssue;
    use crate::issues::crud::db_issue_to_issue;

    let db_issues: Vec<DbIssue> = issues::table
        .filter(issues::manager_id.eq(manager_id))
        .order(issues::number.desc())
        .load(conn)
        .map_err(|e| e.to_string())?;

    Ok(db_issues.into_iter().map(db_issue_to_issue).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::testing::MockClock;
    use crate::test_utils::{create_test_project, test_diesel_conn};

    #[test]
    fn test_create_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Feature Manager".to_string(),
                branch: "feature/test".to_string(),
                description: Some("Manages feature work".to_string()),
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(manager.name, "Feature Manager");
        assert_eq!(manager.branch, "feature/test");
        assert_eq!(manager.description, "Manages feature work");
        assert_eq!(manager.status, ManagerStatus::Active);
        assert!(manager.job_id.is_some());
        assert_eq!(manager.created_at, 1700000000);
    }

    #[test]
    fn test_create_manager_creates_job() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Test Manager".to_string(),
                branch: "mgr/test".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        // Verify job was created
        let job_id = manager.job_id.unwrap();
        let job: crate::diesel_models::DbJob = jobs::table
            .find(&job_id)
            .first(&mut conn)
            .expect("Job should exist");

        assert_eq!(job.branch, Some("mgr/test".to_string()));
        assert_eq!(job.status, "pending");
        assert_eq!(job.project_id, project_id);
        assert_eq!(job.node_name, Some("Test Manager".to_string()));
    }

    #[test]
    fn test_create_manager_defaults_agent_config_to_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        // Create with agent_config_id: None — should default to "manager"
        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Default Config".to_string(),
                branch: "mgr/default".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        // Manager record should have "manager" as agent_config_id
        assert_eq!(
            manager.agent_config_id,
            Some("manager".to_string()),
            "Manager should default agent_config_id to 'manager'"
        );

        // Job record should also have "manager" as agent_config_id
        let job_id = manager.job_id.unwrap();
        let job: crate::diesel_models::DbJob = jobs::table
            .find(&job_id)
            .first(&mut conn)
            .expect("Job should exist");
        assert_eq!(
            job.agent_config_id,
            Some("manager".to_string()),
            "Job should inherit the defaulted 'manager' agent_config_id"
        );

        // Verify round-trip through DB
        let loaded = get(&mut conn, &manager.id).unwrap().unwrap();
        assert_eq!(loaded.agent_config_id, Some("manager".to_string()));
    }

    #[test]
    fn test_create_manager_explicit_agent_config_overrides_default() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        // Create with explicit agent_config_id — should NOT be overridden
        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Custom Config".to_string(),
                branch: "mgr/custom".to_string(),
                description: None,
                agent_config_id: Some("my-custom-agent".to_string()),
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(
            manager.agent_config_id,
            Some("my-custom-agent".to_string()),
            "Explicit agent_config_id should not be overridden by default"
        );

        // Job should also have the explicit value
        let job_id = manager.job_id.unwrap();
        let job: crate::diesel_models::DbJob = jobs::table
            .find(&job_id)
            .first(&mut conn)
            .expect("Job should exist");
        assert_eq!(
            job.agent_config_id,
            Some("my-custom-agent".to_string()),
            "Job should have the explicit agent_config_id, not the default"
        );
    }

    #[test]
    fn test_get_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Test".to_string(),
                branch: "test".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let found = get(&mut conn, &manager.id).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Test");
    }

    #[test]
    fn test_get_manager_nonexistent() {
        let mut conn = test_diesel_conn();
        let found = get(&mut conn, "nonexistent").unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_get_by_branch() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Branch Manager".to_string(),
                branch: "feature/branch-test".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let found = get_by_branch(&mut conn, &project_id, "feature/branch-test").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "Branch Manager");

        let not_found = get_by_branch(&mut conn, &project_id, "nonexistent").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_list_managers() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "First".to_string(),
                branch: "first".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let mut mock_clock2 = MockClock::new();
        mock_clock2.expect_now().returning(|| 1700001000);

        create(
            &mut conn,
            &mock_clock2,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Second".to_string(),
                branch: "second".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let managers = list(&mut conn, &project_id).unwrap();
        assert_eq!(managers.len(), 2);
        // Newest first
        assert_eq!(managers[0].name, "Second");
        assert_eq!(managers[1].name, "First");
    }

    #[test]
    fn test_update_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Original".to_string(),
                branch: "original".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let mut mock_clock2 = MockClock::new();
        mock_clock2.expect_now().returning(|| 1700001000);

        let updated = update(
            &mut conn,
            &mock_clock2,
            UpdateManager {
                id: manager.id.clone(),
                name: Some("Updated".to_string()),
                description: Some("New description".to_string()),
                status: Some(ManagerStatus::Paused),
                tier: None,
                agent_config_id: None,
            },
        )
        .unwrap();

        assert_eq!(updated.name, "Updated");
        assert_eq!(updated.description, "New description");
        assert_eq!(updated.status, ManagerStatus::Paused);
        assert_eq!(updated.updated_at, 1700001000);
    }

    #[test]
    fn test_delete_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "ToDelete".to_string(),
                branch: "delete-me".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        delete(&mut conn, &manager.id).unwrap();

        let found = get(&mut conn, &manager.id).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_list_managed_issues() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Test Manager".to_string(),
                branch: "mgr".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        // Create an issue with this manager
        let mut mock_clock2 = MockClock::new();
        mock_clock2.expect_now().returning(|| 1700001000);

        let issue = crate::issues::crud::create(
            &mut conn,
            &mock_clock2,
            crate::models::CreateIssue {
                project_id: project_id.clone(),
                title: "Managed Issue".to_string(),
                description: None,
                backend_override: None,
                manager_id: Some(manager.id.clone()),
            },
        )
        .unwrap();

        let managed = list_managed_issues(&mut conn, &manager.id).unwrap();
        assert_eq!(managed.len(), 1);
        assert_eq!(managed[0].id, issue.id);
        assert_eq!(managed[0].manager_id, Some(manager.id.clone()));
    }

    #[test]
    fn test_manager_status_display_and_from_str() {
        // Display
        assert_eq!(ManagerStatus::Active.to_string(), "active");
        assert_eq!(ManagerStatus::Paused.to_string(), "paused");
        assert_eq!(ManagerStatus::Completed.to_string(), "completed");

        // FromStr round-trip
        assert_eq!(
            "active".parse::<ManagerStatus>().unwrap(),
            ManagerStatus::Active
        );
        assert_eq!(
            "paused".parse::<ManagerStatus>().unwrap(),
            ManagerStatus::Paused
        );
        assert_eq!(
            "completed".parse::<ManagerStatus>().unwrap(),
            ManagerStatus::Completed
        );

        // Case insensitive
        assert_eq!(
            "ACTIVE".parse::<ManagerStatus>().unwrap(),
            ManagerStatus::Active
        );
        assert_eq!(
            "Paused".parse::<ManagerStatus>().unwrap(),
            ManagerStatus::Paused
        );

        // Unknown string returns error
        assert!("unknown".parse::<ManagerStatus>().is_err());
    }

    #[test]
    fn test_status_check_constraint_rejects_invalid() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Test".to_string(),
                branch: "test".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        // DB CHECK constraint prevents invalid status values
        let result = diesel::update(managers::table.find(&manager.id))
            .set(managers::status.eq("bogus"))
            .execute(&mut conn);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_with_optional_fields() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Full Manager".to_string(),
                branch: "full/test".to_string(),
                description: Some("Has all fields".to_string()),
                agent_config_id: Some("custom-agent".to_string()),
                tier: Some(crate::models::Model::new("opus")),
                parent_manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(manager.agent_config_id, Some("custom-agent".to_string()));
        assert_eq!(manager.tier, Some(crate::models::Model::new("opus")));

        // Verify round-trip through DB
        let loaded = get(&mut conn, &manager.id).unwrap().unwrap();
        assert_eq!(loaded.agent_config_id, Some("custom-agent".to_string()));
        assert_eq!(loaded.tier, Some(crate::models::Model::new("opus")));
    }

    #[test]
    fn test_create_with_parent_manager() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let parent = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Parent".to_string(),
                branch: "parent".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let mut mock_clock2 = MockClock::new();
        mock_clock2.expect_now().returning(|| 1700001000);

        let child = create(
            &mut conn,
            &mock_clock2,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Child".to_string(),
                branch: "child".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: Some(parent.id.clone()),
            },
        )
        .unwrap();

        assert_eq!(child.parent_manager_id, Some(parent.id.clone()));

        let loaded = get(&mut conn, &child.id).unwrap().unwrap();
        assert_eq!(loaded.parent_manager_id, Some(parent.id));
    }

    #[test]
    fn test_update_clears_optional_fields() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        // Create with tier and agent_config_id set
        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Test".to_string(),
                branch: "test".to_string(),
                description: None,
                agent_config_id: Some("my-agent".to_string()),
                tier: Some(crate::models::Model::new("sonnet")),
                parent_manager_id: None,
            },
        )
        .unwrap();

        assert!(manager.tier.is_some());
        assert!(manager.agent_config_id.is_some());

        // Clear both by setting to Some(None)
        let mut mock_clock2 = MockClock::new();
        mock_clock2.expect_now().returning(|| 1700001000);

        let updated = update(
            &mut conn,
            &mock_clock2,
            UpdateManager {
                id: manager.id.clone(),
                name: None,
                description: None,
                status: None,
                tier: Some(None),
                agent_config_id: Some(None),
            },
        )
        .unwrap();

        assert_eq!(updated.tier, None);
        assert_eq!(updated.agent_config_id, None);

        // Verify persisted
        let loaded = get(&mut conn, &manager.id).unwrap().unwrap();
        assert_eq!(loaded.tier, None);
        assert_eq!(loaded.agent_config_id, None);
    }

    #[test]
    fn test_get_by_branch_scoped_to_project() {
        let mut conn = test_diesel_conn();
        let proj_a = create_test_project(&mut conn, "Project A", "PROJA");
        let proj_b = create_test_project(&mut conn, "Project B", "PROJB");

        let mut clock1 = MockClock::new();
        clock1.expect_now().returning(|| 1700000000);
        let mut clock2 = MockClock::new();
        clock2.expect_now().returning(|| 1700000000);

        // Same branch name in two projects
        create(
            &mut conn,
            &clock1,
            CreateManager {
                project_id: proj_a.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Manager A".to_string(),
                branch: "shared-branch".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        create(
            &mut conn,
            &clock2,
            CreateManager {
                project_id: proj_b.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Manager B".to_string(),
                branch: "shared-branch".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let found_a = get_by_branch(&mut conn, &proj_a, "shared-branch")
            .unwrap()
            .unwrap();
        assert_eq!(found_a.name, "Manager A");

        let found_b = get_by_branch(&mut conn, &proj_b, "shared-branch")
            .unwrap()
            .unwrap();
        assert_eq!(found_b.name, "Manager B");
    }

    #[test]
    fn test_list_scoped_to_project() {
        let mut conn = test_diesel_conn();
        let proj_a = create_test_project(&mut conn, "Project A", "PROJA");
        let proj_b = create_test_project(&mut conn, "Project B", "PROJB");

        let mut clock1 = MockClock::new();
        clock1.expect_now().returning(|| 1700000000);
        let mut clock2 = MockClock::new();
        clock2.expect_now().returning(|| 1700000000);

        create(
            &mut conn,
            &clock1,
            CreateManager {
                project_id: proj_a.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "A's Manager".to_string(),
                branch: "a-branch".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        create(
            &mut conn,
            &clock2,
            CreateManager {
                project_id: proj_b.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "B's Manager".to_string(),
                branch: "b-branch".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let a_managers = list(&mut conn, &proj_a).unwrap();
        assert_eq!(a_managers.len(), 1);
        assert_eq!(a_managers[0].name, "A's Manager");

        let b_managers = list(&mut conn, &proj_b).unwrap();
        assert_eq!(b_managers.len(), 1);
        assert_eq!(b_managers[0].name, "B's Manager");
    }

    #[test]
    fn test_list_managed_issues_empty() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Lonely Manager".to_string(),
                branch: "lonely".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let managed = list_managed_issues(&mut conn, &manager.id).unwrap();
        assert!(managed.is_empty());
    }

    #[test]
    fn test_get_by_job_id() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Job Lookup".to_string(),
                branch: "job-lookup".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let job_id = manager.job_id.as_ref().unwrap();

        // Found by job_id
        let found = get_by_job_id(&mut conn, job_id).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, manager.id);

        // Not found for nonexistent job_id
        let not_found = get_by_job_id(&mut conn, "nonexistent-job").unwrap();
        assert!(not_found.is_none());
    }

    #[test]
    fn test_create_description_defaults_to_empty() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let manager = create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "No Desc".to_string(),
                branch: "no-desc".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        assert_eq!(manager.description, "");

        // Verify persisted
        let loaded = get(&mut conn, &manager.id).unwrap().unwrap();
        assert_eq!(loaded.description, "");
    }

    #[test]
    fn test_ensure_default_manager_creates_when_missing() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        let result =
            ensure_default_manager(&mut conn, &mock_clock, &project_id, "Test Project", "main")
                .unwrap();

        assert!(result.is_some(), "Should have created a manager");
        let manager = result.unwrap();
        assert_eq!(manager.name, "Test Project");
        assert_eq!(manager.branch, "main");
        assert_eq!(manager.status, ManagerStatus::Active);
    }

    #[test]
    fn test_ensure_default_manager_noop_when_exists() {
        let mut conn = test_diesel_conn();
        let project_id = create_test_project(&mut conn, "Test Project", "TEST");

        let mut mock_clock = MockClock::new();
        mock_clock.expect_now().returning(|| 1700000000);

        // Create a manager on "main" first
        create(
            &mut conn,
            &mock_clock,
            CreateManager {
                project_id: project_id.clone(),
                home_project_id: None,
                scope_kind: None,
                name: "Existing".to_string(),
                branch: "main".to_string(),
                description: None,
                agent_config_id: None,
                tier: None,
                parent_manager_id: None,
            },
        )
        .unwrap();

        let mut mock_clock2 = MockClock::new();
        mock_clock2.expect_now().returning(|| 1700001000);

        let result =
            ensure_default_manager(&mut conn, &mock_clock2, &project_id, "Test Project", "main")
                .unwrap();

        assert!(result.is_none(), "Should not create a duplicate manager");

        // Verify still only one manager on main
        let all = list(&mut conn, &project_id).unwrap();
        let main_managers: Vec<_> = all.iter().filter(|m| m.branch == "main").collect();
        assert_eq!(main_managers.len(), 1);
    }
}
