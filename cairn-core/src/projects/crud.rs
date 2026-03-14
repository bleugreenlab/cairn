use crate::diesel_models::{DbProject, NewProject, UpdateProjectChangeset};
use crate::models::CreateProject;
use crate::schema::projects;
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use uuid::Uuid;

/// Insert a new project into the database. Returns the DbProject.
///
/// Does NOT create config files, gitignore entries, or emit events.
/// Those side effects belong in the Tauri command layer.
pub fn create_db(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: &CreateProject,
) -> Result<DbProject, String> {
    let now = clock.now() as i32;
    let id = Uuid::new_v4().to_string();

    let new_project = NewProject {
        id: &id,
        workspace_id: "default",
        name: &input.name,
        key: &input.key,
        repo_path: &input.repo_path,
        context: Some(""),
        docs_enabled: Some(1),
        default_branch: Some("main"),
        next_issue_number: Some(1),
        created_at: now,
        updated_at: now,
        remote_url: input.remote_url.as_deref(),
        remote_api_key: input.remote_api_key.as_deref(),
    };

    diesel::insert_into(projects::table)
        .values(&new_project)
        .execute(conn)
        .map_err(|e| e.to_string())?;

    projects::table
        .find(&id)
        .first(conn)
        .map_err(|e| e.to_string())
}

/// Get a project by ID (raw DB record).
pub fn get_db(conn: &mut SqliteConnection, id: &str) -> Result<Option<DbProject>, String> {
    projects::table
        .find(id)
        .first(conn)
        .optional()
        .map_err(|e| e.to_string())
}

/// List all projects ordered by name (raw DB records).
pub fn list_db(conn: &mut SqliteConnection) -> Result<Vec<DbProject>, String> {
    projects::table
        .order(projects::name.asc())
        .load(conn)
        .map_err(|e| e.to_string())
}

/// Update only the timestamp in the database.
///
/// Config file changes (setup_commands, copy_files, etc.) are handled
/// by the Tauri command layer via project_settings.
pub fn update_timestamp_db(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    id: &str,
) -> Result<(), String> {
    let now = clock.now() as i32;
    let changeset = UpdateProjectChangeset {
        updated_at: Some(now),
        ci_commands: None,
        setup_commands: None,
        terminal_commands: None,
        next_issue_number: None,
    };

    diesel::update(projects::table.find(id))
        .set(&changeset)
        .execute(conn)
        .map_err(|e| e.to_string())?;

    Ok(())
}

/// Delete a project from the database. CASCADE handles related records.
///
/// Does NOT clean up worktrees on disk — that's the Tauri command's responsibility.
pub fn delete_db(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    let exists = projects::table
        .find(id)
        .first::<DbProject>(conn)
        .optional()
        .map_err(|e| e.to_string())?;

    if exists.is_none() {
        return Err("Project not found".to_string());
    }

    diesel::delete(projects::table.find(id))
        .execute(conn)
        .map_err(|e| format!("Failed to delete project: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::RealClock;
    use crate::test_utils::test_diesel_conn;

    #[test]
    fn test_create_db() {
        let mut conn = test_diesel_conn();
        let clock = RealClock;

        let input = CreateProject {
            name: "Test".to_string(),
            key: "TEST".to_string(),
            repo_path: "/tmp/test".to_string(),
            remote_url: None,
            remote_api_key: None,
        };

        let db_project = create_db(&mut conn, &clock, &input).unwrap();
        assert_eq!(db_project.name, "Test");
        assert_eq!(db_project.key, "TEST");
        assert_eq!(db_project.repo_path, "/tmp/test");
        assert_eq!(db_project.next_issue_number, Some(1));
    }

    #[test]
    fn test_get_db() {
        let mut conn = test_diesel_conn();
        let clock = RealClock;

        let input = CreateProject {
            name: "Test".to_string(),
            key: "TEST".to_string(),
            repo_path: "/tmp/test".to_string(),
            remote_url: None,
            remote_api_key: None,
        };

        let created = create_db(&mut conn, &clock, &input).unwrap();
        let fetched = get_db(&mut conn, &created.id).unwrap().unwrap();
        assert_eq!(fetched.name, "Test");

        let missing = get_db(&mut conn, "nonexistent").unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_list_db() {
        let mut conn = test_diesel_conn();
        let clock = RealClock;

        create_db(
            &mut conn,
            &clock,
            &CreateProject {
                name: "Beta".to_string(),
                key: "BETA".to_string(),
                repo_path: "/tmp/beta".to_string(),
                remote_url: None,
                remote_api_key: None,
            },
        )
        .unwrap();

        create_db(
            &mut conn,
            &clock,
            &CreateProject {
                name: "Alpha".to_string(),
                key: "ALPHA".to_string(),
                repo_path: "/tmp/alpha".to_string(),
                remote_url: None,
                remote_api_key: None,
            },
        )
        .unwrap();

        let projects = list_db(&mut conn).unwrap();
        assert_eq!(projects.len(), 2);
        // Ordered by name
        assert_eq!(projects[0].name, "Alpha");
        assert_eq!(projects[1].name, "Beta");
    }

    #[test]
    fn test_delete_db() {
        let mut conn = test_diesel_conn();
        let clock = RealClock;

        let input = CreateProject {
            name: "Test".to_string(),
            key: "TEST".to_string(),
            repo_path: "/tmp/test".to_string(),
            remote_url: None,
            remote_api_key: None,
        };

        let created = create_db(&mut conn, &clock, &input).unwrap();

        delete_db(&mut conn, &created.id).unwrap();
        assert!(get_db(&mut conn, &created.id).unwrap().is_none());
    }

    #[test]
    fn test_delete_db_not_found() {
        let mut conn = test_diesel_conn();
        let result = delete_db(&mut conn, "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_delete_db_cascades() {
        use crate::diesel_models::NewIssue;
        use crate::schema::issues;

        let mut conn = test_diesel_conn();
        let clock = RealClock;

        let project = create_db(
            &mut conn,
            &clock,
            &CreateProject {
                name: "Test".to_string(),
                key: "TEST".to_string(),
                repo_path: "/tmp/test".to_string(),
                remote_url: None,
                remote_api_key: None,
            },
        )
        .unwrap();

        // Add an issue
        diesel::insert_into(issues::table)
            .values(NewIssue {
                id: "issue-1",
                project_id: &project.id,
                number: 1,
                title: "Test Issue",
                description: Some(""),
                status: "backlog",
                created_at: 0,
                updated_at: 0,
                priority: None,
                model: None,
                skills: None,
            })
            .execute(&mut conn)
            .unwrap();

        // Verify issue exists
        let count: i64 = issues::table
            .filter(issues::project_id.eq(&project.id))
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(count, 1);

        // Delete project — issue should cascade
        delete_db(&mut conn, &project.id).unwrap();

        let count: i64 = issues::table
            .filter(issues::project_id.eq(&project.id))
            .count()
            .get_result(&mut conn)
            .unwrap();
        assert_eq!(count, 0);
    }
}
