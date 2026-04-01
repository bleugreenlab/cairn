use crate::diesel_models::{DbProject, NewProject, UpdateProjectChangeset};
use crate::models::CreateProject;
use crate::schema::projects;
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use std::path::Path;
use uuid::Uuid;

/// Full project creation: DB insert + filesystem setup.
///
/// - If `repo_path` is empty and `projects_dir` is provided, creates a directory
///   and initializes a git repo there.
/// - Creates `.cairn/config.yaml` and adds `.cairn/assets/` to `.gitignore`.
/// - Skips filesystem setup for remote project bookmarks.
///
/// Both Tauri and cairn-server call this instead of `create_db` directly.
pub fn create(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    mut input: CreateProject,
    projects_dir: Option<&Path>,
) -> Result<DbProject, String> {
    let is_remote = input.remote_url.is_some() || input.server_id.is_some();

    // For non-remote projects with empty repo_path, create a directory
    if !is_remote && input.repo_path.is_empty() {
        if let Some(base) = projects_dir {
            let project_dir = base.join(input.key.to_lowercase());
            std::fs::create_dir_all(&project_dir)
                .map_err(|e| format!("Failed to create project directory: {}", e))?;

            // Init git repo if not already one
            if !project_dir.join(".git").exists() {
                run_git(&["init"], &project_dir)?;

                // Create initial commit so HEAD is valid (needed for worktrees)
                run_git(
                    &["commit", "--allow-empty", "-m", "Initial commit"],
                    &project_dir,
                )?;
            }

            input.repo_path = project_dir.to_string_lossy().to_string();
        }
    }

    let db_project = create_db(conn, clock, &input)?;

    // Filesystem setup for local projects
    if !is_remote && !input.repo_path.is_empty() {
        let repo_path = Path::new(&input.repo_path);
        if repo_path.exists() {
            if let Err(e) =
                crate::config::project_settings::create_default_project_config(repo_path)
            {
                log::warn!("Failed to create project config: {}", e);
            }
            if let Err(e) = add_cairn_assets_to_gitignore(repo_path) {
                log::warn!("Failed to update .gitignore: {}", e);
            }
        }
    }

    // Auto-create default manager on the default branch
    if !is_remote {
        let default_branch = db_project.default_branch.as_deref().unwrap_or("main");
        if let Err(e) = crate::managers::crud::ensure_default_manager(
            conn,
            clock,
            &db_project.id,
            &db_project.name,
            default_branch,
        ) {
            log::warn!(
                "Failed to create default manager for {}: {}",
                db_project.name,
                e
            );
        }
    }

    Ok(db_project)
}

/// Add `.cairn/assets/` to `.gitignore` if not already present.
fn add_cairn_assets_to_gitignore(repo_path: &Path) -> Result<(), String> {
    use std::io::{BufRead, BufReader, Write};

    let gitignore_path = repo_path.join(".gitignore");
    let assets_entry = ".cairn/assets/";

    if gitignore_path.exists() {
        let file = std::fs::File::open(&gitignore_path).map_err(|e| e.to_string())?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line.map_err(|e| e.to_string())?;
            let trimmed = line.trim();
            if trimmed == assets_entry
                || trimmed == ".cairn/assets"
                || trimmed == ".cairn/"
                || trimmed == ".cairn"
            {
                return Ok(());
            }
        }

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&gitignore_path)
            .map_err(|e| e.to_string())?;

        let contents = std::fs::read_to_string(&gitignore_path).map_err(|e| e.to_string())?;
        if !contents.is_empty() && !contents.ends_with('\n') {
            writeln!(file).map_err(|e| e.to_string())?;
        }
        writeln!(file, "{}", assets_entry).map_err(|e| e.to_string())?;
    } else {
        std::fs::write(&gitignore_path, format!("{}\n", assets_entry))
            .map_err(|e| e.to_string())?;
    }

    // Stage the .gitignore change
    run_git(&["add", ".gitignore"], repo_path)?;

    Ok(())
}

/// Run a git command in a directory, returning an error with stderr if it fails.
fn run_git(args: &[&str], dir: &Path) -> Result<(), String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|e| format!("Failed to run `git {}`: {}", args.join(" "), e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "`git {}` failed: {}",
            args.join(" "),
            stderr.trim()
        ));
    }

    Ok(())
}

/// Insert a new project into the database. Returns the DbProject.
///
/// Low-level DB-only insert. Prefer `create()` which also handles filesystem setup.
pub fn create_db(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    input: &CreateProject,
) -> Result<DbProject, String> {
    let now = clock.now() as i32;
    let id = input
        .id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

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
        server_id: input.server_id.as_deref(),
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
/// Config file changes (setup_commands, etc.) are handled
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

/// Set whether a project is hidden from the sidebar.
pub fn set_hidden_db(conn: &mut SqliteConnection, id: &str, hidden: bool) -> Result<(), String> {
    diesel::update(projects::table.find(id))
        .set(projects::hidden.eq(if hidden { 1 } else { 0 }))
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
            id: None,
            name: "Test".to_string(),
            key: "TEST".to_string(),
            repo_path: "/tmp/test".to_string(),
            remote_url: None,
            server_id: None,
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
            id: None,
            name: "Test".to_string(),
            key: "TEST".to_string(),
            repo_path: "/tmp/test".to_string(),
            remote_url: None,
            server_id: None,
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
                id: None,
                name: "Beta".to_string(),
                key: "BETA".to_string(),
                repo_path: "/tmp/beta".to_string(),
                remote_url: None,
                server_id: None,
            },
        )
        .unwrap();

        create_db(
            &mut conn,
            &clock,
            &CreateProject {
                id: None,
                name: "Alpha".to_string(),
                key: "ALPHA".to_string(),
                repo_path: "/tmp/alpha".to_string(),
                remote_url: None,
                server_id: None,
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
            id: None,
            name: "Test".to_string(),
            key: "TEST".to_string(),
            repo_path: "/tmp/test".to_string(),
            remote_url: None,
            server_id: None,
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
                id: None,
                name: "Test".to_string(),
                key: "TEST".to_string(),
                repo_path: "/tmp/test".to_string(),
                remote_url: None,
                server_id: None,
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
                progress: "backlog",
                attention: "none",
                created_at: 0,
                updated_at: 0,
                priority: None,
                model: None,
                manager_id: None,
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

    #[test]
    fn test_create_auto_creates_default_manager() {
        use crate::schema::managers;

        let mut conn = test_diesel_conn();
        let clock = RealClock;

        // Use create_db + auto-manager logic via full create() with a temp dir
        let temp = tempfile::TempDir::new().unwrap();
        let repo_path = temp.path().to_string_lossy().to_string();

        // Init a git repo so create() doesn't fail on filesystem setup
        std::process::Command::new("git")
            .args(["init"])
            .current_dir(temp.path())
            .output()
            .unwrap();
        std::process::Command::new("git")
            .args(["commit", "--allow-empty", "-m", "init"])
            .current_dir(temp.path())
            .output()
            .unwrap();

        let input = CreateProject {
            id: None,
            name: "AutoMgr".to_string(),
            key: "AUTOMGR".to_string(),
            repo_path,
            remote_url: None,
            server_id: None,
        };

        let project = create(&mut conn, &clock, input, None).unwrap();

        // A default manager should have been created
        let mgrs: Vec<crate::diesel_models::DbManager> = managers::table
            .filter(managers::project_id.eq(&project.id))
            .load(&mut conn)
            .unwrap();
        assert_eq!(mgrs.len(), 1, "Expected exactly one default manager");
        assert_eq!(mgrs[0].name, "AutoMgr");
        assert_eq!(mgrs[0].branch.as_deref(), Some("main"));
    }

    #[test]
    fn test_create_remote_project_no_manager() {
        use crate::schema::managers;

        let mut conn = test_diesel_conn();
        let clock = RealClock;

        let input = CreateProject {
            id: None,
            name: "Remote".to_string(),
            key: "REMOTE".to_string(),
            repo_path: String::new(),
            remote_url: Some("https://example.com".to_string()),
            server_id: None,
        };

        let project = create(&mut conn, &clock, input, None).unwrap();

        let mgrs: Vec<crate::diesel_models::DbManager> = managers::table
            .filter(managers::project_id.eq(&project.id))
            .load(&mut conn)
            .unwrap();
        assert_eq!(
            mgrs.len(),
            0,
            "Remote projects should not get a default manager"
        );
    }
}
