use crate::db_records::DbProject;
use crate::error::CairnError;
use crate::models::CreateProject;
use crate::services::Clock;
use crate::storage::{DbError, LocalDb, RowExt};
use std::path::Path;
use uuid::Uuid;

/// Full project creation: DB insert + filesystem setup.
///
/// - If `repo_path` is empty and `projects_dir` is provided, creates a directory
///   and initializes a git repo there.
/// - Creates `.cairn/config.yaml` and adds `.cairn/assets/` to `.gitignore`.
/// - Skips filesystem setup for remote project bookmarks.
///
pub async fn create(
    db: &LocalDb,
    clock: &dyn Clock,
    mut input: CreateProject,
    projects_dir: Option<&Path>,
) -> Result<DbProject, CairnError> {
    let is_remote = input.remote_url.is_some() || input.server_id.is_some();

    if !is_remote && input.repo_path.is_empty() {
        if let Some(base) = projects_dir {
            let project_dir = base.join(input.key.to_lowercase());
            std::fs::create_dir_all(&project_dir)?;

            if !project_dir.join(".git").exists() {
                run_git(&["init"], &project_dir)?;
                run_git(
                    &["commit", "--allow-empty", "-m", "Initial commit"],
                    &project_dir,
                )?;
            }

            input.repo_path = project_dir.to_string_lossy().to_string();
        }
    }

    let mut db_project = create_db(db, clock, &input).await?;

    if !is_remote && !input.repo_path.is_empty() {
        let repo_path = Path::new(&input.repo_path);
        if repo_path.exists() {
            // Persist the repository's actual default branch so worktrees are
            // based on the correct ref. Without this every project defaults to
            // "main", which fails for repos whose trunk is e.g. "staging".
            if let Some(branch) = detect_default_branch(repo_path) {
                match set_default_branch_db(db, &db_project.id, &branch).await {
                    Ok(()) => db_project.default_branch = Some(branch),
                    Err(e) => log::warn!("Failed to persist detected default branch: {}", e),
                }
            }
            if let Err(e) =
                crate::config::project_settings::create_default_project_config(repo_path)
            {
                log::warn!("Failed to create project config: {}", e);
            }
            if let Err(e) = add_cairn_assets_to_gitignore(repo_path) {
                log::warn!("Failed to update .gitignore: {}", e);
            }
            if let Err(e) = ensure_initial_commit(repo_path) {
                log::warn!("Failed to ensure initial project commit: {}", e);
            }
        }
    }

    Ok(db_project)
}

/// Add `.cairn/assets/` to `.gitignore` if not already present.
fn add_cairn_assets_to_gitignore(repo_path: &Path) -> Result<(), CairnError> {
    use std::io::{BufRead, BufReader, Write};

    let gitignore_path = repo_path.join(".gitignore");
    let assets_entry = ".cairn/assets/";

    if gitignore_path.exists() {
        let file = std::fs::File::open(&gitignore_path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
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
            .open(&gitignore_path)?;

        let contents = std::fs::read_to_string(&gitignore_path)?;
        if !contents.is_empty() && !contents.ends_with('\n') {
            writeln!(file)?;
        }
        writeln!(file, "{}", assets_entry)?;
    } else {
        std::fs::write(&gitignore_path, format!("{}\n", assets_entry))?;
    }

    // Stage the .gitignore change
    run_git(&["add", ".gitignore"], repo_path)?;

    Ok(())
}

/// Ensure a local project repository has at least one commit so git worktrees can branch from it.
fn ensure_initial_commit(repo_path: &Path) -> Result<(), CairnError> {
    let has_head = std::process::Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .current_dir(repo_path)
        .output()?;
    if has_head.status.success() {
        return Ok(());
    }

    run_git(&["add", "-A"], repo_path)?;
    run_git(
        &[
            "-c",
            "user.name=Cairn",
            "-c",
            "user.email=cairn@local.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "Initial commit",
        ],
        repo_path,
    )
}

/// Detect a repository's default branch.
///
/// Prefers the remote HEAD (`refs/remotes/origin/HEAD` → e.g. "staging"), then
/// the currently checked-out branch. Returns `None` when neither resolves, in
/// which case callers keep the stored default.
fn detect_default_branch(repo_path: &Path) -> Option<String> {
    let git_line = |args: &[&str]| -> Option<String> {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let line = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if line.is_empty() {
            None
        } else {
            Some(line)
        }
    };

    if let Some(head) = git_line(&["symbolic-ref", "--short", "refs/remotes/origin/HEAD"]) {
        if let Some(branch) = head.strip_prefix("origin/") {
            if !branch.is_empty() {
                return Some(branch.to_string());
            }
        }
    }

    git_line(&["rev-parse", "--abbrev-ref", "HEAD"]).filter(|branch| branch != "HEAD")
}

/// Run a git command in a directory, returning an error with stderr if it fails.
fn run_git(args: &[&str], dir: &Path) -> Result<(), CairnError> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CairnError::Internal(format!(
            "`git {}` failed: {}",
            args.join(" "),
            stderr.trim()
        )));
    }

    Ok(())
}

pub async fn create_db(
    db: &LocalDb,
    clock: &dyn Clock,
    input: &CreateProject,
) -> Result<DbProject, CairnError> {
    let now = clock.now() as i32;
    let id = input
        .id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let remote_url = input.remote_url.clone();
    let server_id = input.server_id.clone();
    let name = input.name.clone();
    let key = input.key.clone();
    let repo_path = input.repo_path.clone();

    db.write(|conn| {
        let id = id.clone();
        let remote_url = remote_url.clone();
        let server_id = server_id.clone();
        let name = name.clone();
        let key = key.clone();
        let repo_path = repo_path.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO projects(
                    id, workspace_id, name, key, repo_path, context, docs_enabled,
                    default_branch, next_issue_number, created_at, updated_at,
                    remote_url, server_id, is_workspace
                 )
                 VALUES (?1, 'default', ?2, ?3, ?4, '', 1, 'main', 1, ?5, ?6, ?7, ?8, 0)",
                (
                    id.as_str(),
                    name.as_str(),
                    key.as_str(),
                    repo_path.as_str(),
                    now,
                    now,
                    remote_url.as_deref(),
                    server_id.as_deref(),
                ),
            )
            .await?;
            Ok(())
        })
    })
    .await?;

    get_db(db, &id).await?.ok_or_else(|| CairnError::NotFound {
        entity: "project",
        id,
    })
}

pub async fn get_db(db: &LocalDb, id: &str) -> Result<Option<DbProject>, CairnError> {
    let id = id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, workspace_id, name, key, repo_path, context, docs_enabled,
                            default_branch, next_issue_number, created_at, updated_at,
                            ci_commands, setup_commands, terminal_commands, config,
                            remote_url, hidden, server_id, is_workspace
                     FROM projects
                     WHERE id = ?1",
                    (id,),
                )
                .await?;
            rows.next()
                .await?
                .map(|row| db_project_from_row(&row))
                .transpose()
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn list_db(db: &LocalDb) -> Result<Vec<DbProject>, CairnError> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, workspace_id, name, key, repo_path, context, docs_enabled,
                            default_branch, next_issue_number, created_at, updated_at,
                            ci_commands, setup_commands, terminal_commands, config,
                            remote_url, hidden, server_id, is_workspace
                     FROM projects
                     ORDER BY name ASC",
                    (),
                )
                .await?;
            let mut projects = Vec::new();
            while let Some(row) = rows.next().await? {
                projects.push(db_project_from_row(&row)?);
            }
            Ok(projects)
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn seed_workspace_project_db(
    db: &LocalDb,
    clock: &dyn Clock,
    repo_path: &Path,
) -> Result<(), CairnError> {
    let now = clock.now() as i32;
    let repo_path = repo_path.to_string_lossy().to_string();
    db.write(|conn| {
        let repo_path = repo_path.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR IGNORE INTO projects(
                    id, workspace_id, name, key, repo_path, context, docs_enabled,
                    default_branch, next_issue_number, created_at, updated_at,
                    remote_url, hidden, server_id, is_workspace
                 )
                 VALUES ('workspace', 'default', 'Workspace', 'WS', ?1, '', 1,
                         'main', 1, ?2, ?3, NULL, 0, NULL, 1)",
                (repo_path.as_str(), now, now),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    crate::memories::db::backfill_workspace_project_id(db).await?;
    Ok(())
}

pub async fn unhide_workspace_project_db(db: &LocalDb) -> Result<(), CairnError> {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute("UPDATE projects SET hidden = 0 WHERE is_workspace = 1", ())
                .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn update_timestamp_db(
    db: &LocalDb,
    clock: &dyn Clock,
    id: &str,
) -> Result<(), CairnError> {
    let now = clock.now() as i32;
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET updated_at = ?1 WHERE id = ?2",
                (now, id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn set_default_branch_db(db: &LocalDb, id: &str, branch: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let branch = branch.to_string();
    db.write(|conn| {
        let id = id.clone();
        let branch = branch.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET default_branch = ?1 WHERE id = ?2",
                (branch.as_str(), id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn set_name_db(db: &LocalDb, id: &str, name: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let name = name.to_string();
    db.write(|conn| {
        let id = id.clone();
        let name = name.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET name = ?1 WHERE id = ?2",
                (name.as_str(), id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn set_hidden_db(db: &LocalDb, id: &str, hidden: bool) -> Result<(), CairnError> {
    let id = id.to_string();
    let hidden = if hidden { 1 } else { 0 };
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE projects SET hidden = ?1 WHERE id = ?2",
                (hidden, id.as_str()),
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn delete_db(db: &LocalDb, id: &str) -> Result<(), CairnError> {
    if get_db(db, id).await?.is_none() {
        return Err(CairnError::NotFound {
            entity: "project",
            id: id.to_string(),
        });
    }

    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute("DELETE FROM projects WHERE id = ?1", (id,))
                .await?;
            Ok(())
        })
    })
    .await?;
    Ok(())
}

pub async fn repo_path(db: &LocalDb, id: &str) -> Result<Option<String>, CairnError> {
    let id = id.to_string();
    db.query_text("SELECT repo_path FROM projects WHERE id = ?1", (id,))
        .await
        .map_err(CairnError::from)
}

pub async fn worktree_paths(db: &LocalDb, project_id: &str) -> Result<Vec<String>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT j.worktree_path
                     FROM jobs j
                     INNER JOIN issues i ON j.issue_id = i.id
                     WHERE i.project_id = ?1
                       AND j.worktree_path IS NOT NULL",
                    (project_id.as_str(),),
                )
                .await?;
            let mut paths = Vec::new();
            while let Some(row) = rows.next().await? {
                paths.push(row.text(0)?);
            }
            Ok(paths)
        })
    })
    .await
    .map_err(CairnError::from)
}

fn db_project_from_row(row: &turso::Row) -> Result<DbProject, DbError> {
    Ok(DbProject {
        id: row.text(0)?,
        workspace_id: row.text(1)?,
        name: row.text(2)?,
        key: row.text(3)?,
        repo_path: row.text(4)?,
        context: row.opt_text(5)?,
        docs_enabled: row.opt_i64(6)?.map(|value| value as i32),
        default_branch: row.opt_text(7)?,
        next_issue_number: row.opt_i64(8)?.map(|value| value as i32),
        created_at: row.i64(9)? as i32,
        updated_at: row.i64(10)? as i32,
        ci_commands: row.opt_text(11)?,
        setup_commands: row.opt_text(12)?,
        terminal_commands: row.opt_text(13)?,
        config: row.opt_text(14)?,
        remote_url: row.opt_text(15)?,
        hidden: row.i64(16)? as i32,
        server_id: row.opt_text(17)?,
        is_workspace: row.i64(18)? as i32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::Clock;
    use crate::storage::{LocalDb, MigrationRunner, RowExt, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    struct FixedClock;
    impl Clock for FixedClock {
        fn now(&self) -> i64 {
            1234
        }
        fn now_u64(&self) -> u64 {
            1234
        }
    }

    async fn migrated_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.keep().join("cairn.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn create_existing_empty_git_repo_creates_initial_commit() {
        let db = migrated_db().await;
        let repo = tempdir().unwrap();
        run_git(&["init"], repo.path()).unwrap();

        create(
            &db,
            &FixedClock,
            CreateProject {
                id: Some("empty-repo".to_string()),
                name: "Empty Repo".to_string(),
                key: "ER".to_string(),
                repo_path: repo.path().to_string_lossy().to_string(),
                remote_url: None,
                server_id: None,
            },
            None,
        )
        .await
        .unwrap();

        let output = std::process::Command::new("git")
            .args(["rev-parse", "--verify", "HEAD"])
            .current_dir(repo.path())
            .output()
            .unwrap();
        assert!(output.status.success());

        let status = std::process::Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(repo.path())
            .output()
            .unwrap();
        assert!(String::from_utf8_lossy(&status.stdout).trim().is_empty());
    }

    #[tokio::test]
    async fn seed_workspace_project_is_visible_and_idempotent() {
        let db = migrated_db().await;
        seed_workspace_project_db(&db, &FixedClock, Path::new("/tmp/cairn-home"))
            .await
            .unwrap();
        seed_workspace_project_db(&db, &FixedClock, Path::new("/tmp/other"))
            .await
            .unwrap();

        let (count, repo_path, hidden, is_workspace, default_branch) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT COUNT(*), repo_path, hidden, is_workspace, default_branch FROM projects WHERE id = 'workspace'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.expect("workspace row");
                    Ok((row.i64(0)?, row.text(1)?, row.i64(2)?, row.i64(3)?, row.text(4)?))
                })
            })
            .await
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(repo_path, "/tmp/cairn-home");
        assert_eq!(hidden, 0);
        assert_eq!(is_workspace, 1);
        assert_eq!(default_branch, "main");
    }

    #[tokio::test]
    async fn unhide_workspace_project_backfills_existing_hidden_rows() {
        let db = migrated_db().await;
        seed_workspace_project_db(&db, &FixedClock, Path::new("/tmp/cairn-home"))
            .await
            .unwrap();
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("UPDATE projects SET hidden = 1 WHERE id = 'workspace'", ())
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        unhide_workspace_project_db(&db).await.unwrap();
        unhide_workspace_project_db(&db).await.unwrap();

        let hidden = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT hidden FROM projects WHERE id = 'workspace'", ())
                        .await?;
                    let row = rows.next().await?.expect("workspace row");
                    row.i64(0)
                })
            })
            .await
            .unwrap();

        assert_eq!(hidden, 0);
    }
}
