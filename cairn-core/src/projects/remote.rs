use crate::db_records::DbProject;
use crate::error::CairnError;
use crate::github::api::parse_repo_from_url;
use crate::models::ProjectRemoteStatus;
use crate::pr_data::helpers::{
    assert_main_checkout_clean_for_default_merge, reconcile_main_checkout_after_merge,
};
use crate::projects::crud;
use crate::services::GitClient;
use crate::storage::LocalDb;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushRemoteSyncOutcome {
    IgnoredNonDefaultRef,
    NoMatchingProject,
    /// A matching default-branch push was processed: the project resolved and its
    /// default branch is known. The main-checkout pull is attempted as a
    /// best-effort side effect (a failure is logged, not surfaced here), so this
    /// variant signals "reconcile in-flight siblings for `default_branch`"
    /// regardless of whether the user's checkout could be fast-forwarded.
    Pulled {
        project_id: String,
        default_branch: String,
    },
}

pub fn attach_remote(
    git: &dyn GitClient,
    repo_path: &Path,
    default_branch: &str,
    remote_url: &str,
) -> Result<(), CairnError> {
    if !git.is_repo(repo_path).map_err(CairnError::Internal)? {
        return Err(CairnError::Internal(format!(
            "{} is not a git repository",
            repo_path.display()
        )));
    }

    git.set_remote(repo_path, "origin", remote_url)
        .map_err(|e| CairnError::Internal(format!("Failed to attach origin remote: {e}")))?;
    git.push_branch(repo_path, default_branch).map_err(|e| {
        CairnError::Internal(format!("Failed to push {default_branch} to origin: {e}"))
    })?;

    Ok(())
}

pub fn remote_status(
    git: &dyn GitClient,
    repo_path: &Path,
    is_workspace: bool,
) -> ProjectRemoteStatus {
    let remote_url = git
        .remote_get_url(repo_path)
        .ok()
        .filter(|url| !url.trim().is_empty());

    ProjectRemoteStatus {
        has_remote: remote_url.is_some(),
        remote_url,
        is_workspace,
    }
}

pub async fn find_project_by_remote_full_name(
    db: &LocalDb,
    git: &dyn GitClient,
    full_name: &str,
) -> Result<Option<DbProject>, CairnError> {
    let target = normalize_full_name(full_name)?;

    for project in crud::list_db(db).await? {
        if project.remote_url.is_some()
            || project.server_id.is_some()
            || project.repo_path.is_empty()
        {
            continue;
        }

        let repo_path = Path::new(&project.repo_path);
        let Ok(remote_url) = git.remote_get_url(repo_path) else {
            continue;
        };
        let Ok(remote_full_name) = normalize_full_name_from_url(&remote_url) else {
            continue;
        };

        if remote_full_name.eq_ignore_ascii_case(&target) {
            return Ok(Some(project));
        }
    }

    Ok(None)
}

pub async fn pull_project_on_default_branch_push(
    db: &LocalDb,
    git: &dyn GitClient,
    repo_full_name: &str,
    git_ref: &str,
    event_default_branch: &str,
) -> Result<PushRemoteSyncOutcome, CairnError> {
    if !is_default_branch_push(git_ref, event_default_branch) {
        return Ok(PushRemoteSyncOutcome::IgnoredNonDefaultRef);
    }

    let Some(project) = find_project_by_remote_full_name(db, git, repo_full_name).await? else {
        return Ok(PushRemoteSyncOutcome::NoMatchingProject);
    };

    let stored_default = project
        .default_branch
        .as_deref()
        .unwrap_or(event_default_branch);
    let config =
        crate::config::project_settings::load_project_settings(Path::new(&project.repo_path));
    let default_branch =
        crate::config::project_settings::resolve_default_branch(&config, Some(stored_default));

    // The main-checkout update is best-effort: webhook processing cannot refuse an
    // external push that already happened. Still run the same dirty-checkout gate
    // used by in-app merges before any checkout mutation. Non-allowlisted tracked
    // dirt skips only the user's main-checkout update; the caller still receives
    // `Pulled` so it can fetch origin into the shared jj store and reconcile
    // in-flight agent workspaces independently of the user's checkout state.
    match assert_main_checkout_clean_for_default_merge(git, &project.repo_path) {
        Ok(()) => {
            if let Err(error) =
                reconcile_main_checkout_after_merge(git, &project.repo_path, &default_branch, true)
            {
                log::warn!(
                    "Default-branch push for project {}: main checkout reconcile failed (continuing to sibling reconcile): {}",
                    project.id, error
                );
            }
        }
        Err(error) => {
            log::warn!(
                "Default-branch push for project {}: skipping main checkout reconcile (continuing to sibling reconcile): {}",
                project.id, error
            );
        }
    }

    Ok(PushRemoteSyncOutcome::Pulled {
        project_id: project.id,
        default_branch,
    })
}

pub fn is_default_branch_push(git_ref: &str, default_branch: &str) -> bool {
    git_ref == format!("refs/heads/{default_branch}")
}

fn normalize_full_name(full_name: &str) -> Result<String, CairnError> {
    let trimmed = full_name.trim().trim_end_matches(".git");
    let parts: Vec<&str> = trimmed.split('/').collect();
    if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
        Ok(format!("{}/{}", parts[0], parts[1]))
    } else {
        Err(CairnError::Internal(format!(
            "Invalid GitHub repository full name: {full_name}"
        )))
    }
}

fn normalize_full_name_from_url(url: &str) -> Result<String, CairnError> {
    let (owner, repo) = parse_repo_from_url(url).map_err(CairnError::Internal)?;
    normalize_full_name(&format!("{owner}/{repo}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CreateProject;
    use crate::services::testing::MockGitClient;
    use crate::services::Clock;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use mockall::predicate::{eq, function};
    use std::path::PathBuf;
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

    async fn insert_project(
        db: &LocalDb,
        id: &str,
        repo_path: &str,
        remote_url: Option<String>,
        server_id: Option<String>,
    ) {
        crud::create_db(
            db,
            &FixedClock,
            &CreateProject {
                id: Some(id.to_string()),
                name: id.to_string(),
                key: id.chars().take(8).collect::<String>().to_uppercase(),
                repo_path: repo_path.to_string(),
                remote_url,
                server_id,
            },
        )
        .await
        .unwrap();
    }

    #[test]
    fn attach_remote_sets_origin_then_pushes_default_branch() {
        let repo = PathBuf::from("/repo");
        let mut git = MockGitClient::new();
        git.expect_is_repo()
            .with(eq(repo.clone()))
            .returning(|_| Ok(true));
        git.expect_set_remote()
            .with(
                eq(repo.clone()),
                eq("origin"),
                eq("https://github.com/acme/ws.git"),
            )
            .returning(|_, _, _| Ok(()));
        git.expect_push_branch()
            .with(eq(repo.clone()), eq("main"))
            .returning(|_, _| Ok(()));

        attach_remote(&git, &repo, "main", "https://github.com/acme/ws.git").unwrap();
    }

    #[test]
    fn attach_remote_propagates_push_failure() {
        let repo = PathBuf::from("/repo");
        let mut git = MockGitClient::new();
        git.expect_is_repo().returning(|_| Ok(true));
        git.expect_set_remote().returning(|_, _, _| Ok(()));
        git.expect_push_branch()
            .returning(|_, _| Err("non-fast-forward".to_string()));

        let err = attach_remote(&git, &repo, "main", "https://github.com/acme/ws.git")
            .unwrap_err()
            .to_string();
        assert!(err.contains("non-fast-forward"));
    }

    #[test]
    fn remote_status_uses_live_git_config() {
        let repo = PathBuf::from("/repo");
        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(eq(repo.clone()))
            .returning(|_| Ok("git@github.com:acme/workspace.git".to_string()));

        let status = remote_status(&git, &repo, true);

        assert!(status.has_remote);
        assert_eq!(
            status.remote_url.as_deref(),
            Some("git@github.com:acme/workspace.git")
        );
        assert!(status.is_workspace);
    }

    #[tokio::test]
    async fn find_project_by_remote_full_name_matches_local_origin_and_skips_bookmarks() {
        let db = migrated_db().await;
        insert_project(&db, "local", "/repos/local", None, None).await;
        insert_project(
            &db,
            "bookmark",
            "/repos/bookmark",
            Some("https://server.example".to_string()),
            None,
        )
        .await;

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("git@github.com:Acme/workspace.git".to_string()));

        let project = find_project_by_remote_full_name(&db, &git, "acme/workspace")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(project.id, "local");
    }

    #[tokio::test]
    async fn find_project_by_remote_full_name_handles_https_and_owner_repo_forms() {
        let db = migrated_db().await;
        insert_project(&db, "https", "/repos/https", None, None).await;
        insert_project(&db, "path", "/repos/path", None, None).await;

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/https")))
            .returning(|_| Ok("https://github.com/acme/workspace.git".to_string()));
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/path")))
            .returning(|_| Ok("other/repo".to_string()));

        let project = find_project_by_remote_full_name(&db, &git, "ACME/workspace")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(project.id, "https");
    }

    #[tokio::test]
    async fn push_sync_ignores_non_default_ref_without_git_lookup() {
        let db = migrated_db().await;
        let git = MockGitClient::new();

        let outcome = pull_project_on_default_branch_push(
            &db,
            &git,
            "acme/workspace",
            "refs/heads/feature",
            "main",
        )
        .await
        .unwrap();

        assert_eq!(outcome, PushRemoteSyncOutcome::IgnoredNonDefaultRef);
    }

    #[tokio::test]
    async fn push_sync_pulls_matching_default_branch_project() {
        let db = migrated_db().await;
        insert_project(&db, "local", "/repos/local", None, None).await;

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("https://github.com/acme/workspace.git".to_string()));
        git.expect_current_branch()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("main".to_string()));
        git.expect_status()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok(String::new()));
        git.expect_pull()
            .with(
                function(|path: &Path| path == Path::new("/repos/local")),
                eq("origin"),
                eq("main"),
            )
            .returning(|_, _, _| Ok(()));

        let outcome = pull_project_on_default_branch_push(
            &db,
            &git,
            "acme/workspace",
            "refs/heads/main",
            "main",
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            PushRemoteSyncOutcome::Pulled {
                project_id: "local".to_string(),
                default_branch: "main".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn push_sync_returns_pulled_even_when_checkout_pull_fails() {
        // A clean-but-diverged main checkout can fail the best-effort pull, but
        // the externally-advanced tip is still on origin and the sibling
        // reconcile fetches it into the shared store itself. The outcome must
        // still be `Pulled` so the caller reconciles in-flight workspaces.
        let db = migrated_db().await;
        insert_project(&db, "local", "/repos/local", None, None).await;

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("https://github.com/acme/workspace.git".to_string()));
        git.expect_current_branch()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("main".to_string()));
        git.expect_status()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok(String::new()));
        git.expect_pull()
            .with(
                function(|path: &Path| path == Path::new("/repos/local")),
                eq("origin"),
                eq("main"),
            )
            .returning(|_, _, _| Err("local changes would be overwritten by merge".to_string()));

        let outcome = pull_project_on_default_branch_push(
            &db,
            &git,
            "acme/workspace",
            "refs/heads/main",
            "main",
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            PushRemoteSyncOutcome::Pulled {
                project_id: "local".to_string(),
                default_branch: "main".to_string(),
            },
            "a failed main-checkout pull must not suppress the sibling reconcile signal"
        );
    }

    #[tokio::test]
    async fn push_sync_skips_main_checkout_reconcile_when_dirty_but_still_signals_pulled() {
        let db = migrated_db().await;
        insert_project(&db, "local", "/repos/local", None, None).await;

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("https://github.com/acme/workspace.git".to_string()));
        git.expect_status()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok(" M src/lib.rs".to_string()));
        git.expect_current_branch().never();
        git.expect_pull().never();
        git.expect_run().never();

        let outcome = pull_project_on_default_branch_push(
            &db,
            &git,
            "acme/workspace",
            "refs/heads/main",
            "main",
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            PushRemoteSyncOutcome::Pulled {
                project_id: "local".to_string(),
                default_branch: "main".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn push_sync_uses_resolved_default_branch_for_checkout_reconcile() {
        let db = migrated_db().await;
        insert_project(&db, "local", "/repos/local", None, None).await;
        crud::set_default_branch_db(&db, "local", "develop")
            .await
            .unwrap();

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("https://github.com/acme/workspace.git".to_string()));
        git.expect_status()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok(String::new()));
        git.expect_current_branch()
            .with(function(|path: &Path| path == Path::new("/repos/local")))
            .returning(|_| Ok("develop".to_string()));
        git.expect_pull()
            .with(
                function(|path: &Path| path == Path::new("/repos/local")),
                eq("origin"),
                eq("develop"),
            )
            .returning(|_, _, _| Ok(()));

        let outcome = pull_project_on_default_branch_push(
            &db,
            &git,
            "acme/workspace",
            "refs/heads/main",
            "main",
        )
        .await
        .unwrap();

        assert_eq!(
            outcome,
            PushRemoteSyncOutcome::Pulled {
                project_id: "local".to_string(),
                default_branch: "develop".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn push_sync_ignores_unmatched_repo() {
        let db = migrated_db().await;
        insert_project(&db, "local", "/repos/local", None, None).await;

        let mut git = MockGitClient::new();
        git.expect_remote_get_url()
            .returning(|_| Ok("https://github.com/acme/other.git".to_string()));

        let outcome = pull_project_on_default_branch_push(
            &db,
            &git,
            "acme/workspace",
            "refs/heads/main",
            "main",
        )
        .await
        .unwrap();

        assert_eq!(outcome, PushRemoteSyncOutcome::NoMatchingProject);
    }
}
