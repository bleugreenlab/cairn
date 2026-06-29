mod common;

use cairn_core::internal::services::Clock;
use cairn_core::models::CreateProject;
use cairn_core::projects::crud;
use cairn_core::CairnError;
use turso::params;

struct FixedClock(i64);

impl Clock for FixedClock {
    fn now(&self) -> i64 {
        self.0
    }

    fn now_u64(&self) -> u64 {
        self.0 as u64
    }
}

fn create_input(id: &str, name: &str, key: &str, repo_path: &str) -> CreateProject {
    CreateProject {
        id: Some(id.to_string()),
        name: name.to_string(),
        key: key.to_string(),
        repo_path: repo_path.to_string(),
        team_id: None,
    }
}

#[tokio::test]
async fn create_db_sets_defaults_and_list_orders_by_name() {
    let (_temp, db) = common::migrated_db().await;

    let beta = crud::create_db(
        &db,
        &FixedClock(1_700_000_000),
        &create_input("project-beta", "Beta", "BETA", "/tmp/beta"),
    )
    .await
    .unwrap();
    crud::create_db(
        &db,
        &FixedClock(1_700_000_010),
        &create_input("project-alpha", "Alpha", "ALPHA", "/tmp/alpha"),
    )
    .await
    .unwrap();

    assert_eq!(beta.id, "project-beta");
    assert_eq!(beta.workspace_id, "default");
    assert_eq!(beta.context.as_deref(), Some(""));
    assert_eq!(beta.docs_enabled, Some(1));
    assert_eq!(beta.default_branch.as_deref(), Some("main"));
    assert_eq!(beta.next_issue_number, Some(1));
    assert_eq!(beta.created_at, 1_700_000_000);
    assert_eq!(beta.updated_at, 1_700_000_000);
    assert_eq!(beta.hidden, 0);

    let projects = crud::list_db(&db).await.unwrap();
    let names = projects
        .iter()
        .map(|project| project.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["Alpha", "Beta"]);
}

#[tokio::test]
async fn create_detects_and_persists_repo_default_branch() {
    let (_temp, db) = common::migrated_db().await;

    let repo = tempfile::tempdir().unwrap();
    let repo_path = repo.path();
    let run = |args: &[&str]| {
        let status = std::process::Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .status()
            .unwrap();
        assert!(status.success(), "git {:?} failed", args);
    };
    run(&["init"]);
    run(&["checkout", "-b", "develop"]);
    run(&["config", "user.email", "test@example.com"]);
    run(&["config", "user.name", "Test"]);
    run(&["commit", "--allow-empty", "-m", "init"]);

    let project = crud::create(
        &db,
        &FixedClock(1_700_000_000),
        create_input("proj-dev", "Dev", "DEV", &repo_path.to_string_lossy()),
        None,
    )
    .await
    .unwrap();

    // The returned project and the stored row both reflect the real trunk,
    // not the hardcoded "main".
    assert_eq!(project.default_branch.as_deref(), Some("develop"));
    let stored = crud::get_db(&db, "proj-dev").await.unwrap().unwrap();
    assert_eq!(stored.default_branch.as_deref(), Some("develop"));
}

#[tokio::test]
async fn update_timestamp_and_hidden_mutate_existing_project() {
    let (_temp, db) = common::migrated_db().await;
    crud::create_db(
        &db,
        &FixedClock(1_700_000_000),
        &create_input("project-1", "Project", "PROJ", "/tmp/project"),
    )
    .await
    .unwrap();

    crud::update_timestamp_db(&db, &FixedClock(1_700_000_500), "project-1")
        .await
        .unwrap();
    crud::set_hidden_db(&db, "project-1", true).await.unwrap();

    let project = crud::get_db(&db, "project-1").await.unwrap().unwrap();
    assert_eq!(project.updated_at, 1_700_000_500);
    assert_eq!(project.hidden, 1);
}

#[tokio::test]
async fn delete_db_removes_project_and_reports_missing_project() {
    let (_temp, db) = common::migrated_db().await;
    crud::create_db(
        &db,
        &FixedClock(1_700_000_000),
        &create_input("project-1", "Project", "PROJ", "/tmp/project"),
    )
    .await
    .unwrap();

    crud::delete_db(&db, "project-1").await.unwrap();
    assert!(crud::get_db(&db, "project-1").await.unwrap().is_none());

    let error = crud::delete_db(&db, "project-1").await.unwrap_err();
    assert!(matches!(
        error,
        CairnError::NotFound {
            entity: "project",
            ..
        }
    ));
}

#[tokio::test]
async fn repo_path_and_worktree_paths_are_project_scoped() {
    let (_temp, db) = common::migrated_db().await;
    crud::create_db(
        &db,
        &FixedClock(1_700_000_000),
        &create_input("project-1", "Project", "PROJ", "/tmp/project"),
    )
    .await
    .unwrap();
    crud::create_db(
        &db,
        &FixedClock(1_700_000_000),
        &create_input("project-2", "Other Project", "OTHER", "/tmp/other"),
    )
    .await
    .unwrap();

    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
                 VALUES ('issue-1', 'project-1', 1, 'Issue one', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
                 VALUES ('issue-2', 'project-2', 1, 'Issue two', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, worktree_path, created_at, updated_at)
                 VALUES (?1, 'project-1', 'issue-1', ?2, 1, 1)",
                params!["job-1", "/tmp/project/.worktrees/job-1"],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, worktree_path, created_at, updated_at)
                 VALUES (?1, 'project-1', 'issue-1', NULL, 1, 1)",
                params!["job-2"],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs(id, project_id, issue_id, worktree_path, created_at, updated_at)
                 VALUES (?1, 'project-2', 'issue-2', ?2, 1, 1)",
                params!["job-3", "/tmp/other/.worktrees/job-3"],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    assert_eq!(
        crud::repo_path(&db, "project-1").await.unwrap().as_deref(),
        Some("/tmp/project")
    );
    assert_eq!(
        crud::worktree_paths(&db, "project-1").await.unwrap(),
        vec!["/tmp/project/.worktrees/job-1".to_string()]
    );
}
