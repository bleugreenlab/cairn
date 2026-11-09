use super::*;

use std::path::PathBuf;
use std::sync::Arc;

use crate::config::project_settings::{get_project_config_path, load_project_settings_read_only};
use crate::db::DbState;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::services::testing::TestServicesBuilder;
use crate::storage::{RowExt, SearchIndex};

struct Fixture {
    orch: Orchestrator,
    repo: PathBuf,
    _root: tempfile::TempDir,
}

impl Fixture {
    fn store(&self) -> PathBuf {
        crate::jj::project_store_dir(&self.orch.config_dir, &self.repo)
    }

    async fn stored_default(&self) -> Option<String> {
        crate::projects::crud::get_db(&self.orch.db.local, "p")
            .await
            .unwrap()
            .unwrap()
            .default_branch
    }

    fn declared_default(&self) -> Option<String> {
        load_project_settings_read_only(&self.repo).default_branch
    }
}

fn git(args: &[&str], dir: &Path) {
    let status = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?} failed");
}

/// A project `p` (key `demo`) whose checkout is a fresh git repository and
/// whose stored default is `main`.
async fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let repo = root.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&["init", "-q", "-b", "main"], &repo);
    git(&["config", "user.email", "t@e.com"], &repo);
    git(&["config", "user.name", "T"], &repo);
    let config_dir = root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let db = crate::storage::LocalDb::open_migrated_for_test(root.path().join("cairn.db"))
        .await
        .unwrap();
    let repo_path = repo.to_string_lossy().into_owned();
    db.execute(
        "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w', 'W', 1, 1)",
        (),
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
         VALUES ('p', 'w', 'Demo', 'demo', ?1, 'main', 1, 1)",
        (repo_path.as_str(),),
    )
    .await
    .unwrap();
    let search = Arc::new(SearchIndex::open_or_create(root.path().join("search")).unwrap());
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(Arc::new(db), search)),
        Arc::new(TestServicesBuilder::new().build()),
        config_dir,
    )
    .build();
    Fixture {
        orch,
        repo,
        _root: root,
    }
}

fn declare(branch: &str) -> Option<DefaultBranchChange<'_>> {
    Some(DefaultBranchChange {
        branch,
        config: ConfigDefault::Declare,
    })
}

async fn change_default(
    fixture: &Fixture,
    change: Option<DefaultBranchChange<'_>>,
    held: Option<&JjStoreGuard>,
    timeout: Duration,
) -> Result<(), String> {
    write_project_settings_within(
        &fixture.orch,
        &fixture.orch.db.local,
        "p",
        &fixture.repo,
        change,
        held,
        timeout,
        |config| {
            config.setup_commands = Some(vec!["echo changed".to_string()]);
            Ok(())
        },
    )
    .await
}

/// A default change that cannot get the store guard refuses before anything
/// is written: not the row, not the config file, not the settings that rode
/// along with it.
#[tokio::test]
async fn busy_store_refuses_a_default_change_before_any_write() {
    let fixture = fixture().await;
    let _preparation = fixture
        .orch
        .hold_jj_store_lock_on_another_task(
            &fixture.store(),
            StoreLockClass::Foreground,
            "test base preparation",
        )
        .await;

    let error = change_default(
        &fixture,
        declare("develop"),
        None,
        Duration::from_millis(50),
    )
    .await
    .unwrap_err();

    assert!(error.contains("busy"), "{error}");
    assert_eq!(fixture.stored_default().await.as_deref(), Some("main"));
    assert!(!get_project_config_path(&fixture.repo).exists());
}

/// A base preparation that holds the guard reads one default for its whole
/// critical section: a concurrent default change waits for the guard and lands
/// only after the preparation releases it.
#[tokio::test]
async fn default_change_waits_for_a_base_preparation_holding_the_guard() {
    let fixture = Arc::new(fixture().await);
    let preparation_guard = fixture
        .orch
        .acquire_jj_store_lock(
            &fixture.store(),
            StoreLockClass::Foreground,
            "test base preparation",
        )
        .await;
    let validated = fixture.stored_default().await;

    let writer = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            change_default(&fixture, declare("develop"), None, Duration::from_secs(30)).await
        })
    };
    // Give the writer every chance to run ahead of the preparation.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!writer.is_finished(), "the writer must wait for the guard");
    let installed = fixture.stored_default().await;
    assert_eq!(validated, installed);
    assert_eq!(installed.as_deref(), Some("main"));
    assert_eq!(fixture.declared_default(), None);
    drop(preparation_guard);

    tokio::time::timeout(Duration::from_secs(10), writer)
        .await
        .expect("the writer proceeds once the guard is released")
        .unwrap()
        .unwrap();
    assert_eq!(fixture.stored_default().await.as_deref(), Some("develop"));
    assert_eq!(fixture.declared_default().as_deref(), Some("develop"));
}

/// The guard is not reentrant: a caller that already holds it (a write batch
/// with file targets) lends it, and the change completes inside that hold.
#[tokio::test]
async fn held_guard_is_lent_rather_than_reacquired() {
    let fixture = Arc::new(fixture().await);
    let task = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            let guard = fixture
                .orch
                .acquire_jj_store_lock(
                    &fixture.store(),
                    StoreLockClass::Foreground,
                    "test logical write transaction",
                )
                .await;
            change_default(
                &fixture,
                declare("develop"),
                Some(&guard),
                Duration::from_millis(50),
            )
            .await
        })
    };
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("a lent guard never waits on itself")
        .unwrap()
        .unwrap();
    assert_eq!(fixture.stored_default().await.as_deref(), Some("develop"));
}

/// A held guard on another store does not stand in for this project's guard.
#[tokio::test]
async fn guard_on_another_store_does_not_cover_the_project() {
    let fixture = fixture().await;
    let other = fixture
        .orch
        .acquire_jj_store_lock(
            Path::new("/fixture/other-store"),
            StoreLockClass::Foreground,
            "test other store",
        )
        .await;
    let _preparation = fixture
        .orch
        .hold_jj_store_lock_on_another_task(
            &fixture.store(),
            StoreLockClass::Foreground,
            "test base preparation",
        )
        .await;

    let error = change_default(
        &fixture,
        declare("develop"),
        Some(&other),
        Duration::from_millis(50),
    )
    .await
    .unwrap_err();
    assert!(error.contains("busy"), "{error}");
    assert_eq!(fixture.stored_default().await.as_deref(), Some("main"));
}

/// The desktop settings path changes the stored default and rewrites a
/// declaration only where one exists, since a declared value wins.
#[tokio::test]
async fn follow_if_declared_rewrites_only_an_existing_declaration() {
    let fixture = fixture().await;
    let follow = |branch| {
        Some(DefaultBranchChange {
            branch,
            config: ConfigDefault::FollowIfDeclared,
        })
    };

    change_default(&fixture, follow("develop"), None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(fixture.stored_default().await.as_deref(), Some("develop"));
    assert_eq!(fixture.declared_default(), None);

    change_default(&fixture, declare("staging"), None, Duration::from_secs(5))
        .await
        .unwrap();
    change_default(&fixture, follow("release"), None, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(fixture.stored_default().await.as_deref(), Some("release"));
    assert_eq!(fixture.declared_default().as_deref(), Some("release"));
}

/// The mixed-batch shape: a write batch holding its store guard dispatches a
/// `settings` patch that changes the default of the same project. The item
/// runs under the lent guard instead of queueing behind the batch's own hold.
#[tokio::test]
async fn settings_patch_runs_under_the_write_batch_guard() {
    let fixture = Arc::new(fixture().await);
    let task = {
        let fixture = fixture.clone();
        tokio::spawn(async move {
            let guard = fixture
                .orch
                .acquire_jj_store_lock(
                    &fixture.store(),
                    StoreLockClass::Foreground,
                    "test logical write transaction",
                )
                .await;
            let item = ChangeItem {
                target: "cairn://p/demo/settings".to_string(),
                mode: ChangeMode::Patch,
                payload: Some(serde_json::json!({"defaultBranch": "develop"})),
            };
            let request = McpCallbackRequest {
                answer_bound_ms: None,
                thread_id: None,
                cwd: "/tmp".to_string(),
                run_id: None,
                tool: "change".to_string(),
                payload: serde_json::json!({}),
                tool_use_id: None,
                origin: None,
            };
            crate::resources::mutations::dispatch_resource_change_under_store(
                &fixture.orch,
                &request,
                0,
                &item,
                false,
                Some(&guard),
            )
            .await
            .map(|change| change.summary)
            .map_err(|failure| failure.error)
        })
    };
    let summary = tokio::time::timeout(Duration::from_secs(20), task)
        .await
        .expect("the batch's own guard must not deadlock the settings item")
        .unwrap()
        .unwrap();
    assert!(summary.contains("defaultBranch"), "{summary}");
    assert_eq!(fixture.stored_default().await.as_deref(), Some("develop"));
    assert_eq!(fixture.declared_default().as_deref(), Some("develop"));
}

/// A `cairn://projects` create whose explicit default contradicts the
/// repository's declared one is refused before anything is written: no route,
/// no row, and the config file is left as it was.
#[tokio::test]
async fn project_create_refuses_a_default_contradicting_the_declared_one() {
    let fixture = fixture().await;
    let other = fixture.repo.parent().unwrap().join("other");
    std::fs::create_dir_all(other.join(".cairn")).unwrap();
    git(&["init", "-q", "-b", "main"], &other);
    let config = get_project_config_path(&other);
    std::fs::write(&config, "defaultBranch: develop\n").unwrap();
    let item = ChangeItem {
        target: "cairn://projects".to_string(),
        mode: ChangeMode::Create,
        payload: Some(serde_json::json!({
            "key": "other",
            "name": "Other",
            "repoPath": other.to_string_lossy(),
            "defaultBranch": "release",
        })),
    };
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "change".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
        origin: None,
    };

    let error = crate::resources::mutations::dispatch_resource_change_under_store(
        &fixture.orch,
        &request,
        0,
        &item,
        false,
        None,
    )
    .await
    .map(|change| change.summary)
    .unwrap_err()
    .error;

    assert!(error.contains(&config.display().to_string()), "{error}");
    assert!(
        error.contains("`develop`") && error.contains("`release`"),
        "{error}"
    );
    let rows = fixture
        .orch
        .db
        .local
        .query_opt(
            "SELECT (SELECT COUNT(*) FROM projects WHERE key = 'other')
                  + (SELECT COUNT(*) FROM project_routes WHERE project_key = 'other')",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(rows, Some(0));
    assert_eq!(
        std::fs::read_to_string(&config).unwrap(),
        "defaultBranch: develop\n"
    );
}
