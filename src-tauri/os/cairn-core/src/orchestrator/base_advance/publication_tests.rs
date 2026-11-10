//! The managed publisher against a real store and a real origin: what it may
//! publish sideways, what it refuses, and what an accepted or unknown push
//! leaves behind.

use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use std::path::PathBuf;

const BRANCH: &str = "agent/proj-1-builder-1";

struct Fixture {
    db: Arc<LocalDb>,
    orch: Orchestrator,
    jj: crate::jj::JjEnv,
    store: PathBuf,
    origin: PathBuf,
    workspace: PathBuf,
    _dirs: Vec<tempfile::TempDir>,
}

impl Fixture {
    /// A project whose job owns `BRANCH`, with an open pull request on it, a
    /// store, and an origin holding `main`.
    async fn new(db_name: &str) -> Option<Self> {
        let bin = jj_bin()?;
        let (home, project, origin, workspaces) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
        init_project(project.path());
        git(
            project.path(),
            &["remote", "add", "origin", &origin.path().to_string_lossy()],
        );
        git(project.path(), &["push", "-q", "origin", "main"]);

        let db = Arc::new(crate::storage::migrated_test_db(db_name).await);
        db.execute_script(format!(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
               VALUES ('proj-1', 'default', 'Project', 'proj', '{repo}', 'main', 1, 1);
             INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
             INSERT INTO jobs (id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('job-1', 'issue-1', 'proj-1', 'running', '{BRANCH}', 'main', 1, 1);
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                status, github_state, github_mergeable, github_fetched_at, opened_at, updated_at)
             VALUES ('mr-1', 'job-1', 'proj-1', 'issue-1', 'PR', '{BRANCH}', 'main',
                'open', 'OPEN', 'MERGEABLE', 100, 1, 1);",
            repo = project.path().to_string_lossy().replace('\'', "''"),
        ))
        .await
        .unwrap();

        let root = home.path().to_path_buf();
        let orch = Self::orchestrator(&db, &root, bin.clone(), "search");
        let jj = crate::jj::environment(&bin, &root);
        let store = crate::jj::project_store_dir(&root, project.path());
        crate::jj::ensure_project_store(&jj, &store, project.path()).unwrap();
        let workspace = workspaces.path().join("builder");
        crate::jj::add_workspace(&jj, &store, &workspace, BRANCH, "main", None).unwrap();
        Some(Self {
            db,
            orch,
            jj,
            store,
            origin: origin.path().to_path_buf(),
            workspace,
            _dirs: vec![home, project, origin, workspaces],
        })
    }

    fn orchestrator(db: &Arc<LocalDb>, root: &Path, bin: String, search: &str) -> Orchestrator {
        Orchestrator::builder(
            Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.join(search)).unwrap()),
            )),
            Arc::new(TestServicesBuilder::new().build()),
            root.to_path_buf(),
        )
        .jj_binary_path(bin)
        .build()
    }

    async fn publish(&self) -> Result<(), String> {
        publish_managed_branch(&self.orch, &self.store, BRANCH).await
    }

    fn tip(&self) -> String {
        crate::jj::bookmark_commit(&self.jj, &self.store, BRANCH).unwrap()
    }

    fn export_tip(&self) -> String {
        let tip = self.tip();
        crate::jj::export_git_verified(
            &self.jj,
            &self.store,
            true,
            "test commit",
            &[(BRANCH, &tip)],
        )
        .unwrap();
        tip
    }

    /// Seal a new file onto the branch, exported.
    fn commit(&self, file: &str) -> String {
        std::fs::write(self.workspace.join(file), format!("{file}\n")).unwrap();
        crate::jj::seal(&self.jj, &self.workspace, &format!("edit {file}"), None).unwrap();
        self.export_tip()
    }

    /// Origin's `BRANCH`, or empty when origin has none.
    fn origin_tip(&self) -> String {
        git_stdout(
            &self.origin,
            &[
                "for-each-ref",
                "--format=%(objectname)",
                &format!("refs/heads/{BRANCH}"),
            ],
        )
    }
}

macro_rules! fixture_or_skip {
    ($name:literal) => {
        match Fixture::new(concat!($name, ".db")).await {
            Some(fixture) => fixture,
            None => {
                eprintln!(concat!("skipping ", $name, ": jj not resolvable"));
                return;
            }
        }
    };
}

/// The publication is admitted against its project's journal: an install the
/// branch still owes refuses it before anything is sent.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owed_install_refuses_the_publication() {
    let f = fixture_or_skip!("publish-owed");
    let first = f.commit("first.rs");
    let store_key = preparation::journal_store_key(&f.store).unwrap();
    f.db.execute(
        "INSERT INTO branch_advance_acceptances
           (id, project_id, store_path, branch, head_before, head_after, base_branch,
            base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
            status, created_at)
         VALUES ('owed', 'proj-1', ?1, ?2, ?3, ?3, 'main', ?3, 'op', '[]', '[]', 'unknown', 1)",
        params![store_key.as_str(), BRANCH, first.as_str()],
    )
    .await
    .unwrap();

    let error = f.publish().await.unwrap_err();
    assert!(
        error.contains("nothing was published") && error.contains("is not settled"),
        "{error}"
    );
    assert_eq!(f.origin_tip(), "", "nothing was sent");
}
