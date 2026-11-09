//! The managed publisher against a real store and a real origin: what it may
//! publish sideways, what it refuses, and what an accepted or unknown push
//! leaves behind.

use super::*;
use crate::db::DbState;
use crate::jj::tests::{git, git_stdout, init_project, jj_bin};
use crate::services::testing::TestServicesBuilder;
use crate::storage::SearchIndex;
use std::path::PathBuf;
use std::time::Duration;

const BRANCH: &str = "agent/proj-1-builder-1";

struct Fixture {
    db: Arc<LocalDb>,
    root: PathBuf,
    bin: String,
    orch: Orchestrator,
    jj: crate::jj::JjEnv,
    store: PathBuf,
    project: PathBuf,
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
            root,
            bin,
            orch,
            jj,
            store,
            project: project.path().to_path_buf(),
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

    /// A new process over the same database and store: nothing the publisher
    /// knows survives except what the journal and the store hold.
    fn restart(&mut self) {
        self.orch = Self::orchestrator(&self.db, &self.root, self.bin.clone(), "search-restarted");
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

    fn run(&self, args: &[&str]) {
        self.jj
            .run(&self.store, args, "test store rewrite")
            .unwrap();
    }

    /// Rewrite `commit` in place, as a replay does: it and everything above it
    /// get new ids, each recording its old id as a predecessor. The bookmark
    /// follows when it named `commit` or a descendant.
    fn replay(&self, commit: &str, message: &str) -> String {
        self.run(&[
            "describe",
            commit,
            "-m",
            message,
            "--ignore-working-copy",
            "--ignore-immutable",
        ]);
        self.export_tip()
    }

    /// A replay that absorbed the branch's tip: `first` is rewritten, and the
    /// bookmark settles on its rewrite, dropping the commit above it. The
    /// pre-replay tip, which origin still holds, stays VISIBLE: a dormant
    /// descendant work was built on keeps it alive (modelled by a bookmark on
    /// it). A rewrite alone would hide it, and the supersession proof counts a
    /// commit the store hid as its own; a visible one needs the accepted
    /// install's drop claim. Answers the new head.
    fn replay_dropping_tip(&self, first: &str) -> String {
        let absorbed = self.tip();
        self.replay(first, "replayed first");
        self.run(&[
            "bookmark",
            "create",
            "dormant/descendant",
            "-r",
            &absorbed,
            "--ignore-working-copy",
        ]);
        let replayed =
            crate::jj::revset_commit(&self.jj, &self.store, &format!("{}-", self.tip())).unwrap();
        self.run(&[
            "bookmark",
            "set",
            BRANCH,
            "-r",
            &replayed,
            "--allow-backwards",
            "--ignore-working-copy",
        ]);
        self.export_tip()
    }

    /// An accepted install of the branch in the journal, as a preparation that
    /// was accepted leaves it.
    async fn accept(
        &self,
        id: &str,
        head_before: &str,
        head_after: &str,
        dropped: &[&str],
        inherited: &[&str],
        created_at: i64,
    ) {
        let store_key = preparation::journal_store_key(&self.store).unwrap();
        let base = crate::jj::bookmark_commit(&self.jj, &self.store, "main").unwrap();
        self.db
            .execute(
                "INSERT INTO branch_advance_acceptances
                   (id, project_id, store_path, branch, head_before, head_after, base_branch,
                    base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
                    status, created_at, settled_at)
                 VALUES (?1, 'proj-1', ?2, ?3, ?4, ?5, 'main', ?6, 'op', ?7, ?8, 'accepted', ?9, ?9)",
                params![
                    id,
                    store_key.as_str(),
                    BRANCH,
                    head_before,
                    head_after,
                    base.as_str(),
                    serde_json::to_string(dropped).unwrap(),
                    serde_json::to_string(inherited).unwrap(),
                    created_at
                ],
            )
            .await
            .unwrap();
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

    /// The store's recorded copy of origin's `BRANCH`.
    fn recorded_origin(&self) -> Option<String> {
        crate::jj::revset_commit(
            &self.jj,
            &self.store,
            &format!("remote_bookmarks(exact:{BRANCH:?}, remote=exact:\"origin\")"),
        )
    }

    /// Push a commit onto origin's `BRANCH` from a clone Cairn knows nothing of.
    fn push_outside(&self, file: &str) -> String {
        let outside = tempfile::tempdir().unwrap();
        let clone = outside.path().join("c");
        git(
            outside.path(),
            &[
                "clone",
                "-q",
                "--branch",
                BRANCH,
                &self.origin.to_string_lossy(),
                &clone.to_string_lossy(),
            ],
        );
        git(&clone, &["config", "user.email", "o@e.com"]);
        git(&clone, &["config", "user.name", "o"]);
        std::fs::write(clone.join(file), format!("{file}\n")).unwrap();
        git(&clone, &["add", "-A"]);
        git(&clone, &["commit", "-q", "-m", "pushed outside Cairn"]);
        git(
            &clone,
            &["push", "-q", "origin", &format!("HEAD:refs/heads/{BRANCH}")],
        );
        git_stdout(&clone, &["rev-parse", "HEAD"])
    }

    async fn pull_request_mergeable(&self) -> String {
        self.db
            .query_one(
                "SELECT github_mergeable FROM merge_requests WHERE id = 'mr-1'",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
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

/// A replay absorbed the published tip. Origin still holds the pre-replay
/// copy, which the replay's head does not contain, and a dormant descendant of
/// the replaced tip stays visible in the store. The accepted install's drop
/// claim proves origin loses only what this store dropped, so the publication
/// moves origin sideways, leased on exactly its old copy.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_replay_that_absorbed_the_published_tip_publishes_over_the_old_copy() {
    let f = fixture_or_skip!("publish-absorbed-tip");
    let first = f.commit("first.rs");
    let absorbed = f.commit("absorbed.rs");
    f.publish().await.unwrap();
    assert_eq!(f.origin_tip(), absorbed);

    let replayed = f.replay_dropping_tip(&first);
    f.accept("row-1", &absorbed, &replayed, &[&absorbed], &[], 10)
        .await;

    f.publish()
        .await
        .expect("a managed replay proven by its accepted drop publishes sideways");
    assert_eq!(f.origin_tip(), replayed);
    assert_eq!(f.tip(), replayed, "the store's head is what was published");
    assert_eq!(f.recorded_origin(), Some(replayed.clone()));
    let checkpoint =
        f.db.query_one(
            "SELECT published_checkpoint FROM branch_advance_acceptances WHERE id = 'row-1'",
            (),
            |row| row.opt_text(0),
        )
        .await
        .unwrap();
    assert_eq!(
        checkpoint,
        Some(replayed),
        "an exact read-back of the recorded publication checkpoints the accepted install"
    );
}

/// The same replay with no accepted install to vouch for it: origin's tip is
/// unaccounted for, so nothing is pushed and origin keeps its copy. And a branch
/// no job manages may never move origin sideways, accepted rows or not.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_rewrite_without_its_accepted_install_or_its_manager_is_refused() {
    let f = fixture_or_skip!("publish-unproven-rewrite");
    let first = f.commit("first.rs");
    let absorbed = f.commit("absorbed.rs");
    f.publish().await.unwrap();
    let replayed = f.replay_dropping_tip(&first);

    let error = f.publish().await.unwrap_err();
    assert!(error.contains("not proven superseded"), "{error}");
    assert_eq!(f.origin_tip(), absorbed, "origin keeps its copy");
    assert_eq!(f.tip(), replayed, "the store's head is untouched");

    f.accept("row-1", &absorbed, &replayed, &[&absorbed], &[], 10)
        .await;
    f.db.execute(
        "UPDATE jobs SET branch = 'agent/elsewhere' WHERE id = 'job-1'",
        (),
    )
    .await
    .unwrap();
    let error = f.publish().await.unwrap_err();
    assert!(error.contains("is not an ancestor"), "{error}");
    assert_eq!(
        f.origin_tip(),
        absorbed,
        "an unmanaged branch only fast-forwards"
    );
}

/// Two accepted replays, the second inheriting the first's drop, then a
/// restart: the publication's proof comes from the journal alone, through the
/// newest install the head descends from.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn two_accepted_replays_prove_the_rewrite_after_a_restart() {
    let mut f = fixture_or_skip!("publish-two-replays");
    let first = f.commit("first.rs");
    let absorbed = f.commit("absorbed.rs");
    f.publish().await.unwrap();

    let once = f.replay_dropping_tip(&first);
    f.accept("row-1", &absorbed, &once, &[&absorbed], &[], 10)
        .await;
    let twice = f.replay(&once, "replayed again");
    assert_ne!(twice, once);
    f.accept("row-2", &once, &twice, &[], &[&absorbed], 20)
        .await;

    f.restart();
    f.publish()
        .await
        .expect("the newest accepted install carries the inherited drop");
    assert_eq!(f.origin_tip(), twice);
    assert_eq!(f.recorded_origin(), Some(twice));
}

/// Origin moves after the store last recorded it (a push from outside Cairn).
/// The push, leased on the recorded copy, is refused by origin; the fresh copy
/// is fetched and recorded, and since the head cannot account for it the
/// publication is refused. Origin keeps the outside commit, and the store's
/// head is neither moved nor left conflicted by recording origin's copy.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_outside_push_is_never_overwritten() {
    let f = fixture_or_skip!("publish-outside-race");
    f.commit("first.rs");
    f.publish().await.unwrap();
    let local = f.commit("local.rs");
    let outside = f.push_outside("outside.rs");

    let error = f.publish().await.unwrap_err();
    assert!(error.contains("not proven superseded"), "{error}");
    assert_eq!(f.origin_tip(), outside, "origin keeps the outside commit");
    assert_eq!(f.tip(), local, "the store's head is untouched");
    assert_eq!(
        f.recorded_origin(),
        Some(outside),
        "the store now records the copy origin holds"
    );
}

/// Recording a publication writes this branch's refs alone, so another
/// operation holding an unrelated branch of the store reserved never holds it
/// up: across two publications, both are pushed and both are recorded.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unrelated_reservation_never_blocks_recording_a_publication() {
    let f = fixture_or_skip!("publish-beside-reservation");
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (orch, store) = (f.orch.clone(), f.store.clone());
    let holder = tokio::spawn(async move {
        let other = orch
            .reserve_branches(
                &store,
                &["agent/other"],
                "another publication",
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        held_tx.send(()).unwrap();
        let _ = release_rx.await;
        drop(other);
    });
    held_rx.await.unwrap();

    for file in ["first.rs", "second.rs"] {
        let tip = f.commit(file);
        f.publish().await.unwrap();
        assert_eq!(f.origin_tip(), tip);
        assert_eq!(
            f.recorded_origin(),
            Some(tip),
            "recorded while another branch is reserved"
        );
    }
    release_tx.send(()).unwrap();
    holder.await.unwrap();
}

/// Origin moved past the store's head (a push from outside Cairn on top of an
/// accepted but unrecorded publication) while another operation holds an
/// unrelated branch of the store reserved. The publication's lease is stale;
/// origin's copy is a strict fast-forward of the head, so the store takes it,
/// a write of this branch alone that the other reservation does not hold up,
/// and origin is left as it is.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_origin_fast_forward_is_taken_beside_an_unrelated_reservation() {
    let f = fixture_or_skip!("publish-fast-forward-beside-reservation");
    let first = f.commit("first.rs");
    f.publish().await.unwrap();
    let second = f.commit("second.rs");
    git(
        &f.project,
        &[
            "push",
            "-q",
            &f.origin.to_string_lossy(),
            &format!("{second}:refs/heads/{BRANCH}"),
        ],
    );
    let outside = f.push_outside("outside.rs");
    assert_eq!(f.recorded_origin(), Some(first));

    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (orch, store) = (f.orch.clone(), f.store.clone());
    let holder = tokio::spawn(async move {
        let other = orch
            .reserve_branches(
                &store,
                &["agent/other"],
                "another publication",
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        held_tx.send(()).unwrap();
        let _ = release_rx.await;
        drop(other);
    });
    held_rx.await.unwrap();

    f.publish()
        .await
        .expect("origin's fast-forward is taken while another branch is reserved");
    assert_eq!(f.tip(), outside, "the store took origin's fast-forward");
    assert_eq!(f.origin_tip(), outside, "origin is left as it is");
    assert_eq!(f.recorded_origin(), Some(outside));
    release_tx.send(()).unwrap();
    holder.await.unwrap();
}

/// Origin accepted a publication whose record never ran (the process stopped
/// between the push and the record): origin holds the head, the store still
/// records the older copy. The next publication's lease is stale, origin
/// already holding the head counts as accepted, and it is recorded.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_accepted_but_unrecorded_publication_is_recorded_by_the_next() {
    let f = fixture_or_skip!("publish-accepted-unrecorded");
    let first = f.commit("first.rs");
    f.publish().await.unwrap();
    let second = f.commit("second.rs");
    // The push to origin's URL leaves every local ref alone, as the
    // publication's own send does.
    git(
        &f.project,
        &[
            "push",
            "-q",
            &f.origin.to_string_lossy(),
            &format!("{second}:refs/heads/{BRANCH}"),
        ],
    );
    assert_eq!(f.recorded_origin(), Some(first));

    f.publish().await.unwrap();
    assert_eq!(f.origin_tip(), second);
    assert_eq!(f.tip(), second);
    assert_eq!(f.recorded_origin(), Some(second));
}

/// A push that reports no outcome is reported as unknown, never as published:
/// the store records nothing and the open pull request's cached verdict is
/// downgraded. Once origin is reachable, the next publication settles it.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_unknown_outcome_is_reported_and_settled_by_the_next_publication() {
    let f = fixture_or_skip!("publish-unknown");
    let first = f.commit("first.rs");
    f.publish().await.unwrap();
    let second = f.commit("second.rs");

    let unreachable = f.project.join("no-such-origin.git");
    git(
        &f.project,
        &[
            "remote",
            "set-url",
            "--push",
            "origin",
            &unreachable.to_string_lossy(),
        ],
    );
    let error = f.publish().await.unwrap_err();
    assert!(
        error.contains("may or may not have reached origin"),
        "{error}"
    );
    assert_eq!(f.origin_tip(), first);
    assert_eq!(f.tip(), second);
    assert_eq!(f.recorded_origin(), Some(first));
    assert_eq!(f.pull_request_mergeable().await, "UNKNOWN");

    git(
        &f.project,
        &[
            "remote",
            "set-url",
            "--push",
            "origin",
            &f.origin.to_string_lossy(),
        ],
    );
    f.publish().await.unwrap();
    assert_eq!(f.origin_tip(), second);
    assert_eq!(f.recorded_origin(), Some(second));
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
