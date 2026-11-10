//! cairn/4987's acceptance corpus, end to end: the real request path against
//! a real store and database. Sessions are recorded the way lazy advance
//! records them, by the owner's commit demanding its base
//! ([`owner_commit_conflicts`]); the agent commits a linear resolution, the
//! base may advance again, and the replay must never land a tree missing what
//! the newer base added.

use super::*;
use crate::orchestrator::conflict_session::{load_active_session, ConflictSession};
use std::path::PathBuf;
use tempfile::TempDir;

const LIST: &str = "list.txt";
const MIGRATIONS: &str = "src-tauri/os/cairn-db/src/storage/migrations.rs";
const JOB: &str = "job-replay";
const BRANCH: &str = "agent/replay";
const MAIN_LINE: &str = "agent/main-line";

struct Fixture {
    _home: TempDir,
    _repo: TempDir,
    home: PathBuf,
    repo: PathBuf,
    workspaces: TempDir,
    jj: crate::jj::JjEnv,
    store: PathBuf,
}

/// A repository whose `main` declares the migration registry an ordered-set
/// merge, so replays onto it read the same landing rules production does.
fn fixture(bin: &str) -> Fixture {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    let workspaces = tempfile::tempdir().unwrap();
    let registry = repo.path().join(MIGRATIONS);
    std::fs::create_dir_all(registry.parent().unwrap()).unwrap();
    std::fs::create_dir_all(repo.path().join("src-tauri/turso_migrations")).unwrap();
    std::fs::create_dir_all(repo.path().join(".cairn")).unwrap();
    std::fs::write(
        repo.path().join(".cairn/config.yaml"),
        "landing:\n  merge:\n    - paths: [src-tauri/os/cairn-db/src/storage/migrations.rs]\n      kind: ordered-set\n",
    )
    .unwrap();
    std::fs::write(
        registry,
        "fn migrations() {\n    vec![\n        migration!(\"0227_before\"),\n        migration!(\"0229_after\"),\n    ];\n}\n",
    )
    .unwrap();
    for stem in ["0227_before", "0229_after"] {
        std::fs::write(
            repo.path()
                .join(format!("src-tauri/turso_migrations/{stem}.sql")),
            format!("-- {stem}\n"),
        )
        .unwrap();
    }
    let git = |args: &[&str]| {
        let out = crate::env::git()
            .args(args)
            .current_dir(repo.path())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    };
    git(&["init", "-q", "-b", "main"]);
    git(&["config", "user.email", "tests@cairn.local"]);
    git(&["config", "user.name", "Cairn tests"]);
    git(&["add", "-A"]);
    git(&["commit", "-q", "-m", "base"]);

    let jj = crate::jj::environment(bin, home.path());
    // Where an orchestrator configured at `home` looks for this repo's store.
    let store = crate::jj::project_store_dir(home.path(), repo.path());
    crate::jj::ensure_project_store(&jj, &store, repo.path()).unwrap();
    Fixture {
        home: home.path().to_path_buf(),
        repo: repo.path().to_path_buf(),
        _home: home,
        _repo: repo,
        workspaces,
        jj,
        store,
    }
}

fn text_at(fx: &Fixture, revision: &str, path: &str) -> String {
    String::from_utf8(crate::jj::file_show(&fx.jj, &fx.store, revision, path).unwrap()).unwrap()
}

fn set_main(fx: &Fixture, revision: &str) {
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                revision,
                "--ignore-working-copy",
            ],
            "advance main",
        )
        .unwrap();
}

struct Replay {
    fx: Fixture,
    db: Arc<LocalDb>,
    orch: Orchestrator,
    main_ws: PathBuf,
    branch_ws: PathBuf,
}

/// A store, a database holding one job on `BRANCH`, and an orchestrator
/// pointed at both. `base_files` are committed to main before the branch is
/// cut.
async fn replay_harness(bin: &str, name: &str, base_files: &[(&str, &str)]) -> Replay {
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;

    let fx = fixture(bin);
    let main_ws = fx.workspaces.path().join("main-line");
    crate::jj::add_workspace(&fx.jj, &fx.store, &main_ws, MAIN_LINE, "main", None).unwrap();
    if !base_files.is_empty() {
        let base = commit(&fx, &main_ws, MAIN_LINE, base_files, "base files");
        set_main(&fx, &base);
    }
    let branch_ws = fx.workspaces.path().join("replay");
    crate::jj::add_workspace(&fx.jj, &fx.store, &branch_ws, BRANCH, "main", None).unwrap();

    let db = Arc::new(crate::storage::migrated_test_db(name).await);
    let repo = fx.repo.to_string_lossy().replace('\'', "''");
    db.execute_script(format!(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
           VALUES ('proj-1', 'default', 'Project', 'proj', '{repo}', 'main', 1, 1);
         INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
           VALUES ('issue-1', 'proj-1', 1, 'Issue', 'active', 1, 1);
         INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
           VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1);
         INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('{JOB}', 'exec-1', 'node', 'issue-1', 'proj-1', 'running', '{BRANCH}', 'main', 1, 1);
         INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
           VALUES ('run-replay', 'issue-1', 'proj-1', '{JOB}', 'live', 1, 1);"
    ))
    .await
    .unwrap();
    let orch = Orchestrator::builder(
        Arc::new(DbState::new(
            db.clone(),
            Arc::new(SearchIndex::open_or_create(fx.home.join("search")).unwrap()),
        )),
        Arc::new(TestServicesBuilder::new().build()),
        fx.home.clone(),
    )
    .jj_binary_path(bin)
    .build();
    Replay {
        fx,
        db,
        orch,
        main_ws,
        branch_ws,
    }
}

/// Write `files` in a workspace, seal them, and return the bookmark's new
/// commit.
fn commit(
    fx: &Fixture,
    workspace: &Path,
    bookmark: &str,
    files: &[(&str, &str)],
    message: &str,
) -> String {
    // A store-side operation since the last seal advanced the op log under
    // this workspace.
    let _ = crate::jj::update_stale(&fx.jj, workspace);
    for (path, content) in files {
        let target = workspace.join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(target, content).unwrap();
    }
    crate::jj::seal(&fx.jj, workspace, message, None).unwrap();
    crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).unwrap()
}

fn branch_tip(r: &Replay) -> String {
    crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, BRANCH).unwrap()
}

/// The owner's commit demanding its current base: under lazy advance, what
/// records the session (or re-records it over a resolution made against an
/// older base), in place of the retired automatic base advance.
async fn owner_commit_conflicts(r: &Replay) {
    let refusal = prepare_branch(
        &r.orch,
        PreparationRequest::new(
            "proj-1",
            BRANCH,
            Some(JOB.to_string()),
            PreparationPurpose::OwnerCommit,
        ),
    )
    .await
    .expect_err("the owner's commit meets a conflicting base");
    assert!(
        matches!(refusal, PreparationRefusal::Conflict(_)),
        "the owner's commit records the conflict: {refusal}"
    );
}

async fn request_take_committed_tip(r: &Replay) -> String {
    request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
        .await
        .unwrap_or_else(|refusal| panic!("the request was refused: {refusal}"))
}

async fn open_session(r: &Replay) -> Option<ConflictSession> {
    load_active_session(&r.db, BRANCH).await.unwrap()
}

fn resolved_against(session: &ConflictSession, path: &str) -> Option<String> {
    session
        .resolution_basis
        .get(path)
        .map(|side| side.theirs.clone())
}

fn carries(r: &Replay, commit: &str) -> bool {
    crate::jj::branch_carries_commit(&r.fx.jj, &r.fx.store, BRANCH, commit)
}

/// Scenario 1's shared start: the branch appends X, D1 appends A at the same
/// spot, the owner's commit records a session, and the agent commits the
/// resolution A-then-X as an ordinary linear commit on the old fork.
async fn same_spot_resolution(bin: &str, name: &str) -> (Replay, String, String) {
    let r = replay_harness(bin, name, &[(LIST, "head\n")]).await;
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nX\n")],
        "branch appends X",
    );
    let d1 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\n")],
        "D1 appends A",
    );
    set_main(&r.fx, &d1);
    owner_commit_conflicts(&r).await;
    let session = open_session(&r)
        .await
        .expect("D1 conflicts and records a session");
    assert_eq!(
        session
            .conflicting()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec![LIST]
    );
    assert_eq!(session.theirs.as_deref(), Some(d1.as_str()));
    let resolved = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nA\nX\n")],
        "resolve against D1",
    );
    (r, d1, resolved)
}

/// The refusal shape scenario 1 requires when D2 appends at the spot the
/// agent resolved: the branch is untouched, D2 is not in its ancestry, and the
/// fresh session against D2 still knows the tip was resolved against D1.
async fn assert_refused_with_fresh_session(r: &Replay, d1: &str, d2: &str, tip: &str) {
    assert_eq!(
        branch_tip(r),
        tip,
        "a refused replay leaves the branch untouched"
    );
    assert!(!carries(r, d2), "no tree landed on D2");
    let session = open_session(r).await.expect("a fresh session against D2");
    assert_eq!(session.theirs.as_deref(), Some(d2));
    assert_eq!(session.ours.as_deref(), Some(tip));
    assert_eq!(resolved_against(&session, LIST).as_deref(), Some(d1));
}

macro_rules! jj_or_skip {
    () => {
        match crate::jj::tests::jj_bin() {
            Some(bin) => bin,
            None => return,
        }
    };
}

/// Scenario 1a, the specimen path: the session recorded against D1 is still
/// open when the request lands, and main is already at D2.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_resolution_against_d1_is_not_restored_over_d2s_same_spot_append() {
    let bin = jj_or_skip!();
    let (r, d1, resolved) = same_spot_resolution(&bin, "corpus-1a.db").await;
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\nB\n")],
        "D2 appends B",
    );
    set_main(&r.fx, &d2);

    request_take_committed_tip(&r).await;
    assert_refused_with_fresh_session(&r, &d1, &d2, &resolved).await;

    // The agent resolves the new pair of sides and the replay lands both.
    let rebased = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nA\nB\nX\n")],
        "resolve against D2",
    );
    assert_ne!(rebased, resolved);
    request_take_committed_tip(&r).await;
    assert!(carries(&r, &d2));
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nB\nX\n");
    assert!(
        open_session(&r).await.is_none(),
        "the landed replay closes the session"
    );
}

// ── cairn/5473: a branch's deletion of a file the base edited ──────────────
