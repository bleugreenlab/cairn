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

fn add_migration(workspace: &Path, stem: &str) {
    std::fs::write(
        workspace.join(format!("src-tauri/turso_migrations/{stem}.sql")),
        format!("-- {stem}\n"),
    )
    .unwrap();
    let path = workspace.join(MIGRATIONS);
    let registry = std::fs::read_to_string(&path).unwrap();
    std::fs::write(
        path,
        registry.replace(
            "        migration!(\"0229_after\"),",
            &format!("        migration!(\"{stem}\"),\n        migration!(\"0229_after\"),"),
        ),
    )
    .unwrap();
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

fn delete(fx: &Fixture, workspace: &Path, bookmark: &str, path: &str, message: &str) -> String {
    let _ = crate::jj::update_stale(&fx.jj, workspace);
    std::fs::remove_file(workspace.join(path)).unwrap();
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

/// Scenario 1b: the owner's commit onto D2 re-records a session whose `ours`
/// IS the resolution, before the request. The guard must still judge the tip
/// against D1, the side it was resolved against.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_session_recorded_over_the_resolution_still_knows_it_was_made_against_d1() {
    let bin = jj_or_skip!();
    let (r, d1, resolved) = same_spot_resolution(&bin, "corpus-1b.db").await;
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\nB\n")],
        "D2 appends B",
    );
    set_main(&r.fx, &d2);
    owner_commit_conflicts(&r).await;
    let session = open_session(&r)
        .await
        .expect("D2 conflicts with the resolution");
    assert_eq!(session.ours.as_deref(), Some(resolved.as_str()));
    assert_eq!(resolved_against(&session, LIST), Some(d1.clone()));

    request_take_committed_tip(&r).await;
    assert_refused_with_fresh_session(&r, &d1, &d2, &resolved).await;
}

/// After the session is re-recorded over the resolution, the agent commits
/// only an UNRELATED file. `list.txt` must stay resolved against D1: promoted
/// to D2, the guard would certify `head A X` against D2 and the carry would
/// restore it whole, dropping B.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_commit_to_an_unrelated_file_does_not_promote_a_resolution_to_d2() {
    let bin = jj_or_skip!();
    let (r, d1, _resolved) = same_spot_resolution(&bin, "corpus-1b-unrelated.db").await;
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\nB\n")],
        "D2 appends B",
    );
    set_main(&r.fx, &d2);
    owner_commit_conflicts(&r).await;
    assert!(open_session(&r).await.is_some());
    let unrelated = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("other.txt", "unrelated\n")],
        "an unrelated change",
    );

    request_take_committed_tip(&r).await;
    assert_refused_with_fresh_session(&r, &d1, &d2, &unrelated).await;
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
}

/// Two resolved files, and D2 changes the resolved region of only one. The
/// replay carries `f`, refuses `g`, and installs neither, so the next session
/// conflicts on `g` alone. When the agent re-resolves `g`, the replay must
/// still place `f`, or the two files trade places forever.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_carried_file_rolled_back_with_its_neighbour_lands_on_the_next_retry() {
    let bin = jj_or_skip!();
    let r = replay_harness(
        &bin,
        "corpus-two-file.db",
        &[("f.txt", "head\n"), ("g.txt", "head\n")],
    )
    .await;
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("f.txt", "head\nX\n"), ("g.txt", "head\nX\n")],
        "branch appends X to both",
    );
    let d1 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[("f.txt", "head\nA\n"), ("g.txt", "head\nA\n")],
        "D1 appends A to both",
    );
    set_main(&r.fx, &d1);
    owner_commit_conflicts(&r).await;
    let resolved = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("f.txt", "head\nA\nX\n"), ("g.txt", "head\nA\nX\n")],
        "resolve both against D1",
    );
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[("g.txt", "head\nA\nB\n")],
        "D2 appends B to g only",
    );
    set_main(&r.fx, &d2);

    request_take_committed_tip(&r).await;
    assert_eq!(branch_tip(&r), resolved, "g's refusal installs f neither");
    let retried = open_session(&r).await.expect("a session against D2");
    assert!(retried.conflicting().any(|file| file.path == "g.txt"));
    assert_eq!(resolved_against(&retried, "f.txt"), Some(d1.clone()));
    assert_eq!(resolved_against(&retried, "g.txt"), Some(d1.clone()));

    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("g.txt", "head\nA\nB\nX\n")],
        "resolve g against D2",
    );
    request_take_committed_tip(&r).await;
    assert!(
        carries(&r, &d2),
        "the retry landed: {:?}",
        open_session(&r).await.map(|session| session
            .conflicting()
            .map(|file| file.path.clone())
            .collect::<Vec<_>>())
    );
    assert_eq!(text_at(&r.fx, BRANCH, "f.txt"), "head\nA\nX\n");
    assert_eq!(text_at(&r.fx, BRANCH, "g.txt"), "head\nA\nB\nX\n");
    assert!(open_session(&r).await.is_none());
}

/// Scenario 1c: D2 changes the same file somewhere the resolution did not
/// touch, so the resolution is carried onto it and all three changes land.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_resolution_is_merged_with_d2s_change_elsewhere_in_the_file() {
    let bin = jj_or_skip!();
    let (r, _d1, _resolved) = same_spot_resolution(&bin, "corpus-1c.db").await;
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "B\nhead\nA\n")],
        "D2 prepends B",
    );
    set_main(&r.fx, &d2);

    request_take_committed_tip(&r).await;
    assert!(carries(&r, &d2));
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "B\nhead\nA\nX\n");
    assert!(open_session(&r).await.is_none());
}

/// cairn/5222 and cairn/5328: the branch commits D1's line beside its own
/// BEFORE any session is recorded. The line merge still conflicts at the
/// shared insertion point, so the session is recorded with that resolution as
/// `ours` and no basis. The tip already holds both sides, so the request is
/// accepted and the replay lands it.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_resolution_committed_before_the_session_was_recorded_is_taken() {
    let bin = jj_or_skip!();
    let r = replay_harness(&bin, "corpus-pre-resolved.db", &[(LIST, "head\n")]).await;
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
    let resolved = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nA\nX\n")],
        "carry D1's line ahead of the advance",
    );
    owner_commit_conflicts(&r).await;
    let session = open_session(&r)
        .await
        .expect("the line merge still conflicts at the shared insertion point");
    assert_eq!(session.ours.as_deref(), Some(resolved.as_str()));
    assert_eq!(
        resolved_against(&session, LIST),
        None,
        "no session existed when the resolution was committed, so none is recorded"
    );
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("other.txt", "unrelated\n")],
        "an unrelated follow-up",
    );

    request_take_committed_tip(&r).await;
    assert!(
        carries(&r, &d1),
        "the replay landed: {:?}",
        open_session(&r).await.map(|session| session.fingerprint())
    );
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    assert_eq!(text_at(&r.fx, BRANCH, "other.txt"), "unrelated\n");
    assert!(open_session(&r).await.is_none());
}

/// A tip whose conflicting file is still only the branch's side is refused,
/// by name, even after commits to other files have moved it, and a stated
/// reason cannot supply a resolution that does not exist.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_one_sided_tip_is_still_refused_by_name() {
    let bin = jj_or_skip!();
    let r = replay_harness(&bin, "corpus-one-sided.db", &[(LIST, "head\n")]).await;
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
    let session = open_session(&r).await.expect("D1 conflicts");
    let ours = session.ours.clone().expect("the session records ours");
    let moved = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("other.txt", "unrelated\n")],
        "an unrelated change",
    );

    let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
        .await
        .expect_err("a one-sided tip has no resolution to take");
    assert!(refusal.contains("resolves none"), "{refusal}");
    assert!(refusal.contains(&format!("- `{LIST}`")), "{refusal}");
    assert!(
        refusal.contains(&format!("changes it on top of `{ours}`")),
        "{refusal}"
    );
    let with_reason =
        request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some("trust me"))
            .await
            .expect_err("a stated reason cannot supply a resolution");
    assert!(with_reason.contains("resolves none"), "{with_reason}");
    assert_eq!(branch_tip(&r), moved, "a refused request moves nothing");
}

/// cairn/5051: both sides insert a module declaration above the same line,
/// and D1 also appends a function further down. A tip holding the function but
/// not D1's declaration is refused, quoting the missing line; the merged
/// view's candidate then lands with both declarations.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_same_spot_insertion_the_tip_lacks_is_refused_by_name_then_lands_with_both() {
    let bin = jj_or_skip!();
    const MODS: &str = "mod.rs";
    let base = "mod policy;\nmod rules;\n\npub fn run() {}\n";
    let r = replay_harness(&bin, "corpus-same-spot-insert.db", &[(MODS, base)]).await;
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(
            MODS,
            "pub mod evidence;\nmod policy;\nmod rules;\n\npub fn run() {}\n",
        )],
        "branch declares evidence",
    );
    let incoming =
        "pub(crate) mod filing;\nmod policy;\nmod rules;\n\npub fn run() {}\n\npub fn answer_and_wake() {}\n";
    let d1 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(MODS, incoming)],
        "D1 declares filing and adds answer_and_wake",
    );
    set_main(&r.fx, &d1);
    owner_commit_conflicts(&r).await;
    let session = open_session(&r)
        .await
        .expect("the shared insertion point conflicts");
    assert_eq!(
        session
            .conflicting()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec![MODS]
    );

    let partial = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(
            MODS,
            "pub mod evidence;\nmod policy;\nmod rules;\n\npub fn run() {}\n\npub fn answer_and_wake() {}\n",
        )],
        "take the listed hunks",
    );
    let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
        .await
        .expect_err("a tip missing D1's declaration would drop it");
    assert!(
        refusal.contains("+pub(crate) mod filing;"),
        "the refusal quotes the dropped line: {refusal}"
    );
    assert_eq!(branch_tip(&r), partial, "a refused request moves nothing");

    let both =
        "pub mod evidence;\npub(crate) mod filing;\nmod policy;\nmod rules;\n\npub fn run() {}\n\npub fn answer_and_wake() {}\n";
    assert_eq!(
        crate::jj::completion_candidate(base, &text_at(&r.fx, BRANCH, MODS), incoming),
        both,
        "the merged view's candidate is the union"
    );
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(MODS, both)],
        "commit the candidate",
    );
    request_take_committed_tip(&r).await;
    assert!(
        carries(&r, &d1),
        "the replay landed: {:?}",
        open_session(&r).await.map(|session| session.fingerprint())
    );
    assert_eq!(text_at(&r.fx, BRANCH, MODS), both);
    assert!(open_session(&r).await.is_none());
}

// ── cairn/5473: a branch's deletion of a file the base edited ──────────────

const FENCE: &str = "src/fence.rs";
const FENCE_BASE: &str = "fn fence() {}\n";

/// The branch retires `FENCE` before any session exists, D1 edits it (and
/// `LIST`, which the branch never touches), and the owner's commit records a
/// session conflicting on the deletion alone, with nothing in the basis for
/// it. Returns the harness, D1, and the tip.
async fn deletion_before_the_session(bin: &str, name: &str) -> (Replay, String, String) {
    let r = replay_harness(bin, name, &[(FENCE, FENCE_BASE), (LIST, "head\n")]).await;
    let deleted = delete(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        FENCE,
        "branch retires fence.rs",
    );
    let d1 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[
            (FENCE, "fn fence() {}\nfn grant() {}\n"),
            (LIST, "head\nA\n"),
        ],
        "D1 tightens the fence",
    );
    set_main(&r.fx, &d1);
    owner_commit_conflicts(&r).await;
    let session = open_session(&r)
        .await
        .expect("the deletion conflicts with D1's edit");
    assert_eq!(
        session
            .conflicting()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec![FENCE]
    );
    assert_eq!(resolved_against(&session, FENCE), None);
    (r, d1, deleted)
}

fn fence_is_absent_at(r: &Replay, revision: &str) -> bool {
    crate::jj::paths_present_at(&r.fx.jj, &r.fx.store, revision, &[FENCE.to_string()])
        .unwrap()
        .is_empty()
}

/// A deletion made before any session opened, with no later base edit, is
/// refused by the commit that edited the file until the requester states
/// where that edit went, and then it carries.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_deletion_made_before_the_session_carries_once_attested() {
    let bin = jj_or_skip!();
    let (r, d1, deleted) = deletion_before_the_session(&bin, "corpus-deletion-1.db").await;

    let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
        .await
        .expect_err("nothing covers what D1 did to the deleted file");
    assert!(
        refusal.contains(&d1[..8]) && refusal.contains("D1 tightens the fence"),
        "the refusal names the incoming commit: {refusal}"
    );
    assert!(!refusal.contains("resolves none"), "{refusal}");
    assert!(
        !refusal.contains("since that resolution was made"),
        "{refusal}"
    );
    assert_eq!(branch_tip(&r), deleted, "a refused request moves nothing");

    let reason = "fence.rs's grant check moved into the policy module";
    let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(reason))
        .await
        .unwrap_or_else(|refusal| panic!("the attested request was refused: {refusal}"));
    assert!(answer.contains("Recorded your statement"), "{answer}");
    assert!(
        carries(&r, &d1),
        "the replay landed: {answer} / {:?}",
        open_session(&r).await.map(|session| session.fingerprint())
    );
    assert!(fence_is_absent_at(&r, BRANCH), "the deletion carried");
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\n");
    assert!(open_session(&r).await.is_none());
}

/// A base edit after the attested side is refused by name (by the carry, and
/// then by the guard of the fresh session) never as a stale resolution; the
/// statement survives on the side it covered, and a new statement lands it.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_base_edit_after_the_attestation_is_refused_by_name() {
    let bin = jj_or_skip!();
    let (r, d1, deleted) = deletion_before_the_session(&bin, "corpus-deletion-2.db").await;
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(FENCE, "fn fence() {}\nfn grant() {}\nfn audit() {}\n")],
        "D2 audits the fence",
    );
    set_main(&r.fx, &d2);

    let first = "grant moved into policy::grant";
    let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(first))
        .await
        .unwrap_or_else(|refusal| panic!("the statement covers D1: {refusal}"));
    assert!(answer.contains(first), "{answer}");
    assert_eq!(
        branch_tip(&r),
        deleted,
        "the carry refused, so nothing moved"
    );
    assert!(!carries(&r, &d2));

    let fresh = open_session(&r).await.expect("a fresh session against D2");
    assert_eq!(fresh.theirs.as_deref(), Some(d2.as_str()));
    let kept = fresh
        .resolution_basis
        .get(FENCE)
        .expect("the statement survives");
    assert_eq!(kept.theirs, d1, "on the side it covered");
    assert_eq!(kept.attested.as_deref(), Some(first));

    // The carry's own refusal.
    let plan = plan_resolution_carry(&r.fx.jj, &r.fx.store, &fresh, &d2, &deleted).unwrap();
    let (path, why) = plan
        .refused
        .first()
        .expect("the carry refuses the deletion");
    assert_eq!(path, FENCE);
    assert!(
        why.contains(&d2[..8]) && why.contains("D2 audits the fence"),
        "{why}"
    );
    assert!(
        why.contains(first),
        "it quotes the statement it outran: {why}"
    );
    assert!(!why.contains("since that resolution was made"), "{why}");

    // The page reads the same assessment: the statement, and what outran it.
    let page =
        crate::orchestrator::conflict_session::assess_session_tip(&r.fx.jj, &r.fx.store, &fresh)
            .expect("the page assesses the tip");
    assert_eq!(
        page.attested
            .get(FENCE)
            .and_then(|side| side.attested.as_deref()),
        Some(first)
    );
    assert!(matches!(
        page.deletions.get(FENCE),
        Some(crate::orchestrator::conflict_session::DeletionStanding::Outpaced(_))
    ));

    let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
        .await
        .expect_err("the statement covered D1, not D2");
    assert!(
        refusal.contains(&d2[..8]) && refusal.contains("D2 audits the fence"),
        "{refusal}"
    );
    assert!(
        !refusal.contains("D1 tightens the fence"),
        "only what came after the statement: {refusal}"
    );

    let second = "audit moved into policy::audit too";
    let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(second))
        .await
        .unwrap_or_else(|refusal| panic!("the new statement covers D2: {refusal}"));
    assert!(answer.contains(second), "{answer}");
    assert!(
        carries(&r, &d2),
        "the replay landed: {answer} / {:?}",
        open_session(&r).await.map(|session| session.fingerprint())
    );
    assert!(fence_is_absent_at(&r, BRANCH));
    assert!(open_session(&r).await.is_none());
}

/// The deletion is made WHILE the session is open, so it reads as a content
/// change since `ours` on every recomputation of the basis. The statement must
/// survive those rereads and the inheritance onto a D2 session, so the next
/// refusal names only what D2 did and quotes the statement it outran.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_deletion_attested_during_the_session_survives_a_rollback_onto_d2() {
    let bin = jj_or_skip!();
    let r = replay_harness(
        &bin,
        "corpus-deletion-in-session.db",
        &[(FENCE, FENCE_BASE)],
    )
    .await;
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(FENCE, "fn fence() {}\nfn branch() {}\n")],
        "branch extends the fence",
    );
    let d1 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(FENCE, "fn fence() {}\nfn grant() {}\n")],
        "D1 tightens the fence",
    );
    set_main(&r.fx, &d1);
    owner_commit_conflicts(&r).await;
    assert!(open_session(&r)
        .await
        .expect("both sides extend the fence")
        .conflicting()
        .any(|file| file.path == FENCE));
    let deleted = delete(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        FENCE,
        "retire fence.rs while the conflict is open",
    );
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(FENCE, "fn fence() {}\nfn grant() {}\nfn audit() {}\n")],
        "D2 audits the fence",
    );
    set_main(&r.fx, &d2);

    let first = "grant moved into policy::grant";
    request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(first))
        .await
        .unwrap_or_else(|refusal| panic!("the statement covers D1: {refusal}"));
    assert_eq!(branch_tip(&r), deleted, "D2 outran the statement");

    let fresh = open_session(&r).await.expect("a fresh session against D2");
    assert_eq!(fresh.theirs.as_deref(), Some(d2.as_str()));
    let kept = fresh
        .resolution_basis
        .get(FENCE)
        .expect("the inheritance kept the statement");
    assert_eq!(kept.theirs, d1);
    assert_eq!(kept.attested.as_deref(), Some(first));
    let reread = fresh
        .resolution_basis_at(&r.fx.jj, &r.fx.store, &deleted)
        .unwrap();
    assert_eq!(
        reread.get(FENCE).and_then(|side| side.attested.as_deref()),
        Some(first),
        "and a reread keeps it"
    );

    let refusal = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None)
        .await
        .expect_err("the statement covered D1, not D2");
    assert!(
        refusal.contains(&d2[..8]) && refusal.contains("D2 audits the fence"),
        "{refusal}"
    );
    assert!(
        refusal.contains(first) && !refusal.contains("D1 tightens the fence"),
        "measured from the statement, not the fork: {refusal}"
    );

    let second = "audit moved into policy::audit too";
    request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, Some(second))
        .await
        .unwrap_or_else(|refusal| panic!("the new statement covers D2: {refusal}"));
    assert!(
        carries(&r, &d2),
        "the replay landed: {:?}",
        open_session(&r).await.map(|session| session.fingerprint())
    );
    assert!(fence_is_absent_at(&r, BRANCH));
    assert!(open_session(&r).await.is_none());
}

/// Scenario 2, the specimen's own file: the branch resolved D1's migration
/// registration and renumbered its own, then D2 registered a migration at the
/// number the branch took. The replayed registry holds every entry once. The
/// replay does not renumber the collision: numbering is the landing's
/// (`landing::bind`).
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_registry_resolution_against_d1_keeps_d2s_registration_and_leaves_numbers_to_the_landing()
{
    let bin = jj_or_skip!();
    let r = replay_harness(&bin, "corpus-2.db", &[("other.txt", "one\n")]).await;
    add_migration(&r.branch_ws, "0228_branch");
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[("other.txt", "one\nbranch\n")],
        "branch",
    );
    add_migration(&r.main_ws, "0228_destination");
    let d1 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[("other.txt", "one\nmain\n")],
        "D1",
    );
    set_main(&r.fx, &d1);
    owner_commit_conflicts(&r).await;
    let session = open_session(&r)
        .await
        .expect("a non-registry path keeps it a session");
    assert_eq!(
        session
            .conflicting()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>(),
        vec!["other.txt", MIGRATIONS]
    );

    // The agent's resolution: D1's registration kept, its own renumbered past
    // it, as one linear commit.
    let _ = crate::jj::update_stale(&r.fx.jj, &r.branch_ws);
    std::fs::rename(
        r.branch_ws
            .join("src-tauri/turso_migrations/0228_branch.sql"),
        r.branch_ws
            .join("src-tauri/turso_migrations/0230_branch.sql"),
    )
    .unwrap();
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[
            ("other.txt", "one\nmain\nbranch\n"),
            (
                MIGRATIONS,
                "fn migrations() {\n    vec![\n        migration!(\"0227_before\"),\n        migration!(\"0228_destination\"),\n        migration!(\"0230_branch\"),\n        migration!(\"0229_after\"),\n    ];\n}\n",
            ),
        ],
        "resolve against D1",
    );
    add_migration(&r.main_ws, "0230_d2");
    let d2 = commit(&r.fx, &r.main_ws, MAIN_LINE, &[], "D2 registers 0230");
    set_main(&r.fx, &d2);

    let answer = request_take_committed_tip(&r).await;
    assert!(carries(&r, &d2), "the replay landed: {answer}");
    let registry = text_at(&r.fx, BRANCH, MIGRATIONS);
    for (stem, expected) in [
        ("0228_destination", 1),
        ("0230_d2", 1),
        ("0230_branch", 1),
        ("0228_branch", 0),
    ] {
        assert_eq!(
            registry.matches(&format!("\"{stem}\"")).count(),
            expected,
            "{stem} in:\n{registry}"
        );
    }
    let files =
        crate::jj::paths_under_at(&r.fx.jj, &r.fx.store, BRANCH, "src-tauri/turso_migrations")
            .unwrap();
    for file in ["0230_branch.sql", "0230_d2.sql"] {
        assert!(
            files.contains(&format!("src-tauri/turso_migrations/{file}")),
            "the replay moves no number: {files:?}"
        );
    }
    assert_eq!(text_at(&r.fx, BRANCH, "other.txt"), "one\nmain\nbranch\n");
}
