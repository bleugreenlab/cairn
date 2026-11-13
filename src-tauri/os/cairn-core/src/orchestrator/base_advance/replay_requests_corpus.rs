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
    let write = || {
        for (path, content) in files {
            let target = workspace.join(path);
            std::fs::create_dir_all(target.parent().unwrap()).unwrap();
            std::fs::write(target, content).unwrap();
        }
    };
    write();
    if let Err(error) = crate::jj::seal(&fx.jj, workspace, message, None) {
        // A landed replay moved the bookmark past this workspace: commit on
        // the tip, as the agent's next write would.
        assert!(
            error.to_string().contains("behind its branch tip"),
            "seal: {error}"
        );
        fx.jj
            .run(workspace, &["new", bookmark], "start on the bookmark tip")
            .unwrap();
        write();
        crate::jj::seal(&fx.jj, workspace, message, None).unwrap();
    }
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
///
/// cairn/7095: the resolution of D1 lands on D1, and D2's same-spot append is
/// a following round with its own session. The guarantee is unchanged: no
/// tree missing D2's append ever lands.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_resolution_against_d1_is_not_restored_over_d2s_same_spot_append() {
    let bin = jj_or_skip!();
    let (r, d1, _resolved) = same_spot_resolution(&bin, "corpus-1a.db").await;
    let d2 = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\nB\n")],
        "D2 appends B",
    );
    set_main(&r.fx, &d2);

    let answer = request_take_committed_tip(&r).await;
    assert!(carries(&r, &d1), "the resolution landed on D1: {answer}");
    assert!(!carries(&r, &d2), "no tree landed on D2: {answer}");
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    let landed = branch_tip(&r);
    let session = open_session(&r)
        .await
        .expect("D2's round opens its own session");
    assert_eq!(session.theirs.as_deref(), Some(d2.as_str()));
    assert_eq!(session.ours.as_deref(), Some(landed.as_str()));
    assert_eq!(conflicting(&session), vec![LIST]);
    assert!(
        answer.contains(&format!("`main` is now `{d2}`"))
            && answer.contains(&format!(
                "these files conflict with `{d2}`'s new commits: {LIST}"
            )),
        "{answer}"
    );

    // The agent resolves D2's append and the replay lands it.
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nA\nB\nX\n")],
        "resolve against D2",
    );
    request_take_committed_tip(&r).await;
    assert!(carries(&r, &d2));
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nB\nX\n");
    assert!(
        open_session(&r).await.is_none(),
        "the landed replay closes the session"
    );
}

// ── cairn/7095: land on the recorded base, then the increment ──────────────────

const OTHER: &str = "other.txt";
const UNRELATED: &str = "unrelated.txt";

fn conflicting(session: &ConflictSession) -> Vec<&str> {
    session
        .conflicting()
        .map(|file| file.path.as_str())
        .collect()
}

async fn set_merge_mode(r: &Replay) {
    let main = crate::jj::bookmark_commit(&r.fx.jj, &r.fx.store, "main").unwrap();
    crate::orchestrator::catch_up::set(
        &r.db,
        crate::orchestrator::catch_up::SetCatchUpMode {
            project_id: "proj-1",
            store: &r.fx.store,
            bookmark: BRANCH,
            job_id: JOB,
            actor: "operator (test)",
            review_paths: &[],
            reviewed_through: &main,
        },
    )
    .await
    .unwrap();
}

async fn prepare_as(
    r: &Replay,
    purpose: PreparationPurpose,
) -> Result<PreparedBranch, PreparationRefusal> {
    prepare_branch(
        &r.orch,
        PreparationRequest::new("proj-1", BRANCH, Some(JOB.to_string()), purpose),
    )
    .await
}

fn parents(r: &Replay, revision: &str) -> Vec<String> {
    r.fx.jj
        .run(
            &r.fx.store,
            &[
                "log",
                "--no-graph",
                "--ignore-working-copy",
                "-r",
                revision,
                "-T",
                "parents.map(|c| c.commit_id()).join(\" \")",
            ],
            "parents",
        )
        .unwrap()
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

async fn count(r: &Replay, sql: &str) -> i64 {
    r.db.query_text(&format!("SELECT CAST(({sql}) AS TEXT)"), ())
        .await
        .unwrap()
        .unwrap()
        .parse()
        .unwrap()
}

/// Every intent is keyed by the relationship's base name, never by a commit
/// standing in for it.
async fn assert_no_commit_named_intents(r: &Replay) {
    assert_eq!(
        count(
            r,
            "SELECT COUNT(*) FROM jj_reconcile_intents WHERE target_branch <> 'main'"
        )
        .await,
        0
    );
}

async fn following_rounds(r: &Replay) -> i64 {
    count(
        r,
        "SELECT COUNT(*) FROM jj_reconcile_items WHERE replay_parent_request_id IS NOT NULL",
    )
    .await
}

/// The branch changes `LIST` and `OTHER`; X appends at `LIST`'s same spot, so
/// the owner's commit records a session at X naming only `LIST`, which the
/// agent then resolves with an ordinary linear commit.
async fn resolved_at_x(bin: &str, name: &str, merge: bool) -> (Replay, String, String) {
    resolved_at_x_with(bin, name, merge, "head\nA\nX\n").await
}

async fn resolved_at_x_with(
    bin: &str,
    name: &str,
    merge: bool,
    resolution: &str,
) -> (Replay, String, String) {
    let r = replay_harness(
        bin,
        name,
        &[(LIST, "head\n"), (OTHER, "o\n"), (UNRELATED, "u\n")],
    )
    .await;
    if merge {
        set_merge_mode(&r).await;
    }
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nX\n"), (OTHER, "o\nmine\n")],
        "branch work",
    );
    let x = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\n")],
        "X appends A",
    );
    set_main(&r.fx, &x);
    // In merge mode the commit itself publishes with the catch-up pending.
    let _ = prepare_as(&r, PreparationPurpose::OwnerCommit).await;
    let session = open_session(&r).await.expect("X records a session");
    assert_eq!(session.destination_commit, x);
    assert_eq!(conflicting(&session), vec![LIST]);
    let resolved = commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, resolution)],
        "resolve against X",
    );
    (r, x, resolved)
}

/// Y edits `OTHER` where the branch did, so its round conflicts there alone.
fn main_moves_to_y(r: &Replay) -> String {
    let y = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(OTHER, "o\ntheirs\n")],
        "Y edits other",
    );
    set_main(&r.fx, &y);
    y
}

async fn assert_y_round_open(r: &Replay, x: &str, y: &str) {
    assert!(carries(r, x));
    assert!(!carries(r, y));
    let session = open_session(r)
        .await
        .expect("Y's round opens its own session");
    assert_eq!(session.destination_commit, y);
    assert_eq!(conflicting(&session), vec![OTHER]);
    assert_eq!(
        conflict_session::load_pending_replay_request(&r.db, &r.fx.store, BRANCH)
            .await
            .unwrap(),
        None,
        "nothing is left waiting for a trigger"
    );
}

/// A recovered request whose following round finds the budget spent: the
/// recovery server still holds the branch's claim, so it serves the child
/// itself, with no further trigger.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_deferred_round_of_a_recovered_request_is_served_by_its_own_server() {
    let bin = jj_or_skip!();
    let (r, x, resolved) = resolved_at_x(&bin, "deferred-round.db", false).await;
    let y = main_moves_to_y(&r);
    super::tests::defer_increments(&r.fx.store);
    conflict_session::queue_replay_request(
        &r.db,
        "proj-1",
        &r.fx.store,
        "main",
        &x,
        BRANCH,
        Some(&resolved),
        true,
        None,
    )
    .await
    .unwrap();
    serve_pending_replay_requests(&r.orch, Some((&r.fx.store, BRANCH))).await;
    assert_y_round_open(&r, &x, &y).await;
    assert_eq!(following_rounds(&r).await, 1);
}

/// A plain request is accepted at the session base it is prepared against,
/// so its following round is its own row and the landing stays readable.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_plain_request_landing_the_session_base_keeps_its_own_row() {
    let bin = jj_or_skip!();
    // Dropping the branch's own edit to LIST makes merge(tip, X) clean while
    // the session at X is still open.
    let (r, x, _) = resolved_at_x_with(&bin, "plain-increment.db", true, "head\n").await;
    let y = main_moves_to_y(&r);
    let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, false, None)
        .await
        .unwrap_or_else(|refusal| panic!("refused: {refusal}"));
    assert_y_round_open(&r, &x, &y).await;
    assert!(answer.contains(&format!("`main` is now `{y}`")), "{answer}");

    let latest = conflict_session::latest_replay_request(&r.db, JOB, BRANCH)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.destination_commit, y);
    let (parent, landed) = latest.opened_by.expect("the round names its parent");
    assert_eq!(landed, x, "the page reads the base the parent landed");
    assert_eq!(
        conflict_session::replay_request_destination(&r.db, &parent)
            .await
            .unwrap()
            .as_deref(),
        Some(x.as_str()),
        "the parent's row survives its child"
    );
}

/// A request accepted against X before this change, whose session an
/// owner's commit then superseded by recording one against Y: recovery
/// serves it against its own session, lands X, and opens Y's round.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_recovered_request_lands_its_own_session_after_an_old_style_supersession() {
    let bin = jj_or_skip!();
    let (r, x, resolved) = resolved_at_x(&bin, "superseded-recovery.db", false).await;
    let y = main_moves_to_y(&r);
    conflict_session::queue_replay_request(
        &r.db,
        "proj-1",
        &r.fx.store,
        "main",
        &x,
        BRANCH,
        Some(&resolved),
        true,
        None,
    )
    .await
    .unwrap();
    // What the old owner-commit preparation did: judge the commit against Y,
    // record a session there and supersede X's.
    let mut old_style = PreparationRequest::new(
        "proj-1",
        BRANCH,
        Some(JOB.to_string()),
        PreparationPurpose::OwnerCommit,
    );
    old_style.exact_destination = Some(y.clone());
    let _ = prepare_branch(&r.orch, old_style).await;
    assert_eq!(open_session(&r).await.unwrap().destination_commit, y);

    serve_pending_replay_requests(&r.orch, Some((&r.fx.store, BRANCH))).await;
    assert_y_round_open(&r, &x, &y).await;
}

async fn lands_on_x_then_opens_the_increment(merge: bool, name: &str) {
    let bin = jj_or_skip!();
    let (r, x, resolved) = resolved_at_x(&bin, name, merge).await;
    let y = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(OTHER, "o\ntheirs\n")],
        "Y edits other",
    );
    set_main(&r.fx, &y);

    let answer = request_take_committed_tip(&r).await;
    assert!(carries(&r, &x), "{answer}");
    assert!(!carries(&r, &y), "{answer}");
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    if merge {
        assert_eq!(
            parents(&r, BRANCH),
            vec![resolved.clone(), x.clone()],
            "one two-parent merge of X onto the resolved tip"
        );
    }
    assert_eq!(
        count(
            &r,
            &format!(
                "SELECT COUNT(*) FROM jj_reconcile_items i JOIN jj_reconcile_intents n \
                 ON n.id = i.intent_id WHERE n.destination_commit = '{x}' \
                 AND i.resolution_state = 'open'"
            ),
        )
        .await,
        0,
        "the session at X is finished"
    );
    let session = open_session(&r)
        .await
        .expect("Y's round opens its own session");
    assert_eq!(session.destination_commit, y);
    assert_eq!(conflicting(&session), vec![OTHER], "only Y's own conflict");
    let landed_on_x = if merge {
        format!("Merged `main` at `{x}` into `{BRANCH}`")
    } else {
        format!("Replayed `{BRANCH}` onto `main` at `{x}`")
    };
    for expected in [
        landed_on_x,
        "landed on the base it was made against".to_string(),
        format!("Resolved against `{x}`; `main` is now `{y}`"),
        format!("these files conflict with `{y}`'s new commits: {OTHER}"),
    ] {
        assert!(answer.contains(&expected), "{expected}: {answer}");
    }
    assert!(
        !answer.contains(&format!("does not yet resolve `{x}`")),
        "a stale resolution never reads as a wrong one: {answer}"
    );
    assert_eq!(following_rounds(&r).await, 1);
    assert_no_commit_named_intents(&r).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_replayed_resolution_lands_on_its_base_and_the_increment_opens_its_own_session() {
    lands_on_x_then_opens_the_increment(false, "increment-replay.db").await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_merged_resolution_lands_on_its_base_and_the_increment_opens_its_own_session() {
    lands_on_x_then_opens_the_increment(true, "increment-merge.db").await;
}

async fn one_round_when_main_did_not_move(merge: bool, name: &str) {
    let bin = jj_or_skip!();
    let (r, x, _) = resolved_at_x(&bin, name, merge).await;
    let answer = request_take_committed_tip(&r).await;
    assert!(carries(&r, &x), "{answer}");
    assert!(open_session(&r).await.is_none(), "{answer}");
    assert!(!answer.contains("following round"), "{answer}");
    assert_eq!(following_rounds(&r).await, 0);
    assert_no_commit_named_intents(&r).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_replay_when_main_did_not_move_is_one_round() {
    one_round_when_main_did_not_move(false, "one-round-replay.db").await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_merge_when_main_did_not_move_is_one_round() {
    one_round_when_main_did_not_move(true, "one-round-merge.db").await;
}

/// A clean increment lands in the same request: X, then Y.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_clean_increment_lands_in_the_same_request() {
    let bin = jj_or_skip!();
    let (r, x, _) = resolved_at_x(&bin, "clean-increment.db", false).await;
    let y = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(UNRELATED, "u\nnew\n")],
        "Y edits a file the branch never touched",
    );
    set_main(&r.fx, &y);
    let answer = request_take_committed_tip(&r).await;
    assert!(carries(&r, &y), "{answer}");
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    assert!(open_session(&r).await.is_none(), "{answer}");
    assert!(
        answer.contains(&format!("onto `main` at `{x}`"))
            && answer.contains(&format!("`main` is now `{y}`, so its commits since `{x}`"))
            && answer.contains(&format!("onto `main` at `{y}`")),
        "{answer}"
    );
}

/// M1: an owner's commit after main moved is judged against the open
/// session's base instead of recording a session against the new main and
/// superseding the one its resolution answers. A landing in the same state is
/// refused naming that session and records nothing.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn an_owner_commit_after_main_moves_keeps_the_session_it_resolves() {
    let bin = jj_or_skip!();
    let (r, x, resolved) = resolved_at_x(&bin, "owner-commit-pin.db", false).await;
    let y = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(OTHER, "o\ntheirs\n")],
        "Y edits other",
    );
    set_main(&r.fx, &y);

    let prepared = prepare_as(&r, PreparationPurpose::OwnerCommit)
        .await
        .expect("the resolution commit binds its session at X");
    assert_eq!(prepared.base_commit, x);
    assert!(prepared.pinned_base && !prepared.advanced);
    assert_eq!(prepared.head_after, resolved);
    let session = open_session(&r).await.unwrap();
    assert_eq!(session.destination_commit, x, "still the session at X");

    match prepare_as(&r, PreparationPurpose::OwnerMerge).await {
        Err(PreparationRefusal::Conflict(conflict)) => {
            assert_eq!(conflict.base_commit, x);
            assert_eq!(conflict.paths, vec![LIST.to_string()]);
            assert!(matches!(
                conflict.record,
                ConflictRecord::Session { reopened: false, ref intent_id } if *intent_id == session.intent_id
            ));
        }
        other => panic!(
            "a landing waits for the open session: {:?}",
            other.map(|p| p.base_commit)
        ),
    }
    assert_eq!(open_session(&r).await.unwrap().intent_id, session.intent_id);
    assert_eq!(
        count(
            &r,
            &format!(
                "SELECT COUNT(*) FROM jj_reconcile_items i JOIN jj_reconcile_intents n \
                 ON n.id = i.intent_id WHERE n.destination_commit = '{y}'"
            ),
        )
        .await,
        0,
        "nothing is recorded against Y"
    );
}

/// M2: main adds `F` between the fork and X; the session at X lists only
/// `LIST`, and the owner's linear hand merge also brings in `F`. The carry
/// must place the whole inventory of merging the tip with X, not only the
/// list recorded at an earlier tip.
async fn hand_merge_with_an_extra_path(
    name: &str,
    tip_f: &str,
) -> (Replay, String, Result<String, String>) {
    const F: &str = "publications.rs";
    let bin = crate::jj::tests::jj_bin().expect("checked by the caller");
    let r = replay_harness(&bin, name, &[(LIST, "head\n")]).await;
    set_merge_mode(&r).await;
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nX\n")],
        "branch work",
    );
    let added = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(F, "f1\nf2\n")],
        "main adds F",
    );
    set_main(&r.fx, &added);
    let x = commit(
        &r.fx,
        &r.main_ws,
        MAIN_LINE,
        &[(LIST, "head\nA\n")],
        "X appends A",
    );
    set_main(&r.fx, &x);
    let _ = prepare_as(&r, PreparationPurpose::OwnerCommit).await;
    let session = open_session(&r).await.expect("X records a session");
    assert_eq!(
        conflicting(&session),
        vec![LIST],
        "F merges cleanly at the first tip"
    );
    commit(
        &r.fx,
        &r.branch_ws,
        BRANCH,
        &[(LIST, "head\nA\nX\n"), (F, tip_f)],
        "linear hand merge of X",
    );
    let answer = request_branch_replay(&r.orch, &r.db, JOB, BRANCH, None, true, None).await;
    (r, x, answer)
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_complete_hand_merge_lands_on_its_base_in_one_request() {
    let _ = jj_or_skip!();
    let (r, x, answer) = hand_merge_with_an_extra_path("hand-merge.db", "f1\nf2\nadapted\n").await;
    let answer = answer.unwrap_or_else(|refusal| panic!("refused: {refusal}"));
    assert!(carries(&r, &x), "{answer}");
    assert_eq!(text_at(&r.fx, BRANCH, LIST), "head\nA\nX\n");
    assert_eq!(
        text_at(&r.fx, BRANCH, "publications.rs"),
        "f1\nf2\nadapted\n"
    );
    assert!(open_session(&r).await.is_none(), "{answer}");
    assert_no_commit_named_intents(&r).await;
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_hand_merge_that_drops_the_bases_file_is_refused_by_name_and_keeps_every_path() {
    let _ = jj_or_skip!();
    let (r, x, answer) = hand_merge_with_an_extra_path("hand-merge-drop.db", "other\n").await;
    let refusal = answer.expect_err("the guard refuses a tip that drops X's F");
    assert!(refusal.contains("publications.rs"), "{refusal}");
    assert!(!carries(&r, &x), "{refusal}");
    let session = open_session(&r).await.expect("the session at X stays open");
    assert_eq!(session.destination_commit, x);
    assert_eq!(
        conflicting(&session),
        vec![LIST, "publications.rs"],
        "the carried path stays listed beside the residual one, so a repeat carries both"
    );
    assert_no_commit_named_intents(&r).await;
}

// ── cairn/5473: a branch's deletion of a file the base edited ──────────────
