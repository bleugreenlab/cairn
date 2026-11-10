use super::*;
use crate::orchestrator::base_advance::preparation::PreparedBranch;
use crate::pr_data::actions::context::MrContext;
use crate::pr_data::actions::merge::MergeAttemptJournal;
use crate::pr_data::actions::test_support::migrated_db;
use crate::services::testing::{MockGitClient, TestServicesBuilder};
use crate::storage::{LocalDb, SearchIndex};
use cairn_db::turso::params;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

use crate::jj::tests::jj_bin;

fn git(repo: &Path, args: &[&str]) {
    assert!(
        crate::env::git()
            .args(args)
            .current_dir(repo)
            .status()
            .unwrap()
            .success(),
        "git {args:?} failed"
    );
}

fn git_stdout(repo: &Path, args: &[&str]) -> String {
    let out = crate::env::git()
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn init_git_project(repo: &Path) {
    git(repo, &["init", "-q", "-b", "main"]);
    git(repo, &["config", "user.email", "p@e.com"]);
    git(repo, &["config", "user.name", "P"]);
    std::fs::write(repo.join("seed.rs"), "base\n").unwrap();
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "base"]);
}

async fn seed_branch_jobs(
    db: &LocalDb,
    repo_path: &str,
    int_branch: &str,
    int_ws: &str,
    source_branch: &str,
    source_ws: &str,
) {
    let (repo_path, int_branch, source_branch) = (
        repo_path.to_string(),
        int_branch.to_string(),
        source_branch.to_string(),
    );
    let _ = (int_ws, source_ws);
    db.write(move |conn| {
            let (repo_path, int_branch, source_branch) = (
                repo_path.clone(),
                int_branch.clone(),
                source_branch.clone(),
            );
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'proj', ?1, 'main', 1, 1)",
                    params![repo_path.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
                     VALUES ('job-int', 'proj-1', 'running', ?1, 'main', 1, 1)",
                    params![int_branch.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
                     VALUES ('job-src', 'proj-1', 'running', ?1, ?2, 1, 1)",
                    params![source_branch.as_str(), int_branch.as_str()],
                )
                .await?;
                // The pull request the fixture's context lands: the final
                // check reads its target again.
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, title, source_branch,
                                                 target_branch, opened_at, updated_at)
                     VALUES ('mr', 'job-src', 'proj-1', 'PR', ?1, ?2, 1, 1)",
                    params![source_branch.as_str(), int_branch.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// The preparation compute budget every fixture landing and direct preparation
/// carries. These tests drive real jj, which a loaded shared host can slow
/// several times over; production's own bound is pinned by
/// `a_landing_without_an_override_prepares_under_the_production_budget`.
const FIXTURE_PREPARATION_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

/// A project with a bare origin, an integration branch published there, and a
/// child cut from `main` that edits `shared.rs`, wired so landings run end to
/// end: an Orchestrator whose `config_dir` keys the store, the jobs and the
/// pull request seeded, and a `MergeMrContext` for the child→integration
/// merge. TempDirs are kept alive by the struct.
struct MergeFixture {
    _home: TempDir,
    _proj: TempDir,
    _wts: TempDir,
    _origin: TempDir,
    orch: Orchestrator,
    jj: crate::jj::JjEnv,
    store: PathBuf,
    origin_path: PathBuf,
    int: &'static str,
    ctx: MergeMrContext,
}

async fn setup_clean_merge(bin: &str) -> MergeFixture {
    let home_root = tempfile::tempdir().unwrap();
    let proj = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    let origin = tempfile::tempdir().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_git_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);

    // Orchestrator whose config_dir keys the store path `store_merge_child`
    // computes, and whose jj binary matches the one under test.
    let config_dir = home_root.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let db = migrated_db().await;
    let orch = {
        use crate::db::DbState;
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_git(MockGitClient::new())
                .build(),
        );
        Orchestrator::builder(db_state, services, config_dir.clone())
            .jj_binary_path(bin.to_string())
            .landing_preparation_budget(FIXTURE_PREPARATION_BUDGET)
            .build()
    };
    let jj = crate::jj::environment(&orch.jj_binary_path, &orch.config_dir);
    let repo_path = proj.path().to_string_lossy().into_owned();
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Coordinator integration branch, published to origin at its pre-conflict tip.
    let int = "agent/PROJ-1-coordinator-0";
    let ws_coord = wts.path().join("coord");
    crate::jj::add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
    crate::jj::ensure_bookmark_on_origin(&jj, &store, int).unwrap();

    // A child forked from `main` that edits shared.rs.
    let source = "agent/PROJ-2-builder-0";
    let ws_child = wts.path().join("child");
    crate::jj::add_workspace(&jj, &store, &ws_child, source, "main", None).unwrap();
    std::fs::write(ws_child.join("shared.rs"), "child-edit\n").unwrap();
    crate::jj::seal(&jj, &ws_child, "child edits shared", None).unwrap();

    seed_branch_jobs(
        &orch.db.local,
        &repo_path,
        int,
        &ws_coord.to_string_lossy(),
        source,
        &ws_child.to_string_lossy(),
    )
    .await;

    let ctx = MergeMrContext {
        mr: MrContext {
            mr_id: "mr".to_string(),
            pr_url: String::new(),
            github_pr_number: None,
            repo_path: repo_path.clone(),
            job_id: "job-src".to_string(),
            is_local: false,
            project_id: "proj-1".to_string(),
            issue_id: None,
        },
        issue_id: Some("issue".to_string()),
        default_branch: "main".to_string(),
        project_id: "proj-1".to_string(),
        target_branch: int.to_string(),
        source_branch: source.to_string(),
        title: "child PR".to_string(),
        is_workspace: false,
        has_triage_batch: false,
    };

    let origin_path = origin.path().to_path_buf();
    MergeFixture {
        _home: home_root,
        _proj: proj,
        _wts: wts,
        _origin: origin,
        orch,
        jj,
        store,
        origin_path,
        int,
        ctx,
    }
}

/// A landing's reservation of its source and target, shared as preparation
/// inherits it.
async fn reserve_landing_shared(
    fx: &MergeFixture,
) -> std::sync::Arc<crate::orchestrator::BranchReservation> {
    std::sync::Arc::new(reserve_landing(fx).await)
}

/// The child landing's reservation of its source and target.
async fn reserve_landing(fx: &MergeFixture) -> crate::orchestrator::BranchReservation {
    fx.orch
        .reserve_branches(
            &fx.store,
            &[&fx.ctx.source_branch, &fx.ctx.target_branch],
            "test landing",
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap()
}

// ── Landing preparation: the exact source and target a landing binds ────────────────

/// A landing whose orchestrator sets no budget of its own prepares under the
/// automatic preparation bound, so widening the fixture's budget can never
/// widen production's silently.
#[tokio::test]
async fn a_landing_without_an_override_prepares_under_the_production_budget() {
    use crate::orchestrator::base_advance::preparation::DEFAULT_COMPUTE_BUDGET;
    let home = tempfile::tempdir().unwrap();
    let config_dir = home.path().join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let orch = {
        use crate::db::DbState;
        let search = Arc::new(SearchIndex::open_or_create(config_dir.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(migrated_db().await), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        Orchestrator::builder(db_state, services, config_dir).build()
    };
    let ctx = MergeMrContext {
        mr: MrContext {
            mr_id: "mr".to_string(),
            pr_url: String::new(),
            github_pr_number: None,
            repo_path: String::new(),
            job_id: "job-src".to_string(),
            is_local: false,
            project_id: "proj-1".to_string(),
            issue_id: None,
        },
        issue_id: None,
        default_branch: "main".to_string(),
        project_id: "proj-1".to_string(),
        target_branch: "agent/PROJ-1-coordinator-0".to_string(),
        source_branch: "agent/PROJ-2-builder-0".to_string(),
        title: "child PR".to_string(),
        is_workspace: false,
        has_triage_batch: false,
    };

    let request = landing_preparation_request(&orch, "job-src", &ctx);
    assert_eq!(request.compute_budget, DEFAULT_COMPUTE_BUDGET);
    assert_eq!(
        request.compute_budget,
        crate::jj::BACKGROUND_RECONCILE_BUDGET
    );
    assert_eq!(request.purpose, PreparationPurpose::OwnerMerge);
    assert_eq!(
        request.target_override.as_deref(),
        Some(ctx.target_branch.as_str())
    );
    assert_ne!(
        FIXTURE_PREPARATION_BUDGET, DEFAULT_COMPUTE_BUDGET,
        "the fixture's budget is an override, not the production bound"
    );
}

/// Seal `content` into `file` in the fixture workspace `name`, returning the
/// new tip of `bookmark`.
fn seal_in(fx: &MergeFixture, name: &str, bookmark: &str, file: &str, content: &str) -> String {
    let workspace = fx._wts.path().join(name);
    let _ = crate::jj::update_stale(&fx.jj, &workspace);
    std::fs::write(workspace.join(file), content).unwrap();
    crate::jj::seal(&fx.jj, &workspace, &format!("edit {file}"), None).unwrap();
    crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).unwrap()
}

/// Advance `main` in the store through a workspace of its own.
fn advance_main(fx: &MergeFixture, file: &str, content: &str) -> String {
    let line = "agent/main-line";
    let workspace = fx._wts.path().join("main-line");
    if !workspace.exists() {
        crate::jj::add_workspace(&fx.jj, &fx.store, &workspace, line, "main", None).unwrap();
    }
    let tip = seal_in(fx, "main-line", line, file, content);
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                "main",
                "-r",
                &tip,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "advance main",
        )
        .unwrap();
    tip
}

fn parents_of(fx: &MergeFixture, commit: &str) -> Vec<String> {
    fx.jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                commit,
                "--no-graph",
                "-T",
                "parents.map(|c| c.commit_id()).join(\" \")",
                "--ignore-working-copy",
            ],
            "parents",
        )
        .unwrap()
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

#[test]
fn a_default_fold_never_replays_or_treats_unknown_history_as_clean() {
    let source = include_str!("mod.rs")
        .split_once("async fn fold_onto_exact_target(")
        .unwrap()
        .1;
    assert!(!source.contains("rebase_branch_onto("));
    assert!(!source.contains("unwrap_or(false)"));
    assert!(!source.contains("falling back to tip"));
    assert!(source.contains("source_head != exact.source"));
}

/// The final comparison before publication catches an owner commit and a
/// target move made after preparation, each on its own, and holds otherwise.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_prepared_landing_stops_holding_when_its_source_or_target_moves() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let holds = |prepared: &PreparedBranch| {
        let (orch, ctx, store) = (&fx.orch, &fx.ctx, fx.store.clone());
        let prepared = prepared.clone();
        async move {
            let guard = orch
                .acquire_jj_store_lock(
                    &store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "landing recheck test",
                )
                .await;
            prepared_landing_holds(orch, ctx, &prepared, &guard)
                .await
                .unwrap()
        }
    };

    let prepared = prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    .unwrap();
    assert!(holds(&prepared).await, "nothing moved");

    seal_in(&fx, "child", &fx.ctx.source_branch, "later.rs", "owner\n");
    assert!(
        !holds(&prepared).await,
        "an owner commit after preparation is a moved source"
    );

    let prepared = prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    .unwrap();
    assert!(holds(&prepared).await);
    seal_in(&fx, "coord", fx.int, "hub.rs", "hub\n");
    assert!(
        !holds(&prepared).await,
        "a target move after preparation is a moved target"
    );

    let again = prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    .unwrap();
    assert_eq!(
        again.base_commit,
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap(),
        "preparing again takes the moved target"
    );
    assert!(holds(&again).await);
}

/// A default landing whose source forked before the target advanced is
/// prepared onto the advanced target and lands as exactly one commit on it.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_local_default_landing_lands_exactly_onto_the_prepared_target() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = advance_main(&fx, "incoming.rs", "incoming\n");
    retarget(&fx, "main").await;
    let mut ctx = fx.ctx.clone();
    ctx.target_branch = "main".to_string();
    ctx.mr.is_local = true;
    let journal = MergeAttemptJournal {
        attempt_id: "attempt",
        project_id: "proj-1",
        merge_request_id: "mr",
        job_id: "job-src",
        target_ref: "refs/heads/main",
        source_ref: "refs/heads/source",
        policy_json: "{}",
    };

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &journal,
    )
    .await
    .expect("a clean stale source lands");

    // The landing hands its reservation on: the source stays reserved until
    // the caller records the merge, so no child can land into it meanwhile.
    let try_source = || {
        let (orch, store, source) = (fx.orch.clone(), fx.store.clone(), ctx.source_branch.clone());
        async move {
            tokio::spawn(async move {
                orch.reserve_branches(
                    &store,
                    &[&source],
                    "child landing",
                    std::time::Duration::from_millis(300),
                )
                .await
                .map(|_| ())
            })
            .await
            .unwrap()
        }
    };
    let mut landed = landed;
    assert!(landed.reservation.is_some());
    assert!(
        try_source().await.is_err(),
        "the landed source is still reserved until the merge is recorded"
    );
    landed.reservation = None;
    assert!(
        try_source().await.is_ok(),
        "released once the caller lets go"
    );
    let commit = landed.landed_commit.expect("the fold names its commit");
    assert!(
        landed.published.is_none(),
        "a local project publishes nothing"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(commit.as_str())
    );
    assert_eq!(
        parents_of(&fx, &commit),
        vec![target.clone()],
        "one commit whose parent is exactly the prepared target"
    );
    for (file, content) in [("incoming.rs", "incoming\n"), ("shared.rs", "child-edit\n")] {
        assert_eq!(
            crate::jj::text_at_revision(&fx.jj, &fx.store, &commit, file)
                .unwrap()
                .as_deref(),
            Some(content),
            "{file} is the combined tree's"
        );
    }
}

// ── Remote default landings: two-phase, leased publication ──────────────────────────

use crate::pr_data::actions::default_landing::publication_barrier;

async fn remote_default_context(fx: &MergeFixture) -> MergeMrContext {
    retarget(fx, "main").await;
    let mut ctx = fx.ctx.clone();
    ctx.target_branch = "main".to_string();
    ctx.mr.is_local = false;
    ctx
}

/// Point the fixture's pull request at `target`.
async fn retarget(fx: &MergeFixture, target: &str) {
    fx.orch
        .db
        .local
        .execute(
            "UPDATE merge_requests SET target_branch = ?1 WHERE id = 'mr'",
            (target.to_string(),),
        )
        .await
        .unwrap();
}

const JOURNAL: MergeAttemptJournal<'static> = MergeAttemptJournal {
    attempt_id: "attempt",
    project_id: "proj-1",
    merge_request_id: "mr",
    job_id: "job-src",
    target_ref: "refs/heads/main",
    source_ref: "refs/heads/source",
    policy_json: "{}",
};

/// `attempt:sequence:state` for every journaled event, in order.
async fn journal_events(fx: &MergeFixture) -> String {
    fx.orch
        .db
        .local
        .query_opt_text(
            "SELECT group_concat(event, ',') FROM (
               SELECT attempt_id || ':' || sequence || ':' || state AS event
                 FROM merge_attempt_events ORDER BY attempt_id, sequence)",
            (),
        )
        .await
        .unwrap()
        .unwrap_or_default()
}

/// While a landing's push is pending the store lock is free, the store still
/// names the prepared target (no other branch can prepare onto an unaccepted
/// landing), the landing's source cannot be moved, and an unrelated branch
/// can. Once origin accepts, origin and the store both hold one commit whose
/// sole parent is the prepared target, and its intent was journaled first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_remote_landing_moves_the_store_only_after_origin_accepts_and_blocks_only_its_branches() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let ctx = remote_default_context(&fx).await;
    let target = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), target);

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    publication_barrier::set(&fx.store, move || {
        let _ = reached_tx.send(());
        let _ = go_rx.recv();
    });
    let during = {
        let (orch, store, jj) = (fx.orch.clone(), fx.store.clone(), fx.jj.clone());
        let (source, target) = (ctx.source_branch.clone(), target.clone());
        tokio::spawn(async move {
            tokio::task::spawn_blocking(move || reached_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let main = crate::jj::bookmark_commit(&jj, &store, "main");
            let lock_free = orch
                .acquire_jj_store_lock_with_timeout(
                    &store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "another writer during the push",
                    Some(std::time::Duration::from_secs(5)),
                )
                .await
                .is_ok();
            let wait = std::time::Duration::from_millis(300);
            let source_refused = orch
                .reserve_branches(&store, &[&source], "owner write", wait)
                .await
                .err();
            let unrelated = orch
                .reserve_branches(&store, &["agent/unrelated"], "other write", wait)
                .await
                .is_ok();
            go_tx.send(()).unwrap();
            assert_eq!(
                main.as_deref(),
                Some(target.as_str()),
                "the store names the prepared target until origin accepts"
            );
            assert!(lock_free, "no network I/O runs under the store lock");
            assert!(
                source_refused.is_some_and(|error| error.contains("landing")),
                "the landing's source cannot move while its push is pending"
            );
            assert!(unrelated, "an unrelated branch is not held up");
        })
    };

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await;
    publication_barrier::clear(&fx.store);
    during.await.unwrap();
    let landed = landed.expect("a clean remote landing lands");

    let commit = landed.landed_commit.expect("the landing names its commit");
    let published = landed.published.expect("a remote landing is published");
    assert_eq!(
        published.publication.shape,
        PublicationShape::Squash,
        "a squash landing journals the one commit it minted"
    );
    assert_eq!(published.publication.expected_old, target);
    assert_eq!(published.publication.landing_commit, commit);
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), commit);
    assert_eq!(parents_of(&fx, &commit), vec![target]);
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(commit.as_str())
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, &ctx.source_branch).as_deref(),
        Some(commit.as_str()),
        "the source moves with the target, so the landing contains it"
    );
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing",
        "the intent precedes the push; the outcome is the merge record's"
    );
}

// ── Child landings onto an integration branch ─────────────────────────────────────────

use crate::pr_data::actions::landing_record::PublicationShape;

// ── Landing admission: debts owed by the landing's branches ────────────────────────────

/// Record, the way the acceptance journal does, that `branch` owes the
/// acceptance of an install from `head_before` to `head_after`, in `status`.
async fn owe(
    fx: &MergeFixture,
    branch: &str,
    status: &str,
    head_before: &str,
    head_after: &str,
) -> String {
    let id = format!("acceptance-{}", uuid::Uuid::new_v4());
    let key = crate::orchestrator::base_advance::preparation::journal_store_key(&fx.store).unwrap();
    fx.orch
        .db
        .local
        .execute(
            "INSERT INTO branch_advance_acceptances
               (id, project_id, store_path, branch, head_before, head_after, base_branch,
                base_commit, captured_operation, dropped_commits_json, inherited_drops_json,
                status, created_at)
             VALUES (?1, 'proj-1', ?2, ?3, ?4, ?5, 'main', ?4, 'op', '[]', '[]', ?6, 1)",
            params![
                id.as_str(),
                key.as_str(),
                branch,
                head_before,
                head_after,
                status
            ],
        )
        .await
        .unwrap();
    id
}

/// A landing whose target still owes an earlier install's acceptance, in a
/// state settlement cannot resolve (unknown), is refused before anything
/// moves: no fold, no push, and both the store's and origin's target unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_landing_onto_a_target_owing_an_unknown_acceptance_is_refused_before_anything_moves() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let ctx = remote_default_context(&fx).await;
    let target = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let source = crate::jj::bookmark_commit(&fx.jj, &fx.store, &ctx.source_branch).unwrap();
    let owed = owe(&fx, "main", "unknown", &"1".repeat(40), &"2".repeat(40)).await;

    let refused = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await
    .expect_err("a target owing an unsettled acceptance takes no landing");

    assert!(
        refused.contains(&owed) || refused.contains("unknown"),
        "the refusal names the debt: {refused}"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(target.as_str()),
        "the store's target did not move"
    );
    assert_eq!(
        git_stdout(&fx.origin_path, &["rev-parse", "main"]),
        target,
        "nothing was pushed"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, &ctx.source_branch).as_deref(),
        Some(source.as_str()),
        "the source was not prepared or folded either"
    );
    assert_eq!(
        journal_events(&fx).await,
        "",
        "no publication was attempted"
    );
}

// ── Landing acceptance: refusal, external movement, conflict wakes ────────────────────
