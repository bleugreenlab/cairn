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

/// A landing prepares its source onto the branch it ACTUALLY lands on, not the
/// one its job declares: a child whose job says `main` but whose pull request
/// targets the integration branch is carried onto the integration tip.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn landing_preparation_carries_a_stale_source_onto_the_actual_target() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let before = crate::jj::bookmark_commit(&fx.jj, &fx.store, &fx.ctx.source_branch).unwrap();
    let target = seal_in(&fx, "coord", fx.int, "hub.rs", "hub\n");
    fx.orch
        .db
        .local
        .execute(
            "UPDATE jobs SET base_branch = 'main' WHERE id = 'job-src'",
            (),
        )
        .await
        .unwrap();

    let prepared = prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    .expect("a clean source prepares onto its landing target");

    assert_eq!(prepared.relationship.base_branch, fx.int);
    assert_eq!(prepared.relationship.declared.as_deref(), Some("main"));
    assert_eq!(prepared.base_commit, target);
    assert_eq!(prepared.head_before, before);
    assert!(prepared.advanced);
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, &fx.ctx.source_branch).as_deref(),
        Some(prepared.head_after.as_str())
    );
    assert!(
        crate::jj::revset_commit(
            &fx.jj,
            &fx.store,
            &format!("{target} & ::{}", prepared.head_after)
        )
        .is_some(),
        "the prepared head descends from the exact target"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).as_deref(),
        Some(target.as_str()),
        "preparing never moves the target"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fold_never_catches_up_a_source_missing_its_exact_target() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let source = crate::jj::bookmark_commit(&fx.jj, &fx.store, &fx.ctx.source_branch).unwrap();
    let target = seal_in(&fx, "coord", fx.int, "hub.rs", "new target\n");
    let reservation = reserve_landing(&fx).await;
    let guard = fx
        .orch
        .acquire_jj_store_lock(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "unprepared fold test",
        )
        .await;
    let operation = cairn_vcs::settled_operation(&fx.store).unwrap();

    for method in ["squash", "merge"] {
        let error = fold_onto_exact_target(
            &fx.orch,
            &fx.ctx,
            method,
            ExactTarget {
                target: &target,
                source: &source,
                tree: None,
            },
            &guard,
            &reservation,
        )
        .await
        .unwrap_err();
        assert!(
            error.contains("does not contain the exact target"),
            "{error}"
        );
        assert_eq!(cairn_vcs::settled_operation(&fx.store).unwrap(), operation);
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, &fx.ctx.source_branch).as_deref(),
            Some(source.as_str()),
            "a refused fold never catches up the source"
        );
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).as_deref(),
            Some(target.as_str()),
            "a refused fold never advances the target"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fold_refuses_a_source_that_moved_past_its_prepared_head() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = seal_in(&fx, "coord", fx.int, "hub.rs", "new target\n");
    let reservation = reserve_landing_shared(&fx).await;
    let prepared = prepare_landing_source(&fx.orch, "job-src", &fx.ctx, &reservation)
        .await
        .unwrap();
    assert_eq!(prepared.base_commit, target);
    let guard = fx
        .orch
        .acquire_jj_store_lock(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "moved fold test",
        )
        .await;
    let source = crate::jj::publish_logical_head_exported(
        &fx.jj,
        &fx.store,
        &fx.ctx.source_branch,
        &prepared.head_after,
        crate::jj::ProposedPublication::Mutations(vec![cairn_vcs::LogicalTreeMutation {
            path: "unjudged.rs".into(),
            content: Some(b"later owner commit\n".to_vec()),
        }]),
        None,
        cairn_vcs::PublicationMode::Child {
            description: "later owner commit".into(),
        },
    )
    .await
    .unwrap()
    .landed
    .head;
    let operation = cairn_vcs::settled_operation(&fx.store).unwrap();
    let error = fold_onto_exact_target(
        &fx.orch,
        &fx.ctx,
        "squash",
        ExactTarget {
            target: &target,
            source: &prepared.head_after,
            tree: None,
        },
        &guard,
        &reservation,
    )
    .await
    .unwrap_err();
    assert!(error.contains("prepared source moved"), "{error}");
    let binding = crate::orchestrator::landing::bind::bind_onto(
        &fx.jj,
        &fx.store,
        &fx.ctx.source_branch,
        &prepared.head_after,
        &target,
        fx.int,
        &[],
    )
    .unwrap_err();
    assert!(binding.contains("prepared source moved"), "{binding}");
    let unchanged = crate::orchestrator::landing::bind::bind_onto(
        &fx.jj,
        &fx.store,
        &fx.ctx.source_branch,
        &source,
        &target,
        fx.int,
        &[],
    )
    .unwrap();
    assert_eq!(unchanged.head, source);
    assert_eq!(cairn_vcs::settled_operation(&fx.store).unwrap(), operation);
    assert_eq!(
        cairn_vcs::resolve_coordinate(&fx.store, &fx.ctx.source_branch)
            .await
            .unwrap(),
        source
    );
    assert_eq!(
        cairn_vcs::resolve_coordinate(&fx.store, fx.int)
            .await
            .unwrap(),
        target
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fully_absorbed_source_needs_no_fold_or_new_commit() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = cairn_vcs::resolve_coordinate(&fx.store, fx.int)
        .await
        .unwrap();
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                &fx.ctx.source_branch,
                "-r",
                &target,
                "--allow-backwards",
                "--ignore-working-copy",
            ],
            "model an absorbed source",
        )
        .unwrap();
    let reservation = reserve_landing(&fx).await;
    let guard = fx
        .orch
        .acquire_jj_store_lock(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "absorbed fold test",
        )
        .await;
    let operation = cairn_vcs::settled_operation(&fx.store).unwrap();
    let result = fold_onto_exact_target(
        &fx.orch,
        &fx.ctx,
        "squash",
        ExactTarget {
            target: &target,
            source: &target,
            tree: None,
        },
        &guard,
        &reservation,
    )
    .await
    .unwrap();
    assert_eq!(result, target);
    assert_eq!(cairn_vcs::settled_operation(&fx.store).unwrap(), operation);
    assert_eq!(
        cairn_vcs::resolve_coordinate(&fx.store, &fx.ctx.source_branch)
            .await
            .unwrap(),
        target
    );
}

/// A recovery fence on `bookmark` at `tip`, as an unverified rollback leaves.
fn fence_coordinates(
    scope: crate::jj::RecoveryScope,
    bookmark: &str,
    tip: &str,
) -> crate::jj::RecoveryCoordinates {
    crate::jj::RecoveryCoordinates {
        scope,
        phase: crate::jj::RollbackFailurePhase::ExportUnverified,
        pre_op: Some("0".repeat(40)),
        pre_view: None,
        pre_git: None,
        failed_view: None,
        bookmarks: vec![crate::jj::BookmarkRecovery {
            bookmark: bookmark.to_string(),
            pre: cairn_vcs::BookmarkState::normal(tip),
            pre_git: Some(tip.to_string()),
            failed: cairn_vcs::BookmarkState::normal(tip),
        }],
        survivors: Vec::new(),
        evidence: vec!["test fence".into()],
    }
}

/// A prepared landing whose source is fenced after preparation is refused
/// under the landing guard, and the store's target does not move. A store
/// fence refuses a landing between two unfenced branches too.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_fenced_branch_is_never_folded_by_a_landing() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let reservation = reserve_landing_shared(&fx).await;
    let prepared = prepare_landing_source(&fx.orch, "job-src", &fx.ctx, &reservation)
        .await
        .unwrap();
    let target_before = crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap();
    let source_tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, &fx.ctx.source_branch).unwrap();
    let guard = fx
        .orch
        .acquire_jj_store_lock(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "fenced fold test",
        )
        .await;
    let exact = ExactTarget {
        target: &prepared.base_commit,
        source: &prepared.head_after,
        tree: None,
    };

    let records = crate::orchestrator::branch_recovery::record_recoveries(
        &fx.orch.db.local,
        "proj-1",
        &fx.store,
        Some(&fx.ctx.source_branch),
        crate::orchestrator::branch_recovery::BUDGET_AUTOMATIC,
        "test",
        vec![fence_coordinates(
            crate::jj::RecoveryScope::Branch,
            &fx.ctx.source_branch,
            &source_tip,
        )],
    )
    .await
    .unwrap();
    let refusal = fold_onto_exact_target(&fx.orch, &fx.ctx, "squash", exact, &guard, &reservation)
        .await
        .unwrap_err();
    assert!(refusal.contains(&records[0].id), "{refusal}");
    assert!(
        refusal.contains("Merging this pull request refused"),
        "{refusal}"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap(),
        target_before
    );

    // A store fence on an unrelated bookmark still refuses the landing.
    fx.orch
        .db
        .local
        .execute("DELETE FROM jj_branch_recoveries", ())
        .await
        .unwrap();
    crate::orchestrator::branch_recovery::record_recoveries(
        &fx.orch.db.local,
        "proj-1",
        &fx.store,
        None,
        crate::orchestrator::branch_recovery::BUDGET_AUTOMATIC,
        "test",
        vec![fence_coordinates(
            crate::jj::RecoveryScope::Store,
            "agent/unrelated",
            &source_tip,
        )],
    )
    .await
    .unwrap();
    let refusal = fold_onto_exact_target(&fx.orch, &fx.ctx, "squash", exact, &guard, &reservation)
        .await
        .unwrap_err();
    assert!(refusal.contains("store scope"), "{refusal}");
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap(),
        target_before
    );

    // With the fence repaired, the same prepared landing folds.
    fx.orch
        .db
        .local
        .execute("UPDATE jj_branch_recoveries SET status = 'repaired'", ())
        .await
        .unwrap();
    let folded = fold_onto_exact_target(&fx.orch, &fx.ctx, "squash", exact, &guard, &reservation)
        .await
        .expect("an unfenced landing folds");
    assert_ne!(folded, target_before);
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

/// Push a commit to origin's `main` from a clone, as a merge made outside
/// Cairn would, returning it.
fn race_origin_main(origin: &Path, scratch: &Path) -> String {
    let clone = scratch.join("outside");
    git(
        scratch,
        &["clone", "-q", &origin.to_string_lossy(), "outside"],
    );
    std::fs::write(clone.join("outside.rs"), "outside\n").unwrap();
    git(&clone, &["add", "-A"]);
    git(
        &clone,
        &[
            "-c",
            "user.email=o@e.com",
            "-c",
            "user.name=O",
            "commit",
            "-q",
            "-m",
            "merged outside",
        ],
    );
    git(&clone, &["push", "-q", "origin", "HEAD:main"]);
    git_stdout(&clone, &["rev-parse", "HEAD"])
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

/// A workspace pull request with a real remote number (a thread-branch PR)
/// keeps its own commits: its default landing fast-forwards origin's target to
/// the prepared head and journals that shape, so recovery and refresh judge
/// origin's history by what was published (`landing_record`'s
/// `a_preserved_history_landing_is_recovered_by_its_fast_forward_shape`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_remote_workspace_landing_journals_the_fast_forward_it_published() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let mut ctx = remote_default_context(&fx).await;
    ctx.is_workspace = true;
    ctx.mr.github_pr_number = Some(43);
    let target = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let first = crate::jj::bookmark_commit(&fx.jj, &fx.store, &ctx.source_branch).unwrap();
    let head = seal_in(&fx, "child", &ctx.source_branch, "second.rs", "second\n");
    assert_ne!(first, head, "the source carries two commits of its own");

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "merge", None, &JOURNAL,
    )
    .await
    .expect("a clean remote workspace landing lands");

    let commit = landed.landed_commit.expect("the landing names its commit");
    let published = landed.published.expect("a remote landing is published");
    assert_eq!(
        commit, head,
        "the preserved history's own tip is the landing"
    );
    assert_eq!(parents_of(&fx, &commit), vec![first.clone()]);
    assert_eq!(parents_of(&fx, &first), vec![target.clone()]);
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), commit);
    assert_eq!(published.publication.shape, PublicationShape::FastForward);
    assert_eq!(published.publication.expected_old, target);
    assert_eq!(published.publication.landing_commit, commit);
    let intent = fx
        .orch
        .db
        .local
        .query_opt_text(
            "SELECT detail FROM merge_attempt_events WHERE state = 'executing'",
            (),
        )
        .await
        .unwrap()
        .unwrap_or_default();
    assert!(
        intent.contains("\"shape\":\"fastForward\""),
        "the journaled intent names the fast-forward: {intent}"
    );
}

/// A merge made outside Cairn while the push is pending moves origin's
/// target: the leased push is refused with nothing published and nothing in
/// the store to undo, and the landing prepares, validates and lands again on
/// the target origin now holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_target_moved_during_the_push_is_refused_then_landed_on_the_new_target() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let ctx = remote_default_context(&fx).await;
    let target = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let raced = Arc::new(std::sync::Mutex::new(None::<String>));
    let main_during_first_push = Arc::new(std::sync::Mutex::new(None::<String>));
    {
        let (origin, scratch) = (fx.origin_path.clone(), scratch.path().to_path_buf());
        let (raced, seen) = (raced.clone(), main_during_first_push.clone());
        let (jj, store) = (fx.jj.clone(), fx.store.clone());
        publication_barrier::set(&fx.store, move || {
            let mut raced = raced.lock().unwrap();
            if raced.is_none() {
                *seen.lock().unwrap() = crate::jj::bookmark_commit(&jj, &store, "main");
                *raced = Some(race_origin_main(&origin, &scratch));
            }
        });
    }

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await;
    publication_barrier::clear(&fx.store);
    let landed = landed.expect("the landing lands on the moved target");
    let raced = raced.lock().unwrap().clone().expect("the race ran");

    assert_eq!(
        main_during_first_push.lock().unwrap().as_deref(),
        Some(target.as_str()),
        "the refused landing was never the store's target"
    );
    let commit = landed.landed_commit.unwrap();
    assert_eq!(
        parents_of(&fx, &commit),
        vec![raced.clone()],
        "landed on exactly the target origin moved to"
    );
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), commit);
    assert_eq!(
        crate::jj::text_at_revision(&fx.jj, &fx.store, &commit, "outside.rs")
            .unwrap()
            .as_deref(),
        Some("outside\n"),
        "the outside merge is kept"
    );
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing,attempt-publication-1:2:failed,\
         attempt-publication-2:1:executing",
        "each try journals its own intent; the refused one its failure"
    );
}

/// A pull request retargeted after its merge read it (its integration branch
/// merged, so it now points at `main`) is refused at the final check before
/// anything moves, rather than landing where it used to point.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_landing_whose_pull_request_was_retargeted_is_refused_before_publishing() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let prepared = prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    .unwrap();
    let target_before = crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap();
    retarget(&fx, "main").await;

    let guard = fx
        .orch
        .acquire_jj_store_lock(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "retarget test",
        )
        .await;
    let refused = prepared_landing_holds(&fx.orch, &fx.ctx, &prepared, &guard)
        .await
        .expect_err("a retargeted pull request is refused, not retried onto its old target");
    drop(guard);
    assert!(refused.contains("now targets `main`"), "{refused}");
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).as_deref(),
        Some(target_before.as_str()),
        "nothing landed into the old target"
    );
}

/// A child whose integration branch has already merged is refused at the
/// final check even while its pull request still names that branch (its
/// re-point failed or has not run): it never lands into a landed branch.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(jj)]
async fn a_child_is_refused_from_landing_into_an_integration_branch_that_already_merged() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let prepared = prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    .unwrap();
    fx.orch
        .db
        .local
        .execute(
            "INSERT INTO merge_requests (id, job_id, project_id, title, source_branch,
                                         target_branch, status, opened_at, updated_at)
             VALUES ('mr-int', 'job-int', 'proj-1', 'Integration', ?1, 'main', 'merged', 1, 1)",
            (fx.int.to_string(),),
        )
        .await
        .unwrap();

    let guard = fx
        .orch
        .acquire_jj_store_lock(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "landed target test",
        )
        .await;
    let refused = prepared_landing_holds(&fx.orch, &fx.ctx, &prepared, &guard)
        .await
        .expect_err("a landed integration branch takes nothing more");
    assert!(refused.contains("has itself already merged"), "{refused}");
}

/// An integration branch X lands on `main`, and its children's re-point
/// failed: the child's pull request still targets X. Until the merge is
/// recorded, X stays reserved by the landing, so the child cannot land; once
/// X's pull request reads merged, the final check refuses the child. At no
/// point can it land into the already-landed X.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_child_of_a_landed_branch_whose_repoint_failed_can_never_land_into_it() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    seal_in(&fx, "coord", fx.int, "hub.rs", "hub\n");
    fx.orch
        .db
        .local
        .execute(
            "INSERT INTO merge_requests (id, job_id, project_id, title, source_branch,
                                         target_branch, opened_at, updated_at)
             VALUES ('mr-int', 'job-int', 'proj-1', 'Integration', ?1, 'main', 1, 1)",
            (fx.int.to_string(),),
        )
        .await
        .unwrap();
    let mut parent = fx.ctx.clone();
    parent.source_branch = fx.int.to_string();
    parent.target_branch = "main".to_string();
    parent.mr.mr_id = "mr-int".to_string();
    parent.mr.job_id = "job-int".to_string();
    parent.mr.is_local = true;

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-int", &parent, "squash", None, &JOURNAL,
    )
    .await
    .expect("the integration branch lands");
    // The re-point "failed": the child still names X.
    retarget(&fx, fx.int).await;
    fx.orch
        .db
        .local
        .execute(
            "UPDATE jobs SET base_branch = ?1 WHERE id = 'job-src'",
            (fx.int.to_string(),),
        )
        .await
        .unwrap();

    // Between the landing returning and the merge being recorded.
    let child = {
        let (orch, store) = (fx.orch.clone(), fx.store.clone());
        let names = [fx.ctx.source_branch.clone(), fx.int.to_string()];
        tokio::spawn(async move {
            orch.reserve_branches(
                &store,
                &[&names[0], &names[1]],
                "child landing",
                std::time::Duration::from_millis(300),
            )
            .await
            .map(|_| ())
        })
        .await
        .unwrap()
    };
    assert!(
        child.is_err_and(|error| error.contains("landing")),
        "the child waits behind the landed branch's reservation"
    );

    // The merge is recorded, then the reservation goes (merge.rs's tail order).
    fx.orch
        .db
        .local
        .execute(
            "UPDATE merge_requests SET status = 'merged' WHERE id = 'mr-int'",
            (),
        )
        .await
        .unwrap();
    drop(landed);

    let refused = match prepare_landing_source(
        &fx.orch,
        "job-src",
        &fx.ctx,
        &reserve_landing_shared(&fx).await,
    )
    .await
    {
        Err(refused) => refused,
        Ok(prepared) => {
            let guard = fx
                .orch
                .acquire_jj_store_lock(
                    &fx.store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "child final check",
                )
                .await;
            prepared_landing_holds(&fx.orch, &fx.ctx, &prepared, &guard)
                .await
                .expect_err("a child never lands into its already-landed integration branch")
        }
    };
    assert!(refused.contains("already merged"), "{refused}");
}

// ── Child landings onto an integration branch ─────────────────────────────────────────

use crate::pr_data::actions::child_landing::land_on_integration;
use crate::pr_data::actions::landing_record::PublicationShape;

/// Advance the integration branch past the child's fork point and publish it,
/// so the child is behind its target (its landing must really prepare it) and
/// the store's integration branch is exactly origin's. Returns its tip.
fn advance_and_publish_integration(fx: &MergeFixture) -> String {
    let tip = seal_in(fx, "coord", fx.int, "hub.rs", "hub\n");
    crate::jj::push_store_bookmark(&fx.jj, &fx.store, fx.int).unwrap();
    assert_eq!(origin_ref(fx, fx.int).as_deref(), Some(tip.as_str()));
    tip
}

/// Origin's `branch`, or `None` when origin has no such branch.
fn origin_ref(fx: &MergeFixture, branch: &str) -> Option<String> {
    let out = crate::env::git()
        .args([
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("refs/heads/{branch}"),
        ])
        .current_dir(&fx.origin_path)
        .output()
        .unwrap();
    let commit = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (out.status.success() && !commit.is_empty()).then_some(commit)
}

/// The commit the landing pin of `attempt_id` names, if it exists.
fn landing_pin(fx: &MergeFixture, attempt_id: &str) -> Option<String> {
    let pin = crate::pr_data::actions::default_landing::landing_pin_ref(attempt_id);
    let out = crate::env::git()
        .args(["rev-parse", "--verify", "--quiet", &pin])
        .current_dir(&fx.ctx.mr.repo_path)
        .output()
        .unwrap();
    let commit = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (out.status.success() && !commit.is_empty()).then_some(commit)
}

/// Whether another writer could reserve `branches` now, from its own task.
async fn try_reserve(fx: &MergeFixture, branches: &[&str]) -> Result<(), String> {
    let (orch, store) = (fx.orch.clone(), fx.store.clone());
    let branches: Vec<String> = branches.iter().map(|branch| branch.to_string()).collect();
    tokio::spawn(async move {
        let names: Vec<&str> = branches.iter().map(String::as_str).collect();
        orch.reserve_branches(
            &store,
            &names,
            "another writer",
            std::time::Duration::from_millis(300),
        )
        .await
        .map(|_| ())
    })
    .await
    .unwrap()
}

/// Push a commit to origin's `branch` from a clone, as a push made outside
/// Cairn would, returning it.
fn race_origin_branch(origin: &Path, scratch: &Path, branch: &str, file: &str) -> String {
    let clone = scratch.join(format!("outside-{file}"));
    git(
        scratch,
        &[
            "clone",
            "-q",
            "-b",
            branch,
            &origin.to_string_lossy(),
            &clone.to_string_lossy(),
        ],
    );
    std::fs::write(clone.join(file), "outside\n").unwrap();
    git(&clone, &["add", "-A"]);
    git(
        &clone,
        &[
            "-c",
            "user.email=o@e.com",
            "-c",
            "user.name=O",
            "commit",
            "-q",
            "-m",
            "pushed outside",
        ],
    );
    git(&clone, &["push", "-q", "origin", &format!("HEAD:{branch}")]);
    git_stdout(&clone, &["rev-parse", "HEAD"])
}

fn store_ref(fx: &MergeFixture, branch: &str) -> Option<String> {
    crate::jj::bookmark_commit(&fx.jj, &fx.store, branch)
}

/// A child behind its integration branch, in a project with no remote, is
/// prepared onto the integration tip by the canonical preparer (under the
/// landing's own reservation) and lands as a fast-forward of the integration
/// branch to the prepared head: its own commit, on exactly the prepared
/// target. The reservation is handed to the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_local_child_landing_fast_forwards_its_integration_branch_to_the_prepared_head() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = seal_in(&fx, "coord", fx.int, "hub.rs", "hub\n");
    let source_before = store_ref(&fx, &fx.ctx.source_branch).unwrap();
    let mut ctx = fx.ctx.clone();
    ctx.mr.is_local = true;
    let db = fx.orch.db.local.clone();

    let mut landed = land_on_integration(&fx.orch, "job-src", &db, &ctx, &JOURNAL)
        .await
        .expect("a clean child behind its target lands");

    let commit = landed
        .landed_commit
        .clone()
        .expect("the landing names its commit");
    assert!(
        landed.published.is_none(),
        "a local project publishes nothing"
    );
    assert_ne!(commit, source_before, "the stale child was prepared first");
    assert_eq!(store_ref(&fx, fx.int).as_deref(), Some(commit.as_str()));
    assert_eq!(
        store_ref(&fx, &ctx.source_branch).as_deref(),
        Some(commit.as_str()),
        "the integration branch fast-forwarded to the prepared head itself"
    );
    assert_eq!(
        parents_of(&fx, &commit),
        vec![target],
        "the child's own commit, on exactly the prepared target"
    );
    for (file, content) in [("hub.rs", "hub\n"), ("shared.rs", "child-edit\n")] {
        assert_eq!(
            crate::jj::text_at_revision(&fx.jj, &fx.store, &commit, file)
                .unwrap()
                .as_deref(),
            Some(content),
            "{file} is the combined tree's"
        );
    }
    assert_eq!(
        journal_events(&fx).await,
        "",
        "nothing was journaled as published"
    );

    assert!(
        try_reserve(&fx, &[fx.int]).await.is_err(),
        "the integration branch stays reserved until the caller records the merge"
    );
    landed.reservation = None;
    assert!(try_reserve(&fx, &[fx.int, &ctx.source_branch])
        .await
        .is_ok());
}

/// A remote child landing publishes source and target to the prepared head in
/// ONE push, with no store lock held while it is pending. Meanwhile the store
/// still names the prepared target and head (no other branch can prepare onto
/// an unaccepted landing), the landing's source and target cannot be
/// reserved, an unrelated branch can, and origin holds neither ref yet. Once
/// origin accepts, origin and the store agree on one commit, the intent was
/// journaled first, and the pin is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_remote_child_landing_publishes_source_and_target_in_one_push_and_blocks_only_its_branches(
) {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = advance_and_publish_integration(&fx);
    let source = fx.ctx.source_branch.clone();
    assert_eq!(
        origin_ref(&fx, &source),
        None,
        "the child was never published"
    );

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    publication_barrier::set(&fx.store, move || {
        let _ = reached_tx.send(());
        let _ = go_rx.recv();
    });
    let during = {
        let (orch, store, jj) = (fx.orch.clone(), fx.store.clone(), fx.jj.clone());
        let (source, int, target) = (source.clone(), fx.int.to_string(), target.clone());
        let origin = fx.origin_path.clone();
        tokio::spawn(async move {
            tokio::task::spawn_blocking(move || reached_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let int_now = crate::jj::bookmark_commit(&jj, &store, &int);
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
                .reserve_branches(&store, &[source.as_str()], "owner write", wait)
                .await
                .err();
            let target_refused = orch
                .reserve_branches(&store, &[int.as_str()], "coordinator write", wait)
                .await
                .err();
            let unrelated = orch
                .reserve_branches(&store, &["agent/unrelated"], "other write", wait)
                .await
                .is_ok();
            let origin_int = git_stdout(&origin, &["rev-parse", &int]);
            go_tx.send(()).unwrap();
            assert_eq!(
                int_now.as_deref(),
                Some(target.as_str()),
                "the store names the prepared target until origin accepts"
            );
            assert_eq!(origin_int, target, "nothing reached origin before the push");
            assert!(lock_free, "no network I/O runs under the store lock");
            assert!(
                source_refused.is_some_and(|error| error.contains("landing")),
                "the child cannot move while its push is pending"
            );
            assert!(
                target_refused.is_some_and(|error| error.contains("landing")),
                "the integration branch cannot move while the push is pending"
            );
            assert!(unrelated, "an unrelated branch is not held up");
        })
    };

    let db = fx.orch.db.local.clone();
    let landed = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL).await;
    publication_barrier::clear(&fx.store);
    during.await.unwrap();
    let landed = landed.expect("a clean remote child landing lands");

    let commit = landed.landed_commit.expect("the landing names its commit");
    let published = landed.published.expect("a remote landing is published");
    assert_eq!(published.publication.shape, PublicationShape::FastForward);
    assert_eq!(published.publication.expected_old, target);
    assert_eq!(published.publication.landing_commit, commit);
    assert_eq!(published.publication.source_before, commit);
    assert_eq!(parents_of(&fx, &commit), vec![target]);
    for branch in [fx.int, source.as_str()] {
        assert_eq!(
            origin_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "origin {branch}"
        );
        assert_eq!(
            store_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "store {branch}"
        );
    }
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing",
        "the intent precedes the push; the outcome is the merge record's"
    );
    assert_eq!(
        landing_pin(&fx, "attempt-publication-1"),
        None,
        "settled, so unpinned"
    );
}

/// A push to the integration branch made outside Cairn while the child's
/// push is pending refuses that push whole: origin's copy of the child is
/// untouched by it (atomic, so no partial publication), the store never named
/// the refused landing, and the landing prepares again onto the commit origin
/// now holds and lands there, keeping the outside commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_target_moved_during_the_child_push_publishes_nothing_then_lands_on_the_new_target() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = advance_and_publish_integration(&fx);
    let source = fx.ctx.source_branch.clone();
    let scratch = tempfile::tempdir().unwrap();
    // (calls, store target during the first push, origin's child during the
    // second, the raced commit)
    type Seen = (
        usize,
        Option<String>,
        Option<Option<String>>,
        Option<String>,
    );
    let initial: Seen = (0, None, None, None);
    let seen = Arc::new(std::sync::Mutex::new(initial));
    {
        let (origin, scratch) = (fx.origin_path.clone(), scratch.path().to_path_buf());
        let (jj, store, int, source) = (
            fx.jj.clone(),
            fx.store.clone(),
            fx.int.to_string(),
            source.clone(),
        );
        let seen = seen.clone();
        publication_barrier::set(&fx.store, move || {
            let mut seen = seen.lock().unwrap();
            seen.0 += 1;
            if seen.0 == 1 {
                seen.1 = crate::jj::bookmark_commit(&jj, &store, &int);
                seen.3 = Some(race_origin_branch(&origin, &scratch, &int, "outside.rs"));
            } else if seen.0 == 2 {
                let out = crate::env::git()
                    .args([
                        "rev-parse",
                        "--verify",
                        "--quiet",
                        &format!("refs/heads/{source}"),
                    ])
                    .current_dir(&origin)
                    .output()
                    .unwrap();
                let commit = String::from_utf8_lossy(&out.stdout).trim().to_string();
                seen.2 = Some((out.status.success() && !commit.is_empty()).then_some(commit));
            }
        });
    }

    let db = fx.orch.db.local.clone();
    let landed = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL).await;
    publication_barrier::clear(&fx.store);
    let landed = landed.expect("the child lands on the moved integration branch");
    let (calls, int_during_first, child_on_origin_during_second, raced) =
        seen.lock().unwrap().clone();
    let raced = raced.expect("the race ran");

    assert_eq!(calls, 2, "one refused push, one accepted");
    assert_eq!(
        int_during_first.as_deref(),
        Some(target.as_str()),
        "the refused landing was never the store's integration branch"
    );
    assert_eq!(
        child_on_origin_during_second,
        Some(None),
        "the refused push published neither ref"
    );
    let commit = landed.landed_commit.unwrap();
    assert_eq!(
        parents_of(&fx, &commit),
        vec![raced.clone()],
        "prepared again onto exactly the commit origin moved to"
    );
    for branch in [fx.int, source.as_str()] {
        assert_eq!(
            origin_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "origin {branch}"
        );
    }
    assert_eq!(
        crate::jj::text_at_revision(&fx.jj, &fx.store, &commit, "outside.rs")
            .unwrap()
            .as_deref(),
        Some("outside\n"),
        "the outside commit is kept"
    );
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing,attempt-publication-1:2:failed,\
         attempt-publication-2:1:executing",
        "each try journals its own intent; the refused one its failure"
    );
    assert_eq!(landing_pin(&fx, "attempt-publication-1"), None);
    assert_eq!(landing_pin(&fx, "attempt-publication-2"), None);
}

/// The child's own ref is leased on what the store last recorded of it on
/// origin. A push to the pull request head made outside Cairn refuses the
/// landing instead of being overwritten: nothing is published, the outside
/// commit stays on origin, and the integration branch does not move.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_outside_push_to_the_child_head_refuses_the_landing_rather_than_being_overwritten() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = advance_and_publish_integration(&fx);
    let source = fx.ctx.source_branch.clone();
    crate::jj::push_store_bookmark(&fx.jj, &fx.store, &source).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let outside = race_origin_branch(&fx.origin_path, scratch.path(), &source, "reviewer.rs");

    let db = fx.orch.db.local.clone();
    let refused = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL)
        .await
        .expect_err("an outside push to the pull request head is never overwritten");

    assert!(
        refused.contains("source moved") && refused.contains("Nothing was published"),
        "{refused}"
    );
    assert_eq!(origin_ref(&fx, &source).as_deref(), Some(outside.as_str()));
    assert_eq!(origin_ref(&fx, fx.int).as_deref(), Some(target.as_str()));
    assert_eq!(store_ref(&fx, fx.int).as_deref(), Some(target.as_str()));
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing,attempt-publication-1:2:indeterminate",
        "a refusal is journaled for recovery to close from origin"
    );
    assert_eq!(landing_pin(&fx, "attempt-publication-1"), None);
    assert!(
        try_reserve(&fx, &[fx.int, &source]).await.is_ok(),
        "a refused landing releases its reservation"
    );
}

/// A push whose outcome cannot be known (origin unreachable mid-push) is
/// never rolled back and never retried blind: the store keeps the prepared
/// coordinate as it was when the lock was released, the intent and an
/// indeterminate outcome are journaled, and the pin keeps the landing commit
/// alive for recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_unknown_child_publication_keeps_the_pin_and_journal_for_recovery() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = advance_and_publish_integration(&fx);
    let source = fx.ctx.source_branch.clone();
    let gone = fx.origin_path.with_extension("gone");
    {
        let (origin, gone) = (fx.origin_path.clone(), gone.clone());
        publication_barrier::set(&fx.store, move || {
            std::fs::rename(&origin, &gone).unwrap();
        });
    }

    let db = fx.orch.db.local.clone();
    let unknown = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL).await;
    publication_barrier::clear(&fx.store);
    std::fs::rename(&gone, &fx.origin_path).unwrap();
    let unknown = unknown.expect_err("an unknown outcome is not reported as landed");

    assert!(unknown.contains("unknown outcome"), "{unknown}");
    let head = store_ref(&fx, &source).unwrap();
    assert_eq!(
        parents_of(&fx, &head),
        vec![target.clone()],
        "the store still holds the prepared head: nothing was restored after the lock was released"
    );
    assert_eq!(store_ref(&fx, fx.int).as_deref(), Some(target.as_str()));
    assert_eq!(origin_ref(&fx, fx.int).as_deref(), Some(target.as_str()));
    assert_eq!(origin_ref(&fx, &source), None);
    assert_eq!(
        landing_pin(&fx, "attempt-publication-1").as_deref(),
        Some(head.as_str()),
        "the pin keeps the landing commit for recovery"
    );
    assert_eq!(
        fx.orch
            .db
            .local
            .query_opt_text(
                "SELECT CAST(count(*) AS TEXT) FROM branch_advance_acceptances
                  WHERE published_checkpoint IS NOT NULL",
                (),
            )
            .await
            .unwrap()
            .as_deref(),
        Some("0"),
        "an unknown outcome checkpoints nothing"
    );
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing,attempt-publication-1:2:indeterminate"
    );
    assert!(try_reserve(&fx, &[fx.int, &source]).await.is_ok());
}

/// The integration branch X lands on `main` with its push pending when a
/// child of X starts landing into X. The child waits behind X's landing
/// reservation, which X's landing hands to the caller and keeps until X's
/// merge is recorded; by then X has landed, and the child is refused from
/// landing into it. The child publishes nothing at any point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_child_that_starts_landing_while_its_integration_branch_is_publishing_never_lands_into_it(
) {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let int_tip = advance_and_publish_integration(&fx);
    fx.orch
        .db
        .local
        .execute(
            "INSERT INTO merge_requests (id, job_id, project_id, title, source_branch,
                                         target_branch, opened_at, updated_at)
             VALUES ('mr-int', 'job-int', 'proj-1', 'Integration', ?1, 'main', 1, 1)",
            (fx.int.to_string(),),
        )
        .await
        .unwrap();
    let mut parent = fx.ctx.clone();
    parent.source_branch = fx.int.to_string();
    parent.target_branch = "main".to_string();
    parent.mr.mr_id = "mr-int".to_string();
    parent.mr.job_id = "job-int".to_string();

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    publication_barrier::set(&fx.store, move || {
        let _ = reached_tx.send(());
        let _ = go_rx.recv();
    });
    let child = {
        let (orch, ctx, db) = (fx.orch.clone(), fx.ctx.clone(), fx.orch.db.local.clone());
        tokio::spawn(async move {
            tokio::task::spawn_blocking(move || reached_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let landing = {
                let (orch, ctx, db) = (orch.clone(), ctx.clone(), db.clone());
                tokio::spawn(async move {
                    land_on_integration(&orch, "job-src", &db, &ctx, &JOURNAL)
                        .await
                        .map(|landed| landed.landed_commit)
                })
            };
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let waiting = !landing.is_finished();
            go_tx.send(()).unwrap();
            (waiting, landing)
        })
    };

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-int", &parent, "squash", None, &JOURNAL,
    )
    .await;
    publication_barrier::clear(&fx.store);
    let (waiting, child_landing) = child.await.unwrap();
    let landed = landed.expect("the integration branch lands");
    assert!(
        waiting,
        "the child waited behind the integration branch's landing"
    );
    assert!(landed.reservation.is_some());

    // merge.rs's tail order: the merge is recorded, then the reservation goes.
    fx.orch
        .db
        .local
        .execute(
            "UPDATE merge_requests SET status = 'merged' WHERE id = 'mr-int'",
            (),
        )
        .await
        .unwrap();
    drop(landed);

    let refused = child_landing
        .await
        .unwrap()
        .expect_err("a child never lands into an integration branch that already landed");
    assert!(
        refused.contains("already merged")
            || refused.contains("now targets")
            || refused.contains("diverged"),
        "{refused}"
    );
    assert_eq!(
        origin_ref(&fx, fx.int).as_deref(),
        Some(int_tip.as_str()),
        "the child published nothing into the landed integration branch"
    );
    assert_eq!(origin_ref(&fx, &fx.ctx.source_branch), None);
}

/// The integration branch holds its owner's commit that origin was never sent
/// (T descends origin's O). The child lands on T, and the one push
/// fast-forwards origin from exactly O over both: the lease is O, the store's
/// compare-and-swap is T.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_child_lands_on_an_integration_branch_holding_commits_origin_was_never_sent() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let origin_copy = advance_and_publish_integration(&fx);
    let store_copy = seal_in(&fx, "coord", fx.int, "hub2.rs", "unpublished\n");
    assert_eq!(
        origin_ref(&fx, fx.int).as_deref(),
        Some(origin_copy.as_str())
    );
    let source = fx.ctx.source_branch.clone();

    let db = fx.orch.db.local.clone();
    let landed = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL)
        .await
        .expect("a child lands on an integration branch ahead of origin");

    let commit = landed.landed_commit.unwrap();
    let published = landed.published.expect("a remote landing is published");
    assert_eq!(
        published.publication.expected_old, origin_copy,
        "leased on origin's copy"
    );
    assert_eq!(
        published.publication.target_before, store_copy,
        "moved from the store's"
    );
    assert_eq!(parents_of(&fx, &commit), vec![store_copy]);
    for branch in [fx.int, source.as_str()] {
        assert_eq!(
            origin_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "origin {branch}"
        );
        assert_eq!(
            store_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "store {branch}"
        );
    }
    assert_eq!(
        crate::jj::text_at_revision(&fx.jj, &fx.store, &commit, "hub2.rs")
            .unwrap()
            .as_deref(),
        Some("unpublished\n"),
        "the owner's unpublished commit reaches origin with the landing"
    );
}

/// Origin holds an earlier publication of the integration branch that the
/// store has since rewritten (carried onto an advanced `main`). Every commit
/// origin's copy holds that the store's lacks has an exact successor in the
/// store's, so the landing rewrites origin's copy, leased on exactly it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_child_lands_when_origin_holds_an_integration_copy_the_store_rewrote() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let origin_copy = advance_and_publish_integration(&fx);
    advance_main(&fx, "incoming.rs", "incoming\n");
    fx.jj
        .run(
            &fx.store,
            &[
                "rebase",
                "-b",
                fx.int,
                "-d",
                "main",
                "--ignore-working-copy",
            ],
            "rewrite the integration branch onto main",
        )
        .unwrap();
    let rewritten = store_ref(&fx, fx.int).unwrap();
    assert_ne!(rewritten, origin_copy);

    let db = fx.orch.db.local.clone();
    let landed = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL)
        .await
        .expect("a provable rewrite of origin's integration copy lands");

    let commit = landed.landed_commit.unwrap();
    let published = landed.published.expect("a remote landing is published");
    assert_eq!(
        published.publication.expected_old, origin_copy,
        "leased on origin's copy"
    );
    assert_eq!(published.publication.target_before, rewritten);
    assert_eq!(parents_of(&fx, &commit), vec![rewritten]);
    for branch in [fx.int, fx.ctx.source_branch.as_str()] {
        assert_eq!(
            origin_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "origin {branch}"
        );
    }
}

/// A push made outside Cairn to origin's integration branch while the store
/// holds an unpublished commit of its own: each side holds commits the other
/// lacks. Nothing proves the outside commit is the store's own, so the
/// supersession proof refuses the rewrite before anything is sent, and the
/// outside commit stays on origin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_child_is_refused_when_origin_and_the_store_hold_diverged_integration_branches() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    advance_and_publish_integration(&fx);
    let store_copy = seal_in(&fx, "coord", fx.int, "hub2.rs", "unpublished\n");
    let scratch = tempfile::tempdir().unwrap();
    let outside = race_origin_branch(&fx.origin_path, scratch.path(), fx.int, "outside.rs");

    let db = fx.orch.db.local.clone();
    let refused = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL)
        .await
        .expect_err("an unproven rewrite of origin's integration branch is refused");

    assert!(
        refused.contains("not proven superseded") && refused.contains("Nothing was published"),
        "{refused}"
    );
    assert_eq!(origin_ref(&fx, fx.int).as_deref(), Some(outside.as_str()));
    assert_eq!(store_ref(&fx, fx.int).as_deref(), Some(store_copy.as_str()));
    assert_eq!(origin_ref(&fx, &fx.ctx.source_branch), None);
    assert_eq!(
        journal_events(&fx).await,
        "attempt-publication-1:1:executing,attempt-publication-1:2:indeterminate",
        "refused at send, after the intent; recovery closes it from origin"
    );
    assert_eq!(landing_pin(&fx, "attempt-publication-1"), None);
}

const DORMANT: &str = "agent/PROJ-3-builder-0";

/// The integration branch's own commit is published (origin's copy O), a
/// child cut from it lies dormant (keeping O visible in the store), and the
/// integration branch is then prepared twice by the canonical preparer, never
/// pushed: `main` first absorbs its change, so its commit is dropped as empty
/// (an accepted drop claim), then advances again (a second accepted install
/// that inherits the claim). Returns (O, the store's copy, the dormant tip).
async fn two_local_advances_over_a_dormant_child(fx: &MergeFixture) -> (String, String, String) {
    let origin_copy = advance_and_publish_integration(fx);
    let workspace = fx._wts.path().join("dormant");
    crate::jj::add_workspace(&fx.jj, &fx.store, &workspace, DORMANT, fx.int, None).unwrap();
    let dormant = seal_in(fx, "dormant", DORMANT, "dormant.rs", "dormant\n");
    fx.orch
        .db
        .local
        .execute(
            "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
             VALUES ('job-dormant', 'proj-1', 'running', ?1, ?2, 1, 1)",
            (DORMANT.to_string(), fx.int.to_string()),
        )
        .await
        .unwrap();
    for (file, content) in [("hub.rs", "hub\n"), ("incoming.rs", "incoming\n")] {
        advance_main(fx, file, content);
        let mut request = crate::orchestrator::base_advance::preparation::PreparationRequest::new(
            "proj-1".to_string(),
            fx.int.to_string(),
            Some("job-int".to_string()),
            crate::orchestrator::base_advance::preparation::PreparationPurpose::Check,
        );
        request.compute_budget = FIXTURE_PREPARATION_BUDGET;
        let prepared =
            crate::orchestrator::base_advance::preparation::prepare_branch(&fx.orch, request)
                .await
                .expect("the integration branch prepares onto main");
        assert!(prepared.advanced, "a real local advance");
    }
    let store_copy = store_ref(fx, fx.int).unwrap();
    assert!(
        crate::jj::revset_commit(
            &fx.jj,
            &fx.store,
            &format!("{origin_copy} & ::{store_copy}")
        )
        .is_none(),
        "origin's copy is not in the store's: publishing it is a rewrite"
    );
    assert_eq!(
        origin_ref(fx, fx.int).as_deref(),
        Some(origin_copy.as_str()),
        "never pushed"
    );
    (origin_copy, store_copy, dormant)
}

/// The original goal: an integration branch advanced lazily (twice, locally,
/// with a dormant child keeping its old commit visible and that commit dropped
/// as empty) still takes its child. The landing rewrites origin's copy on the
/// accepted installs' evidence, leased on exactly O, and moves nothing else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_child_lands_on_an_integration_branch_advanced_twice_locally_over_a_dormant_child() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let (origin_copy, store_copy, dormant) = two_local_advances_over_a_dormant_child(&fx).await;

    let db = fx.orch.db.local.clone();
    let landed = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL)
        .await
        .expect("accepted-drop evidence proves the rewrite of origin's copy");

    let commit = landed.landed_commit.unwrap();
    let published = landed.published.expect("a remote landing is published");
    assert_eq!(
        published.publication.expected_old, origin_copy,
        "leased on exactly O"
    );
    assert_eq!(published.publication.target_before, store_copy);
    assert_eq!(parents_of(&fx, &commit), vec![store_copy]);
    for branch in [fx.int, fx.ctx.source_branch.as_str()] {
        assert_eq!(
            origin_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "origin {branch}"
        );
        assert_eq!(
            store_ref(&fx, branch).as_deref(),
            Some(commit.as_str()),
            "store {branch}"
        );
    }
    assert_eq!(
        store_ref(&fx, DORMANT).as_deref(),
        Some(dormant.as_str()),
        "a dormant child is never moved"
    );
    assert_eq!(origin_ref(&fx, DORMANT), None);
    for branch in [fx.int, fx.ctx.source_branch.as_str()] {
        assert_eq!(
            newest_checkpoint(&fx, branch).await.as_deref(),
            Some(commit.as_str()),
            "the confirmed publication resets `{branch}`'s claims"
        );
    }
}

/// The publication checkpoint on `branch`'s newest accepted install.
async fn newest_checkpoint(fx: &MergeFixture, branch: &str) -> Option<String> {
    fx.orch
        .db
        .local
        .query_opt_text(
            "SELECT published_checkpoint FROM branch_advance_acceptances
              WHERE branch = ?1 AND status = 'accepted'
              ORDER BY created_at DESC, id DESC LIMIT 1",
            (branch.to_string(),),
        )
        .await
        .unwrap()
}

/// The same lazily advanced integration branch with its acceptance journal
/// gone: the dropped commit then has no evidence, so the rewrite is refused
/// before anything is sent. The journal is the only proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn without_its_acceptance_journal_a_lazily_advanced_integration_branch_refuses_the_rewrite() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let (origin_copy, store_copy, _) = two_local_advances_over_a_dormant_child(&fx).await;
    fx.orch
        .db
        .local
        .execute("DELETE FROM branch_advance_acceptances", ())
        .await
        .unwrap();

    let db = fx.orch.db.local.clone();
    let refused = land_on_integration(&fx.orch, "job-src", &db, &fx.ctx, &JOURNAL)
        .await
        .expect_err("no evidence, no rewrite");

    assert!(refused.contains("not proven superseded"), "{refused}");
    assert_eq!(
        origin_ref(&fx, fx.int).as_deref(),
        Some(origin_copy.as_str())
    );
    assert_eq!(store_ref(&fx, fx.int).as_deref(), Some(store_copy.as_str()));
    assert_eq!(origin_ref(&fx, &fx.ctx.source_branch), None);
}

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

async fn acceptance_status(fx: &MergeFixture, id: &str) -> String {
    fx.orch
        .db
        .local
        .query_opt_text(
            "SELECT status FROM branch_advance_acceptances WHERE id = ?1",
            (id.to_string(),),
        )
        .await
        .unwrap()
        .expect("the row exists")
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

/// A target owing a pending acceptance whose candidate is exactly the commit
/// the target holds is settled by the landing's admission (that install did
/// complete) and the landing proceeds onto it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_target_owing_a_settleable_acceptance_is_settled_and_the_landing_proceeds() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let ctx = remote_default_context(&fx).await;
    let target = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let owed = owe(&fx, "main", "pending", &"1".repeat(40), &target).await;

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await
    .expect("the owed install is settled and the landing proceeds");

    assert_eq!(
        acceptance_status(&fx, &owed).await,
        "accepted",
        "admission settled the row it found as the install that completed"
    );
    let commit = landed.landed_commit.unwrap();
    assert_eq!(parents_of(&fx, &commit), vec![target]);
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), commit);
}

/// A landing cancelled while its origin take is running (the caller stopped
/// awaiting) does not release the store: the take owns the guard on its own
/// worker until it completes, so a writer queued behind it cannot enter until
/// then. Nothing is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_cancelled_landing_keeps_the_store_locked_until_its_origin_take_completes() {
    use crate::pr_data::actions::default_landing::origin_take_barrier;
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let target = advance_and_publish_integration(&fx);

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    origin_take_barrier::set(&fx.store, move || {
        let _ = reached_tx.send(());
        let _ = go_rx.recv();
    });
    let landing = {
        let (orch, ctx, db) = (fx.orch.clone(), fx.ctx.clone(), fx.orch.db.local.clone());
        tokio::spawn(async move {
            land_on_integration(&orch, "job-src", &db, &ctx, &JOURNAL)
                .await
                .map(|landed| landed.landed_commit)
        })
    };
    tokio::task::spawn_blocking(move || reached_rx.recv())
        .await
        .unwrap()
        .unwrap();
    landing.abort();
    assert!(landing.await.is_err_and(|error| error.is_cancelled()));

    let queued = fx
        .orch
        .acquire_jj_store_lock_with_timeout(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "a writer queued behind the take",
            Some(std::time::Duration::from_millis(300)),
        )
        .await
        .is_err();
    go_tx.send(()).unwrap();
    let after = fx
        .orch
        .acquire_jj_store_lock_with_timeout(
            &fx.store,
            crate::orchestrator::StoreLockClass::Foreground,
            "a writer after the take",
            Some(std::time::Duration::from_secs(10)),
        )
        .await
        .is_ok();
    origin_take_barrier::clear(&fx.store);

    assert!(
        queued,
        "the cancelled landing's take still owns the store lock"
    );
    assert!(after, "the lock is released once the take completes");
    assert_eq!(origin_ref(&fx, fx.int).as_deref(), Some(target.as_str()));
    assert_eq!(origin_ref(&fx, &fx.ctx.source_branch), None);
    assert_eq!(
        journal_events(&fx).await,
        "",
        "nothing was about to be published"
    );
}

/// The origin take moves only the landing's target, so an unrelated branch
/// being reserved meanwhile does not refuse it, and a writer queued behind the
/// take enters only once it is done, seeing the target fast-forwarded to what
/// origin took.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_unrelated_reservation_does_not_hold_up_the_origin_take_and_a_queued_writer_sees_its_fast_forward(
) {
    use crate::pr_data::actions::default_landing::origin_take_barrier;
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    advance_and_publish_integration(&fx);
    let scratch = tempfile::tempdir().unwrap();
    let raced = race_origin_branch(&fx.origin_path, scratch.path(), fx.int, "outside.rs");
    let unrelated = fx
        .orch
        .reserve_branches(
            &fx.store,
            &["agent/unrelated"],
            "an unrelated write",
            std::time::Duration::from_secs(5),
        )
        .await
        .unwrap();

    let (reached_tx, reached_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    origin_take_barrier::set(&fx.store, move || {
        let _ = reached_tx.send(());
        let _ = go_rx.recv();
    });
    let landing = {
        let (orch, ctx, db) = (fx.orch.clone(), fx.ctx.clone(), fx.orch.db.local.clone());
        tokio::spawn(async move {
            land_on_integration(&orch, "job-src", &db, &ctx, &JOURNAL)
                .await
                .map(|landed| landed.landed_commit)
        })
    };
    tokio::task::spawn_blocking(move || reached_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let queued = {
        let (orch, store, jj) = (fx.orch.clone(), fx.store.clone(), fx.jj.clone());
        let int = fx.int.to_string();
        tokio::spawn(async move {
            let _guard = orch
                .acquire_jj_store_lock_with_timeout(
                    &store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "a writer queued behind the take",
                    Some(std::time::Duration::from_secs(30)),
                )
                .await
                .expect("the queued writer enters once the take is done");
            crate::jj::bookmark_commit(&jj, &store, &int)
        })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !queued.is_finished(),
        "the queued writer waits behind the take"
    );
    go_tx.send(()).unwrap();
    let seen = queued
        .await
        .unwrap()
        .expect("the integration branch resolves");
    let landed = landing.await.unwrap();
    origin_take_barrier::clear(&fx.store);
    drop(unrelated);

    let commit = landed
        .expect("an unrelated reservation does not refuse the landing")
        .expect("the landing names its commit");
    let descends = |tip: &str| {
        crate::jj::revset_commit(&fx.jj, &fx.store, &format!("{raced} & ::{tip}")).is_some()
    };
    assert!(
        descends(&seen),
        "the queued writer saw the take's fast-forward: {seen}"
    );
    assert!(
        descends(&commit),
        "the landing is built on what origin took"
    );
    assert_eq!(origin_ref(&fx, fx.int).as_deref(), Some(commit.as_str()));
    // Not deferred by the unrelated reservation: the store recorded origin at
    // the landing, and the source's confirmed publication was checkpointed.
    for branch in [fx.int, fx.ctx.source_branch.as_str()] {
        assert_eq!(
            crate::jj::revset_commit_checked(
                &fx.jj,
                &fx.store,
                &format!("remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")"),
            )
            .unwrap()
            .as_deref(),
            Some(commit.as_str()),
            "the store recorded origin's `{branch}` at the landing"
        );
    }
    assert_eq!(
        newest_checkpoint(&fx, &fx.ctx.source_branch)
            .await
            .as_deref(),
        Some(commit.as_str()),
        "the source's confirmed publication is its claims reset point"
    );
}

// ── Landing acceptance: refusal, external movement, conflict wakes ────────────────────

/// A target origin protects (its receiver refuses the update) is refused
/// after the leased push, with nothing moved anywhere: origin, the store's
/// target and the source all as they were.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_protected_target_refuses_the_landing_and_nothing_moves() {
    use std::os::unix::fs::PermissionsExt;
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let ctx = remote_default_context(&fx).await;
    let target = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let source = crate::jj::bookmark_commit(&fx.jj, &fx.store, &ctx.source_branch).unwrap();
    let hook = fx.origin_path.join("hooks").join("pre-receive");
    std::fs::create_dir_all(hook.parent().unwrap()).unwrap();
    std::fs::write(&hook, "#!/bin/sh\necho 'main is protected' >&2\nexit 1\n").unwrap();
    std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();

    let refused = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await
    .expect_err("a protected target refuses the landing");

    assert!(refused.contains("Nothing was published"), "{refused}");
    assert_eq!(git_stdout(&fx.origin_path, &["rev-parse", "main"]), target);
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(target.as_str()),
        "the store's target never named the refused landing"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, &ctx.source_branch).as_deref(),
        Some(source.as_str()),
        "the source is as it was"
    );
}

/// Messages addressed to `job`: on its channel, or to any of its runs.
async fn messages_for(fx: &MergeFixture, job: &str) -> i64 {
    fx.orch
        .db
        .local
        .query_opt_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM messages
              WHERE channel_id = ?1
                 OR recipient_run_id IN (SELECT id FROM runs WHERE job_id = ?1)",
            (job.to_string(),),
        )
        .await
        .ok()
        .flatten()
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// A merge made outside Cairn (the operator in GitHub's UI) moves origin's
/// base. The next landing takes it in on demand and lands onto it, and the
/// dormant integration branch beside it is neither replayed nor told.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn an_external_merge_is_taken_on_demand_without_touching_a_dormant_sibling() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let ctx = remote_default_context(&fx).await;
    let sibling = crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let outside = race_origin_main(&fx.origin_path, scratch.path());
    let before = messages_for(&fx, "job-int").await;

    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await
    .expect("the landing lands onto the moved base");

    assert_eq!(
        parents_of(&fx, &landed.landed_commit.unwrap()),
        vec![outside]
    );
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).as_deref(),
        Some(sibling.as_str()),
        "the dormant sibling was not replayed"
    );
    assert_eq!(
        messages_for(&fx, "job-int").await,
        before,
        "the dormant sibling's job was told nothing"
    );
}

/// Wakes pushed to anyone, by kind, excluding passive notes.
async fn active_wakes(fx: &MergeFixture) -> i64 {
    fx.orch
        .db
        .local
        .query_opt_text(
            "SELECT CAST(COUNT(*) AS TEXT) FROM attention_pushes WHERE wake != 'passive'",
            (),
        )
        .await
        .ok()
        .flatten()
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// A merge that conflicts with its base opens the owner's conflict session
/// and wakes the owner once per conflict, even though a check had already
/// recorded the same conflict as evidence (which wakes no one). Merging again
/// against the same conflict wakes no one again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_conflicting_merge_wakes_its_owner_once_even_after_a_check_recorded_the_conflict() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    fx.orch
        .db
        .local
        .execute_script(
            "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
               VALUES ('issue', 'proj-1', 1, 'Issue', 'active', 1, 1);
             UPDATE jobs SET issue_id = 'issue' WHERE id = 'job-src';
             INSERT INTO runs (id, issue_id, project_id, job_id, status, created_at, updated_at)
               VALUES ('run-src', 'issue', 'proj-1', 'job-src', 'live', 1, 1);"
                .to_string(),
        )
        .await
        .unwrap();
    advance_main(&fx, "shared.rs", "main-side\n");
    retarget(&fx, "main").await;
    let mut ctx = fx.ctx.clone();
    ctx.target_branch = "main".to_string();
    ctx.mr.is_local = true;

    // The check prepares onto the same target the merge lands on (the job
    // itself declares the integration branch).
    let mut check = crate::orchestrator::base_advance::preparation::PreparationRequest::for_check(
        "proj-1",
        ctx.source_branch.clone(),
        Some("job-src".to_string()),
    );
    check.target_override = Some("main".to_string());
    check.compute_budget = FIXTURE_PREPARATION_BUDGET;
    let checked =
        crate::orchestrator::base_advance::preparation::prepare_branch(&fx.orch, check).await;
    assert!(
        checked
            .as_ref()
            .is_err_and(|refusal| refusal.kind() == "conflicts with base"),
        "the check sees the conflict: {checked:?}"
    );
    assert_eq!(
        active_wakes(&fx).await,
        0,
        "a check's conflict wakes no one"
    );

    for attempt in 0..2 {
        let refused = crate::pr_data::actions::default_landing::land_on_default(
            &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
        )
        .await
        .expect_err("a conflicting source does not land");
        assert!(refused.contains("conflict"), "{refused}");
        assert_eq!(
            active_wakes(&fx).await,
            1,
            "one owner wake for this conflict (attempt {attempt}), not suppressed by the check"
        );
    }
}

/// A row count from a test-local query that yields one integer.
async fn count(fx: &MergeFixture, sql: &str) -> i64 {
    fx.orch
        .db
        .local
        .query_opt_text(format!("SELECT CAST(({sql}) AS TEXT)"), ())
        .await
        .unwrap()
        .and_then(|count| count.parse().ok())
        .unwrap_or(0)
}

/// A default landing touches only its own source and target. Eight dormant
/// branches beside it and a child cut from its source keep their exact tips;
/// none of their jobs gets a message or a wake, and no reconcile intent or
/// item is created for any of them. The child's recorded base is re-pointed
/// at the branch its parent landed on (the one relationship change a landing
/// makes), its tip untouched. Afterwards a dormant branch advances only when
/// something demands it, and only that one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(jj)]
async fn a_default_landing_beside_many_dormant_branches_moves_and_tells_none_of_them() {
    let Some(bin) = jj_bin() else { return };
    let fx = setup_clean_merge(&bin).await;
    let mut dormant = Vec::new();
    for n in 0..8 {
        let bookmark = format!("agent/dormant-{n}");
        let workspace = fx._wts.path().join(format!("dormant-{n}"));
        crate::jj::add_workspace(&fx.jj, &fx.store, &workspace, &bookmark, "main", None).unwrap();
        let tip = seal_in(
            &fx,
            &format!("dormant-{n}"),
            &bookmark,
            &format!("d{n}.rs"),
            "d\n",
        );
        dormant.push((format!("job-dormant-{n}"), bookmark, tip));
    }
    let nested_bookmark = "agent/nested-child".to_string();
    let nested_ws = fx._wts.path().join("nested");
    crate::jj::add_workspace(
        &fx.jj,
        &fx.store,
        &nested_ws,
        &nested_bookmark,
        &fx.ctx.source_branch,
        None,
    )
    .unwrap();
    let nested_tip = seal_in(&fx, "nested", &nested_bookmark, "nested.rs", "n\n");
    let mut seed = String::new();
    for (job, bookmark, _) in &dormant {
        seed.push_str(&format!(
            "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
               VALUES ('{job}', 'proj-1', 'running', '{bookmark}', 'main', 1, 1);\n"
        ));
    }
    seed.push_str(&format!(
        "INSERT INTO jobs (id, project_id, status, branch, base_branch, created_at, updated_at)
           VALUES ('job-nested', 'proj-1', 'running', '{nested_bookmark}', '{}', 1, 1);",
        fx.ctx.source_branch
    ));
    fx.orch.db.local.execute_script(seed).await.unwrap();

    let others = || {
        dormant
            .iter()
            .map(|(job, _, _)| format!("'{job}'"))
            .chain(["'job-nested'".to_string()])
            .collect::<Vec<_>>()
            .join(", ")
    };
    let messages_sql = format!(
        "SELECT COUNT(*) FROM messages WHERE channel_id IN ({0})
            OR recipient_run_id IN (SELECT id FROM runs WHERE job_id IN ({0}))",
        others()
    );
    let wakes_sql = format!(
        "SELECT COUNT(*) FROM attention_pushes WHERE recipient IN ({0})
            OR recipient IN (SELECT id FROM runs WHERE job_id IN ({0}))",
        others()
    );
    let branches = dormant
        .iter()
        .map(|(_, bookmark, _)| format!("'{bookmark}'"))
        .chain([format!("'{nested_bookmark}'")])
        .collect::<Vec<_>>()
        .join(", ");
    let items_sql =
        format!("SELECT COUNT(*) FROM jj_reconcile_items WHERE bookmark IN ({branches})");
    let intents_sql = "SELECT COUNT(*) FROM jj_reconcile_intents";
    let before = (
        count(&fx, &messages_sql).await,
        count(&fx, &wakes_sql).await,
        count(&fx, &items_sql).await,
        count(&fx, intents_sql).await,
    );

    retarget(&fx, "main").await;
    let mut ctx = fx.ctx.clone();
    ctx.target_branch = "main".to_string();
    ctx.mr.is_local = true;
    let landed = crate::pr_data::actions::default_landing::land_on_default(
        &fx.orch, "job-src", &ctx, "squash", None, &JOURNAL,
    )
    .await
    .expect("the source lands");
    let landing = landed.landed_commit.clone().unwrap();
    drop(landed);

    for (_, bookmark, tip) in &dormant {
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).as_deref(),
            Some(tip.as_str()),
            "dormant `{bookmark}` was not replayed"
        );
    }
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, &nested_bookmark).as_deref(),
        Some(nested_tip.as_str()),
        "the nested child was not replayed"
    );
    assert_eq!(
        fx.orch
            .db
            .local
            .query_opt_text("SELECT base_branch FROM jobs WHERE id = 'job-nested'", ())
            .await
            .unwrap()
            .as_deref(),
        Some("main"),
        "the child's base follows its landed parent"
    );
    // Not only the store's view: the backing repository's refs name the same
    // exact tips, so a later import of Git cannot bring a rewrite back.
    let repo = std::path::PathBuf::from(&fx.ctx.mr.repo_path);
    let untouched: Vec<(String, String)> = dormant
        .iter()
        .map(|(_, bookmark, tip)| (bookmark.clone(), tip.clone()))
        .chain([(nested_bookmark.clone(), nested_tip.clone())])
        .collect();
    for (bookmark, tip) in &untouched {
        assert_eq!(
            &git_stdout(&repo, &["rev-parse", &format!("refs/heads/{bookmark}")]),
            tip,
            "`refs/heads/{bookmark}` was not rewritten"
        );
    }
    {
        let _guard = fx
            .orch
            .acquire_jj_store_lock(
                &fx.store,
                crate::orchestrator::StoreLockClass::Foreground,
                "full import after the landing",
            )
            .await;
        crate::jj::import_git(&fx.jj, &fx.store).unwrap();
    }
    for (bookmark, tip) in &untouched {
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).as_deref(),
            Some(tip.as_str()),
            "a full import moves `{bookmark}` nowhere"
        );
    }
    assert_eq!(
        (
            count(&fx, &messages_sql).await,
            count(&fx, &wakes_sql).await,
            count(&fx, &items_sql).await,
            count(&fx, intents_sql).await,
        ),
        before,
        "no message, wake, reconcile item or intent for anything beside the landing"
    );

    // Only a branch that is demanded advances, and only it.
    let (demanded_job, demanded, demanded_tip) = dormant[0].clone();
    let mut demand = crate::orchestrator::base_advance::preparation::PreparationRequest::for_check(
        "proj-1",
        demanded.clone(),
        Some(demanded_job),
    );
    demand.compute_budget = FIXTURE_PREPARATION_BUDGET;
    let prepared = crate::orchestrator::base_advance::preparation::prepare_branch(&fx.orch, demand)
        .await
        .expect("a demanded dormant branch prepares onto the landed base");
    assert!(prepared.advanced);
    assert_eq!(prepared.base_commit, landing);
    assert_ne!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, &demanded).as_deref(),
        Some(demanded_tip.as_str())
    );
    for (_, bookmark, tip) in &dormant[1..] {
        assert_eq!(
            crate::jj::bookmark_commit(&fx.jj, &fx.store, bookmark).as_deref(),
            Some(tip.as_str()),
            "`{bookmark}` stays dormant"
        );
    }
}
