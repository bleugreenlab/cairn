use super::*;
use crate::pr_data::actions::context::MrContext;
use crate::pr_data::actions::test_support::{migrated_db, test_orchestrator};
use crate::services::testing::{MockGitClient, TestServicesBuilder};
use crate::storage::SearchIndex;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

// ---- rollback_merge refreshes BOTH source and target worktrees ----

fn jj_bin() -> Option<String> {
    let bin = std::env::var("CAIRN_JJ_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "jj".to_string());
    crate::env::command(&bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        .then_some(bin)
}

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

/// Run a jj command with the managed config, asserting success and returning
/// trimmed stdout (`JjEnv::run` is private to the jj module).
fn jj_raw(bin: &str, cfg: &Path, cwd: &Path, args: &[&str]) -> String {
    let out = crate::env::command(bin)
        .args(args)
        .current_dir(cwd)
        .env("JJ_CONFIG", cfg)
        .output()
        .unwrap();
    assert!(out.status.success(), "jj {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

async fn seed_branch_jobs(
    db: &LocalDb,
    repo_path: &str,
    int_branch: &str,
    int_ws: &str,
    source_branch: &str,
    source_ws: &str,
) {
    let (repo_path, int_branch, int_ws, source_branch, source_ws) = (
        repo_path.to_string(),
        int_branch.to_string(),
        int_ws.to_string(),
        source_branch.to_string(),
        source_ws.to_string(),
    );
    db.write(move |conn| {
            let (repo_path, int_branch, int_ws, source_branch, source_ws) = (
                repo_path.clone(),
                int_branch.clone(),
                int_ws.clone(),
                source_branch.clone(),
                source_ws.clone(),
            );
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Project', 'PROJ', ?1, 'main', 1, 1)",
                    params![repo_path.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, worktree_path, branch, base_branch, created_at, updated_at)
                     VALUES ('job-int', 'proj-1', 'running', ?1, ?2, 'main', 1, 1)",
                    params![int_ws.as_str(), int_branch.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, project_id, status, worktree_path, branch, base_branch, created_at, updated_at)
                     VALUES ('job-src', 'proj-1', 'running', ?1, ?2, ?3, 1, 1)",
                    params![source_ws.as_str(), source_branch.as_str(), int_branch.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// The transactional-merge guarantee: after a failed post-mutation merge,
/// `rollback_merge` restores the store AND refreshes BOTH the source and the
/// (advanced) target worktrees. The mutation is modeled by the coordinator
/// sealing a file (advancing the integration branch and materializing it on
/// the coordinator's disk) — this touches only the coordinator workspace, as
/// production does, never the store's own working copy. Assertions read the
/// on-disk tree via `std::fs` because a jj read command would auto-recover a
/// stale workspace and mask the difference the fix makes. The negative control
/// (`target_branch = None`) proves the hazard the fix closes: the coordinator
/// worktree is left on the rolled-back-away tree.
#[tokio::test(flavor = "current_thread")]
async fn rollback_refreshes_source_and_target_worktrees() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping rollback_refreshes_source_and_target_worktrees: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let proj = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    init_git_project(proj.path());
    let jj = crate::jj::JjEnv::resolve(&bin, home.path());
    let cfg = home.path().join("jj").join("config.toml");
    let store = home.path().join("jj-stores").join("proj");
    crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/PROJ-1-coordinator-0";
    let ws_coord = wts.path().join("coord");
    crate::jj::add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
    let source = "agent/PROJ-2-builder-0";
    let ws_child = wts.path().join("child");
    crate::jj::add_workspace(&jj, &store, &ws_child, source, int, None).unwrap();
    std::fs::write(ws_child.join("child.rs"), "child\n").unwrap();
    crate::jj::seal(&jj, &ws_child, "child work", None).unwrap();

    let db = migrated_db().await;
    seed_branch_jobs(
        &db,
        &proj.path().to_string_lossy(),
        int,
        &ws_coord.to_string_lossy(),
        source,
        &ws_child.to_string_lossy(),
    )
    .await;
    let orch = test_orchestrator(db, MockGitClient::new());

    // Export bookmarks so the backing git refs are consistent with the store
    // before the snapshot (in production, prior seals/merges always export, so
    // the op-restore rewind is clean).
    jj_raw(
        &bin,
        &cfg,
        &store,
        &["git", "export", "--ignore-working-copy"],
    );
    let int_pre = crate::jj::bookmark_commit(&jj, &store, int).unwrap();
    let source_pre = crate::jj::bookmark_commit(&jj, &store, source).unwrap();
    let op = crate::jj::operation_id(&jj, &store).unwrap();

    // Advance the integration branch by having the coordinator seal a file, and
    // rebase the source onto it (the mid-merge mutations rollback must undo).
    std::fs::write(ws_coord.join("hub.rs"), "hub-1\n").unwrap();
    crate::jj::seal(&jj, &ws_coord, "hub work", None).unwrap();
    crate::jj::rebase_branch_onto(&jj, &store, source, int).unwrap();
    assert!(
        ws_coord.join("hub.rs").exists(),
        "precondition: the coordinator's worktree carries the advanced tree"
    );

    // WITH THE FIX: target=Some refreshes the coordinator workspace, so after
    // the op-restore its on-disk tree returns to the restored (pre-merge) state
    // (no hub.rs), and both bookmarks are rolled back.
    let err = rollback_merge(
        &orch,
        "proj-1",
        &jj,
        &store,
        &op,
        source,
        Some(int),
        "boom".to_string(),
    )
    .await;
    assert!(
        err.contains("safe to retry"),
        "the rollback error tells the caller a retry is safe"
    );
    assert_eq!(
        crate::jj::bookmark_commit(&jj, &store, int).unwrap(),
        int_pre
    );
    assert_eq!(
        crate::jj::bookmark_commit(&jj, &store, source).unwrap(),
        source_pre
    );
    assert!(
        !ws_coord.join("hub.rs").exists(),
        "the target/coordinator worktree is refreshed back to the restored (pre-merge) state"
    );

    // NEGATIVE CONTROL (old behavior): re-run the same mutation on the now-fresh
    // coordinator, then roll back with target=None. Only the source is
    // refreshed, so the coordinator's on-disk tree is left on the rolled-back-
    // away state (hub.rs still present) — the hazard the fix closes.
    std::fs::write(ws_coord.join("hub.rs"), "hub-2\n").unwrap();
    crate::jj::seal(&jj, &ws_coord, "hub work again", None).unwrap();
    crate::jj::rebase_branch_onto(&jj, &store, source, int).unwrap();
    let _ = rollback_merge(
        &orch,
        "proj-1",
        &jj,
        &store,
        &op,
        source,
        None,
        "boom".to_string(),
    )
    .await;
    assert_eq!(
        crate::jj::bookmark_commit(&jj, &store, int).unwrap(),
        int_pre
    );
    assert_eq!(
        crate::jj::bookmark_commit(&jj, &store, source).unwrap(),
        source_pre
    );
    assert!(
        ws_coord.join("hub.rs").exists(),
        "without the target refresh the coordinator worktree keeps the rolled-back-away tree"
    );
}

/// A wedged hub with a bare origin plus a source child that genuinely conflicts
/// on rebase, wired so `store_merge_child` runs end-to-end: an Orchestrator
/// whose `config_dir` keys the store path the function computes, jobs seeded so
/// the target-base and worktree lookups resolve, and a `MergeMrContext` for the
/// child→integration merge. TempDirs are kept alive by the struct.
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

async fn setup_wedged_merge(bin: &str) -> MergeFixture {
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
            .build()
    };
    let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
    let cfg = orch.config_dir.join("jj").join("config.toml");
    let repo_path = proj.path().to_string_lossy().into_owned();
    let store = crate::jj::project_store_dir(&orch.config_dir, Path::new(&repo_path));
    crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Coordinator integration branch, published to origin at its pre-conflict tip.
    let int = "agent/PROJ-1-coordinator-0";
    let ws_coord = wts.path().join("coord");
    crate::jj::add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
    crate::jj::ensure_bookmark_on_origin(&jj, &store, int).unwrap();

    // A source child forked from the ORIGINAL main base that edits shared.rs, so
    // once the hub's divergent resolution is flattened onto the advanced main
    // the rebase is a genuine 3-way conflict.
    let source = "agent/PROJ-2-builder-0";
    let ws_child = wts.path().join("child");
    crate::jj::add_workspace(&jj, &store, &ws_child, source, "main", None).unwrap();
    std::fs::write(ws_child.join("shared.rs"), "child-edit\n").unwrap();
    crate::jj::seal(&jj, &ws_child, "child edits shared", None).unwrap();

    // Wedge the hub: hub edits shared, main advances conflictingly, the hub
    // rebases (baking the conflict into its intermediate) and resolves at its
    // tip — a clean tip over a conflicted intermediate.
    std::fs::write(ws_coord.join("shared.rs"), "hub-edit\n").unwrap();
    crate::jj::seal(&jj, &ws_coord, "hub edits shared", None).unwrap();
    jj_raw(bin, &cfg, &store, &["new", "main"]);
    std::fs::write(store.join("shared.rs"), "main-advanced\n").unwrap();
    jj_raw(bin, &cfg, &store, &["describe", "-m", "main advances"]);
    jj_raw(
        bin,
        &cfg,
        &store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            "@",
            "--ignore-working-copy",
        ],
    );
    crate::jj::rebase_branch_onto(&jj, &store, int, "main").unwrap();
    assert!(crate::jj::branch_has_conflict(&jj, &store, int).unwrap());
    crate::jj::update_stale(&jj, &ws_coord).unwrap();
    std::fs::write(ws_coord.join("shared.rs"), "hub-resolved\n").unwrap();
    crate::jj::seal(&jj, &ws_coord, "resolve hub", None).unwrap();
    assert!(!crate::jj::branch_has_conflict(&jj, &store, int).unwrap());

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

/// A child merge into a WEDGED hub whose own SOURCE hits a genuine conflict
/// still makes the target preflight's flatten durable: `store_merge_child`
/// publishes the flattened integration branch to origin in the preflight, then
/// returns the source-conflict refusal (no rollback — the markers stay for
/// resolution), so the hub is unwedged everywhere rather than only locally.
#[tokio::test(flavor = "current_thread")]
async fn source_conflict_refusal_leaves_target_unwedged_on_origin() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping source_conflict_refusal_leaves_target_unwedged_on_origin: jj not resolvable"
        );
        return;
    };
    let fx = setup_wedged_merge(&bin).await;
    let origin_int_before = git_stdout(&fx.origin_path, &["rev-parse", fx.int]);

    let err = store_merge_child(&fx.orch, &fx.ctx, "squash")
        .await
        .expect_err("the source conflict must refuse the merge");
    assert!(
        err.contains("recorded a conflict"),
        "the source conflict is surfaced for resolution: {err}"
    );

    // The target repair was published to origin during the preflight (before
    // the source rebase), so origin's integration branch advanced to the
    // flattened (clean) tip even though this child could not complete.
    let origin_int_after = git_stdout(&fx.origin_path, &["rev-parse", fx.int]);
    assert_ne!(
        origin_int_before, origin_int_after,
        "origin's integration branch was unwedged by the durable target flatten"
    );
    assert!(
        crate::jj::push_store_bookmark(&fx.jj, &fx.store, fx.int).is_ok(),
        "the flattened integration branch on origin is conflict-free and pushable"
    );
}

/// Fail-closed: if the preflight cannot PUBLISH the target repair to origin,
/// the whole merge rolls back to the pre-repair state, so local and origin stay
/// identical (both wedged) rather than leaving origin behind a locally-clean
/// target — and the error says the repair could not be published and a retry is
/// safe. Origin is broken (its bare repo is removed) so the preflight push fails.
#[tokio::test(flavor = "current_thread")]
async fn target_repair_push_failure_rolls_the_whole_merge_back() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping target_repair_push_failure_rolls_the_whole_merge_back: jj not resolvable"
        );
        return;
    };
    let fx = setup_wedged_merge(&bin).await;
    // The wedged (pre-repair) local integration tip, and its flatten dest.
    let int_pre = crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap();
    let main_tip = crate::jj::bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    assert_eq!(
        crate::jj::flatten_state(&fx.jj, &fx.store, &main_tip, fx.int).unwrap(),
        crate::jj::FlattenState::IntermediateOnly,
        "precondition: the hub is wedged (clean tip over a conflicted intermediate)"
    );

    // Break origin so the preflight's publish of the target repair fails.
    std::fs::remove_dir_all(&fx.origin_path).unwrap();

    let err = store_merge_child(&fx.orch, &fx.ctx, "squash")
        .await
        .expect_err("a failed target-repair publish must refuse the merge");
    assert!(
        err.contains("could not be published to origin"),
        "the error names the failed target-repair publish: {err}"
    );
    assert!(
        err.contains("safe to retry"),
        "the error advertises a safe retry: {err}"
    );

    // The whole merge rewound: the local integration branch is back at its
    // wedged pre-repair tip (matching origin, which never received the flatten),
    // so there is no local/origin divergence.
    assert_eq!(
        crate::jj::bookmark_commit(&fx.jj, &fx.store, fx.int).unwrap(),
        int_pre,
        "the local integration branch rolled back to its pre-repair (wedged) tip"
    );
    assert_eq!(
        crate::jj::flatten_state(&fx.jj, &fx.store, &main_tip, fx.int).unwrap(),
        crate::jj::FlattenState::IntermediateOnly,
        "the local integration branch is still wedged after the rollback (matches origin)"
    );
}
