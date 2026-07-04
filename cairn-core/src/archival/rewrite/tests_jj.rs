use super::testsupport::*;
use super::*;

// ---------------------------------------------------------------------
// jj forward-mapping (CAIRN-1964): the coordinate a write event is
// archived under must be the CURRENT in-pack commit, not the commit-id
// jj rewrote out from under it.
// ---------------------------------------------------------------------

fn jj_bin() -> Option<String> {
    let bin = std::env::var("CAIRN_JJ_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "jj".to_string());
    Command::new(&bin)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
        .then_some(bin)
}

/// Run a jj command in `dir` against an isolated config; assert success.
fn jj_raw(bin: &str, cfg: &Path, dir: &Path, args: &[&str]) -> String {
    let out = Command::new(bin)
        .current_dir(dir)
        .env("JJ_CONFIG", cfg)
        .env("EDITOR", "true")
        .env("JJ_EDITOR", "true")
        .args(args)
        .output()
        .expect("spawn jj");
    assert!(
        out.status.success(),
        "jj {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Post-teardown counterpart to the live node-diff base-advance regression.
/// Archival must persist the effective fork point as `execution_history.base_sha`
/// so an archived diff can be rendered after the jj graph and worktree are gone.
#[tokio::test]
#[serial_test::serial(jj)]
async fn archived_node_diff_excludes_rebased_external_base_deletion() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping archived_node_diff_excludes_rebased_external_base_deletion: jj not resolvable"
            );
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let origin = tempfile::tempdir().unwrap();
    let proj_dir = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    let proj = proj_dir.path().to_str().unwrap().to_string();

    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_repo(proj_dir.path());
    write_file(proj_dir.path(), "shared.rs", b"base\n");
    write_file(proj_dir.path(), "base-only.rs", b"base-only\n");
    let base = commit_all(proj_dir.path(), "base");
    git(
        proj_dir.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj_dir.path(), &["push", "-q", "origin", "main"]);

    let jj = crate::jj::JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
    let ws = wts.path().join("job");
    let branch = "agent/CAIRN-1-builder-0";
    crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    std::fs::write(ws.join("node.rs"), "node\n").unwrap();
    crate::jj::seal(&jj, &ws, "node work", None).unwrap();

    std::fs::remove_file(proj_dir.path().join("base-only.rs")).unwrap();
    git(proj_dir.path(), &["add", "-A"]);
    git(
        proj_dir.path(),
        &["commit", "-q", "-m", "external base deletion"],
    );
    let advanced_base = git(proj_dir.path(), &["rev-parse", "HEAD"])
        .trim()
        .to_string();
    git(proj_dir.path(), &["push", "-q", "origin", "main"]);
    crate::jj::fetch_remote(&jj, &store, "origin").unwrap();
    crate::jj::reconcile_siblings(
        &jj,
        &store,
        "main@origin",
        &[(branch.to_string(), ws.clone())],
    )
    .unwrap();

    let ws_str = ws.to_str().unwrap().to_string();
    let db = migrated_test_db("archival-base-advance-diff.db").await;
    seed_chain(&db, &proj, &ws_str, Some(&base), Some(&base), false).await;
    insert_event(
        &db,
        "a-text",
        "run",
        1,
        1,
        "assistant",
        &assistant_text("archival trigger"),
    )
    .await;
    archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
        .await
        .unwrap();

    let archived_base = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT base_sha FROM execution_history WHERE execution_id = 'exec'",
                        (),
                    )
                    .await?;
                rows.next().await?.map(|row| row.text(0)).transpose()
            })
        })
        .await
        .unwrap()
        .expect("execution_history row present");
    assert_eq!(archived_base, advanced_base);

    crate::jj::forget_workspace(&jj, &store, branch).unwrap();
    std::fs::remove_dir_all(&ws).unwrap();

    let diff = crate::diff::node_base_tip_diff(&db, "job", &bin, home.path())
        .await
        .unwrap()
        .expect("archived diff present");
    let paths: Vec<&str> = diff.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["node.rs"],
        "archived diff must not attribute the base deletion to the node: {paths:?}"
    );
}

async fn event_coord(db: &LocalDb, id: &str) -> (Option<String>, Option<String>) {
    let id = id.to_string();
    db.read(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT content_commit, content_change_id FROM events WHERE id = ?1",
                    (id.as_str(),),
                )
                .await?;
            let row = rows.next().await?.unwrap();
            DbResult::Ok((row.opt_text(0)?, row.opt_text(1)?))
        })
    })
    .await
    .unwrap()
}

async fn exec_pack_oids(db: &LocalDb) -> std::collections::BTreeSet<String> {
    let idx = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT pack_idx FROM execution_history WHERE execution_id = 'exec'",
                        (),
                    )
                    .await?;
                DbResult::Ok(
                    rows.next()
                        .await?
                        .and_then(|r| r.opt_blob(0).ok().flatten()),
                )
            })
        })
        .await
        .unwrap()
        .expect("execution_history pack_idx present");
    cairn_codec::packfile::pack_index_oids(&idx)
}

/// The non-colocated regression the colocated keystone masks: a production jj
/// workspace (`jj workspace add`) carries `.jj` and NO `.git`, so the
/// `resolve_full`-first path could never resolve a recorded write/run commit
/// there and silently dropped the coordinate. `resolve_coord` must forward-map
/// the recorded SHORT id through jj directly, with no git in the worktree.
#[test]
#[serial_test::serial(jj)]
fn resolve_coord_forward_maps_short_id_in_noncolocated_jj_workspace() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping resolve_coord_forward_maps_short_id_in_noncolocated_jj_workspace: jj not resolvable");
        return;
    };
    let home = tempfile::tempdir().unwrap();
    let proj = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    init_repo(proj.path());
    write_file(proj.path(), "a.txt", b"base\n");
    commit_all(proj.path(), "base");

    let jj = crate::jj::JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("job");
    crate::jj::add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // The real workspace shape: a `.jj` dir and no `.git`.
    assert!(ws.join(".jj").exists());
    assert!(
        !ws.join(".git").exists(),
        "a jj workspace is non-colocated (no .git)"
    );

    // Seal a parent then a write commit; record the write's SHORT, original id.
    std::fs::write(ws.join("p.txt"), "p\n").unwrap();
    crate::jj::seal(&jj, &ws, "P", None).unwrap();
    std::fs::write(ws.join("w.txt"), "w\n").unwrap();
    crate::jj::seal(&jj, &ws, "W", None).unwrap();

    let cfg = home.path().join("probe-config.toml");
    std::fs::write(
        &cfg,
        "ui.paginate = \"never\"\n[user]\nname = \"t\"\nemail = \"t@e.com\"\n",
    )
    .unwrap();
    let w_short = jj_raw(
        &bin,
        &cfg,
        &ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
    );
    let w_full = jj_raw(
        &bin,
        &cfg,
        &ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    let w_change = jj_raw(
        &bin,
        &cfg,
        &ws,
        &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
    );

    // Reword the parent: auto-rebase churns the write's commit-id.
    jj_raw(
        &bin,
        &cfg,
        &ws,
        &["describe", "-r", "@--", "-m", "P reworded"],
    );
    let w_new = jj_raw(
        &bin,
        &cfg,
        &ws,
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    assert_ne!(
        w_full, w_new,
        "precondition: the rebase churned the write commit-id"
    );

    // The fix: resolve_coord forward-maps the short, now-hidden id with no git
    // in the worktree. The plain-git path genuinely cannot resolve it here.
    let mut cache = HashMap::new();
    assert!(
        resolve_full(&ws, &w_short, &mut cache).is_none(),
        "git rev-parse cannot resolve the id in a .jj-only worktree"
    );
    let coord = resolve_coord(&ws, Some(&jj), &w_short, &mut cache)
        .expect("jj resolves the recorded short commit without git in the worktree");
    assert_eq!(
        coord,
        (w_new, Some(w_change)),
        "the coordinate is the forward-mapped commit plus its stable change-id"
    );
}

/// The keystone of CAIRN-1964: a write commit whose commit-id jj churned via
/// auto-rebase is archived under its CURRENT (in-pack) commit-id with the
/// stable change-id recorded, while the bare-git control pins the stale
/// commit-id that the durable pack no longer contains. A drift read after the
/// write proves the unchanged render-and-compare verify still refuses to
/// substitute a forward-mapped commit's bytes.
#[tokio::test]
#[serial_test::serial(jj)]
async fn jj_forward_maps_churned_write_coordinate() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping jj_forward_maps_churned_write_coordinate: jj not resolvable");
        return;
    };

    // A colocated jj repo (git + jj in one dir), so the worktree's git HEAD —
    // which `build_execution_pack` shells out to — tracks jj's post-rebase
    // state and the pack spans the forward-mapped commit.
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().to_str().unwrap().to_string();
    init_repo(dir.path());
    write_file(dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
    let m = commit_all(dir.path(), "base");

    let cfg = dir.path().join("jj-config.toml");
    std::fs::write(
        &cfg,
        "ui.paginate = \"never\"\n[user]\nname = \"t\"\nemail = \"t@e.com\"\n",
    )
    .unwrap();
    jj_raw(&bin, &cfg, dir.path(), &["git", "init", "--colocate"]);

    // Parent commit P (the one we churn), then the write commit W whose change
    // matches `assistant_write` (a.txt patched, c.txt created).
    write_file(dir.path(), "p.txt", b"p\n");
    jj_raw(&bin, &cfg, dir.path(), &["commit", "-m", "P"]);
    write_file(dir.path(), "a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n");
    write_file(dir.path(), "c.txt", b"new file\n");
    jj_raw(&bin, &cfg, dir.path(), &["commit", "-m", "W"]);
    let w_old = jj_raw(
        &bin,
        &cfg,
        dir.path(),
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    let w_change = jj_raw(
        &bin,
        &cfg,
        dir.path(),
        &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
    );

    // Reword P (@--): auto-rebase churns W's commit-id, preserving its
    // change-id; colocated jj advances git HEAD to the rebased W.
    jj_raw(
        &bin,
        &cfg,
        dir.path(),
        &["describe", "-r", "@--", "-m", "P reworded"],
    );
    let w_new = jj_raw(
        &bin,
        &cfg,
        dir.path(),
        &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
    );
    assert_ne!(
        w_old, w_new,
        "precondition: the rebase churned W's commit-id"
    );

    let drift = rendered(&[("file:a.txt", b"DRIFTED CONTENT THE COMMIT NEVER HAD\n")]);

    // Shared event sequence: the committed write (recording W's ORIGINAL id)
    // plus a read pinned to the post-write commit whose recorded bytes drift.
    async fn seed_events(db: &LocalDb, w_old: &str, drift: &str) {
        insert_event(db, "a-w1", "run", 1, 1, "assistant", &assistant_write("w1")).await;
        insert_event(
            db,
            "e-w1",
            "run",
            2,
            2,
            "tool_result",
            &write_result("w1", w_old),
        )
        .await;
        insert_event(
            db,
            "a-rd",
            "run",
            3,
            3,
            "assistant",
            &assistant_read("rd", &["file:a.txt"]),
        )
        .await;
        insert_event(
            db,
            "e-rd",
            "run",
            4,
            4,
            "tool_result",
            &read_result("rd", drift),
        )
        .await;
    }

    let jj = crate::jj::JjEnv::resolve(&bin, dir.path());

    // ---- jj path: forward-map the churned write to the current commit. ----
    let db_ok = migrated_test_db("archival-jj-forward-ok.db").await;
    seed_chain(&db_ok, &repo, &repo, Some(&m), Some(&m), false).await;
    seed_events(&db_ok, &w_old, &drift).await;
    let summary = archive_target(&db_ok, &repo, &repo, &["job".to_string()], Some(&jj))
        .await
        .unwrap();
    assert_eq!(summary.gitcoord_write, 1, "the write is git-addressed");
    assert!(
        summary.mismatch_fallback >= 1,
        "the drift read fails the verify"
    );

    let (commit, change_id) = event_coord(&db_ok, "a-w1").await;
    assert_eq!(
        commit.as_deref(),
        Some(w_new.as_str()),
        "content_commit is the forward-mapped (current) commit, not the churned-away one"
    );
    assert_eq!(
        change_id.as_deref(),
        Some(w_change.as_str()),
        "the stable change-id is persisted as provenance"
    );

    let oids = exec_pack_oids(&db_ok).await;
    assert!(
        oids.contains(&w_new),
        "the forward-mapped commit is inside the durable pack"
    );
    assert!(
        !oids.contains(&w_old),
        "the churned-away commit is excluded from the durable pack"
    );

    let recon = reconstruct_events(&db_ok, load_events(&db_ok).await).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    assert!(
        !by_id["a-w1"].data.contains(STUB_PREFIX),
        "the forward-mapped write reconstructs its committed diff, no stub"
    );
    // The drift read is preserved verbatim (zstd), never silently replaced by
    // the forward-mapped commit's bytes — the verify guardrail holds.
    assert_eq!(
        tool_result_of(by_id["e-rd"]),
        drift,
        "a drifted forward-mapped read keeps its recorded bytes, not the commit's"
    );
    let (rd_commit, _) = event_coord(&db_ok, "e-rd").await;
    assert!(rd_commit.is_none(), "the drifted read is not git-addressed");

    // ---- bare-git control: no forward-map, so the stale id is pinned. ----
    let db_ctrl = migrated_test_db("archival-jj-forward-ctrl.db").await;
    seed_chain(&db_ctrl, &repo, &repo, Some(&m), Some(&m), false).await;
    seed_events(&db_ctrl, &w_old, &drift).await;
    archive_target(&db_ctrl, &repo, &repo, &["job".to_string()], None)
        .await
        .unwrap();
    let (ctrl_commit, ctrl_change) = event_coord(&db_ctrl, "a-w1").await;
    assert_eq!(
        ctrl_commit.as_deref(),
        Some(w_old.as_str()),
        "without forward-mapping the coordinate pins the churned-away commit"
    );
    assert!(
        ctrl_change.is_none(),
        "no change-id is recorded on the plain-git path"
    );
}

/// CAIRN-1956: a production jj workspace is `.jj`-only (no `.git`), so the
/// teardown packfile build — which shelled `git rev-parse HEAD` / `rev-list` /
/// `pack-objects` in the worktree — failed with `bad object` and dropped the
/// WHOLE execution to zstd (no gitcoord rows, no `execution_history`). The
/// colocated keystone above masked this because it carries a `.git`. With the
/// pack routed to the project repo (where `jj git export` lands the objects)
/// and the tip resolved from jj's `@-`, a merged jj session archives: the
/// write is git-addressed with its change-id, the durable pack holds the
/// commit, and reconstruction renders the committed diff.
#[tokio::test]
#[serial_test::serial(jj)]
async fn jj_noncolocated_workspace_archives_via_project_repo() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping jj_noncolocated_workspace_archives_via_project_repo: jj not resolvable"
        );
        return;
    };

    let home = tempfile::tempdir().unwrap();
    let proj_dir = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    let proj = proj_dir.path().to_str().unwrap().to_string();

    init_repo(proj_dir.path());
    write_file(proj_dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
    let anchor = commit_all(proj_dir.path(), "base");

    let jj = crate::jj::JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
    let ws = wts.path().join("job");
    crate::jj::add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // The real production shape: a `.jj` dir and NO `.git`. This is exactly
    // where the old worktree-local git invocations had no repository.
    assert!(ws.join(".jj").exists());
    assert!(
        !ws.join(".git").exists(),
        "a jj workspace is non-colocated (no .git)"
    );

    // The write commit matching `assistant_write` (a.txt patched, c.txt added).
    std::fs::write(ws.join("a.txt"), "ALPHA\nbeta\ngamma\ndelta\nepsilon\n").unwrap();
    std::fs::write(ws.join("c.txt"), "new file\n").unwrap();
    crate::jj::seal(&jj, &ws, "W", None).unwrap();
    let w_full = crate::jj::head_commit(&jj, &ws).unwrap();

    let ws_str = ws.to_str().unwrap().to_string();
    let db = migrated_test_db("archival-jj-noncolocated.db").await;
    seed_chain(&db, &proj, &ws_str, Some(&anchor), Some(&anchor), false).await;
    insert_event(
        &db,
        "a-w1",
        "run",
        1,
        1,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    insert_event(
        &db,
        "e-w1",
        "run",
        2,
        2,
        "tool_result",
        &write_result("w1", &w_full),
    )
    .await;

    // The regression: before the fix this returned Err ("bad object") because
    // git ran in the `.jj`-only worktree. It must now succeed.
    let summary = archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
        .await
        .expect("archival succeeds for a non-colocated jj workspace");
    assert_eq!(
        summary.gitcoord_write, 1,
        "the write is git-addressed, not dropped to zstd"
    );

    let (commit, change_id) = event_coord(&db, "a-w1").await;
    assert_eq!(
        commit.as_deref(),
        Some(w_full.as_str()),
        "content_commit pins the write commit resolved through the project repo"
    );
    assert!(
        change_id.is_some(),
        "the durable jj change-id is recorded once the pack builds (CAIRN-1964 path)"
    );

    let oids = exec_pack_oids(&db).await;
    assert!(
        oids.contains(&w_full),
        "the write commit is inside the durable execution pack"
    );

    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    assert!(
        !by_id["a-w1"].data.contains(STUB_PREFIX),
        "reconstruction renders the committed diff, not a 'content no longer resolvable' stub"
    );
}

// ---------------------------------------------------------------------
// CAIRN-1988: no-pack (empty-range) archival durability under git gc for
// MERGED jj executions. The read path resolves purely by `content_commit`
// against the project ODB; with a NULL `execution_history.pack` there is no
// pack to fall back on, so reconstruction depends entirely on the git object
// surviving the project repo's `git gc`. These tests lock that down.
// ---------------------------------------------------------------------

/// True when the execution's `execution_history` row stores a NULL `pack`
/// (the empty-range classification — everything reachable from the tip is
/// already durable on the default branch, so reconstruction is ODB-only).
async fn exec_pack_is_null(db: &LocalDb) -> bool {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT pack FROM execution_history WHERE execution_id = 'exec'",
                    (),
                )
                .await?;
            let row = rows.next().await?.expect("execution_history row present");
            DbResult::Ok(row.opt_blob(0)?.is_none())
        })
    })
    .await
    .unwrap()
}

/// Run git in `dir` returning `(success, trimmed stdout)` WITHOUT panicking on
/// a non-zero exit, so a boolean query (`merge-base --is-ancestor`, `cat-file
/// -t`) is usable where `testutil::git` would abort the test on exit 1.
fn git_check(dir: &Path, args: &[&str]) -> (bool, String) {
    let out = Command::new("git")
        .current_dir(dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .args(args)
        .output()
        .expect("spawn git");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).trim().to_string(),
    )
}

/// The dominant production case end to end: an empty-range (NULL `pack`)
/// MERGED jj execution still reconstructs its read content AND its write diff
/// from the project ODB ALONE after its `.jj` workspace is forgotten/removed
/// AND the project repo is `git gc --prune=now`-ed. The store-owns-merge fold
/// folds the seal onto the default branch, so the archival range is empty (no
/// pack); the durable branch ref then keeps `content_commit` alive across gc.
/// No predicate fix is required — this regression test is the deliverable, and
/// the at-risk path (NULL pack + workspace-forget + gc) had zero coverage.
#[tokio::test]
#[serial_test::serial(jj)]
async fn no_pack_merged_execution_reconstructs_after_workspace_forget_and_git_gc() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping no_pack_merged_execution_reconstructs_after_workspace_forget_and_git_gc: jj not resolvable"
            );
        return;
    };

    let home = tempfile::tempdir().unwrap();
    let proj_dir = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    let proj = proj_dir.path().to_str().unwrap().to_string();

    init_repo(proj_dir.path());
    write_file(proj_dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
    let anchor = commit_all(proj_dir.path(), "base");

    let jj = crate::jj::JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
    let ws = wts.path().join("job");
    let branch = "agent/CAIRN-1-builder-0";
    crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    // True production shape: a `.jj` workspace, no `.git` in the worktree.
    assert!(ws.join(".jj").exists());
    assert!(
        !ws.join(".git").exists(),
        "a jj workspace is non-colocated (no .git)"
    );

    // The write commit matching `assistant_write` (a.txt patched, c.txt added).
    std::fs::write(ws.join("a.txt"), "ALPHA\nbeta\ngamma\ndelta\nepsilon\n").unwrap();
    std::fs::write(ws.join("c.txt"), "new file\n").unwrap();
    crate::jj::seal(&jj, &ws, "W", None).unwrap();
    let w = crate::jj::head_commit(&jj, &ws).unwrap();

    // Store-owns-merge fold: FF `main` to the child's seal and export it to the
    // project git, so `rev-list w --not anchor main` is empty → NULL pack.
    crate::jj::merge_into_bookmark(&jj, &store, "main", branch).unwrap();

    let ws_str = ws.to_str().unwrap().to_string();
    let db = migrated_test_db("archival-nopack-merged.db").await;
    seed_chain(&db, &proj, &ws_str, Some(&anchor), Some(&anchor), false).await;

    // A committed write, then a read of the committed file (git-addressed to w).
    let stored = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
    insert_event(
        &db,
        "a-w1",
        "run",
        1,
        1,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    insert_event(
        &db,
        "e-w1",
        "run",
        2,
        2,
        "tool_result",
        &write_result("w1", &w),
    )
    .await;
    insert_event(
        &db,
        "a-rd",
        "run",
        3,
        3,
        "assistant",
        &assistant_read("rd", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-rd",
        "run",
        4,
        4,
        "tool_result",
        &read_result("rd", &stored),
    )
    .await;

    let summary = archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
        .await
        .unwrap();

    // The NULL-pack precondition this test exists for: both events are git
    // coordinates, and `execution_history.pack` is NULL (ODB-only).
    assert_eq!(summary.gitcoord_read, 1, "the read is git-addressed");
    assert_eq!(summary.gitcoord_write, 1, "the write is git-addressed");
    assert!(
        exec_pack_is_null(&db).await,
        "an empty-range merged execution stores a NULL pack (ODB-only reconstruction)"
    );
    let (w_commit, _) = event_coord(&db, "a-w1").await;
    assert_eq!(
        w_commit.as_deref(),
        Some(w.as_str()),
        "the write pins the seal commit"
    );
    let (rd_commit, _) = event_coord(&db, "e-rd").await;
    assert_eq!(
        rd_commit.as_deref(),
        Some(w.as_str()),
        "the read pins the seal commit"
    );

    // Teardown the way a merged execution's worktree is reclaimed: forget the
    // workspace, remove it, and `git gc --prune=now` the project store.
    crate::jj::forget_workspace(&jj, &store, branch).unwrap();
    std::fs::remove_dir_all(&ws).unwrap();
    git(proj_dir.path(), &["gc", "--prune=now"]);

    // The crux: reconstruct from the project ODB ALONE (no pack, no workspace).
    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    assert!(
        !tool_result_of(by_id["e-rd"]).contains(STUB_PREFIX),
        "the read reconstructs from the post-gc ODB, not a 'no longer resolvable' stub"
    );
    assert_eq!(
        tool_result_of(by_id["e-rd"]),
        stored,
        "the read returns its exact original bytes after workspace-forget + git gc"
    );
    assert!(
        !by_id["a-w1"].data.contains(STUB_PREFIX),
        "the write diff still renders from the durable seal commit"
    );
}

/// Hardening for the non-obvious guarantee: a `content_commit` that churned to
/// a HIDDEN commit-id AFTER teardown (the base advanced and the change
/// auto-rebased, so the recorded commit is reachable from NO `refs/heads/*`)
/// still reconstructs after `git gc --prune=now`, because jj pins it with a
/// `refs/jj/keep/*` ref in the backing git repo. If this ever fails it is the
/// early-warning that the NULL-pack read path needs a change-id fallback
/// (resolve `content_change_id` → current commit via the still-present store).
#[tokio::test]
#[serial_test::serial(jj)]
async fn no_pack_churned_content_commit_survives_git_gc_via_keep_refs() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping no_pack_churned_content_commit_survives_git_gc_via_keep_refs: jj not resolvable"
            );
        return;
    };

    let home = tempfile::tempdir().unwrap();
    let proj_dir = tempfile::tempdir().unwrap();
    let wts = tempfile::tempdir().unwrap();
    let proj = proj_dir.path().to_str().unwrap().to_string();

    init_repo(proj_dir.path());
    write_file(proj_dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
    let anchor = commit_all(proj_dir.path(), "base");

    let jj = crate::jj::JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
    let ws = wts.path().join("job");
    let branch = "agent/CAIRN-1-builder-0";
    crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    std::fs::write(ws.join("a.txt"), "ALPHA\nbeta\ngamma\ndelta\nepsilon\n").unwrap();
    std::fs::write(ws.join("c.txt"), "new file\n").unwrap();
    crate::jj::seal(&jj, &ws, "W", None).unwrap();
    let w = crate::jj::head_commit(&jj, &ws).unwrap();

    // Fold main → w so the archival range is empty (NULL pack).
    crate::jj::merge_into_bookmark(&jj, &store, "main", branch).unwrap();

    let ws_str = ws.to_str().unwrap().to_string();
    let db = migrated_test_db("archival-nopack-churn.db").await;
    seed_chain(&db, &proj, &ws_str, Some(&anchor), Some(&anchor), false).await;
    let stored = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
    insert_event(
        &db,
        "a-w1",
        "run",
        1,
        1,
        "assistant",
        &assistant_write("w1"),
    )
    .await;
    insert_event(
        &db,
        "e-w1",
        "run",
        2,
        2,
        "tool_result",
        &write_result("w1", &w),
    )
    .await;
    insert_event(
        &db,
        "a-rd",
        "run",
        3,
        3,
        "assistant",
        &assistant_read("rd", &["file:a.txt"]),
    )
    .await;
    insert_event(
        &db,
        "e-rd",
        "run",
        4,
        4,
        "tool_result",
        &read_result("rd", &stored),
    )
    .await;

    let summary = archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
        .await
        .unwrap();
    assert_eq!(summary.gitcoord_read, 1);
    assert_eq!(summary.gitcoord_write, 1);
    assert!(exec_pack_is_null(&db).await, "NULL pack precondition");
    let (_, change_id) = event_coord(&db, "a-w1").await;
    let change_id = change_id.expect("the durable jj change-id is recorded");

    // Teardown: forget + remove the workspace.
    crate::jj::forget_workspace(&jj, &store, branch).unwrap();
    std::fs::remove_dir_all(&ws).unwrap();

    // Churn AFTER teardown: reword the write change in the store so its
    // commit-id is rewritten (w → w_new). Both `main` and the child bookmark
    // advance to w_new, so the recorded `content_commit` (w) is now reachable
    // from no branch — exactly the worst case the worry named.
    let cfg = home.path().join("probe-config.toml");
    std::fs::write(
        &cfg,
        "ui.paginate = \"never\"\n[user]\nname = \"t\"\nemail = \"t@e.com\"\n",
    )
    .unwrap();
    jj_raw(
        &bin,
        &cfg,
        &store,
        &[
            "describe",
            "-r",
            &change_id,
            "-m",
            "W reworded",
            "--ignore-working-copy",
        ],
    );
    let w_new = jj_raw(
        &bin,
        &cfg,
        &store,
        &[
            "log",
            "-r",
            &change_id,
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
    );
    assert_ne!(
        w, w_new,
        "precondition: the post-teardown churn rewrote the write commit-id"
    );
    // Export the rewritten bookmarks so the project git heads advance off w.
    jj_raw(
        &bin,
        &cfg,
        &store,
        &["git", "export", "--ignore-working-copy"],
    );
    let (hidden, _) = git_check(
        proj_dir.path(),
        &["merge-base", "--is-ancestor", &w, "main"],
    );
    assert!(
        !hidden,
        "precondition: the recorded content_commit is hidden (no longer on any branch)"
    );

    // gc the project store: only jj's keep-ref can save the hidden commit now.
    git(proj_dir.path(), &["gc", "--prune=now"]);
    let (alive, kind) = git_check(proj_dir.path(), &["cat-file", "-t", &w]);
    assert!(
            alive && kind == "commit",
            "jj's keep-refs pin the hidden content_commit across git gc (got alive={alive} kind={kind:?})"
        );

    // The durability guarantee: reconstruction still returns the original bytes
    // from the ODB alone, with no pack and the commit reachable from no branch.
    let recon = reconstruct_events(&db, load_events(&db).await).await;
    let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
    assert_eq!(
        tool_result_of(by_id["e-rd"]),
        stored,
        "a churned-then-gc'd content_commit still reconstructs its original bytes"
    );
    assert!(
        !by_id["a-w1"].data.contains(STUB_PREFIX),
        "the write diff renders from the keep-ref-pinned hidden commit"
    );
}
