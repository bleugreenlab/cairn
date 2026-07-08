use super::*;
use crate::mcp::git::GitAuthor;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

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

#[test]
fn base_marker_round_trips() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join(".jj")).unwrap();

    // Absent marker reads as None.
    assert_eq!(read_base_marker(dir.path()), None);

    // Branch + rev round-trip through the two-line format, landing beside
    // the branch marker in the non-snapshotted `.jj` dir.
    write_base_marker(dir.path(), "agent/CAIRN-2091-coordinator-0", "e4555f70").unwrap();
    assert_eq!(
        read_base_marker(dir.path()),
        Some((
            "agent/CAIRN-2091-coordinator-0".to_string(),
            "e4555f70".to_string()
        ))
    );
    assert!(dir.path().join(".jj").join("cairn-base").exists());

    // A branch-only marker yields an empty rev rather than failing.
    write_base_marker(dir.path(), "main", "").unwrap();
    assert_eq!(
        read_base_marker(dir.path()),
        Some(("main".to_string(), String::new()))
    );
}

#[test]
fn project_root_marker_round_trips() {
    let dir = TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join(".jj")).unwrap();

    assert_eq!(read_project_root_marker(dir.path()), None);

    write_project_root_marker(dir.path(), Path::new("/Users/dev/projects/cairn")).unwrap();
    assert_eq!(
        read_project_root_marker(dir.path()),
        Some(PathBuf::from("/Users/dev/projects/cairn"))
    );
    assert!(dir.path().join(".jj").join("cairn-project-root").exists());
}

/// Provision a real non-colocated workspace, record the base marker as
/// production does (after `add_workspace`), and assert it persists across a
/// seal — the `.jj` dir is never snapshotted, so the marker is invisible to
/// the working-copy commit, exactly like the branch marker.
#[test]
#[serial_test::serial(jj)]
fn base_marker_provisions_and_survives_a_seal() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping base_marker_provisions_and_survives_a_seal: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();
    write_base_marker(&ws, "main", "deadbeef").unwrap();
    assert_eq!(
        read_base_marker(&ws),
        Some(("main".to_string(), "deadbeef".to_string()))
    );

    // Seal real work; the marker still reads (non-snapshotted).
    std::fs::write(ws.join("f.rs"), "code\n").unwrap();
    seal(&jj, &ws, "work", None).unwrap();
    assert_eq!(
        read_base_marker(&ws),
        Some(("main".to_string(), "deadbeef".to_string()))
    );
}

/// CAIRN-2260 (b)+(c): a `when:write` check that rewrites a TRACKED file folds
/// that edit into the just-sealed commit and leaves `@` clean; a GITIGNORED
/// write the same check makes is neither folded into the commit nor counted as
/// working-copy dirt.
#[test]
#[serial_test::serial(jj)]
fn fold_worktree_into_seal_amends_tracked_and_skips_gitignored() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping fold_worktree_into_seal_amends_tracked_and_skips_gitignored: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-1-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    // write1: a normal path-scoped seal of source + a .gitignore that ignores caches.
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::write(ws.join("src/foo.ts"), "const x=1\n").unwrap();
    std::fs::write(ws.join(".gitignore"), "*.cache\n").unwrap();
    seal_paths(&jj, &ws, "edit1", None, &["src/foo.ts", ".gitignore"]).unwrap();
    let sealed_before = head_commit(&jj, &ws).unwrap();

    // The check reformats the tracked source AND writes a gitignored cache.
    std::fs::write(ws.join("src/foo.ts"), "const x = 1;\n").unwrap();
    std::fs::write(ws.join("vitest.cache"), "junk\n").unwrap();

    let outcome = fold_worktree_into_seal(&jj, &ws)
        .unwrap()
        .expect("a tracked reformat folds into the seal");
    assert_eq!(outcome.folded_files, vec!["src/foo.ts".to_string()]);

    // `@` is clean == the amended tip; the seal's commit id changed (amended).
    assert!(
        !is_working_copy_dirty(&jj, &ws).unwrap(),
        "@ is clean after the fold"
    );
    assert_ne!(
        sealed_before,
        head_commit(&jj, &ws).unwrap(),
        "the seal was amended in place"
    );

    // The reformat is in the commit; the gitignored cache is not.
    let foo_in_commit = jj
        .run(&ws, &["file", "show", "-r", "@-", "src/foo.ts"], "show foo")
        .unwrap();
    assert!(
        foo_in_commit.contains("const x = 1;"),
        "the reformatted source is folded into the commit: {foo_in_commit}"
    );
    let committed = jj
        .run(&ws, &["diff", "-r", "@-", "--name-only"], "committed files")
        .unwrap();
    assert!(
        committed.contains("src/foo.ts"),
        "the source file is in the amended commit: {committed}"
    );
    assert!(
        !committed.contains("vitest.cache"),
        "the gitignored cache must NOT be committed: {committed}"
    );
}

/// CAIRN-2260 (a): with the check's changes folded into the seal, a concurrent
/// base advance (a sibling merge that rebases this workspace) in the lock-free
/// check window leaves the NEXT write's seal clean — no stale / divergent /
/// behind-tip wedge, and no divergent `@` twin (the bug's signature).
#[test]
#[serial_test::serial(jj)]
fn folded_check_keeps_next_seal_clean_under_a_concurrent_advance() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping folded_check_keeps_next_seal_clean_under_a_concurrent_advance: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Coordinator integration bookmark + one sibling builder branched off it.
    let int = "agent/CAIRN-1-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let branch = "agent/CAIRN-2-builder-0";
    let ws = wts.path().join("builder");
    add_workspace(&jj, &store, &ws, branch, int, None).unwrap();

    // write1: a path-scoped seal on the builder.
    std::fs::write(ws.join("a.rs"), "fn a() {}\n").unwrap();
    seal_paths(&jj, &ws, "edit1", None, &["a.rs"]).unwrap();

    // The check reformats a.rs (tracked dirt in @); the fix folds it into the seal.
    std::fs::write(ws.join("a.rs"), "fn a() {} // fmt\n").unwrap();
    fold_worktree_into_seal(&jj, &ws)
        .unwrap()
        .expect("the tracked reformat folds into the seal");
    assert!(
        !is_working_copy_dirty(&jj, &ws).unwrap(),
        "@ is clean after the fold"
    );

    // A child merges into the integration branch: advance its tip with a
    // different file, then reconcile rebases the sibling onto the new tip
    // (the concurrent advance that, on a check-dirtied @, used to wedge).
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("z.rs"), "fn z() {}\n").unwrap();
    jj.run(&store, &["describe", "-m", "child merged"], "describe")
        .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    let report = reconcile_siblings(&jj, &store, int, &[(branch.to_string(), ws.clone())]).unwrap();
    assert_eq!(report.rebased_clean, vec![branch.to_string()]);

    // write2: the next seal must SUCCEED and leave `@` clean == tip.
    std::fs::write(ws.join("b.rs"), "fn b() {}\n").unwrap();
    seal_paths(&jj, &ws, "edit2", None, &["b.rs"])
        .expect("the second seal succeeds after a concurrent advance");
    assert!(
        !is_working_copy_dirty(&jj, &ws).unwrap(),
        "@ is clean after the second seal"
    );

    // No divergent twin of `@` (the bug's signature): change_id(@) resolves to one.
    let cid = snapshot_change_id(&jj, &ws).unwrap();
    let twins = jj
        .run(
            &ws,
            &[
                "log",
                "-r",
                &format!("change_id({})", cid.trim()),
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
            "divergence check",
        )
        .unwrap();
    assert_eq!(
        twins.lines().filter(|l| !l.trim().is_empty()).count(),
        1,
        "no divergent @ twin after the fold + advance + reseal"
    );
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

fn init_project(repo: &Path) {
    git(repo, &["init", "-q", "-b", "main"]);
    git(repo, &["config", "user.email", "p@e.com"]);
    git(repo, &["config", "user.name", "P"]);
    std::fs::write(repo.join("shared.rs"), "base\n").unwrap();
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "base"]);
}

/// Capture trimmed stdout of a git command (test helper).
fn git_stdout(repo: &Path, args: &[&str]) -> String {
    let out = crate::env::git()
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn advance_project(repo: &Path) -> String {
    std::fs::write(repo.join("more.rs"), "more\n").unwrap();
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "advance"]);
    let out = crate::env::git()
        .args(["rev-parse", "HEAD"])
        .current_dir(repo)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A store created earlier must re-import the backing git when the project
/// advances, or a later job based on the new head fails to provision with
/// `Revision <sha> doesn't exist`.
#[test]
#[serial_test::serial(jj)]
fn add_workspace_after_project_git_advances() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping add_workspace_after_project_git_advances: jj not resolvable via CAIRN_JJ_BIN/PATH"
            );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");

    ensure_project_store(&jj, &store, proj.path()).unwrap();
    add_workspace(
        &jj,
        &store,
        &wts.path().join("a"),
        "agent/CAIRN-1-x-0",
        "main",
        None,
    )
    .unwrap();

    // The project's git advances after the store was first created.
    let new_sha = advance_project(proj.path());

    // ensure_project_store is a no-op for the existing store dir, but must
    // re-import so the advanced base resolves for the next job.
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    add_workspace(
        &jj,
        &store,
        &wts.path().join("b"),
        "agent/CAIRN-2-x-0",
        &new_sha,
        None,
    )
    .unwrap();
    assert!(
        is_jj_dir(&wts.path().join("b")),
        "a later job on the advanced base must provision"
    );
}

/// The Coordinator topology, WITHOUT any manual bookmark creation: a
/// coordinator workspace based on `main`, then a child workspace based on the
/// coordinator's integration branch. Before the fix the child add failed with
/// `Revision <branch> doesn't exist`, because a coordinator never seals and so
/// its integration bookmark was never created. `add_workspace` now creates
/// the branch bookmark at base, so the integration branch is a resolvable,
/// pushable bookmark from creation and a child bases off it.
#[test]
#[serial_test::serial(jj)]
fn child_workspace_bases_off_unsealed_coordinator_branch() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping child_workspace_bases_off_unsealed_coordinator_branch: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let coordinator = "agent/CAIRN-1940-coordinator-0";
    let child = "agent/CAIRN-1959-builder-0";

    // The coordinator workspace bases on main and never seals.
    add_workspace(
        &jj,
        &store,
        &wts.path().join("coord"),
        coordinator,
        "main",
        None,
    )
    .unwrap();

    // Its integration branch resolves as a bookmark immediately (no seal).
    assert!(
        bookmark_commit(&jj, &store, coordinator).is_some(),
        "add_workspace must create the workspace's branch bookmark at base"
    );

    // The child bases off the coordinator's integration branch — this is the
    // add that failed with `Revision ... doesn't exist` before the fix.
    add_workspace(
        &jj,
        &store,
        &wts.path().join("child"),
        child,
        coordinator,
        None,
    )
    .unwrap();
    assert!(
        is_jj_dir(&wts.path().join("child")),
        "child workspace based on the unsealed coordinator branch must provision"
    );
}

/// A failed `jj workspace add` registers the workspace name (and writes a
/// half-created `.jj` dir) before it resolves `-r`, so a retried job would hit
/// `Workspace named X already exists` / `Destination path exists`.
/// `add_workspace` forgets the stale registration and clears the dir, so the
/// retry recovers and provisions cleanly.
#[test]
#[serial_test::serial(jj)]
fn add_workspace_recovers_from_half_created_workspace() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping add_workspace_recovers_from_half_created_workspace: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-1-builder-0";
    let ws = wts.path().join("job");
    let name = workspace_name_for_branch(branch);

    // Simulate the live failure: an add against an unresolvable revision still
    // registers the workspace name and writes a `.jj` dir, then errors.
    let _ = jj.run(
        &store,
        &[
            "workspace",
            "add",
            "--name",
            &name,
            "-r",
            "does-not-exist",
            &ws.to_string_lossy(),
        ],
        "seed half-created workspace",
    );
    assert!(
        ws.join(".jj").exists(),
        "the failed add still wrote a stale .jj dir"
    );

    // The retry recovers rather than failing on the stale registration/dir.
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    assert!(is_jj_dir(&ws), "the retried add provisions the workspace");
    assert!(
        bookmark_commit(&jj, &store, branch).is_some(),
        "the retried add creates the branch bookmark"
    );
}

/// The whole topology, proven in-tree: one shared store backed by the
/// project `.git`, two sibling workspaces on one graph, a `.jj`-only
/// workspace whose branch resolves via the marker, a seal that lands one
/// addressable commit reachable in the project's object db, and a discard.
#[test]
#[serial_test::serial(jj)]
fn shared_store_workspaces_seal_and_discard() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping shared_store_workspaces_seal_and_discard: jj not resolvable via CAIRN_JJ_BIN/PATH"
            );
        return;
    };
    let home = TempDir::new().unwrap(); // cairn home: JJ_CONFIG + the store live here
    let proj = TempDir::new().unwrap(); // the user's project checkout
    let wts = TempDir::new().unwrap(); // worktrees root
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    let author = GitAuthor::new("Alice", "alice@example.com");

    // Shared store backed by the project's .git; user checkout stays clean.
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    assert!(is_jj_dir(&store));
    assert!(
        !proj.path().join(".jj").exists(),
        "the user's checkout must stay pristine (no .jj)"
    );
    // Idempotent.
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Two sibling job workspaces off the one store.
    let a = wts.path().join("jobA");
    let b = wts.path().join("jobB");
    add_workspace(
        &jj,
        &store,
        &a,
        "agent/CAIRN-1-builder-0",
        "main",
        Some(&author),
    )
    .unwrap();
    add_workspace(
        &jj,
        &store,
        &b,
        "agent/CAIRN-2-builder-0",
        "main",
        Some(&author),
    )
    .unwrap();

    // Branch resolves inside the .jj-only workspace via the marker.
    assert!(!a.join(".git").exists(), "workspace is .jj-only (no .git)");
    assert_eq!(
        read_branch_marker(&a).as_deref(),
        Some("agent/CAIRN-1-builder-0")
    );

    // Shared graph: one op log / one repo, both workspaces listed.
    let list = jj
        .run(&store, &["workspace", "list"], "workspace list")
        .unwrap();
    assert!(
        list.contains("agent-CAIRN-1-builder-0") && list.contains("agent-CAIRN-2-builder-0"),
        "both workspaces share one store: {list}"
    );

    // Seal in jobA: clean @, edit, dirty, seal -> one addressable commit.
    assert!(!is_working_copy_dirty(&jj, &a).unwrap());
    std::fs::write(a.join("mod.rs"), "code\n").unwrap();
    assert!(is_working_copy_dirty(&jj, &a).unwrap());
    let res = seal(&jj, &a, "agent work", Some(&author)).unwrap();
    assert!(!res.sha.is_empty(), "seal returns the sealed commit id");
    assert!(
        !is_working_copy_dirty(&jj, &a).unwrap(),
        "@ is empty again after seal"
    );

    // The sealed commit is reachable in the PROJECT's object db (shared backend).
    let full = jj
        .run(
            &a,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "id",
        )
        .unwrap();
    assert!(
        crate::env::git()
            .args(["cat-file", "-t", &full])
            .current_dir(proj.path())
            .output()
            .unwrap()
            .status
            .success(),
        "sealed commit {full} must be reachable in the project .git"
    );

    // Discard in jobB returns @ to clean and removes the dirt.
    std::fs::write(b.join("scratch.rs"), "junk\n").unwrap();
    assert!(is_working_copy_dirty(&jj, &b).unwrap());
    discard(&jj, &b).unwrap();
    assert!(!is_working_copy_dirty(&jj, &b).unwrap());
    assert!(!b.join("scratch.rs").exists(), "discard removes the dirt");
}

/// The `--git` parser classifies modify/add/delete and counts `+`/`-` lines
/// per file. Input is verbatim `jj diff --git` output (jj 0.42).
#[test]
fn parse_git_diff_classifies_modify_add_delete_with_counts() {
    let diff = "\
diff --git a/a.txt b/a.txt
index df967b96a5..f6474b4ea7 100644
--- a/a.txt
+++ b/a.txt
@@ -1,1 +1,3 @@
 base
+more
+loose
diff --git a/b.txt b/b.txt
deleted file mode 100644
index 3367afdbbf..0000000000
--- a/b.txt
+++ /dev/null
@@ -1,1 +0,0 @@
-old
diff --git a/c.txt b/c.txt
new file mode 100644
index 0000000000..fa49b07797
--- /dev/null
+++ b/c.txt
@@ -0,0 +1,1 @@
+new file
";
    let changes = parse_git_diff(diff);
    assert_eq!(changes.len(), 3, "{changes:?}");

    let a = &changes[0];
    assert_eq!(a.path, "a.txt");
    assert_eq!(a.status, "modified");
    assert_eq!((a.additions, a.deletions), (2, 0));
    assert_eq!(a.previous_path, None);

    let b = &changes[1];
    assert_eq!(b.path, "b.txt");
    assert_eq!(b.status, "deleted");
    assert_eq!((b.additions, b.deletions), (0, 1));

    let c = &changes[2];
    assert_eq!(c.path, "c.txt");
    assert_eq!(c.status, "added");
    assert_eq!((c.additions, c.deletions), (1, 0));
}

/// A rename carries the previous path and counts only real content lines
/// (the `rename from/to` headers are not edits).
#[test]
fn parse_git_diff_reports_rename_with_previous_path() {
    let diff = "\
diff --git a/orig.txt b/renamed.txt
rename from orig.txt
rename to renamed.txt
index 83db48f84e..788e1a6204 100644
--- a/orig.txt
+++ b/renamed.txt
@@ -1,3 +1,4 @@
 line1
 line2
 line3
+added
";
    let changes = parse_git_diff(diff);
    assert_eq!(changes.len(), 1, "{changes:?}");
    let r = &changes[0];
    assert_eq!(r.status, "renamed");
    assert_eq!(r.path, "renamed.txt");
    assert_eq!(r.previous_path.as_deref(), Some("orig.txt"));
    assert_eq!((r.additions, r.deletions), (1, 0));
}

/// A removed line whose content begins with `-` (e.g. a markdown rule) must
/// count as a deletion, not be mistaken for a `--- ` file header. The header
/// `---`/`+++` lines only precede the first `@@`.
#[test]
fn parse_git_diff_counts_dashy_content_lines_inside_hunks() {
    let diff = "\
diff --git a/doc.md b/doc.md
index 1111111111..2222222222 100644
--- a/doc.md
+++ b/doc.md
@@ -1,2 +1,2 @@
 title
---- old rule
+++ new rule
";
    let changes = parse_git_diff(diff);
    assert_eq!(changes.len(), 1, "{changes:?}");
    let d = &changes[0];
    assert_eq!(d.path, "doc.md");
    assert_eq!(d.status, "modified");
    assert_eq!((d.additions, d.deletions), (1, 1));
}

#[test]
fn parse_git_diff_empty_is_empty() {
    assert!(parse_git_diff("").is_empty());
}

/// A non-jj directory yields `None` so the projection falls back to the
/// recorded cache. No jj binary is invoked (the `.jj` probe short-circuits).
#[test]
fn node_changed_files_returns_none_for_non_jj_dir() {
    let jj = JjEnv::resolve("jj", Path::new("/tmp"));
    let dir = TempDir::new().unwrap();
    assert!(node_changed_files(&jj, dir.path(), Some("main"), None).is_none());
}

fn setup_base_advance_with_node_change(
    bin: &str,
    rebase_workspace: bool,
) -> (
    TempDir,
    TempDir,
    TempDir,
    TempDir,
    JjEnv,
    std::path::PathBuf,
    String,
) {
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();

    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    std::fs::write(proj.path().join("base-only.rs"), "base-only\n").unwrap();
    git(proj.path(), &["add", "-A"]);
    git(proj.path(), &["commit", "-q", "-m", "add base-only"]);
    let base = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);

    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("job");
    let branch = "agent/CAIRN-1-builder-0";
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    std::fs::write(ws.join("node.rs"), "node\n").unwrap();
    seal(&jj, &ws, "node work", None).unwrap();

    std::fs::remove_file(proj.path().join("base-only.rs")).unwrap();
    git(proj.path(), &["add", "-A"]);
    git(
        proj.path(),
        &["commit", "-q", "-m", "external base deletion"],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);
    fetch_remote(&jj, &store, "origin").unwrap();

    if rebase_workspace {
        reconcile_siblings(
            &jj,
            &store,
            "main@origin",
            &[(branch.to_string(), ws.clone())],
        )
        .unwrap();
    }

    (home, origin, proj, wts, jj, ws, base)
}

/// The graph-derived change set lists a SEALED file (the omission this fixes)
/// AND a loose, un-sealed `@` edit, while leaving the unchanged base file
/// out. Drives the real shared-store seal path, mirroring the seal/discard
/// fixture.
#[test]
#[serial_test::serial(jj)]
fn node_changed_files_derives_sealed_and_loose_against_base() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping node_changed_files_derives_sealed_and_loose_against_base: jj not resolvable via CAIRN_JJ_BIN/PATH"
            );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    let author = GitAuthor::new("Alice", "alice@example.com");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let a = wts.path().join("jobA");
    add_workspace(
        &jj,
        &store,
        &a,
        "agent/CAIRN-1-builder-0",
        "main",
        Some(&author),
    )
    .unwrap();

    // Seal a new file: it must appear in the graph-derived change set.
    std::fs::write(a.join("mod.rs"), "code\n").unwrap();
    let res = seal(&jj, &a, "agent work", Some(&author)).unwrap();
    assert!(!res.sha.is_empty());

    // A loose, un-sealed edit, snapshotted into `@` by a jj op (as the
    // agent's own operations do before a reviewer reads `/changed`).
    std::fs::write(a.join("loose.rs"), "wip\n").unwrap();
    assert!(is_working_copy_dirty(&jj, &a).unwrap());

    let changes = node_changed_files(&jj, &a, Some("main"), None)
        .expect("jj workspace resolves the base bookmark");
    let by_path: std::collections::HashMap<&str, &GraphFileChange> =
        changes.iter().map(|c| (c.path.as_str(), c)).collect();

    let sealed = by_path.get("mod.rs").expect("sealed file present in graph");
    assert_eq!(sealed.status, "added");
    assert_eq!(sealed.additions, 1);

    let loose = by_path
        .get("loose.rs")
        .expect("loose @ edit present in graph");
    assert_eq!(loose.status, "added");

    assert!(
        !by_path.contains_key("shared.rs"),
        "the unchanged base file must not appear: {changes:?}"
    );
}

/// When origin/main advances externally and deletes a base file, but the node
/// has not yet been rebased, the effective fork point remains the original
/// base. `/changed` must report only the node's own file, not the unrelated
/// base deletion.
#[test]
#[serial_test::serial(jj)]
fn node_changed_files_excludes_unrebased_external_base_deletion() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping node_changed_files_excludes_unrebased_external_base_deletion: jj not resolvable");
        return;
    };
    let (_home, _origin, _proj, _wts, jj, ws, base) =
        setup_base_advance_with_node_change(&bin, false);

    let changes = node_changed_files(&jj, &ws, Some("main"), Some(&base))
        .expect("jj workspace resolves an effective fork point");
    let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["node.rs"],
        "base deletion must be absent: {changes:?}"
    );
}

/// After the same external advance, reconcile rebases the node onto
/// `main@origin`. The effective fork point must move to that remote-tracking
/// tip, so the base branch's deletion still does not appear as node work.
#[test]
#[serial_test::serial(jj)]
fn node_changed_files_excludes_rebased_external_base_deletion() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping node_changed_files_excludes_rebased_external_base_deletion: jj not resolvable");
        return;
    };
    let (_home, _origin, _proj, _wts, jj, ws, base) =
        setup_base_advance_with_node_change(&bin, true);

    let changes = node_changed_files(&jj, &ws, Some("main"), Some(&base))
        .expect("jj workspace resolves an effective fork point");
    let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
    assert_eq!(
        paths,
        vec!["node.rs"],
        "base deletion must be absent: {changes:?}"
    );
}

/// `list_files` enumerates a non-colocated workspace's tracked files — the
/// exact `.jj`-only shape (no `.git`) where the File tab's old `git ls-files`
/// returned nothing and rendered "Path not found" for everything. Asserts the
/// newly added, workspace-relative paths appear and that no `.jj/…` metadata
/// entry leaks into the listing.
#[test]
#[serial_test::serial(jj)]
fn list_files_enumerates_jj_workspace_tracked_files() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping list_files_enumerates_jj_workspace_tracked_files: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // A non-colocated workspace: `.jj` only, no `.git` — the shape that broke
    // git-in-worktree listing.
    assert!(
        !ws.join(".git").exists() && ws.join(".jj").is_dir(),
        "workspace is non-colocated (.jj only, no .git)"
    );

    // Write files in a subdir, then seal so they are snapshotted into the
    // working-copy commit `list_files` reads with --ignore-working-copy.
    std::fs::create_dir_all(ws.join("src")).unwrap();
    std::fs::write(ws.join("src").join("feature.rs"), "code\n").unwrap();
    std::fs::write(ws.join("notes.md"), "notes\n").unwrap();
    seal(&jj, &ws, "add files", None).unwrap();

    let files = list_files(&jj, &ws).unwrap();
    assert!(
        files.iter().any(|f| f == "src/feature.rs"),
        "workspace-relative subdir path is listed: {files:?}"
    );
    assert!(
        files.iter().any(|f| f == "notes.md"),
        "top-level file is listed: {files:?}"
    );
    assert!(
        files.iter().any(|f| f == "shared.rs"),
        "the base commit's tracked files are listed too: {files:?}"
    );
    assert!(
        !files.iter().any(|f| f.starts_with(".jj")),
        "the .jj metadata dir never leaks into the listing: {files:?}"
    );
    assert!(
        files.windows(2).all(|w| w[0] <= w[1]),
        "listing is sorted: {files:?}"
    );
}

/// `head_commit` is the jj analogue of `git rev-parse HEAD`: it returns the
/// base sha for a fresh workspace and the latest sealed commit after a seal.
#[test]
#[serial_test::serial(jj)]
fn head_commit_returns_base_then_sealed() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping head_commit_returns_base_then_sealed: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let base_sha = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // Fresh workspace: @- is the base commit.
    assert_eq!(
        head_commit(&jj, &ws).unwrap(),
        base_sha,
        "head_commit of a fresh workspace is the base sha"
    );

    // After a seal, @- is the newly sealed commit.
    std::fs::write(ws.join("mod.rs"), "code\n").unwrap();
    let sealed = seal(&jj, &ws, "agent work", None).unwrap();
    let head = head_commit(&jj, &ws).unwrap();
    assert_ne!(head, base_sha, "head advanced past base after seal");
    assert!(
        head.starts_with(&sealed.sha),
        "head_commit ({head}) is the sealed commit ({})",
        sealed.sha
    );
}

/// `sealed_tree_hash` returns the sealed commit's git **tree** object, so it
/// is content-addressed: two genuinely distinct commits with identical tree
/// content (different branches, messages, and authors) hash identically,
/// which is what lets the check cache and the merge-gate baseline carry
/// forward across an equivalent-tree squash/rebase. Different content hashes
/// differently, and the hash is distinct from the commit id itself.
#[test]
#[serial_test::serial(jj)]
fn sealed_tree_hash_is_content_addressed() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping sealed_tree_hash_is_content_addressed: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Two sibling workspaces off `main` seal IDENTICAL file content under
    // different branches, messages, and authors — distinct commit ids over
    // one tree.
    let a = wts.path().join("a");
    let b = wts.path().join("b");
    add_workspace(&jj, &store, &a, "agent/CAIRN-1-builder-0", "main", None).unwrap();
    add_workspace(&jj, &store, &b, "agent/CAIRN-2-builder-0", "main", None).unwrap();
    std::fs::write(a.join("mod.rs"), "code\n").unwrap();
    std::fs::write(b.join("mod.rs"), "code\n").unwrap();
    let author_a = GitAuthor::new("Alice", "alice@example.com");
    let author_b = GitAuthor::new("Bob", "bob@example.com");
    seal(&jj, &a, "message one", Some(&author_a)).unwrap();
    seal(&jj, &b, "a totally different message", Some(&author_b)).unwrap();

    let hash_a = sealed_tree_hash(&jj, &a).unwrap();
    let hash_b = sealed_tree_hash(&jj, &b).unwrap();

    // Stable for repeated reads of the same sealed revision.
    assert_eq!(
        hash_a,
        sealed_tree_hash(&jj, &a).unwrap(),
        "helper is stable for repeated reads"
    );

    // The two sealed commits are genuinely distinct ids …
    assert_ne!(
        head_commit(&jj, &a).unwrap(),
        head_commit(&jj, &b).unwrap(),
        "the two sealed commits are distinct commit ids"
    );
    // … yet identical tree content yields an identical content hash.
    assert_eq!(
        hash_a, hash_b,
        "identical tree content hashes identically across distinct commits"
    );
    // The hash is the git tree object, NOT the commit id — true content
    // addressing, which is exactly what the old commit-id fallback lacked.
    assert_ne!(
        hash_a,
        head_commit(&jj, &a).unwrap(),
        "sealed_tree_hash is the content tree, distinct from the sealed commit id"
    );

    // Different tree content hashes differently.
    let c = wts.path().join("c");
    add_workspace(&jj, &store, &c, "agent/CAIRN-3-builder-0", "main", None).unwrap();
    std::fs::write(c.join("mod.rs"), "different content\n").unwrap();
    seal(&jj, &c, "message one", Some(&author_a)).unwrap();
    assert_ne!(
        hash_a,
        sealed_tree_hash(&jj, &c).unwrap(),
        "different tree content yields a different hash"
    );
}

/// `parse_ls_tree` extracts `(path, blob)` from `-z` records, ignores
/// mode/type, tolerates a trailing NUL, and sorts by path. Pure — no jj
/// binary needed.
#[test]
fn parse_ls_tree_extracts_sorted_path_blob_pairs() {
    let out = "100644 blob aaa\tsrc/b.rs\x00100644 blob bbb\tsrc/a.rs\x00";
    assert_eq!(
        super::parse_ls_tree(out),
        vec![
            ("src/a.rs".to_string(), "bbb".to_string()),
            ("src/b.rs".to_string(), "aaa".to_string()),
        ]
    );
}

/// A seal followed by `push_to_origin` lands the workspace's bookmark on a
/// bare `origin` — the in-tree form of the bare-origin spike.
#[test]
#[serial_test::serial(jj)]
fn push_to_origin_lands_bookmark_in_bare_origin() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping push_to_origin_lands_bookmark_in_bare_origin: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();

    // A bare origin, with the project checkout wired to push main there.
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);

    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-9-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("f.rs"), "x\n").unwrap();
    seal(&jj, &ws, "agent work", None).unwrap();

    push_to_origin(&jj, &ws, branch);

    let refs = git_stdout(
        origin.path(),
        &["for-each-ref", "--format=%(refname)", "refs/heads/"],
    );
    assert!(
        refs.contains(branch),
        "pushed bookmark {branch} must appear on origin: {refs}"
    );

    // main/master are skipped (the same guard git uses); no panic, no push.
    push_to_origin(&jj, &ws, "main");
}

/// `ensure_bookmark_on_origin` publishes a Coordinator integration-branch
/// base that lives only as a bookmark in the shared store (the project
/// checkout has no local ref for it), and no-ops cleanly when the bookmark
/// does not exist.
#[test]
#[serial_test::serial(jj)]
fn ensure_bookmark_on_origin_publishes_store_bookmark() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping ensure_bookmark_on_origin_publishes_store_bookmark: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();

    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);

    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let base = "agent/CAIRN-1940-coordinator-0";
    // A nonexistent bookmark is a clean no-op (base not sealed yet).
    ensure_bookmark_on_origin(&jj, &store, base).unwrap();
    let before = git_stdout(
        origin.path(),
        &["for-each-ref", "--format=%(refname)", "refs/heads/"],
    );
    assert!(
        !before.contains(base),
        "absent bookmark must not be created on origin: {before}"
    );

    // Seal an integration bookmark in the store, then publish it.
    jj.run(
        &store,
        &["bookmark", "create", "-r", "main", base],
        "bookmark create",
    )
    .unwrap();
    ensure_bookmark_on_origin(&jj, &store, base).unwrap();
    let after = git_stdout(
        origin.path(),
        &["for-each-ref", "--format=%(refname)", "refs/heads/"],
    );
    assert!(
        after.contains(base),
        "published integration bookmark {base} must appear on origin: {after}"
    );

    // Idempotent: a second call is a no-op (already matches origin).
    ensure_bookmark_on_origin(&jj, &store, base).unwrap();
}

/// The headline: the coordinator topology reconciled in-tree. Two sibling
/// workspaces off a shared integration bookmark; the integration tip advances
/// (a child PR merged into it); `reconcile_siblings` non-blockingly rebases
/// both. The overlapping sibling records a conflict with its change-id
/// preserved (only the commit-id churns), its workspace goes stale and
/// `update-stale` materializes conflict markers, and jj refuses to push that
/// conflicted bookmark while the cleanly-rebased sibling pushes fine.
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_auto_rebases_with_recorded_conflict() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping reconcile_siblings_auto_rebases_with_recorded_conflict: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();

    // Project wired to a bare origin; shared store over its .git.
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // The Coordinator's integration bookmark, created by the coordinator's
    // own `add_workspace` (the real flow — a coordinator never seals, so its
    // bookmark must exist from creation) and published to origin.
    let int = "agent/CAIRN-1940-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    ensure_bookmark_on_origin(&jj, &store, int).unwrap();

    // Two sibling jobs off the integration tip: one edits the shared file
    // (conflict-bound), one edits a different file (clean).
    let overlap = "agent/CAIRN-1-builder-0";
    let clean = "agent/CAIRN-2-builder-0";
    let ws_overlap = wts.path().join("overlap");
    let ws_clean = wts.path().join("clean");
    add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
    add_workspace(&jj, &store, &ws_clean, clean, int, None).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "sibling-A-change\n").unwrap();
    seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();
    std::fs::write(ws_clean.join("other.rs"), "b-only\n").unwrap();
    seal(&jj, &ws_clean, "clean edits other", None).unwrap();
    // Establish each sibling's PR head on origin (both clean so far).
    push_to_origin(&jj, &ws_overlap, overlap);
    push_to_origin(&jj, &ws_clean, clean);

    let change_overlap_before = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{overlap:?})"),
                "--no-graph",
                "-T",
                "change_id.short()",
            ],
            "change before",
        )
        .unwrap();
    let commit_overlap_before = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{overlap:?})"),
                "--no-graph",
                "-T",
                "commit_id.short()",
            ],
            "commit before",
        )
        .unwrap();

    // A child PR merges into the integration branch: advance its tip with a
    // conflicting change to the shared file, and publish it to origin.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "child merged: shared advanced"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    jj.run(
        &store,
        &["git", "push", "--remote", "origin", "--bookmark", int],
        "push advanced int",
    )
    .unwrap();

    // The cleanly-rebased sibling's PR head on origin before reconcile.
    let clean_origin_before = git_stdout(origin.path(), &["rev-parse", clean]);

    // The reconcile: both siblings rebase onto the advanced tip.
    let report = reconcile_siblings(
        &jj,
        &store,
        int,
        &[
            (overlap.to_string(), ws_overlap.clone()),
            (clean.to_string(), ws_clean.clone()),
        ],
    )
    .unwrap();
    assert_eq!(report.conflicted, vec![overlap.to_string()]);
    assert_eq!(report.rebased_clean, vec![clean.to_string()]);

    // reconcile pushed the cleanly-rebased sibling, advancing its PR head on
    // origin (no force-push needed); the conflicted one was not pushed.
    let clean_origin_after = git_stdout(origin.path(), &["rev-parse", clean]);
    assert_ne!(
        clean_origin_before, clean_origin_after,
        "reconcile pushes the cleanly-rebased sibling's advanced tip to origin"
    );

    // The overlapping sibling kept its change-id; only the commit churned.
    let change_overlap_after = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{overlap:?})"),
                "--no-graph",
                "-T",
                "change_id.short()",
            ],
            "change after",
        )
        .unwrap();
    let commit_overlap_after = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{overlap:?})"),
                "--no-graph",
                "-T",
                "commit_id.short()",
            ],
            "commit after",
        )
        .unwrap();
    assert_eq!(
        change_overlap_before, change_overlap_after,
        "auto-rebase preserves the sibling's change-id"
    );
    assert_ne!(
        commit_overlap_before, commit_overlap_after,
        "the rebased commit-id churns"
    );

    // jj records the conflict on the overlapping sibling, not the clean one.
    assert!(branch_has_conflict(&jj, &store, overlap).unwrap());
    assert!(!branch_has_conflict(&jj, &store, clean).unwrap());

    // update-stale materialized the conflict markers in the workspace file.
    let conflicted_file = std::fs::read_to_string(ws_overlap.join("shared.rs")).unwrap();
    assert!(
        conflicted_file.contains("<<<<<<<") && conflicted_file.contains(">>>>>>>"),
        "the agent sees materialized conflict markers: {conflicted_file}"
    );

    // jj refuses to push the conflicted bookmark (so a conflicted sibling
    // cannot advance its PR head on origin); the clean one pushes fine.
    assert!(
        jj.run(
            &store,
            &["git", "push", "--remote", "origin", "--bookmark", overlap],
            "push conflicted",
        )
        .is_err(),
        "jj must refuse to push a conflicted bookmark"
    );
    assert!(
        jj.run(
            &store,
            &["git", "push", "--remote", "origin", "--bookmark", clean],
            "push clean",
        )
        .is_ok(),
        "the cleanly-rebased sibling pushes its advanced tip"
    );
}

/// External default-branch advance: origin/main moves OUT OF BAND (a non-Cairn
/// merge or direct push, not folded through the store). `fetch_remote` brings
/// the new tip into the store as `main@origin`, which resolves as the rebase
/// dest; siblings based on `main` auto-rebase onto it exactly as the
/// Cairn-merge path does. Also proves the double-fire guard's premise: a
/// second reconcile at the same tip leaves the conflicted commit id unchanged
/// (a `jj rebase` no-op), so the before/after wake guard suppresses a
/// redundant wake.
#[test]
#[serial_test::serial(jj)]
fn reconcile_external_advance_via_origin_fetch_is_idempotent() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping reconcile_external_advance_via_origin_fetch_is_idempotent: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();

    // Project wired to a bare origin; shared store over its .git.
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Two sibling jobs based directly on the default branch `main`: one edits
    // the shared file (conflict-bound vs the external advance), one edits a
    // different file (clean).
    let overlap = "agent/CAIRN-1-builder-0";
    let clean = "agent/CAIRN-2-builder-0";
    let ws_overlap = wts.path().join("overlap");
    let ws_clean = wts.path().join("clean");
    add_workspace(&jj, &store, &ws_overlap, overlap, "main", None).unwrap();
    add_workspace(&jj, &store, &ws_clean, clean, "main", None).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "sibling-A-change\n").unwrap();
    seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();
    std::fs::write(ws_clean.join("other.rs"), "b-only\n").unwrap();
    seal(&jj, &ws_clean, "clean edits other", None).unwrap();
    // Establish each sibling's PR head on origin (both clean so far).
    push_to_origin(&jj, &ws_overlap, overlap);
    push_to_origin(&jj, &ws_clean, clean);

    // The default branch advances OUTSIDE Cairn: edit + commit + push to
    // origin/main directly from the project checkout, with a change that
    // conflicts with the overlapping sibling. This never folds through the
    // store, so the store's view of main is stale until we fetch.
    std::fs::write(proj.path().join("shared.rs"), "external-advance\n").unwrap();
    git(proj.path(), &["add", "-A"]);
    git(
        proj.path(),
        &["commit", "-q", "-m", "external merge advances main"],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);

    // Store-sync: fetch origin so `main@origin` resolves to the new tip.
    fetch_remote(&jj, &store, "origin").unwrap();
    let dest = "main@origin";
    assert!(
        bookmark_commit(&jj, &store, dest).is_some()
            || jj
                .run(
                    &store,
                    &["log", "-r", dest, "--no-graph", "-T", "commit_id"],
                    "resolve dest",
                )
                .is_ok(),
        "the externally-advanced tip must resolve as the rebase dest after fetch"
    );

    let clean_origin_before = git_stdout(origin.path(), &["rev-parse", clean]);

    // First reconcile: both siblings rebase onto the externally-advanced tip.
    let report = reconcile_siblings(
        &jj,
        &store,
        dest,
        &[
            (overlap.to_string(), ws_overlap.clone()),
            (clean.to_string(), ws_clean.clone()),
        ],
    )
    .unwrap();
    assert_eq!(report.conflicted, vec![overlap.to_string()]);
    assert_eq!(report.rebased_clean, vec![clean.to_string()]);

    // The cleanly-rebased sibling's PR head advanced on origin.
    let clean_origin_after = git_stdout(origin.path(), &["rev-parse", clean]);
    assert_ne!(
        clean_origin_before, clean_origin_after,
        "reconcile pushes the cleanly-rebased sibling's advanced tip to origin"
    );
    assert!(branch_has_conflict(&jj, &store, overlap).unwrap());
    assert!(!branch_has_conflict(&jj, &store, clean).unwrap());

    // The conflicted sibling's commit id after the first reconcile.
    let commit_overlap_after_first = bookmark_commit(&jj, &store, overlap).unwrap();

    // Second reconcile at the SAME tip (the double-fire): a `jj rebase` no-op,
    // so the conflicted commit id is unchanged. The before/after wake guard
    // reads exactly this equality to suppress a redundant wake.
    fetch_remote(&jj, &store, "origin").unwrap();
    let report2 = reconcile_siblings(
        &jj,
        &store,
        dest,
        &[
            (overlap.to_string(), ws_overlap.clone()),
            (clean.to_string(), ws_clean.clone()),
        ],
    )
    .unwrap();
    assert_eq!(
        report2.conflicted,
        vec![overlap.to_string()],
        "the sibling is still conflicted on the second pass"
    );
    let commit_overlap_after_second = bookmark_commit(&jj, &store, overlap).unwrap();
    assert_eq!(
            commit_overlap_after_first, commit_overlap_after_second,
            "a second reconcile at the same dest tip leaves the conflicted commit id unchanged (no redundant wake)"
        );
}

/// A sibling that has not sealed work yet has its bookmark sitting exactly on
/// the old base. When the base advances, reconcile must fast-forward that idle
/// bookmark and re-parent the workspace `@` onto the new base instead of
/// handing it to `jj rebase -b`, whose revset is empty for an ancestor bookmark.
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_fast_forwards_no_work_sibling() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_siblings_fast_forwards_no_work_sibling: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2345-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let idle = "agent/CAIRN-2345-builder-0";
    let ws_idle = wts.path().join("idle");
    add_workspace(&jj, &store, &ws_idle, idle, int, None).unwrap();
    let old_idle_commit = bookmark_commit(&jj, &store, idle).unwrap();

    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("base-advance.rs"), "advanced base\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "integration advances base"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    let dest_commit = bookmark_commit(&jj, &store, int).unwrap();
    assert_ne!(
        old_idle_commit, dest_commit,
        "the integration bookmark must advance past the idle sibling's old base"
    );

    let specs = vec![(idle.to_string(), ws_idle.clone())];
    let report = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report.rebased_clean, vec![idle.to_string()]);
    assert!(report.conflicted.is_empty());
    assert_eq!(bookmark_commit(&jj, &store, idle).unwrap(), dest_commit);
    assert_eq!(
        std::fs::read_to_string(ws_idle.join("base-advance.rs")).unwrap(),
        "advanced base\n",
        "the fast-forwarded workspace materializes the new base file"
    );

    let commit_after_first = bookmark_commit(&jj, &store, idle).unwrap();
    let report2 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report2.rebased_clean, vec![idle.to_string()]);
    assert!(report2.conflicted.is_empty());
    assert_eq!(
        bookmark_commit(&jj, &store, idle).unwrap(),
        commit_after_first,
        "a second reconcile is caught by the already-on-dest skip and does not rewrite"
    );
}

/// The fast-forward path still has to surface an idle sibling's unsealed WIP
/// when re-parenting that workspace `@` onto the new base records a conflict.
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_no_work_sibling_with_conflicting_wip_classified_conflicted() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_siblings_no_work_sibling_with_conflicting_wip_classified_conflicted: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2345-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let idle = "agent/CAIRN-2345-builder-0";
    let ws_idle = wts.path().join("idle-conflict");
    add_workspace(&jj, &store, &ws_idle, idle, int, None).unwrap();

    std::fs::write(ws_idle.join("shared.rs"), "idle unsealed change\n").unwrap();
    jj.run(&ws_idle, &["log", "-r", "@"], "snapshot idle wip")
        .unwrap();

    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration advanced shared\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "integration advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    let dest_commit = bookmark_commit(&jj, &store, int).unwrap();

    let specs = vec![(idle.to_string(), ws_idle.clone())];
    let report = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report.conflicted, vec![idle.to_string()]);
    assert!(report.rebased_clean.is_empty());
    assert_eq!(
        bookmark_commit(&jj, &store, idle).unwrap(),
        dest_commit,
        "the idle bookmark still fast-forwards to the new base"
    );

    let conflicted_file = std::fs::read_to_string(ws_idle.join("shared.rs")).unwrap();
    assert!(
        conflicted_file.contains("<<<<<<<") && conflicted_file.contains(">>>>>>>"),
        "the agent sees materialized conflict markers: {conflicted_file}"
    );
}

/// Acceptance: advancing the integration base with a conflicting change under
/// N in-flight children, then reconciling REPEATEDLY (with the real
/// `jj git import` default-advance round-trip between passes), must not
/// accumulate divergent conflicted copies. The first reconcile rebases each
/// child; every later pass finds each child already descended from the dest
/// and SKIPS the rebase, so the conflicted child's commit id is stable and
/// every change-id resolves to exactly one visible commit — no `<id>/0 /1`
/// thrash. This is the structural-idempotence half of the 2041 fix (the
/// per-store mutex is the concurrency half).
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_idempotent_no_divergence_across_import_round_trips() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_siblings_idempotent_no_divergence_across_import_round_trips: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // A coordinator integration bookmark with three children branched FROM it:
    // one overlaps the shared file (conflict-bound vs the base advance), two
    // edit distinct files (clean).
    let int = "agent/CAIRN-2041-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let overlap = "agent/CAIRN-1-builder-0";
    let clean_a = "agent/CAIRN-2-builder-0";
    let clean_b = "agent/CAIRN-3-builder-0";
    let ws_overlap = wts.path().join("overlap");
    let ws_a = wts.path().join("a");
    let ws_b = wts.path().join("b");
    add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
    add_workspace(&jj, &store, &ws_a, clean_a, int, None).unwrap();
    add_workspace(&jj, &store, &ws_b, clean_b, int, None).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "sibling-overlap\n").unwrap();
    seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();
    std::fs::write(ws_a.join("a.rs"), "a\n").unwrap();
    seal(&jj, &ws_a, "a edits a", None).unwrap();
    std::fs::write(ws_b.join("b.rs"), "b\n").unwrap();
    seal(&jj, &ws_b, "b edits b", None).unwrap();

    // The integration tip advances with a change that conflicts with overlap.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "integration advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();

    let specs = vec![
        (overlap.to_string(), ws_overlap.clone()),
        (clean_a.to_string(), ws_a.clone()),
        (clean_b.to_string(), ws_b.clone()),
    ];

    // First reconcile: overlap conflicts, the other two land clean.
    let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report1.conflicted, vec![overlap.to_string()]);
    assert_eq!(
        report1.rebased_clean,
        vec![clean_a.to_string(), clean_b.to_string()]
    );
    assert!(branch_has_conflict(&jj, &store, overlap).unwrap());

    // Snapshot every child's post-reconcile commit id; later passes must not
    // move any of them.
    let commit_overlap_1 = bookmark_commit(&jj, &store, overlap).unwrap();
    let commit_a_1 = bookmark_commit(&jj, &store, clean_a).unwrap();
    let commit_b_1 = bookmark_commit(&jj, &store, clean_b).unwrap();
    let cid_overlap = change_id_of(&jj, &store, overlap);
    let cid_a = change_id_of(&jj, &store, clean_a);
    let cid_b = change_id_of(&jj, &store, clean_b);

    // Repeated reconciles, each preceded by the real default-advance round-trip
    // (`jj git import` via `ensure_project_store`). Every pass is a no-op.
    for pass in 0..3 {
        ensure_project_store(&jj, &store, proj.path()).unwrap();
        let report = reconcile_siblings(&jj, &store, int, &specs).unwrap();
        assert_eq!(
            report.conflicted,
            vec![overlap.to_string()],
            "pass {pass}: overlap stays classified conflicted"
        );

        // The conflicted child's commit id is UNCHANGED — the rebase was
        // skipped (no re-rewrite), which is what stops divergent twins.
        assert_eq!(
            bookmark_commit(&jj, &store, overlap).unwrap(),
            commit_overlap_1,
            "pass {pass}: conflicted commit id is stable (rebase skipped)"
        );
        assert_eq!(bookmark_commit(&jj, &store, clean_a).unwrap(), commit_a_1);
        assert_eq!(bookmark_commit(&jj, &store, clean_b).unwrap(), commit_b_1);

        // Exactly one visible commit per change-id: no `<id>/0 /1` divergence.
        assert_eq!(
            visible_commits_for_change(&jj, &store, &cid_overlap),
            1,
            "pass {pass}: overlap change-id resolves to exactly one commit (no divergence)"
        );
        assert_eq!(visible_commits_for_change(&jj, &store, &cid_a), 1);
        assert_eq!(visible_commits_for_change(&jj, &store, &cid_b), 1);
    }
}

/// A manually-resolved bookmark (clean tip over a conflicted intermediate) is
/// FLATTENED by the next reconcile, never dragged back onto a conflicted copy.
/// After the base advance conflicts the overlapping child, the agent resolves
/// the markers and re-seals — but that leaves the conflicted rebase commit as a
/// conflicted INTERMEDIATE in the history, so the branch is still unmergeable
/// (jj refuses a conflicted history). The reconcile-time flatten collapses it
/// to ONE clean commit on the dest, preserving the agent's resolved TREE and
/// clearing the conflicted intermediate, so the branch becomes genuinely
/// mergeable. The resolution is preserved (never regenerated as a conflict) and
/// the pre-flatten change-id is cleaned up (no divergent twin). This is the
/// automation of the old hand-run resolve-at-base flatten.
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_preserves_resolved_bookmark() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_siblings_preserves_resolved_bookmark: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2041-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let overlap = "agent/CAIRN-1-builder-0";
    let ws_overlap = wts.path().join("overlap");
    add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "sibling-overlap\n").unwrap();
    seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();

    // The integration tip advances with a conflicting change.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "integration advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();

    let specs = vec![(overlap.to_string(), ws_overlap.clone())];

    // First reconcile records the conflict and materializes markers on disk.
    let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report1.conflicted, vec![overlap.to_string()]);
    assert!(branch_has_conflict(&jj, &store, overlap).unwrap());

    // The agent resolves the markers in its workspace and re-seals: the
    // bookmark advances to a CLEAN commit on top of the conflicted rebase.
    update_stale(&jj, &ws_overlap).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "resolved-by-agent\n").unwrap();
    seal(&jj, &ws_overlap, "resolve base conflict", None).unwrap();
    assert!(
        !branch_has_conflict(&jj, &store, overlap).unwrap(),
        "the re-seal resolves the conflict; the bookmark is clean"
    );
    let resolved_commit = bookmark_commit(&jj, &store, overlap).unwrap();
    let resolved_cid = change_id_of(&jj, &store, overlap);

    // The next reconcile FLATTENS the resolved-but-conflicted-intermediate
    // branch: it already descends from the dest, but its history still carries
    // the conflicted rebase commit (unmergeable), so the reconcile collapses it
    // to one clean commit — preserving the resolved tree, never regenerating a
    // conflict.
    let _ = resolved_commit; // the flatten deliberately rewrites this commit id
    let report2 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(
        report2.rebased_clean,
        vec![overlap.to_string()],
        "the resolved child is classified clean, not conflicted"
    );
    assert!(report2.conflicted.is_empty());
    assert!(
        !branch_has_conflict(&jj, &store, overlap).unwrap(),
        "the resolution is preserved — no regenerated conflict"
    );
    // The branch is now genuinely mergeable: the flatten cleared the conflicted
    // intermediate from its history.
    let dest = bookmark_commit(&jj, &store, int).unwrap();
    assert!(
        conflicted_commits(
            &jj,
            &store,
            &format!("{dest}..bookmarks(exact:{overlap:?})")
        )
        .is_empty(),
        "the flatten cleared the conflicted intermediate — the branch is mergeable"
    );
    assert_eq!(
        count_commits(
            &jj,
            &store,
            &format!("{dest}..bookmarks(exact:{overlap:?})")
        ),
        1,
        "the branch is collapsed to a single clean commit on the dest"
    );
    // The agent's resolved TREE is preserved though the commit was rewritten.
    assert_eq!(
        String::from_utf8_lossy(&file_show(&jj, &store, overlap, "shared.rs").unwrap()),
        "resolved-by-agent\n",
        "the flatten preserves the agent's resolved content"
    );
    // The pre-flatten change-id is cleaned up: no lingering (divergent) twin.
    assert_eq!(
        visible_commits_for_change(&jj, &store, &resolved_cid),
        0,
        "the pre-flatten change-id is abandoned by twin cleanup — no divergent twin"
    );
}

/// The no-propagate guard: when the rebase dest itself carries a recorded
/// conflict, every sibling is HELD on its prior clean commit rather than
/// rebased onto the conflicted base — the load-bearing fix for the live bug
/// where a conflicted integration tip was handed to all in-flight children.
/// The hold is self-clearing: once the base re-seals clean, the next reconcile
/// rebases the child normally.
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_holds_children_off_conflicted_base() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping reconcile_siblings_holds_children_off_conflicted_base: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2042-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    // The clean integration tip the child branches from.
    let int_base = bookmark_commit(&jj, &store, int).unwrap();

    // The child branches from the clean int tip and edits a NON-overlapping
    // file, so on a clean base it would rebase cleanly.
    let child = "agent/CAIRN-1-builder-0";
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(ws_child.join("other.rs"), "child-edit\n").unwrap();
    seal(&jj, &ws_child, "child edits other", None).unwrap();
    let child_commit_before = bookmark_commit(&jj, &store, child).unwrap();

    // Drive the integration bookmark to a CONFLICTED tip without rewriting the
    // child's ancestor: two changes from the same base edit shared.rs
    // conflictingly, and rebasing one onto the other records a conflict in its
    // commit; int is pointed at that conflicted commit.
    jj.run(&store, &["new", &int_base, "-m", "left"], "new left")
        .unwrap();
    std::fs::write(store.join("shared.rs"), "left-side\n").unwrap();
    jj.run(
        &store,
        &["bookmark", "create", "tmp-left", "-r", "@"],
        "create tmp-left",
    )
    .unwrap();
    jj.run(&store, &["new", &int_base, "-m", "right"], "new right")
        .unwrap();
    std::fs::write(store.join("shared.rs"), "right-side\n").unwrap();
    jj.run(
        &store,
        &["bookmark", "create", "tmp-right", "-r", "@"],
        "create tmp-right",
    )
    .unwrap();
    jj.run(
        &store,
        &[
            "rebase",
            "-r",
            "tmp-left",
            "-d",
            "tmp-right",
            "--ignore-working-copy",
        ],
        "rebase tmp-left onto tmp-right to record a conflict",
    )
    .unwrap();
    let conflicted_tip = bookmark_commit(&jj, &store, "tmp-left").unwrap();
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            int,
            "-r",
            &conflicted_tip,
            "--ignore-working-copy",
        ],
        "point int at the conflicted commit",
    )
    .unwrap();
    assert!(
        branch_has_conflict(&jj, &store, int).unwrap(),
        "the integration tip is conflicted"
    );

    let specs = vec![(child.to_string(), ws_child.clone())];

    // First reconcile: the dest (int) is conflicted, so the child is HELD on
    // its prior clean commit — never rebased onto the conflicted base.
    let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(
        report1.held,
        vec![child.to_string()],
        "the child is held off the conflicted base"
    );
    assert!(
        report1.conflicted.is_empty(),
        "a held child is not classified conflicted"
    );
    assert!(
        report1.rebased_clean.is_empty(),
        "a held child is not classified clean"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, child).unwrap(),
        child_commit_before,
        "the held child's commit is unchanged — never rebased onto the conflicted base"
    );
    assert!(
        !branch_has_conflict(&jj, &store, child).unwrap(),
        "the held child stayed clean"
    );

    // The base is resolved and re-sealed: a fresh commit on int fully rewrites
    // the conflicted file, advancing int to a clean tip.
    jj.run(&store, &["new", int, "-m", "resolve"], "new on int")
        .unwrap();
    std::fs::write(store.join("shared.rs"), "resolved\n").unwrap();
    jj.run(
        &store,
        &["bookmark", "set", int, "-r", "@"],
        "advance int clean",
    )
    .unwrap();
    assert!(
        !branch_has_conflict(&jj, &store, int).unwrap(),
        "the base re-sealed clean"
    );

    // Second reconcile: the guard no longer fires, the child rebases normally
    // onto the clean tip (the hold clears), and it now descends from int.
    let report2 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert!(report2.held.is_empty(), "with a clean base nothing is held");
    assert_eq!(
        report2.rebased_clean,
        vec![child.to_string()],
        "the child rebases cleanly onto the resolved base"
    );
    assert!(report2.conflicted.is_empty());
    let int_clean = bookmark_commit(&jj, &store, int).unwrap();
    assert!(
        branch_descends_from(&jj, &store, child, &int_clean),
        "the child now descends from the resolved int tip"
    );
}

/// `conflicted_files` enumerates the conflicting file paths in a workspace
/// whose markers are materialized — the detail threaded into the stop-the-line
/// note so the agent knows exactly where to look.
#[test]
#[serial_test::serial(jj)]
fn conflicted_files_lists_conflicting_paths() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping conflicted_files_lists_conflicting_paths: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2042-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let child = "agent/CAIRN-1-builder-0";
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(ws_child.join("shared.rs"), "child-side\n").unwrap();
    seal(&jj, &ws_child, "child edits shared", None).unwrap();

    // The integration tip advances with a conflicting change to the same file.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "integration advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();

    // The reconcile rebases the child onto the advanced tip, recording a
    // conflict and materializing the markers in the child workspace.
    let report =
        reconcile_siblings(&jj, &store, int, &[(child.to_string(), ws_child.clone())]).unwrap();
    assert_eq!(report.conflicted, vec![child.to_string()]);

    update_stale(&jj, &ws_child).unwrap();
    let files = conflicted_files(&jj, &ws_child);
    assert_eq!(
        files,
        vec!["shared.rs".to_string()],
        "the conflicting path is listed"
    );
}

/// `conflicted_commits` enumerates each conflicted commit in a range with its
/// conflicted file paths — store-side, no workspace — and reports nothing for
/// a clean range. This is the detail the pre-flight diagnostic surfaces.
#[test]
#[serial_test::serial(jj)]
fn conflicted_commits_enumerates_conflicting_commits_and_files() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping conflicted_commits_enumerates_conflicting_commits_and_files: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2042-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let child = "agent/CAIRN-1-builder-0";
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(ws_child.join("shared.rs"), "child-side\n").unwrap();
    seal(&jj, &ws_child, "child edits shared", None).unwrap();

    // A clean range reports nothing before any conflict is recorded.
    assert!(
        conflicted_commits(&jj, &store, &format!("bookmarks(exact:{child:?})")).is_empty(),
        "a clean source has no conflicted commits"
    );

    // The integration tip advances with a conflicting change to the same file,
    // then the reconcile rebases the child onto it, recording a conflict.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "integration advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    let report =
        reconcile_siblings(&jj, &store, int, &[(child.to_string(), ws_child.clone())]).unwrap();
    assert_eq!(report.conflicted, vec![child.to_string()]);

    // The conflicted child commit is enumerated with its conflicted path.
    let conflicts = conflicted_commits(&jj, &store, &format!("bookmarks(exact:{child:?})"));
    assert_eq!(
        conflicts.len(),
        1,
        "the conflicted child commit is reported"
    );
    assert_eq!(conflicts[0].files, vec!["shared.rs".to_string()]);
    assert!(
        !conflicts[0].commit_id.is_empty() && !conflicts[0].change_id.is_empty(),
        "commit and change ids are populated"
    );

    // The cleanly-advanced integration tip itself carries no conflict.
    assert!(
        conflicted_commits(&jj, &store, &format!("bookmarks(exact:{int:?})")).is_empty(),
        "the clean integration tip reports no conflicted commits"
    );
}

/// The current operation id over the store.
fn current_op_id(jj: &JjEnv, store: &Path) -> String {
    jj.run(
        store,
        &["op", "log", "--no-graph", "-n", "1", "-T", "id"],
        "current op id",
    )
    .unwrap()
    .trim()
    .to_string()
}

/// Deterministic reproduction of the divergence MECHANISM, plus proof the fix
/// avoids it. Two rebases of the same child from the SAME base operation
/// (`--at-op`) fork the operation log; the next command merges the divergent
/// op heads, and jj keeps BOTH rewritten commits as a divergent change
/// (`<id>/0 /1`) — exactly the `spnmzyvp/0../5` accumulation observed live.
/// This is what concurrent, unserialized reconciles did on the shared store.
/// The fix's single-writer discipline (the per-store mutex) plus the
/// resolve-dest-once + descends skip in `reconcile_siblings` make a serialized
/// re-reconcile a structural no-op, so it converges to ONE commit.
#[test]
#[serial_test::serial(jj)]
fn forked_op_rebase_diverges_but_reconcile_converges() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping forked_op_rebase_diverges_but_reconcile_converges: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2041-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let overlap = "agent/CAIRN-1-builder-0";
    let ws_overlap = wts.path().join("overlap");
    add_workspace(&jj, &store, &ws_overlap, overlap, int, None).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "sibling-overlap\n").unwrap();
    seal(&jj, &ws_overlap, "overlap edits shared", None).unwrap();

    // overlap is sealed on the original integration base P.
    let p = bookmark_commit(&jj, &store, int).unwrap();

    // Two DISTINCT advances of the integration base off P, each conflicting
    // with overlap differently. A moving dest is what made the live
    // reconciles rewrite the same change to different commits.
    let commit_of_at = |jj: &JjEnv| {
        jj.run(
            &store,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "commit of @",
        )
        .unwrap()
        .trim()
        .to_string()
    };
    jj.run(&store, &["new", &p], "new D1 off base").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced-1\n").unwrap();
    jj.run(&store, &["describe", "-m", "advance 1"], "describe D1")
        .unwrap();
    let d1 = commit_of_at(&jj);
    jj.run(&store, &["new", &p], "new D2 off base").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced-2\n").unwrap();
    jj.run(&store, &["describe", "-m", "advance 2"], "describe D2")
        .unwrap();
    let d2 = commit_of_at(&jj);
    // The integration bookmark tracks the canonical advanced tip D1.
    jj.run(
        &store,
        &["bookmark", "set", int, "-r", &d1, "--ignore-working-copy"],
        "set int = D1",
    )
    .unwrap();

    let cid_overlap = change_id_of(&jj, &store, overlap);

    // MECHANISM: fork the op log. Rebase overlap onto D1 in one forked op and
    // onto D2 in another, both from the SAME base operation. The two ops
    // rewrite overlap to DIFFERENT commits (distinct parents); merging the
    // divergent op heads keeps both as a divergent change `<id>/0 /1`.
    let base_op = current_op_id(&jj, &store);
    jj.run(
        &store,
        &[
            "rebase",
            "-b",
            overlap,
            "-o",
            &d1,
            "--ignore-working-copy",
            "--at-op",
            &base_op,
        ],
        "forked rebase onto D1",
    )
    .unwrap();
    jj.run(
        &store,
        &[
            "rebase",
            "-b",
            overlap,
            "-o",
            &d2,
            "--ignore-working-copy",
            "--at-op",
            &base_op,
        ],
        "forked rebase onto D2",
    )
    .unwrap();
    // Trigger the concurrent-op merge (any normal command does it).
    let _ = jj.run(
        &store,
        &["log", "-r", "root()", "--no-graph", "-T", "commit_id"],
        "trigger op merge",
    );
    assert_eq!(
        visible_commits_for_change(&jj, &store, &cid_overlap),
        2,
        "two forked rebases onto distinct tips accumulate a divergent copy (the bug)"
    );

    // Converge the corrupted store the way a live one is hand-repaired: point
    // the bookmark at the twin that descends from the canonical tip D1
    // (= int) and abandon the orphaned D2 twin.
    let twins = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("change_id({cid_overlap})"),
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "list divergent twins",
        )
        .unwrap();
    let twin_ids: Vec<String> = twins
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();
    assert_eq!(twin_ids.len(), 2);
    let keep = twin_ids
        .iter()
        .find(|c| revset_descends_from(&jj, &store, c, &d1))
        .cloned()
        .expect("one twin descends from the canonical tip D1");
    let drop = twin_ids
        .iter()
        .find(|c| **c != keep)
        .cloned()
        .expect("the other twin");
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            overlap,
            "-r",
            &keep,
            "--ignore-working-copy",
        ],
        "point bookmark at kept twin",
    )
    .unwrap();
    jj.run(
        &store,
        &["abandon", &drop, "--ignore-working-copy"],
        "abandon divergent twin",
    )
    .unwrap();
    assert_eq!(
        visible_commits_for_change(&jj, &store, &cid_overlap),
        1,
        "after convergence the change resolves to a single commit"
    );

    // FIX: a serialized re-reconcile at the same dest is now a structural
    // no-op (the child already descends from `int`), so it never re-mints a
    // divergent twin. This is the single-writer + skip behavior the mutex
    // guarantees in production.
    let specs = vec![(overlap.to_string(), ws_overlap.clone())];
    let before = bookmark_commit(&jj, &store, overlap).unwrap();
    reconcile_siblings(&jj, &store, int, &specs).unwrap();
    reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(
        bookmark_commit(&jj, &store, overlap).unwrap(),
        before,
        "the skip-guarded reconcile leaves the commit id unchanged"
    );
    assert_eq!(
        visible_commits_for_change(&jj, &store, &cid_overlap),
        1,
        "the skip-guarded reconcile does not re-mint a divergent twin"
    );
}

const DIV_INT: &str = "agent/CAIRN-2100-coordinator-0";
const DIV_SIBLING: &str = "agent/CAIRN-1-builder-0";

/// A project store with an integration bookmark and one `agent/...` sibling
/// branched from it, the sibling sealed editing `shared.rs`. The TempDirs are
/// kept alive by the returned struct.
struct DivergenceFixture {
    _home: TempDir,
    _proj: TempDir,
    _wts: TempDir,
    jj: JjEnv,
    store: PathBuf,
    ws_sibling: PathBuf,
}

fn setup_divergence_fixture(bin: &str) -> DivergenceFixture {
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    add_workspace(
        &jj,
        &store,
        &wts.path().join("coord"),
        DIV_INT,
        "main",
        None,
    )
    .unwrap();
    let ws_sibling = wts.path().join("sibling");
    add_workspace(&jj, &store, &ws_sibling, DIV_SIBLING, DIV_INT, None).unwrap();
    std::fs::write(ws_sibling.join("shared.rs"), "sibling-edit\n").unwrap();
    seal(&jj, &ws_sibling, "sibling edits shared", None).unwrap();
    DivergenceFixture {
        _home: home,
        _proj: proj,
        _wts: wts,
        jj,
        store,
        ws_sibling,
    }
}

/// Advance the integration tip with a `shared.rs` edit that conflicts with the
/// sibling's edit; returns the new tip commit id (a conflicting rebase dest).
fn advance_int_conflicting(jj: &JjEnv, store: &Path, content: &str) -> String {
    jj.run(store, &["new", DIV_INT], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), content).unwrap();
    jj.run(
        store,
        &["describe", "-m", "int advances shared"],
        "describe int",
    )
    .unwrap();
    let tip = jj
        .run(
            store,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "int tip",
        )
        .unwrap()
        .trim()
        .to_string();
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            DIV_INT,
            "-r",
            "@",
            "--ignore-working-copy",
        ],
        "advance int bookmark",
    )
    .unwrap();
    tip
}

/// Mint a divergent change on the sibling carrying one CONFLICTED twin (the
/// base-advance copy rebased onto a conflicting dest) and one CLEAN twin (the
/// original commit re-described, standing in for the agent's resolved
/// re-seal), via two forked ops from the same base operation. Returns
/// (shared change-id, conflicted twin id, clean twin id). The change-id is
/// captured BEFORE the fork (a single pre-fork commit) because the forked
/// bookmark itself goes divergent.
fn fork_conflicted_and_clean(
    jj: &JjEnv,
    store: &Path,
    conflicting_dest: &str,
) -> (String, String, String) {
    let cid = change_id_of(jj, store, DIV_SIBLING);
    let base_op = current_op_id(jj, store);
    jj.run(
        store,
        &[
            "rebase",
            "-b",
            DIV_SIBLING,
            "-o",
            conflicting_dest,
            "--ignore-working-copy",
            "--at-op",
            &base_op,
        ],
        "fork conflicted twin",
    )
    .unwrap();
    jj.run(
        store,
        &[
            "describe",
            DIV_SIBLING,
            "-m",
            "agent resolved re-seal",
            "--ignore-working-copy",
            "--at-op",
            &base_op,
        ],
        "fork clean twin",
    )
    .unwrap();
    // Any normal command merges the divergent op heads.
    let _ = jj.run(
        store,
        &[
            "log",
            "-r",
            "root()",
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
        "trigger op merge",
    );
    let ids = visible_commit_ids_for_change(jj, store, &cid);
    assert_eq!(ids.len(), 2, "fork mints exactly two twins");
    let conflicted = ids
        .iter()
        .find(|c| revset_has_conflict(jj, store, c).unwrap())
        .cloned()
        .expect("one conflicted twin");
    let clean = ids
        .iter()
        .find(|c| !revset_has_conflict(jj, store, c).unwrap())
        .cloned()
        .expect("one clean twin");
    (cid, conflicted, clean)
}

/// (1) Self-heal: a divergent change with one conflicted twin (the
/// base-advance copy) and one clean twin (the agent's resolved re-seal)
/// collapses to the clean twin — the bookmark repoints, the conflicted twin is
/// abandoned, and the change resolves to a single visible commit.
#[test]
#[serial_test::serial(jj)]
fn collapse_self_heals_one_conflicted_one_clean_twin() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping collapse_self_heals_one_conflicted_one_clean_twin: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
    let (cid, conflicted, clean) = fork_conflicted_and_clean(&fx.jj, &fx.store, &dest);

    // Production: the agent's re-seal leaves the bookmark on the CLEAN twin
    // while the base-advance conflicted twin orphans. Pin it there.
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                DIV_SIBLING,
                "-r",
                &clean,
                "--ignore-working-copy",
            ],
            "pin bookmark to clean twin",
        )
        .unwrap();

    // Precondition: divergent, exactly one conflicted + one clean twin.
    assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 2);
    assert!(revset_has_conflict(&fx.jj, &fx.store, &conflicted).unwrap());
    assert!(!revset_has_conflict(&fx.jj, &fx.store, &clean).unwrap());

    let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert_eq!(
        outcome,
        CollapseOutcome::Collapsed {
            kept: clean.clone(),
            abandoned: vec![conflicted.clone()],
        }
    );
    assert_eq!(
        visible_commits_for_change(&fx.jj, &fx.store, &cid),
        1,
        "the change resolves to a single visible commit after collapse"
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        clean,
        "the bookmark points at the surviving clean twin"
    );
}

/// (2) THE #162 closure: after collapsing the divergence, an agent re-seal on
/// the sibling workspace returns `Ok` — `sealed_commit_is_lost` no longer fires
/// (a single visible commit) and the re-sealed commit carries no resurfaced
/// conflict. This is the invariant the thrash violated.
#[test]
#[serial_test::serial(jj)]
fn reseal_after_collapse_returns_ok_without_resurfaced_conflict() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reseal_after_collapse_returns_ok_without_resurfaced_conflict: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    // Park the workspace `@` off the sibling's sealed change before forking, so
    // ONLY the sealed change diverges. In production `@` is a fresh working
    // copy on the sibling tip, never itself divergent; without this, the
    // `--at-op` fork rewrites the whole subtree (including `@`) and corrupts
    // the working-copy commit — a test artifact, not the real failure mode.
    fx.jj
        .run(
            &fx.ws_sibling,
            &["new", "main"],
            "park workspace @ off the sibling",
        )
        .unwrap();
    let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
    let (cid, _conflicted, clean) = fork_conflicted_and_clean(&fx.jj, &fx.store, &dest);
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                DIV_SIBLING,
                "-r",
                &clean,
                "--ignore-working-copy",
            ],
            "pin bookmark to clean twin",
        )
        .unwrap();

    let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert!(matches!(outcome, CollapseOutcome::Collapsed { .. }));
    assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 1);

    // Bring the workspace current onto the collapsed tip (the production
    // re-parent a reconcile performs), then drive an agent re-seal. Before the
    // collapse this seal tripped the lost-seal backstop (still-divergent
    // change) and returned `LOST_SEAL_MSG`; now it lands cleanly.
    let conflicted =
        advance_workspace_onto(&fx.jj, &fx.store, &fx.ws_sibling, DIV_SIBLING, &clean).unwrap();
    assert!(
        !conflicted,
        "advancing onto the clean collapsed tip is conflict-free"
    );
    std::fs::write(fx.ws_sibling.join("resolved.rs"), "resolved\n").unwrap();
    let result = seal(&fx.jj, &fx.ws_sibling, "re-seal after collapse", None);
    assert!(
        result.is_ok(),
        "the re-seal returns Ok (no lost-seal thrash): {result:?}"
    );
    assert!(
        !branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        "the re-sealed sibling carries no resurfaced conflict"
    );
}

/// (3) Ambiguous — both twins conflicted: every twin still conflicts, so there
/// is no single clean keep. The helper holds and surfaces, mutating nothing.
#[test]
#[serial_test::serial(jj)]
fn collapse_holds_when_all_twins_conflicted() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping collapse_holds_when_all_twins_conflicted: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let d1 = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced-1\n");
    // A second distinct conflicting tip off the same base, so the two forked
    // rebases land the sibling on different parents (both conflicted).
    let cid = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
    fx.jj
        .run(&fx.store, &["new", "main"], "new D2 off base")
        .unwrap();
    std::fs::write(fx.store.join("shared.rs"), "int-advanced-2\n").unwrap();
    fx.jj
        .run(&fx.store, &["describe", "-m", "advance 2"], "describe D2")
        .unwrap();
    let d2 = fx
        .jj
        .run(
            &fx.store,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "D2 tip",
        )
        .unwrap()
        .trim()
        .to_string();
    let base_op = current_op_id(&fx.jj, &fx.store);
    for (dest, label) in [(&d1, "rebase onto D1"), (&d2, "rebase onto D2")] {
        fx.jj
            .run(
                &fx.store,
                &[
                    "rebase",
                    "-b",
                    DIV_SIBLING,
                    "-o",
                    dest,
                    "--ignore-working-copy",
                    "--at-op",
                    &base_op,
                ],
                label,
            )
            .unwrap();
    }
    let _ = fx.jj.run(
        &fx.store,
        &[
            "log",
            "-r",
            "root()",
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
        "trigger op merge",
    );
    let ids = visible_commit_ids_for_change(&fx.jj, &fx.store, &cid);
    assert_eq!(ids.len(), 2);
    assert!(ids
        .iter()
        .all(|c| revset_has_conflict(&fx.jj, &fx.store, c).unwrap()));
    // Pin the bookmark to one twin so it resolves to a single tip.
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                DIV_SIBLING,
                "-r",
                &ids[0],
                "--ignore-working-copy",
            ],
            "pin bookmark",
        )
        .unwrap();
    let pinned = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();

    let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    match outcome {
        CollapseOutcome::Ambiguous { change_id, twins } => {
            assert_eq!(change_id, cid);
            assert_eq!(twins.len(), 2);
        }
        other => panic!("expected Ambiguous, got {other:?}"),
    }
    assert_eq!(
        visible_commits_for_change(&fx.jj, &fx.store, &cid),
        2,
        "an ambiguous tangle leaves the store untouched"
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        pinned,
        "the bookmark is not moved"
    );
}

/// (4) Ambiguous — both twins clean (both carry edits): more than one clean
/// keep means picking one would guess. Hold and surface, mutating nothing.
#[test]
#[serial_test::serial(jj)]
fn collapse_holds_when_multiple_clean_twins() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping collapse_holds_when_multiple_clean_twins: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let cid = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
    let base_op = current_op_id(&fx.jj, &fx.store);
    // Two re-describes of the same change from one base op: two clean twins.
    for (msg, label) in [("twin a", "fork clean a"), ("twin b", "fork clean b")] {
        fx.jj
            .run(
                &fx.store,
                &[
                    "describe",
                    DIV_SIBLING,
                    "-m",
                    msg,
                    "--ignore-working-copy",
                    "--at-op",
                    &base_op,
                ],
                label,
            )
            .unwrap();
    }
    let _ = fx.jj.run(
        &fx.store,
        &[
            "log",
            "-r",
            "root()",
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
        "trigger op merge",
    );
    let ids = visible_commit_ids_for_change(&fx.jj, &fx.store, &cid);
    assert_eq!(ids.len(), 2);
    assert!(ids
        .iter()
        .all(|c| !revset_has_conflict(&fx.jj, &fx.store, c).unwrap()));
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                DIV_SIBLING,
                "-r",
                &ids[0],
                "--ignore-working-copy",
            ],
            "pin bookmark",
        )
        .unwrap();
    let pinned = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();

    let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert!(
        matches!(outcome, CollapseOutcome::Ambiguous { .. }),
        "two clean twins are ambiguous: {outcome:?}"
    );
    assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 2);
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        pinned
    );
}

/// (5) A healthy single-commit bookmark is NotDivergent and mutates nothing.
#[test]
#[serial_test::serial(jj)]
fn collapse_noops_on_healthy_bookmark() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping collapse_noops_on_healthy_bookmark: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let cid = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
    let before = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 1);

    let outcome = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert_eq!(outcome, CollapseOutcome::NotDivergent);
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        before
    );
}

/// (6) Idempotence: collapsing an already-collapsed bookmark is a no-op — the
/// second pass sees a single visible commit and returns NotDivergent.
#[test]
#[serial_test::serial(jj)]
fn collapse_is_idempotent() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping collapse_is_idempotent: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
    let (cid, _conflicted, clean) = fork_conflicted_and_clean(&fx.jj, &fx.store, &dest);
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "set",
                DIV_SIBLING,
                "-r",
                &clean,
                "--ignore-working-copy",
            ],
            "pin bookmark to clean twin",
        )
        .unwrap();

    assert!(matches!(
        collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        CollapseOutcome::Collapsed { .. }
    ));
    let after_first = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();

    let second = collapse_divergent_bookmark(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert_eq!(second, CollapseOutcome::NotDivergent);
    assert_eq!(visible_commits_for_change(&fx.jj, &fx.store, &cid), 1);
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        after_first
    );
}

/// The store-owns-merge fold: `merge_into_bookmark` fast-forwards the
/// integration bookmark to the child's *real* commit (not a squash), and
/// refuses a backwards move once integration has advanced past the child.
#[test]
#[serial_test::serial(jj)]
fn merge_into_bookmark_folds_child_and_refuses_backwards() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping merge_into_bookmark_folds_child_and_refuses_backwards: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-1940-coordinator-0";
    let child = "agent/CAIRN-1-builder-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();

    // The child seals a real commit on top of the integration tip.
    std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
    seal(&jj, &ws_child, "child work", None).unwrap();
    let child_tip = bookmark_commit(&jj, &store, child).unwrap();

    // Fold the child's real commit into integration (forward-only).
    merge_into_bookmark(&jj, &store, int, child).unwrap();
    assert_eq!(
        bookmark_commit(&jj, &store, int).unwrap(),
        child_tip,
        "the fold advances integration to the child's real commit, not a squash"
    );

    // Advance integration beyond the child, then attempt to fold the
    // now-older child: a backwards move must be refused.
    jj.run(&store, &["new", int, "--ignore-working-copy"], "new on int")
        .unwrap();
    jj.run(
        &store,
        &[
            "describe",
            "-m",
            "integration advances",
            "--ignore-working-copy",
        ],
        "describe",
    )
    .unwrap();
    jj.run(
        &store,
        &["bookmark", "set", int, "-r", "@", "--ignore-working-copy"],
        "advance int",
    )
    .unwrap();
    assert!(
        merge_into_bookmark(&jj, &store, int, child).is_err(),
        "folding an older child into an advanced integration is refused (forward-only)"
    );

    // The backwards refusal must never leak jj's raw `--allow-backwards`
    // hint (which would clobber the commits that advanced integration); the
    // error is mapped to safe rebase-first guidance.
    let err = merge_into_bookmark(&jj, &store, int, child).unwrap_err();
    assert!(
        !err.to_lowercase().contains("allow-backwards"),
        "the backwards refusal must not surface the dangerous --allow-backwards hint: {err}"
    );
    assert!(
        err.contains("not a descendant"),
        "the sanitized error names the real cause: {err}"
    );
}

/// `bookmark_landed_in` is the ancestor test the merge postcondition and the
/// merged-teardown guard both rely on: a child sealed ON TOP of integration is
/// NOT landed until the fold, and IS landed once `merge_into_bookmark`
/// fast-forwards integration onto it. Empty/missing bookmarks fail closed.
#[test]
#[serial_test::serial(jj)]
fn bookmark_landed_in_tracks_the_fold() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping bookmark_landed_in_tracks_the_fold: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2287-coordinator-0";
    let child = "agent/CAIRN-1-builder-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();

    // The child seals a commit ON TOP of integration: its tip descends from
    // int, so it has NOT yet landed in int.
    std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
    seal(&jj, &ws_child, "child work", None).unwrap();
    assert!(
        !bookmark_landed_in(&jj, &store, child, int),
        "an un-folded child is not landed in integration"
    );

    // Fold it, and now the child's tip IS an ancestor of (equal to) int.
    merge_into_bookmark(&jj, &store, int, child).unwrap();
    assert!(
        bookmark_landed_in(&jj, &store, child, int),
        "once folded, the child has landed in integration"
    );

    // Fail-closed on empty or unknown bookmarks.
    assert!(!bookmark_landed_in(&jj, &store, "", int));
    assert!(!bookmark_landed_in(&jj, &store, child, ""));
    assert!(!bookmark_landed_in(&jj, &store, "agent/nonexistent", int));
}

/// Drive `DIV_SIBLING` into the clean-tip / conflicted-intermediate shape: the
/// integration tip advances with a conflicting `shared.rs` edit, the sibling's
/// original sealed commit is rebased onto it (recording a conflict on that
/// INTERMEDIATE commit), then a resolving seal on top leaves the TIP clean.
/// Returns the advanced integration tip commit id (the flatten dest).
fn make_intermediate_only(fx: &DivergenceFixture) -> String {
    let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
    rebase_branch_onto(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT).unwrap();
    assert!(
        branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        "the rebase records a conflict on the sibling's sealed commit"
    );
    update_stale(&fx.jj, &fx.ws_sibling).unwrap();
    std::fs::write(fx.ws_sibling.join("shared.rs"), "resolved\n").unwrap();
    seal(&fx.jj, &fx.ws_sibling, "resolve conflict", None).unwrap();
    assert!(
        !branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        "the resolving seal leaves the tip clean"
    );
    dest
}

/// A wedged coordinator hub for the CAIRN-2288 merge-time repro, built the way
/// the incident arose. The hub (`int`) seals an edit to `shared.rs`; a child
/// (`child`) branches from it and seals a DISTINCT file; `main` then advances
/// with a CONFLICTING edit to `shared.rs`. The hub auto-rebases onto the
/// advanced main (baking the conflict into the shared `hub-edit` intermediate,
/// which the child also descends from) and the coordinator resolves at its tip
/// and re-seals; the child, dragged onto the same conflicted intermediate,
/// resolves at ITS OWN tip and re-seals. Both branches end with a CLEAN tip
/// over the conflicted intermediate, and — crucially, mirroring the live
/// topology — the child carries its own resolution rather than depending on
/// the hub's. `main_tip` is the advanced base (the flatten dest); origin holds
/// the hub at its pre-conflict tip so a post-merge push is a real advance.
struct WedgedHub {
    _home: TempDir,
    _proj: TempDir,
    _wts: TempDir,
    _origin: TempDir,
    origin_path: PathBuf,
    jj: JjEnv,
    store: PathBuf,
    ws_coord: PathBuf,
    main_tip: String,
    int: &'static str,
    child: &'static str,
}

fn setup_wedged_hub(bin: &str) -> WedgedHub {
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Coordinator integration branch on main, published to origin, with its own
    // edit to shared.rs.
    let int = "agent/CAIRN-2241-coordinator-0";
    let ws_coord = wts.path().join("coord");
    add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
    ensure_bookmark_on_origin(&jj, &store, int).unwrap();
    std::fs::write(ws_coord.join("shared.rs"), "hub-edit\n").unwrap();
    seal(&jj, &ws_coord, "hub edits shared", None).unwrap();

    // A child branches from the hub's tip and seals a DISTINCT file, so it
    // shares the `hub-edit` commit as an ancestor.
    let child = "agent/CAIRN-2284-builder-0";
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(ws_child.join("child.rs"), "child-work\n").unwrap();
    seal(&jj, &ws_child, "child work", None).unwrap();

    // `main` advances out of band with a CONFLICTING edit to shared.rs.
    jj.run(&store, &["new", "main"], "new on main").unwrap();
    std::fs::write(store.join("shared.rs"), "main-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "main advances shared"],
        "describe main",
    )
    .unwrap();
    let main_tip = jj
        .run(
            &store,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "main tip",
        )
        .unwrap()
        .trim()
        .to_string();
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            "@",
            "--ignore-working-copy",
        ],
        "advance main",
    )
    .unwrap();

    // The hub auto-rebases onto the advanced main, baking the conflict into the
    // shared `hub-edit` commit; the child (which descends from it) is dragged
    // onto the same conflicted commit. Resolve each branch at ITS OWN tip and
    // re-seal, leaving both with a CLEAN tip over the conflicted INTERMEDIATE.
    rebase_branch_onto(&jj, &store, int, "main").unwrap();
    assert!(branch_has_conflict(&jj, &store, int).unwrap());
    update_stale(&jj, &ws_coord).unwrap();
    std::fs::write(ws_coord.join("shared.rs"), "hub-resolved\n").unwrap();
    seal(&jj, &ws_coord, "resolve hub conflict", None).unwrap();
    assert!(!branch_has_conflict(&jj, &store, int).unwrap());

    // The child was dragged onto the rewritten conflicted `hub-edit`; resolve
    // it independently (its own resolution commit, not the hub's).
    assert!(branch_has_conflict(&jj, &store, child).unwrap());
    update_stale(&jj, &ws_child).unwrap();
    std::fs::write(ws_child.join("shared.rs"), "hub-resolved\n").unwrap();
    seal(&jj, &ws_child, "resolve child conflict", None).unwrap();
    assert!(!branch_has_conflict(&jj, &store, child).unwrap());

    assert_eq!(
        flatten_state(&jj, &store, &main_tip, int).unwrap(),
        FlattenState::IntermediateOnly,
        "hub: clean tip over a conflicted intermediate"
    );

    let origin_path = origin.path().to_path_buf();
    WedgedHub {
        _home: home,
        _proj: proj,
        _wts: wts,
        _origin: origin,
        origin_path,
        jj,
        store,
        ws_coord,
        main_tip,
        int,
        child,
    }
}

/// Count the commits a range revset resolves to over the store.
fn count_commits(jj: &JjEnv, store: &Path, range: &str) -> usize {
    jj.run(
        store,
        &[
            "log",
            "-r",
            range,
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "count commits",
    )
    .unwrap()
    .lines()
    .filter(|l| !l.trim().is_empty())
    .count()
}

/// The core recovery: a branch with a conflicted INTERMEDIATE commit and a
/// clean tip is classified `IntermediateOnly`, and `flatten_branch_recovery`
/// collapses it to ONE clean commit on the dest whose tree equals the clean
/// tip — no conflict anywhere, exact tree preserved, parented on the dest.
#[test]
#[serial_test::serial(jj)]
fn flatten_recovers_clean_tip_conflicted_intermediate() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping flatten_recovers_clean_tip_conflicted_intermediate: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let dest = make_intermediate_only(&fx);

    assert_eq!(
        flatten_state(&fx.jj, &fx.store, &dest, DIV_SIBLING).unwrap(),
        FlattenState::IntermediateOnly,
        "clean tip over a conflicted intermediate is flatten-recoverable"
    );
    let pre_tree = file_show(&fx.jj, &fx.store, DIV_SIBLING, "shared.rs").unwrap();

    let report =
        flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened recovery")
            .unwrap();
    assert!(
        report.collapsed_conflicted_commits >= 1,
        "the flatten collapsed at least the conflicted intermediate"
    );

    // Exactly one commit remains in dest..branch, and it carries no conflict.
    let range = format!("{dest}..bookmarks(exact:{DIV_SIBLING:?})");
    assert_eq!(
        count_commits(&fx.jj, &fx.store, &range),
        1,
        "the branch is collapsed to a single commit on the dest"
    );
    assert!(
        conflicted_commits(&fx.jj, &fx.store, &range).is_empty(),
        "no conflicted commit survives the flatten"
    );
    assert!(!branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap());

    // The net tree is preserved exactly.
    let post_tree = file_show(&fx.jj, &fx.store, DIV_SIBLING, "shared.rs").unwrap();
    assert_eq!(
        post_tree, pre_tree,
        "the flattened tree equals the clean tip tree"
    );
    assert_eq!(String::from_utf8_lossy(&post_tree), "resolved\n");

    // The single commit's only parent is the dest.
    let parents = fx
        .jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{DIV_SIBLING:?})"),
                "--no-graph",
                "--ignore-working-copy",
                "-T",
                "parents.map(|c| c.commit_id()).join(\",\")",
            ],
            "flattened parents",
        )
        .unwrap();
    assert_eq!(
        parents, dest,
        "the flattened commit is parented on the dest"
    );
}

/// The footprint pre-guard: flattening onto a base the branch does NOT descend
/// from returns the typed guard error and leaves the bookmark UNMUTATED (the
/// squash never runs), so a wrong-base flatten can never revert base files.
#[test]
#[serial_test::serial(jj)]
fn flatten_footprint_guard_rejects_wrong_base() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping flatten_footprint_guard_rejects_wrong_base: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    // A second sibling off the integration branch on a divergent line: the
    // first sibling does not descend from it, so it is a wrong flatten base.
    let wts2 = TempDir::new().unwrap();
    let other = "agent/CAIRN-9-builder-0";
    let ws_other = wts2.path().join("other");
    add_workspace(&fx.jj, &fx.store, &ws_other, other, DIV_INT, None).unwrap();
    std::fs::write(ws_other.join("other2.rs"), "x\n").unwrap();
    seal(&fx.jj, &ws_other, "other edits other2", None).unwrap();
    let wrong_dest = bookmark_commit(&fx.jj, &fx.store, other).unwrap();

    let before = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    let err =
        flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &wrong_dest, "wrong").unwrap_err();
    assert!(
        err.contains("does not descend"),
        "the pre-guard names the wrong-base cause: {err}"
    );
    let after = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    assert_eq!(
        before, after,
        "a rejected flatten does not mutate the bookmark"
    );
}

/// Twin/orphan cleanup: the squash mints a fresh change-id, so every commit
/// sharing the PRE-flatten change-id (the orphaned old lineage tip, and any
/// conflicted divergent twin) is abandoned — no commit retains the old id.
#[test]
#[serial_test::serial(jj)]
fn flatten_abandons_orphaned_change_id_commits() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping flatten_abandons_orphaned_change_id_commits: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let dest = make_intermediate_only(&fx);
    let pre_change = change_id_of(&fx.jj, &fx.store, DIV_SIBLING);
    assert!(
        visible_commits_for_change(&fx.jj, &fx.store, &pre_change) >= 1,
        "precondition: the pre-flatten change-id is visible"
    );

    let report =
        flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened").unwrap();
    assert!(
        !report.abandoned_twins.is_empty(),
        "the orphaned old lineage tip is abandoned"
    );
    assert_eq!(
        visible_commits_for_change(&fx.jj, &fx.store, &pre_change),
        0,
        "no commit retains the pre-flatten change-id after cleanup"
    );
    assert_ne!(
        pre_change,
        change_id_of(&fx.jj, &fx.store, DIV_SIBLING),
        "the flattened commit carries a fresh change-id"
    );
}

/// End-to-end reconcile: a sibling in the clean-tip / conflicted-intermediate
/// shape is FLATTENED by `reconcile_siblings` (not left wedged), classified
/// `rebased_clean`, and pushed so its PR head advances on origin — the branch is
/// pushable/mergeable with no hand-run jj.
#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_flattens_intermediate_only_sibling() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping reconcile_siblings_flattens_intermediate_only_sibling: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(proj.path());
    git(
        proj.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(proj.path(), &["push", "-q", "origin", "main"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-1940-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    ensure_bookmark_on_origin(&jj, &store, int).unwrap();

    let sibling = "agent/CAIRN-1-builder-0";
    let ws = wts.path().join("sib");
    add_workspace(&jj, &store, &ws, sibling, int, None).unwrap();
    std::fs::write(ws.join("shared.rs"), "sibling-edit\n").unwrap();
    seal(&jj, &ws, "sibling edits shared", None).unwrap();
    push_to_origin(&jj, &ws, sibling);
    let origin_before = git_stdout(origin.path(), &["rev-parse", sibling]);

    // Advance the integration tip conflictingly and publish it.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "integration-advanced\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "int advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    jj.run(
        &store,
        &["git", "push", "--remote", "origin", "--bookmark", int],
        "push int",
    )
    .unwrap();

    // Drive the sibling into the clean-tip / conflicted-intermediate shape:
    // rebase onto the conflicting tip, then resolve on top.
    rebase_branch_onto(&jj, &store, sibling, int).unwrap();
    assert!(branch_has_conflict(&jj, &store, sibling).unwrap());
    update_stale(&jj, &ws).unwrap();
    std::fs::write(ws.join("shared.rs"), "resolved\n").unwrap();
    seal(&jj, &ws, "resolve conflict", None).unwrap();
    assert!(!branch_has_conflict(&jj, &store, sibling).unwrap());
    let dest = bookmark_commit(&jj, &store, int).unwrap();
    assert_eq!(
        flatten_state(&jj, &store, &dest, sibling).unwrap(),
        FlattenState::IntermediateOnly
    );

    // The reconcile flattens the sibling (already-on-dest path), classifies it
    // clean, and pushes the flattened tip to origin.
    let report =
        reconcile_siblings(&jj, &store, int, &[(sibling.to_string(), ws.clone())]).unwrap();
    assert_eq!(report.rebased_clean, vec![sibling.to_string()]);
    assert!(report.conflicted.is_empty());

    let range = format!("{dest}..bookmarks(exact:{sibling:?})");
    assert_eq!(
        count_commits(&jj, &store, &range),
        1,
        "sibling collapsed to one commit"
    );
    assert!(conflicted_commits(&jj, &store, &range).is_empty());
    assert!(!branch_has_conflict(&jj, &store, sibling).unwrap());

    let origin_after = git_stdout(origin.path(), &["rev-parse", sibling]);
    assert_ne!(
        origin_before, origin_after,
        "the flattened sibling's PR head advanced on origin"
    );
    // A flattened (clean) bookmark pushes; the wedge is gone.
    assert!(
        jj.run(
            &store,
            &["git", "push", "--remote", "origin", "--bookmark", sibling],
            "re-push flattened",
        )
        .is_ok(),
        "the flattened sibling is pushable"
    );
}

/// Component C: a sibling bookmark riding a conflicted INTERMEDIATE commit
/// (the live `agent/CAIRN-2285-planner-0 @ c6b16933` shape) does NOT block the
/// flatten, is re-pointed onto the flattened commit, and is reported in
/// `repointed_bookmarks` — so a later reconcile finds it clean on the new tip
/// instead of resurrecting the orphaned conflicted lineage.
#[test]
#[serial_test::serial(jj)]
fn flatten_repoints_rider_bookmark_on_conflicted_intermediate() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping flatten_repoints_rider_bookmark_on_conflicted_intermediate: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    // Advance the integration tip conflictingly and rebase the sibling onto it,
    // recording a conflict on the sibling's (now INTERMEDIATE) sealed commit.
    let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
    rebase_branch_onto(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT).unwrap();
    assert!(branch_has_conflict(&fx.jj, &fx.store, DIV_SIBLING).unwrap());

    // A sibling planner bookmark rides the conflicted intermediate.
    let rider = "agent/CAIRN-2285-planner-0";
    let conflicted_intermediate = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    fx.jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "create",
                rider,
                "-r",
                &conflicted_intermediate,
                "--ignore-working-copy",
            ],
            "create rider bookmark",
        )
        .unwrap();

    // Resolve on top so the sibling's TIP is clean over the conflicted intermediate.
    update_stale(&fx.jj, &fx.ws_sibling).unwrap();
    std::fs::write(fx.ws_sibling.join("shared.rs"), "resolved\n").unwrap();
    seal(&fx.jj, &fx.ws_sibling, "resolve conflict", None).unwrap();
    assert_eq!(
        flatten_state(&fx.jj, &fx.store, &dest, DIV_SIBLING).unwrap(),
        FlattenState::IntermediateOnly
    );

    let report =
        flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened").unwrap();

    // The rider did not block the flatten and was re-pointed onto the flattened commit.
    assert!(
        report.repointed_bookmarks.contains(&rider.to_string()),
        "the rider is reported as re-pointed: {:?}",
        report.repointed_bookmarks
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, rider).unwrap(),
        report.flattened_commit,
        "the rider now points at the flattened commit"
    );
    // The re-pointed rider is a clean descendant of the dest — pushable, no
    // orphaned conflicted lineage for a later reconcile to resurrect.
    assert!(!branch_has_conflict(&fx.jj, &fx.store, rider).unwrap());
    assert!(branch_descends_from(&fx.jj, &fx.store, rider, &dest));
}

/// Component B: `operation_id` + `restore_operation` roll a fold back to its
/// exact pre-merge state — after a rebase+fold advances the integration
/// bookmark, restoring the snapshot returns both bookmarks to their pre-merge
/// commits and realigns the backing git refs, and a retry then lands cleanly
/// with no divergent-change accumulation.
#[test]
#[serial_test::serial(jj)]
fn operation_id_and_restore_operation_roll_back_a_fold() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping operation_id_and_restore_operation_roll_back_a_fold: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2288-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let child = "agent/CAIRN-1-builder-0";
    let ws = wts.path().join("child");
    add_workspace(&jj, &store, &ws, child, int, None).unwrap();
    std::fs::write(ws.join("child.rs"), "child\n").unwrap();
    seal(&jj, &ws, "child edits child.rs", None).unwrap();

    let source_pre = bookmark_commit(&jj, &store, child).unwrap();
    let target_pre = bookmark_commit(&jj, &store, int).unwrap();

    // Snapshot, then fold the child into the integration bookmark.
    let op = operation_id(&jj, &store).unwrap();
    rebase_branch_onto(&jj, &store, child, int).unwrap();
    merge_into_bookmark(&jj, &store, int, child).unwrap();
    assert_ne!(
        bookmark_commit(&jj, &store, int).unwrap(),
        target_pre,
        "the fold advanced the integration bookmark"
    );

    // Roll back to the snapshot: both bookmarks return to their pre-merge commits.
    restore_operation(&jj, &store, &op).unwrap();
    assert_eq!(
        bookmark_commit(&jj, &store, child).unwrap(),
        source_pre,
        "the source bookmark is restored to its pre-merge commit"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, int).unwrap(),
        target_pre,
        "the target bookmark is restored to its pre-merge commit"
    );
    // The exported backing git ref realigns with the restored bookmark.
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", int]),
        target_pre,
        "the git ref realigned to the restored target"
    );

    // A retry after the rollback lands cleanly — no empty-commit accumulation,
    // no divergent twin for the child change.
    rebase_branch_onto(&jj, &store, child, int).unwrap();
    merge_into_bookmark(&jj, &store, int, child).unwrap();
    assert!(
        bookmark_landed_in(&jj, &store, child, int),
        "the retried fold carries the child into the integration branch"
    );
    assert_eq!(
        visible_commits_for_change(&jj, &store, &change_id_of(&jj, &store, child)),
        1,
        "no divergent twin accumulated for the child change across the rollback+retry"
    );
}

/// Component A (the load-bearing fix), positive case: a hub with a CLEAN tip
/// over a conflicted INTERMEDIATE (a `main` advance baked the conflict into the
/// hub's own history; the coordinator resolved at the tip and re-sealed) is
/// flattened at merge time, the child rebases + folds onto it, and the
/// integration branch pushes to a bare origin — the wedge is cleared.
#[test]
#[serial_test::serial(jj)]
fn target_flatten_unwedges_child_merge_into_conflicted_hub() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping target_flatten_unwedges_child_merge_into_conflicted_hub: jj not resolvable"
        );
        return;
    };
    let hub = setup_wedged_hub(&bin);
    // The fixture's child: clean tip over its own conflicted intermediate,
    // carrying its own resolution (as the live child B did).
    let child = hub.child;

    // === Replicate store_merge_child's integration path ===
    // 1. Target preflight: flatten the hub onto its base (main_tip).
    let report =
        flatten_branch_recovery(&hub.jj, &hub.store, hub.int, &hub.main_tip, "hub flattened")
            .unwrap();
    assert!(
        report.collapsed_conflicted_commits >= 1,
        "the hub flatten collapsed its conflicted intermediate"
    );
    assert!(!branch_has_conflict(&hub.jj, &hub.store, hub.int).unwrap());
    let flattened_int = bookmark_commit(&hub.jj, &hub.store, hub.int).unwrap();
    advance_workspace_onto(&hub.jj, &hub.store, &hub.ws_coord, hub.int, &flattened_int).unwrap();

    // 2. Rebase the source onto the flattened integration tip. The rebase
    //    re-applies the child's inherited (old) hub lineage, so it now carries a
    //    conflicted intermediate of its own — the SOURCE flatten clears it.
    rebase_branch_onto(&hub.jj, &hub.store, child, hub.int).unwrap();
    if let FlattenState::IntermediateOnly =
        flatten_state(&hub.jj, &hub.store, hub.int, child).unwrap()
    {
        let d = bookmark_commit(&hub.jj, &hub.store, hub.int).unwrap();
        flatten_branch_recovery(&hub.jj, &hub.store, child, &d, "child flattened").unwrap();
    }
    let range = format!("bookmarks(exact:{:?})..bookmarks(exact:{child:?})", hub.int);
    assert!(
        conflicted_commits(&hub.jj, &hub.store, &range).is_empty(),
        "no conflicted commit survives on the child after the flattens"
    );

    // 3. Fold + push: the integration branch advances on origin (was wedged).
    merge_into_bookmark(&hub.jj, &hub.store, hub.int, child).unwrap();
    let origin_before = git_stdout(&hub.origin_path, &["rev-parse", hub.int]);
    track_bookmark(&hub.jj, &hub.store, child).ok();
    push_store_bookmark(&hub.jj, &hub.store, child).unwrap();
    push_store_bookmark(&hub.jj, &hub.store, hub.int).unwrap();
    let origin_after = git_stdout(&hub.origin_path, &["rev-parse", hub.int]);
    assert_ne!(
        origin_before, origin_after,
        "the integration branch advanced on origin after the merge"
    );
    assert!(
        bookmark_landed_in(&hub.jj, &hub.store, child, hub.int),
        "the child's tip is an ancestor of the advanced integration branch"
    );
}

/// Component A, the pinned failure mode: WITHOUT the target flatten, folding a
/// child into the conflicted hub leaves the integration branch's ancestry
/// carrying a conflicted commit, so the push is REFUSED — exactly the live
/// wedge (`Won't push commit ... since it has conflicts`).
#[test]
#[serial_test::serial(jj)]
fn child_merge_push_refused_without_target_flatten() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping child_merge_push_refused_without_target_flatten: jj not resolvable");
        return;
    };
    let hub = setup_wedged_hub(&bin);
    let child = hub.child;

    // Skip the target flatten; fold the child straight into the conflicted hub.
    rebase_branch_onto(&hub.jj, &hub.store, child, hub.int).unwrap();
    merge_into_bookmark(&hub.jj, &hub.store, hub.int, child).unwrap();

    // The integration branch's ancestry now includes the hub's conflicted
    // intermediate, so jj refuses to push it.
    let err = push_store_bookmark(&hub.jj, &hub.store, hub.int).unwrap_err();
    assert!(
        err.to_lowercase().contains("conflict"),
        "the push is refused for the conflicted ancestor: {err}"
    );
}

/// `restore_bookmark` undoes a `squash_branch_onto`: after the squash moves the
/// bookmark to a new flattened commit, restoring it returns the bookmark to the
/// exact pre-squash tip and its full multi-commit lineage. This is the recovery
/// the post-squash flatten guards run so a refused flatten never leaves the
/// branch rewritten. (The footprint guard itself cannot be triggered through the
/// real jj harness — `squash_branch_onto` restores the exact tip tree, so the
/// post/pre footprints are equal by construction — so the restore mechanism is
/// covered directly here rather than via a forced guard failure.)
#[test]
#[serial_test::serial(jj)]
fn restore_bookmark_resets_a_squashed_branch() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping restore_bookmark_resets_a_squashed_branch: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/CAIRN-3001-builder-0";
    let ws = wts.path().join("src");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    for i in 1..=2 {
        std::fs::write(ws.join(format!("c{i}.rs")), format!("c{i}\n")).unwrap();
        seal(&jj, &ws, &format!("c{i}"), None).unwrap();
    }
    let pre_tip = bookmark_commit(&jj, &store, branch).unwrap();

    squash_branch_onto(&jj, &store, branch, "main", "squashed").unwrap();
    assert_ne!(
        bookmark_commit(&jj, &store, branch).unwrap(),
        pre_tip,
        "the squash moved the bookmark off the pre-squash tip"
    );

    restore_bookmark(&jj, &store, branch, &pre_tip).unwrap();
    assert_eq!(
        bookmark_commit(&jj, &store, branch).unwrap(),
        pre_tip,
        "restore returns the bookmark to the exact pre-squash tip"
    );
    assert_eq!(
        count_commits(&jj, &store, &format!("main..bookmarks(exact:{branch:?})")),
        2,
        "the original multi-commit lineage is restored (the squash is fully undone)"
    );
}

/// `squash_branch_onto` collapses a multi-commit branch into a single commit
/// on top of a base, preserving the branch's tree and taking the given
/// message — the store-side primitive that restores the squash shape at a
/// default-branch landing.
#[test]
#[serial_test::serial(jj)]
fn squash_branch_onto_collapses_chain_to_one_commit() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping squash_branch_onto_collapses_chain_to_one_commit: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // A branch cut from main with THREE sealed commits, each adding a file.
    let branch = "agent/CAIRN-2001-builder-0";
    let ws = wts.path().join("src");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    for i in 1..=3 {
        std::fs::write(ws.join(format!("change{i}.rs")), format!("change {i}\n")).unwrap();
        seal(&jj, &ws, &format!("change {i}"), None).unwrap();
    }
    let base = bookmark_commit(&jj, &store, "main").unwrap();

    squash_branch_onto(&jj, &store, branch, "main", "Squashed PR title").unwrap();

    // One commit: its only parent is the base (the main tip).
    let parents = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{branch:?})"),
                "--no-graph",
                "--ignore-working-copy",
                "-T",
                "parents.map(|c| c.commit_id()).join(\",\")",
            ],
            "squash parents",
        )
        .unwrap();
    assert_eq!(
        parents, base,
        "the squashed commit's only parent is the base"
    );

    // Tree equals the source: all three files survive in the single commit.
    let files = jj
        .run(
            &store,
            &["file", "list", "--ignore-working-copy", "-r", branch],
            "squash files",
        )
        .unwrap();
    for i in 1..=3 {
        assert!(
            files.contains(&format!("change{i}.rs")),
            "file change{i}.rs present in the squashed tree: {files}"
        );
    }

    // The single commit carries the squash message (the PR title).
    let desc = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{branch:?})"),
                "--no-graph",
                "--ignore-working-copy",
                "-T",
                "description",
            ],
            "squash description",
        )
        .unwrap();
    assert!(
        desc.contains("Squashed PR title"),
        "the squashed commit's description is the PR title: {desc}"
    );
}

/// `rebase_then_fold_into`'s clean path: the project default branch advances
/// OUT OF BAND past the source's fork point (another PR merged into it), so a
/// bare FF would be refused. The primitive rebases the source onto the
/// advanced default, then FFs the default to it — landing the source's real
/// (rebased) commit, never a squash, and moving the default strictly forward.
#[test]
#[serial_test::serial(jj)]
fn rebase_then_fold_lands_source_after_out_of_band_default_advance() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping rebase_then_fold_lands_source_after_out_of_band_default_advance: jj not resolvable"
            );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // A source branch cut from main, sealing a commit that edits a NEW file
    // (so it never conflicts with the out-of-band default advance below).
    let source = "agent/CAIRN-1987-coordinator-0";
    let ws_src = wts.path().join("src");
    add_workspace(&jj, &store, &ws_src, source, "main", None).unwrap();
    std::fs::write(ws_src.join("feature.rs"), "feature\n").unwrap();
    seal(&jj, &ws_src, "feature work", None).unwrap();

    // The default branch advances OUT OF BAND past the source's fork point,
    // via its own workspace editing a different file, then main FFs to it.
    let oob = "agent/CAIRN-9-oob-0";
    let ws_oob = wts.path().join("oob");
    add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
    std::fs::write(ws_oob.join("infra.rs"), "infra\n").unwrap();
    seal(&jj, &ws_oob, "main advances out of band", None).unwrap();
    let oob_tip = bookmark_commit(&jj, &store, oob).unwrap();
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            &oob_tip,
            "--ignore-working-copy",
        ],
        "advance main out of band",
    )
    .unwrap();

    // A bare FF is refused now (source is sideways from the advanced main).
    assert!(
        merge_into_bookmark(&jj, &store, "main", source).is_err(),
        "precondition: a bare fold is refused once main advanced past the source"
    );

    // Rebase-then-fold against the LOCAL default tip (no remote needed).
    rebase_then_fold_into(&jj, &store, "main", source, "main").unwrap();

    // The source landed as its real rebased commit (not a squash): main and
    // the source bookmark resolve to the same commit.
    assert_eq!(
        bookmark_commit(&jj, &store, "main").unwrap(),
        bookmark_commit(&jj, &store, source).unwrap(),
        "the fold advances main to the source's rebased commit, not a squash"
    );
    // Forward-only: the out-of-band tip is an ancestor of the new main.
    let main_after = bookmark_commit(&jj, &store, "main").unwrap();
    let fwd = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("{oob_tip} & ::{main_after}"),
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "forward-only check",
        )
        .unwrap();
    assert_eq!(
        fwd, oob_tip,
        "main moved forward: the out-of-band commit is an ancestor of the folded tip"
    );
    assert!(
        !branch_has_conflict(&jj, &store, source).unwrap(),
        "the clean rebase recorded no conflict"
    );
}

/// `rebase_then_fold_into`'s conflict path: the source and the out-of-band
/// default advance edit the same file conflictingly. The rebase records a
/// conflict, so the primitive returns a SAFE error (resolve-and-retry, never
/// `--allow-backwards`) and leaves the default bookmark UNCHANGED — it is
/// never moved backward.
#[test]
#[serial_test::serial(jj)]
fn rebase_then_fold_conflict_returns_safe_error_and_leaves_default_unmoved() {
    let Some(bin) = jj_bin() else {
        eprintln!(
                "skipping rebase_then_fold_conflict_returns_safe_error_and_leaves_default_unmoved: jj not resolvable"
            );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // The source edits `shared.rs` (present at base) and seals.
    let source = "agent/CAIRN-1987-coordinator-0";
    let ws_src = wts.path().join("src");
    add_workspace(&jj, &store, &ws_src, source, "main", None).unwrap();
    std::fs::write(ws_src.join("shared.rs"), "source-change\n").unwrap();
    seal(&jj, &ws_src, "source edits shared", None).unwrap();

    // The default advances out of band editing the SAME file conflictingly.
    let oob = "agent/CAIRN-9-oob-0";
    let ws_oob = wts.path().join("oob");
    add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
    std::fs::write(ws_oob.join("shared.rs"), "out-of-band-change\n").unwrap();
    seal(&jj, &ws_oob, "main advances conflictingly", None).unwrap();
    let oob_tip = bookmark_commit(&jj, &store, oob).unwrap();
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            &oob_tip,
            "--ignore-working-copy",
        ],
        "advance main out of band",
    )
    .unwrap();

    let main_before = bookmark_commit(&jj, &store, "main").unwrap();

    let err = rebase_then_fold_into(&jj, &store, "main", source, "main").unwrap_err();
    assert!(
        !err.to_lowercase().contains("allow-backwards"),
        "the conflict error must never surface the dangerous --allow-backwards hint: {err}"
    );
    assert!(
        err.to_lowercase().contains("conflict"),
        "the error explains a conflict was recorded: {err}"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, "main").unwrap(),
        main_before,
        "the default bookmark is left unchanged — never moved backward on a conflict"
    );

    // The conflict was recorded on the source bookmark, but its live
    // workspace `@` was rebased out from under it and is stale. Refreshing it
    // (what `store_merge_child` does via `materialize_source_conflict_in_workspaces`
    // on this conflict) materializes the markers the resolve-and-retry error
    // points the agent at — without this, the guidance would be empty.
    assert!(
        branch_has_conflict(&jj, &store, source).unwrap(),
        "the conflict is recorded on the source bookmark"
    );
    update_stale(&jj, &ws_src).unwrap();
    let on_disk = std::fs::read_to_string(ws_src.join("shared.rs")).unwrap();
    assert!(
        on_disk.contains("<<<<<<<") && on_disk.contains(">>>>>>>"),
        "refreshing the source workspace materializes the conflict markers on disk: {on_disk}"
    );
}

/// A store-side rebase must export the moved bookmark back to the backing git
/// ref. Otherwise jj leaves the bookmark conflicted between the local tip and
/// the stale `@git` tracking ref, which makes later descendant checks stop
/// seeing the branch as already reconciled.
#[test]
#[serial_test::serial(jj)]
fn rebase_branch_exports_git_ref_to_rebased_tip() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping rebase_branch_exports_git_ref_to_rebased_tip: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-2078-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("agent.rs"), "agent work\n").unwrap();
    seal(&jj, &ws, "agent work", None).unwrap();
    let git_before = git_stdout(proj.path(), &["rev-parse", branch]);
    assert_eq!(
        git_before,
        bookmark_commit(&jj, &store, branch).unwrap(),
        "seal exports the initial branch ref"
    );

    advance_project(proj.path());
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    rebase_branch_onto(&jj, &store, branch, "main").unwrap();
    let rebased_tip = bookmark_commit(&jj, &store, branch).unwrap();
    let git_after = git_stdout(proj.path(), &["rev-parse", branch]);

    assert_ne!(git_before, rebased_tip, "the rebase moved the branch tip");
    assert_eq!(
        git_after, rebased_tip,
        "rebase_branch_onto exports the moved bookmark to the backing git ref"
    );
    let bookmarks = jj
        .run(
            &store,
            &["bookmark", "list", branch],
            "jj bookmark list branch",
        )
        .unwrap();
    assert!(
        !bookmarks.contains("@git"),
        "the branch must not remain conflicted against a stale @git ref: {bookmarks}"
    );
}

/// After a fold, the project's backing git ref for the integration branch must
/// track the advanced tip, so a later child — provisioned the way
/// `execution/jobs/worktrees.rs` does (rev-parse the base ref in the project
/// git, then `add_workspace`) — bases on the folded tip rather than a stale
/// pre-merge ref left behind by an earlier child's `jj git export`.
#[test]
#[serial_test::serial(jj)]
fn fold_exports_so_a_later_child_bases_on_the_folded_tip() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping fold_exports_so_a_later_child_bases_on_the_folded_tip: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-1940-coordinator-0";
    let child_a = "agent/CAIRN-1-builder-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let ws_a = wts.path().join("a");
    add_workspace(&jj, &store, &ws_a, child_a, int, None).unwrap();
    std::fs::write(ws_a.join("a.rs"), "a work\n").unwrap();
    // Sealing exports ALL store bookmarks to the project git, creating
    // `refs/heads/<int>` at the *pre-fold* integration tip — the stale ref the
    // bug would later rev-parse.
    seal(&jj, &ws_a, "child a work", None).unwrap();
    let child_tip = bookmark_commit(&jj, &store, child_a).unwrap();
    let int_before = git_stdout(proj.path(), &["rev-parse", int]);
    assert_ne!(
        int_before, child_tip,
        "precondition: the project git int ref starts at the pre-fold tip"
    );

    // Fold child A into integration; this must export the advanced ref.
    merge_into_bookmark(&jj, &store, int, child_a).unwrap();
    let int_after = git_stdout(proj.path(), &["rev-parse", int]);
    assert_eq!(
        int_after, child_tip,
        "the fold exports the advanced integration ref to the backing git"
    );

    // Provision a later child exactly as worktrees.rs does: rev-parse the base
    // ref in the project, then add_workspace off that commit id.
    let base_rev = git_stdout(proj.path(), &["rev-parse", int]);
    let child_b = "agent/CAIRN-2-builder-0";
    let ws_b = wts.path().join("b");
    add_workspace(&jj, &store, &ws_b, child_b, &base_rev, None).unwrap();
    assert_eq!(
        bookmark_commit(&jj, &store, child_b).unwrap(),
        child_tip,
        "the later child bases off the folded integration tip, not a stale project ref"
    );
}

/// Shared setup for the coordinator-advance tests: a coordinator workspace on
/// its integration bookmark plus a child workspace branched from it; the child
/// seals a file and folds into integration. Returns the integration tip after
/// the fold and the coordinator workspace path — whose `@` is now STALE behind
/// the tip (the exact post-merge state CAIRN-1994 is about).
#[cfg(test)]
fn fold_child_leaving_coordinator_stale(
    jj: &JjEnv,
    store: &Path,
    wts: &Path,
) -> (
    String,  /* int_tip */
    PathBuf, /* ws_coord */
    String,  /* int branch */
) {
    let int = "agent/CAIRN-1987-coordinator-0";
    let child = "agent/CAIRN-1988-builder-0";
    let ws_coord = wts.join("coord");
    add_workspace(jj, store, &ws_coord, int, "main", None).unwrap();
    let ws_child = wts.join("child");
    add_workspace(jj, store, &ws_child, child, int, None).unwrap();

    std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
    seal(jj, &ws_child, "child work", None).unwrap();
    merge_into_bookmark(jj, store, int, child).unwrap();
    let int_tip = bookmark_commit(jj, store, int).unwrap();

    // Precondition: the coordinator `@` is stale — its parent is the pre-fold
    // base, not the folded tip, and the child's file is absent on disk.
    let coord_parent = jj
        .run(
            &ws_coord,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "coord @-",
        )
        .unwrap();
    assert_ne!(
        coord_parent, int_tip,
        "precondition: the coordinator @ is stale behind the folded tip"
    );
    assert!(
        !ws_coord.join("child.rs").exists(),
        "precondition: the child's file is absent from the stale coordinator workspace"
    );
    (int_tip, ws_coord, int.to_string())
}

/// `is_stale_error` classifies the two jj refusals the commit barrier must
/// self-heal — the `working copy is stale` message and the `seal_paths`
/// "behind its branch tip" precheck — and nothing else.
#[test]
fn is_stale_error_classifies_the_stale_family() {
    assert!(is_stale_error(
        "Error: The working copy is stale (not updated since operation abc123)."
    ));
    assert!(is_stale_error(
        "seal refused: workspace `agent/x` is behind its branch tip — the branch advanced"
    ));
    assert!(!is_stale_error("nothing to commit, working tree clean"));
    assert!(!is_stale_error("error: pre-commit hook failed"));
    // The lost-seal marker is its OWN family, not folded into the stale one:
    // the cause and remediation differ, so the predicates stay distinct.
    assert!(!is_stale_error(LOST_SEAL_MSG));
    // The conflicted-branch refusal is ALSO its own family — it must PRESERVE
    // the working copy, not discard it — so the stale classifier must not claim
    // it (it deliberately omits the "behind its branch tip" phrase).
    assert!(!is_stale_error(CONFLICTED_BRANCH_SEAL_MSG));
}

/// `is_conflicted_branch_seal_error` recognizes the conflicted-branch marker
/// and rejects every other seal-failure family, so the routing sites can give
/// it its own non-destructive arm without stealing the stale / lost-seal
/// cases that recover by discard / re-seal.
#[test]
fn is_conflicted_branch_seal_error_classifies_the_conflicted_branch_message() {
    assert!(is_conflicted_branch_seal_error(CONFLICTED_BRANCH_SEAL_MSG));
    // Wrapped in the write-path's surrounding text it still classifies.
    assert!(is_conflicted_branch_seal_error(&format!(
        "Applied file changes but the seal was refused: {CONFLICTED_BRANCH_SEAL_MSG}. ..."
    )));
    // Distinct from the stale and lost-seal families it is routed alongside.
    assert!(!is_conflicted_branch_seal_error(
        "Error: The working copy is stale (not updated since operation abc123)."
    ));
    assert!(!is_conflicted_branch_seal_error(
        "seal refused: workspace `agent/x` is behind its branch tip"
    ));
    assert!(!is_conflicted_branch_seal_error(LOST_SEAL_MSG));
    // And neither sibling classifier claims the conflicted-branch message.
    assert!(!is_stale_error(CONFLICTED_BRANCH_SEAL_MSG));
    assert!(!is_lost_seal_error(CONFLICTED_BRANCH_SEAL_MSG));
}

/// Real-store regression guard: when the branch bookmark tip carries a
/// recorded conflict and `@` has been moved to a fresh line off the current
/// base (the deliberate resolve-at-base flatten), `seal_paths` refuses with
/// the DISTINCT conflicted-branch error — NOT the stale "behind its branch
/// tip" message that would route the seal into a destructive discard. This is
/// the empirical confirmation that the conflicted-tip distinguisher fires and
/// the new classifier never lets the silent-data-loss path reach the flatten.
#[test]
#[serial_test::serial(jj)]
fn seal_refuses_conflicted_branch_with_distinct_error() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping seal_refuses_conflicted_branch_with_distinct_error: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path()); // main: shared.rs = "base\n"
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-2081-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    // Feature edit on shared.rs, sealed on the agent branch.
    std::fs::write(ws.join("shared.rs"), "feature change\n").unwrap();
    seal(&jj, &ws, "feature edit", None).unwrap();

    // main advances with a CONFLICTING change to the same file, re-imported.
    std::fs::write(proj.path().join("shared.rs"), "main change\n").unwrap();
    git(proj.path(), &["commit", "-aqm", "main change"]);
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Reconcile: rebase the feature branch onto main — the bookmark tip now
    // carries a recorded conflict.
    rebase_branch_onto(&jj, &store, branch, "main").unwrap();
    assert!(
        branch_has_conflict(&jj, &store, branch).unwrap(),
        "precondition: the rebased bookmark tip carries a recorded conflict"
    );

    // Refresh the now-stale workspace, then move `@` to a fresh line off the
    // current base tip — the resolve-at-base flatten shape, where `@` no longer
    // descends from the conflicted bookmark.
    let _ = update_stale(&jj, &ws);
    let base_tip = revset_commit(&jj, &store, "main").unwrap();
    jj.run(&ws, &["new", &base_tip], "jj new off base").unwrap();
    std::fs::write(ws.join("flat.rs"), "resolved flat\n").unwrap();

    // Sealing through the commit_msg path is refused with the DISTINCT
    // conflicted-branch error, not the stale "behind its branch tip" message.
    let err = seal(&jj, &ws, "flatten", None).unwrap_err();
    assert!(
            is_conflicted_branch_seal_error(&err),
            "a divergent seal over a conflicted bookmark tip returns the conflicted-branch error: {err}"
        );
    assert!(
        !is_stale_error(&err),
        "and it is NOT misclassified as the stale family: {err}"
    );
}

/// `is_lost_seal_error` recognizes the lost-seal marker (even wrapped in the
/// write-path's surrounding text) and rejects unrelated jj errors, including
/// the stale family it is OR'd with at the routing sites.
#[test]
fn is_lost_seal_error_classifies_the_lost_seal_marker() {
    assert!(is_lost_seal_error(LOST_SEAL_MSG));
    assert!(is_lost_seal_error(&format!(
        "Applied file changes but commit failed: {LOST_SEAL_MSG}; the worktree was restored."
    )));
    assert!(!is_lost_seal_error("working copy is stale"));
    assert!(!is_lost_seal_error("nothing to commit"));
    assert!(!is_lost_seal_error(
        "seal refused: workspace `agent/x` is behind its branch tip"
    ));
}

/// Fork a committed change into a DIVERGENT twin via two `--at-op` describes
/// from the same base operation: each rewrites the change to a distinct
/// commit, and merging the divergent op heads keeps BOTH (`<id>/0 /1`). This
/// is the op-fork shape a concurrent, unserialized store advance leaves —
/// reused from the `forked_op_rebase_*` tests, scoped to a single change.
fn fork_into_divergent(jj: &JjEnv, ws: &Path, change_id: &str) {
    let base_op = jj
        .run(
            ws,
            &["op", "log", "--no-graph", "-n", "1", "-T", "id"],
            "op id",
        )
        .unwrap()
        .trim()
        .to_string();
    for (i, msg) in ["twin a", "twin b"].iter().enumerate() {
        jj.run(
            ws,
            &[
                "describe",
                change_id,
                "-m",
                msg,
                "--at-op",
                &base_op,
                "--ignore-working-copy",
            ],
            &format!("fork twin {i}"),
        )
        .unwrap();
    }
    // Any normal command merges the divergent op heads.
    let _ = jj.run(
        ws,
        &[
            "log",
            "-r",
            "root()",
            "--no-graph",
            "-T",
            "commit_id",
            "--ignore-working-copy",
        ],
        "trigger op merge",
    );
}

/// `scoped_dirty` measures the WHOLE working copy for an empty path slice and
/// only the named filesets when scoped. The scoped case is what keeps a
/// legitimately no-op scoped seal (whose unrelated dirt makes the whole `@`
/// look dirty) from false-positiving as a lost seal.
#[test]
#[serial_test::serial(jj)]
fn scoped_dirty_measures_whole_and_scoped_paths() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping scoped_dirty_measures_whole_and_scoped_paths: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("w");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // Clean working copy: nothing dirty either way.
    assert!(!scoped_dirty(&jj, &ws, &[]).unwrap());
    assert!(!scoped_dirty(&jj, &ws, &["a.txt"]).unwrap());

    // Dirt in a.txt: whole-`@` is dirty and a check scoped to a.txt is dirty,
    // but a check scoped to an UNTOUCHED path is NOT — the no-op-scoped guard.
    std::fs::write(ws.join("a.txt"), "change\n").unwrap();
    assert!(scoped_dirty(&jj, &ws, &[]).unwrap());
    assert!(scoped_dirty(&jj, &ws, &["a.txt"]).unwrap());
    assert!(
        !scoped_dirty(&jj, &ws, &["shared.rs"]).unwrap(),
        "a scoped check on an untouched path is clean even when the whole `@` is dirty"
    );
}

/// `sealed_commit_is_lost` flags the empty-with-pre-dirt and divergent shapes
/// and clears a genuine no-op (empty, no pre-dirt) and a real non-empty seal —
/// the true/false-positive matrix the seal-path detection depends on.
#[test]
#[serial_test::serial(jj)]
fn sealed_commit_is_lost_flags_empty_and_divergent_not_clean() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping sealed_commit_is_lost_flags_empty_and_divergent_not_clean: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("w");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // A real, non-empty seal is NOT lost even with pre-commit dirt measured.
    std::fs::write(ws.join("a.txt"), "v1\n").unwrap();
    seal(&jj, &ws, "real work", None).unwrap();
    assert!(
        !sealed_commit_is_lost(&jj, &ws, true).unwrap(),
        "a real non-empty seal is not the lost shape"
    );

    // An EMPTY `@-`: a bare `jj commit` on a clean `@` seals nothing. With
    // pre-commit dirt it is the lost shape; from a genuine no-op (no
    // pre-dirt) it is NOT flagged.
    jj.run(&ws, &["commit", "-m", "empty seal"], "empty commit")
        .unwrap();
    assert!(
        sealed_commit_is_lost(&jj, &ws, true).unwrap(),
        "an empty `@-` despite pre-commit dirt is the lost shape"
    );
    assert!(
        !sealed_commit_is_lost(&jj, &ws, false).unwrap(),
        "an empty `@-` from a genuine no-op (no pre-dirt) is not flagged"
    );

    // A DIVERGENT `@-`: fork the just-sealed change into a twin. Flagged
    // regardless of pre-dirt (a concurrent-op merge, never a clean seal).
    std::fs::write(ws.join("b.txt"), "v2\n").unwrap();
    seal(&jj, &ws, "seal to fork", None).unwrap();
    let cid = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
            "@- change id",
        )
        .unwrap()
        .trim()
        .to_string();
    fork_into_divergent(&jj, &ws, &cid);
    assert_eq!(
        visible_commits_for_change(&jj, &ws, &cid),
        2,
        "precondition: `@-` resolves to a divergent change"
    );
    assert!(
        sealed_commit_is_lost(&jj, &ws, false).unwrap(),
        "a divergent `@-` is the lost shape regardless of pre-dirt"
    );
}

/// End-to-end: a `seal_paths` whose commit lands on a divergent change DETECTS
/// the anomaly, returns a typed lost-seal `Err` (not `Ok` with a phantom sha),
/// and backs the bad commit out so `@` reparents onto its pre-seal parent —
/// the silent-data-loss-as-success regression this fix closes. A normal seal
/// in the same workspace shape still succeeds (no false positive).
#[test]
#[serial_test::serial(jj)]
fn seal_paths_detects_and_backs_out_a_lost_seal() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping seal_paths_detects_and_backs_out_a_lost_seal: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("w");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // A clean seal succeeds normally (the no-false-positive baseline).
    std::fs::write(ws.join("a.txt"), "v1\n").unwrap();
    let ok = seal_paths(&jj, &ws, "seal1", None, &[]).expect("a clean seal succeeds");
    assert!(!ok.sha.is_empty());
    let parent_cid = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
            "@- change id",
        )
        .unwrap()
        .trim()
        .to_string();

    // Fork the sealed parent into a divergent twin. A subsequent seal's own
    // commit then inherits the divergence — the empty/divergent shape a
    // concurrent store advance leaves.
    fork_into_divergent(&jj, &ws, &parent_cid);
    // The fork rewrote the bookmarked commit; repoint the bookmark to the live
    // parent twin so the seal's fast-forward precheck (an orthogonal concern,
    // covered by its own test) passes and this test exercises the ANOMALY path.
    jj.run(
        &ws,
        &[
            "bookmark",
            "set",
            "agent/CAIRN-1-builder-0",
            "-r",
            "@-",
            "--ignore-working-copy",
        ],
        "repoint bookmark to live twin",
    )
    .unwrap();

    std::fs::write(ws.join("b.txt"), "v2\n").unwrap();
    let err = seal_paths(&jj, &ws, "seal2", None, &[])
        .expect_err("a lost seal must surface as Err, not Ok with a phantom sha");
    assert!(
        is_lost_seal_error(&err),
        "the seal error is classified lost-seal: {err}"
    );

    // Backout: `jj abandon @-` reparented `@` onto the original seal1 parent,
    // so the bad seal2 commit is gone rather than reported as committed.
    let after = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id.short()"],
            "@- after backout",
        )
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(
        after, parent_cid,
        "the backed-out seal returns `@` to its pre-seal parent"
    );
}

/// The data-loss regression guard: `discard` on a STALE workspace carrying
/// loose (unsnapshotted) edits self-heals via `update-stale` instead of
/// dead-ending on the stale refusal — leaving the worktree clean and equal to
/// the advanced `@`, with the loose batch edits discarded (not orphaned
/// uncommitted, which is how the production 28-patch batch was later wiped).
#[test]
#[serial_test::serial(jj)]
fn discard_self_heals_stale_working_copy_with_loose_edits() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping discard_self_heals_stale_working_copy_with_loose_edits: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-1-coordinator-0";
    let child = "agent/CAIRN-2-builder-0";
    let ws_coord = wts.path().join("coord");
    add_workspace(&jj, &store, &ws_coord, int, "main", None).unwrap();
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();

    // Seal a sibling commit to rebase the coordinator onto.
    std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
    seal(&jj, &ws_child, "child work", None).unwrap();

    // Loose, UNSNAPSHOTTED edits in the coordinator: write files but run no jj
    // command there, so they never enter `@`. A new file plus a modification.
    std::fs::write(ws_coord.join("loose.txt"), "loose work\n").unwrap();
    std::fs::write(ws_coord.join("shared.rs"), "coordinator change\n").unwrap();

    // Rewrite the coordinator's OWN `@` from the store (the reconcile-rebase
    // shape: `advance_workspace_onto` minus its `update_stale`). Rewriting the
    // workspace's working-copy commit out from under it is what makes the
    // workspace OP-LOG stale — the condition that blocks `jj restore` and
    // `jj commit` alike, unlike a mere bookmark advance. (A fold via
    // `merge_into_bookmark` only advances the bookmark; `jj restore` still
    // succeeds there. This store-side rebase is the true data-loss shape.)
    let source = format!("{}@", workspace_name_for_branch(int));
    jj.run(
        &store,
        &[
            "rebase",
            "-s",
            &source,
            "-o",
            child,
            "--ignore-working-copy",
        ],
        "rebase coordinator @ onto sibling (no update-stale)",
    )
    .unwrap();

    // Precondition: the workspace is now stale, so every working-copy command
    // refuses — the snapshot-taking dirty probe and the rollback alike.
    let dirty = is_working_copy_dirty(&jj, &ws_coord);
    assert!(
        dirty.as_ref().err().is_some_and(|e| is_stale_error(e)),
        "precondition: a stale workspace blocks the snapshot/dirty probe: {dirty:?}"
    );
    // Reproduce the bug: a bare `jj restore` (the OLD discard) is ALSO blocked
    // by staleness and would dead-end, orphaning the loose edits uncommitted.
    let bare = jj.run(&ws_coord, &["restore"], "bare restore");
    let bare_err = bare.expect_err("bare restore is blocked on a stale copy");
    assert!(
        is_stale_error(&bare_err),
        "the block is the stale refusal: {bare_err}"
    );

    // The self-healing discard returns Ok, clears staleness, and discards the
    // loose edits → worktree == fresh @.
    discard(&jj, &ws_coord).unwrap();
    assert!(
        !ws_coord.join("loose.txt").exists(),
        "the loose new file is discarded by the self-heal"
    );
    assert_eq!(
        std::fs::read_to_string(ws_coord.join("shared.rs")).unwrap(),
        "base\n",
        "the loose modification is reverted to the committed base"
    );
    assert!(
        ws_coord.join("child.rs").exists(),
        "update-stale advanced @ onto the rewritten parent, materializing the sibling's file"
    );
    // No longer stale: a dirty check (which snapshots) now succeeds and is clean.
    assert_eq!(
        is_working_copy_dirty(&jj, &ws_coord),
        Ok(false),
        "the worktree is clean and equals the advanced @ after self-heal"
    );
}

/// The #158 recovery contract at the jj layer: a child folds into the
/// coordinator's integration base, leaving the coordinator workspace STALE.
/// The primitives `recover_stale_file_commit` chains — reconcile the stale
/// workspace onto the advanced base (the `update_stale` it calls, here via the
/// production `advance_workspace_onto`), re-apply the agent's edit, then
/// re-seal — must land the batch ON the advanced base, preserving BOTH the
/// merged sibling work and the agent's edit, never losing either. (The
/// loose-edits-during-rebase shape that mints a divergent twin — correctly
/// caught by the lost-seal guard — is a distinct failure tracked as a separate
/// follow-up; this pins the common clean case.)
#[test]
#[serial_test::serial(jj)]
fn stale_seal_recovers_batch_onto_advanced_base() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping stale_seal_recovers_batch_onto_advanced_base: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // A child folds into the coordinator's integration bookmark, advancing the
    // base and leaving the coordinator workspace STALE behind it — the
    // post-advance race that routes a write into recovery.
    let (int_tip, ws_coord, int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

    // Recovery (a): reconcile the stale coordinator onto the advanced base.
    // `advance_workspace_onto` performs the store-side rebase plus the same
    // `update_stale` `recover_stale_file_commit` calls, materializing the
    // merged sibling work. Re-parenting the empty `@` leaves no divergent twin.
    advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();
    assert!(
        ws_coord.join("child.rs").exists(),
        "the merged sibling's work is materialized on the advanced base"
    );

    // Recovery (b): re-apply the agent's batch edit against the advanced base.
    std::fs::write(ws_coord.join("feature.rs"), "agent feature\n").unwrap();

    // Recovery (c): re-seal — lands a real commit on the advanced base.
    let result = seal(&jj, &ws_coord, "agent feature", None).unwrap();
    assert!(
        !result.sha.is_empty(),
        "the recovered batch seals a real commit"
    );
    assert_eq!(
        is_working_copy_dirty(&jj, &ws_coord),
        Ok(false),
        "the worktree equals HEAD after the recovered seal"
    );

    // The sealed commit (`@-`) carries the agent's edit...
    let sealed_names = jj
        .run(
            &ws_coord,
            &["diff", "-r", "@-", "--name-only"],
            "recovered seal contents",
        )
        .unwrap();
    assert!(
        sealed_names.contains("feature.rs"),
        "the recovered seal commits the agent's edit: {sealed_names}"
    );

    // ...and the integration bookmark advanced FORWARD over the merged sibling
    // tip, so neither the sibling's merged work nor the agent's edit was lost.
    let int_after = bookmark_commit(&jj, &store, &int).unwrap();
    let fwd = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("{int_tip} & ::{int_after}"),
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "forward-only check",
        )
        .unwrap();
    assert_eq!(
        fwd, int_tip,
        "the recovered seal advances the base forward over the merged sibling work, never backward"
    );
    assert!(
        ws_coord.join("feature.rs").exists() && ws_coord.join("child.rs").exists(),
        "both the agent's edit and the merged sibling work coexist after recovery"
    );
}

/// `advance_workspace_onto` re-parents the stale coordinator `@` onto the
/// folded integration tip and materializes the merged child's file on disk —
/// the jj-native restoration of §6's post-merge fast-forward. Idempotent: a
/// second advance at the same tip is a no-op.
#[test]
#[serial_test::serial(jj)]
fn advance_coordinator_workspace_after_fold() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping advance_coordinator_workspace_after_fold: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let (int_tip, ws_coord, int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

    let conflicted = advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();
    assert!(
        !conflicted,
        "re-parenting the empty coordinator @ never conflicts"
    );

    let coord_parent_after = jj
        .run(
            &ws_coord,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "coord @- after",
        )
        .unwrap();
    assert_eq!(
        coord_parent_after, int_tip,
        "the coordinator @ is re-parented onto the folded tip"
    );
    assert!(
        ws_coord.join("child.rs").exists(),
        "update-stale materialized the merged child's file in the coordinator workspace"
    );

    // Idempotency under the merge/webhook double-fire: a second advance when
    // `@` already sits on the tip is a no-op.
    let again = advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();
    assert!(!again);
    let coord_parent_twice = jj
        .run(
            &ws_coord,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "coord @- twice",
        )
        .unwrap();
    assert_eq!(
        coord_parent_twice, int_tip,
        "re-running the advance when already on the tip leaves @ in place"
    );
}

/// After the coordinator is advanced onto the folded tip, an edit + `seal`
/// moves the integration bookmark FORWARD to the new sealed commit (a
/// descendant of the folded tip) — never backward, so no merged child is
/// dropped.
#[test]
#[serial_test::serial(jj)]
fn seal_after_fold_advances_bookmark_forward() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping seal_after_fold_advances_bookmark_forward: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let (int_tip, ws_coord, int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());
    advance_workspace_onto(&jj, &store, &ws_coord, &int, &int_tip).unwrap();

    std::fs::write(ws_coord.join("coord.rs"), "coord work\n").unwrap();
    seal(&jj, &ws_coord, "coord work", None).unwrap();

    let int_after = bookmark_commit(&jj, &store, &int).unwrap();
    assert_ne!(
        int_after, int_tip,
        "the seal advanced the integration bookmark"
    );
    // Forward-only: the folded tip is an ancestor of the new bookmark commit.
    let fwd = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("{int_tip} & ::{int_after}"),
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "forward-only check",
        )
        .unwrap();
    assert_eq!(
        fwd, int_tip,
        "the folded tip is an ancestor of the new bookmark (forward-only, never backward)"
    );
}

/// Fix (b) backstop: with the coordinator `@` left deliberately STALE (no
/// advance), a `seal` must fail loudly and — critically — BEFORE creating any
/// commit, so it never produces an orphaned off-branch commit that the generic
/// discard (`jj restore`) could not recover. The working copy stays dirty on
/// the stale line and the integration tip is preserved.
#[test]
#[serial_test::serial(jj)]
fn seal_refuses_non_fast_forward_bookmark_move() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping seal_refuses_non_fast_forward_bookmark_move: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let (int_tip, ws_coord, int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());
    // The coordinator head `@-` before the (refused) seal — must be unchanged
    // afterwards: a true pre-commit guard creates no commit at all.
    let head_before = jj
        .run(
            &ws_coord,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "head before",
        )
        .unwrap();
    // Deliberately skip the advance: the coordinator @ stays stale.
    std::fs::write(ws_coord.join("coord.rs"), "coord work\n").unwrap();
    let result = seal(&jj, &ws_coord, "coord work", None);

    assert!(
        result.is_err(),
        "a stale-@ seal must fail loudly, not silently orphan the commit off the branch"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("behind its branch tip"),
        "the error explains the stale-@ cause: {err}"
    );
    // No orphan: the seal was refused BEFORE `jj commit`, so the workspace
    // head is unchanged and the working copy is still dirty on the stale line.
    let head_after = jj
        .run(
            &ws_coord,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "head after",
        )
        .unwrap();
    assert_eq!(
        head_before, head_after,
        "the refused seal creates NO commit — the workspace head is unchanged (no orphan)"
    );
    assert!(
        is_working_copy_dirty(&jj, &ws_coord).unwrap(),
        "the working-copy changes are NOT sealed away — they remain for a post-advance reseal"
    );
    let int_after = bookmark_commit(&jj, &store, &int).unwrap();
    assert_eq!(
        int_after, int_tip,
        "the refused seal never moves the integration bookmark backward/sideways"
    );
}

/// The archival load-bearing case: jj's auto-rebase churns a descendant's
/// commit-id while its change-id stays stable, and `forward_resolve_commit`
/// maps the now-hidden original commit-id forward to the current one. A
/// non-jj directory yields `None` (identity at the call site).
#[test]
#[serial_test::serial(jj)]
fn forward_resolve_commit_maps_churned_commit_to_current() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping forward_resolve_commit_maps_churned_commit_to_current: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // Seal a parent then a child commit on top of it.
    std::fs::write(ws.join("parent.rs"), "p\n").unwrap();
    seal(&jj, &ws, "parent", None).unwrap();
    std::fs::write(ws.join("child.rs"), "c\n").unwrap();
    seal(&jj, &ws, "child", None).unwrap();

    let child_commit_before = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "child commit",
        )
        .unwrap();
    let child_change = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
            "child change",
        )
        .unwrap();
    // The SHORT form a write tool_result actually records.
    let child_short_before = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
            "child short",
        )
        .unwrap();
    // This is a non-colocated workspace: it has no .git, so a git-in-worktree
    // resolution of the recorded id is impossible — only jj can resolve it.
    assert!(
        !ws.join(".git").exists(),
        "a jj workspace is non-colocated (no .git)"
    );

    // Reword the PARENT (@--), which auto-rebases the child and churns its
    // commit-id while preserving its change-id.
    jj.run(
        &ws,
        &["describe", "-r", "@--", "-m", "parent reworded"],
        "reword parent",
    )
    .unwrap();
    let child_commit_after = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "child commit after",
        )
        .unwrap();
    assert_ne!(
        child_commit_before, child_commit_after,
        "auto-rebase churns the child commit-id"
    );

    let (change_id, current) = forward_resolve_commit(&jj, &ws, &child_commit_before)
        .expect("forward-map resolves a churned commit");
    assert_eq!(change_id, child_change, "the stable change-id is recovered");
    assert_eq!(
        current, child_commit_after,
        "forward-maps the hidden original commit-id to the current one"
    );

    // The same holds for the SHORT, now-hidden id — the form archival actually
    // hands in — with no git in the worktree.
    let (_, from_short) = forward_resolve_commit(&jj, &ws, &child_short_before)
        .expect("forward-map resolves a churned SHORT commit-id");
    assert_eq!(from_short, child_commit_after);

    // A non-jj directory is a clean identity no-op (returns None).
    let plain = wts.path().join("plain-git");
    std::fs::create_dir_all(&plain).unwrap();
    assert!(forward_resolve_commit(&jj, &plain, &child_commit_before).is_none());
}

/// A divergent change resolves to several visible commits; `forward_resolve_commit`
/// must pick the one reachable from the worktree tip (`@`) — the side that
/// lands in the archival pack — rather than erroring on the bare change-id.
#[test]
#[serial_test::serial(jj)]
fn forward_resolve_commit_prefers_tip_reachable_on_divergence() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping forward_resolve_commit_prefers_tip_reachable_on_divergence: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let dir = TempDir::new().unwrap();
    let d = dir.path();
    // A colocated jj repo over a git repo, so `--at-op` divergence is easy to
    // construct and the change-id resolves the same way archival sees it.
    git(d, &["init", "-q", "-b", "main"]);
    git(d, &["config", "user.email", "t@e.com"]);
    git(d, &["config", "user.name", "t"]);
    std::fs::write(d.join("a.txt"), "base\n").unwrap();
    git(d, &["add", "-A"]);
    git(d, &["commit", "-q", "-m", "base"]);
    let jj = JjEnv::resolve(&bin, home.path());
    jj.run(d, &["git", "init", "--colocate"], "colocate")
        .unwrap();
    jj.run(d, &["new"], "new").unwrap();
    std::fs::write(d.join("c.rs"), "c\n").unwrap();
    jj.run(d, &["commit", "-m", "C"], "commit C").unwrap();

    let c0 = jj
        .run(
            d,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "c0",
        )
        .unwrap();
    let change = jj
        .run(
            d,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
            "change",
        )
        .unwrap();
    let op0 = jj
        .run(
            d,
            &["op", "log", "--no-graph", "--limit", "1", "-T", "id"],
            "op0",
        )
        .unwrap();

    // Rewrite C one way, then concurrently rewrite the ORIGINAL C a different
    // way at the earlier operation, creating a divergent change.
    jj.run(d, &["describe", "-r", &c0, "-m", "A"], "describe A")
        .unwrap();
    jj.run(
        d,
        &["--at-op", &op0, "describe", "-r", &c0, "-m", "B"],
        "describe B",
    )
    .unwrap();
    // A normal command resolves the concurrent operations.
    let _ = jj.run(
        d,
        &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
        "resolve ops",
    );

    let visible = jj
        .run(
            d,
            &[
                "log",
                "-r",
                &format!("change_id({change})"),
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
            ],
            "visible",
        )
        .unwrap();
    let count = visible.lines().filter(|l| !l.is_empty()).count();
    assert!(
        count >= 2,
        "the change must be divergent (>=2 visible commits): {visible}"
    );

    let (_, current) =
        forward_resolve_commit(&jj, d, &c0).expect("forward-map resolves a divergent change");
    assert!(
        visible.lines().any(|l| l == current),
        "the chosen commit is one of the divergent commits: {current} in {visible}"
    );
    let reachable = jj
        .run(
            d,
            &[
                "log",
                "-r",
                &format!("({current}) & ::@"),
                "--no-graph",
                "-T",
                "commit_id",
            ],
            "reachable check",
        )
        .unwrap();
    assert!(
        !reachable.is_empty(),
        "forward-map picks the tip-reachable side of the divergence: {current}"
    );
}

/// The populate glob -> jj `snapshot.auto-track` fileset translation: file
/// globs become depth-agnostic `glob:"**/<p>"`, a `dir/` becomes both its
/// subtree and its own path, an empty config keeps jj's `all()` default, and
/// backstop `extra_paths` are added as exact literal filesets.
#[test]
fn populate_auto_track_expr_translates_patterns() {
    use crate::config::project_settings::PopulateConfig;
    let config = PopulateConfig {
        copy: vec![".env".into(), ".env*".into(), "target/".into()],
        symlink: vec!["node_modules/".into()],
    };
    let expr = populate_auto_track_expr(&config, &[]).unwrap();
    assert!(expr.starts_with("all() ~ ("), "got: {expr}");
    assert!(expr.contains("glob:\"**/.env\""), "got: {expr}");
    assert!(expr.contains("glob:\"**/.env*\""), "got: {expr}");
    assert!(expr.contains("glob:\"**/target/**\""), "got: {expr}");
    assert!(expr.contains("glob:\"**/target\""), "got: {expr}");
    assert!(expr.contains("glob:\"**/node_modules/**\""), "got: {expr}");
    assert!(expr.contains("glob:\"**/node_modules\""), "got: {expr}");

    assert!(
        populate_auto_track_expr(&PopulateConfig::default(), &[]).is_none(),
        "empty config leaves jj's all() default untouched"
    );

    let healed =
        populate_auto_track_expr(&PopulateConfig::default(), &["secret/leaked.txt".into()])
            .unwrap();
    assert!(healed.contains("\"secret/leaked.txt\""), "got: {healed}");
}

/// `quote_fileset` wraps a repo-relative path as a jj string literal so paths
/// with fileset metacharacters (a Next.js `(app)` route group) match
/// literally instead of parsing as a fileset expression. `"` and `\` are
/// backslash-escaped per jj's double-quoted-string rules.
#[test]
fn quote_fileset_wraps_and_escapes() {
    // A plain path quotes to itself wrapped in quotes (happy-path no-op).
    assert_eq!(quote_fileset("src/app/page.tsx"), "\"src/app/page.tsx\"");
    // The reported bug: parentheses are preserved verbatim inside the quotes.
    assert_eq!(
        quote_fileset("apps/quarry/src/app/(app)/drawings/page.tsx"),
        "\"apps/quarry/src/app/(app)/drawings/page.tsx\""
    );
    // Other fileset metacharacters ride through literally once quoted.
    assert_eq!(
        quote_fileset("a b/c & d|e~f:g.tsx"),
        "\"a b/c & d|e~f:g.tsx\""
    );
    // A literal double-quote is backslash-escaped.
    assert_eq!(quote_fileset("a\"b.tsx"), "\"a\\\"b.tsx\"");
    // A literal backslash is doubled (and escaped before the quote escape).
    assert_eq!(quote_fileset("a\\b.tsx"), "\"a\\\\b.tsx\"");
}

/// REGRESSION (the reported bug): sealing a path whose directory is a fileset
/// metacharacter group — a Next.js `(app)` route group — must commit cleanly
/// instead of failing with `Failed to parse fileset`. Before the
/// `quote_fileset` fix, the bare `(app)` positional arg parsed as a grouping
/// operator and the whole batch was restored to HEAD, losing the edit.
#[test]
#[serial_test::serial(jj)]
fn seal_paths_commits_path_with_fileset_metacharacters() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping seal_paths_commits_path_with_fileset_metacharacters: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-2019-builder-0", "main", None).unwrap();

    // Edit a file under a parens route-group directory, then path-scope seal it.
    let rel = "apps/quarry/src/app/(app)/drawings/page.tsx";
    let abs = ws.join(rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(&abs, "export default function Page() {}\n").unwrap();

    let res = seal_paths(&jj, &ws, "add drawings page", None, &[rel]).unwrap();
    assert!(
        !res.sha.is_empty(),
        "path-scoped seal of a parens path returns a commit id"
    );

    // The file landed in @- (the sealed commit), not left dangling in @.
    let listed = jj
        .run(&ws, &["file", "list", "-r", "@-"], "file list @-")
        .unwrap();
    assert!(
        listed.contains("(app)/drawings/page.tsx"),
        "the parens path is committed in @-: {listed}"
    );
    assert!(
        !is_working_copy_dirty(&jj, &ws).unwrap(),
        "@ is clean after the path-scoped seal"
    );
}

/// SECURITY: explicitly-populated gitignored content must stay UNCOMMITTED in
/// a jj workspace. With `snapshot.auto-track` set BEFORE the files appear, a
/// populated path that has NO ignore rule in the workspace (the residual
/// leak: ignored only by an untracked source `.gitignore`) never enters `@`
/// and cannot be sealed, while a normal new source file IS tracked and
/// sealable. Runs in the production NON-colocated shape (store + workspace,
/// asserted no `.git` — a colocated repo would mask the bug).
#[test]
#[serial_test::serial(jj)]
fn populate_auto_track_keeps_ignored_content_out_of_snapshot_and_seals() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping populate_auto_track_keeps_ignored_content_out_of_snapshot_and_seals: jj not resolvable");
        return;
    };
    use crate::config::project_settings::PopulateConfig;
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());

    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/CAIRN-7-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    // Production shape: a `.jj`-only workspace with NO `.git`.
    assert!(ws.join(".jj").is_dir());
    assert!(
        !ws.join(".git").exists(),
        "workspace must be .jj-only (non-colocated) or both bugs are masked"
    );

    let config = PopulateConfig {
        copy: vec![".env".into()],
        symlink: vec!["node_modules/".into()],
    };
    // Establish the exclude BEFORE the populated files appear.
    set_populate_auto_track(&jj, &store, &config, &[]).unwrap();

    // Simulate populate: a secret with NO ignore rule in the workspace, a
    // populated dir, plus a normal source file an agent would later write.
    std::fs::write(ws.join(".env"), "SECRET=token\n").unwrap();
    std::fs::create_dir_all(ws.join("node_modules")).unwrap();
    std::fs::write(ws.join("node_modules").join("pkg.js"), "x\n").unwrap();
    std::fs::write(ws.join("real.rs"), "fn main() {}\n").unwrap();

    let dirty = working_copy_dirty_paths(&jj, &ws).unwrap();
    assert!(
        dirty.iter().any(|p| p == "real.rs"),
        "a normal source file must be tracked: {dirty:?}"
    );
    assert!(
        !dirty.iter().any(|p| p.contains(".env")),
        "populated secret must NOT be snapshot-visible: {dirty:?}"
    );
    assert!(
        !dirty.iter().any(|p| p.contains("node_modules")),
        "populated dir must NOT be snapshot-visible: {dirty:?}"
    );

    // A seal must not fold the populated content into the commit.
    seal(&jj, &ws, "agent work", None).unwrap();
    let cfg = home.path().join("jj").join("config.toml");
    let out = crate::env::command(&bin)
        .args(["diff", "-r", "@-", "--name-only"])
        .current_dir(&ws)
        .env("JJ_CONFIG", &cfg)
        .output()
        .unwrap();
    let names = String::from_utf8_lossy(&out.stdout);
    assert!(
        names.contains("real.rs"),
        "the agent's file must be sealed: {names}"
    );
    assert!(
        !names.contains(".env"),
        "the secret must NEVER be sealed: {names}"
    );
    assert!(
        !names.contains("node_modules"),
        "populated dir must NEVER be sealed: {names}"
    );
}

/// Backstop self-heal: a populated path the initial glob translation missed
/// gets tracked; feeding it back through `set_populate_auto_track(extra)` +
/// `untrack_paths` removes it from the snapshot WITHOUT deleting it from disk
/// — the recovery that runs before fail-loud in `prepare_worktree_for_job`.
#[test]
#[serial_test::serial(jj)]
fn untrack_self_heals_a_leaked_populated_path() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping untrack_self_heals_a_leaked_populated_path: jj not resolvable");
        return;
    };
    use crate::config::project_settings::PopulateConfig;
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());

    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-7-builder-0", "main", None).unwrap();

    // A populate config whose globs do NOT cover this path — it leaks.
    let config = PopulateConfig {
        copy: vec![".env".into()],
        symlink: vec![],
    };
    set_populate_auto_track(&jj, &store, &config, &[]).unwrap();
    std::fs::write(ws.join("leaked.secret"), "token\n").unwrap();
    assert!(
        working_copy_dirty_paths(&jj, &ws)
            .unwrap()
            .iter()
            .any(|p| p == "leaked.secret"),
        "the unmatched path is tracked until self-heal"
    );

    // Self-heal: add the exact leaked path to auto-track and untrack it.
    set_populate_auto_track(&jj, &store, &config, &["leaked.secret".into()]).unwrap();
    untrack_paths(&jj, &ws, &["leaked.secret".into()]).unwrap();

    assert!(
        !working_copy_dirty_paths(&jj, &ws)
            .unwrap()
            .iter()
            .any(|p| p == "leaked.secret"),
        "self-heal removes the leaked path from the snapshot"
    );
    assert!(
        ws.join("leaked.secret").exists(),
        "untrack must NOT delete the file from disk"
    );
}

/// A `git rev-parse` test closure over `repo` mirroring the production
/// `GitService::rev_parse` contract: `Some(trimmed_sha)` for a ref git
/// resolves, `None` otherwise (non-zero exit — unborn or unmatched ref).
fn rev_parse_closure(repo: &Path) -> impl Fn(&str) -> Option<String> + '_ {
    move |r: &str| {
        let out = crate::env::git()
            .args(["rev-parse", r])
            .current_dir(repo)
            .output()
            .ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

/// Ladder step 1: a base ref the project git resolves yields its commit SHA,
/// equal to `git rev-parse <ref>`.
#[test]
#[serial_test::serial(jj)]
fn resolve_base_rev_prefers_project_git_sha() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping resolve_base_rev_prefers_project_git_sha: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let expected = git_stdout(proj.path(), &["rev-parse", "main"]);
    let got = resolve_base_rev(&jj, &store, "main", rev_parse_closure(proj.path()));
    assert_eq!(got, expected, "a project git ref resolves to its SHA");
}

/// Ladder step 2: a base ref that is NOT a project git ref but IS a store
/// bookmark (the unsealed-coordinator case) is kept literal, and
/// `add_workspace` provisions off it. Guards the coordinator path.
#[test]
#[serial_test::serial(jj)]
fn resolve_base_rev_keeps_store_only_bookmark() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping resolve_base_rev_keeps_store_only_bookmark: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // A bookmark that lives only in the shared store, never as a git ref in
    // the project repo — the shape of an unsealed coordinator branch.
    let bookmark = "agent/coord-0";
    jj.run(
        &store,
        &["bookmark", "create", bookmark, "-r", "main"],
        "seed store-only bookmark",
    )
    .unwrap();
    let rev_parse = rev_parse_closure(proj.path());
    assert!(
        rev_parse(bookmark).is_none(),
        "the store bookmark is not a project git ref"
    );

    let got = resolve_base_rev(&jj, &store, bookmark, &rev_parse);
    assert_eq!(got, bookmark, "a store-only bookmark is kept literal");

    // And it provisions, the way a child workspace bases off the coordinator.
    let ws = wts.path().join("child");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-9-builder-0", &got, None).unwrap();
    assert!(
        is_jj_dir(&ws),
        "workspace based on the store bookmark provisions"
    );
}

/// Ladder step 3: a base ref matching neither a project git ref nor a store
/// bookmark, in a repo that HAS commits, falls back to the repo's HEAD tip
/// (git parity for a local-only repo with a mismatched default branch name).
#[test]
#[serial_test::serial(jj)]
fn resolve_base_rev_falls_back_to_repo_head() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping resolve_base_rev_falls_back_to_repo_head: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let head = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
    let got = resolve_base_rev(
        &jj,
        &store,
        "does-not-exist",
        rev_parse_closure(proj.path()),
    );
    assert_eq!(
        got, head,
        "an unmatched base falls back to the repo HEAD tip"
    );
}

/// Ladder step 4 — the direct regression test for this bug: an unborn repo
/// (`git init -b main`, no commit) whose default branch resolves nowhere
/// yields `root()`, and `add_workspace(.., "main", "root()", ..)` provisions a
/// workspace and creates the `main` bookmark at root. Before the fix this
/// path produced `Revision "main" doesn't exist`.
#[test]
#[serial_test::serial(jj)]
fn resolve_base_rev_uses_root_for_unborn_repo() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping resolve_base_rev_uses_root_for_unborn_repo: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    // Unborn repo: an initialized repo with the branch set but no commit.
    git(proj.path(), &["init", "-q", "-b", "main"]);
    git(proj.path(), &["config", "user.email", "p@e.com"]);
    git(proj.path(), &["config", "user.name", "P"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let got = resolve_base_rev(&jj, &store, "main", rev_parse_closure(proj.path()));
    assert_eq!(got, "root()", "an unborn repo bases off jj's root commit");

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "main", &got, None).unwrap();
    assert!(
        is_jj_dir(&ws),
        "a workspace on an unborn repo provisions off root()"
    );
    assert!(
        bookmark_commit(&jj, &store, "main").is_some(),
        "the branch bookmark is created at root"
    );
}

// ---------------------------------------------------------------------------
// CAIRN-2422: pre-flight staleness reconcile, amend-conversion, the jj shim,
// and the create-pr empty-delta discriminator.
// ---------------------------------------------------------------------------

/// The load-bearing pre-flight assumption: `update_stale` on a NON-stale
/// (fresh) workspace exits 0 — jj prints "not stale" — so
/// `reconcile_workspace` can run it unconditionally at every tool-call boundary
/// without failing the happy path. Idempotent across repeated runs.
#[test]
#[serial_test::serial(jj)]
fn update_stale_on_fresh_workspace_is_a_clean_noop() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping update_stale_on_fresh_workspace_is_a_clean_noop: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    update_stale(&jj, &ws).expect("update-stale on a fresh workspace is a clean no-op");
    assert_eq!(is_working_copy_dirty(&jj, &ws), Ok(false));
    // Idempotent: running it again is still a no-op.
    update_stale(&jj, &ws).expect("update-stale is idempotent on a fresh workspace");
}

/// Pin the stale+CLEAN sidecar behavior: a workspace whose `@` was rebased out
/// from under it in the store (clean, no loose edits) is genuinely jj-stale — a
/// snapshot is refused — and `update_stale` advances it onto the rewritten
/// commit, leaving a clean working copy with the merged file materialized. This
/// is the shape `reconcile_workspace` relies on for its step-1 heal.
#[test]
#[serial_test::serial(jj)]
fn induced_stale_clean_workspace_heals_via_update_stale() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping induced_stale_clean_workspace_heals_via_update_stale: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let (int_tip, ws_coord, int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

    // Induce GENUINE jj-staleness: rebase the coordinator's working-copy commit
    // in the store WITHOUT refreshing it on disk (the store-side half of
    // `advance_workspace_onto`, minus its `update_stale`).
    let name = workspace_name_for_branch(&int);
    jj.run(
        &store,
        &[
            "rebase",
            "-s",
            &format!("{name}@"),
            "-o",
            &int_tip,
            "--ignore-working-copy",
        ],
        "induce genuine staleness",
    )
    .unwrap();

    // Clean+stale: a snapshot (diff) is now refused with the stale message.
    let probe = is_working_copy_dirty(&jj, &ws_coord);
    assert!(
        probe.is_err() && is_stale_error(&probe.unwrap_err()),
        "precondition: a rebased-out workspace is genuinely stale"
    );

    // update_stale heals it: clean working copy on the advanced commit.
    update_stale(&jj, &ws_coord).unwrap();
    assert_eq!(
        is_working_copy_dirty(&jj, &ws_coord),
        Ok(false),
        "clean+stale heals to a clean advanced @"
    );
    assert!(
        ws_coord.join("child.rs").exists(),
        "the merged sibling file is materialized after the heal"
    );
}

/// The core WS1 fix at the backend level: a workspace sitting BEHIND its
/// advanced branch tip (the coordinator-fold shape) is reconciled by
/// `reconcile_workspace` — `@` re-parented onto the tip, merged sibling work
/// materialized, working copy clean — and a subsequent seal succeeds with no
/// agent-visible error.
#[test]
#[serial_test::serial(jj)]
fn reconcile_workspace_heals_behind_tip_and_reseals() {
    use crate::mcp::vcs::WorktreeVcs;
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_workspace_heals_behind_tip_and_reseals: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let (int_tip, ws_coord, int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

    // Precondition: a seal is refused — `@` is behind the advanced branch tip.
    let premature = seal(&jj, &ws_coord, "premature", None);
    assert!(
        premature
            .as_ref()
            .err()
            .map(|e| is_stale_error(e))
            .unwrap_or(false),
        "precondition: sealing a behind-tip workspace is refused: {premature:?}"
    );

    // Reconcile via the production backend.
    let backend = crate::mcp::vcs::JjBackend::new(JjEnv::resolve(&bin, home.path()));
    backend.reconcile_workspace(&ws_coord).unwrap();

    // `@` now sits on the branch tip; the merged sibling file is materialized.
    let coord_parent = jj
        .run(
            &ws_coord,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "coord @- after reconcile",
        )
        .unwrap();
    assert_eq!(
        coord_parent, int_tip,
        "reconcile re-parents @ onto the branch tip"
    );
    assert!(
        ws_coord.join("child.rs").exists(),
        "the merged sibling work is materialized"
    );
    assert_eq!(
        is_working_copy_dirty(&jj, &ws_coord),
        Ok(false),
        "the worktree is clean after reconcile"
    );

    // A subsequent seal succeeds and advances the branch forward.
    std::fs::write(ws_coord.join("feature.rs"), "coord feature\n").unwrap();
    let sealed = seal(&jj, &ws_coord, "coord feature", None).unwrap();
    assert!(
        !sealed.sha.is_empty(),
        "the post-reconcile seal lands a commit"
    );
    let int_after = bookmark_commit(&jj, &store, &int).unwrap();
    assert_ne!(
        int_after, int_tip,
        "the branch advanced forward past the folded tip"
    );
}

/// `reconcile_workspace` leaves a DIRTY working copy untouched — it does not
/// advance `@` and does not disturb the agent's loose edits — so the seal-time
/// stale-recovery path retains ownership of the dirty case (running an
/// unconditional advance on a dirty tree would mint a divergent recovery
/// commit, jj's own tangle).
#[test]
#[serial_test::serial(jj)]
fn reconcile_workspace_leaves_dirty_workspace_untouched() {
    use crate::mcp::vcs::WorktreeVcs;
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping reconcile_workspace_leaves_dirty_workspace_untouched: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let (_int_tip, ws_coord, _int) = fold_child_leaving_coordinator_stale(&jj, &store, wts.path());

    // The agent left uncommitted work in `@` (behind-tip AND dirty).
    std::fs::write(ws_coord.join("wip.rs"), "work in progress\n").unwrap();

    let backend = crate::mcp::vcs::JjBackend::new(JjEnv::resolve(&bin, home.path()));
    backend.reconcile_workspace(&ws_coord).unwrap();

    // The dirty edit is preserved and `@` was NOT advanced onto the tip.
    assert_eq!(
        std::fs::read_to_string(ws_coord.join("wip.rs")).unwrap(),
        "work in progress\n",
        "the dirty edit is preserved for seal-time handling"
    );
    assert!(
        !ws_coord.join("child.rs").exists(),
        "a dirty tree is left to seal-time, not advanced onto the tip"
    );
}

/// WS3: a `^` amend whose target commit `@-` is SHARED with a sibling bookmark
/// is converted into a regular child commit — the shared commit is never
/// rewritten, only the workspace's own bookmark advances, and the foreign
/// bookmark stays put. The conversion is surfaced on `CommitResult.amend_note`.
#[test]
#[serial_test::serial(jj)]
fn seal_amend_converts_to_child_when_target_commit_is_shared() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping seal_amend_converts_to_child_when_target_commit_is_shared: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-1-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    // Seal a real commit; `@-` is the sealed tip carrying the own branch.
    std::fs::write(ws.join("a.rs"), "one\n").unwrap();
    seal(&jj, &ws, "shared work", None).unwrap();
    let shared_before = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "shared @-",
        )
        .unwrap();

    // Park a FOREIGN bookmark on `@-` (a sibling/integration bookmark).
    jj.run(
        &ws,
        &[
            "bookmark",
            "set",
            "integration",
            "-r",
            "@-",
            "--ignore-working-copy",
        ],
        "park foreign bookmark",
    )
    .unwrap();

    // New edit + `^` amend: because `@-` is shared, convert to a CHILD commit.
    std::fs::write(ws.join("b.rs"), "two\n").unwrap();
    let result = seal(&jj, &ws, "^", None).unwrap();
    assert!(
        result
            .amend_note
            .as_deref()
            .map(|n| n.contains("integration"))
            .unwrap_or(false),
        "the conversion names the shared bookmark: {:?}",
        result.amend_note
    );

    // `@-` is a NEW child commit; the shared commit id is unchanged.
    let child = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
            "child @-",
        )
        .unwrap();
    assert_ne!(
        child, shared_before,
        "a child commit was sealed, not a rewrite"
    );
    let child_parent = jj
        .run(
            &ws,
            &["log", "-r", "@--", "--no-graph", "-T", "commit_id"],
            "child parent",
        )
        .unwrap();
    assert_eq!(
        child_parent, shared_before,
        "the child descends from the shared commit"
    );

    // The own branch advanced to the child; the foreign bookmark stayed put.
    assert_eq!(
        bookmark_commit(&jj, &ws, branch).unwrap(),
        child,
        "the workspace's own bookmark advanced onto the child"
    );
    assert_eq!(
        bookmark_commit(&jj, &ws, "integration").unwrap(),
        shared_before,
        "the foreign bookmark still points at the untouched shared commit"
    );
    // The child carries the amend's new edit.
    let names = jj
        .run(&ws, &["diff", "-r", "@-", "--name-only"], "child contents")
        .unwrap();
    assert!(
        names.contains("b.rs"),
        "the child commits the amend's edit: {names}"
    );
}

/// WS3 boundary: a plain `^` amend whose target commit is NOT shared still
/// squashes into the prior commit (keeping its change id) and sets no
/// amend-conversion note.
#[test]
#[serial_test::serial(jj)]
fn seal_amend_squashes_when_target_commit_is_not_shared() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping seal_amend_squashes_when_target_commit_is_not_shared: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-1-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

    std::fs::write(ws.join("a.rs"), "one\n").unwrap();
    seal(&jj, &ws, "orig", None).unwrap();
    let change_before = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
            "change before amend",
        )
        .unwrap();

    std::fs::write(ws.join("b.rs"), "two\n").unwrap();
    let result = seal(&jj, &ws, "^", None).unwrap();
    assert!(
        result.amend_note.is_none(),
        "no conversion without a foreign bookmark: {:?}",
        result.amend_note
    );
    let change_after = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
            "change after amend",
        )
        .unwrap();
    assert_eq!(
        change_before, change_after,
        "a squash amend keeps the change id (folded, not a new child)"
    );
    let names = jj
        .run(
            &ws,
            &["diff", "-r", "@-", "--name-only"],
            "amended contents",
        )
        .unwrap();
    assert!(
        names.contains("a.rs") && names.contains("b.rs"),
        "both edits are folded into one commit: {names}"
    );
}

/// WS2: the generated jj shim intercepts `jj workspace update-stale` (exit 0 +
/// explanation, real jj never invoked) and execs the real binary for every
/// other command (`--version` output matches the real jj byte-for-byte).
#[cfg(unix)]
#[test]
#[serial_test::serial(jj)]
fn jj_shim_intercepts_update_stale_and_passes_through() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping jj_shim_intercepts_update_stale_and_passes_through: jj not resolvable");
        return;
    };
    // The universal jj shim forwards to an ABSOLUTE bundled path (never a bare
    // `jj`, which would infinitely re-exec through the on-PATH shim), so resolve
    // an absolute jj to bake in; skip if only a bare `jj` is available.
    let abs_jj = if std::path::Path::new(&bin).is_absolute() {
        bin.clone()
    } else {
        match crate::env::find_binary("jj") {
            Ok(p) => p,
            Err(_) => {
                eprintln!(
                    "skipping jj_shim_intercepts_update_stale_and_passes_through: no absolute jj"
                );
                return;
            }
        }
    };
    let home = TempDir::new().unwrap();
    let bin_dir = home.path().join("bin");
    crate::env::ensure_jj_shim_in(&bin_dir, &abs_jj);
    let shim = bin_dir.join("jj");
    assert!(shim.exists(), "the shim script is generated");

    // `workspace update-stale` is intercepted: exit 0, explanatory stderr, and
    // the real jj (pointed at a bogus path) is NEVER invoked.
    let intercepted = std::process::Command::new(&shim)
        .args(["workspace", "update-stale"])
        .env("CAIRN_JJ_BIN", "/definitely/not/a/real/jj")
        .output()
        .unwrap();
    assert!(
        intercepted.status.success(),
        "the intercepted update-stale exits 0"
    );
    let stderr = String::from_utf8_lossy(&intercepted.stderr);
    assert!(
        stderr.contains("Cairn reconciles jj workspace staleness"),
        "the interception explains itself: {stderr}"
    );

    // Every other command execs the real jj: `--version` matches byte-for-byte.
    let via_shim = std::process::Command::new(&shim)
        .arg("--version")
        .env("CAIRN_JJ_BIN", &bin)
        .output()
        .unwrap();
    let direct = crate::env::command(&bin).arg("--version").output().unwrap();
    assert!(via_shim.status.success(), "pass-through --version succeeds");
    assert_eq!(
        via_shim.stdout, direct.stdout,
        "the shim execs the real jj untouched for non-intercepted commands"
    );
}

/// WS4 discriminator: the create-pr gate refuses a branch whose delta versus
/// base is empty. `node_changed_files` reports an empty set over a zero-delta
/// branch (the +0/-0 PR the gate catches) and a non-empty set once work is
/// sealed — the exact boolean the sweep-and-gate turns into a refusal.
#[test]
#[serial_test::serial(jj)]
fn node_changed_files_empty_over_zero_delta_branch() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping node_changed_files_empty_over_zero_delta_branch: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    // No seals yet: the branch delta vs main is empty (the empty-PR shape).
    let empty = node_changed_files(&jj, &ws, Some("main"), None)
        .expect("base resolves so the delta is measurable");
    assert!(
        empty.is_empty(),
        "a branch with no commits over base has an empty delta: {empty:?}"
    );

    // After a seal, the delta is non-empty and the gate would pass.
    std::fs::write(ws.join("feature.rs"), "real work\n").unwrap();
    seal(&jj, &ws, "real work", None).unwrap();
    let nonempty = node_changed_files(&jj, &ws, Some("main"), None).unwrap();
    assert!(
        nonempty.iter().any(|c| c.path == "feature.rs"),
        "a sealed branch carries a real delta: {nonempty:?}"
    );
}
