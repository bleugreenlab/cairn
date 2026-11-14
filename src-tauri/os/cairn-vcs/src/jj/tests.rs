use super::testing::*;
use super::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn failed_amend_description_read_uses_and_announces_fallback() {
    let (description, note) = amend_description(Err("injected description failure".to_string()));
    assert_eq!(description, "amend");
    assert_eq!(
        note.as_deref(),
        Some(
            "the previous description could not be read, so `amend` was used: injected description failure"
        )
    );
}

#[test]
#[serial_test::serial(jj)]
fn seal_amend_converts_when_branch_has_no_owned_commit() {
    let Some(bin) = jj_bin() else { return };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/cairn-1-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    let base = bookmark_commit(&jj, &ws, "main").unwrap();
    write_base_marker(&ws, "main", &base).unwrap();

    std::fs::write(ws.join("first.rs"), "one\n").unwrap();
    let result = seal(&jj, &ws, "^", None).unwrap();
    assert!(
        result
            .amend_note
            .as_deref()
            .is_some_and(|note| note.contains("amend converted to a new commit")),
        "the base-target decision is explicit: {:?}",
        result.amend_note
    );
    let child = bookmark_commit(&jj, &ws, branch).unwrap();
    assert_ne!(child, base, "the integration base was not rewritten");
    let parent = jj
        .run(
            &ws,
            &["log", "-r", "@--", "--no-graph", "-T", "commit_id"],
            "first amend child parent",
        )
        .unwrap();
    assert_eq!(
        parent, base,
        "the first commit descends from the integration base"
    );
}

/// A managed jj store can be nested below the runner's ordinary Git checkout.
/// The agent-only commit exists in jj's backing store, not in that ancestor
/// checkout, so successful ancestor discovery must not select the wrong object
/// database.
#[test]
#[serial_test::serial(jj)]
fn logical_tree_hash_uses_jj_backend_for_agent_only_revision() {
    let Some(bin) = jj_bin() else {
        return;
    };
    let home = TempDir::new().unwrap();
    let runner = TempDir::new().unwrap();
    init_project(runner.path());
    let backing = TempDir::new().unwrap();
    init_project(backing.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = runner
        .path()
        .join("config")
        .join("jj-stores")
        .join("project");
    ensure_project_store(&jj, &store, backing.path()).unwrap();
    let workspaces = TempDir::new().unwrap();
    let workspace = workspaces.path().join("builder");
    add_workspace(
        &jj,
        &store,
        &workspace,
        "agent/CAIRN-3604-builder-0",
        "main",
        None,
    )
    .unwrap();
    std::fs::write(workspace.join("agent-only.rs"), "agent branch\n").unwrap();
    seal(&jj, &workspace, "agent-only revision", None).unwrap();
    let commit = head_commit(&jj, &workspace).unwrap();
    assert!(
        !cairn_platform::env::git()
            .args(["cat-file", "-e", &format!("{commit}^{{commit}}")])
            .current_dir(runner.path())
            .status()
            .unwrap()
            .success(),
        "fixture requires the revision to be absent from the runner checkout"
    );
    assert_eq!(
        logical_tree_hash(&jj, &store, &commit).unwrap(),
        sealed_tree_hash(&jj, &workspace).unwrap()
    );
}

/// A child built on a hub's conflict-flagged commit that carries its OWN
/// resolution (the CAIRN-2288 topology) must come through the hub's heal with
/// its tree exactly as it was: its resolution commit, re-conflicted when the
/// hub's commit below it changes, is restored from its own content rather than
/// the hub's, and the heal never leaves it newly conflicted.
#[test]
#[serial_test::serial(jj)]
fn healing_a_hub_keeps_a_childs_own_resolution() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping healing_a_hub_keeps_a_childs_own_resolution: jj not resolvable");
        return;
    };
    let hub = setup_wedged_hub(&bin);
    let child_tip = bookmark_commit(&hub.jj, &hub.store, hub.child).unwrap();
    let main_tip = bookmark_commit(&hub.jj, &hub.store, "main").unwrap();

    let heal = heal_conflicted_ancestry(&hub.jj, &hub.store, hub.int, &main_tip).unwrap();
    assert_eq!(heal.healed.len(), 1, "{heal:?}");
    assert!(conflicted_commits(
        &hub.jj,
        &hub.store,
        &format!("{main_tip}..bookmarks(exact:{:?})", hub.int)
    )
    .is_empty());
    assert!(
        !branch_has_conflict(&hub.jj, &hub.store, hub.child).unwrap(),
        "the child is not left conflicted"
    );
    assert_eq!(
        String::from_utf8_lossy(&file_show(&hub.jj, &hub.store, hub.child, "shared.rs").unwrap()),
        "hub-resolved\n"
    );
    assert_eq!(
        String::from_utf8_lossy(&file_show(&hub.jj, &hub.store, hub.child, "child.rs").unwrap()),
        "child-work\n"
    );
    assert!(
        hub.jj
            .run(
                &hub.store,
                &[
                    "diff",
                    "--from",
                    &child_tip,
                    "--to",
                    &format!("bookmarks(exact:{:?})", hub.child),
                    "--summary",
                    "--ignore-working-copy",
                ],
                "child tree",
            )
            .unwrap()
            .trim()
            .is_empty(),
        "the child's tree is exactly what it was"
    );
}

fn is_visible(jj: &JjEnv, store: &Path, commit: &str) -> bool {
    count_commits(jj, store, &format!("{commit} & ::visible_heads()")) == 1
}

struct SweepFixture {
    _home: TempDir,
    _proj: TempDir,
    wts: TempDir,
    jj: JjEnv,
    store: PathBuf,
}

fn sweep_fixture(bin: &str) -> SweepFixture {
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    SweepFixture {
        _home: home,
        _proj: proj,
        wts,
        jj,
        store,
    }
}

impl SweepFixture {
    /// Seal one file onto `branch`, cut from `base`, through a workspace that is
    /// then forgotten, so no working copy anchors anything. Returns the tip.
    fn seal_branch(&self, branch: &str, base: &str, file: &str) -> String {
        let ws = self.wts.path().join(branch.replace('/', "-"));
        add_workspace(&self.jj, &self.store, &ws, branch, base, None).unwrap();
        std::fs::write(ws.join(file), format!("{file}\n")).unwrap();
        seal(&self.jj, &ws, &format!("add {file}"), None).unwrap();
        forget_workspace(&self.jj, &self.store, branch).unwrap();
        bookmark_commit(&self.jj, &self.store, branch).unwrap()
    }

    /// An unbookmarked child of `parent`.
    fn stray(&self, parent: &str, message: &str) -> String {
        self.jj
            .run(
                &self.store,
                &[
                    "new",
                    "--no-edit",
                    "-r",
                    parent,
                    "-m",
                    message,
                    "--ignore-working-copy",
                ],
                "stray commit",
            )
            .unwrap();
        self.jj
            .run(
                &self.store,
                &[
                    "log",
                    "-r",
                    &format!("children({parent}) & description(substring:{message:?})"),
                    "--no-graph",
                    "-T",
                    "commit_id",
                    "--ignore-working-copy",
                ],
                "stray commit id",
            )
            .unwrap()
    }
}

/// The sweep hides exactly what nothing references: a stray stack off the base,
/// a stray child of a live branch, nothing a bookmark or a recent write still
/// holds, and without rewriting anything it keeps.
#[test]
#[serial_test::serial(jj)]
fn the_store_sweep_hides_only_what_nothing_references() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping the_store_sweep_hides_only_what_nothing_references: jj not resolvable");
        return;
    };
    let fx = sweep_fixture(&bin);
    let branch = "agent/cairn-7001-builder-0";
    let tip = fx.seal_branch(branch, "main", "kept.rs");
    let main = bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let stray_root = fx.stray(&main, "stray root");
    let stray_tip = fx.stray(&stray_root, "stray tip");
    let off_branch = fx.stray(&tip, "stray off the branch");
    let bases = vec!["main".to_string()];

    let recent = sweep_store_view(&fx.jj, &fx.store, &bases, Duration::from_secs(3600)).unwrap();
    assert_eq!(
        (recent.heads_hidden, recent.commits_hidden),
        (0, 0),
        "a commit younger than the grace window is spared"
    );

    let sweep = sweep_store_view(&fx.jj, &fx.store, &bases, Duration::ZERO).unwrap();
    assert_eq!((sweep.heads_hidden, sweep.commits_hidden), (2, 3));
    for hidden in [&stray_root, &stray_tip, &off_branch] {
        assert!(
            !is_visible(&fx.jj, &fx.store, hidden),
            "{hidden} is still visible"
        );
    }
    assert_eq!(bookmark_commit(&fx.jj, &fx.store, branch).unwrap(), tip);
    assert_eq!(bookmark_commit(&fx.jj, &fx.store, "main").unwrap(), main);
    assert!(is_visible(&fx.jj, &fx.store, &tip));
    assert!(!sweep.operation_before.is_empty());

    let again = sweep_store_view(&fx.jj, &fx.store, &bases, Duration::ZERO).unwrap();
    assert_eq!((again.heads_hidden, again.commits_hidden), (0, 0));
}

#[test]
#[serial_test::serial(jj)]
fn origin_presence_is_workspace_safe() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping origin_presence_is_workspace_safe: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    init_project(project.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("project");
    ensure_project_store(&jj, &store, project.path()).unwrap();
    let workspace = workspaces.path().join("builder");
    add_workspace(&jj, &store, &workspace, "agent/origin-probe", "main", None).unwrap();
    std::fs::write(workspace.join("unsealed.rs"), "unsealed\n").unwrap();
    let commit_before = jj
        .run(
            &workspace,
            &[
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "capture working-copy commit before origin probe",
        )
        .unwrap();

    assert_eq!(
        discover_origin_presence(&jj, &workspace),
        OriginPresence::Absent
    );
    let commit_after = jj
        .run(
            &workspace,
            &[
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "capture working-copy commit after origin probe",
        )
        .unwrap();
    assert_eq!(commit_before, commit_after);
    assert!(is_working_copy_dirty(&jj, &workspace).unwrap());

    let origin = TempDir::new().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    git(
        project.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    assert_eq!(
        discover_origin_presence(&jj, &workspace),
        OriginPresence::Present
    );
}

#[test]
#[serial_test::serial(jj)]
#[cfg(unix)]
fn malformed_post_commit_probe_restores_the_pre_seal_graph_and_bytes() {
    use std::os::unix::fs::PermissionsExt;

    let Some(bin) = jj_bin() else {
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let real_jj = JjEnv::with_binary(bin.clone(), home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&real_jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("w");
    let branch = "agent/CAIRN-2968-builder-0";
    add_workspace(&real_jj, &store, &ws, branch, "main", None).unwrap();
    let parent_before = head_commit(&real_jj, &ws).unwrap();
    let bookmark_before = bookmark_commit(&real_jj, &store, branch).unwrap();
    let bytes = b"probe failure must preserve these bytes\n\0binary\n";
    std::fs::write(ws.join("preserved.bin"), bytes).unwrap();

    let shim = home.path().join("malformed-probe-jj");
    let escaped_bin = bin.replace('\'', "'\\''");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh\ncase \"$*\" in\n  *'commit_id.short() ++ \"|\" ++ if(empty'*) printf 'malformed-probe-output'; exit 0;;\nesac\nexec '{escaped_bin}' \"$@\"\n"
        ),
    )
    .unwrap();
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    let injected = JjEnv::with_binary(shim.to_string_lossy(), home.path());

    let error = seal(&injected, &ws, "must roll back", None).unwrap_err();
    assert!(error.contains("pre-seal operation was restored"), "{error}");
    assert_eq!(std::fs::read(ws.join("preserved.bin")).unwrap(), bytes);
    assert_eq!(head_commit(&real_jj, &ws).unwrap(), parent_before);
    assert_eq!(
        bookmark_commit(&real_jj, &store, branch).unwrap(),
        bookmark_before
    );
    assert!(is_working_copy_dirty(&real_jj, &ws).unwrap());
}

/// A validated executor commit is deliberately not published as a Git ref.
/// The runner must still be able to read its tree through jj's Git backend and
/// fold it into the working-copy change with exactly one jj operation.
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

/// A Cairn-owned seal that cannot PUBLISH is not a seal. The marker says Cairn
/// provisioned this workspace and owns `branch` in it, so landing the sealed
/// commit ON that branch is the operation's contract, not incidental cleanup: a
/// refused export exits 0 (jj reports the refusal on stderr only) and does NOT
/// self-heal on the next seal, so swallowing it leaves the branch silently not
/// carrying work every later reader is told it carries — the CAIRN-3270 shape.
///
/// The refusal here is git's own directory/file rule — `refs/heads/<branch>`
/// cannot exist while `refs/heads/<branch>/inner` does — which is the one freeze
/// the verifier's import-and-re-export repair cannot close.
#[test]
#[serial_test::serial(jj)]
fn a_seal_whose_export_is_refused_rolls_back_and_reports_the_unpublished_commit() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping seal_whose_export_is_refused_rolls_back: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-3280-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    // Occupy the branch's ref path with a directory, so git can never write the
    // ref itself and the export stays refused however often it is retried.
    let main_commit = git_stdout(proj.path(), &["rev-parse", "refs/heads/main"]);
    git(
        proj.path(),
        &["update-ref", "-d", &format!("refs/heads/{branch}")],
    );
    git(
        proj.path(),
        &[
            "update-ref",
            &format!("refs/heads/{branch}/inner"),
            &main_commit,
        ],
    );

    let pre_seal_head = head_commit(&jj, &ws).unwrap();
    std::fs::write(ws.join("work.rs"), "agent work\n").unwrap();

    let error = seal(&jj, &ws, "agent work", None).expect_err(
        "a seal that cannot publish must not \
            report success",
    );

    assert!(
        error.contains(branch),
        "the error names the branch: {error}"
    );
    assert!(
        error.contains("unpublished"),
        "the error says the commit never reached its branch: {error}"
    );

    // The rollback is the load-bearing half. A reported failure that still left
    // the commit in the workspace would strand an orphan off the branch, which
    // the generic discard (`jj restore` only resets `@` to its parent) cannot
    // recover — the same invariant the integrity probe's restore keeps.
    assert_eq!(
        head_commit(&jj, &ws).unwrap(),
        pre_seal_head,
        "the failed seal is rolled back, leaving no orphan commit behind"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(pre_seal_head.as_str()),
        "the bookmark is back where it started"
    );
    // And the agent's work is still on disk to retry with, not destroyed.
    assert_eq!(
        std::fs::read_to_string(ws.join("work.rs")).unwrap(),
        "agent work\n"
    );
}

/// The marker is an OWNERSHIP predicate, not a provisioning receipt. Absent, the
/// checkout is somebody else's — a user-colocated jj repo is the standing case —
/// so the seal commits locally and publishes NOTHING: no bookmark moves and no
/// git ref is written.
///
/// Withholding is the intended answer rather than a degraded one. Absence is in
/// fact the only answer production gives (nothing outside tests provisions a
/// workspace), so a seal that REFUSED on a missing marker would break every
/// ambient seal instead of surfacing a fault. This test pins that, so the
/// behavior is not re-filed as a bug.
#[test]
#[serial_test::serial(jj)]
fn a_seal_in_a_checkout_cairn_does_not_own_commits_and_publishes_nothing() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping seal_in_a_checkout_cairn_does_not_own: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/CAIRN-3280-builder-1";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    let bookmark_before = bookmark_commit(&jj, &store, branch).unwrap();

    // Strip the marker: what remains is the shape of a jj checkout Cairn merely
    // found rather than provisioned.
    std::fs::remove_file(ws.join(".jj").join(BRANCH_MARKER)).unwrap();
    assert!(read_branch_marker(&ws).is_none());

    std::fs::write(ws.join("work.rs"), "somebody else's work\n").unwrap();
    let sealed =
        seal(&jj, &ws, "local commit", None).expect("an unowned checkout still commits locally");

    assert!(!sealed.sha.is_empty(), "the commit is real and addressable");
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(bookmark_before.as_str()),
        "no bookmark may move in a checkout Cairn does not own"
    );
    let exported = cairn_platform::env::git()
        .args(["rev-parse", "--verify", &format!("refs/heads/{branch}")])
        .current_dir(proj.path())
        .output()
        .unwrap();
    assert!(
        !exported.status.success(),
        "no git ref may be written for a branch Cairn does not own"
    );
}

fn advance_project(repo: &Path) -> String {
    std::fs::write(repo.join("more.rs"), "more\n").unwrap();
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "advance"]);
    let out = cairn_platform::env::git()
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

/// A routine Git writer may own `packed-refs.lock` longer than gix's one-second
/// internal wait. Import must remain patient across that transient contention
/// rather than permanently killing the execution being prepared.
#[test]
#[serial_test::serial(jj)]
fn import_waits_for_a_held_then_released_packed_refs_lock() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping packed-refs retry test: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let advanced = advance_project(proj.path());
    git(proj.path(), &["pack-refs", "--all"]);

    let lock = proj.path().join(".git").join("packed-refs.lock");
    std::fs::write(&lock, "held by test\n").unwrap();
    let release = lock.clone();
    let holder = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(1500));
        std::fs::remove_file(release).unwrap();
    });

    let started = std::time::Instant::now();
    import_git(&jj, &store).expect("import must retry until the ordinary Git lock is released");
    holder.join().unwrap();
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the fixture must exercise gix's first lock timeout"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, "main").as_deref(),
        Some(advanced.as_str())
    );
}

#[test]
#[serial_test::serial(jj)]
fn marker_failure_rolls_back_new_workspace_registration_bookmark_and_path() {
    let Some(bin) = jj_bin() else { return };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/CAIRN-2924-marker-failure";
    let ws = wts.path().join("job");

    let error = add_workspace_with_marker_writer(
        &jj,
        &store,
        &ws,
        branch,
        "main",
        None,
        |_path, _branch| Err("injected branch marker failure".into()),
    )
    .unwrap_err();
    assert!(error.contains("injected branch marker failure"));
    assert!(!ws.exists(), "partial workspace directory is removed");
    assert!(bookmark_commit(&jj, &store, branch).is_none());
}

#[test]
#[serial_test::serial(jj)]
fn marker_failure_preserves_preexisting_branch_bookmark() {
    let Some(bin) = jj_bin() else { return };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/CAIRN-2924-existing-bookmark";
    jj.run(
        &store,
        &["bookmark", "create", branch, "-r", "main"],
        "seed existing bookmark",
    )
    .unwrap();
    let tip = bookmark_commit(&jj, &store, branch).unwrap();
    let ws = wts.path().join("job");

    add_workspace_with_marker_writer(&jj, &store, &ws, branch, "main", None, |_path, _branch| {
        Err("injected branch marker failure".into())
    })
    .unwrap_err();

    assert!(!ws.exists(), "new workspace path is rolled back");
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(tip.as_str()),
        "rollback preserves the bookmark that predated this add"
    );
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

/// Manual checks may be requested from agent shells, whose checkouts are plain
/// Git worktrees rather than jj workspaces. Content verification must use the
/// repository coordinate's object store without asking the caller to own jj
/// metadata.
#[test]
fn logical_tree_hash_accepts_plain_git_repository() {
    let repo = TempDir::new().unwrap();
    init_project(repo.path());
    let commit = git_stdout(repo.path(), &["rev-parse", "HEAD"]);
    let expected_tree = git_stdout(repo.path(), &["rev-parse", "HEAD^{tree}"]);
    let jj = JjEnv::resolve("jj-must-not-be-needed", repo.path());

    assert_eq!(
        logical_tree_hash(&jj, repo.path(), &commit).unwrap(),
        expected_tree
    );
    assert_eq!(
        tree_entries(&jj, repo.path(), &commit).unwrap(),
        vec![(
            "shared.rs".to_string(),
            git_stdout(repo.path(), &["rev-parse", "HEAD:shared.rs"])
        )]
    );
}

/// A base advance that replays a branch without touching its change keeps the
/// branch's own patch, even when the base added lines above the hunk or edited
/// the same file far from it; a base edit inside the hunk's context, or another
/// commit on the branch, is a different patch.
#[test]
fn own_patch_survives_a_clean_replay_and_moves_with_the_change() {
    let repo = TempDir::new().unwrap();
    let path = repo.path();
    init_project(path);
    let lines: Vec<String> = (1..=30).map(|n| format!("line {n}")).collect();
    std::fs::write(path.join("a.txt"), lines.join("\n") + "\n").unwrap();
    git(path, &["add", "-A"]);
    git(path, &["commit", "-q", "-m", "seed"]);

    git(path, &["checkout", "-q", "-b", "feature"]);
    let edit = |path: &Path, from: &str, to: &str| {
        let text = std::fs::read_to_string(path.join("a.txt")).unwrap();
        std::fs::write(path.join("a.txt"), text.replacen(from, to, 1)).unwrap();
    };
    edit(path, "line 15\n", "line fifteen\n");
    git(path, &["commit", "-q", "-am", "feature"]);
    let jj = JjEnv::resolve("jj-must-not-be-needed", path);
    let head = |path: &Path| git_stdout(path, &["rev-parse", "feature"]);
    let main = |path: &Path| git_stdout(path, &["rev-parse", "main"]);
    let advance_main = |path: &Path, change: &dyn Fn(&Path)| {
        git(path, &["checkout", "-q", "main"]);
        change(path);
        git(path, &["add", "-A"]);
        git(path, &["commit", "-q", "-m", "main advances"]);
        git(path, &["checkout", "-q", "feature"]);
        git(path, &["rebase", "-q", "main"]);
    };
    let original = own_patch_against(&jj, path, &head(path), &main(path)).unwrap();
    assert!(
        original.patch.contains("+line fifteen"),
        "{}",
        original.patch
    );

    // Another file, then lines added above the hunk, then an edit to the same
    // file well outside the hunk's context: all three replays keep the patch.
    let before = head(path);
    advance_main(path, &|path| {
        std::fs::write(path.join("b.txt"), "b\n").unwrap()
    });
    advance_main(path, &|path| {
        edit(path, "line 1\n", "new 0\nnew 0b\nline 1\n")
    });
    advance_main(path, &|path| edit(path, "line 29\n", "line twenty-nine\n"));
    let replayed = own_patch_against(&jj, path, &head(path), &main(path)).unwrap();
    assert_ne!(head(path), before, "each advance rewrote the branch tip");
    assert_ne!(replayed.base, original.base, "the fork point moved");
    assert_eq!(replayed.patch, original.patch);
    assert_eq!(
        own_patch_against(&jj, path, &before, &main(path)).unwrap(),
        original,
        "the pre-replay tip still reads against the base it was on"
    );

    // A base edit three lines below the change lands in the hunk's context.
    advance_main(path, &|path| edit(path, "line 18\n", "line eighteen\n"));
    let near = own_patch_against(&jj, path, &head(path), &main(path)).unwrap();
    assert_ne!(near.patch, original.patch);

    // A commit on the branch changes its patch whatever the base did.
    let settled = own_patch_against(&jj, path, &head(path), &main(path)).unwrap();
    edit(path, "line 20\n", "line twenty\n");
    git(path, &["commit", "-q", "-am", "more"]);
    let committed = own_patch_against(&jj, path, &head(path), &main(path)).unwrap();
    assert_eq!(committed.base, settled.base);
    assert_ne!(committed.patch, settled.patch);
}

/// Normalization drops only what the base decides: blob ids and hunk
/// positions. Pure.
#[test]
fn normalize_own_patch_drops_index_lines_and_hunk_positions() {
    let patch = "diff --git a/x b/x\nindex 1111111..2222222 100644\n--- a/x\n+++ b/x\n@@ -10,3 +12,3 @@ fn f() {\n ctx\n-old\n+new\n";
    assert_eq!(
        normalize_own_patch(patch),
        "diff --git a/x b/x\n--- a/x\n+++ b/x\n@@\n ctx\n-old\n+new\n"
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

/// `parse_grep_z` strips the `<commit>:` prefix git grep puts on every path when
/// searching a tree, keeps the matched text verbatim even when it holds colons,
/// and drops the empty record after the final newline. Pure.
#[test]
fn parse_grep_z_reads_path_line_and_text() {
    let out = "abc:src/a.rs\x0012\x00assert_eq!(v, \"0256\"); // a: b\nabc:scripts/b.ts\x003\x00x('0256')\n";
    assert_eq!(
        super::parse_grep_z(out, "abc"),
        vec![
            super::GrepHit {
                path: "src/a.rs".into(),
                line: 12,
                text: "assert_eq!(v, \"0256\"); // a: b".into(),
            },
            super::GrepHit {
                path: "scripts/b.ts".into(),
                line: 3,
                text: "x('0256')".into(),
            },
        ]
    );
}

/// `parse_cat_file_batch` reads the length-delimited `--batch` wire format:
/// content is taken by the declared byte count (never scanned for line
/// structure, so a manifest containing a blank line or a `\n<sha> blob` lookalike
/// cannot desynchronize the scan), non-blob and `missing` records carry no body,
/// and a truncated stream ends the scan instead of yielding garbage. Pure — no
/// git binary needed.
#[test]
fn parse_cat_file_batch_reads_length_delimited_records() {
    let body = "[package]\n\nname = \"x\"\n";
    let mut out = Vec::new();
    out.extend_from_slice(format!("aaa blob {}\n", body.len()).as_bytes());
    out.extend_from_slice(body.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(b"deadbeef missing\n");
    out.extend_from_slice(b"ccc tree 4\n");
    out.extend_from_slice(b"xxxx\n");
    out.extend_from_slice(b"bbb blob 2\n");
    out.extend_from_slice(b"hi\n");

    let blobs = super::parse_cat_file_batch(&out);
    assert_eq!(blobs.len(), 2, "only blob records carry content");
    assert_eq!(blobs["aaa"], body.as_bytes());
    assert_eq!(blobs["bbb"], b"hi");

    // A stream cut mid-body yields the records that completed, not a partial one.
    let truncated = &out[..out.len() - 2];
    let blobs = super::parse_cat_file_batch(truncated);
    assert!(blobs.contains_key("aaa"));
    assert!(!blobs.contains_key("bbb"));

    assert!(super::parse_cat_file_batch(b"").is_empty());
}

/// `read_blobs` resolves real object ids through the store's git backend, so
/// check-input derivation reads a sealed tree's manifests with no checkout.
#[test]
#[serial_test::serial(jj)]
fn read_blobs_returns_sealed_tree_content_without_a_checkout() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping read_blobs_returns_sealed_tree_content: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let ws = wts.path().join("a");
    add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None).unwrap();

    let manifest = "[package]\nname = \"probe\"\n";
    std::fs::write(ws.join("Cargo.toml"), manifest).unwrap();
    seal(&jj, &ws, "add a manifest", None).unwrap();
    let commit = head_commit(&jj, &ws).unwrap();

    let entries = super::tree_entries(&jj, &ws, &commit).unwrap();
    let blob = entries
        .iter()
        .find(|(path, _)| path == "Cargo.toml")
        .map(|(_, blob)| blob.clone())
        .expect("the sealed tree lists the manifest");

    let blobs = super::read_blobs(&jj, &ws, &[blob.as_str()]).unwrap();
    assert_eq!(
        blobs.get(&blob).map(|bytes| String::from_utf8_lossy(bytes)),
        Some(std::borrow::Cow::Borrowed(manifest)),
        "the manifest's bytes come back by object id alone"
    );
    assert!(
        super::read_blobs(&jj, &ws, &[]).unwrap().is_empty(),
        "an empty id list spawns nothing"
    );
}

/// A seal followed by `push_store_bookmark` lands the workspace's bookmark on a
/// bare `origin` — the in-tree form of the bare-origin spike.
#[test]
#[serial_test::serial(jj)]
fn push_store_bookmark_lands_bookmark_in_bare_origin() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping push_store_bookmark_lands_bookmark_in_bare_origin: jj not resolvable");
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

    push_store_bookmark(&jj, &store, branch).unwrap();

    let refs = git_stdout(
        origin.path(),
        &["for-each-ref", "--format=%(refname)", "refs/heads/"],
    );
    assert!(
        refs.contains(branch),
        "pushed bookmark {branch} must appear on origin: {refs}"
    );
}

#[test]
#[serial_test::serial(jj)]
fn push_store_bookmark_reports_publication_failure() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping push_store_bookmark_reports_publication_failure: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let unavailable_origin = home.path().join("unavailable-origin");
    git(
        proj.path(),
        &[
            "remote",
            "add",
            "origin",
            &unavailable_origin.to_string_lossy(),
        ],
    );

    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/CAIRN-2679-builder-1";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("f.rs"), "x\n").unwrap();
    seal(&jj, &ws, "agent work", None).unwrap();

    let error = push_store_bookmark(&jj, &store, branch).unwrap_err();
    assert!(error.contains("jj git push"), "{error}");
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

/// The regression fence for the defect the three-way merge primitive exists to
/// close, over a real store rather than in the abstract.
///
/// The shape is the one that produced it: a small conflict at the top of a file,
/// and a block of incoming work further down that the branch never touched. A
/// top-only resolution looks complete to the agent who wrote it, and
/// `take-committed-tip` then restores the file WHOLE and the block simply
/// vanishes — surfacing much later as a compile error about something nobody
/// edited.
///
/// The arc asserted here is the whole remedy: the invariant sees the loss, the
/// completion candidate carries both sides, committing it satisfies the
/// invariant, and the replay then lands a file containing both.
#[test]
#[serial_test::serial(jj)]
fn a_whole_file_restore_that_would_drop_incoming_work_is_detected_and_remediable() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping a_whole_file_restore_that_would_drop_incoming_work_is_detected_and_remediable: jj not resolvable"
        );
        return;
    };
    // The branch changes only the header. Main changes the header too (so the
    // rebase genuinely conflicts) AND appends an unrelated block far away from
    // it, which merges cleanly and is therefore invisible in the conflict.
    const BASE: &str = "header\nbody-1\nbody-2\nbody-3\n";
    const OURS: &str = "header-from-branch\nbody-1\nbody-2\nbody-3\n";
    const THEIRS: &str =
        "header-from-main\nbody-1\nbody-2\nbody-3\ntimeout_secs = 30\nretries = 3\n";

    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let advance_main = |content: &str, message: &str| {
        jj.run(&store, &["new", "main"], "new on main").unwrap();
        std::fs::write(store.join("shared.rs"), content).unwrap();
        jj.run(&store, &["describe", "-m", message], "describe main")
            .unwrap();
        jj.run(
            &store,
            &["bookmark", "set", "main", "-r", "@"],
            "advance main",
        )
        .unwrap();
    };

    advance_main(BASE, "base content both sides fork from");

    let branch = "agent/CAIRN-3627-builder-0";
    let ws = wts.path().join("builder");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("shared.rs"), OURS).unwrap();
    seal(&jj, &ws, "branch rewrites the header", None).unwrap();

    advance_main(THEIRS, "main rewrites the header and appends config");

    let RebaseOutcome::Conflicted { diagnostic } =
        rebase_branch_onto(&jj, &store, branch, "main").unwrap()
    else {
        panic!("precondition: the two header edits must conflict");
    };
    let paths = diagnostic.conflicting_paths();
    assert_eq!(paths, vec!["shared.rs".to_string()]);
    let (base, theirs) = (
        diagnostic.base.clone().unwrap(),
        diagnostic.theirs.clone().unwrap(),
    );

    // The branch's own tip, unchanged by the rolled-back rebase. Restoring
    // `shared.rs` whole from it would keep the header resolution and throw the
    // appended config away.
    let unresolved_tip = bookmark_commit(&jj, &store, branch).unwrap();
    let assessed = assess_paths(&jj, &store, &base, &unresolved_tip, &theirs, &paths);
    let RestoreVerdict::Lossy(dropped) = &assessed[0].verdict else {
        panic!(
            "a whole-file restore that discards the appended config must be reported as lossy, \
             got {:?}",
            assessed[0].verdict
        );
    };
    assert!(
        dropped.diff.contains("+timeout_secs = 30") && dropped.diff.contains("+retries = 3"),
        "the diff names exactly the incoming work the restore would drop: {}",
        dropped.diff
    );
    assert_eq!(
        dropped.candidate,
        THEIRS.replace("header-from-main", "header-from-branch"),
        "the candidate keeps the branch's header and carries main's appended config"
    );

    // The documented remedy: commit the candidate the resource hands over.
    update_stale(&jj, &ws).unwrap();
    std::fs::write(ws.join("shared.rs"), &dropped.candidate).unwrap();
    seal(
        &jj,
        &ws,
        "carry the incoming config alongside the resolution",
        None,
    )
    .unwrap();

    let resolved_tip = bookmark_commit(&jj, &store, branch).unwrap();
    assert_eq!(
        assess_paths(&jj, &store, &base, &resolved_tip, &theirs, &paths)[0].verdict,
        RestoreVerdict::Lossless,
        "once the tip carries both sides the whole-file restore is exactly right"
    );

    // And the replay that restore drives lands a file containing both sides.
    let report = reconcile_resolved_sibling_without_publication(
        &jj,
        &store,
        "main",
        branch,
        &ResolutionCarry::from_tip(&resolved_tip, &paths),
    )
    .unwrap();
    assert_eq!(report.rebased_clean, vec![branch.to_string()]);
    let landed = String::from_utf8(file_show(&jj, &store, branch, "shared.rs").unwrap()).unwrap();
    assert!(
        landed.contains("header-from-branch"),
        "the branch's resolution survives: {landed}"
    );
    assert!(
        landed.contains("timeout_secs = 30") && landed.contains("retries = 3"),
        "and the incoming work that used to vanish is still there: {landed}"
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
    jj: JjEnv,
    store: PathBuf,
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
    rebase_recording_conflict(&jj, &store, int, "main");
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

    WedgedHub {
        _home: home,
        _proj: proj,
        _wts: wts,
        _origin: origin,
        jj,
        store,
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

/// The other arm of the same specimen: a branch that already sat on its dest,
/// so this pass rebased nothing and there is nothing to roll back. A lost lock
/// there used to be filed as a content conflict and steer the agent to
/// resolve markers that did not exist.
#[test]
fn an_unmoved_branch_whose_heal_cannot_run_is_a_retried_failure_not_a_conflict() {
    let branch = "agent/cairn-4994-builder-0";
    let mut lock = ReconcileReport::default();
    record_unmoved_heal_refusal(
        &mut lock,
        branch,
        HealError::Failed(
            "jj restore (ancestry heal) failed: The lock for the packed-ref file could not be \
             obtained\npacked-refs lock: contention — held by live process 77 (git) after 30s"
                .to_string(),
        ),
    );
    assert!(lock.conflicted.is_empty(), "a lock is not a conflict");
    assert_eq!(lock.failed.len(), 1);
    assert_eq!(
        reconcile_failure_kind(&lock.failed[0].error),
        REF_LOCK_CONTENTION
    );

    let mut guard = ReconcileReport::default();
    let refusal = HealRefusal {
        guard: HealGuard::TipTreeChanged,
        pre_heal_tip: "1dc97f55".to_string(),
        pre_heal_tree: "0520dad1".to_string(),
        healed_tip: Some("920cd1ed".to_string()),
        healed_tree: Some("b93db1c1".to_string()),
        paths: vec!["permission.rs".to_string()],
    };
    record_unmoved_heal_refusal(
        &mut guard,
        branch,
        HealError::Refused(Box::new(refusal.clone())),
    );
    assert_eq!(
        guard.conflicted,
        vec![branch.to_string()],
        "a guard refusal is still a finding about the tree"
    );
    assert!(guard.failed.is_empty());
    let diagnostic = &guard.conflict_diagnostics[branch];
    assert_eq!(
        diagnostic.condition,
        ConflictCondition::HealRefused,
        "never a content conflict: there is no content to resolve"
    );
    assert_eq!(diagnostic.heal_refusal.as_ref(), Some(&refusal));
    assert_eq!(
        refusal.to_string(),
        "ancestry heal guard: the healed tip `920cd1ed` (tree `b93db1c1`) does not hold the tree \
         `0520dad1` of the pre-heal tip `1dc97f55`; differs in: permission.rs"
    );
}

/// A rollback that did not complete outranks the lock that caused it. Such an
/// error quotes the lock, and filing it as routine contention would tell the
/// agent and the page that nothing moved when the store may disagree with git.
#[test]
fn an_incomplete_rollback_outranks_a_nested_lost_lock() {
    let compound = format!(
        "jj rebase of `b` onto `main` moved the bookmark, and exporting it to git failed: The \
         lock for the packed-ref file could not be obtained\n\
         packed-refs lock: contention — held by live process 77 (git) after 30s\n{}",
        rollback_incomplete(
            "rolling the rebase back to operation op1 also failed: The lock for \
                             the packed-ref file could not be obtained"
        )
    );
    assert_eq!(reconcile_failure_kind(&compound), ROLLBACK_FAILED);
    assert!(!reconcile_failure_is_ref_lock(ROLLBACK_FAILED));
    assert!(!reconcile_failure_is_permanent(ROLLBACK_FAILED));
    assert_eq!(
        ref_lock_deferral(&compound),
        None,
        "it does not defer the pass either"
    );

    // An unmoved branch whose heal failed and could not be undone is a
    // reported failure, whatever refused it first.
    let branch = "agent/cairn-4994-builder-0";
    let mut report = ReconcileReport::default();
    record_unmoved_heal_refusal(
        &mut report,
        branch,
        HealError::Failed(format!(
            "the healed tip carries jj conflict scaffolding\n{}",
            rollback_incomplete(
                "the refused heal of `b` could not be undone to operation op1: lock"
            )
        )),
    );
    assert!(report.conflicted.is_empty(), "{report:?}");
    assert_eq!(report.failed.len(), 1);
    assert_eq!(
        reconcile_failure_kind(&report.failed[0].error),
        ROLLBACK_FAILED
    );
    assert!(
        !report.failed[0].error.contains("unchanged"),
        "it claims nothing about the branch: {}",
        report.failed[0].error
    );
}

#[test]
fn lost_packed_refs_locks_classify_as_infrastructure_never_permanent() {
    let contention = "jj rebase failed: Internal error: Unexpected error from backend\n\
                      The lockfile at '/r/.git/packed-refs.lock' might need manual deletion.\n\
                      packed-refs lock: contention — held by live process 77 (git) after 30s";
    let stale = "jj rebase failed: The lock for the packed-ref file could not be obtained\n\
                 packed-refs lock: stale — older than 30s with no live holder, not removed: \
                 the lock was replaced after stale-lock proof";
    let bare = "seal failed: The lock for the packed-ref file could not be obtained";
    assert_eq!(reconcile_failure_kind(contention), REF_LOCK_CONTENTION);
    assert_eq!(reconcile_failure_kind(stale), REF_LOCK_STALE);
    assert_eq!(
        reconcile_failure_kind(bare),
        REF_LOCK_CONTENTION,
        "a lock lost outside patience carries no line and is still contention"
    );
    for kind in [REF_LOCK_CONTENTION, REF_LOCK_STALE] {
        assert!(!reconcile_failure_is_permanent(kind));
        assert!(reconcile_failure_is_ref_lock(kind));
    }
    assert!(!reconcile_failure_is_ref_lock("transient_process"));

    assert_eq!(
        ref_lock_diagnosis(contention).as_deref(),
        Some("packed-refs lock: contention — held by live process 77 (git) after 30s")
    );
    assert!(ref_lock_diagnosis(bare)
        .unwrap()
        .starts_with(CONTENTION_DIAGNOSIS));
    assert_eq!(ref_lock_diagnosis("Revision main does not exist"), None);
    let deferral = ref_lock_deferral(stale).unwrap();
    assert_eq!(
        reconcile_failure_kind(&deferral),
        REF_LOCK_STALE,
        "a deferral classifies as the lock that caused it"
    );
}

/// Advance the shared store's `main` bookmark by one commit WITHOUT touching the
/// project checkout's git ref, mirroring a fetch/import that lands in the store
/// before the checkout pulls. Returns the new store `main` tip. The advance is an
/// empty commit: its commit-id differs from the pre-advance tip (so a re-attach's
/// fast-forward is observable) while its tree is unchanged (so a `reset --hard`
/// never needs real content in the store working copy).
#[cfg(test)]
fn advance_store_main(jj: &JjEnv, store: &Path) -> String {
    jj.run(
        store,
        &["new", "main", "-m", "store advance"],
        "test: new child of main",
    )
    .unwrap();
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            "@",
            "--ignore-working-copy",
        ],
        "test: move store main bookmark forward",
    )
    .unwrap();
    bookmark_commit(jj, store, "main").unwrap()
}

/// The core repair: an export that moves the checkout's attached branch (`main`)
/// detaches HEAD, and the wrapper must re-attach it and fast-forward the clean
/// tree to the exported tip.
#[test]
#[serial_test::serial(jj)]
fn export_reattaches_detached_main_checkout() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping export_reattaches_detached_main_checkout: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Precondition: the checkout is attached to main.
    assert_eq!(
        git_stdout(proj.path(), &["symbolic-ref", "HEAD"]),
        "refs/heads/main"
    );
    let main_before = git_stdout(proj.path(), &["rev-parse", "main"]);

    let store_main = advance_store_main(&jj, &store);
    assert_ne!(
        store_main, main_before,
        "the store's main must have advanced"
    );

    // The export moves refs/heads/main and would leave HEAD detached; the wrapper
    // must repair it synchronously.
    export_git_preserving_checkout(&jj, &store, true, "test export").unwrap();

    assert_eq!(
        git_stdout(proj.path(), &["symbolic-ref", "HEAD"]),
        "refs/heads/main",
        "HEAD must be re-attached to main after the export detached it"
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", "HEAD"]),
        store_main,
        "the checkout must be fast-forwarded to the exported main tip"
    );
}

/// An export that moves a branch OTHER than the one HEAD is attached to must
/// leave the checkout untouched — jj only detaches HEAD when it moves the branch
/// HEAD is a symref to.
#[test]
#[serial_test::serial(jj)]
fn export_moving_a_non_checkout_branch_leaves_head_attached() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping export_moving_a_non_checkout_branch_leaves_head_attached: jj missing");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let main_before = git_stdout(proj.path(), &["rev-parse", "main"]);

    // Advance an agent branch (not main) in the store, then export.
    jj.run(
        &store,
        &["new", "main", "-m", "agent work"],
        "test: agent commit",
    )
    .unwrap();
    jj.run(
        &store,
        &[
            "bookmark",
            "create",
            "agent/foo",
            "-r",
            "@",
            "--ignore-working-copy",
        ],
        "test: create agent bookmark",
    )
    .unwrap();

    export_git_preserving_checkout(&jj, &store, true, "test export").unwrap();

    assert_eq!(
        git_stdout(proj.path(), &["symbolic-ref", "HEAD"]),
        "refs/heads/main",
        "HEAD must stay attached to main when the export did not move main"
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", "main"]),
        main_before,
        "main must be unchanged"
    );
}

/// A checkout the user deliberately detached must be left detached: the wrapper
/// only repairs a detach the export itself caused, never a pre-existing one.
#[test]
#[serial_test::serial(jj)]
fn export_leaves_preexisting_detached_head_alone() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping export_leaves_preexisting_detached_head_alone: jj missing");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // The user detaches HEAD themselves before any export.
    git(proj.path(), &["checkout", "--detach", "HEAD"]);
    assert!(
        !cairn_platform::env::git()
            .args(["symbolic-ref", "-q", "HEAD"])
            .current_dir(proj.path())
            .status()
            .unwrap()
            .success(),
        "precondition: HEAD is detached"
    );

    advance_store_main(&jj, &store);
    export_git_preserving_checkout(&jj, &store, true, "test export").unwrap();

    assert!(
        !cairn_platform::env::git()
            .args(["symbolic-ref", "-q", "HEAD"])
            .current_dir(proj.path())
            .status()
            .unwrap()
            .success(),
        "a user-deliberate detached HEAD must be left detached"
    );
}

/// When the export detaches HEAD but the working tree has real uncommitted
/// changes, the repair must NOT `reset --hard` (that would destroy the edits):
/// HEAD is left detached and the user's work is preserved.
#[test]
#[serial_test::serial(jj)]
fn export_with_dirty_tree_leaves_head_detached_and_preserves_edits() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping export_with_dirty_tree_leaves_head_detached_and_preserves_edits: jj missing"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Uncommitted edit to a tracked file (not the regenerable-lockfile allowlist).
    std::fs::write(proj.path().join("shared.rs"), "UNCOMMITTED USER WORK\n").unwrap();

    advance_store_main(&jj, &store);
    export_git_preserving_checkout(&jj, &store, true, "test export").unwrap();

    assert!(
        !cairn_platform::env::git()
            .args(["symbolic-ref", "-q", "HEAD"])
            .current_dir(proj.path())
            .status()
            .unwrap()
            .success(),
        "HEAD must stay detached rather than reset away a dirty tree"
    );
    assert_eq!(
        std::fs::read_to_string(proj.path().join("shared.rs")).unwrap(),
        "UNCOMMITTED USER WORK\n",
        "the uncommitted user edit must be preserved"
    );
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
    add_workspace(&jj, &store, &ws, "agent/cairn-1-builder-0", "main", None).unwrap();

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
    add_workspace(&jj, &store, &ws, "agent/cairn-2019-builder-0", "main", None).unwrap();

    // Edit a file under a parens route-group directory, then path-scope seal it.
    let rel = "apps/quarry/src/app/(app)/drawings/page.tsx";
    let abs = ws.join(rel);
    std::fs::create_dir_all(abs.parent().unwrap()).unwrap();
    std::fs::write(&abs, "export default function Page() {}\n").unwrap();

    let res = seal_paths(
        &jj,
        &ws,
        "add drawings page",
        None,
        &[rel],
        Some("agent/cairn-2019-builder-0"),
    )
    .unwrap();
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
/// A `git rev-parse` test closure over `repo` mirroring the production
/// `GitService::rev_parse` contract: `Some(trimmed_sha)` for a ref git
/// resolves, `None` otherwise (non-zero exit — unborn or unmatched ref).
fn rev_parse_closure(repo: &Path) -> impl Fn(&str) -> Option<String> + '_ {
    move |r: &str| {
        let out = cairn_platform::env::git()
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
    add_workspace(&jj, &store, &ws, "agent/cairn-9-builder-0", &got, None).unwrap();
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
    git(proj.path(), &["config", "user.name", "p"]);
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
    add_workspace(&jj, &store, &ws, "agent/cairn-1-builder-0", "main", None).unwrap();

    update_stale(&jj, &ws).expect("update-stale on a fresh workspace is a clean no-op");
    assert_eq!(is_working_copy_dirty(&jj, &ws), Ok(false));
    // Idempotent: running it again is still a no-op.
    update_stale(&jj, &ws).expect("update-stale is idempotent on a fresh workspace");
}

/// An unmanaged, unbookmarked checkout can still rewrite in place, and reports
/// the exact commit it rewrote.
#[test]
#[serial_test::serial(jj)]
fn seal_amend_squashes_in_unmanaged_unbookmarked_checkout() {
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

    let branch = "agent/cairn-1-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("a.rs"), "one\n").unwrap();
    seal(&jj, &ws, "orig", None).unwrap();
    let commit_before = bookmark_commit(&jj, &ws, branch).unwrap();
    jj.run(
        &ws,
        &["bookmark", "delete", branch, "--ignore-working-copy"],
        "remove managed bookmark for ambient amend fixture",
    )
    .unwrap();
    let change_before = jj
        .run(
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
            "change before amend",
        )
        .unwrap();

    std::fs::write(ws.join("b.rs"), "two\n").unwrap();
    let result = seal_paths(&jj, &ws, "^", None, &[], None).unwrap();
    assert!(
        result
            .amend_note
            .as_deref()
            .is_some_and(|note| note.contains("rewrote commit") && note.contains(&commit_before)),
        "the in-place rewrite names its target: {:?}",
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
        match cairn_platform::env::find_binary("jj") {
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
    cairn_platform::env::ensure_jj_shim_in(&bin_dir, &abs_jj);
    let shim = bin_dir.join("jj");
    assert!(shim.exists(), "the shim script is generated");

    // `workspace update-stale` is intercepted: exit 0, no output at all, and the
    // real jj (pointed at a bogus path) is NEVER invoked.
    let intercepted = std::process::Command::new(&shim)
        .args(["workspace", "update-stale"])
        .env("CAIRN_JJ_BIN", "/definitely/not/a/real/jj")
        .output()
        .unwrap();
    assert!(
        intercepted.status.success(),
        "the intercepted update-stale exits 0"
    );
    // Narrated, never silent. `CAIRN_JJ_BIN` points at a path that cannot be
    // executed, so a clean exit is itself proof the real jj was never reached;
    // what this asserts is that the caller is TOLD. A silent clean exit is
    // indistinguishable from a successful repair, and that is what made a stale
    // store cost an evening to diagnose — the operator's shell resolves this
    // shim too, so `jj workspace update-stale` reported success and did nothing.
    let narration = String::from_utf8_lossy(&intercepted.stderr);
    assert!(
        narration.contains("changed nothing"),
        "the interception must say it changed nothing: {narration:?}"
    );
    assert!(
        narration.contains("/definitely/not/a/real/jj"),
        "and must name the real binary as the escape hatch: {narration:?}"
    );
    assert!(
        intercepted.stdout.is_empty(),
        "the narration goes to stderr, so stdout stays clean for callers that parse it: {:?}",
        String::from_utf8_lossy(&intercepted.stdout)
    );

    // Every other command execs the real jj: `--version` matches byte-for-byte.
    let via_shim = std::process::Command::new(&shim)
        .arg("--version")
        .env("CAIRN_JJ_BIN", &bin)
        .output()
        .unwrap();
    let direct = cairn_platform::env::command(&bin)
        .arg("--version")
        .output()
        .unwrap();
    assert!(via_shim.status.success(), "pass-through --version succeeds");
    assert_eq!(
        via_shim.stdout, direct.stdout,
        "the shim execs the real jj untouched for non-intercepted commands"
    );
}
#[test]
#[serial_test::serial(jj)]
fn add_workspace_never_cleans_an_existing_registration_or_directory() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping add_workspace_never_cleans_an_existing_registration_or_directory");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/prior-lineage";
    let ws = wts.path().join("occupied");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("sentinel"), "preserve me").unwrap();
    let tip = bookmark_commit(&jj, &store, branch).unwrap();

    let error = add_workspace(&jj, &store, &ws, branch, "main", None).unwrap_err();
    assert!(error.contains("already exists") || error.contains("Destination path exists"));
    assert_eq!(
        std::fs::read_to_string(ws.join("sentinel")).unwrap(),
        "preserve me"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(tip.as_str())
    );
}

// ---------------------------------------------------------------------------
// VCS reconciliation spine: a self-healing default bookmark, and an export that
// is proven to have reached git.
//
// Every fixture here drives real jj against a real bare origin, because the
// behaviors under test are jj's own and none of them is inferable from prose:
// that both sides moving conflicts a tracked bookmark NAME, that a conflicted
// name resolves to several commits, that `jj git export` reports a refused ref
// only on stderr while exiting 0, and that re-running the export does not clear
// it.
// ---------------------------------------------------------------------------

/// A project checkout wired to a bare `origin`, with a shared store whose `main`
/// bookmark TRACKS `main@origin`. This is the production topology for a default
/// branch, and the only one in which the two sides can move independently.
struct TrackedOrigin {
    _home: TempDir,
    _origin: TempDir,
    proj: TempDir,
    jj: JjEnv,
    store: PathBuf,
    base: String,
}

fn setup_tracked_origin(bin: &str) -> TrackedOrigin {
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
    let base = git_stdout(proj.path(), &["rev-parse", "HEAD"]);

    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    jj.run(
        &store,
        &["bookmark", "track", "main", "--remote=origin"],
        "track main",
    )
    .unwrap();

    TrackedOrigin {
        _home: home,
        _origin: origin,
        proj,
        jj,
        store,
        base,
    }
}

/// Advance `origin/main` onto a NEW commit parented on `parent` — the shape of a
/// GitHub squash-merge, which mints a commit id the store has never seen.
fn advance_origin_from(fx: &TrackedOrigin, parent: &str, file: &str, contents: &str) -> String {
    git(fx.proj.path(), &["checkout", "-q", "--detach", parent]);
    std::fs::write(fx.proj.path().join(file), contents).unwrap();
    git(fx.proj.path(), &["add", "-A"]);
    git(fx.proj.path(), &["commit", "-q", "-m", "squash merge (#1)"]);
    let sha = git_stdout(fx.proj.path(), &["rev-parse", "HEAD"]);
    git(fx.proj.path(), &["push", "-q", "-f", "origin", "HEAD:main"]);
    git(fx.proj.path(), &["fetch", "-q", "origin"]);
    sha
}

/// Advance the store's LOCAL `main` onto a fresh commit and export it, the way a
/// Cairn-driven fold does.
fn fold_local_main(fx: &TrackedOrigin, message: &str) -> String {
    fx.jj
        .run(
            &fx.store,
            &["new", "main", "-m", message, "--ignore-working-copy"],
            "test fold",
        )
        .unwrap();
    let tip = fx
        .jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                "heads(main::)",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "test fold tip",
        )
        .unwrap();
    set_bookmark_at(&fx.jj, &fx.store, "main", &tip).unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "test fold export").unwrap();
    tip
}

/// A fresh commit in the store parented on `parent`, without touching any
/// workspace. The commit id is read back from `@`, which `jj new` just moved.
fn store_child_of(fx: &TrackedOrigin, parent: &str, message: &str) -> String {
    fx.jj
        .run(
            &fx.store,
            &["new", parent, "-m", message, "--ignore-working-copy"],
            "test child commit",
        )
        .unwrap();
    fx.jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "test child commit id",
        )
        .unwrap()
}

fn proj_ref(fx: &TrackedOrigin, branch: &str) -> String {
    git_stdout(
        fx.proj.path(),
        &["rev-parse", &format!("refs/heads/{branch}")],
    )
}

/// The plain case, and the one the old code reached least often: origin advanced,
/// nothing else did, and NO sibling is in flight. Reconciliation is owed here
/// regardless, because the store must be able to answer for `main` whether or
/// not anything downstream needs rebasing.
#[test]
#[serial_test::serial(jj)]
fn reconcile_converges_when_only_origin_advanced() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_converges_when_only_origin_advanced: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let advanced = advance_origin_from(&fx, &fx.base, "gh.txt", "advance\n");

    reconcile_tracked_bookmark(&fx.jj, &fx.store, "main").unwrap();

    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(advanced.as_str()),
        "the store's `main` must equal origin after a reconcile"
    );
    assert_eq!(proj_ref(&fx, "main"), advanced);

    // Idempotent: a second pass reports no change and moves nothing.
    let again = reconcile_tracked_bookmark(&fx.jj, &fx.store, "main").unwrap();
    assert_eq!(
        again,
        BookmarkReconciliation::Unchanged {
            commit: advanced.clone()
        }
    );
}

/// A local bookmark that strictly DESCENDS from origin holds work origin has not
/// seen. Reconciliation reports it and moves nothing: the backward repair is only
/// ever correct where origin is the sole authority, and "ahead" is a signal, not
/// a state to overwrite.
#[test]
#[serial_test::serial(jj)]
fn reconcile_never_moves_a_bookmark_that_is_ahead_of_origin() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_never_moves_a_bookmark_ahead_of_origin: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let folded = fold_local_main(&fx, "unpushed local work");

    let outcome = reconcile_tracked_bookmark(&fx.jj, &fx.store, "main").unwrap();

    assert_eq!(
        outcome,
        BookmarkReconciliation::AheadOfOrigin {
            local: folded.clone(),
            remote: fx.base.clone(),
        }
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(folded.as_str()),
        "an ahead bookmark must not be reset onto origin"
    );
}

/// A project with no `origin` has no authority to reconcile against, so this is a
/// clean no-op rather than an error. Local-only projects must not be made to fail.
#[test]
#[serial_test::serial(jj)]
fn reconcile_is_a_clean_noop_without_a_remote() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_is_a_clean_noop_without_a_remote: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    assert_eq!(
        reconcile_tracked_bookmark(&jj, &store, "main").unwrap(),
        BookmarkReconciliation::NoRemote
    );
}

/// A conflicted bookmark name resolves to SEVERAL commits. The single-commit
/// resolver templated `commit_id` with no separator, so it used to return their
/// 80-character concatenation — a string that is not a commit id, handed to
/// callers as a revision. It must resolve to nothing instead, leaving the
/// conflicted state to the probe and repair that can actually name it.
#[test]
#[serial_test::serial(jj)]
fn a_conflicted_bookmark_resolves_to_no_commit_rather_than_a_concatenation() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping conflicted_bookmark_resolves_to_no_commit: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    fold_local_main(&fx, "cairn fold: land child");
    advance_origin_from(&fx, &fx.base, "gh.txt", "squash\n");
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap());

    let resolved = bookmark_commit(&fx.jj, &fx.store, "main");

    assert_eq!(
        resolved, None,
        "a conflicted bookmark must not resolve to a concatenation of its targets"
    );
}

// ---------------------------------------------------------------------------
// A stale observation of origin must never walk a store-owned branch back.
//
// The live specimen (cairn/5019): a whole-remote `git fetch origin` received an
// advertisement about 80 s behind a push the store had already made, rewrote the
// agent branch's tracking ref backwards, and the next import moved the bookmark
// back and hid the branch's newest commit. A base-advance replay then rewrote
// what the bookmark held, and when the next fetch brought the true tip back the
// name was left conflicted between the rewrite and the hidden child.
// ---------------------------------------------------------------------------

/// A commit made with plain git in the project checkout, parented on `parent`.
/// It moves no branch; the checkout is left detached on it.
fn git_commit_on(
    fx: &TrackedOrigin,
    parent: &str,
    file: &str,
    contents: &str,
    message: &str,
) -> String {
    git(fx.proj.path(), &["checkout", "-q", "--detach", parent]);
    std::fs::write(fx.proj.path().join(file), contents).unwrap();
    git(fx.proj.path(), &["add", "-A"]);
    git(fx.proj.path(), &["commit", "-q", "-m", message]);
    git_stdout(fx.proj.path(), &["rev-parse", "HEAD"])
}

/// Make git-side commits visible to the store without leaving a bookmark on
/// them.
fn import_commits(fx: &TrackedOrigin, commits: &[&str]) {
    for (n, commit) in commits.iter().enumerate() {
        git(
            fx.proj.path(),
            &["update-ref", &format!("refs/heads/fixture-{n}"), commit],
        );
    }
    import_git(&fx.jj, &fx.store).unwrap();
    for n in 0..commits.len() {
        fx.jj
            .run(
                &fx.store,
                &[
                    "bookmark",
                    "delete",
                    &format!("fixture-{n}"),
                    "--ignore-working-copy",
                ],
                "jj bookmark delete",
            )
            .unwrap();
    }
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "drop fixture bookmarks").unwrap();
}

fn tracking_ref(fx: &TrackedOrigin, branch: &str) -> String {
    git_stdout(
        fx.proj.path(),
        &["rev-parse", &format!("refs/remotes/origin/{branch}")],
    )
}

/// Move the store's default-workspace `@` onto an empty commit off the root, so
/// it is nobody's descendant. `store_child_of` leaves it on whatever it made.
fn park_working_copy(fx: &TrackedOrigin) {
    fx.jj
        .run(
            &fx.store,
            &["new", "root()", "--ignore-working-copy"],
            "park the store working copy",
        )
        .unwrap();
}

fn scoped_import_settings(jj: &JjEnv, store: &Path) -> String {
    jj.run(
        store,
        &[
            "config",
            "list",
            "--include-defaults",
            "--ignore-working-copy",
        ],
        "read import settings",
    )
    .unwrap()
}

#[test]
#[serial_test::serial(jj)]
fn timed_out_scoped_import_worker_cannot_publish_after_the_caller_returns() {
    let bin = jj_bin().expect("real jj required");
    let fx = setup_tracked_origin(&bin);
    let scoped = fx.jj.preserving_import_bookmarks(vec!["main".into()]);
    git(
        fx.proj.path(),
        &["update-ref", "refs/heads/arrived", &fx.base],
    );
    let source = operation_id(&fx.jj, &fx.store).unwrap();
    let config = scoped_import_settings(&fx.jj, &fx.store);
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let (finished, done) = std::sync::mpsc::channel();
    super::env::during_next_native_import(
        move || {
            entered.send(()).unwrap();
            resume.recv().unwrap();
        },
        move || {
            finished.send(()).unwrap();
        },
    );
    let error = super::env::publish_scoped_import_candidate(
        &scoped,
        &fx.store,
        config.clone(),
        source.clone(),
        std::time::Instant::now() + Duration::from_millis(200),
    )
    .unwrap_err();
    assert!(is_jj_timeout_error(&error), "{error}");
    entry.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_eq!(operation_id(&fx.jj, &fx.store).unwrap(), source);
    let refused = super::env::publish_scoped_import_candidate(
        &scoped,
        &fx.store,
        config,
        source.clone(),
        std::time::Instant::now() + Duration::from_secs(10),
    )
    .unwrap_err();
    assert!(refused.contains("still preparing"), "{refused}");
    release.send(()).unwrap();
    done.recv_timeout(Duration::from_secs(60)).unwrap();
    assert_eq!(operation_id(&fx.jj, &fx.store).unwrap(), source);
    assert!(bookmark_commit(&fx.jj, &fx.store, "arrived").is_none());
    import_git(&scoped, &fx.store).unwrap();
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "arrived").as_deref(),
        Some(fx.base.as_str())
    );
}

#[test]
#[serial_test::serial(jj)]
fn scoped_import_candidate_refuses_a_changed_source_operation() {
    let bin = jj_bin().expect("real jj required");
    let fx = setup_tracked_origin(&bin);
    let scoped = fx.jj.preserving_import_bookmarks(vec!["main".into()]);
    git(
        fx.proj.path(),
        &["update-ref", "refs/heads/arrived", &fx.base],
    );
    let source = operation_id(&fx.jj, &fx.store).unwrap();
    let config = scoped_import_settings(&fx.jj, &fx.store);
    let jj = fx.jj.clone();
    let store = fx.store.clone();
    let base = fx.base.clone();
    super::env::during_next_native_import(
        move || {
            create_bookmark_at(&jj, &store, "concurrent", &base).unwrap();
        },
        || {},
    );
    let error = super::env::publish_scoped_import_candidate(
        &scoped,
        &fx.store,
        config,
        source,
        std::time::Instant::now() + Duration::from_secs(60),
    )
    .unwrap_err();
    assert!(error.contains("source operation changed"), "{error}");
    assert!(bookmark_commit(&fx.jj, &fx.store, "arrived").is_none());
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "concurrent").as_deref(),
        Some(fx.base.as_str())
    );
}

#[test]
#[serial_test::serial(jj)]
fn scoped_import_observes_detached_head_without_moving_fenced_bookmarks() {
    let bin = jj_bin().expect("real jj required");
    let fx = setup_tracked_origin(&bin);
    let tip = git_commit_on(
        &fx,
        &fx.base,
        "head-only.txt",
        "head only\n",
        "detached HEAD only",
    );
    let scoped = fx.jj.preserving_import_bookmarks(vec!["main".into()]);
    import_git(&scoped, &fx.store).unwrap();
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &tip).as_deref(),
        Some(tip.as_str())
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(fx.base.as_str())
    );
}

/// The guard has exactly one named exemption: the default branch's reconcile,
/// where origin is the sole authority. Origin rewinding `main` still wins there.
#[test]
#[serial_test::serial(jj)]
fn origin_still_rewinds_the_default_branch_through_its_reconcile() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping origin_still_rewinds_the_default_branch: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let advanced = advance_origin_from(&fx, &fx.base, "gh.txt", "advance\n");
    reconcile_tracked_bookmark(&fx.jj, &fx.store, "main").unwrap();
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(advanced.as_str())
    );

    // Someone force-pushes main back on origin, and Cairn fetches it.
    git(
        fx.proj.path(),
        &["push", "-q", "-f", "origin", &format!("{}:main", fx.base)],
    );
    git(fx.proj.path(), &["fetch", "-q", "origin", "main"]);
    assert_eq!(tracking_ref(&fx, "main"), fx.base);
    // An unrelated ordinary import (job provisioning, say) gets there first.
    // The authority the reconcile declared outlives that reconcile, so this
    // import observes the rewind instead of holding it off.
    import_git(&fx.jj, &fx.store).unwrap();
    assert_eq!(tracking_ref(&fx, "main"), fx.base);

    let outcome = reconcile_tracked_bookmark(&fx.jj, &fx.store, "main").unwrap();

    // The authority import itself carries the rewind onto the tracked bookmark,
    // so the reconcile may find nothing left to repair. Either way origin won.
    assert!(
        matches!(
            &outcome,
            BookmarkReconciliation::Repaired { to, .. }
                | BookmarkReconciliation::Unchanged { commit: to } if to == &fx.base
        ),
        "{outcome:?}"
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(fx.base.as_str())
    );
    assert_eq!(proj_ref(&fx, "main"), fx.base);
    assert_eq!(tracking_ref(&fx, "main"), fx.base);
}

/// Leave `branch` conflicted in the rewritten-base shape without origin: `r` is
/// rewritten onto `onto` while `c`, built on `r` and hidden for the rewrite,
/// comes back through the branch's git ref. Returns the rewrite.
fn conflict_rewritten_base(
    fx: &TrackedOrigin,
    branch: &str,
    r: &str,
    c: &str,
    onto: &str,
) -> String {
    create_bookmark_at(&fx.jj, &fx.store, branch, r).unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "export r").unwrap();
    park_working_copy(fx);
    fx.jj
        .run(
            &fx.store,
            &["abandon", &format!("{r}..{c}"), "--ignore-working-copy"],
            "hide the child for the rewrite",
        )
        .unwrap();
    fx.jj
        .run(
            &fx.store,
            &["rebase", "-r", r, "-d", onto, "--ignore-working-copy"],
            "rewrite r",
        )
        .unwrap();
    let rewrite = bookmark_commit(&fx.jj, &fx.store, branch).unwrap();
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), c],
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    rewrite
}

fn assert_refused_without_writing(fx: &TrackedOrigin, branch: &str, reason: &str) {
    let before = operation_id(&fx.jj, &fx.store).unwrap();
    let error = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .expect_err("the shape must be refused");
    let refusal = error
        .refusal()
        .unwrap_or_else(|| panic!("not a refusal: {error}"));
    assert!(refusal.reason.contains(reason), "{}", refusal.reason);
    assert!(!refusal.commands.is_empty());
    assert_eq!(operation_id(&fx.jj, &fx.store).unwrap(), before);
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
}

/// Something else built on the child would be re-parented by the replay, so
/// the shape is refused rather than moving work this branch does not own.
#[test]
#[serial_test::serial(jj)]
fn a_rewritten_base_whose_child_has_other_work_on_it_is_refused() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping child_has_other_work_on_it_is_refused: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-8-builder-0";
    let r = git_commit_on(&fx, &fx.base, "r.txt", "r\n", "R");
    let c = git_commit_on(&fx, &r, "c.txt", "c\n", "C");
    let onto = git_commit_on(&fx, &fx.base, "onto.txt", "o\n", "onto");
    import_commits(&fx, &[&r, &c, &onto]);
    conflict_rewritten_base(&fx, branch, &r, &c, &onto);
    store_child_of(&fx, &c, "a delegated child's work on c");

    assert_refused_without_writing(&fx, branch, "other work is built on");
}

/// Nothing to repair is not an error, and it costs no store write.
#[test]
#[serial_test::serial(jj)]
fn repairing_an_unconflicted_branch_is_a_clean_noop() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping repairing_an_unconflicted_branch: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    assert_eq!(
        repair_conflicted_bookmark_name(&fx.jj, &fx.store, "main").unwrap(),
        None
    );
    assert_eq!(
        repair_conflicted_bookmark_name(&fx.jj, &fx.store, "never-existed").unwrap(),
        None
    );
}

/// The default branch takes the origin-authoritative route through the same
/// entry point, so one call site serves both kinds of branch.
#[test]
#[serial_test::serial(jj)]
fn the_default_branch_repairs_against_origin_through_the_shared_entry_point() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping default_branch_repairs_against_origin: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    fold_local_main(&fx, "cairn fold: land child");
    let squashed = advance_origin_from(&fx, &fx.base, "gh.txt", "squash\n");
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap());

    let settled =
        repair_conflicted_branch_name(&fx.jj, &fx.store, "main", BranchAuthority::Origin).unwrap();

    assert_eq!(settled.as_deref(), Some(squashed.as_str()));
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap());
}

/// A conflicted bookmark is a reconciliation TODO, not a terminal state. Recording
/// it as permanent meant it was never retried and never repaired.
#[test]
fn a_conflicted_bookmark_is_a_retryable_reconcile_failure() {
    assert_eq!(
        reconcile_failure_kind("Error: Name `main` is conflicted"),
        "conflicted_bookmark"
    );
    assert!(
        !reconcile_failure_is_permanent("conflicted_bookmark"),
        "a conflicted bookmark must be retried so the next reconcile can repair it"
    );
    // The genuinely unrepairable families are unchanged.
    assert!(reconcile_failure_is_permanent("immutable_commit"));
    assert!(reconcile_failure_is_permanent("ambiguous_divergence"));
    assert!(reconcile_failure_is_permanent("missing_bookmark"));
}

/// THE EXPORT FREEZE. A `refs/heads/*` ref moved outside jj — which Cairn itself
/// does, via HEAD re-attachment, executor resets, and worktree operations — makes
/// every later `jj git export` refuse that one ref, report it on stderr, and EXIT
/// 0. The bookmark advances; the git ref does not; the next push carries a tree
/// nobody produced.
///
/// The unverified export cannot tell, which is the point: the same call with an
/// expectation detects the freeze, repairs it, and leaves the ref correct.
#[test]
#[serial_test::serial(jj)]
fn a_verified_export_detects_and_repairs_a_frozen_git_ref() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping verified_export_detects_and_repairs_a_frozen_ref: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    // Detach so the export's HEAD repair never enters the picture.
    git(proj.path(), &["checkout", "-q", "--detach"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/cairn-2765-builder-0";
    jj.run(
        &store,
        &["new", "main", "-m", "agent work", "--ignore-working-copy"],
        "seal",
    )
    .unwrap();
    let first = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                "heads(main::)",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "tip",
        )
        .unwrap();
    create_bookmark_at(&jj, &store, branch, &first).unwrap();
    export_git_preserving_checkout(&jj, &store, true, "initial export").unwrap();
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        first
    );

    // Something outside jj moves the ref, and the bookmark then advances.
    let main_commit = git_stdout(proj.path(), &["rev-parse", "refs/heads/main"]);
    git(
        proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), &main_commit],
    );
    jj.run(
        &store,
        &[
            "new",
            branch,
            "-m",
            "more agent work",
            "--ignore-working-copy",
        ],
        "seal again",
    )
    .unwrap();
    let second = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("heads({branch}::)"),
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "tip",
        )
        .unwrap();
    set_bookmark_at(&jj, &store, branch, &second).unwrap();

    // The unverified export reports success and changes nothing. This is the
    // freeze, and it is exactly what shipped a PR whose tests ran on an unpushed
    // tree.
    export_git_preserving_checkout(&jj, &store, true, "frozen export")
        .expect("jj reports success even though it refused the ref");
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        main_commit,
        "the unverified export leaves the git ref frozen behind the bookmark"
    );

    // The same export, told what it is publishing, detects and repairs it.
    export_git_verified(
        &jj,
        &store,
        true,
        "verified export",
        &[(branch, second.as_str())],
    )
    .expect("the verifier must repair a frozen ref rather than report it");
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        second,
        "after repair the git ref equals the bookmark it must publish"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(second.as_str())
    );
}

/// A freeze the repair cannot close returns the typed error. It is never `Ok`: a
/// push carrying a ref that disagrees with its bookmark publishes a tree nobody
/// tested, which is the failure this whole slice exists to make impossible.
///
/// The unwritable ref here is git's own directory/file rule — `refs/heads/feat`
/// cannot exist while `refs/heads/feat/inner` does — which is the very case jj's
/// export-failure hint calls out.
#[test]
#[serial_test::serial(jj)]
fn an_unrepairable_export_freeze_is_an_error_not_a_success() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping unrepairable_export_freeze_is_an_error: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    git(proj.path(), &["checkout", "-q", "--detach"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Block `refs/heads/feat` by occupying it as a directory.
    let main_commit = git_stdout(proj.path(), &["rev-parse", "refs/heads/main"]);
    git(
        proj.path(),
        &["update-ref", "refs/heads/feat/inner", &main_commit],
    );

    jj.run(
        &store,
        &["new", "main", "-m", "agent work", "--ignore-working-copy"],
        "seal",
    )
    .unwrap();
    let tip = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                "heads(main::)",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "tip",
        )
        .unwrap();
    create_bookmark_at(&jj, &store, "feat", &tip).unwrap();

    let error = export_git_verified(
        &jj,
        &store,
        true,
        "blocked export",
        &[("feat", tip.as_str())],
    )
    .expect_err("an unrepairable freeze must not be reported as a successful export");

    assert!(
        is_export_freeze_error(&error),
        "expected the typed export-freeze family, got: {error}"
    );
    assert!(
        error.contains("feat"),
        "the error must name the branch: {error}"
    );
    assert!(
        error.contains(&tip),
        "the error must name the bookmark commit that did not reach git: {error}"
    );
}

/// "Which commit is this push publishing?" has no single answer while the
/// bookmark name is conflicted, so the push is refused rather than allowed to
/// publish an arbitrary side.
#[test]
#[serial_test::serial(jj)]
fn publication_refuses_a_conflicted_bookmark() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping publication_refuses_a_conflicted_bookmark: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    fold_local_main(&fx, "cairn fold: land child");
    advance_origin_from(&fx, &fx.base, "gh.txt", "squash\n");
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap());

    let error = verified_publish_target(&fx.jj, &fx.store, "main")
        .expect_err("a conflicted bookmark has no single commit to publish");

    assert!(error.contains("conflicted"), "{error}");
}

/// The push-side half: a push that reports success while origin gained nothing —
/// or gained something else — is the phantom-PR specimen, and it is caught by
/// asking origin rather than trusting the exit code.
#[test]
#[serial_test::serial(jj)]
fn origin_confirmation_catches_a_push_origin_did_not_record() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping origin_confirmation_catches_a_push_origin_did_not_record: jj not resolvable"
        );
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-3189-builder-0";

    // Nothing has been pushed yet: origin has no such branch.
    let error = confirm_origin_tip(&fx.jj, &fx.store, branch, &fx.base)
        .expect_err("origin has no such branch, which must not read as a successful publish");
    assert!(error.contains("no `"), "{error}");

    // Publish it, then confirm both the agreeing and the disagreeing verdicts.
    create_bookmark_at(&fx.jj, &fx.store, branch, &fx.base).unwrap();
    push_store_bookmark(&fx.jj, &fx.store, branch).unwrap();
    confirm_origin_tip(&fx.jj, &fx.store, branch, &fx.base)
        .expect("origin's tip matches what was published");

    let squashed = advance_origin_from(&fx, &fx.base, "gh.txt", "other\n");
    let error = confirm_origin_tip(&fx.jj, &fx.store, branch, &squashed)
        .expect_err("origin's tip disagrees with the claimed publication");
    assert!(error.contains(&squashed), "{error}");
}

/// The push-side verifier must not report "could not confirm" as "confirmed".
///
/// An unreachable origin means the publication is UNVERIFIED, and on a
/// publication path that has to be an error — treating it as success is exactly
/// the trust-the-exit-code reasoning that produced a create-pr artifact for a
/// pull request that never existed.
#[test]
#[serial_test::serial(jj)]
fn origin_confirmation_fails_closed_when_origin_is_unreachable() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping origin_confirmation_fails_closed_when_unreachable: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let unreachable = home.path().join("no-such-origin");
    git(
        proj.path(),
        &["remote", "add", "origin", &unreachable.to_string_lossy()],
    );
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let head = git_stdout(proj.path(), &["rev-parse", "HEAD"]);

    let error = confirm_origin_tip(&jj, &store, "main", &head).expect_err(
        "an unreachable origin leaves the publication unverified, which is not success",
    );

    assert!(
        error.contains("cannot confirm"),
        "the error must say the publication could not be confirmed: {error}"
    );
    assert!(error.contains("main"), "{error}");
}

/// The resolver keeps its three answers apart, which is what lets a caller that
/// just moved a bookmark tell "there is nothing to publish" from "I cannot tell
/// what to publish".
#[test]
#[serial_test::serial(jj)]
fn the_checked_resolver_separates_absent_from_unresolvable() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping checked_resolver_separates_absent_from_unresolvable: jj not resolvable"
        );
        return;
    };
    let fx = setup_tracked_origin(&bin);

    // Present: exactly one commit.
    assert_eq!(
        bookmark_commit_checked(&fx.jj, &fx.store, "main").unwrap(),
        Some(fx.base.clone())
    );
    // Absent: jj exits 0 with empty output, so this is a real answer, not a failure.
    assert_eq!(
        bookmark_commit_checked(&fx.jj, &fx.store, "agent/never-existed").unwrap(),
        None
    );
    // Unresolvable: a nonexistent remote-tracking name is a jj ERROR, and must not
    // be laundered into "absent".
    assert!(revset_commit_checked(&fx.jj, &fx.store, "nope@origin").is_err());

    // Conflicted: several targets for one name is an error, never a commit id and
    // never a concatenation of them.
    fold_local_main(&fx, "cairn fold: land child");
    advance_origin_from(&fx, &fx.base, "gh.txt", "squash\n");
    import_git(&fx.jj, &fx.store).unwrap();
    let error = bookmark_commit_checked(&fx.jj, &fx.store, "main")
        .expect_err("a conflicted name does not resolve to a single commit");
    assert!(error.contains("more than one commit"), "{error}");
}

/// An export that cannot prove what it advanced must FAIL, not fall back to an
/// unverified export. Every caller of this helper reaches it having just moved
/// that bookmark, so "it does not resolve" means the operation's own
/// postcondition is unprovable — and a load-bearing fold reporting success there
/// is the same fail-open the verifier exists to close.
#[test]
#[serial_test::serial(jj)]
fn a_bookmark_advance_export_fails_when_the_bookmark_does_not_resolve() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping bookmark_advance_export_fails_when_unresolvable: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);

    // Absent bookmark: nothing was advanced, so nothing can be proven.
    let error = export_bookmark_advance(
        &fx.jj,
        &fx.store,
        true,
        "agent/never-existed",
        "test export",
    )
    .expect_err("an absent bookmark cannot be verified, so the export must not report success");
    assert!(is_export_freeze_error(&error), "{error}");
    assert!(error.contains("does not exist"), "{error}");

    // Conflicted name: the commit to publish is ambiguous.
    fold_local_main(&fx, "cairn fold: land child");
    advance_origin_from(&fx, &fx.base, "gh.txt", "squash\n");
    import_git(&fx.jj, &fx.store).unwrap();
    let error = export_bookmark_advance(&fx.jj, &fx.store, true, "main", "test export")
        .expect_err("a conflicted bookmark cannot be verified");
    assert!(is_export_freeze_error(&error), "{error}");
    assert!(error.contains("more than one commit"), "{error}");
}

// ── Conflict scaffolding never reaches trees or history (CAIRN-3197) ─────────

/// A base-advance replay is judged by the TREE it would publish, not only by
/// jj's conflict flag.
///
/// The incident shape: jj reports the rebased tip as conflict-free, yet the
/// commit's tree root carries `JJ-CONFLICT-README` and `.jjconflict-*`
/// sidecars. Exported, that ref can never be materialized on any machine, and
/// the branch was being reported as cleanly rebased. Here the advanced base
/// carries the scaffolding as ordinary files, which gives exactly that pair of
/// facts — a commit jj calls clean whose root listing is scaffolding — without
/// depending on whatever jj state produces it in the wild.
#[test]
#[serial_test::serial(jj)]
fn a_replay_whose_rebased_tree_carries_conflict_scaffolding_is_rolled_back_as_a_conflict() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_replay_whose_rebased_tree_carries_conflict_scaffolding_is_rolled_back_as_a_conflict: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();

    git(proj.path(), &["init", "-q", "-b", "main"]);
    git(proj.path(), &["config", "user.email", "p@e.com"]);
    git(proj.path(), &["config", "user.name", "p"]);
    let body: String = (0..40).map(|n| format!("line {n}\n")).collect();
    std::fs::write(proj.path().join("shared.rs"), &body).unwrap();
    git(proj.path(), &["add", "-A"]);
    git(proj.path(), &["commit", "-q", "-m", "base"]);

    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/cairn-5171-builder-0";
    let ws = wts.path().join("agent");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(
        ws.join("shared.rs"),
        body.replacen("line 0\n", "line 0 BRANCH\n", 1),
    )
    .unwrap();
    std::fs::write(ws.join("own.rs"), "AGENT GENUINE WORK\n").unwrap();
    seal(&jj, &ws, "branch work", None).unwrap();
    let pre_tip = bookmark_commit(&jj, &store, branch).unwrap();
    let branch_ref = format!("refs/heads/{branch}");
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &branch_ref]),
        pre_tip
    );

    // The base advances in the store only: one edit at the far end of the file
    // the branch also touched, and the scaffolding at the tree root.
    jj.run(
        &store,
        &["new", "main", "-m", "base advance"],
        "new on main",
    )
    .unwrap();
    std::fs::write(
        store.join("shared.rs"),
        body.replacen("line 39\n", "line 39 MAIN\n", 1),
    )
    .unwrap();
    std::fs::write(store.join("JJ-CONFLICT-README"), "conflict\n").unwrap();
    std::fs::create_dir_all(store.join(".jjconflict-base-0")).unwrap();
    std::fs::write(store.join(".jjconflict-base-0").join("shared.rs"), "base\n").unwrap();
    jj.run(
        &store,
        &["bookmark", "set", "main", "-r", "@"],
        "advance main",
    )
    .unwrap();

    let report =
        reconcile_siblings_without_publication(&jj, &store, "main", &[branch.to_string()]).unwrap();

    assert!(
        !report.rebased_clean.contains(&branch.to_string()),
        "a replay whose tree carries conflict scaffolding is never reported clean: {report:?}"
    );
    assert_eq!(
        report.conflicted,
        vec![branch.to_string()],
        "it is a conflict, so the attention layer opens a session: {report:?}"
    );
    assert!(
        report.failed.is_empty(),
        "not a retried failure: {report:?}"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, branch).unwrap(),
        pre_tip,
        "the store bookmark is back on its last clean tip"
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &branch_ref]),
        pre_tip,
        "the git ref never moved onto the scaffolded tree"
    );
    let diagnostic = report
        .conflict_diagnostics
        .get(branch)
        .expect("the conflict carries a diagnostic");
    assert_eq!(
        diagnostic.conflicting_paths(),
        vec!["shared.rs".to_string()],
        "the file both sides changed is named, captured before the rollback: {diagnostic:?}"
    );
    assert_eq!(diagnostic.ours.as_deref(), Some(pre_tip.as_str()));
    assert_eq!(
        diagnostic.condition,
        ConflictCondition::ContentConflict,
        "never base drift: the next replay would meet the same tree"
    );
    let scaffolding = diagnostic
        .scaffolding
        .as_ref()
        .expect("the diagnostic says the TREE was refused, not a jj conflict");
    assert_eq!(
        scaffolding.entries,
        vec![
            ".jjconflict-base-0".to_string(),
            "JJ-CONFLICT-README".to_string()
        ]
    );
    assert_eq!(
        diagnostic.conflicted_tip.as_ref(),
        Some(&scaffolding.commit)
    );
    assert_ne!(scaffolding.commit, pre_tip);
}

/// The verified export is a tree-level postcondition: it refuses to move a ref
/// onto a commit whose tree root carries jj conflict scaffolding, before `jj git
/// export` runs, and names the commit and the entries. A clean commit still
/// exports, and a ref already sitting on its commit is not re-judged (a rollback
/// onto a published tip must stay restorable).
#[test]
#[serial_test::serial(jj)]
fn the_verified_export_refuses_a_bookmark_on_a_tree_carrying_conflict_scaffolding() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping the_verified_export_refuses_a_bookmark_on_a_tree_carrying_conflict_scaffolding: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/cairn-5171-export-0";
    let branch_ref = format!("refs/heads/{branch}");

    // A clean commit exports, so the guard is not refusing everything.
    jj.run(&store, &["new", "main", "-m", "clean"], "new clean")
        .unwrap();
    std::fs::write(store.join("clean.rs"), "clean\n").unwrap();
    jj.run(
        &store,
        &["bookmark", "create", branch, "-r", "@"],
        "bookmark clean",
    )
    .unwrap();
    let clean = bookmark_commit(&jj, &store, branch).unwrap();
    export_bookmark_advance(&jj, &store, true, branch, "clean export")
        .expect("a clean tree exports");
    assert_eq!(git_stdout(proj.path(), &["rev-parse", &branch_ref]), clean);

    // The bookmark advances onto a commit whose root carries scaffolding.
    jj.run(&store, &["new", "-m", "poisoned"], "new poisoned")
        .unwrap();
    std::fs::write(store.join("JJ-CONFLICT-README"), "conflict\n").unwrap();
    std::fs::create_dir_all(store.join(".jjconflict-side-0")).unwrap();
    std::fs::write(store.join(".jjconflict-side-0").join("clean.rs"), "side\n").unwrap();
    jj.run(
        &store,
        &["bookmark", "set", branch, "-r", "@"],
        "bookmark poisoned",
    )
    .unwrap();
    let poisoned = bookmark_commit(&jj, &store, branch).unwrap();
    assert_ne!(poisoned, clean);

    let refusal = export_bookmark_advance_guarded(&jj, &store, true, branch, "poisoned export")
        .expect_err("a tree carrying conflict scaffolding must never be exported");
    assert_eq!(
        refusal,
        ExportError::ConflictScaffolding {
            branch: branch.to_string(),
            commit: poisoned.clone(),
            entries: vec![
                ".jjconflict-side-0".to_string(),
                "JJ-CONFLICT-README".to_string()
            ],
        }
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &branch_ref]),
        clean,
        "the git ref did not move"
    );

    // The String form every other caller sees names the commit and the entries
    // and is recognizable as the tree finding it is.
    let error = export_git_verified(
        &jj,
        &store,
        true,
        "poisoned export",
        &[(branch, poisoned.as_str())],
    )
    .expect_err("the String form refuses too");
    assert!(is_conflict_scaffolding_export_refusal(&error), "{error}");
    assert!(error.contains(&poisoned), "{error}");
    assert!(error.contains("JJ-CONFLICT-README"), "{error}");
    assert!(error.contains(".jjconflict-side-0"), "{error}");
    assert_eq!(git_stdout(proj.path(), &["rev-parse", &branch_ref]), clean);

    // Putting the bookmark back proves the store and git agree again.
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &clean,
            "--allow-backwards",
            "--ignore-working-copy",
        ],
        "restore",
    )
    .unwrap();
    export_bookmark_advance(&jj, &store, true, branch, "restored export")
        .expect("the restored clean tip re-exports");
    assert_eq!(git_stdout(proj.path(), &["rev-parse", &branch_ref]), clean);
}

/// The publish boundary refuses a delta whose tree root carries jj conflict
/// scaffolding, so a poisoned tree that somehow got materialized can never be
/// folded into history. Plain git — no jj needed to build the shape.
#[test]
fn pin_validated_delta_refuses_a_tree_carrying_conflict_scaffolding() {
    let repo = TempDir::new().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    git(repo.path(), &["config", "user.email", "p@e.com"]);
    git(repo.path(), &["config", "user.name", "p"]);
    std::fs::write(repo.path().join("shared.rs"), "clean\n").unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-q", "-m", "base"]);
    let base = git_stdout(repo.path(), &["rev-parse", "HEAD"]);

    // Exactly what `jj git export` writes for a conflict-flagged commit.
    for sidecar in [
        ".jjconflict-base-0",
        ".jjconflict-side-0",
        ".jjconflict-side-1",
    ] {
        std::fs::create_dir_all(repo.path().join(sidecar)).unwrap();
        std::fs::write(repo.path().join(sidecar).join("shared.rs"), "side\n").unwrap();
    }
    std::fs::write(repo.path().join("JJ-CONFLICT-README"), "conflict\n").unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-q", "-m", "scaffolding"]);
    let poisoned = git_stdout(repo.path(), &["rev-parse", "HEAD"]);

    let error = pin_validated_delta(repo.path(), &base, &poisoned, None)
        .expect_err("a delta carrying conflict scaffolding must never be pinned");
    assert!(error.contains(".jjconflict-side-0"), "{error}");
    assert!(error.contains(&poisoned), "{error}");

    // A clean delta still pins, so the guard is not simply refusing everything.
    std::fs::remove_file(repo.path().join("JJ-CONFLICT-README")).unwrap();
    for sidecar in [
        ".jjconflict-base-0",
        ".jjconflict-side-0",
        ".jjconflict-side-1",
    ] {
        std::fs::remove_dir_all(repo.path().join(sidecar)).unwrap();
    }
    std::fs::write(repo.path().join("shared.rs"), "real work\n").unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-q", "-m", "real work"]);
    let clean = git_stdout(repo.path(), &["rev-parse", "HEAD"]);
    pin_validated_delta(repo.path(), &base, &clean, None).expect("a clean delta pins");
}

/// The straddle's shape in plain git: a base commit, an advance on top of it,
/// and a delta parented at the base with the checkout detached there. That is
/// exactly what the runner holds at publication time when the bookmark moved
/// while a batch was running.
fn straddle_fixture(
    repo: &Path,
    base_files: &[(&str, &str)],
    advance_files: &[(&str, &str)],
    delta_files: &[(&str, &str)],
) -> (String, String, String) {
    let write_all = |files: &[(&str, &str)]| {
        for (name, content) in files {
            std::fs::write(repo.join(name), content).unwrap();
        }
    };
    git(repo, &["init", "-q", "-b", "main"]);
    git(repo, &["config", "user.email", "p@e.com"]);
    git(repo, &["config", "user.name", "p"]);
    write_all(base_files);
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "base"]);
    let base = git_stdout(repo, &["rev-parse", "HEAD"]);

    write_all(advance_files);
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "a base advance"]);
    let head = git_stdout(repo, &["rev-parse", "HEAD"]);

    git(repo, &["checkout", "-q", "--detach", &base]);
    write_all(delta_files);
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-q", "-m", "the straddling batch"]);
    let delta = git_stdout(repo, &["rev-parse", "HEAD"]);
    (base, head, delta)
}

/// Every object in the repository, reachable or not — the only count that can
/// see a commit written without a ref.
fn object_count(repo: &Path) -> usize {
    git_stdout(
        repo,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objecttype)",
        ],
    )
    .lines()
    .count()
}

fn commit_count(repo: &Path) -> usize {
    git_stdout(
        repo,
        &[
            "cat-file",
            "--batch-all-objects",
            "--batch-check=%(objecttype)",
        ],
    )
    .lines()
    .filter(|kind| kind.trim() == "commit")
    .count()
}

fn blob_at(repo: &Path, commit: &str, path: &str) -> String {
    git_stdout(repo, &["show", &format!("{commit}:{path}")])
}

/// The ordinary straddle: the advance and the batch touched different files.
/// Integration parents the batch's work at the head the branch actually holds
/// and keeps both sides, which is what makes the publication that follows land
/// the advance AND the batch rather than choosing between them.
#[test]
fn an_integrated_delta_carries_both_the_advance_and_the_batch() {
    let repo = TempDir::new().unwrap();
    let (base, head, delta) = straddle_fixture(
        repo.path(),
        &[("advanced.rs", "before\n"), ("mine.rs", "base\n")],
        &[("advanced.rs", "after the advance\n")],
        &[("mine.rs", "the batch's work\n")],
    );

    let Integration::Commit(integrated) =
        integrate_delta_onto_head(repo.path(), &base, &delta, &head).unwrap()
    else {
        panic!("disjoint paths integrate without conflict");
    };

    assert_eq!(
        git_stdout(repo.path(), &["rev-parse", &format!("{integrated}^")]),
        head,
        "the integrated commit is parented at the head the branch holds, which is the \
         parent the logical-head transaction requires"
    );
    assert_eq!(
        blob_at(repo.path(), &integrated, "advanced.rs"),
        "after the advance",
        "the advance survives: a replay of the batch's own diff would have dropped it"
    );
    assert_eq!(
        blob_at(repo.path(), &integrated, "mine.rs"),
        "the batch's work"
    );

    // The patch the runner reports is taken against the head published onto.
    // Against the routed base it would attribute the advance's line to the
    // batch, which is the counter this correction exists to keep honest.
    let against_head = git_stdout(repo.path(), &["diff", "--no-ext-diff", &head, &integrated]);
    let against_base = git_stdout(repo.path(), &["diff", "--no-ext-diff", &base, &integrated]);
    let counted = |patch: &str| {
        parse_git_patch(patch)
            .iter()
            .fold((0, 0), |(add, del), change| {
                (add + change.additions, del + change.deletions)
            })
    };
    assert_eq!(counted(&against_head), (1, 1), "only the batch's own line");
    assert_eq!(
        counted(&against_base),
        (2, 2),
        "against the routed base the advance's line is counted as the batch's too"
    );
}

/// The case CAIRN-3214 pins from the seal side, seen from the publication side.
/// A mid-batch refresh moved the checkout onto the advance, the batch then
/// edited a file the advance had just rewritten, and the delta's content is the
/// advance plus the batch's addition. Both sides agree on the advance's hunk, so
/// the merge is clean and the batch's extra content survives — a false conflict
/// here would strand exactly the work 3214 went to the trouble of preserving.
#[test]
fn an_edit_made_after_the_advance_integrates_without_a_false_conflict() {
    let repo = TempDir::new().unwrap();
    let (base, head, delta) = straddle_fixture(
        repo.path(),
        &[("shared.rs", "one\ntwo\nthree\n")],
        &[("shared.rs", "one\nADVANCED\nthree\n")],
        &[("shared.rs", "one\nADVANCED\nthree\nthe batch's line\n")],
    );

    let Integration::Commit(integrated) =
        integrate_delta_onto_head(repo.path(), &base, &delta, &head).unwrap()
    else {
        panic!("an edit written on top of the advance is not a conflict");
    };
    assert_eq!(
        blob_at(repo.path(), &integrated, "shared.rs"),
        "one\nADVANCED\nthree\nthe batch's line"
    );
}

/// Genuinely divergent edits to one region are the one case that must not be
/// resolved silently. Taking either side would be a lost update, so integration
/// names the paths and writes no commit — leaving the batch's work where the
/// agent can still act on it.
#[test]
fn divergent_edits_to_one_region_conflict_and_write_no_commit() {
    let repo = TempDir::new().unwrap();
    let (base, head, delta) = straddle_fixture(
        repo.path(),
        &[("shared.rs", "one\ntwo\nthree\n")],
        &[("shared.rs", "one\nADVANCED\nthree\n")],
        &[("shared.rs", "one\nTHE BATCH\nthree\n")],
    );
    let refs_before = git_stdout(repo.path(), &["show-ref"]);
    let commits_before = commit_count(repo.path());

    let integration = integrate_delta_onto_head(repo.path(), &base, &delta, &head).unwrap();

    assert_eq!(
        integration,
        Integration::Conflicted {
            paths: vec!["shared.rs".to_string()]
        },
        "the conflicting path is named, once, so the agent knows where to look"
    );
    assert_eq!(git_stdout(repo.path(), &["show-ref"]), refs_before);
    assert_eq!(commit_count(repo.path()), commits_before);
}

/// The ordinary path costs nothing. When the bookmark still holds the commit the
/// batch was routed against there is no merge to perform and no commit to
/// construct, and the sealed delta publishes byte-for-byte as it was.
#[test]
fn an_unmoved_head_constructs_nothing() {
    let repo = TempDir::new().unwrap();
    let (base, _head, delta) = straddle_fixture(
        repo.path(),
        &[("shared.rs", "base\n")],
        &[("advanced.rs", "advance\n")],
        &[("shared.rs", "the batch's work\n")],
    );
    let objects_before = object_count(repo.path());

    assert_eq!(
        integrate_delta_onto_head(repo.path(), &base, &delta, &base).unwrap(),
        Integration::Unmoved
    );
    assert_eq!(object_count(repo.path()), objects_before);
}

/// A delta whose content the advance already delivered would publish as an empty
/// commit. Saying so is better than putting one on the branch.
#[test]
fn a_delta_the_advance_already_delivered_is_already_landed() {
    let repo = TempDir::new().unwrap();
    let (base, head, delta) = straddle_fixture(
        repo.path(),
        &[("shared.rs", "base\n")],
        &[("shared.rs", "the same content\n")],
        &[("shared.rs", "the same content\n")],
    );

    assert_eq!(
        integrate_delta_onto_head(repo.path(), &base, &delta, &head).unwrap(),
        Integration::AlreadyLanded
    );
}

/// The whole sequence, against real jj: a batch is routed at the bookmark's
/// tip, a sibling lands while it runs, and the batch's sealed delta now declares
/// a parent the bookmark no longer holds. The publication is refused, as it must
/// be — and integration turns that refusal into a landing, with the branch
/// carrying both the sibling's commit and the batch's work.
#[test]
#[serial_test::serial(jj)]
fn a_straddled_delta_integrates_onto_the_moved_bookmark_and_lands() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping a_straddled_delta_integrates_onto_the_moved_bookmark: jj not resolvable"
        );
        return;
    };
    let home = TempDir::new().unwrap();
    let repo = TempDir::new().unwrap();
    let jj = JjEnv::resolve(&bin, home.path());
    let path = repo.path();

    git(path, &["init", "-q", "-b", "main"]);
    git(path, &["config", "user.email", "p@e.com"]);
    git(path, &["config", "user.name", "p"]);
    std::fs::write(path.join("advanced.rs"), "before\n").unwrap();
    std::fs::write(path.join("mine.rs"), "base\n").unwrap();
    git(path, &["add", "-A"]);
    git(path, &["commit", "-q", "-m", "base"]);
    let base = git_stdout(path, &["rev-parse", "HEAD"]);

    // Both the sibling's commit and the batch's delta are built off to the side,
    // parented at the base and referenced by nothing — which is exactly the
    // shape a sealed delta arrives in.
    let side_commit = |files: &[(&str, &str)], message: &str| {
        git(path, &["checkout", "-q", "--detach", &base]);
        for (name, content) in files {
            std::fs::write(path.join(name), content).unwrap();
        }
        git(path, &["add", "-A"]);
        git(path, &["commit", "-q", "-m", message]);
        git_stdout(path, &["rev-parse", "HEAD"])
    };
    let sibling = side_commit(&[("advanced.rs", "after the advance\n")], "a sibling lands");
    let delta = side_commit(&[("mine.rs", "the batch's work\n")], "the straddling batch");
    git(path, &["checkout", "-q", "main"]);

    jj.run(
        path,
        &["git", "init", "--colocate", "."],
        "colocate the fixture",
    )
    .unwrap();
    jj.run(
        path,
        &["bookmark", "create", "feature", "-r", &base],
        "create the job bookmark",
    )
    .unwrap();

    // The sibling lands while the batch is still running.
    let advanced = crate::publish_logical_head(
        path,
        "feature",
        &base,
        &sibling,
        None,
        crate::PublicationMode::Child {
            description: "a sibling lands".into(),
        },
    )
    .unwrap();
    assert_ne!(advanced.head, base);

    // The batch's own delta cannot publish: it declares a parent the bookmark
    // has moved past. This is the failure the whole issue is about.
    let refused = crate::publish_logical_head(
        path,
        "feature",
        &base,
        &delta,
        None,
        crate::PublicationMode::Child {
            description: "the straddling batch".into(),
        },
    )
    .unwrap_err();
    assert!(refused.contains("changed from"), "{refused}");

    // The runner's answer: merge the batch's changes onto the head the bookmark
    // actually holds, then publish onto that head.
    let Integration::Commit(integrated) =
        integrate_delta_onto_head(path, &base, &delta, &advanced.head).unwrap()
    else {
        panic!("a straddle over disjoint paths integrates");
    };
    let _pin = pin_validated_delta(path, &advanced.head, &integrated, None).unwrap();
    let landed = crate::publish_logical_head(
        path,
        "feature",
        &advanced.head,
        &integrated,
        None,
        crate::PublicationMode::Child {
            description: "the straddling batch".into(),
        },
    )
    .unwrap();

    assert_eq!(
        jj.run(
            path,
            &[
                "log",
                "-r",
                "feature",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy"
            ],
            "read the landed bookmark",
        )
        .unwrap(),
        landed.head,
        "the bookmark advanced to the integrated commit"
    );
    assert_eq!(
        git_stdout(path, &["rev-parse", &format!("{}^", landed.head)]),
        advanced.head
    );
    assert_eq!(
        blob_at(path, &landed.head, "advanced.rs"),
        "after the advance",
        "the sibling's work is still on the branch"
    );
    assert_eq!(
        blob_at(path, &landed.head, "mine.rs"),
        "the batch's work",
        "and so is the batch's, which before this had no route back into the branch"
    );
}

// ---------------------------------------------------------------------------
// The store's default workspace: staleness there must reach nothing (CAIRN-3223)
// ---------------------------------------------------------------------------

/// Whether a flagless read of the store is refused for staleness — the ground
/// truth these tests assert preconditions against, deliberately NOT routed
/// through any Cairn helper, since the helpers are the thing under test.
fn store_default_workspace_is_stale(jj: &JjEnv, store: &Path) -> bool {
    jj.run(
        store,
        &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
        "flagless store probe",
    )
    .err()
    .is_some_and(|error| is_stale_error(&error))
}

/// Drive the store's DEFAULT workspace into jj's stale state exactly the way
/// Cairn's own traffic does: every store write passes `--ignore-working-copy`,
/// so one that rewrites the default `@` advances the repo view and leaves the
/// disk behind. Returns the advanced `main` commit.
fn make_store_default_workspace_stale(jj: &JjEnv, store: &Path, project: &Path) -> String {
    use std::sync::atomic::{AtomicUsize, Ordering};
    // Each call must move the backing git by a DISTINCT commit: a repeat of the
    // same content is an empty commit git refuses, and jj detects staleness by
    // comparing trees, so an advance that does not change main's tree would not
    // make the rebased `@` stale either.
    static ADVANCE: AtomicUsize = AtomicUsize::new(0);
    let n = ADVANCE.fetch_add(1, Ordering::Relaxed);
    std::fs::write(
        project.join(format!("store-advance-{n}.rs")),
        format!("advance {n}\n"),
    )
    .unwrap();
    git(project, &["add", "-A"]);
    git(project, &["commit", "-q", "-m", "advance the backing git"]);
    ensure_project_store(jj, store, project).unwrap();
    jj.run(
        store,
        &["rebase", "-r", "@", "-d", "main", "--ignore-working-copy"],
        "rebase the store default @ onto the advanced main",
    )
    .unwrap();
    assert!(
        store_default_workspace_is_stale(jj, store),
        "fixture must actually produce a stale store default workspace, or every \
         assertion downstream passes against a healthy store"
    );
    bookmark_commit(jj, store, "main").unwrap()
}

/// The reason [`run_needing_store_workspace`] has to exist, pinned against jj
/// rather than asserted in prose: `jj workspace add` REJECTS
/// `--ignore-working-copy`, so it is the one store operation Cairn cannot make
/// workspace-free by convention. If a future jj accepts the flag here, this test
/// fails and the repair can be deleted in favour of the flag.
#[test]
#[serial_test::serial(jj)]
fn jj_rejects_ignore_working_copy_on_workspace_add() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping jj_rejects_ignore_working_copy_on_workspace_add: no jj");
        return;
    };
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    init_project(project.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("project");
    ensure_project_store(&jj, &store, project.path()).unwrap();

    let path = workspaces.path().join("probe");
    let error = jj
        .run(
            &store,
            &[
                "workspace",
                "add",
                "--ignore-working-copy",
                "--name",
                "probe",
                &path.to_string_lossy(),
            ],
            "probe whether workspace add accepts the flag",
        )
        .expect_err("jj refuses the flag on a command that must write a working copy");
    assert!(
        error.contains("must be able to update the working copy"),
        "the refusal must be jj's flag rejection, not some other failure: {error}"
    );
}

/// `jj workspace update-stale` reconciles every staleness state the store can
/// reach, and says so plainly when there is nothing to reconcile.
///
/// This encodes the empirical finding CAIRN-3223 was opened to settle. An
/// incident had recorded `update-stale` as "exits 0, changes nothing" in the
/// very state its own error message prescribes it for, which would have meant
/// the repair had to run the other way (`jj edit` back to the commit the disk
/// remembers). It does not: the no-op was Cairn's own `jj` shim intercepting the
/// command before it reached jj. Real jj repairs all three states below. If a
/// future jj regresses any of them, this fails rather than the runner quietly
/// stranding a spawn.
#[test]
#[serial_test::serial(jj)]
fn update_stale_reconciles_every_staleness_state_the_store_can_reach() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping update_stale_reconciles_every_staleness_state: no jj");
        return;
    };
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    init_project(project.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("project");
    ensure_project_store(&jj, &store, project.path()).unwrap();

    // State 1: `@` rewritten out from under the disk by an `--ignore-working-copy`
    // rebase. This is the shape Cairn's own store traffic produces.
    make_store_default_workspace_stale(&jj, &store, project.path());
    update_stale(&jj, &store).unwrap();
    assert!(
        !store_default_workspace_is_stale(&jj, &store),
        "a rebased-out `@` is reconciled"
    );

    // State 2: `@` ABANDONED, the view minting a replacement — the "disk
    // remembers a commit the repo view abandoned" state, the one the plan
    // suspected `update-stale` could not handle. Snapshotting a scratch file
    // first is what makes the abandoned commit's tree differ from its
    // replacement's; jj detects staleness by tree, so an empty `@` abandoned and
    // replaced by an empty `@` is not stale at all.
    std::fs::write(store.join("scratch.txt"), "scratch\n").unwrap();
    let abandoned = jj
        .run(
            &store,
            &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
            "snapshot the scratch file into the store's @",
        )
        .unwrap();
    jj.run(
        &store,
        &["abandon", "-r", &abandoned, "--ignore-working-copy"],
        "abandon the store's working-copy commit",
    )
    .unwrap();
    assert!(
        store_default_workspace_is_stale(&jj, &store),
        "precondition: abandoning a non-empty `@` leaves the disk stale"
    );
    update_stale(&jj, &store).unwrap();
    assert!(
        !store_default_workspace_is_stale(&jj, &store),
        "an abandoned `@` is reconciled — there is no inverse repair to add"
    );
    assert!(
        !store.join("scratch.txt").exists(),
        "the reconcile moved the disk to the view, discarding what only the disk held"
    );

    // State 3: the view rewound BELOW the disk by `jj op restore` — reachable
    // from the store, which restores operations to roll back a conflicting
    // rebase.
    let op_before = jj
        .run(
            &store,
            &[
                "op",
                "log",
                "--no-graph",
                "--limit",
                "1",
                "-T",
                "id.short()",
                "--ignore-working-copy",
            ],
            "capture the store op to rewind to",
        )
        .unwrap();
    std::fs::write(store.join("rewind.txt"), "rewind\n").unwrap();
    jj.run(
        &store,
        &["log", "-r", "@", "--no-graph", "-T", "commit_id"],
        "snapshot the rewind file into the store's @",
    )
    .unwrap();
    jj.run(
        &store,
        &["op", "restore", &op_before, "--ignore-working-copy"],
        "rewind the store view below the disk",
    )
    .unwrap();
    assert!(
        store_default_workspace_is_stale(&jj, &store),
        "precondition: a rewound view leaves the disk ahead and therefore stale"
    );
    update_stale(&jj, &store).unwrap();
    assert!(
        !store_default_workspace_is_stale(&jj, &store),
        "a rewound view is reconciled"
    );

    // State 4: nothing to do. `update-stale` is a no-op here, and that is the
    // ONLY no-op it has — it reports it rather than exiting silently.
    update_stale(&jj, &store).unwrap();
    assert!(!store_default_workspace_is_stale(&jj, &store));
}

/// **The regression test for CAIRN-3270.** A publication that moves the jj
/// bookmark and stops there looks entirely successful — it reports a real
/// commit on a real branch — while `refs/heads/<branch>` in the backing
/// checkout stays frozen at whatever it last held. That is how a coordinator
/// made two integration commits, saw `✓ Committed` for both, and left its
/// branch ref pinned at a tip containing neither.
///
/// Asserted across THREE successive publications on purpose: the single-
/// publication case can pass by accident, because any unrelated jj CLI
/// operation running against the store in between exports every bookmark as a
/// side effect. Only a run where each publication carries its own export can
/// keep the ref level with the bookmark every time.
#[tokio::test]
#[serial_test::serial(jj)]
async fn three_successive_publications_each_reach_the_backing_branch_ref() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping three_successive_publications_reach_the_branch_ref: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/cairn-3270-coordinator-0";
    let base = bookmark_commit(&jj, &store, "main").unwrap();
    create_bookmark_at(&jj, &store, branch, &base).unwrap();

    let mut expected = base;
    for round in 1..=3 {
        let published = publish_logical_head_exported(
            &jj,
            &store,
            branch,
            &expected,
            ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
                path: format!("integration-{round}.rs"),
                content: Some(format!("integration {round}\n").into_bytes()),
            }]),
            None,
            crate::PublicationMode::Child {
                description: format!("integrate child {round}"),
            },
        )
        .await
        .unwrap_or_else(|error| panic!("round {round} publication: {error}"));
        published
            .export
            .unwrap_or_else(|error| panic!("round {round} export: {error}"));

        assert_eq!(
            bookmark_commit(&jj, &store, branch).as_deref(),
            Some(published.landed.head.as_str()),
            "round {round}: the bookmark carries the published commit"
        );
        assert_eq!(
            git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
            published.landed.head,
            "round {round}: the BRANCH REF must carry the commit the publication reported. A ref \
             left behind here is invisible to every consumer outside jj — a push, a child branch \
             cut from it, GitHub's view of the PR head."
        );
        expected = published.landed.head;
    }

    // Every round's content is present at the ref, so no publication was lost.
    let tree = git_stdout(
        proj.path(),
        &["ls-tree", "--name-only", &format!("refs/heads/{branch}")],
    );
    for round in 1..=3 {
        assert!(
            tree.contains(&format!("integration-{round}.rs")),
            "round {round}'s content is missing from the published tree: {tree}"
        );
    }
}

/// Aborting a publication's caller between the two legs stops neither: the
/// abort cannot take effect inside the poll that runs them, so the caller is
/// still running, and the export still lands, after the abort. The store ends
/// on one operation head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(jj)]
async fn an_aborted_caller_cannot_stop_a_publication_between_its_legs() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping aborted publication test: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/aborted-publication";
    let base = bookmark_commit(&jj, &store, "main").unwrap();
    create_bookmark_at(&jj, &store, branch, &base).unwrap();

    let pause = super::testing::pause_publication_after_landing(&store);
    let caller = tokio::spawn({
        let (jj, store, base) = (jj.clone(), store.clone(), base.clone());
        async move {
            publish_logical_head_exported(
                &jj,
                &store,
                branch,
                &base,
                ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
                    path: "aborted.rs".to_string(),
                    content: Some(b"aborted\n".to_vec()),
                }]),
                None,
                crate::PublicationMode::Child {
                    description: "published under an abort".into(),
                },
            )
            .await
        }
    });
    assert!(
        in_place_blocking(|| pause.wait_until_paused(std::time::Duration::from_secs(30))),
        "the publication reached the pause between its legs"
    );
    caller.abort();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        !caller.is_finished(),
        "an abort cannot end the caller inside the poll that is publishing"
    );
    pause.release();
    let _ = caller.await;

    let head = bookmark_commit(&jj, &store, branch).expect("the bookmark exists");
    assert_ne!(head, base, "the store transaction landed");
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        head,
        "the export leg ran after the abort"
    );
    assert!(
        matches!(
            crate::settled_operation(&store),
            Ok(crate::SettledOperation::Single(_))
        ),
        "one operation head"
    );
}

type StalePublicationFixture = (
    TempDir,
    TempDir,
    TempDir,
    TempDir,
    JjEnv,
    PathBuf,
    String,
    PathBuf,
);

fn stale_publication_fixture() -> Option<StalePublicationFixture> {
    let bin = jj_bin()?;
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let origin = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    git(origin.path(), &["init", "-q", "--bare", "-b", "main"]);
    init_project(project.path());
    git(
        project.path(),
        &["remote", "add", "origin", &origin.path().to_string_lossy()],
    );
    git(project.path(), &["push", "-q", "origin", "main"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("publication");
    ensure_project_store(&jj, &store, project.path()).unwrap();
    let branch = "agent/cairn-3307-builder-0".to_string();
    let workspace = workspaces.path().join("builder");
    add_workspace(&jj, &store, &workspace, &branch, "main", None).unwrap();
    std::fs::write(workspace.join("published.rs"), "published\n").unwrap();
    seal(&jj, &workspace, "published frontier", None).unwrap();
    export_bookmark_advance(&jj, &store, true, &branch, "stale publication fixture").unwrap();
    push_store_bookmark_classified(&jj, &store, &branch).unwrap();
    Some((
        home, project, origin, workspaces, jj, store, branch, workspace,
    ))
}

/// Whether `branch@origin` exists, and if so whether this store tracks it.
fn origin_tracking(jj: &JjEnv, store: &Path, branch: &str) -> Option<bool> {
    let any = revset_commit(
        jj,
        store,
        &format!("remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")"),
    );
    let untracked = revset_commit(
        jj,
        store,
        &format!("untracked_remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")"),
    );
    any.map(|_| untracked.is_none())
}

/// Mint a fresh managed branch off `main` with one sealed, exported commit that
/// has never been pushed — the shape a create-pr push publishes for the first time.
fn mint_unpublished_branch(jj: &JjEnv, store: &Path, workspaces: &Path, branch: &str) -> PathBuf {
    let workspace = workspaces.join(branch.replace('/', "-"));
    add_workspace(jj, store, &workspace, branch, "main", None).unwrap();
    std::fs::write(workspace.join("first.rs"), "first\n").unwrap();
    seal(jj, &workspace, "first commit", None).unwrap();
    export_bookmark_advance(jj, store, true, branch, "mint unpublished branch").unwrap();
    workspace
}

/// Reproduce the residue of cairn/4810: a lock-holding caller snapshots the op
/// log, a first push of `branch` lands unlocked, the caller rolls back with the
/// old WHOLE-STORE `jj op restore` (forgetting the push), and a later recovery
/// fetches and imports origin's copy — which jj records UNTRACKED.
fn push_then_forget_then_reimport(jj: &JjEnv, store: &Path, branch: &str) {
    let snapshot = operation_id(jj, store).unwrap();
    push_store_bookmark(jj, store, branch).unwrap();
    jj.run(
        store,
        &["op", "restore", &snapshot, "--ignore-working-copy"],
        "pre-fix whole-store rollback",
    )
    .unwrap();
    jj.run(
        store,
        &["git", "export", "--ignore-working-copy"],
        "export after whole-store rollback",
    )
    .unwrap();
    fetch_remote_branch_via_git(store, "origin", branch).unwrap();
    import_git(jj, store).unwrap();
}

/// An untracked origin copy with history foreign to the branch is refused by
/// converge and left exactly as it was: untracked, and unchanged on origin.
#[test]
#[serial_test::serial(jj)]
fn an_untracked_remote_with_foreign_history_is_refused_and_left_untracked() {
    let Some((_home, project, origin, _workspaces, jj, store, branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    jj.run(
        &store,
        &[
            "bookmark",
            "untrack",
            &branch,
            "--remote=origin",
            "--ignore-working-copy",
        ],
        "untrack the fixture branch",
    )
    .unwrap();
    let local_before = bookmark_commit(&jj, &store, &branch).unwrap();
    let tree = git_stdout(project.path(), &["rev-parse", "main^{tree}"]);
    let foreign = git_stdout(
        project.path(),
        &["commit-tree", &tree, "-m", "another clone's unrelated push"],
    );
    git(
        project.path(),
        &[
            "push",
            "-q",
            "--force",
            "origin",
            &format!("{foreign}:refs/heads/{branch}"),
        ],
    );
    fetch_remote_branch_via_git(&store, "origin", &branch).unwrap();
    import_git(&jj, &store).unwrap();
    assert_eq!(origin_tracking(&jj, &store, &branch), Some(false));
    assert!(
        !track_own_remote_of_same_lineage(&jj, &store, &branch).unwrap(),
        "a foreign remote copy is never tracked"
    );

    let error = converge_managed_branch_after_remote_rewrite(&jj, &store, &branch).unwrap_err();
    assert!(error.contains("no change-id twin"), "{error}");
    assert_eq!(origin_tracking(&jj, &store, &branch), Some(false));
    assert_eq!(
        bookmark_commit(&jj, &store, &branch).as_deref(),
        Some(local_before.as_str())
    );
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        foreign
    );
}

/// jj's default `immutable_heads()` includes untracked remote bookmarks, so an
/// untracked origin copy of the branch's own history froze its pushed commits and
/// the base-advance rebase was refused as rewriting immutable commits. The rebase
/// heals the copy first, then rebases, and the rewritten branch still publishes.
#[test]
#[serial_test::serial(jj)]
fn a_base_advance_rebases_a_branch_whose_origin_copy_was_untracked() {
    let Some((_home, _project, origin, workspaces, jj, store, _branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let branch = "agent/cairn-4804-builder-5";
    mint_unpublished_branch(&jj, &store, workspaces.path(), branch);
    push_then_forget_then_reimport(&jj, &store, branch);
    assert_eq!(origin_tracking(&jj, &store, branch), Some(false));
    let new_base = advance_store_main(&jj, &store);

    let outcome = rebase_branch_onto(&jj, &store, branch, "main").unwrap();
    assert_eq!(outcome, RebaseOutcome::Rebased);
    assert!(branch_descends_from(&jj, &store, branch, &new_base));
    assert_eq!(origin_tracking(&jj, &store, branch), Some(true));

    push_store_bookmark(&jj, &store, branch).unwrap();
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        bookmark_commit(&jj, &store, branch).unwrap()
    );
}

/// A branch one commit ahead of a `main` that has since advanced, with
/// `packed-refs` present so gix consults (and locks) it on every ref write.
/// The store replay's lock tests share it.
struct ReplayLockFixture {
    _home: TempDir,
    _proj: TempDir,
    _wts: TempDir,
    jj: JjEnv,
    store: PathBuf,
    branch: &'static str,
    lock: PathBuf,
}

fn replay_lock_fixture(bin: &str) -> ReplayLockFixture {
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/cairn-4994-builder-0";
    let ws = wts.path().join("job");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("agent.rs"), "agent work\n").unwrap();
    seal(&jj, &ws, "agent work", None).unwrap();
    advance_project(proj.path());
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    git(proj.path(), &["pack-refs", "--all"]);
    let lock = proj.path().join(".git").join("packed-refs.lock");
    ReplayLockFixture {
        _home: home,
        _proj: proj,
        _wts: wts,
        jj,
        store,
        branch,
        lock,
    }
}

/// The premise every patient store rewrite rests on, pinned against the
/// bundled jj: a rewrite that loses `packed-refs.lock` fails BEFORE its
/// transaction commits. jj-lib writes `refs/jj/keep/<id>` inside the commit
/// write, ahead of `Transaction::commit`, so the operation head and every
/// bookmark stay put and repeating the identical command is safe. A jj upgrade
/// that moved the keep-ref write after the commit would make the repeat
/// double-apply, and this is where that would show.
#[test]
#[serial_test::serial(jj)]
fn a_rewrite_that_loses_the_packed_refs_lock_commits_no_operation() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping lost-lock rewrite premise test: jj not resolvable");
        return;
    };
    let fx = replay_lock_fixture(&bin);
    let pre_op = operation_id(&fx.jj, &fx.store).unwrap();
    let pre_tip = bookmark_commit(&fx.jj, &fx.store, fx.branch).unwrap();

    std::fs::write(&fx.lock, "held by a live writer\n").unwrap();
    let held = std::fs::File::open(&fx.lock).unwrap();
    let result = fx.jj.run(
        &fx.store,
        &[
            "rebase",
            "-b",
            fx.branch,
            "-o",
            "main",
            "--ignore-working-copy",
        ],
        "bare jj rebase under a held lock",
    );
    drop(held);
    std::fs::remove_file(&fx.lock).unwrap();

    let error = result.expect_err("a rebase cannot write its commits while the lock is held");
    assert!(
        packed_refs_lock_error(&error),
        "the failure is recognizable as lock contention: {error}"
    );
    assert_eq!(
        operation_id(&fx.jj, &fx.store).unwrap(),
        pre_op,
        "the failed rewrite committed no operation"
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, fx.branch).as_deref(),
        Some(pre_tip.as_str()),
        "and moved no bookmark"
    );
}

/// Put an empty commit described `message` on top of `bookmark`, from the store
/// side, and move the bookmark onto it: the shape a write-check fix or a
/// structural merge leaves once the base carries the same change.
fn store_empty_commit_on(jj: &JjEnv, store: &Path, bookmark: &str, message: &str) -> String {
    let on = format!("bookmarks(exact:{bookmark:?})");
    jj.run(
        store,
        &[
            "new",
            "--no-edit",
            &on,
            "-m",
            message,
            "--ignore-working-copy",
        ],
        "new empty commit",
    )
    .unwrap();
    let commit = revset_commit(
        jj,
        store,
        &format!("children({on}) & empty() & description(substring:{message:?})"),
    )
    .unwrap();
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            bookmark,
            "-r",
            &commit,
            "--ignore-working-copy",
        ],
        "move bookmark onto the empty commit",
    )
    .unwrap();
    commit
}

/// A branch on which every commit is empty has nothing to land. It is refused
/// as such, and left where it was rather than moved onto its base.
#[test]
#[serial_test::serial(jj)]
fn publication_refuses_a_branch_that_is_only_empty_commits() {
    let Some(bin) = jj_bin() else { return };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/cairn-5603-builder-0";
    jj.run(
        &store,
        &[
            "bookmark",
            "create",
            branch,
            "-r",
            "main",
            "--ignore-working-copy",
        ],
        "create branch",
    )
    .unwrap();
    let only = store_empty_commit_on(&jj, &store, branch, "fix: apply write-check changes");

    let error = drop_empty_commits(&jj, &store, branch, "main").unwrap_err();
    assert!(
        error.contains("no changes beyond `main`") && error.contains("nothing to land"),
        "{error}"
    );
    assert_eq!(bookmark_commit(&jj, &store, branch), Some(only));
}

/// A store and project for the stale-expectation tests: the project checkout
/// detached so the export's HEAD repair stays out of the picture, and `branch`
/// created at `main`.
fn stale_expectation_fixture(
    bin: &str,
    branch: &str,
) -> (TempDir, TempDir, JjEnv, PathBuf, String) {
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    git(proj.path(), &["checkout", "-q", "--detach"]);
    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let base = bookmark_commit(&jj, &store, "main").unwrap();
    create_bookmark_at(&jj, &store, branch, &base).unwrap();
    (home, proj, jj, store, base)
}

async fn publish_file(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    expected: &str,
    path: &str,
) -> Result<String, String> {
    let published = publish_logical_head_exported(
        jj,
        store,
        branch,
        expected,
        ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
            path: path.to_string(),
            content: Some(format!("{path}\n").into_bytes()),
        }]),
        None,
        crate::PublicationMode::Child {
            description: format!("add {path}"),
        },
    )
    .await?;
    published.export?;
    Ok(published.landed.head)
}

/// cairn/5757. A push runs after its publication, outside the store lock, and
/// verifies the export against the bookmark as it read it. A second publication
/// built on the first landed and exported between that read and the
/// verification, so the git ref held the newer commit. The verifier called that a
/// freeze and "repaired" it with `jj bookmark set --allow-backwards` to the stale
/// read, orphaning the second publication after its write had reported it
/// committed.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_stale_export_expectation_never_rewinds_a_later_publication() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping stale export expectation test: jj not resolvable");
        return;
    };
    let branch = "agent/cairn-5757-builder-0";
    let (_home, proj, jj, store, base) = stale_expectation_fixture(&bin, branch);

    let first = publish_file(&jj, &store, branch, &base, "first.rs")
        .await
        .unwrap();
    // The first publication's push reads the bookmark here...
    let push_read = bookmark_commit(&jj, &store, branch).unwrap();
    assert_eq!(push_read, first);
    // ...and the second lands on top of it before that push verifies.
    let second = publish_file(&jj, &store, branch, &first, "second.rs")
        .await
        .unwrap();

    export_git_verified(
        &jj,
        &store,
        true,
        "push verifies a stale read",
        &[(branch, push_read.as_str())],
    )
    .expect("git carrying a descendant of the expected commit is not a freeze");

    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(second.as_str()),
        "the second publication must still be the branch head"
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        second
    );
    assert_eq!(
        verified_publish_target(&jj, &store, branch)
            .unwrap()
            .as_deref(),
        Some(second.as_str()),
        "a push publishes the bookmark as it stands, not an earlier read"
    );
}

/// The other side of the same judgment: a bookmark moved somewhere that does not
/// carry the expected commit is not a freeze either, and not this caller's to
/// fix. The verification fails loudly and leaves the bookmark where it is,
/// because the expected commit is genuinely not on the branch.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_bookmark_moved_off_the_expected_commit_is_an_error_and_left_alone() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping moved-bookmark export test: jj not resolvable");
        return;
    };
    let branch = "agent/cairn-5757-builder-1";
    let (_home, proj, jj, store, base) = stale_expectation_fixture(&bin, branch);

    let first = publish_file(&jj, &store, branch, &base, "first.rs")
        .await
        .unwrap();
    let scratch = "agent/cairn-5757-scratch";
    create_bookmark_at(&jj, &store, scratch, &base).unwrap();
    let sibling = publish_file(&jj, &store, scratch, &base, "sibling.rs")
        .await
        .unwrap();
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &sibling,
            "--allow-backwards",
            "--ignore-working-copy",
        ],
        "move the branch sideways",
    )
    .unwrap();
    export_git_preserving_checkout(&jj, &store, true, "export the sideways move").unwrap();

    let error = export_git_verified(
        &jj,
        &store,
        true,
        "verify a commit the branch no longer carries",
        &[(branch, first.as_str())],
    )
    .expect_err("the expected commit is not on the branch, so this cannot be success");
    assert!(is_export_freeze_error(&error), "{error}");
    assert!(
        error.contains(&first) && error.contains(&sibling),
        "{error}"
    );

    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(sibling.as_str()),
        "the verifier must not move a bookmark another writer moved"
    );
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        sibling
    );
}

#[test]
fn operation_budget_timeout_classification_keeps_rollback_failure_loudest() {
    for error in [
        "jj rebase timed out after 10s and was killed",
        "jj log timed out: operation budget exhausted before spawning",
        "jj rebase follow-through timed out: operation budget exhausted",
    ] {
        assert_eq!(reconcile_failure_kind(error), RECONCILE_TIMEOUT);
        assert_eq!(
            reconcile_failure_kind(&format!("{error}\n{ROLLBACK_INCOMPLETE} could not restore")),
            ROLLBACK_FAILED,
        );
    }
}

/// An assessment reading pinned commits outside the store lock must not settle
/// the store: pinned to an operation, a read leaves divergent operation heads
/// exactly as it found them. The unpinned read beside it shows the hazard it
/// avoids, which is jj merging the heads and writing that merge as an operation.
#[test]
#[serial_test::serial(jj)]
fn a_read_pinned_to_an_operation_never_settles_divergent_heads() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_read_pinned_to_an_operation_never_settles_divergent_heads: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let op_heads = || {
        std::fs::read_dir(
            store
                .join(".jj")
                .join("repo")
                .join("op_heads")
                .join("heads"),
        )
        .unwrap()
        .count()
    };

    let base_op = current_op_id(&jj, &store);
    for name in ["fork-a", "fork-b"] {
        jj.run(
            &store,
            &[
                "bookmark",
                "create",
                name,
                "-r",
                "main",
                "--ignore-working-copy",
                "--at-op",
                &base_op,
            ],
            "fork the operation log",
        )
        .unwrap();
    }
    assert_eq!(op_heads(), 2, "two operations forked from one base");

    let pinned = jj.at_operation(&base_op);
    let listed = file_list(&pinned, &store, "main", "").unwrap();
    assert!(!listed.is_empty(), "the pinned read answered");
    assert!(
        paths_changed_between(&pinned, &store, "main", "main", &listed)
            .unwrap()
            .is_empty()
    );
    assert_eq!(op_heads(), 2, "a pinned read settles nothing");

    file_list(&jj, &store, "main", "").unwrap();
    assert_eq!(
        op_heads(),
        1,
        "an unpinned read merges the divergent heads: the write the pin avoids"
    );
}

#[cfg(unix)]
fn admission_marker_env(home: &Path) -> (JjEnv, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    // A fresh test process can spend three seconds probing its login-shell PATH.
    // Resolve it before callers start budgets meant to measure the marker child.
    let _ = cairn_platform::env::get_user_path();
    let script = home.join("admission-jj");
    std::fs::write(
        &script,
        "#!/bin/sh\nprintf x >> \"$1\"\nsleep \"$2\"\nexit \"$3\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    (
        JjEnv::with_binary(script.to_string_lossy(), home),
        home.join("spawn-marker"),
    )
}

#[cfg(unix)]
#[test]
#[serial_test::serial(jj)]
fn operation_budget_minimum_rejects_before_spawn_and_records_admission() {
    let home = tempfile::tempdir().unwrap();
    let (original, marker) = admission_marker_env(home.path());
    let budgeted = original.with_operation_budget(Duration::from_millis(18));
    let error = budgeted
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0", "0"],
            "forward below minimum",
        )
        .unwrap_err();
    assert!(is_jj_timeout_error(&error), "{error}");
    assert!(error.contains("forward below minimum"), "{error}");
    assert!(
        error.contains("remaining") && error.contains("minimum 100.000ms"),
        "{error}"
    );
    assert!(error.contains("before spawning"), "{error}");
    let outer_error = budgeted
        .check_operation_budget("outer final check")
        .unwrap_err();
    assert!(is_jj_timeout_error(&outer_error), "{outer_error}");
    assert!(outer_error.contains("minimum 100.000ms"), "{outer_error}");
    assert!(
        !marker.exists(),
        "admission must reject before executing the marker"
    );
    let summary = budgeted.operation_phase_summary();
    assert!(
        summary.contains("forward below minimum [forward; not spawned]"),
        "{summary}"
    );
    assert!(
        summary.contains("ms:") && summary.contains("minimum 100.000ms"),
        "{summary}"
    );
    assert_eq!(
        original.operation_phase_summary(),
        "no subprocess phases recorded"
    );
}

#[cfg(unix)]
#[test]
#[serial_test::serial(jj)]
fn operation_budget_phase_trace_is_shared_reset_and_measures_completed_commands() {
    let home = tempfile::tempdir().unwrap();
    let (original, marker) = admission_marker_env(home.path());
    let scoped = original.with_operation_budget(Duration::from_secs(30));
    let clone = scoped.clone();
    assert_eq!(scoped.operation_deadline, clone.operation_deadline);
    clone
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0.05", "0"],
            "measured forward",
        )
        .unwrap();
    let error = scoped
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0", "7"],
            "failed forward",
        )
        .unwrap_err();
    assert!(error.contains("failed forward failed"), "{error}");
    let cleanup = scoped.cleanup_env();
    cleanup
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0", "0"],
            "measured rollback",
        )
        .unwrap();
    let summary = clone.operation_phase_summary();
    assert_eq!(summary, scoped.operation_phase_summary());
    assert_eq!(summary, cleanup.operation_phase_summary());
    assert!(
        summary.contains("measured rollback [cleanup; spawn attempted]"),
        "{summary}"
    );
    assert!(
        summary.contains("failed forward [forward; spawn attempted]"),
        "{summary}"
    );
    let duration: f64 = summary
        .split("measured forward [forward; spawn attempted] ")
        .nth(1)
        .unwrap()
        .split("ms:")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        duration >= 50.0,
        "phase must measure subprocess execution: {summary}"
    );
    assert_eq!(std::fs::read(marker).unwrap(), b"xxx");
    assert_eq!(
        clone.operation_deadline, scoped.operation_deadline,
        "commands must not renew the deadline"
    );
    assert_eq!(
        scoped
            .with_operation_budget(Duration::from_secs(30))
            .operation_phase_summary(),
        "no subprocess phases recorded",
    );
    assert_eq!(
        original.operation_phase_summary(),
        "no subprocess phases recorded"
    );
}

#[cfg(unix)]
#[test]
#[serial_test::serial(jj)]
fn operation_budget_cleanup_below_forward_minimum_still_attempts_spawn() {
    let home = tempfile::tempdir().unwrap();
    let forward = JjEnv::with_binary("unused", home.path()).with_operation_budget(Duration::ZERO);
    let mut cleanup = forward.cleanup_env();
    // A missing executable gives deterministic evidence of reaching spawn even
    // when a loaded executor cannot schedule a marker process in this interval.
    cleanup.operation_deadline =
        Some(std::time::Instant::now() + JJ_FORWARD_SUBPROCESS_MINIMUM / 2);
    let mut command = std::process::Command::new(home.path().join("missing-executable"));
    let error = cleanup
        .command_output(&mut command, Duration::from_secs(30), "short cleanup")
        .unwrap_err();
    assert!(
        !is_jj_timeout_error(&error),
        "cleanup must reach spawn, not minimum admission: {error}"
    );
    let summary = forward.operation_phase_summary();
    assert!(
        summary.contains("short cleanup [cleanup; spawn attempted]"),
        "{summary}"
    );
    assert!(!summary.contains("minimum"), "{summary}");
    cleanup.operation_deadline = Some(std::time::Instant::now() + Duration::from_millis(18));
    assert!(cleanup
        .check_operation_budget("cleanup final check")
        .is_ok());
}

#[cfg(unix)]
#[test]
#[serial_test::serial(jj)]
fn operation_budget_successor_preserves_deadline_trace_and_old_cleanup_barriers() {
    let home = tempfile::tempdir().unwrap();
    let (original, marker) = admission_marker_env(home.path());
    let forward = original.with_operation_budget(Duration::from_secs(30));
    let old_clone = forward.clone();
    let cleanup = forward.cleanup_env();
    cleanup
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0", "0"],
            "first restored conflict",
        )
        .unwrap();
    let successor = forward.next_transaction_after_restored_conflict().unwrap();
    assert_eq!(successor.operation_deadline, forward.operation_deadline);
    assert_eq!(
        successor.operation_phase_summary(),
        cleanup.operation_phase_summary()
    );
    assert!(
        successor
            .next_transaction_after_restored_conflict()
            .is_err(),
        "successor has not rolled back"
    );
    successor
        .clone()
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0", "0"],
            "structural successor",
        )
        .unwrap();
    for old in [&forward, &old_clone] {
        let error = old
            .run(
                home.path(),
                &[marker.to_str().unwrap(), "0", "0"],
                "old transaction retry",
            )
            .unwrap_err();
        assert!(error.contains("cleanup has started"), "{error}");
    }
    assert_eq!(std::fs::read(&marker).unwrap(), b"xx");
    assert_eq!(
        successor.operation_phase_summary(),
        old_clone.operation_phase_summary()
    );
    let summary = forward.operation_phase_summary();
    assert!(
        summary.contains("first restored conflict [cleanup; spawn attempted]"),
        "{summary}"
    );
    assert!(
        summary.contains("structural successor [forward; spawn attempted]"),
        "{summary}"
    );
    assert!(
        summary.contains("old transaction retry [forward; not spawned]"),
        "{summary}"
    );

    let successor_cleanup = successor.cleanup_env();
    let next = successor
        .next_transaction_after_restored_conflict()
        .unwrap();
    assert_eq!(next.operation_deadline, forward.operation_deadline);
    assert_eq!(
        next.operation_phase_summary(),
        successor_cleanup.operation_phase_summary()
    );
    assert!(successor
        .run(
            home.path(),
            &[marker.to_str().unwrap(), "0", "0"],
            "closed successor retry",
        )
        .unwrap_err()
        .contains("cleanup has started"));
}

#[test]
fn operation_budget_successor_rejects_cleanup_expiry_and_insufficient_remainder() {
    let home = tempfile::tempdir().unwrap();
    let original = JjEnv::with_binary("unused", home.path());
    assert!(original.next_transaction_after_restored_conflict().is_err());
    let forward = original.with_operation_budget(Duration::from_secs(30));
    assert!(forward.next_transaction_after_restored_conflict().is_err());
    assert!(forward
        .cleanup_env()
        .next_transaction_after_restored_conflict()
        .is_err());
    for budget in [Duration::ZERO, Duration::from_millis(18)] {
        let short = original.with_operation_budget(budget);
        let _cleanup = short.cleanup_env();
        let error = match short.next_transaction_after_restored_conflict() {
            Ok(_) => panic!("an exhausted or subminimum operation must not be revived"),
            Err(error) => error,
        };
        assert!(is_jj_timeout_error(&error), "{error}");
        assert!(
            error.contains("remaining") && error.contains("minimum 100.000ms"),
            "{error}"
        );
    }
}

/// cairn/5326's specimen in miniature. A take-committed-tip replay restores the
/// resolution into the TIP, so the commit where the conflict first arose stays
/// conflict-flagged below it, and the reconcile used to heal that by collapsing
/// the whole branch into one commit under the tip's message. Five commits, the
/// third conflicting with main and the fifth the agent's committed resolution:
/// after the replay and the heal, commits one and two are the very same commits
/// the rebase produced, the third holds the resolution under its own message
/// plus a note saying so, the fourth is untouched, and the fifth — left empty
/// once its content lives in the third — is dropped and named.
#[test]
#[serial_test::serial(jj)]
fn a_take_committed_tip_replay_heals_a_five_commit_branch_in_place() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_take_committed_tip_replay_heals_a_five_commit_branch_in_place: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/cairn-5326-builder-0";
    let ws = wts.path().join("agent");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    for (file, content, message) in [
        ("one.rs", "one\n", "first: add one"),
        ("two.rs", "two\n", "second: add two"),
        ("shared.rs", "BRANCH-SIDE\n", "third: reword shared"),
        ("four.rs", "four\n", "fourth: add four"),
    ] {
        std::fs::write(ws.join(file), content).unwrap();
        seal(&jj, &ws, message, None).unwrap();
    }
    advance_main_shared(&jj, &store, wts.path(), "MAIN-SIDE\n");

    // The advance conflicts and is rolled back; the agent commits its
    // resolution on the old base, as the conflict session asks.
    let RebaseOutcome::Conflicted { diagnostic } =
        rebase_branch_onto(&jj, &store, branch, "main").unwrap()
    else {
        panic!("precondition: the advance conflicts on the third commit");
    };
    let paths = diagnostic.conflicting_paths();
    assert_eq!(paths, vec!["shared.rs".to_string()]);
    update_stale(&jj, &ws).unwrap();
    std::fs::write(ws.join("shared.rs"), "MAIN-SIDE plus branch\n").unwrap();
    seal(&jj, &ws, "fifth: take main's wording for shared", None).unwrap();
    let tip = bookmark_commit(&jj, &store, branch).unwrap();

    // Preparation publishes only the fully healed history, never an intermediate conflict.
    assert!(matches!(
        rebase_branch_onto_with_resolution(
            &jj,
            &store,
            branch,
            "main",
            Some(&ResolutionCarry::from_tip(&tip, &paths)),
        )
        .unwrap(),
        RebaseOutcome::Rebased
    ));
    let dest = bookmark_commit(&jj, &store, "main").unwrap();
    let range = format!("{dest}..bookmarks(exact:{branch:?})");
    let replayed = commits_oldest_first(&jj, &store, &range);
    assert_eq!(replayed.len(), 4);
    assert!(conflicted_commits(&jj, &store, &range).is_empty());

    let report =
        reconcile_siblings_without_publication(&jj, &store, "main", &[branch.to_string()]).unwrap();
    assert_eq!(report.rebased_clean, vec![branch.to_string()], "{report:?}");
    assert!(
        report.conflicted.is_empty() && report.failed.is_empty(),
        "{report:?}"
    );

    let healed = commits_oldest_first(&jj, &store, &range);
    let subjects: Vec<&str> = healed.iter().map(|(_, subject)| subject.as_str()).collect();
    assert_eq!(
        subjects,
        vec![
            "first: add one",
            "second: add two",
            "third: reword shared",
            "fourth: add four"
        ],
        "nothing is collapsed: every commit that still changes something keeps its place and message"
    );
    assert_eq!(
        healed[..2],
        replayed[..2],
        "the clean prefix below the conflict is not rewritten at all"
    );
    assert!(conflicted_commits(&jj, &store, &range).is_empty());

    let third = description_of(&jj, &store, &healed[2].0);
    assert!(
        third.starts_with("third: reword shared\n\nCairn conflict heal:")
            && third.contains("`shared.rs`")
            && third.contains("Commits this left with no changes were dropped:")
            && third.contains("\"fifth: take main's wording for shared\""),
        "the healed commit says what changed, where it came from, and what was dropped: {third}"
    );
    for (commit, subject) in [&healed[0], &healed[1], &healed[3]] {
        assert_eq!(
            description_of(&jj, &store, commit).trim_end(),
            subject.as_str(),
            "a commit the heal did not change carries no note"
        );
    }
    assert_eq!(
        String::from_utf8_lossy(&file_show(&jj, &store, &healed[2].0, "shared.rs").unwrap()),
        "MAIN-SIDE plus branch\n",
        "the resolution lives in the commit where the conflict arose"
    );
    assert_eq!(commit_summary(&jj, &store, &healed[3].0), "A four.rs");
    assert_eq!(
        String::from_utf8_lossy(&file_show(&jj, &store, branch, "shared.rs").unwrap()),
        "MAIN-SIDE plus branch\n",
        "and the tip holds exactly the tree it held before the heal"
    );
}

/// Component A, the pinned failure mode: WITHOUT the target flatten, folding a
/// child into the conflicted hub would carry a conflicted commit into the
/// integration branch. That fold now stops at its export — the folded commit's
/// tree carries jj conflict scaffolding, so the integration git ref never moves
/// onto it — well before the push that used to be the first refusal (`Won't
/// push commit ... since it has conflicts`).
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
    // Unguarded on purpose: this is the pre-flatten wedge itself, which
    // `rebase_branch_onto` now rolls back rather than lands.
    rebase_recording_conflict(&hub.jj, &hub.store, child, hub.int);
    let checkout = resolve_backing_checkout(&hub.store).expect("backing checkout");
    let int_ref = format!("refs/heads/{}", hub.int);
    let int_ref_before = git_stdout(&checkout, &["rev-parse", &int_ref]);

    let err = merge_into_bookmark(&hub.jj, &hub.store, hub.int, child)
        .expect_err("a fold onto conflicted ancestry never reaches git");
    assert!(is_conflict_scaffolding_export_refusal(&err), "{err}");
    assert!(err.contains("JJ-CONFLICT-README"), "{err}");
    assert_eq!(
        git_stdout(&checkout, &["rev-parse", &int_ref]),
        int_ref_before,
        "the integration git ref did not move onto the scaffolded fold"
    );

    // Nothing reached git, so there is nothing a push could publish either.
    let err = push_store_bookmark(&hub.jj, &hub.store, hub.int).unwrap_err();
    assert!(
        err.to_lowercase().contains("conflict"),
        "the push is refused for the conflicted ancestor: {err}"
    );
}

/// The repair is never a guess. Where the two sides hold genuinely different
/// histories, settling on either one silently discards the other's commits — the
/// exact failure this whole arc exists to prevent — so it reports and refuses.
#[test]
#[serial_test::serial(jj)]
fn a_genuinely_divergent_branch_is_reported_rather_than_settled() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping divergent_branch_is_reported: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-2-builder-1";

    let exported = store_child_of(&fx, "main", "agent work");
    create_bookmark_at(&fx.jj, &fx.store, branch, &exported).unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "test agent branch export").unwrap();
    let store_side = store_child_of(&fx, &exported, "store-only work");
    set_bookmark_at(&fx.jj, &fx.store, branch, &store_side).unwrap();
    // A sibling of the store's tip rather than an ancestor or descendant of it.
    let git_side = advance_origin_from(&fx, &exported, "stranger.txt", "unrelated\n");
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), &git_side],
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());

    let before = operation_id(&fx.jj, &fx.store).unwrap();
    let error = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .expect_err("divergent targets must not be settled by picking one");

    let text = error.to_string();
    assert!(text.contains(&store_side), "{text}");
    assert!(text.contains(&git_side), "{text}");
    assert_eq!(
        reconcile_failure_kind(&text),
        "conflicted_bookmark",
        "{text}"
    );
    assert!(
        bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap(),
        "a refused repair must leave both sides intact"
    );
    assert_eq!(
        operation_id(&fx.jj, &fx.store).unwrap(),
        before,
        "a refusal writes nothing"
    );

    // The refusal carries a maintainer's way out: inspect, then for each
    // ordering, keep one version and replay the other's own commits onto it.
    let refusal = error.refusal().expect("a refusal, not a jj failure");
    assert!(
        refusal.reason.contains("neither version is a rewrite"),
        "{}",
        refusal.reason
    );
    let store = fx.store.to_string_lossy();
    let commands = refusal.commands.join("\n");
    assert!(
        commands.contains(&format!(
            "jj -R '{store}' --ignore-working-copy log -r 'bookmarks(exact:\"{branch}\") | {exported}'"
        )),
        "{commands}"
    );
    // `git_side` is origin's `main`: it has no commits of its own, so the
    // ordering that would replay it (rewriting `main`) is withheld, and only
    // the store side's own commit is replayed onto it.
    let foreign =
        format!("::(bookmarks(~exact:\"{branch}\") | remote_bookmarks(~exact:\"{branch}\"))");
    assert!(
        commands.contains(&format!(
            "jj -R '{store}' --ignore-working-copy rebase -r '({git_side}..{store_side}) ~ {foreign}' -d {git_side}"
        )),
        "{commands}"
    );
    assert!(
        commands.contains(&format!(
            "jj -R '{store}' --ignore-working-copy bookmark set '{branch}' --allow-backwards -r '{git_side}:: & change_id({})'",
            change_of(&fx, &store_side)
        )),
        "{commands}"
    );
    assert!(
        commands.contains(&format!(
            "# Not offered: keeping {store_side} and replaying {git_side}'s commits onto it"
        )),
        "{commands}"
    );
    assert!(
        !commands.contains(&format!("-d {store_side}")),
        "{commands}"
    );
    assert_no_rebase_touches_main(&fx, &refusal.commands);
    // The store side only adds a file, and the in-memory replay says so.
    assert!(
        commands.contains("(tried in memory: replays cleanly)"),
        "{commands}"
    );
    // Never a command that keeps one side and drops the other.
    assert!(
        !refusal
            .commands
            .iter()
            .any(|line| line.contains("bookmark set") && !line.contains("::")),
        "{commands}"
    );

    // The rebase page's read-only view of the same name says the same thing.
    let assessment = assess_conflicted_bookmark_name(&fx.jj, &fx.store, branch, None)
        .unwrap()
        .expect("the name is conflicted");
    assert_eq!(assessment.conflict, refusal.conflict);
    assert_eq!(
        assessment.outlook,
        ConflictedNameOutlook::Refused(Box::new(refusal.clone()))
    );
    assert!(assessment.bases.is_empty(), "no base branch was named");

    // And the offered repair works as written. `git_side` is origin's `main`,
    // which jj holds immutable, so the ordering that keeps it is the one a
    // maintainer can run here.
    let keep_git_side = refusal
        .commands
        .iter()
        .position(|line| line.contains(" rebase ") && line.ends_with(&format!("-d {git_side}")))
        .expect("an ordering keeping the git side");
    for line in &refusal.commands[keep_git_side..keep_git_side + 2] {
        run_offered_command(&fx, line);
    }
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    for side in [&store_side, &git_side] {
        assert!(
            revset_commit(
                &fx.jj,
                &fx.store,
                &format!("::{branch} & change_id({})", change_of(&fx, side))
            )
            .is_some(),
            "{side} survives the offered repair"
        );
    }
}

#[test]
#[serial_test::serial(jj)]
fn a_foreign_commit_imported_into_the_tracking_ref_is_never_superseded() {
    let Some((_home, _project, origin, workspaces, jj, store, branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let rewritten = rewrite_published_branch(&jj, &store, workspaces.path(), &branch);
    let outside = advance_origin_outside(origin.path(), &branch, "outside.rs");
    // Stale-publication recovery fetches where imports read, so the tracked
    // `<branch>@origin` then names the outside commit itself.
    fetch_remote_branch_via_git(&store, "origin", &branch).unwrap();
    import_git(&jj, &store).unwrap();
    assert_eq!(
        revset_commit(
            &jj,
            &store,
            &format!("tracked_remote_bookmarks(exact:{branch:?}, remote=exact:\"origin\")")
        ),
        Some(outside.clone())
    );
    assert!(
        !origin_copy_is_superseded(&jj, &store, &branch, &rewritten, &outside).unwrap(),
        "a commit the store never rewrote is foreign, whatever the tracking ref says"
    );
}

/// The fold's own contract says its export "fails the fold rather than silently
/// leaving a stale ref". A refused export exits 0, so before verification the
/// fold could satisfy that contract while leaving precisely the stale ref it
/// promises never to leave — and a later child provisioned off the integration
/// branch resolves its base through that ref and starts from the pre-merge tip.
///
/// With the expectation passed, the fold repairs the frozen ref instead.
#[test]
#[serial_test::serial(jj)]
fn a_fold_repairs_a_frozen_integration_ref_instead_of_reporting_success() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping fold_repairs_a_frozen_integration_ref: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    git(proj.path(), &["checkout", "-q", "--detach"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let integration = "agent/cairn-1-coordinator-0";
    let child = "agent/cairn-2-builder-0";
    let base = git_stdout(proj.path(), &["rev-parse", "refs/heads/main"]);
    create_bookmark_at(&jj, &store, integration, &base).unwrap();
    jj.run(
        &store,
        &[
            "new",
            integration,
            "-m",
            "child work",
            "--ignore-working-copy",
        ],
        "child seal",
    )
    .unwrap();
    let child_tip = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("heads({integration}::)"),
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "child tip",
        )
        .unwrap();
    create_bookmark_at(&jj, &store, child, &child_tip).unwrap();
    export_git_preserving_checkout(&jj, &store, true, "seed export").unwrap();

    // Something outside jj moves the integration ref to a commit jj did not put
    // there. The move must differ from BOTH what jj last exported (`base`) and the
    // commit the fold will land (`child_tip`), or there is nothing for jj to
    // refuse and nothing for the verifier to catch — an earlier version of this
    // fixture moved the ref to the value it already held and passed against the
    // unfixed code, proving nothing.
    git(proj.path(), &["checkout", "-q", "--detach", &base]);
    std::fs::write(proj.path().join("outside.rs"), "moved outside jj\n").unwrap();
    git(proj.path(), &["add", "-A"]);
    git(proj.path(), &["commit", "-q", "-m", "moved outside jj"]);
    let outside = git_stdout(proj.path(), &["rev-parse", "HEAD"]);
    assert_ne!(outside, base);
    assert_ne!(outside, child_tip);
    git(
        proj.path(),
        &["update-ref", &format!("refs/heads/{integration}"), &outside],
    );

    merge_into_bookmark(&jj, &store, integration, child).unwrap();

    assert_eq!(
        bookmark_commit(&jj, &store, integration).as_deref(),
        Some(child_tip.as_str())
    );
    assert_eq!(
        git_stdout(
            proj.path(),
            &["rev-parse", &format!("refs/heads/{integration}")]
        ),
        child_tip,
        "the fold must leave the integration ref at the folded tip, not the pre-merge one"
    );
}

/// An export that cannot be repaired is reported, never swallowed. The
/// transaction has already committed by then, so the commit is real — but a
/// caller that reads this as success republishes nothing and tells its agent the
/// work landed where it did not.
///
/// The unwritable ref is git's own directory/file rule: `refs/heads/feat` cannot
/// exist while `refs/heads/feat/inner` does.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_publication_reports_an_unrepairable_export_instead_of_success() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping publication_reports_an_unrepairable_export: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    git(proj.path(), &["checkout", "-q", "--detach"]);
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let base = bookmark_commit(&jj, &store, "main").unwrap();
    git(proj.path(), &["update-ref", "refs/heads/feat/inner", &base]);
    create_bookmark_at(&jj, &store, "feat", &base).unwrap();

    let published = publish_logical_head_exported(
        &jj,
        &store,
        "feat",
        &base,
        ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
            path: "blocked.rs".to_string(),
            content: Some(b"blocked\n".to_vec()),
        }]),
        None,
        crate::PublicationMode::Child {
            description: "a commit whose ref cannot be written".into(),
        },
    )
    .await
    .expect("the transaction itself succeeds; only its export leg is blocked");

    assert!(
        !published.landed.head.is_empty(),
        "the commit is real \u{2014} the transaction committed before the export ran"
    );
    let error = published
        .export
        .expect_err("a blocked export must not be reported as a published commit");
    assert!(
        is_export_freeze_error(&error),
        "expected the typed export-freeze family, got: {error}"
    );
    assert!(
        error.contains("feat"),
        "the error must name the branch: {error}"
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

/// Advance `main` with `shared.rs` set to `content`, through a workspace of
/// its own, as an out-of-band merge would.
fn advance_main_shared(jj: &JjEnv, store: &Path, wts: &Path, content: &str) {
    let oob = "agent/cairn-9-oob-0";
    let ws = wts.join("oob");
    add_workspace(jj, store, &ws, oob, "main", None).unwrap();
    std::fs::write(ws.join("shared.rs"), content).unwrap();
    seal(jj, &ws, "main advances shared", None).unwrap();
    let tip = bookmark_commit(jj, store, oob).unwrap();
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            &tip,
            "--ignore-working-copy",
        ],
        "advance main",
    )
    .unwrap();
}

/// Advance origin's copy of `branch` from a clone that is not the project
/// checkout, the way a pull request merged in GitHub's UI does: the project's
/// own tracking refs learn nothing.
fn advance_origin_outside(origin: &Path, branch: &str, file: &str) -> String {
    let outside = TempDir::new().unwrap();
    let clone = outside.path().join("c");
    git(
        outside.path(),
        &[
            "clone",
            "-q",
            "--branch",
            branch,
            &origin.to_string_lossy(),
            &clone.to_string_lossy(),
        ],
    );
    git(&clone, &["config", "user.email", "o@e.com"]);
    git(&clone, &["config", "user.name", "o"]);
    std::fs::write(clone.join(file), format!("{file}\n")).unwrap();
    git(&clone, &["add", "-A"]);
    git(&clone, &["commit", "-q", "-m", "merged outside Cairn"]);
    git(
        &clone,
        &["push", "-q", "origin", &format!("HEAD:refs/heads/{branch}")],
    );
    git_stdout(&clone, &["rev-parse", "HEAD"])
}

/// Every rebase a refusal offers replays only commits `main` does not hold:
/// evaluated against the store, each `-r` range intersected with `::main` is
/// empty.
fn assert_no_rebase_touches_main(fx: &TrackedOrigin, commands: &[String]) {
    for line in commands.iter().filter(|line| line.contains(" rebase -r '")) {
        let revset = line
            .split(" rebase -r '")
            .nth(1)
            .and_then(|rest| rest.split("' -d ").next())
            .unwrap_or_else(|| panic!("unparseable rebase: {line}"));
        let touched = fx
            .jj
            .run(
                &fx.store,
                &[
                    "log",
                    "-r",
                    &format!("({revset}) & ::main"),
                    "--no-graph",
                    "-T",
                    "commit_id ++ \"\\n\"",
                    "--ignore-working-copy",
                ],
                "offered rebase range against main",
            )
            .unwrap();
        assert!(
            touched.trim().is_empty(),
            "the offered `{line}` would rewrite main's commits: {touched}"
        );
    }
}

fn change_of(fx: &TrackedOrigin, commit: &str) -> String {
    fx.jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                commit,
                "--no-graph",
                "-T",
                "change_id",
                "--ignore-working-copy",
            ],
            "test change id",
        )
        .unwrap()
}

/// The paths `commit` changes relative to its parent, as `jj diff --summary`.
fn commit_summary(jj: &JjEnv, store: &Path, commit: &str) -> String {
    jj.run(
        store,
        &["diff", "-r", commit, "--summary", "--ignore-working-copy"],
        "commit summary",
    )
    .unwrap()
    .trim()
    .to_string()
}

/// `(commit id, subject)` of every commit in `range`, oldest first.
fn commits_oldest_first(jj: &JjEnv, store: &Path, range: &str) -> Vec<(String, String)> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            range,
            "--reversed",
            "--no-graph",
            "-T",
            "commit_id ++ \"\\x1f\" ++ description.first_line() ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "commits oldest first",
    )
    .unwrap()
    .lines()
    .filter_map(|line| line.split_once('\u{1f}'))
    .map(|(id, subject)| (id.to_string(), subject.to_string()))
    .collect()
}

/// The full description of the single commit `revset` names.
fn description_of(jj: &JjEnv, store: &Path, revset: &str) -> String {
    jj.run(
        store,
        &[
            "log",
            "-r",
            revset,
            "--no-graph",
            "-T",
            "description",
            "--ignore-working-copy",
        ],
        "description",
    )
    .unwrap()
}

/// Replay a published branch onto an advanced `main`, the way base advance
/// does: every commit gets a new id and origin keeps the pre-rewrite tip.
fn rewrite_published_branch(jj: &JjEnv, store: &Path, workspaces: &Path, branch: &str) -> String {
    advance_main_to(
        jj,
        store,
        workspaces,
        "advance",
        "advanced.rs",
        "advanced\n",
    );
    assert!(matches!(
        rebase_branch_onto(jj, store, branch, "main").unwrap(),
        RebaseOutcome::Rebased
    ));
    bookmark_commit(jj, store, branch).unwrap()
}

/// Run one line of a refusal's commands exactly as a maintainer would, through a
/// shell, with the store's managed jj environment and binary.
fn run_offered_command(fx: &TrackedOrigin, line: &str) {
    let line = line
        .strip_prefix("jj ")
        .unwrap_or_else(|| panic!("not a jj command: {line}"));
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(format!("'{}' {line}", fx.jj.binary_path()))
        .envs(fx.jj.shell_env())
        .status()
        .unwrap();
    assert!(status.success(), "offered command failed: jj {line}");
}

fn advance_main_to(jj: &JjEnv, store: &Path, wts: &Path, name: &str, path: &str, content: &str) {
    let bookmark = format!("agent/CAIRN-9-{name}-0");
    let workspace = wts.join(name);
    add_workspace(jj, store, &workspace, &bookmark, "main", None).unwrap();
    std::fs::write(workspace.join(path), content).unwrap();
    seal(jj, &workspace, name, None).unwrap();
    let tip = bookmark_commit(jj, store, &bookmark).unwrap();
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            &tip,
            "--ignore-working-copy",
        ],
        "advance main",
    )
    .unwrap();
}
