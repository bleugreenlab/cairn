use super::testing::*;
use super::*;
use crate::PublicationIdentity;
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

#[test]
#[serial_test::serial(jj)]
fn seal_amend_converts_to_child_when_bookmark_read_fails() {
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
    std::fs::write(ws.join("a.rs"), "one\n").unwrap();
    seal(&jj, &ws, "orig", None).unwrap();
    let original = bookmark_commit(&jj, &ws, branch).unwrap();

    std::fs::write(ws.join("b.rs"), "two\n").unwrap();
    fail_next_local_bookmarks_at();
    let result = seal(&jj, &ws, "^", None).unwrap();
    assert!(
        result.amend_note.as_deref().is_some_and(|note| {
            note.contains("converted to a new commit")
                && note.contains("injected local bookmark read failure")
        }),
        "the failed guard is surfaced: {:?}",
        result.amend_note
    );
    let child = bookmark_commit(&jj, &ws, branch).unwrap();
    assert_ne!(child, original, "the target was not rewritten");
    let parent = jj
        .run(
            &ws,
            &["log", "-r", "@--", "--no-graph", "-T", "commit_id"],
            "amend child parent",
        )
        .unwrap();
    assert_eq!(
        parent, original,
        "the fallback commit descends from the untouched target"
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

/// A sanctioned replay must distinguish the original same-hunk conflict from a
/// later tip commit that deliberately resolves it. The replay reapplies the
/// lineage, restores the committed resolution only for the session's conflicting
/// paths, and heals the conflict-flagged commit below in place: it keeps its
/// own message, gains a note naming what the heal changed, and the resolution
/// commit it absorbed is dropped and named.
#[test]
#[serial_test::serial(jj)]
fn committed_tip_resolution_lands_same_hunk_replay() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping committed_tip_resolution_lands_same_hunk_replay: jj not resolvable");
        return;
    };
    let fx = setup_conflicting_advance(&bin);

    let RebaseOutcome::Conflicted { diagnostic } =
        rebase_branch_onto(&fx.jj, &fx.store, fx.branch, "main").unwrap()
    else {
        panic!("precondition: the first replay must conflict");
    };
    let paths = diagnostic.conflicting_paths();

    update_stale(&fx.jj, &fx.workspace).unwrap();
    std::fs::write(fx.workspace.join("shared.rs"), "RESOLVED-34\n").unwrap();
    seal(
        &fx.jj,
        &fx.workspace,
        "resolve same-hunk base conflict",
        None,
    )
    .unwrap();

    // The base may advance again while the agent is resolving. Replay onto the
    // live destination and preserve that newer, non-conflicting content rather
    // than requiring the resolution session's old `theirs` coordinate.
    fx.jj
        .run(&fx.store, &["new", "main"], "advance main after resolution")
        .unwrap();
    std::fs::write(fx.store.join("after-resolution.rs"), "newer base\n").unwrap();
    fx.jj
        .run(
            &fx.store,
            &["describe", "-m", "newer base while resolution is pending"],
            "describe newer base",
        )
        .unwrap();
    fx.jj
        .run(
            &fx.store,
            &["bookmark", "set", "main", "-r", "@"],
            "advance main bookmark again",
        )
        .unwrap();

    let tip = bookmark_commit(&fx.jj, &fx.store, fx.branch).unwrap();
    let report = reconcile_resolved_sibling_without_publication(
        &fx.jj,
        &fx.store,
        "main",
        fx.branch,
        &ResolutionCarry::from_tip(&tip, &paths),
    )
    .unwrap();
    assert_eq!(report.rebased_clean, vec![fx.branch.to_string()]);
    assert!(report.conflicted.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&file_show(&fx.jj, &fx.store, fx.branch, "shared.rs").unwrap()),
        "RESOLVED-34\n"
    );
    assert_eq!(
        String::from_utf8_lossy(
            &file_show(&fx.jj, &fx.store, fx.branch, "after-resolution.rs").unwrap()
        ),
        "newer base\n"
    );
    let dest = bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let range = format!("{dest}..bookmarks(exact:{:?})", fx.branch);
    assert_eq!(count_commits(&fx.jj, &fx.store, &range), 1);
    assert!(conflicted_commits(&fx.jj, &fx.store, &range).is_empty());
    let message = description_of(
        &fx.jj,
        &fx.store,
        &format!("bookmarks(exact:{:?})", fx.branch),
    );
    assert!(
        message.starts_with("agent edits shared and adds its own work\n"),
        "the surviving commit is the agent's own, under its own message: {message}"
    );
    assert!(
        message.contains("Cairn conflict heal")
            && message.contains("`shared.rs`")
            && message.contains("\"resolve same-hunk base conflict\""),
        "the note names the healed path, the resolution, and the dropped commit: {message}"
    );
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

/// Seal one commit onto `bookmark` from the store side and move the bookmark.
fn store_commit_on(
    jj: &JjEnv,
    store: &Path,
    bookmark: &str,
    file: &str,
    content: &str,
    message: &str,
) {
    jj.run(store, &["new", bookmark], "new on bookmark")
        .unwrap();
    std::fs::write(store.join(file), content).unwrap();
    jj.run(store, &["describe", "-m", message], "describe")
        .unwrap();
    jj.run(
        store,
        &["bookmark", "set", bookmark, "-r", "@"],
        "move bookmark",
    )
    .unwrap();
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

    // The take-committed-tip replay: the tip is resolved, the third commit is not.
    assert!(matches!(
        rebase_branch_onto_with_resolution(
            &jj,
            &store,
            branch,
            "main",
            Some(&ResolutionCarry::from_tip(&tip, &paths)),
        )
        .unwrap(),
        RebaseOutcome::RebasedOverConflictedAncestry { .. }
    ));
    let dest = bookmark_commit(&jj, &store, "main").unwrap();
    let range = format!("{dest}..bookmarks(exact:{branch:?})");
    let replayed = commits_oldest_first(&jj, &store, &range);
    assert_eq!(replayed.len(), 5);

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
            && third.contains(&replayed[4].0[..12])
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

/// cairn/5326's integration-branch shape: a coordinator branch with a child PR
/// folded into it and a later commit of its own. A take-committed-tip replay of
/// the coordinator leaves the hub's first commit, the child's landing, and the
/// later commit conflict-flagged. The reconcile heals them in place: the
/// child's landing is still a commit of its own, still the child bookmark's
/// target, and still changes only the child's file. The coordinator is never
/// collapsed.
#[test]
#[serial_test::serial(jj)]
fn a_coordinator_with_a_folded_child_is_healed_without_collapsing_it() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_coordinator_with_a_folded_child_is_healed_without_collapsing_it: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/cairn-5326-coordinator-0";
    let ws_int = wts.path().join("coord");
    add_workspace(&jj, &store, &ws_int, int, "main", None).unwrap();
    std::fs::write(ws_int.join("shared.rs"), "HUB-SIDE\n").unwrap();
    seal(&jj, &ws_int, "hub rewords shared", None).unwrap();
    forget_workspace(&jj, &store, int).unwrap();

    let child = "agent/cairn-5327-builder-0";
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(ws_child.join("child.rs"), "child work\n").unwrap();
    seal(&jj, &ws_child, "child work", None).unwrap();
    forget_workspace(&jj, &store, child).unwrap();
    merge_into_bookmark(&jj, &store, int, child).unwrap();
    assert!(bookmark_landed_in(&jj, &store, child, int));
    // A second child, still in flight, cut from the first child's landing.
    let pending = "agent/cairn-5328-builder-0";
    let ws_pending = wts.path().join("pending");
    add_workspace(&jj, &store, &ws_pending, pending, int, None).unwrap();
    std::fs::write(ws_pending.join("pending.rs"), "pending work\n").unwrap();
    seal(&jj, &ws_pending, "pending child work", None).unwrap();
    forget_workspace(&jj, &store, pending).unwrap();
    store_commit_on(
        &jj,
        &store,
        int,
        "later.rs",
        "later\n",
        "hub after the child landed",
    );

    advance_main_shared(&jj, &store, wts.path(), "MAIN-SIDE\n");
    let RebaseOutcome::Conflicted { diagnostic } =
        rebase_branch_onto(&jj, &store, int, "main").unwrap()
    else {
        panic!("precondition: the advance conflicts on the hub's first commit");
    };
    let paths = diagnostic.conflicting_paths();
    store_commit_on(
        &jj,
        &store,
        int,
        "shared.rs",
        "MAIN-SIDE plus hub\n",
        "coordinator takes main's wording",
    );
    let tip = bookmark_commit(&jj, &store, int).unwrap();
    assert!(matches!(
        rebase_branch_onto_with_resolution(
            &jj,
            &store,
            int,
            "main",
            Some(&ResolutionCarry::from_tip(&tip, &paths)),
        )
        .unwrap(),
        RebaseOutcome::RebasedOverConflictedAncestry { .. }
    ));
    let dest = bookmark_commit(&jj, &store, "main").unwrap();
    let range = format!("{dest}..bookmarks(exact:{int:?})");
    assert!(
        conflicted_commits(&jj, &store, &range).len() >= 3,
        "precondition: the hub, the child's landing, and the later commit are all flagged"
    );

    let report =
        reconcile_siblings_without_publication(&jj, &store, "main", &[int.to_string()]).unwrap();
    assert_eq!(report.rebased_clean, vec![int.to_string()], "{report:?}");
    assert!(
        report.conflicted.is_empty() && report.failed.is_empty(),
        "{report:?}"
    );

    let healed = commits_oldest_first(&jj, &store, &range);
    let subjects: Vec<&str> = healed.iter().map(|(_, subject)| subject.as_str()).collect();
    assert_eq!(
        subjects,
        vec![
            "hub rewords shared",
            "child work",
            "hub after the child landed"
        ],
        "the coordinator keeps every commit, the child's landing included"
    );
    assert!(conflicted_commits(&jj, &store, &range).is_empty());
    assert_eq!(
        bookmark_commit(&jj, &store, child).as_deref(),
        Some(healed[1].0.as_str()),
        "the child bookmark still names its own landing"
    );
    assert!(bookmark_landed_in(&jj, &store, child, int));
    assert_eq!(commit_summary(&jj, &store, &healed[1].0), "A child.rs");
    assert_eq!(
        description_of(&jj, &store, &healed[1].0).trim_end(),
        "child work",
        "the child's landing was not changed, so it carries no note"
    );
    assert!(description_of(&jj, &store, &healed[0].0).contains("Cairn conflict heal:"));
    assert_eq!(
        String::from_utf8_lossy(&file_show(&jj, &store, int, "shared.rs").unwrap()),
        "MAIN-SIDE plus hub\n"
    );

    // The in-flight child the replay dragged onto the flagged lineage now sits
    // on the healed one: its parent is the healed landing, it is clean, and it
    // still changes only its own file.
    let pending_tip = bookmark_commit(&jj, &store, pending).unwrap();
    assert_eq!(
        commits_oldest_first(&jj, &store, &format!("{}..{pending_tip}", healed[1].0)),
        vec![(pending_tip.clone(), "pending child work".to_string())],
        "the pending child rides the healed landing, not an abandoned lineage"
    );
    assert!(!branch_has_conflict(&jj, &store, pending).unwrap());
    assert_eq!(commit_summary(&jj, &store, &pending_tip), "A pending.rs");
}

/// A descendant whose conflict is its OWN — a child that edited the very path
/// the coordinator's replay conflicts on — cannot be cleared by healing the
/// coordinator. Publishing would export that child as conflict scaffolding, so
/// the heal refuses and the whole replay rolls back: the coordinator returns to
/// the resolution its agent committed, and the child to where it was.
#[test]
#[serial_test::serial(jj)]
fn a_heal_that_would_leave_a_child_bookmark_conflicted_rolls_the_replay_back() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_heal_that_would_leave_a_child_bookmark_conflicted_rolls_the_replay_back: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/cairn-5326-coordinator-0";
    let ws_int = wts.path().join("coord");
    add_workspace(&jj, &store, &ws_int, int, "main", None).unwrap();
    std::fs::write(ws_int.join("shared.rs"), "HUB-SIDE\n").unwrap();
    seal(&jj, &ws_int, "hub rewords shared", None).unwrap();
    forget_workspace(&jj, &store, int).unwrap();
    let child = "agent/cairn-5329-builder-0";
    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(ws_child.join("shared.rs"), "CHILD-SIDE\n").unwrap();
    seal(&jj, &ws_child, "child rewords shared again", None).unwrap();
    forget_workspace(&jj, &store, child).unwrap();
    let child_before = bookmark_commit(&jj, &store, child).unwrap();
    store_commit_on(&jj, &store, int, "later.rs", "later\n", "hub moves on");

    advance_main_shared(&jj, &store, wts.path(), "MAIN-SIDE\n");
    let RebaseOutcome::Conflicted { diagnostic } =
        rebase_branch_onto(&jj, &store, int, "main").unwrap()
    else {
        panic!("precondition: the advance conflicts on the hub's first commit");
    };
    let paths = diagnostic.conflicting_paths();
    store_commit_on(
        &jj,
        &store,
        int,
        "shared.rs",
        "MAIN-SIDE plus hub\n",
        "coordinator resolves",
    );
    let tip = bookmark_commit(&jj, &store, int).unwrap();

    let report = reconcile_resolved_sibling_without_publication(
        &jj,
        &store,
        "main",
        int,
        &ResolutionCarry::from_tip(&tip, &paths),
    )
    .unwrap();
    assert_eq!(report.conflicted, vec![int.to_string()], "{report:?}");
    assert!(report.rebased_clean.is_empty(), "{report:?}");
    assert_eq!(
        bookmark_commit(&jj, &store, int).as_deref(),
        Some(tip.as_str()),
        "the coordinator is back on the resolution its agent committed"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, child).as_deref(),
        Some(child_before.as_str()),
        "and the child is back where it was, not published conflicted"
    );
    assert!(!branch_has_conflict(&jj, &store, child).unwrap());
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

/// Visible commits whose change id more than one visible commit shares.
fn divergent_commits(jj: &JjEnv, store: &Path) -> Vec<String> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            "divergent()",
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "divergent commits",
    )
    .unwrap()
    .lines()
    .map(ToOwned::to_owned)
    .collect()
}

fn is_visible(jj: &JjEnv, store: &Path, commit: &str) -> bool {
    count_commits(jj, store, &format!("{commit} & ::visible_heads()")) == 1
}

/// A replay that carries a committed resolution as TEXT builds that text in a
/// scratch commit. Built off the pre-replay tip, which the rebase has just
/// hidden, the scratch made the whole superseded stack visible again, and it
/// stayed behind as a divergent twin of the rebased branch once the scratch was
/// abandoned. A replay hides what it supersedes.
#[test]
#[serial_test::serial(jj)]
fn a_replay_carrying_a_text_resolution_leaves_no_twin_of_the_superseded_branch() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_replay_carrying_a_text_resolution: jj not resolvable");
        return;
    };
    let fx = setup_conflicting_advance(&bin);
    // Agents work in git worktrees, not jj workspaces; the fixture's workspace
    // would otherwise keep its own working copy's ancestry visible.
    forget_workspace(&fx.jj, &fx.store, fx.branch).unwrap();
    assert!(divergent_commits(&fx.jj, &fx.store).is_empty());

    let carry = ResolutionCarry {
        expected_pre_tip: fx.pre_tip.clone(),
        paths: vec![(
            "shared.rs".to_string(),
            CarriedContent::Text("RESOLVED\n".to_string()),
        )],
    };
    let outcome =
        rebase_branch_onto_with_resolution(&fx.jj, &fx.store, fx.branch, "main", Some(&carry))
            .unwrap();
    assert_eq!(outcome, RebaseOutcome::Rebased);
    assert_eq!(
        String::from_utf8_lossy(&file_show(&fx.jj, &fx.store, fx.branch, "shared.rs").unwrap()),
        "RESOLVED\n"
    );
    assert_eq!(
        divergent_commits(&fx.jj, &fx.store),
        Vec::<String>::new(),
        "the replay left a twin of the branch it superseded"
    );
    assert!(
        !is_visible(&fx.jj, &fx.store, &fx.pre_tip),
        "the pre-replay tip is still visible"
    );
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

    /// Advance `main` by one commit adding `file`.
    fn advance_main(&self, file: &str) -> String {
        let scratch = format!("agent/advance-{}", file.replace('.', "-"));
        let tip = self.seal_branch(&scratch, "main", file);
        self.jj
            .run(
                &self.store,
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
        self.jj
            .run(
                &self.store,
                &["bookmark", "delete", &scratch, "--ignore-working-copy"],
                "drop the advance bookmark",
            )
            .unwrap();
        tip
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

/// A bookmark conflicted between two replays of one branch onto successive base
/// tips (two concurrent operations, the shape the live store accumulated) is
/// settled on the replay onto the newer base, and the older replay is hidden.
#[test]
#[serial_test::serial(jj)]
fn the_store_sweep_settles_a_bookmark_conflicted_between_two_replays() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping the_store_sweep_settles_a_bookmark_conflicted: jj not resolvable");
        return;
    };
    let fx = sweep_fixture(&bin);
    let branch = "agent/cairn-7002-builder-0";
    fx.seal_branch(branch, "main", "own.rs");
    let first = fx.advance_main("first.rs");
    let second = fx.advance_main("second.rs");
    let fork = operation_id(&fx.jj, &fx.store).unwrap();
    fx.jj
        .run(
            &fx.store,
            &[
                "rebase",
                "-b",
                branch,
                "-o",
                &second,
                "--ignore-working-copy",
            ],
            "replay onto the newer base",
        )
        .unwrap();
    fx.jj
        .run(
            &fx.store,
            &[
                "--at-op",
                &fork,
                "rebase",
                "-b",
                branch,
                "-o",
                &first,
                "--ignore-working-copy",
            ],
            "concurrent replay onto the older base",
        )
        .unwrap();
    let conflicted = fx
        .jj
        .run(
            &fx.store,
            &[
                "bookmark",
                "list",
                "--conflicted",
                "--ignore-working-copy",
                "-T",
                "name ++ \"\\n\"",
            ],
            "conflicted bookmarks",
        )
        .unwrap();
    assert!(
        conflicted.contains(branch),
        "precondition: the bookmark is conflicted: {conflicted:?}"
    );
    assert_eq!(divergent_commits(&fx.jj, &fx.store).len(), 2);

    let sweep = sweep_store_view(&fx.jj, &fx.store, &["main".to_string()], Duration::ZERO).unwrap();
    assert_eq!(sweep.bookmarks_resolved.len(), 1, "{sweep:?}");
    let settled = bookmark_commit(&fx.jj, &fx.store, branch).unwrap();
    assert_eq!(settled, sweep.bookmarks_resolved[0].kept);
    assert_eq!(
        count_commits(&fx.jj, &fx.store, &format!("{second} & ::{settled}")),
        1,
        "the kept version sits on the newer base"
    );
    assert!(!is_visible(
        &fx.jj,
        &fx.store,
        &sweep.bookmarks_resolved[0].dropped[0]
    ));
    assert_eq!(divergent_commits(&fx.jj, &fx.store), Vec::<String>::new());
}

/// Retiring a finished branch's origin tracking ref drops `<branch>@origin` and
/// nothing else: a local bookmark at the remote's commit, and one that moved on
/// past it, both stay where they were and neither is conflicted (an import of
/// the deleted ref without the untrack would delete the first and conflict the
/// second). A branch only the remote held becomes unreferenced history the view
/// sweep hides, and a branch the caller did not name keeps its tracking ref.
#[test]
#[serial_test::serial(jj)]
fn retiring_finished_tracking_refs_drops_only_the_remote_bookmarks() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping retiring_finished_tracking_refs: jj not resolvable");
        return;
    };
    let fx = sweep_fixture(&bin);
    let project = fx._proj.path();
    let same = "agent/cairn-7003-builder-0";
    let moved = "agent/cairn-7004-builder-0";
    let gone = "agent/cairn-7005-builder-0";
    let unfinished = "agent/cairn-7006-builder-0";
    let mut tips = std::collections::HashMap::new();
    for (branch, file) in [
        (same, "same.rs"),
        (moved, "moved.rs"),
        (gone, "gone.rs"),
        (unfinished, "unfinished.rs"),
    ] {
        let tip = fx.seal_branch(branch, "main", file);
        git(
            project,
            &["update-ref", &format!("refs/remotes/origin/{branch}"), &tip],
        );
        tips.insert(branch, tip);
    }
    import_git(&fx.jj, &fx.store).unwrap();
    let store_cmd = |args: &[&str], context: &str| {
        let mut args = args.to_vec();
        args.push("--ignore-working-copy");
        fx.jj.run(&fx.store, &args, context).unwrap()
    };
    store_cmd(
        &["bookmark", "track", "glob:agent/*", "--remote=origin"],
        "track the pushed branches",
    );
    let moved_tip = fx.stray(&tips[moved], "local work past origin");
    store_cmd(
        &["bookmark", "set", moved, "-r", &moved_tip],
        "move the local bookmark past origin",
    );
    store_cmd(&["bookmark", "delete", gone], "delete the local bookmark");
    store_cmd(&["git", "export"], "export the local bookmarks");

    let finished = [same, moved, gone, "agent/cairn-7007-never-pushed"].map(String::from);
    let before = operation_id(&fx.jj, &fx.store).unwrap();
    let retired = retire_remote_tracking_refs(&fx.jj, &fx.store, "origin", &finished).unwrap();
    assert_eq!(
        retired,
        RetiredTrackingRefs {
            operation_before: before,
            refs: [same, moved, gone]
                .map(|branch| RefTarget {
                    name: format!("refs/remotes/origin/{branch}"),
                    oid: tips[branch].clone(),
                })
                .to_vec(),
            locked: 0,
        },
        "each retired ref is reported with the object that re-creates it"
    );

    let has_ref = |name: &str| {
        cairn_platform::env::git()
            .args(["rev-parse", "--verify", "--quiet", name])
            .current_dir(project)
            .output()
            .unwrap()
            .status
            .success()
    };
    for branch in [same, moved, gone] {
        assert!(
            !has_ref(&format!("refs/remotes/origin/{branch}")),
            "{branch}"
        );
    }
    assert!(has_ref(&format!("refs/remotes/origin/{unfinished}")));
    let origin_bookmarks = store_cmd(
        &[
            "bookmark",
            "list",
            "--all-remotes",
            "-T",
            "if(remote == \"origin\", name ++ \"\\n\", \"\")",
        ],
        "origin bookmarks",
    );
    assert_eq!(origin_bookmarks.trim(), unfinished);
    assert_eq!(
        store_cmd(
            &["bookmark", "list", "--conflicted"],
            "conflicted bookmarks"
        )
        .trim(),
        ""
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, same).unwrap(),
        tips[same]
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, moved).unwrap(),
        moved_tip
    );
    assert!(has_ref(&format!("refs/heads/{same}")));
    assert!(has_ref(&format!("refs/heads/{moved}")));

    sweep_store_view(&fx.jj, &fx.store, &["main".to_string()], Duration::ZERO).unwrap();
    assert!(
        !is_visible(&fx.jj, &fx.store, &tips[gone]),
        "history only the retired tracking ref held is hidden"
    );
    for kept in [&tips[same], &moved_tip, &tips[unfinished]] {
        assert!(is_visible(&fx.jj, &fx.store, kept), "{kept}");
    }

    let again = retire_remote_tracking_refs(&fx.jj, &fx.store, "origin", &finished).unwrap();
    assert_eq!(again, RetiredTrackingRefs::default());
}

/// Tree identity sees through a stale base coordinate; the changed-file diff
/// does not. This is the VCS-level fact the turn-end zero-delta gate rests on
/// (CAIRN-3108), reproduced against real jj rather than asserted in prose.
///
/// The topology is job 7d9755b2's: the node's branch has been brought level with
/// a main that advanced after the node started, so its TREE is main's, but its
/// recorded `base_commit` still points at the older main. Diffing against that
/// row reports every intervening change and fires the full review suite on a
/// node that changed nothing.
///
/// Note the node's head is deliberately a DIFFERENT COMMIT from main's tip with
/// the same tree, so a gate that compared coordinates instead of trees would
/// fail this even with a perfectly fresh base row.
#[test]
#[serial_test::serial(jj)]
fn tree_identity_sees_through_a_stale_base_commit_that_the_diff_does_not() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping tree_identity_sees_through_a_stale_base_commit: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // The base coordinate the node recorded when it started.
    let stale_base = bookmark_commit(&jj, &store, "main").unwrap();

    // main advances underneath it, the way a merged PR advances it in practice.
    let advancing = wts.path().join("advancing");
    add_workspace(&jj, &store, &advancing, "agent/advance", "main", None).unwrap();
    std::fs::write(advancing.join("shared.rs"), "advanced\n").unwrap();
    seal(&jj, &advancing, "advance main", None).unwrap();
    let new_main = head_commit(&jj, &advancing).unwrap();
    jj.run(
        &store,
        &[
            "bookmark",
            "set",
            "main",
            "-r",
            &new_main,
            "--allow-backwards",
        ],
        "advance main for the stale-base fixture",
    )
    .unwrap();

    // The node reaches main's content on its own branch: same tree, own commit.
    let node = wts.path().join("node");
    add_workspace(&jj, &store, &node, "agent/zero-delta", &stale_base, None).unwrap();
    std::fs::write(node.join("shared.rs"), "advanced\n").unwrap();
    seal(&jj, &node, "reach main's content", None).unwrap();
    let node_head = head_commit(&jj, &node).unwrap();

    assert_ne!(
        node_head, new_main,
        "the node must sit at its OWN commit, so this tests trees and not coordinates"
    );

    // The trap: diffed against the stale row, this zero-delta node looks like a
    // real change, and the changed-file gate selects checks for it.
    let changed = logical_changed_files(&jj, &store, &stale_base, &node_head)
        .expect("the stale-base range is resolvable");
    assert!(
        !changed.is_empty(),
        "a stale base coordinate makes a zero-delta node look changed — this is the bug the \
         tree gate exists to survive, so if this ever goes empty the fixture stopped reproducing it"
    );

    // The fact the gate uses: content is identical, whatever the row says.
    let node_tree = logical_tree_hash(&jj, &store, &node_head).unwrap();
    let main_tree = logical_tree_hash(&jj, &store, &new_main).unwrap();
    let stale_tree = logical_tree_hash(&jj, &store, &stale_base).unwrap();
    assert_eq!(
        node_tree, main_tree,
        "the node's tree is byte-identical to the live base branch's"
    );
    assert_ne!(
        node_tree, stale_tree,
        "and differs from the stale coordinate's, which is exactly why resolving the base tree \
         from the BRANCH rather than the recorded commit is what makes the gate correct"
    );
}

/// A branch that merged its base in instead of being rebased onto it. The fork
/// point is the merged base commit, and the branch's own commit behind the
/// merge does not descend from it, so jj refuses to diff the revset
/// `fork..tip` ("Cannot diff revsets with gaps in"). The logical change is a
/// diff between trees and answers anyway: the branch's own file, and nothing
/// the base contributed. An unreadable diff says what jj said.
#[test]
#[serial_test::serial(jj)]
fn logical_changes_of_a_tip_that_merged_its_base_are_the_branchs_own() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping logical_changes_of_a_tip_that_merged_its_base: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let node = wts.path().join("node");
    add_workspace(&jj, &store, &node, "agent/merged", "main", None).unwrap();
    std::fs::write(node.join("branch.rs"), "branch\n").unwrap();
    seal(&jj, &node, "branch work", None).unwrap();
    let own = head_commit(&jj, &node).unwrap();

    let advancing = wts.path().join("advancing");
    add_workspace(&jj, &store, &advancing, "agent/advance", "main", None).unwrap();
    std::fs::write(advancing.join("staging.rs"), "staging\n").unwrap();
    seal(&jj, &advancing, "advance main", None).unwrap();
    let base = head_commit(&jj, &advancing).unwrap();

    jj.run(
        &store,
        &[
            "new",
            "--no-edit",
            "--ignore-working-copy",
            &own,
            &base,
            "-m",
            "Merge main into the branch",
        ],
        "merge the base into the branch",
    )
    .unwrap();
    let tip = revset_commit(&jj, &store, &format!("children({own}) & children({base})"))
        .expect("the merge commit resolves");

    let refused = jj
        .run(
            &store,
            &[
                "diff",
                "--ignore-working-copy",
                "--git",
                "-r",
                &format!("{base}..{tip}"),
            ],
            "range diff over the merged tip",
        )
        .expect_err("jj refuses the gapped range, or this fixture no longer reproduces the defect");
    assert!(refused.contains("gaps"), "{refused}");

    let changed = logical_changed_files(&jj, &store, &base, &tip)
        .expect("the merged tip's logical change resolves");
    let paths: Vec<&str> = changed.iter().map(|file| file.path.as_str()).collect();
    assert_eq!(paths, ["branch.rs"]);

    let unreadable = logical_changed_files(&jj, &store, "deadbeefdeadbeef", &tip)
        .expect_err("a revision that does not exist cannot be diffed");
    assert!(
        unreadable.contains("deadbeefdeadbeef") && unreadable.contains("doesn't exist"),
        "the error carries jj's own message: {unreadable}"
    );
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

/// The same transient contention, met on the export leg instead of the import
/// one. This is the leg that moves `refs/heads/<branch>` in the backing
/// checkout, and `publish_logical_head_exported` may not report a publication as
/// committed unless it succeeds — so an impatient export rolls back a patch that
/// already applied cleanly. That is the failure agents actually reported.
#[test]
#[serial_test::serial(jj)]
fn export_waits_for_a_held_then_released_packed_refs_lock() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping packed-refs export retry test: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "packed-refs-export";
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
    create_bookmark_at(&jj, &store, branch, &tip).unwrap();
    git(proj.path(), &["pack-refs", "--all"]);

    let lock = proj.path().join(".git").join("packed-refs.lock");
    std::fs::write(&lock, "held by test\n").unwrap();
    let release = lock.clone();
    let holder = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(1500));
        std::fs::remove_file(release).unwrap();
    });

    // Deliberately the UNVERIFIED export. It names no expectation, so the
    // freeze-repair path cannot rescue it and patience is the only thing that
    // can make the ref land — which is also the shape of every export site that
    // publishes without naming what it publishes.
    let started = std::time::Instant::now();
    export_git_preserving_checkout(&jj, &store, true, "patient export")
        .expect("export must retry until the ordinary Git lock is released");
    holder.join().unwrap();
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the fixture must actually have made the export wait"
    );
    // The load-bearing assertion. `jj git export` reports a refused ref on
    // stderr and still exits 0, so without patience this export "succeeds"
    // while `refs/heads/<branch>` never comes into existence at all.
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &format!("refs/heads/{branch}")]),
        tip,
        "the patient export must actually publish the bookmark"
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
    let author = PublicationIdentity::new("Alice", "alice@example.com");

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
        cairn_platform::env::git()
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
    let author_a = PublicationIdentity::new("Alice", "alice@example.com");
    let author_b = PublicationIdentity::new("Bob", "bob@example.com");
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

    let error = logical_tree_hash(&jj, &store, &"f".repeat(40)).unwrap_err();
    assert!(
        error.contains("refusing to record check evidence"),
        "an unverifiable revision must fail closed instead of substituting its commit id: {error}"
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
    push_store_bookmark(&jj, &store, overlap).unwrap();
    push_store_bookmark(&jj, &store, clean).unwrap();

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

    // Store-sync follows the production staged shape: network transfer updates
    // the ordinary Git repository, then the locked/local jj import observes it.
    git(proj.path(), &["fetch", "-q", "origin"]);
    ensure_project_store(&jj, &store, proj.path()).unwrap();
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
    let overlap_before = bookmark_commit(&jj, &store, overlap).unwrap();

    // First reconcile: the clean sibling rebases onto the externally-advanced
    // tip; the overlapping one conflicts and is rolled back.
    let report =
        reconcile_siblings(&jj, &store, dest, &[overlap.to_string(), clean.to_string()]).unwrap();
    assert_eq!(report.conflicted, vec![overlap.to_string()]);
    assert_eq!(report.rebased_clean, vec![clean.to_string()]);

    // The cleanly-rebased sibling's PR head advanced on origin.
    let clean_origin_after = git_stdout(origin.path(), &["rev-parse", clean]);
    assert_ne!(
        clean_origin_before, clean_origin_after,
        "reconcile pushes the cleanly-rebased sibling's advanced tip to origin"
    );
    assert!(
        !branch_has_conflict(&jj, &store, overlap).unwrap(),
        "the conflicting rebase was rolled back, so nothing conflict-flagged is left"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, overlap).unwrap(),
        overlap_before,
        "the conflicting sibling is exactly where it was"
    );
    assert!(!branch_has_conflict(&jj, &store, clean).unwrap());

    // The conflicted sibling's commit id after the first reconcile.
    let commit_overlap_after_first = bookmark_commit(&jj, &store, overlap).unwrap();

    // Second reconcile at the SAME tip (the double-fire): a `jj rebase` no-op,
    // so the conflicted commit id is unchanged. The before/after wake guard
    // reads exactly this equality to suppress a redundant wake.
    git(proj.path(), &["fetch", "-q", "origin"]);
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let report2 =
        reconcile_siblings(&jj, &store, dest, &[overlap.to_string(), clean.to_string()]).unwrap();
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
/// bookmark instead of handing it to `jj rebase -b`, whose revset is empty for
/// an ancestor bookmark.
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
    add_workspace(&jj, &store, &wts.path().join("idle"), idle, int, None).unwrap();
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

    let specs = vec![idle.to_string()];
    let report = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report.rebased_clean, vec![idle.to_string()]);
    assert!(report.conflicted.is_empty());
    assert_eq!(bookmark_commit(&jj, &store, idle).unwrap(), dest_commit);

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
        overlap.to_string(),
        clean_a.to_string(),
        clean_b.to_string(),
    ];

    // First reconcile: overlap conflicts, the other two land clean.
    let report1 = reconcile_siblings(&jj, &store, int, &specs).unwrap();
    assert_eq!(report1.conflicted, vec![overlap.to_string()]);
    assert_eq!(
        report1.rebased_clean,
        vec![clean_a.to_string(), clean_b.to_string()]
    );
    assert!(
        !branch_has_conflict(&jj, &store, overlap).unwrap(),
        "the conflicting rebase was rolled back; the branch stays on its own content"
    );

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
/// HEALED by the next reconcile, never dragged back onto a conflicted copy.
/// After the base advance conflicts the overlapping child, the agent resolves
/// the markers and re-seals — but that leaves the conflicted rebase commit as a
/// conflicted INTERMEDIATE in the history, so the branch is still unmergeable
/// (jj refuses a conflicted history). The reconcile-time heal moves the
/// resolution into that commit, preserving the agent's resolved TREE and the
/// commit's own identity, so the branch becomes genuinely mergeable. The
/// resolution is preserved (never regenerated as a conflict) and the resolving
/// commit it emptied is dropped (no empty debris, no divergent twin).
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

    let specs = vec![overlap.to_string()];

    // The clean-tip-over-conflicted-intermediate shape this heals, built the way
    // it actually arrives now. A Cairn reconcile no longer produces it —
    // `rebase_branch_onto` rolls a conflicting rebase back — so a branch carrying
    // a conflicted commit comes from a store predating that guard, or from `jj`
    // run outside Cairn. Flatten recovery still has to clear it.
    rebase_recording_conflict(&jj, &store, overlap, int);
    assert!(branch_has_conflict(&jj, &store, overlap).unwrap());

    // The agent resolves the conflict and re-seals: the bookmark advances to a
    // CLEAN commit on top of the conflicted rebase.
    update_stale(&jj, &ws_overlap).unwrap();
    std::fs::write(ws_overlap.join("shared.rs"), "resolved-by-agent\n").unwrap();
    seal(&jj, &ws_overlap, "resolve base conflict", None).unwrap();
    assert!(
        !branch_has_conflict(&jj, &store, overlap).unwrap(),
        "the re-seal resolves the conflict; the bookmark is clean"
    );
    let resolved_cid = change_id_of(&jj, &store, overlap);
    let dest = bookmark_commit(&jj, &store, int).unwrap();
    let overlap_cid = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("roots({dest}..bookmarks(exact:{overlap:?}))"),
                "--no-graph",
                "-T",
                "change_id",
                "--ignore-working-copy",
            ],
            "overlap change id",
        )
        .unwrap();

    // The next reconcile HEALS the resolved-but-conflicted-intermediate branch:
    // it already descends from the dest, but its history still carries the
    // conflicted rebase commit (unmergeable), so the reconcile resolves that
    // commit in place — preserving the resolved tree, never regenerating a
    // conflict.
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
    // The branch is now genuinely mergeable: the heal cleared the conflicted
    // intermediate from its history.
    assert!(
        conflicted_commits(
            &jj,
            &store,
            &format!("{dest}..bookmarks(exact:{overlap:?})")
        )
        .is_empty(),
        "the heal cleared the conflicted intermediate — the branch is mergeable"
    );
    assert_eq!(
        change_id_of(&jj, &store, overlap),
        overlap_cid,
        "the branch is its own original commit, resolved in place, not a fresh collapse"
    );
    assert!(
        description_of(&jj, &store, &format!("bookmarks(exact:{overlap:?})"))
            .starts_with("overlap edits shared\n"),
        "and it keeps its own message"
    );
    // The agent's resolved TREE is preserved though the commit was rewritten.
    assert_eq!(
        String::from_utf8_lossy(&file_show(&jj, &store, overlap, "shared.rs").unwrap()),
        "resolved-by-agent\n",
        "the heal preserves the agent's resolved content"
    );
    // The resolving commit the heal emptied is dropped: no empty debris.
    assert_eq!(
        visible_commits_for_change(&jj, &store, &resolved_cid),
        0,
        "the emptied resolving commit is abandoned"
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

    let specs = vec![child.to_string()];

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
/// captured inside the rebase and carried out on the report — the detail threaded
/// into the stop-the-line note so the agent knows exactly where to look.
///
/// This is the assertion that pins the capture ORDER. The rebase is rolled back,
/// so after `reconcile_siblings` returns there is no conflict left in the store
/// to enumerate; if the paths were read at notification time they would always be
/// empty and the note would name no files at all.
#[test]
#[serial_test::serial(jj)]
fn conflicting_paths_survive_the_rollback_on_the_report() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping conflicting_paths_survive_the_rollback_on_the_report: jj not resolvable"
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

    let child_before = bookmark_commit(&jj, &store, child).unwrap();

    // The reconcile attempts the rebase, sees the conflict, and rolls it back.
    let report = reconcile_siblings(&jj, &store, int, &[child.to_string()]).unwrap();
    assert_eq!(report.conflicted, vec![child.to_string()]);
    assert_eq!(
        report
            .conflict_diagnostics
            .get(child)
            .map(|diagnostic| diagnostic.conflicting_paths()),
        Some(vec!["shared.rs".to_string()]),
        "the conflicting path rides out on the report"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, child).unwrap(),
        child_before,
        "the conflicting rebase was rolled back; the branch never moved"
    );
    assert!(
        !branch_has_conflict(&jj, &store, child).unwrap(),
        "nothing conflict-flagged survives in the store — which is exactly why the \
         paths had to be captured inside the rebase"
    );
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
    // Unguarded on purpose: a reconcile now rolls a conflicting rebase back, so a
    // conflicted commit has to be constructed directly for the enumerator to have
    // anything to enumerate. (What the reconcile carries out instead is asserted
    // by `conflicting_paths_survive_the_rollback_on_the_report`.)
    rebase_recording_conflict(&jj, &store, child, int);

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
    let specs = vec![overlap.to_string()];
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

/// `branch_carries_commit` decides whether a branch still owes a replay, and
/// both of its consumers act on a `true` by doing nothing: the sessionless
/// replay returns "nothing to replay", and `cairn:~/rebase` tells the agent
/// nothing needs doing. A wrong `true` therefore reproduces the exact failure
/// this probe exists to detect — a branch silently behind its base with an
/// unmergeable PR and no signal anywhere — so the case that matters most is a
/// sibling left behind by a base advance.
#[test]
#[serial_test::serial(jj)]
fn branch_carries_commit_tracks_a_base_advance() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping branch_carries_commit_tracks_a_base_advance: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let lands = "agent/CAIRN-1-builder-0";
    let stranded = "agent/CAIRN-2-builder-0";
    let ws_lands = wts.path().join("lands");
    let ws_stranded = wts.path().join("stranded");
    add_workspace(&jj, &store, &ws_lands, lands, "main", None).unwrap();
    add_workspace(&jj, &store, &ws_stranded, stranded, "main", None).unwrap();

    let original_main = revset_commit(&jj, &store, "main").unwrap();
    assert!(
        branch_carries_commit(&jj, &store, stranded, &original_main),
        "a branch cut from main carries main's tip before anything moves"
    );

    // Its own work does not cost it the base it already had.
    std::fs::write(ws_stranded.join("mine.rs"), "my work\n").unwrap();
    seal(&jj, &ws_stranded, "my work", None).unwrap();
    assert!(
        branch_carries_commit(&jj, &store, stranded, &original_main),
        "committing on the branch keeps the base in its ancestry"
    );

    // A sibling lands and main advances. This is the state that must read as
    // "behind": the branch is untouched, still green, and now unmergeable.
    std::fs::write(ws_lands.join("theirs.rs"), "their work\n").unwrap();
    seal(&jj, &ws_lands, "their work", None).unwrap();
    merge_into_bookmark(&jj, &store, "main", lands).unwrap();
    let advanced_main = revset_commit(&jj, &store, "main").unwrap();
    assert_ne!(advanced_main, original_main, "main actually moved");

    assert!(
        !branch_carries_commit(&jj, &store, stranded, &advanced_main),
        "the stranded branch does NOT carry the advanced base, and must not be told otherwise"
    );
    assert!(
        branch_carries_commit(&jj, &store, lands, &advanced_main),
        "the branch that landed does carry it"
    );

    // Fail closed rather than claiming a base is carried on an unresolvable
    // input: the safe direction is to offer a replay that turns out redundant.
    assert!(!branch_carries_commit(&jj, &store, "", &advanced_main));
    assert!(!branch_carries_commit(
        &jj,
        &store,
        "agent/nonexistent",
        &advanced_main
    ));
    assert!(!branch_carries_commit(&jj, &store, stranded, ""));
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
    rebase_recording_conflict(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT);
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
/// A flatten refused after THIS pass rebased the branch puts the branch back
/// where the pass found it, and says truthfully why: a guard refusal is a
/// conflict with paths, a jj failure is a retried failure. Before, both left
/// the branch on the rebased tip under a `conflicted` label naming nothing
/// (cairn/4987's 11:07 specimen, where the cause was a packed-ref lock).
#[test]
#[serial_test::serial(jj)]
fn a_refused_heal_rolls_the_rebase_back_and_says_why() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_refused_heal_rolls_the_rebase_back_and_says_why: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-4987-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();
    let sibling = "agent/CAIRN-4987-builder-0";
    let ws = wts.path().join("sib");
    add_workspace(&jj, &store, &ws, sibling, int, None).unwrap();
    std::fs::write(ws.join("shared.rs"), "sibling-edit\n").unwrap();
    seal(&jj, &ws, "sibling edits shared", None).unwrap();

    let advance = |file: &str, content: &str| {
        jj.run(&store, &["new", int], "new on int").unwrap();
        std::fs::write(store.join(file), content).unwrap();
        jj.run(&store, &["describe", "-m", "int advances"], "describe")
            .unwrap();
        jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
            .unwrap();
    };
    // Give the sibling conflicted history with a resolving tip.
    advance("shared.rs", "integration-advanced\n");
    rebase_recording_conflict(&jj, &store, sibling, int);
    update_stale(&jj, &ws).unwrap();
    std::fs::write(ws.join("shared.rs"), "resolved\n").unwrap();
    seal(&jj, &ws, "resolve conflict", None).unwrap();
    // A further, unrelated advance that this pass rebases the sibling onto.
    advance("unrelated.rs", "later\n");
    let dest = bookmark_commit(&jj, &store, int).unwrap();
    let pre_tip = bookmark_commit(&jj, &store, sibling).unwrap();

    for (refusal, expect_conflict) in [
        (
            format!("{ANCESTRY_HEAL_GUARD} the healed tip does not hold the tree of its pre-heal tip"),
            true,
        ),
        (
            "jj restore (ancestry heal) failed: The lock for the packed-ref file could not be obtained"
                .to_string(),
            false,
        ),
    ] {
        let rollback = RebaseRollback {
            pre_op: operation_id(&jj, &store).unwrap(),
            pre_tip: Some(pre_tip.clone()),
        };
        assert!(matches!(
            rebase_branch_onto(&jj, &store, sibling, int).unwrap(),
            RebaseOutcome::RebasedOverConflictedAncestry { .. }
        ));
        assert_ne!(bookmark_commit(&jj, &store, sibling).unwrap(), pre_tip);

        let mut report = ReconcileReport::default();
        roll_back_refused_heal(
            &jj,
            &store,
            sibling,
            &dest,
            &rollback,
            refusal.clone(),
            &mut report,
        );
        assert_eq!(
            bookmark_commit(&jj, &store, sibling).as_deref(),
            Some(pre_tip.as_str()),
            "the branch is back where the pass found it ({refusal})"
        );
        if expect_conflict {
            assert_eq!(report.conflicted, vec![sibling.to_string()]);
            let diagnostic = &report.conflict_diagnostics[sibling];
            assert_eq!(
                diagnostic.conflicting_paths(),
                vec!["shared.rs".to_string()]
            );
            assert_eq!(diagnostic.ours.as_deref(), Some(pre_tip.as_str()));
            assert!(report.failed.is_empty());
        } else {
            assert!(report.conflicted.is_empty(), "a lock is not a conflict");
            assert_eq!(report.failed.len(), 1);
            assert!(report.failed[0].error.contains("packed-ref"));
            let kind = reconcile_failure_kind(&report.failed[0].error);
            assert_eq!(kind, REF_LOCK_CONTENTION);
            assert!(
                !reconcile_failure_is_permanent(kind),
                "so the durable worker retries it"
            );
        }
    }
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
        "jj restore (ancestry heal) failed: The lock for the packed-ref file could not be obtained\n\
         packed-refs lock: contention — held by live process 77 (git) after 30s"
            .to_string(),
    );
    assert!(lock.conflicted.is_empty(), "a lock is not a conflict");
    assert_eq!(lock.failed.len(), 1);
    assert_eq!(
        reconcile_failure_kind(&lock.failed[0].error),
        REF_LOCK_CONTENTION
    );

    let mut guard = ReconcileReport::default();
    record_unmoved_heal_refusal(
        &mut guard,
        branch,
        format!("{ANCESTRY_HEAL_GUARD} commit abc is still conflict-flagged after the heal"),
    );
    assert_eq!(
        guard.conflicted,
        vec![branch.to_string()],
        "a guard refusal is still a finding about the tree"
    );
    assert!(guard.failed.is_empty());
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
    // reported failure — even when the failure was a guard refusal.
    let branch = "agent/cairn-4994-builder-0";
    let mut report = ReconcileReport::default();
    record_unmoved_heal_refusal(
        &mut report,
        branch,
        format!(
            "{ANCESTRY_HEAL_GUARD} commit abc is still conflict-flagged after the heal\n{}",
            rollback_incomplete(
                "the refused heal of `b` could not be undone to operation op1: lock"
            )
        ),
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

#[test]
#[serial_test::serial(jj)]
fn reconcile_siblings_heals_intermediate_only_sibling() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_siblings_heals_intermediate_only_sibling: jj not resolvable");
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
    push_store_bookmark(&jj, &store, sibling).unwrap();
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
    rebase_recording_conflict(&jj, &store, sibling, int);
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

    // The reconcile heals the sibling (already-on-dest path), classifies it
    // clean, and pushes the healed tip to origin.
    let report = reconcile_siblings(&jj, &store, int, &[sibling.to_string()]).unwrap();
    assert_eq!(report.rebased_clean, vec![sibling.to_string()]);
    assert!(report.conflicted.is_empty());

    let range = format!("{dest}..bookmarks(exact:{sibling:?})");
    assert_eq!(
        count_commits(&jj, &store, &range),
        1,
        "the sibling's own commit survives; the resolving commit it absorbed is dropped"
    );
    assert!(
        description_of(&jj, &store, &format!("bookmarks(exact:{sibling:?})"))
            .starts_with("sibling edits shared\n"),
        "under its own message"
    );
    assert!(conflicted_commits(&jj, &store, &range).is_empty());
    assert!(!branch_has_conflict(&jj, &store, sibling).unwrap());

    let origin_after = git_stdout(origin.path(), &["rev-parse", sibling]);
    assert_ne!(
        origin_before, origin_after,
        "the healed sibling's PR head advanced on origin"
    );
    // A healed (clean) bookmark pushes; the wedge is gone.
    assert!(
        jj.run(
            &store,
            &["git", "push", "--remote", "origin", "--bookmark", sibling],
            "re-push healed",
        )
        .is_ok(),
        "the healed sibling is pushable"
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
    rebase_recording_conflict(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT);
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

/// The GIT half of the same proof. Moving a rider in the store is only half of
/// "this bookmark has a proven surviving target": until `refs/heads/<rider>`
/// actually holds the flattened commit, the survivor exists only in jj's view
/// while the backing ref still names the lineage the twin cleanup is about to
/// abandon — and the next `jj git import` reads that ref back, resurrecting the
/// orphaned conflicted line the re-point exists to retire. So the export verdict
/// gates the abandon exactly as the bookmark move does.
///
/// The freeze is forced with git's own directory/file rule (`refs/heads/X`
/// cannot exist while `refs/heads/X/inner` does), the unrepairable shape
/// [`an_unrepairable_export_freeze_is_an_error_not_a_success`] pins. Discard the
/// verdict and this test fails in the loudest possible way: the flatten reports
/// success, having abandoned lineage on a proof it never obtained.
#[test]
#[serial_test::serial(jj)]
fn flatten_refuses_to_abandon_when_a_rider_ref_never_reaches_git() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping flatten_refuses_to_abandon_when_a_rider_ref_never_reaches_git: jj not resolvable"
        );
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let dest = advance_int_conflicting(&fx.jj, &fx.store, "int-advanced\n");
    rebase_recording_conflict(&fx.jj, &fx.store, DIV_SIBLING, DIV_INT);

    let rider = "agent/CAIRN-4616-planner-0";
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

    // Occupy `refs/heads/<rider>` as a directory so no export can ever write it.
    // Whatever a prior export published there is cleared first; the block is what
    // makes the freeze unrepairable either way.
    let checkout = resolve_backing_checkout(&fx.store).expect("the fixture store has a checkout");
    let main_commit = git_stdout(&checkout, &["rev-parse", "refs/heads/main"]);
    let _ = cairn_platform::env::git()
        .args(["update-ref", "-d", &format!("refs/heads/{rider}")])
        .current_dir(&checkout)
        .status();
    git(
        &checkout,
        &[
            "update-ref",
            &format!("refs/heads/{rider}/inner"),
            &main_commit,
        ],
    );

    let pre_tip = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    let pre_lineage = format!("{dest}..bookmarks(exact:{DIV_SIBLING:?})");
    let pre_conflicted = conflicted_commits(&fx.jj, &fx.store, &pre_lineage);
    assert!(
        !pre_conflicted.is_empty(),
        "the fixture must have a conflicted intermediate for the flatten to collapse"
    );

    let error = flatten_branch_recovery(&fx.jj, &fx.store, DIV_SIBLING, &dest, "flattened")
        .expect_err(
            "a rider whose ref never reached git is not a proven surviving target, so the flatten \
             must refuse rather than abandon its lineage",
        );
    assert!(
        error.contains(rider),
        "the refusal must name the bookmark that could not be proven: {error}"
    );
    assert!(
        is_export_freeze_error(&error),
        "the refusal must carry the typed export-freeze cause: {error}"
    );

    // Fail-closed: the branch is back on its pre-flatten tip and NOTHING was
    // abandoned, so the whole lineage is still there for a later attempt.
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap(),
        pre_tip,
        "the refused flatten restored the branch to its pre-flatten tip"
    );
    assert_eq!(
        conflicted_commits(&fx.jj, &fx.store, &pre_lineage),
        pre_conflicted,
        "no candidate lineage was abandoned"
    );
    // The branch's restore is proven in BOTH halves — the backing ref came back
    // with the store, so nothing is left publishing the rejected flatten.
    assert_eq!(
        git_stdout(
            &checkout,
            &["rev-parse", &format!("refs/heads/{DIV_SIBLING}")]
        ),
        pre_tip,
        "the restored branch reached git, not just the store"
    );

    // The rider is put BACK on the commit it rode, not left on the flattened one.
    // Left there it would be a child of the dest, hence outside `dest..branch`,
    // hence invisible to a later attempt's rider enumeration — while its git ref
    // still named a commit on the lineage that attempt would abandon.
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, rider).as_deref(),
        Some(conflicted_intermediate.as_str()),
        "the refused flatten put the rider back on the commit it rode"
    );
    // Its own ref is the one this fixture blocked, so it can never reach git here.
    // Asserting the block held keeps the test honest about which half it proved.
    assert!(
        cairn_platform::env::git()
            .args(["rev-parse", "--verify", &format!("refs/heads/{rider}")])
            .current_dir(&checkout)
            .output()
            .map(|out| !out.status.success())
            .unwrap_or(true),
        "the blocked rider ref must still be unwritable — otherwise the export this \
         test forces to fail could have succeeded"
    );
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

/// The squash moves the bookmark sideways onto a fresh commit, which hides
/// nothing by itself; the branch's old commits are then reachable from no
/// bookmark and must not stay visible.
#[test]
#[serial_test::serial(jj)]
fn squash_branch_onto_hides_the_history_it_replaced() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping squash_branch_onto_hides_the_history_it_replaced: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/CAIRN-2002-builder-0";
    let ws = wts.path().join("src");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    for i in 1..=3 {
        std::fs::write(ws.join(format!("change{i}.rs")), format!("change {i}\n")).unwrap();
        seal(&jj, &ws, &format!("change {i}"), None).unwrap();
    }
    forget_workspace(&jj, &store, branch).unwrap();
    let old: Vec<String> = jj
        .run(
            &store,
            &[
                "log",
                "-r",
                &format!("main..bookmarks(exact:{branch:?})"),
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "old branch commits",
        )
        .unwrap()
        .lines()
        .map(ToOwned::to_owned)
        .collect();
    assert_eq!(old.len(), 3);

    squash_branch_onto(&jj, &store, branch, "main", "Squashed PR title").unwrap();

    for commit in &old {
        assert!(
            !is_visible(&jj, &store, commit),
            "{commit} of the replaced history is still visible"
        );
    }
    assert_eq!(
        count_commits(&jj, &store, &format!("main..bookmarks(exact:{branch:?})")),
        1
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

    let source_before = bookmark_commit(&jj, &store, source).unwrap();

    let err = rebase_then_fold_into(&jj, &store, "main", source, "main").unwrap_err();
    assert!(
        !err.to_lowercase().contains("allow-backwards"),
        "the conflict error must never surface the dangerous --allow-backwards hint: {err}"
    );
    assert!(
        err.contains("shared.rs"),
        "the refusal names the file that did not merge: {err}"
    );
    assert!(
        !err.to_lowercase().contains("marker"),
        "agents hold detached git worktrees, so there are no markers to point them at: {err}"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, "main").unwrap(),
        main_before,
        "the default bookmark is left unchanged — never moved backward on a conflict"
    );

    // The refusal's central claim, asserted rather than promised: the source is
    // bit-identical to its pre-merge self, carries no recorded conflict, and its
    // backing git ref still holds the agent's own content.
    assert_eq!(
        bookmark_commit(&jj, &store, source).unwrap(),
        source_before,
        "the refused merge left the source exactly where it was"
    );
    assert!(
        !branch_has_conflict(&jj, &store, source).unwrap(),
        "no conflict was left on the source for anything to export"
    );
    let source_ref = format!("refs/heads/{source}");
    assert_eq!(
        git_stdout(proj.path(), &["rev-parse", &source_ref]),
        source_before,
        "the backing git ref stayed on the pre-merge tip"
    );
    assert_eq!(
        git_stdout(proj.path(), &["show", &format!("{source_ref}:shared.rs")]),
        "source-change",
        "the source's own content survived; the destination side never landed on it"
    );
    let _ = &ws_src;
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

    let int = "agent/cairn-1940-coordinator-0";
    let child_a = "agent/cairn-1-builder-0";
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
    let child_b = "agent/cairn-2-builder-0";
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
    let int = "agent/cairn-1987-coordinator-0";
    let child = "agent/cairn-1988-builder-0";
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

    let branch = "agent/cairn-2081-builder-0";
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
    rebase_recording_conflict(&jj, &store, branch, "main");
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
    add_workspace(&jj, &store, &ws, "agent/cairn-1-builder-0", "main", None).unwrap();

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
    add_workspace(&jj, &store, &ws, "agent/cairn-1-builder-0", "main", None).unwrap();

    // A clean seal succeeds normally (the no-false-positive baseline).
    std::fs::write(ws.join("a.txt"), "v1\n").unwrap();
    let ok = seal_paths(
        &jj,
        &ws,
        "seal1",
        None,
        &[],
        Some("agent/cairn-1-builder-0"),
    )
    .expect("a clean seal succeeds");
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
            "agent/cairn-1-builder-0",
            "-r",
            "@-",
            "--ignore-working-copy",
        ],
        "repoint bookmark to live twin",
    )
    .unwrap();

    std::fs::write(ws.join("b.txt"), "v2\n").unwrap();
    let err = seal_paths(
        &jj,
        &ws,
        "seal2",
        None,
        &[],
        Some("agent/cairn-1-builder-0"),
    )
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

    let int = "agent/cairn-1-coordinator-0";
    let child = "agent/cairn-2-builder-0";
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

    // Bookmark/revset diagnostics must remain truthful even while the working
    // copy is stale; resolving over the store must not try to snapshot `@`.
    assert_eq!(
        bookmark_commit(&jj, &ws_coord, &int),
        Some(int_tip.clone()),
        "an existing bookmark resolves from a stale workspace"
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
/// WS3: a `^` amend whose target commit `@-` is SHARED with a sibling bookmark
/// is converted into a regular child commit — the shared commit is never
/// rewritten, only the workspace's own bookmark advances, and the foreign
/// bookmark stays put. The conversion is surfaced on `SealResult.amend_note`.
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

    let branch = "agent/cairn-1-builder-0";
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
    wts: TempDir,
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
        wts: TempDir::new().unwrap(),
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

/// THE defect: an operator squash-merges a PR on GitHub, so origin advances onto
/// a commit id the store has never seen while the store's local `main` sits
/// elsewhere. The import that observes it CONFLICTS the bookmark name, and every
/// `main`-resolving verb — job spawn included — dies until someone repairs it by
/// hand.
///
/// Note what this also proves: the import ALONE does not resolve the conflict.
/// The reflex recorded in the incident dossier repaired a narrower case; the
/// repair that closes this one is the bookmark move plus the export that follows
/// it.
#[test]
#[serial_test::serial(jj)]
fn reconcile_clears_a_conflicted_default_bookmark_after_a_github_squash() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping reconcile_clears_a_conflicted_default_bookmark: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);

    // Both sides move off the same base: the store folds locally, GitHub squashes.
    let folded = fold_local_main(&fx, "cairn fold: land child");
    let squashed = advance_origin_from(&fx, &fx.base, "gh.txt", "squash\n");
    assert_ne!(folded, squashed);

    // Observing origin is what conflicts the name, and re-importing does not undo it.
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(
        bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap(),
        "a local fold plus an independent origin advance must conflict the bookmark name"
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(
        bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap(),
        "a second import does not by itself resolve the conflicted name"
    );

    let outcome = reconcile_tracked_bookmark(&fx.jj, &fx.store, "main").unwrap();
    assert!(
        matches!(&outcome, BookmarkReconciliation::Repaired { to, .. } if to == &squashed),
        "expected a repair onto origin's tip, got {outcome:?}"
    );

    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, "main").unwrap());
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(squashed.as_str())
    );
    // The backing git ref followed, or the next import re-conflicts the name and
    // the whole cycle starts again.
    assert_eq!(proj_ref(&fx, "main"), squashed);

    // The verb the conflicted name was killing: provisioning a job off `main`.
    let ws = fx.wts.path().join("spawned-job");
    add_workspace(
        &fx.jj,
        &fx.store,
        &ws,
        "agent/cairn-1-builder-0",
        "main",
        None,
    )
    .expect("a job must spawn off `main` once the bookmark is reconciled");
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

/// Drive an AGENT branch into jj's conflicted-name state the way the incidents
/// did: the store advances the bookmark while the export is frozen, and the
/// backing git ref moves outside jj. Both sides leave the last exported point,
/// so the next import records them as competing targets.
///
/// Returns the store-side tip and the git-side target. The store side is a
/// DESCENDANT of the git side here, which is the shape every observed occurrence
/// had: no commit on either side is outside the other's history.
fn conflict_agent_branch(fx: &TrackedOrigin, branch: &str) -> (String, String) {
    let exported = store_child_of(fx, "main", "agent work: first");
    create_bookmark_at(&fx.jj, &fx.store, branch, &exported).unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "test agent branch export").unwrap();
    assert_eq!(proj_ref(fx, branch), exported);

    // The store keeps advancing; the export does not follow (the freeze).
    let tip = store_child_of(fx, &exported, "agent work: second");
    set_bookmark_at(&fx.jj, &fx.store, branch, &tip).unwrap();

    // The git ref moves under jj, which is a thing Cairn itself does.
    let git_side = fx.base.clone();
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), &git_side],
    );

    import_git(&fx.jj, &fx.store).unwrap();
    assert!(
        bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap(),
        "the fixture must actually conflict `{branch}`"
    );
    (tip, git_side)
}

/// THE stranding specimen, from the branch side. An agent branch whose name went
/// conflicted made its node completely inert: no run could hydrate a checkout,
/// so the agent could not even attempt a repair from inside. The repair settles
/// the name on the target that contains every other, which here is the store's
/// own tip — the agent's sealed work, kept.
#[test]
#[serial_test::serial(jj)]
fn a_conflicted_agent_branch_settles_on_the_target_that_contains_the_rest() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping conflicted_agent_branch_settles: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-1-builder-1";
    let (tip, git_side) = conflict_agent_branch(&fx, branch);
    assert_ne!(tip, git_side);

    let settled = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch).unwrap();

    assert_eq!(settled.as_deref(), Some(tip.as_str()));
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
        Some(tip.as_str())
    );
    // The git ref has to follow, or the next import re-conflicts the name and
    // the whole cycle starts again.
    assert_eq!(proj_ref(&fx, branch), tip);
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(
        !bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap(),
        "a repair the next import undoes is not a repair"
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

/// Visible commits of `revset`, for counting copies of a change.
fn visible_commits(fx: &TrackedOrigin, revset: &str) -> Vec<String> {
    fx.jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                &format!("({revset}) & ::visible_heads()"),
                "--no-graph",
                "-T",
                "commit_id ++ \"\\n\"",
                "--ignore-working-copy",
            ],
            "test visible commits",
        )
        .unwrap()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// Advance origin's `main` onto a new commit and let the store observe it, so
/// the store's `main` and `main@origin` both hold it. Returns the commit.
fn advance_main(fx: &TrackedOrigin, file: &str) -> String {
    let advanced = advance_origin_from(fx, &fx.base, file, "base advance\n");
    import_git(&fx.jj, &fx.store).unwrap();
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(advanced.as_str()),
        "the store's main follows origin"
    );
    advanced
}

/// THE twin-stack specimen (cairn/5195's builder branch). Version A holds two
/// commits on base N; version B holds the SAME two change ids, replayed by the
/// store onto N+1, plus a newer commit. Both are visible, because the git side
/// re-introduced A after the store rewrote it.
///
/// With `edit_twin`, B's copy of A's second commit gets a different
/// description, so it is no longer provably a copy. Returns `(a, b, n1)`.
fn drive_twin_specimen(
    fx: &TrackedOrigin,
    branch: &str,
    edit_twin: bool,
) -> (String, String, String) {
    let n1 = advance_main(fx, "n1.txt");
    let a1 = git_commit_on(fx, &fx.base, "a1.txt", "one\n", "A one");
    let a2 = git_commit_on(fx, &a1, "a2.txt", "two\n", "A two");
    import_commits(fx, &[&a1, &a2]);

    // Exported at a1; the store then holds a2.
    create_bookmark_at(&fx.jj, &fx.store, branch, &a1).unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "export a1").unwrap();
    set_bookmark_at(&fx.jj, &fx.store, branch, &a2).unwrap();
    park_working_copy(fx);

    // The store's replay onto N+1, then newer work on top.
    fx.jj
        .run(
            &fx.store,
            &["rebase", "-s", &a1, "-d", &n1, "--ignore-working-copy"],
            "replay A onto n1",
        )
        .unwrap();
    let twin_tip = bookmark_commit(&fx.jj, &fx.store, branch).unwrap();
    assert_eq!(change_of(fx, &twin_tip), change_of(fx, &a2));
    if edit_twin {
        fx.jj
            .run(
                &fx.store,
                &[
                    "describe",
                    "-r",
                    &twin_tip,
                    "-m",
                    "A two, reworded",
                    "--ignore-working-copy",
                ],
                "reword the twin",
            )
            .unwrap();
    }
    let twin_tip = bookmark_commit(&fx.jj, &fx.store, branch).unwrap();
    let b = store_child_of(fx, &twin_tip, "B three");
    park_working_copy(fx);
    set_bookmark_at(&fx.jj, &fx.store, branch, &b).unwrap();

    // The git side brings A back.
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), &a2],
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    assert_eq!(
        visible_commits(fx, &format!("change_id({})", change_of(fx, &a1))).len(),
        2,
        "both copies of A's first change are visible"
    );
    (a2, b, n1)
}

/// Acceptance for cairn/5210: on the specimen the page offers no rebase at all,
/// only "keep B, abandon A's twins", and the store settles it itself: B kept,
/// A's copies gone, `main` untouched, nothing copied a third time.
#[test]
#[serial_test::serial(jj)]
fn a_twin_stack_keeps_the_superset_version_and_abandons_the_twins() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping twin_stack_keeps_the_superset_version: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-5195-builder-5";
    let (a, b, n1) = drive_twin_specimen(&fx, branch, false);
    let a1_change = change_of(&fx, &format!("{a}-"));

    let assessment = assess_conflicted_bookmark_name(&fx.jj, &fx.store, branch, Some("main"))
        .unwrap()
        .expect("conflicted");
    let ConflictedNameOutlook::Settleable { how, commands } = &assessment.outlook else {
        panic!(
            "a twin stack nothing else references settles itself: {:?}",
            assessment.outlook
        );
    };
    assert!(
        how.contains(&format!("keep `{b}`")) && how.contains("abandon the 2 twin commit(s)"),
        "{how}"
    );
    assert!(commands.is_empty(), "{commands:?}");

    // The versions table can say which version is on the newer base.
    let base_of = |version: &str| {
        let index = assessment
            .conflict
            .added
            .iter()
            .position(|target| target.commit == version)
            .unwrap();
        assessment.bases[index].clone().expect("a base")
    };
    assert_eq!(base_of(&b).commit, n1);
    assert_eq!(base_of(&b).pull_request, Some(1));
    assert!(base_of(&b).newest);
    assert_eq!(base_of(&a).commit, fx.base);
    assert!(!base_of(&a).newest);

    // The by-hand commands are the same single ordering, and replay nothing.
    let refusal = repair_refusal(
        &fx.jj,
        &fx.store,
        assessment.conflict.clone(),
        String::new(),
    );
    assert!(
        !refusal
            .commands
            .iter()
            .any(|line| line.contains(" rebase ")),
        "{:#?}",
        refusal.commands
    );
    assert!(
        refusal
            .commands
            .iter()
            .any(|line| line.ends_with(&format!("abandon '{b}..{a}'"))),
        "{:#?}",
        refusal.commands
    );
    assert_no_rebase_touches_main(&fx, &refusal.commands);

    let settled = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .unwrap()
        .expect("settled");

    assert_eq!(settled, b);
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
        Some(b.as_str())
    );
    assert_eq!(proj_ref(&fx, branch), b);
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(n1.as_str()),
        "main is untouched"
    );
    assert_eq!(
        visible_commits(&fx, &format!("change_id({a1_change})")).len(),
        1,
        "one copy of each change remains, and it is B's"
    );
    assert!(
        visible_commits(&fx, &a).is_empty(),
        "A's copies are abandoned"
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(
        !bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap(),
        "a settlement the next import undoes is not a settlement"
    );
}

/// The same topology, but B's copy of a commit was reworded, so it is not a
/// proven copy and nothing is abandoned. The orderings that remain still never
/// rebase a `main` commit: replaying B onto A (whose range would include N+1)
/// is withheld as a newer base, and only A's own commits are offered onto B.
#[test]
#[serial_test::serial(jj)]
fn twins_that_differ_are_not_abandoned_and_no_ordering_rebases_main() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping twins_that_differ_are_not_abandoned: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-5195-builder-5";
    let (a, b, n1) = drive_twin_specimen(&fx, branch, true);

    // The range the page used to print for keeping A contains main's commit.
    assert_eq!(
        visible_commits(&fx, &format!("(fork_point({a}|{b})..{b}) & ::main")),
        vec![n1.clone()],
        "the fixture reproduces the specimen's shared-history range"
    );

    assert_refused_without_writing(&fx, branch, "neither version is a rewrite");
    let refusal = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .unwrap_err()
        .refusal()
        .cloned()
        .expect("a refusal");
    let commands = refusal.commands.join("\n");
    assert_no_rebase_touches_main(&fx, &refusal.commands);
    assert!(
        commands.contains(&format!(
            "# Not offered: keeping {a} and replaying {b}'s commits onto it, because `{b}` sits on \
             a newer base than `{a}`"
        )),
        "{commands}"
    );
    assert!(
        commands.contains(&format!("rebase -r '({b}..{a}) ~ "))
            && commands.contains(&format!("-d {b}")),
        "{commands}"
    );
    assert!(
        commands.contains("(tried in memory: replays cleanly)"),
        "{commands}"
    );
    assert!(!commands.contains(" abandon "), "{commands}");
}

/// Two versions with different work, one on a newer base. Keeping the older
/// one and replaying the newer used to sweep the base advance into the range;
/// now that ordering is withheld, and the one offered runs as printed without
/// moving `main`.
#[test]
#[serial_test::serial(jj)]
fn a_version_on_a_newer_base_is_never_replayed_with_its_base() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping version_on_a_newer_base: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-11-builder-0";
    let n1 = advance_main(&fx, "n1.txt");
    let a = git_commit_on(&fx, &fx.base, "a.txt", "a\n", "A's work");
    let b = git_commit_on(&fx, &n1, "b.txt", "b\n", "B's work");
    import_commits(&fx, &[&a, &b]);
    create_bookmark_at(&fx.jj, &fx.store, branch, &fx.base).unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "export the base").unwrap();
    set_bookmark_at(&fx.jj, &fx.store, branch, &b).unwrap();
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), &a],
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    park_working_copy(&fx);

    let error = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .expect_err("different work is never settled by picking one");
    let refusal = error.refusal().cloned().expect("a refusal");
    let commands = refusal.commands.join("\n");
    assert_no_rebase_touches_main(&fx, &refusal.commands);
    assert!(
        commands.contains(&format!(
            "# Not offered: keeping {a} and replaying {b}'s commits"
        )),
        "{commands}"
    );
    assert_eq!(commands.matches(" rebase ").count(), 1, "{commands}");

    let keep_b = refusal
        .commands
        .iter()
        .position(|line| line.contains(" rebase ") && line.ends_with(&format!("-d {b}")))
        .expect("an ordering keeping the newer-base version");
    for line in &refusal.commands[keep_b..keep_b + 2] {
        run_offered_command(&fx, line);
    }
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, "main").as_deref(),
        Some(n1.as_str()),
        "main is untouched"
    );
    for side in [&a, &b] {
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

/// Point `branch` at `commit` from the git side and import, so the store learns
/// the commit and the bookmark follows it.
fn point_branch_from_git(fx: &TrackedOrigin, branch: &str, commit: &str) {
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/heads/{branch}"), commit],
    );
    import_git(&fx.jj, &fx.store).unwrap();
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
        Some(commit)
    );
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
        delete_bookmark(&fx.jj, &fx.store, &format!("fixture-{n}")).unwrap();
    }
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "drop fixture bookmarks").unwrap();
}

/// The import as it ran before the guard: jj takes every tracking ref at its
/// word, backwards included.
fn raw_import(fx: &TrackedOrigin) {
    fx.jj
        .run(
            &fx.store,
            &["git", "import", "--ignore-working-copy"],
            "raw jj git import",
        )
        .unwrap();
}

fn tracking_ref(fx: &TrackedOrigin, branch: &str) -> String {
    git_stdout(
        fx.proj.path(),
        &["rev-parse", &format!("refs/remotes/origin/{branch}")],
    )
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

/// An agent branch pushed at `r`, then advanced to `c` (built on `r`) and
/// pushed again, so origin, its tracking ref, and the store all hold `c`.
fn push_branch_through_child(
    fx: &TrackedOrigin,
    branch: &str,
    parent: &str,
    child_file: (&str, &str),
) -> (String, String) {
    let r = git_commit_on(fx, parent, "r.txt", "r\n", "agent work R");
    point_branch_from_git(fx, branch, &r);
    push_store_bookmark(&fx.jj, &fx.store, branch).unwrap();
    let c = git_commit_on(fx, &r, child_file.0, child_file.1, "agent work C");
    point_branch_from_git(fx, branch, &c);
    push_store_bookmark(&fx.jj, &fx.store, branch).unwrap();
    assert_eq!(tracking_ref(fx, branch), c);
    (r, c)
}

/// Acceptance #1: the 20:50:48 write — origin's tracking ref for an agent branch
/// rewritten back to a commit the store had already pushed past — no longer
/// moves the branch.
#[test]
#[serial_test::serial(jj)]
fn a_stale_origin_observation_cannot_walk_a_managed_branch_back() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping stale_origin_observation_cannot_walk_back: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-4966-builder-0";
    let (r, c) = push_branch_through_child(&fx, branch, &fx.base, ("c.txt", "c\n"));

    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/remotes/origin/{branch}"), &r],
    );
    import_git(&fx.jj, &fx.store).unwrap();

    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
        Some(c.as_str()),
        "the branch keeps the commit the store already pushed"
    );
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("{branch}@origin")).as_deref(),
        Some(c.as_str())
    );
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("::visible_heads() & {c}")).as_deref(),
        Some(c.as_str()),
        "the newest commit stays visible"
    );
    assert_eq!(
        tracking_ref(&fx, branch),
        c,
        "the regressed tracking ref is restored, so no later import can act on it"
    );
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
}

/// The guard is judged on what the import recorded, not on a snapshot taken
/// before it: a fetch that writes the stale value after the guard looked, and
/// before jj reads the refs, is caught, restored, and re-imported, and the
/// commit the first import hid comes back.
#[test]
#[serial_test::serial(jj)]
fn a_stale_write_between_the_guard_and_the_import_is_still_held() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping stale_write_between_guard_and_import: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-4966-builder-0";
    let (r, c) = push_branch_through_child(&fx, branch, &fx.base, ("c.txt", "c\n"));
    park_working_copy(&fx);

    let wrote = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let (proj, wrote, r) = (fx.proj.path().to_path_buf(), wrote.clone(), r.clone());
        super::env::between_next_guard_and_import(move || {
            for refname in [
                format!("refs/remotes/origin/{branch}"),
                format!("refs/heads/{branch}"),
            ] {
                git(&proj, &["update-ref", &refname, &r]);
            }
            wrote.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }
    import_git(&fx.jj, &fx.store).unwrap();

    assert!(
        wrote.load(std::sync::atomic::Ordering::SeqCst),
        "the race was staged"
    );
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
        Some(c.as_str()),
        "the branch is carried forward again"
    );
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("{branch}@origin")).as_deref(),
        Some(c.as_str())
    );
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("::visible_heads() & {c}")).as_deref(),
        Some(c.as_str()),
        "the commit the first import hid is visible again"
    );
    assert_eq!(tracking_ref(&fx, branch), c);
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

/// The specimen, driven with the guard bypassed: returns `(r, c, rewrite)` with
/// the branch's name left conflicted exactly as the live store held it.
fn drive_the_specimen(
    fx: &TrackedOrigin,
    branch: &str,
    child_file: (&str, &str),
    advance_file: (&str, &str),
) -> (String, String, String) {
    let base = git_commit_on(fx, &fx.base, "shared.txt", "a\nb\nc\n", "shared base");
    git(
        fx.proj.path(),
        &["push", "-q", "origin", &format!("{base}:main")],
    );
    git(fx.proj.path(), &["fetch", "-q", "origin", "main"]);
    import_git(&fx.jj, &fx.store).unwrap();

    // 20:47 to 20:49: the builder's commit `c` reaches origin.
    let (r, c) = push_branch_through_child(fx, branch, &base, child_file);
    // The next base advance lands on origin.
    let advanced = git_commit_on(fx, &base, advance_file.0, advance_file.1, "base advance");
    git(
        fx.proj.path(),
        &["push", "-q", "origin", &format!("{advanced}:main")],
    );
    git(fx.proj.path(), &["fetch", "-q", "origin", "main"]);
    import_git(&fx.jj, &fx.store).unwrap();
    park_working_copy(fx);

    // 20:50:48: a stale advertisement writes the branch back to `r`, and the
    // 20:51:05 import, unguarded, follows it and hides `c`.
    for refname in [
        format!("refs/remotes/origin/{branch}"),
        format!("refs/heads/{branch}"),
    ] {
        git(fx.proj.path(), &["update-ref", &refname, &r]);
    }
    raw_import(fx);
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
        Some(r.as_str()),
        "without the guard the import walks the branch back"
    );

    // 20:52:08: the replay correctly rebases what the bookmark now holds.
    fx.jj
        .run(
            &fx.store,
            &[
                "rebase",
                "-b",
                branch,
                "-d",
                "main@origin",
                "--ignore-working-copy",
            ],
            "replay onto the advanced base",
        )
        .unwrap();
    export_git_preserving_checkout(&fx.jj, &fx.store, true, "export the replay").unwrap();
    let rewrite = bookmark_commit(&fx.jj, &fx.store, branch).unwrap();

    // 20:53:33: the next fetch brings origin's true tip back.
    git(
        fx.proj.path(),
        &["update-ref", &format!("refs/remotes/origin/{branch}"), &c],
    );
    raw_import(fx);
    (r, c, rewrite)
}

/// Acceptance #3: the specimen reproduces from its sequence, and the store
/// settles it by replaying the hidden child onto the rewrite.
#[test]
#[serial_test::serial(jj)]
fn the_specimen_sequence_wedges_and_then_settles() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping the_specimen_sequence_wedges_and_then_settles: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-4966-builder-0";
    let (r, c, rewrite) = drive_the_specimen(&fx, branch, ("c.txt", "c\n"), ("m.txt", "m\n"));

    // Exactly the live store's state.
    assert!(bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    let conflict = describe_conflicted_bookmark(&fx.jj, &fx.store, branch)
        .unwrap()
        .expect("conflicted");
    assert_eq!(
        conflict
            .removed
            .iter()
            .map(|t| t.commit.as_str())
            .collect::<Vec<_>>(),
        vec![r.as_str()]
    );
    let mut added = conflict
        .added
        .iter()
        .map(|t| t.commit.clone())
        .collect::<Vec<_>>();
    added.sort();
    let mut expected = vec![rewrite.clone(), c.clone()];
    expected.sort();
    assert_eq!(added, expected);
    assert_eq!(change_of(&fx, &rewrite), change_of(&fx, &r));
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("{branch}@git")).as_deref(),
        Some(rewrite.as_str())
    );
    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("{branch}@origin")).as_deref(),
        Some(c.as_str())
    );
    let outlook = assess_conflicted_bookmark_name(&fx.jj, &fx.store, branch, None)
        .unwrap()
        .unwrap()
        .outlook;
    assert!(
        matches!(
            &outlook,
            ConflictedNameOutlook::Settleable { how, commands }
                if how.contains("replay 1 commit(s)")
                    && commands.iter().any(|line| line.contains(&format!("'({r}..{c}) ~ ")))
        ),
        "{outlook:?}"
    );

    let settled = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .unwrap()
        .expect("settled");

    assert_eq!(
        revset_commit(&fx.jj, &fx.store, &format!("{settled}-")).as_deref(),
        Some(rewrite.as_str()),
        "the child now sits on the rewrite"
    );
    assert_eq!(change_of(&fx, &settled), change_of(&fx, &c));
    assert_eq!(
        git_stdout(fx.proj.path(), &["diff", &rewrite, &settled]),
        git_stdout(fx.proj.path(), &["diff", &r, &c]),
        "the replayed child carries exactly the child's own change"
    );
    assert!(!bookmark_name_is_conflicted(&fx.jj, &fx.store, branch).unwrap());
    assert_eq!(proj_ref(&fx, branch), settled);

    // Origin holds `c`, which the store still tracks, so the push is a
    // sideways move with a correct lease.
    push_store_bookmark(&fx.jj, &fx.store, branch).unwrap();
    assert_eq!(
        git_stdout(
            fx._origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        settled
    );
}

/// Stale-publication recovery meets the same state and settles it through the
/// shared function, leaving origin's copy tracked for the retried push.
#[test]
#[serial_test::serial(jj)]
fn converge_settles_the_specimen_through_the_shared_settlement() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping converge_settles_the_specimen: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-4966-builder-0";
    let (_r, c, rewrite) = drive_the_specimen(&fx, branch, ("c.txt", "c\n"), ("m.txt", "m\n"));

    let convergence =
        converge_managed_branch_after_remote_rewrite(&fx.jj, &fx.store, branch).unwrap();

    let ManagedBranchConvergence::Rebased { from, to, frontier } = convergence else {
        panic!("expected the settlement to be reported as a rebase, got {convergence:?}");
    };
    assert_eq!(from, c);
    assert_eq!(frontier, rewrite);
    assert_eq!(change_of(&fx, &to), change_of(&fx, &c));
    assert_eq!(bookmark_commit(&fx.jj, &fx.store, branch), Some(to.clone()));
    push_store_bookmark(&fx.jj, &fx.store, branch).unwrap();
    assert_eq!(
        git_stdout(
            fx._origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        to
    );
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

/// A suffix that already carries a conflict is never replayed: what it would
/// produce cannot be told apart from what the replay itself conflicted.
#[test]
#[serial_test::serial(jj)]
fn a_rewritten_base_whose_child_carries_a_conflict_is_refused() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping child_carries_a_conflict_is_refused: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-7-builder-0";
    let r = git_commit_on(&fx, &fx.base, "r.txt", "r\n", "R");
    let c1 = git_commit_on(&fx, &r, "new.txt", "one\n", "C1");
    let k0 = git_commit_on(&fx, &fx.base, "new.txt", "two\n", "K");
    let onto = git_commit_on(&fx, &fx.base, "onto.txt", "o\n", "onto");
    import_commits(&fx, &[&r, &c1, &k0, &onto]);
    fx.jj
        .run(
            &fx.store,
            &["rebase", "-r", &k0, "-d", &c1, "--ignore-working-copy"],
            "stack a conflicted commit on c1",
        )
        .unwrap();
    let k = revset_commit(&fx.jj, &fx.store, &format!("children({c1})")).unwrap();
    assert!(revset_has_conflict(&fx.jj, &fx.store, &k).unwrap());
    conflict_rewritten_base(&fx, branch, &r, &k, &onto);

    assert_refused_without_writing(&fx, branch, "already carry a conflict");
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

/// A replay that conflicts is rolled back and refused: resolving it is a
/// choice, and the store never makes one.
#[test]
#[serial_test::serial(jj)]
fn a_rewritten_base_whose_replay_conflicts_is_rolled_back_and_refused() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping replay_conflicts_is_refused: jj not resolvable");
        return;
    };
    let fx = setup_tracked_origin(&bin);
    let branch = "agent/cairn-9-builder-0";
    let base = git_commit_on(&fx, &fx.base, "shared.txt", "a\nb\nc\n", "shared");
    let r = git_commit_on(&fx, &base, "r.txt", "r\n", "R");
    let c = git_commit_on(&fx, &r, "shared.txt", "C\nb\nc\n", "C edits line one");
    let onto = git_commit_on(&fx, &base, "shared.txt", "M\nb\nc\n", "base edits line one");
    import_commits(&fx, &[&r, &c, &onto]);
    let rewrite = conflict_rewritten_base(&fx, branch, &r, &c, &onto);
    let before = describe_conflicted_bookmark(&fx.jj, &fx.store, branch).unwrap();

    let error = repair_conflicted_bookmark_name(&fx.jj, &fx.store, branch)
        .expect_err("a conflicting replay must be refused");

    let refusal = error
        .refusal()
        .unwrap_or_else(|| panic!("not a refusal: {error}"));
    assert!(refusal.reason.contains("conflicts"), "{}", refusal.reason);
    assert!(
        refusal
            .commands
            .join("\n")
            .contains("(tried in memory: conflicts)"),
        "{:?}",
        refusal.commands
    );
    assert_eq!(
        describe_conflicted_bookmark(&fx.jj, &fx.store, branch).unwrap(),
        before,
        "the trial replay is restored"
    );
    assert_eq!(
        revset_commit(
            &fx.jj,
            &fx.store,
            &format!("{rewrite}:: & change_id({})", change_of(&fx, &c))
        ),
        None,
        "no replayed copy of the child survives the rollback"
    );
    assert_eq!(proj_ref(&fx, branch), c);

    // The rebase page, read after the refusal, says the same: refused, with
    // the one combination that keeps both, flagged as expected to conflict.
    let outlook = assess_conflicted_bookmark_name(&fx.jj, &fx.store, branch, None)
        .unwrap()
        .expect("still conflicted")
        .outlook;
    let ConflictedNameOutlook::Refused(refusal) = outlook else {
        panic!("a replay known to conflict must not read as settleable: {outlook:?}");
    };
    assert!(
        refusal.reason.contains("conflicts when tried in memory"),
        "{}",
        refusal.reason
    );
    let commands = refusal.commands.join("\n");
    assert!(
        commands.contains("(tried in memory: conflicts)"),
        "{commands}"
    );
    assert!(
        commands.contains(&format!("rebase -r '({r}..{c}) ~ "))
            && commands.contains(&format!("-d {rewrite}")),
        "{commands}"
    );
    assert_eq!(
        commands.matches(" rebase ").count(),
        1,
        "replaying the rewrite's ancestry onto the child would drag the base along: {commands}"
    );
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
    let error = confirm_origin_tip(&fx.store, branch, &fx.base)
        .expect_err("origin has no such branch, which must not read as a successful publish");
    assert!(error.contains("no `"), "{error}");

    // Publish it, then confirm both the agreeing and the disagreeing verdicts.
    create_bookmark_at(&fx.jj, &fx.store, branch, &fx.base).unwrap();
    push_store_bookmark(&fx.jj, &fx.store, branch).unwrap();
    confirm_origin_tip(&fx.store, branch, &fx.base)
        .expect("origin's tip matches what was published");

    let squashed = advance_origin_from(&fx, &fx.base, "gh.txt", "other\n");
    let error = confirm_origin_tip(&fx.store, branch, &squashed)
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

    let error = confirm_origin_tip(&store, "main", &head).expect_err(
        "an unreachable origin leaves the publication unverified, which is not success",
    );

    assert!(
        error.contains("cannot confirm"),
        "the error must say the publication could not be confirmed: {error}"
    );
    assert!(error.contains("main"), "{error}");
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

/// A store whose agent branch carries BOTH an edit that will conflict with an
/// advanced base (`shared.rs`) and genuine unrelated work (`own.rs`), with the
/// base already advanced conflictingly.
///
/// The unrelated work is load-bearing to the fixture, not decoration: the
/// specimen this whole slice exists for had one file silently resolved to the
/// destination side while every other file on the branch stayed intact, so an
/// assertion that only checks "the branch still has work on it" would pass on
/// the broken behavior.
struct ConflictingAdvance {
    _home: TempDir,
    proj: TempDir,
    _wts: TempDir,
    jj: JjEnv,
    store: PathBuf,
    workspace: PathBuf,
    branch: &'static str,
    pre_tip: String,
}

fn setup_conflicting_advance(bin: &str) -> ConflictingAdvance {
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let branch = "agent/cairn-3197-builder-0";
    let ws = wts.path().join("agent");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("shared.rs"), "AGENT-SIDE\n").unwrap();
    std::fs::write(ws.join("own.rs"), "AGENT GENUINE WORK\n").unwrap();
    seal(&jj, &ws, "agent edits shared and adds its own work", None).unwrap();
    let pre_tip = bookmark_commit(&jj, &store, branch).unwrap();

    // The base advances out of band with a conflicting edit to the same file.
    let oob = "agent/cairn-9-oob-0";
    let ws_oob = wts.path().join("oob");
    add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
    std::fs::write(ws_oob.join("shared.rs"), "MAIN-SIDE\n").unwrap();
    seal(&jj, &ws_oob, "main advances shared", None).unwrap();
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

    ConflictingAdvance {
        _home: home,
        proj,
        _wts: wts,
        jj,
        store,
        workspace: ws,
        branch,
        pre_tip,
    }
}

/// The probe that separates BASE DRIFT from a content conflict, exercised over a
/// real store.
///
/// Everything downstream of the classification turns on this one question: do
/// the two sides actually disagree about the bytes of the conflicting paths? A
/// probe that answered "no" when they differ would tell an agent with real
/// merging to do that its work was already finished, so it is tested directly
/// rather than only through whatever fixture happens to reach it.
#[test]
fn the_base_drift_probe_answers_only_for_the_paths_it_is_given() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping the_base_drift_probe_answers_only_for_the_paths_it_is_given: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    // Two branches that AGREE on `shared.rs` and differ on `own.rs`.
    let left = "agent/cairn-3337-left-0";
    let ws_left = wts.path().join("left");
    add_workspace(&jj, &store, &ws_left, left, "main", None).unwrap();
    std::fs::write(ws_left.join("shared.rs"), "AGREED\n").unwrap();
    std::fs::write(ws_left.join("own.rs"), "LEFT\n").unwrap();
    seal(&jj, &ws_left, "left", None).unwrap();
    let left_tip = bookmark_commit(&jj, &store, left).unwrap();

    let right = "agent/cairn-3337-right-0";
    let ws_right = wts.path().join("right");
    add_workspace(&jj, &store, &ws_right, right, "main", None).unwrap();
    std::fs::write(ws_right.join("shared.rs"), "AGREED\n").unwrap();
    std::fs::write(ws_right.join("own.rs"), "RIGHT\n").unwrap();
    seal(&jj, &ws_right, "right", None).unwrap();
    let right_tip = bookmark_commit(&jj, &store, right).unwrap();

    assert!(
        !paths_differ_between(
            &jj,
            &store,
            &left_tip,
            &right_tip,
            &["shared.rs".to_string()]
        ),
        "the two sides agree on shared.rs, which is what base drift looks like"
    );
    assert!(
        paths_differ_between(&jj, &store, &left_tip, &right_tip, &["own.rs".to_string()]),
        "a genuine content difference must never be classified as drift"
    );
    assert!(
        paths_differ_between(
            &jj,
            &store,
            &left_tip,
            &right_tip,
            &["shared.rs".to_string(), "own.rs".to_string()]
        ),
        "one genuinely differing path makes the whole conflict a content conflict"
    );
    // An empty path set is not evidence of agreement. Answering "they agree"
    // there would classify a conflict whose paths jj could not enumerate as
    // "your work is already done" — the most misleading thing this could say.
    assert!(
        paths_differ_between(&jj, &store, &left_tip, &right_tip, &[]),
        "no enumerated paths must fall back to a content conflict, never to drift"
    );
}

/// THE core invariant: a conflict-flagged commit never becomes a git ref.
///
/// jj exports such a commit as the DESTINATION side of every conflicted file at
/// the top level with no markers, plus `.jjconflict-*` sidecars — and every agent
/// cell is a `git worktree add --detach` of exactly that ref. So the rebase is
/// rolled back and the branch stays on its own content.
///
/// The last two assertions are the ones that actually encode the incident: a tree
/// can be perfectly free of scaffolding and still silently hold the wrong side,
/// which is how a residue cleanup came to delete the only surviving copy of a
/// branch's work in good faith.
#[test]
#[serial_test::serial(jj)]
fn a_conflicting_rebase_is_rolled_back_and_never_reaches_git() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping a_conflicting_rebase_is_rolled_back_and_never_reaches_git: jj not resolvable"
        );
        return;
    };
    let fx = setup_conflicting_advance(&bin);

    let outcome = rebase_branch_onto(&fx.jj, &fx.store, fx.branch, "main").unwrap();
    let RebaseOutcome::Conflicted { diagnostic } = outcome else {
        panic!("expected a rolled-back conflict, got {outcome:?}");
    };
    assert_eq!(
        diagnostic.conflicting_paths(),
        vec!["shared.rs".to_string()],
        "the caller is told it conflicted, and on which file"
    );
    // Both sides really did edit the file, so this is a content conflict and the
    // agent has merging to do.
    assert_eq!(diagnostic.condition, ConflictCondition::ContentConflict);
    // The immutable coordinates survive the rollback that erased everything else.
    assert_eq!(
        diagnostic.ours.as_deref(),
        Some(fx.pre_tip.as_str()),
        "`ours` is the pre-rebase tip"
    );
    assert!(
        diagnostic.base.is_some() && diagnostic.theirs.is_some(),
        "the base and destination resolve to commits: {diagnostic:?}"
    );
    assert_ne!(
        diagnostic.base, diagnostic.theirs,
        "the destination advanced past the fork point"
    );

    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, fx.branch).unwrap(),
        fx.pre_tip,
        "the bookmark is bit-identical to its pre-rebase value"
    );
    assert!(
        !branch_has_conflict(&fx.jj, &fx.store, fx.branch).unwrap(),
        "nothing conflict-flagged remains in the store"
    );

    let branch_ref = format!("refs/heads/{}", fx.branch);
    assert_eq!(
        git_stdout(fx.proj.path(), &["rev-parse", &branch_ref]),
        fx.pre_tip,
        "the backing git ref never moved off the clean commit"
    );
    let tree = git_stdout(
        fx.proj.path(),
        &["ls-tree", "-r", "--name-only", &branch_ref],
    );
    assert!(
        !tree.contains(".jjconflict-") && !tree.contains("JJ-CONFLICT-README"),
        "no conflict scaffolding reached the exported tree: {tree}"
    );
    assert_eq!(
        git_stdout(
            fx.proj.path(),
            &["show", &format!("{branch_ref}:shared.rs")]
        ),
        "AGENT-SIDE",
        "the branch's own side of the conflicted file survived — a scaffolding-free \
         tree silently holding the DESTINATION side is the actual incident"
    );
    assert_eq!(
        git_stdout(fx.proj.path(), &["show", &format!("{branch_ref}:own.rs")]),
        "AGENT GENUINE WORK",
        "the branch's unrelated work is untouched"
    );
}

/// The rollback is scoped to the one rebase that conflicted.
///
/// `jj op restore` rewinds WHOLE-STORE state, so a snapshot taken once per
/// reconcile rather than once per rebase would undo the earlier siblings that
/// rebased perfectly well. Two siblings in one reconcile: the first rebases
/// cleanly and must STAY rebased; the second conflicts and must return to clean.
#[test]
#[serial_test::serial(jj)]
fn the_rollback_undoes_only_the_conflicting_siblings_rebase() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping the_rollback_undoes_only_the_conflicting_siblings_rebase: jj not resolvable"
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

    let int = "agent/cairn-3197-coordinator-0";
    add_workspace(&jj, &store, &wts.path().join("coord"), int, "main", None).unwrap();

    // Sibling A touches only its own file, so it rebases cleanly.
    let clean = "agent/cairn-1-builder-0";
    let ws_clean = wts.path().join("clean");
    add_workspace(&jj, &store, &ws_clean, clean, int, None).unwrap();
    std::fs::write(ws_clean.join("clean.rs"), "clean work\n").unwrap();
    seal(&jj, &ws_clean, "clean sibling work", None).unwrap();

    // Sibling B edits the file the base is about to change.
    let conflicting = "agent/cairn-2-builder-0";
    let ws_conflicting = wts.path().join("conflicting");
    add_workspace(&jj, &store, &ws_conflicting, conflicting, int, None).unwrap();
    std::fs::write(ws_conflicting.join("shared.rs"), "BRANCH-SIDE\n").unwrap();
    seal(&jj, &ws_conflicting, "conflicting sibling work", None).unwrap();
    let conflicting_before = bookmark_commit(&jj, &store, conflicting).unwrap();

    // The integration tip advances conflictingly.
    jj.run(&store, &["new", int], "new on int").unwrap();
    std::fs::write(store.join("shared.rs"), "INT-SIDE\n").unwrap();
    jj.run(
        &store,
        &["describe", "-m", "int advances shared"],
        "describe",
    )
    .unwrap();
    jj.run(&store, &["bookmark", "set", int, "-r", "@"], "advance int")
        .unwrap();
    let int_tip = bookmark_commit(&jj, &store, int).unwrap();

    let report = reconcile_siblings(
        &jj,
        &store,
        int,
        &[clean.to_string(), conflicting.to_string()],
    )
    .unwrap();

    assert_eq!(report.rebased_clean, vec![clean.to_string()]);
    assert_eq!(report.conflicted, vec![conflicting.to_string()]);
    assert!(
        branch_descends_from(&jj, &store, clean, &int_tip),
        "the cleanly-rebased sibling stayed on the advanced tip — the rollback of a \
         LATER sibling must not rewind it"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, conflicting).unwrap(),
        conflicting_before,
        "the conflicting sibling returned to its own clean commit"
    );
    assert!(!branch_has_conflict(&jj, &store, conflicting).unwrap());
}

/// The unflagged half of the loss, made visible.
///
/// When both sides edit the same file in different regions, jj merges them and
/// records NO conflict — so the rollback above never fires and the branch's
/// version of an overlapping region can simply be replaced with no marker, no
/// error, and no record. Prevention is not proven; detection is. The instrument
/// must name that file, and must not cry wolf over files only one side touched.
#[test]
#[serial_test::serial(jj)]
fn a_clean_rebase_names_the_files_both_sides_changed() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_clean_rebase_names_the_files_both_sides_changed: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();

    // A file long enough that two edits at opposite ends merge without conflict.
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

    let branch = "agent/cairn-3197-builder-0";
    let ws = wts.path().join("agent");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(
        ws.join("shared.rs"),
        body.replacen("line 0\n", "line 0 BRANCH\n", 1),
    )
    .unwrap();
    std::fs::write(ws.join("branch-only.rs"), "only the branch\n").unwrap();
    seal(&jj, &ws, "branch edits the top of shared", None).unwrap();
    let pre_tip = bookmark_commit(&jj, &store, branch).unwrap();

    // The base edits the far end of the SAME file, plus a file of its own.
    let oob = "agent/cairn-9-oob-0";
    let ws_oob = wts.path().join("oob");
    add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
    std::fs::write(
        ws_oob.join("shared.rs"),
        body.replacen("line 39\n", "line 39 MAIN\n", 1),
    )
    .unwrap();
    std::fs::write(ws_oob.join("base-only.rs"), "only the base\n").unwrap();
    seal(&jj, &ws_oob, "base edits the bottom of shared", None).unwrap();
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
        "advance main",
    )
    .unwrap();

    assert_eq!(
        rebase_branch_onto(&jj, &store, branch, "main").unwrap(),
        RebaseOutcome::Rebased,
        "edits at opposite ends of one file merge with no conflict recorded"
    );
    let post_tip = bookmark_commit(&jj, &store, branch).unwrap();
    assert_ne!(post_tip, pre_tip, "the rebase moved the branch");

    let selection = rebase_side_selection(&jj, &store, "main", &pre_tip, &post_tip);
    assert!(
        selection.overlapping.contains(&"shared.rs".to_string()),
        "the file BOTH sides changed is named: {selection:?}"
    );
    assert!(
        !selection.overlapping.contains(&"base-only.rs".to_string()),
        "a file only the base changed is not a side-selection risk: {selection:?}"
    );
    assert!(
        !selection
            .overlapping
            .contains(&"branch-only.rs".to_string()),
        "a file only the branch changed is not a side-selection risk: {selection:?}"
    );
    assert!(
        selection
            .changed
            .iter()
            .any(|(status, path)| !status.is_empty() && path == "shared.rs"),
        "the name-status record covers what the rebase actually changed: {selection:?}"
    );
}

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

/// A clean TIP is not a clean branch, and a commit-preserving fold must refuse
/// the difference.
///
/// `branch_has_conflict` answers for the bookmark tip only. jj propagates a
/// conflict to every descendant until something resolves it, so a *fresh*
/// conflict always lands on the tip and is rolled back — but a branch that
/// already carries a conflict-flagged commit keeps it through every later rebase,
/// with a clean resolving commit on top hiding it from that probe. Folding such a
/// branch carries the conflicted commit onto the target as ordinary history,
/// after which jj refuses to push the target at all.
///
/// Built the way it actually arises: an unguarded rebase records the conflict
/// (a store predating the guard, or `jj` run outside Cairn), the agent resolves
/// on top and re-seals, and the base then advances again.
#[test]
#[serial_test::serial(jj)]
fn a_clean_tip_over_conflicted_history_is_never_folded() {
    let Some(bin) = jj_bin() else {
        eprintln!(
            "skipping a_clean_tip_over_conflicted_history_is_never_folded: jj not resolvable"
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

    let branch = "agent/cairn-3197-builder-0";
    let ws = wts.path().join("agent");
    add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
    std::fs::write(ws.join("shared.rs"), "BRANCH-SIDE\n").unwrap();
    seal(&jj, &ws, "branch edits shared", None).unwrap();

    // `main` advances conflictingly, and the branch is rebased UNGUARDED — the
    // pre-guard shape this has to keep coping with.
    let oob = "agent/cairn-9-oob-0";
    let ws_oob = wts.path().join("oob");
    add_workspace(&jj, &store, &ws_oob, oob, "main", None).unwrap();
    std::fs::write(ws_oob.join("shared.rs"), "MAIN-SIDE\n").unwrap();
    seal(&jj, &ws_oob, "main advances shared", None).unwrap();
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
        "advance main",
    )
    .unwrap();
    rebase_recording_conflict(&jj, &store, branch, "main");

    // The agent resolves on top and re-seals: clean tip, conflicted ancestor.
    update_stale(&jj, &ws).unwrap();
    std::fs::write(ws.join("shared.rs"), "RESOLVED\n").unwrap();
    seal(&jj, &ws, "resolve the base conflict", None).unwrap();
    assert!(
        !branch_has_conflict(&jj, &store, branch).unwrap(),
        "precondition: the TIP is clean — which is exactly what makes this dangerous"
    );
    assert!(
        !conflicted_commits(&jj, &store, &format!("main..bookmarks(exact:{branch:?})")).is_empty(),
        "precondition: the branch's own history still carries a conflicted commit"
    );

    // The base advances once more, and the branch is rebased through the guarded
    // path. The conflicted ancestor survives the rebase, and the outcome says so
    // rather than reporting a clean rebase.
    std::fs::write(ws_oob.join("unrelated.rs"), "base moves on\n").unwrap();
    seal(&jj, &ws_oob, "main advances again", None).unwrap();
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
        "advance main again",
    )
    .unwrap();

    let outcome = rebase_branch_onto(&jj, &store, branch, "main").unwrap();
    match &outcome {
        RebaseOutcome::RebasedOverConflictedAncestry { paths } => {
            assert!(
                paths.contains(&"shared.rs".to_string()),
                "the conflicted ancestry names its file: {paths:?}"
            );
        }
        other => panic!("a clean tip over conflicted history must not read as clean: {other:?}"),
    }

    // The commit-preserving fold refuses it, and the default branch never moves.
    let main_before = bookmark_commit(&jj, &store, "main").unwrap();
    let err = rebase_then_fold_into(&jj, &store, "main", branch, "main").unwrap_err();
    assert!(
        err.contains("history") && err.contains("conflicted"),
        "the refusal names the conflicted history rather than a tip conflict: {err}"
    );
    assert!(err.contains("shared.rs"), "{err}");
    assert_eq!(
        bookmark_commit(&jj, &store, "main").unwrap(),
        main_before,
        "the default branch never gained the conflicted ancestry"
    );
    assert!(
        conflicted_commits(&jj, &store, "bookmarks(exact:\"main\")").is_empty(),
        "the default branch tip is still clean"
    );

    // And the healing path still works: the heal resolves the conflicted commit
    // in place, after which the same commit-preserving fold succeeds. This is why
    // such a branch is exported rather than rolled back — the heal can only run
    // on a branch that has been rebased onto its dest.
    let dest = bookmark_commit(&jj, &store, "main").unwrap();
    heal_conflicted_ancestry(&jj, &store, branch, &dest).unwrap();
    assert_eq!(
        rebase_branch_onto(&jj, &store, branch, "main").unwrap(),
        RebaseOutcome::Rebased,
        "the healed branch is genuinely clean"
    );
    rebase_then_fold_into(&jj, &store, "main", branch, "main").unwrap();
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

/// The completion of "prepare is workspace-free": staleness in the store's
/// default workspace must not fail, or silently distort, ANY store operation.
///
/// `--ignore-working-copy` on `jj git import` closed one of eleven store-facing
/// sites. Measured on jj 0.42, the other ten all failed against a stale store,
/// and four of them failed into a wrong answer rather than an error:
/// `revset_resolves` returning `false` sends [`resolve_base_rev`] down to `HEAD`
/// or `root()`, so a stale store would silently provision a job off the wrong
/// base, and the conflict probes fall to the permissive side of a merge gate.
#[test]
#[serial_test::serial(jj)]
fn a_stale_store_default_workspace_fails_no_store_operation() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_stale_store_default_workspace_fails_no_store_operation: no jj");
        return;
    };
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let workspaces = TempDir::new().unwrap();
    init_project(project.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("project");
    ensure_project_store(&jj, &store, project.path()).unwrap();
    let original_main = bookmark_commit(&jj, &store, "main").unwrap();

    // A sibling branch to interrogate, cut before the store goes stale.
    let sibling = workspaces.path().join("sibling");
    add_workspace(&jj, &store, &sibling, "agent/sibling", "main", None).unwrap();
    std::fs::write(sibling.join("sibling.rs"), "sibling\n").unwrap();
    seal(&jj, &sibling, "sibling work", None).unwrap();
    let sibling_tip = bookmark_commit(&jj, &store, "agent/sibling").unwrap();

    let advanced_main = make_store_default_workspace_stale(&jj, &store, project.path());
    assert_ne!(advanced_main, original_main);

    // Reads stay truthful.
    assert_eq!(
        bookmark_commit(&jj, &store, "main").as_deref(),
        Some(advanced_main.as_str()),
        "a bookmark read resolves over a stale store"
    );
    assert_eq!(
        bookmark_commit_checked(&jj, &store, "agent/sibling"),
        Ok(Some(sibling_tip.clone())),
        "the proving resolver too — it must not report a stale store as an absent bookmark"
    );
    assert!(revset_resolves(&jj, &store, "agent/sibling"));
    assert_eq!(
        resolve_base_rev(&jj, &store, "agent/sibling", |_: &str| None),
        "agent/sibling",
        "a store-only bookmark is still recognised as a store revset; falling through here is \
         how a stale store provisions the next job off the wrong base without erroring"
    );
    assert_eq!(branch_has_conflict(&jj, &store, "agent/sibling"), Ok(false));
    assert!(
        branch_descends_from(&jj, &store, "agent/sibling", &original_main),
        "the descendant probe must not fall to `false` merely because the store is stale"
    );
    assert!(branch_is_ancestor_of(
        &jj,
        &store,
        "agent/sibling",
        &sibling_tip
    ));
    assert!(conflicted_commits(&jj, &store, "bookmarks(exact:\"agent/sibling\")").is_empty());

    // None of those reads may have repaired the store on the way past, or every
    // assertion after this point would pass vacuously against a healthy store.
    assert!(
        store_default_workspace_is_stale(&jj, &store),
        "the read block must leave the store exactly as stale as it found it"
    );

    // And every write the spawn path performs still lands — `jj workspace add`
    // included, which jj will not let Cairn run without a current default
    // workspace and which therefore carries the repair.
    let child = workspaces.path().join("child");
    add_workspace(&jj, &store, &child, "agent/child", &advanced_main, None).unwrap();
    assert_eq!(
        bookmark_commit(&jj, &store, "agent/child").as_deref(),
        Some(advanced_main.as_str()),
        "the new workspace's bookmark was created at its base"
    );
    assert!(
        child.join(".jj").is_dir(),
        "the workspace is real, not merely registered"
    );
    forget_workspace(&jj, &store, "agent/child").unwrap();

    // The import that slice 1 fixed still passes on a store the repair has now
    // refreshed, and on one made stale again afterwards.
    ensure_project_store(&jj, &store, project.path()).unwrap();
    make_store_default_workspace_stale(&jj, &store, project.path());
    ensure_project_store(&jj, &store, project.path()).unwrap();
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

/// A publication's commit write contends for `packed-refs.lock` too: jj-lib
/// writes a `refs/jj/keep/<commit>` ref for every commit, and gix checks that
/// update against `packed-refs`. A routine rewrite by any other writer used to
/// refuse the whole publication after gix's one-second wait.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_publication_waits_out_a_held_then_released_packed_refs_lock() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping publication packed-refs retry test: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/packed-refs-publication";
    let base = bookmark_commit(&jj, &store, "main").unwrap();
    create_bookmark_at(&jj, &store, branch, &base).unwrap();
    // gix only consults (and locks) `packed-refs` when the file exists.
    git(proj.path(), &["pack-refs", "--all"]);

    let lock = proj.path().join(".git").join("packed-refs.lock");
    std::fs::write(&lock, "held by test\n").unwrap();
    let release = lock.clone();
    let holder = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(2000));
        std::fs::remove_file(release).unwrap();
    });

    let started = std::time::Instant::now();
    let published = publish_logical_head_exported(
        &jj,
        &store,
        branch,
        &base,
        ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
            path: "patient.rs".to_string(),
            content: Some(b"patient\n".to_vec()),
        }]),
        None,
        crate::PublicationMode::Child {
            description: "published through contention".into(),
        },
    )
    .await
    .expect("the publication must retry until the ordinary Git lock is released");
    holder.join().unwrap();
    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the fixture must actually have made the commit write wait"
    );
    published.export.expect("the export leg lands too");

    // Exactly one commit landed, directly on the base: the failed attempts
    // moved nothing.
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(published.landed.head.as_str())
    );
    assert_eq!(
        git_stdout(
            proj.path(),
            &["rev-parse", &format!("{}^", published.landed.head)]
        ),
        base,
        "the published commit sits directly on the expected head"
    );
}

/// Patience is bounded. A lock a live process keeps holding past the whole
/// wait is never healed, and the publication reports the contention with
/// nothing published rather than hanging or landing half a write.
#[tokio::test]
#[serial_test::serial(jj)]
async fn a_publication_surfaces_a_lock_held_past_patience_with_nothing_published() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping publication packed-refs exhaustion test: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();
    let branch = "agent/packed-refs-exhausted";
    let base = bookmark_commit(&jj, &store, "main").unwrap();
    create_bookmark_at(&jj, &store, branch, &base).unwrap();
    git(proj.path(), &["pack-refs", "--all"]);

    // Held open by this process, so `lsof` proves a live holder and the
    // healer must decline.
    let lock = proj.path().join(".git").join("packed-refs.lock");
    std::fs::write(&lock, "held by a live writer\n").unwrap();
    let held = std::fs::File::open(&lock).unwrap();

    let result = publish_logical_head_exported(
        &jj,
        &store,
        branch,
        &base,
        ProposedPublication::Mutations(vec![crate::LogicalTreeMutation {
            path: "never.rs".to_string(),
            content: Some(b"never\n".to_vec()),
        }]),
        None,
        crate::PublicationMode::Child {
            description: "refused by a live lock".into(),
        },
    )
    .await;
    drop(held);
    let error = match result {
        Ok(_) => panic!("a lock held past patience must refuse the publication"),
        Err(error) => error,
    };
    assert!(
        crate::jj::packed_refs_lock_error(&error),
        "the refusal names the contention: {error}"
    );
    assert!(lock.exists(), "a live writer's lock is never removed");
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(base.as_str()),
        "nothing was published"
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

/// A force-moved tracked branch is the sole push failure eligible for recovery.
#[test]
#[serial_test::serial(jj)]
fn store_bookmark_push_classifies_a_remote_that_moved_after_tracking() {
    let Some((_home, _project, origin, _workspaces, jj, store, branch, workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let base = bookmark_commit(&jj, &store, "main").unwrap();
    git(
        origin.path(),
        &["update-ref", &format!("refs/heads/{branch}"), &base],
    );
    std::fs::write(workspace.join("local.rs"), "local\n").unwrap();
    seal(&jj, &workspace, "unpublished local work", None).unwrap();
    let error = push_store_bookmark_classified(&jj, &store, &branch).unwrap_err();
    assert!(
        matches!(error, StoreBookmarkPushError::StaleRemote(_)),
        "{error}"
    );
}

#[test]
#[serial_test::serial(jj)]
fn managed_branch_convergence_refuses_an_unrelated_remote_without_mutation() {
    let Some((_home, project, origin, _workspaces, jj, store, branch, workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    std::fs::write(workspace.join("local.rs"), "local\n").unwrap();
    seal(&jj, &workspace, "unpublished local work", None).unwrap();
    let local_before = bookmark_commit(&jj, &store, &branch).unwrap();
    let exported_before = git_stdout(
        project.path(),
        &["rev-parse", &format!("refs/heads/{branch}")],
    );
    let tree = git_stdout(project.path(), &["rev-parse", "main^{tree}"]);
    let unrelated = git_stdout(
        project.path(),
        &["commit-tree", &tree, "-m", "unrelated remote rewrite"],
    );
    git(
        project.path(),
        &[
            "push",
            "-q",
            "--force",
            "origin",
            &format!("{unrelated}:refs/heads/{branch}"),
        ],
    );
    fetch_remote_branch_via_git(&store, "origin", &branch).unwrap();

    let error = converge_managed_branch_after_remote_rewrite(&jj, &store, &branch).unwrap_err();
    assert!(error.contains("no change-id twin"), "{error}");
    assert_eq!(
        bookmark_commit(&jj, &store, &branch).as_deref(),
        Some(local_before.as_str())
    );
    assert_eq!(
        git_stdout(
            project.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        exported_before
    );
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        unrelated
    );
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

fn seal_and_export(jj: &JjEnv, store: &Path, workspace: &Path, branch: &str, file: &str) -> String {
    std::fs::write(workspace.join(file), format!("{file}\n")).unwrap();
    seal(jj, workspace, &format!("edit {file}"), None).unwrap();
    export_bookmark_advance(jj, store, true, branch, "seal and export").unwrap();
    bookmark_commit(jj, store, branch).unwrap()
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

/// The cairn/4810 incident: a merge preview's rollback interleaved with a PR's
/// first push. Rolling back must restore only the local view, so origin's copy
/// stays tracked, git keeps `refs/remotes/origin/<branch>`, and the next push of
/// the branch fast-forwards origin instead of being refused.
#[test]
#[serial_test::serial(jj)]
fn a_rollback_interleaved_with_a_push_keeps_origin_tracked() {
    let Some((_home, project, origin, workspaces, jj, store, _branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let branch = "agent/cairn-4804-builder-5";
    let workspace = mint_unpublished_branch(&jj, &store, workspaces.path(), branch);
    let pushed = bookmark_commit(&jj, &store, branch).unwrap();

    let snapshot = operation_id(&jj, &store).unwrap();
    push_store_bookmark(&jj, &store, branch).unwrap();
    restore_operation(&jj, &store, &snapshot).unwrap();

    assert_eq!(
        origin_tracking(&jj, &store, branch),
        Some(true),
        "the rollback must not forget a push that really landed on origin"
    );
    assert_eq!(
        git_stdout(
            project.path(),
            &["rev-parse", &format!("refs/remotes/origin/{branch}")]
        ),
        pushed,
        "the export after the rollback must keep git's remote-tracking ref"
    );

    let advanced = seal_and_export(&jj, &store, &workspace, branch, "second.rs");
    push_store_bookmark(&jj, &store, branch).unwrap();
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        advanced,
        "the next push advances origin"
    );
}

/// An untracked origin copy carrying the branch's own history is classified as
/// recoverable, and converging tracks it so the retried push lands.
#[test]
#[serial_test::serial(jj)]
fn an_untracked_remote_of_the_same_lineage_is_tracked_and_pushed() {
    let Some((_home, _project, origin, workspaces, jj, store, _branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let branch = "agent/cairn-4804-builder-5";
    let workspace = mint_unpublished_branch(&jj, &store, workspaces.path(), branch);
    push_then_forget_then_reimport(&jj, &store, branch);
    assert_eq!(origin_tracking(&jj, &store, branch), Some(false));

    let advanced = seal_and_export(&jj, &store, &workspace, branch, "second.rs");
    let error = push_store_bookmark_classified(&jj, &store, branch).unwrap_err();
    assert!(
        matches!(error, StoreBookmarkPushError::UntrackedRemote(_)),
        "{error:?}"
    );

    let convergence = converge_managed_branch_after_remote_rewrite(&jj, &store, branch).unwrap();
    assert!(
        matches!(convergence, ManagedBranchConvergence::AheadOfOrigin { .. }),
        "{convergence:?}"
    );
    assert_eq!(origin_tracking(&jj, &store, branch), Some(true));

    push_store_bookmark_classified(&jj, &store, branch).unwrap();
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        advanced
    );
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

/// A branch minted with a later execution's sequence number (`-builder-5`
/// where `-builder-4` never existed) is an ordinary new branch: its first push
/// tracks it and its next push advances origin.
#[test]
#[serial_test::serial(jj)]
fn a_freshly_minted_branch_after_a_superseded_execution_publishes_and_advances() {
    let Some((_home, _project, origin, workspaces, jj, store, _branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let branch = "agent/p-1-builder-5";
    let workspace = mint_unpublished_branch(&jj, &store, workspaces.path(), branch);
    push_store_bookmark(&jj, &store, branch).unwrap();
    assert_eq!(origin_tracking(&jj, &store, branch), Some(true));

    let advanced = seal_and_export(&jj, &store, &workspace, branch, "second.rs");
    push_store_bookmark(&jj, &store, branch).unwrap();
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        advanced
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

/// When commits are frozen by an untracked remote bookmark Cairn cannot safely
/// track, the rebase refusal says nothing landed rather than leaving the reader
/// to guess from jj's bare "immutable".
#[test]
#[serial_test::serial(jj)]
fn an_immutable_rebase_refusal_names_an_untracked_holder_not_a_landing() {
    let Some((_home, project, _origin, workspaces, jj, store, _branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let branch = "agent/cairn-4742-builder-1";
    mint_unpublished_branch(&jj, &store, workspaces.path(), branch);
    let tip = bookmark_commit(&jj, &store, branch).unwrap();
    // Another clone pushed this branch's commit under a different name.
    git(
        project.path(),
        &[
            "push",
            "-q",
            "origin",
            &format!("{tip}:refs/heads/elsewhere"),
        ],
    );
    fetch_remote_branch_via_git(&store, "origin", "elsewhere").unwrap();
    import_git(&jj, &store).unwrap();
    advance_store_main(&jj, &store);

    let error = rebase_branch_onto(&jj, &store, branch, "main").unwrap_err();
    assert!(error.contains("immutable"), "{error}");
    assert!(error.contains("nothing has landed"), "{error}");
    assert_eq!(
        bookmark_commit(&jj, &store, branch).as_deref(),
        Some(tip.as_str())
    );
}

/// A branch one commit ahead of a `main` that has since advanced, with
/// `packed-refs` present so gix consults (and locks) it on every ref write.
/// The store replay's lock tests share it.
struct ReplayLockFixture {
    _home: TempDir,
    proj: TempDir,
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
        proj,
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

fn replay_range_commits(fx: &ReplayLockFixture, range: &str) -> usize {
    count_commits(&fx.jj, &fx.store, range)
}

/// The replay itself is patient now. A routine writer holding
/// `packed-refs.lock` for two seconds used to fail the rebase after gix's
/// one-second wait; the replay now waits it out and applies exactly once.
#[test]
#[serial_test::serial(jj)]
fn a_replay_waits_out_a_lock_held_for_two_seconds() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping replay packed-refs wait test: jj not resolvable");
        return;
    };
    let fx = replay_lock_fixture(&bin);
    let range = format!("main..{}", fx.branch);
    let commits_before = replay_range_commits(&fx, &range);

    std::fs::write(&fx.lock, "held by test\n").unwrap();
    let release = fx.lock.clone();
    let holder = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(2000));
        std::fs::remove_file(release).unwrap();
    });
    let started = std::time::Instant::now();
    let report =
        reconcile_siblings_without_publication(&fx.jj, &fx.store, "main", &[fx.branch.to_string()])
            .unwrap();
    holder.join().unwrap();

    assert!(
        started.elapsed() >= std::time::Duration::from_secs(1),
        "the fixture must actually have made the replay wait"
    );
    assert_eq!(
        report.rebased_clean,
        vec![fx.branch.to_string()],
        "{report:?}"
    );
    assert_eq!(
        replay_range_commits(&fx, &range),
        commits_before,
        "the rebase applied once: the branch carries its own commits and no more"
    );
    let main = bookmark_commit(&fx.jj, &fx.store, "main").unwrap();
    let parent = fx
        .jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                &format!("bookmarks(exact:{:?})-", fx.branch),
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "rebased parent",
        )
        .unwrap();
    assert_eq!(
        parent.trim(),
        main,
        "the branch now sits on the advanced main"
    );
}

/// Patience is bounded, and a replay that runs out of it moves nothing and
/// says what it met. The lock held by a live process is never removed, the
/// failure is infrastructure rather than a conflict, and the next branch in
/// the pass is deferred without an attempt rather than waiting out the same
/// lock again.
#[test]
#[serial_test::serial(jj)]
fn a_replay_refused_past_patience_moves_nothing_and_defers_the_rest() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping replay packed-refs exhaustion test: jj not resolvable");
        return;
    };
    let fx = replay_lock_fixture(&bin);
    let second = "agent/cairn-4994-builder-1";
    let wts = TempDir::new().unwrap();
    let ws = wts.path().join("second");
    let base = fx
        .jj
        .run(
            &fx.store,
            &[
                "log",
                "-r",
                "main-",
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
            "pre-advance main",
        )
        .unwrap();
    add_workspace(&fx.jj, &fx.store, &ws, second, base.trim(), None).unwrap();
    std::fs::write(ws.join("second.rs"), "second\n").unwrap();
    seal(&fx.jj, &ws, "second work", None).unwrap();
    git(fx.proj.path(), &["pack-refs", "--all"]);

    let pre_op = operation_id(&fx.jj, &fx.store).unwrap();
    let tips: Vec<String> = [fx.branch, second]
        .iter()
        .map(|branch| bookmark_commit(&fx.jj, &fx.store, branch).unwrap())
        .collect();
    let git_ref = git_stdout(
        fx.proj.path(),
        &["rev-parse", &format!("refs/heads/{}", fx.branch)],
    );

    std::fs::write(&fx.lock, "held by a live writer\n").unwrap();
    let held = std::fs::File::open(&fx.lock).unwrap();
    let report = reconcile_siblings_without_publication(
        &fx.jj,
        &fx.store,
        "main",
        &[fx.branch.to_string(), second.to_string()],
    )
    .unwrap();
    assert!(fx.lock.exists(), "a live writer's lock is never removed");
    drop(held);
    std::fs::remove_file(&fx.lock).unwrap();

    assert!(
        report.conflicted.is_empty(),
        "a lock is not a conflict: {report:?}"
    );
    assert!(report.rebased_clean.is_empty(), "{report:?}");
    assert_eq!(report.failed.len(), 2, "{report:?}");
    let first = &report.failed[0];
    assert_eq!(first.branch, fx.branch);
    assert_eq!(
        reconcile_failure_kind(&first.error),
        REF_LOCK_CONTENTION,
        "{}",
        first.error
    );
    assert!(
        first
            .error
            .contains(&format!("held by live process {}", std::process::id())),
        "the give-up names the live holder: {}",
        first.error
    );
    let deferred = &report.failed[1];
    assert_eq!(deferred.branch, second);
    assert!(
        deferred.error.starts_with("deferred without an attempt"),
        "{}",
        deferred.error
    );
    assert_eq!(reconcile_failure_kind(&deferred.error), REF_LOCK_CONTENTION);

    assert_eq!(
        operation_id(&fx.jj, &fx.store).unwrap(),
        pre_op,
        "no operation was committed"
    );
    for (branch, tip) in [fx.branch, second].iter().zip(&tips) {
        assert_eq!(
            bookmark_commit(&fx.jj, &fx.store, branch).as_deref(),
            Some(tip.as_str()),
            "{branch} did not move"
        );
    }
    assert_eq!(
        git_stdout(
            fx.proj.path(),
            &["rev-parse", &format!("refs/heads/{}", fx.branch)]
        ),
        git_ref,
        "the backing git ref did not move"
    );
}

/// A lock abandoned by a crashed writer is proven stale — old, and held by no
/// process — removed once, and the replay goes through.
#[test]
#[serial_test::serial(jj)]
fn a_stale_lock_is_healed_and_the_replay_proceeds() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping replay stale-lock test: jj not resolvable");
        return;
    };
    let fx = replay_lock_fixture(&bin);
    std::fs::write(&fx.lock, "abandoned by a crashed writer\n").unwrap();
    std::fs::File::options()
        .write(true)
        .open(&fx.lock)
        .unwrap()
        .set_modified(std::time::SystemTime::now() - std::time::Duration::from_secs(120))
        .unwrap();

    let report =
        reconcile_siblings_without_publication(&fx.jj, &fx.store, "main", &[fx.branch.to_string()])
            .unwrap();
    assert_eq!(
        report.rebased_clean,
        vec![fx.branch.to_string()],
        "{report:?}"
    );
    assert!(!fx.lock.exists(), "the proven-stale lock was removed");
}

type HeldLock = std::sync::Arc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>;

/// Take `lock` the moment this thread's store rewrite reaches `step`, hold it
/// open (so `lsof` sees a live writer) for `held`, then release it. Long
/// enough to exhaust the step's patience, short enough for the rollback that
/// follows to win the lock back.
fn hold_lock_at_step(step: &'static str, lock: PathBuf, held: std::time::Duration) -> HeldLock {
    let slot: HeldLock = Default::default();
    let handle_slot = slot.clone();
    at_step(step, move || {
        std::fs::write(&lock, "held by a live writer\n").unwrap();
        let file = std::fs::File::open(&lock).unwrap();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(held);
            drop(file);
            std::fs::remove_file(&lock).unwrap();
        });
        *handle_slot.lock().unwrap() = Some(handle);
    });
    slot
}

fn release(held: HeldLock) {
    if let Some(handle) = held.lock().unwrap().take() {
        handle.join().unwrap();
    }
}

/// The heal rewrites in several transactions: a restore per conflicted commit,
/// then the drop of what it emptied and the notes. A lock lost after the first
/// restore must not leave a half-healed branch while the pass reports it
/// unchanged and retries quietly. The refusal puts the branch, its tree, and
/// its git ref back, and the next pass heals it properly.
#[test]
#[serial_test::serial(jj)]
fn a_heal_that_loses_the_lock_partway_is_undone_and_retried() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping heal mid-sequence lock test: jj not resolvable");
        return;
    };
    let fx = setup_divergence_fixture(&bin);
    let dest = make_intermediate_only(&fx);
    let proj = fx._proj.path();
    git(proj, &["pack-refs", "--all"]);
    let lock = proj.join(".git").join("packed-refs.lock");
    let pre_tip = bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).unwrap();
    let git_ref = git_stdout(proj, &["rev-parse", &format!("refs/heads/{DIV_SIBLING}")]);

    let held = hold_lock_at_step(
        "ancestry heal: commit resolved",
        lock.clone(),
        std::time::Duration::from_secs(36),
    );
    let report = reconcile_siblings_without_publication(
        &fx.jj,
        &fx.store,
        DIV_INT,
        &[DIV_SIBLING.to_string()],
    )
    .unwrap();
    release(held);

    assert!(
        report.conflicted.is_empty(),
        "a lock is not a conflict: {report:?}"
    );
    assert_eq!(report.failed.len(), 1, "{report:?}");
    let error = &report.failed[0].error;
    assert_eq!(
        reconcile_failure_kind(error),
        REF_LOCK_CONTENTION,
        "{error}"
    );
    assert!(!error.contains(ROLLBACK_INCOMPLETE), "{error}");
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, DIV_SIBLING).as_deref(),
        Some(pre_tip.as_str()),
        "the half-healed branch is back where it was"
    );
    assert_eq!(
        git_stdout(proj, &["rev-parse", &format!("refs/heads/{DIV_SIBLING}")]),
        git_ref,
        "and so is its git ref"
    );
    assert_eq!(
        String::from_utf8_lossy(&file_show(&fx.jj, &fx.store, DIV_SIBLING, "shared.rs").unwrap()),
        "resolved\n",
        "the source tree survived"
    );

    let retried = reconcile_siblings_without_publication(
        &fx.jj,
        &fx.store,
        DIV_INT,
        &[DIV_SIBLING.to_string()],
    )
    .unwrap();
    assert_eq!(
        retried.rebased_clean,
        vec![DIV_SIBLING.to_string()],
        "{retried:?}"
    );
    assert_eq!(
        count_commits(
            &fx.jj,
            &fx.store,
            &format!("{dest}..bookmarks(exact:{DIV_SIBLING:?})")
        ),
        1,
        "the retry healed it: the sibling's commit survives and the emptied resolution is dropped"
    );
    assert_eq!(
        String::from_utf8_lossy(&file_show(&fx.jj, &fx.store, DIV_SIBLING, "shared.rs").unwrap()),
        "resolved\n",
        "carrying the same tree"
    );
}

/// A rebase that lands in the store and then cannot export is rolled back, so
/// "the branch is unchanged" is true when the failure is classified as a lost
/// lock, and the next pass performs the whole rebase again.
#[test]
#[serial_test::serial(jj)]
fn a_rebase_whose_export_loses_the_lock_is_rolled_back_and_retried() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping rebase export lock test: jj not resolvable");
        return;
    };
    let fx = replay_lock_fixture(&bin);
    let pre_tip = bookmark_commit(&fx.jj, &fx.store, fx.branch).unwrap();
    let git_ref = git_stdout(
        fx.proj.path(),
        &["rev-parse", &format!("refs/heads/{}", fx.branch)],
    );

    let held = hold_lock_at_step(
        "rebase: before export",
        fx.lock.clone(),
        std::time::Duration::from_secs(36),
    );
    let report =
        reconcile_siblings_without_publication(&fx.jj, &fx.store, "main", &[fx.branch.to_string()])
            .unwrap();
    release(held);

    assert!(report.conflicted.is_empty(), "{report:?}");
    assert_eq!(report.failed.len(), 1, "{report:?}");
    let error = &report.failed[0].error;
    assert_eq!(
        reconcile_failure_kind(error),
        REF_LOCK_CONTENTION,
        "{error}"
    );
    assert!(error.contains("rolled back"), "{error}");
    assert_eq!(
        bookmark_commit(&fx.jj, &fx.store, fx.branch).as_deref(),
        Some(pre_tip.as_str()),
        "the landed rebase was undone in the store"
    );
    assert_eq!(
        git_stdout(
            fx.proj.path(),
            &["rev-parse", &format!("refs/heads/{}", fx.branch)]
        ),
        git_ref,
        "and git agrees"
    );

    let retried =
        reconcile_siblings_without_publication(&fx.jj, &fx.store, "main", &[fx.branch.to_string()])
            .unwrap();
    assert_eq!(
        retried.rebased_clean,
        vec![fx.branch.to_string()],
        "{retried:?}"
    );
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

/// A fold whose child left a file unparseable that parses on the target is
/// refused before the target moves, naming the file and the parser's line.
#[test]
#[serial_test::serial(jj)]
fn a_fold_that_breaks_a_parseable_file_is_refused_and_the_target_stays() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_fold_that_breaks_a_parseable_file_is_refused: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let int = "agent/CAIRN-2-coordinator-0";
    let child = "agent/CAIRN-3-builder-0";
    let ws_int = wts.path().join("int");
    add_workspace(&jj, &store, &ws_int, int, "main", None).unwrap();
    std::fs::write(
        ws_int.join("registry.rs"),
        "const A: &[u8] = &[\n    1,\n];\n",
    )
    .unwrap();
    seal(&jj, &ws_int, "registry", None).unwrap();
    let int_tip = bookmark_commit(&jj, &store, int).unwrap();

    let ws_child = wts.path().join("child");
    add_workspace(&jj, &store, &ws_child, child, int, None).unwrap();
    std::fs::write(
        ws_child.join("registry.rs"),
        "const A: &[u8] = &[\n    1,\n];\n    2,\n];\n",
    )
    .unwrap();
    seal(&jj, &ws_child, "an entry after the closing bracket", None).unwrap();

    let refusal = merge_into_bookmark(&jj, &store, int, child).unwrap_err();
    assert!(refusal.contains("- registry.rs:5:"), "{refusal}");
    assert!(refusal.contains("a file that parses"), "{refusal}");
    assert_eq!(bookmark_commit(&jj, &store, int).unwrap(), int_tip);
}

/// A replay jj merges without a recorded conflict into a file that does not
/// parse is rolled back and recorded as an unparseable conflict. Each side's
/// TOML is valid; together they define one key twice.
#[test]
#[serial_test::serial(jj)]
fn a_clean_replay_into_an_unparseable_file_is_rolled_back_as_a_conflict() {
    let Some(bin) = jj_bin() else {
        eprintln!("skipping a_clean_replay_into_an_unparseable_file: jj not resolvable");
        return;
    };
    let home = TempDir::new().unwrap();
    let proj = TempDir::new().unwrap();
    let wts = TempDir::new().unwrap();
    init_project(proj.path());
    let jj = JjEnv::resolve(&bin, home.path());
    let store = home.path().join("jj-stores").join("proj");
    ensure_project_store(&jj, &store, proj.path()).unwrap();

    let base = "[t]\na = 1\n\n\nb = 2\n";
    advance_main_to(&jj, &store, wts.path(), "seed", "settings.toml", base);

    let branch = "agent/CAIRN-4-builder-0";
    let ws_branch = wts.path().join("branch");
    add_workspace(&jj, &store, &ws_branch, branch, "main", None).unwrap();
    std::fs::write(
        ws_branch.join("settings.toml"),
        "[t]\na = 1\nc = 3\n\n\nb = 2\n",
    )
    .unwrap();
    seal(&jj, &ws_branch, "add c", None).unwrap();
    let pre_tip = bookmark_commit(&jj, &store, branch).unwrap();

    advance_main_to(
        &jj,
        &store,
        wts.path(),
        "incoming",
        "settings.toml",
        "[t]\na = 1\n\n\nc = 4\n",
    );

    let RebaseOutcome::Conflicted { diagnostic } =
        rebase_branch_onto(&jj, &store, branch, "main").unwrap()
    else {
        panic!("a clean merge that does not parse must be refused");
    };
    assert_eq!(diagnostic.condition, ConflictCondition::Unparseable);
    assert_eq!(diagnostic.conflicting_paths(), vec!["settings.toml"]);
    let refusal = diagnostic
        .validation
        .expect("the parser's failures ride along");
    assert_eq!(refusal.failures.len(), 1);
    assert_eq!(refusal.failures[0].path, "settings.toml");
    assert_eq!(
        bookmark_commit(&jj, &store, branch).unwrap(),
        pre_tip,
        "the branch stays on its old base"
    );
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

fn fast_forward_now(jj: &JjEnv, store: &Path, branch: &str) -> OriginFastForward {
    let observed = fetch_origin_observation(store, branch)
        .unwrap()
        .expect("origin holds the branch");
    fast_forward_branch_from_origin(jj, store, branch, &observed).unwrap()
}

#[test]
#[serial_test::serial(jj)]
fn an_outside_advance_of_a_store_owned_branch_is_taken_as_a_fast_forward() {
    let Some((_home, project, origin, _workspaces, jj, store, branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let before = bookmark_commit(&jj, &store, &branch).unwrap();
    let landed = advance_origin_outside(origin.path(), &branch, "landed.rs");

    assert_eq!(
        fast_forward_now(&jj, &store, &branch),
        OriginFastForward::Advanced {
            from: before,
            to: landed.clone()
        }
    );
    assert_eq!(bookmark_commit(&jj, &store, &branch), Some(landed.clone()));
    assert_eq!(
        git_stdout(
            project.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        landed,
        "the advance is exported"
    );
    assert_eq!(
        revset_commit(&jj, &store, &format!("{branch}@origin")),
        Some(landed.clone()),
        "the store's view of origin agrees with origin"
    );
    assert!(
        cairn_platform::env::git()
            .args([
                "rev-parse",
                "--verify",
                "--quiet",
                &origin_observation_ref(&branch)
            ])
            .current_dir(project.path())
            .output()
            .map(|output| !output.status.success())
            .unwrap(),
        "a promoted observation is not left behind as a pin"
    );
    assert_eq!(
        fast_forward_now(&jj, &store, &branch),
        OriginFastForward::Current { local: landed },
        "a second look finds nothing to take"
    );
}

#[test]
#[serial_test::serial(jj)]
fn a_diverged_origin_copy_moves_nothing_and_never_reaches_an_import() {
    let Some((_home, project, origin, _workspaces, jj, store, branch, workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let local = seal_and_export(&jj, &store, &workspace, &branch, "unpublished.rs");
    let remote = advance_origin_outside(origin.path(), &branch, "outside.rs");

    assert_eq!(
        fast_forward_now(&jj, &store, &branch),
        OriginFastForward::Diverged {
            local: local.clone(),
            remote: remote.clone()
        }
    );
    assert_eq!(bookmark_commit(&jj, &store, &branch), Some(local.clone()));
    assert_ne!(
        git_stdout(
            project.path(),
            &["rev-parse", &format!("refs/remotes/origin/{branch}")]
        ),
        remote,
        "the diverged copy is parked, not written where imports read"
    );
    import_git(&jj, &store).unwrap();
    assert!(
        !bookmark_name_is_conflicted(&jj, &store, &branch).unwrap(),
        "a later ordinary import must not turn the branch into a conflicted name"
    );
    assert_eq!(bookmark_commit(&jj, &store, &branch), Some(local));
    assert!(
        !observation_exists(project.path(), &branch),
        "the observation ref is deleted after a Diverged answer too"
    );
    git(
        project.path(),
        &["cat-file", "-e", &format!("{remote}^{{commit}}")],
    );
}

fn observation_exists(project: &Path, branch: &str) -> bool {
    cairn_platform::env::git()
        .args([
            "rev-parse",
            "--verify",
            "--quiet",
            &origin_observation_ref(branch),
        ])
        .current_dir(project)
        .output()
        .map(|output| output.status.success())
        .unwrap()
}

#[test]
#[serial_test::serial(jj)]
fn a_store_ahead_of_origin_is_current_and_an_absent_branch_is_none() {
    let Some((_home, project, _origin, _workspaces, jj, store, branch, workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let local = seal_and_export(&jj, &store, &workspace, &branch, "ahead.rs");
    assert_eq!(
        fast_forward_now(&jj, &store, &branch),
        OriginFastForward::Current {
            local: local.clone()
        }
    );
    assert_eq!(bookmark_commit(&jj, &store, &branch), Some(local));
    assert!(
        !observation_exists(project.path(), &branch),
        "the observation ref is deleted after a Current answer"
    );
    assert_eq!(
        fetch_origin_observation(&store, "agent/never-published").unwrap(),
        None
    );
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

#[test]
#[serial_test::serial(jj)]
fn a_rewritten_publication_on_origin_is_superseded_and_published_sideways_on_a_lease() {
    let Some((_home, _project, origin, workspaces, jj, store, branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let published = bookmark_commit(&jj, &store, &branch).unwrap();
    let rewritten = rewrite_published_branch(&jj, &store, workspaces.path(), &branch);
    assert_ne!(rewritten, published);

    assert!(origin_copy_is_superseded(&jj, &store, &branch, &rewritten, &published).unwrap());
    assert_eq!(
        fast_forward_now(&jj, &store, &branch),
        OriginFastForward::Superseded {
            local: rewritten.clone(),
            remote: published.clone()
        },
        "origin's copy is the store's own replaced history, not a divergence"
    );
    assert_eq!(
        bookmark_commit(&jj, &store, &branch),
        Some(rewritten.clone())
    );

    push_store_bookmark_classified(&jj, &store, &branch).unwrap();
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        rewritten
    );
}

#[test]
#[serial_test::serial(jj)]
fn a_superseded_judgement_never_outlives_origin_moving_the_push_is_leased() {
    let Some((_home, _project, origin, workspaces, jj, store, branch, _workspace)) =
        stale_publication_fixture()
    else {
        return;
    };
    let published = bookmark_commit(&jj, &store, &branch).unwrap();
    let rewritten = rewrite_published_branch(&jj, &store, workspaces.path(), &branch);
    // Origin gains a commit the store has not seen. From what the store knows,
    // origin still holds its replaced publication...
    let outside = advance_origin_outside(origin.path(), &branch, "outside.rs");
    assert!(origin_copy_is_superseded(&jj, &store, &branch, &rewritten, &published).unwrap());
    // ...so the push is what has to refuse, and it does: it is leased on that.
    let error = push_store_bookmark_classified(&jj, &store, &branch).unwrap_err();
    assert!(
        matches!(error, StoreBookmarkPushError::StaleRemote(_)),
        "{error}"
    );
    assert_eq!(
        git_stdout(
            origin.path(),
            &["rev-parse", &format!("refs/heads/{branch}")]
        ),
        outside,
        "the outside commit survives"
    );
    // And once the outside commit is observed, nothing calls it superseded.
    assert_eq!(
        fast_forward_now(&jj, &store, &branch),
        OriginFastForward::Diverged {
            local: rewritten,
            remote: outside
        }
    );
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
