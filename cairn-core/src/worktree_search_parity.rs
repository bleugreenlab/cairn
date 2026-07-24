//! Cross-engine parity: the warm fff worktree index (cairn-symbols) must format
//! and filter its `?grep=`/`?glob=` output byte-identically to cairn-core's
//! canonical ripgrep walk. These tests live here, not in cairn-symbols, because
//! only cairn-core sees both engines: the fff index is `crate::worktree_search`
//! (re-exported from cairn-symbols) and the ripgrep reference is the mcp grep
//! handler in `crate::mcp::handlers::search`. cairn-symbols must never depend on
//! cairn-core, so the comparison is anchored on this side of the boundary.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::mcp::handlers::search::{
    glob_matched_paths_walk, grep_search, grep_search_native, grep_uncovered_files, GrepPayload,
    GrepWalkLimits,
};
use crate::worktree_search::{WorktreeGrepOutcome, WorktreeGrepParams, WorktreeSearch};
use cairn_symbols::search_util::render_grep_lines;

fn write_file(root: &Path, rel: &str, contents: &str) {
    let path = root.join(rel);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, contents).unwrap();
}

fn git_init(root: &Path) {
    // Worktrees are real git checkouts, so index a git repo to exercise the
    // same gitignore semantics fff sees in production.
    let ok = std::process::Command::new("git")
        .arg("init")
        .current_dir(root)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    assert!(ok, "git init failed in test worktree");
}

fn grep_payload(pattern: &str, glob: Option<&str>, output_mode: &str) -> GrepPayload {
    GrepPayload {
        pattern: pattern.to_string(),
        path: None,
        glob: glob.map(str::to_string),
        file_type: None,
        output_mode: Some(output_mode.to_string()),
        context: None,
        after_context: None,
        before_context: None,
        context_alias: None,
        case_insensitive: None,
        line_numbers: Some(true),
        head_limit: None,
        offset: None,
        multiline: None,
    }
}

fn warm_params(
    subdir: Option<&str>,
    glob: Option<&str>,
    output_mode: &str,
    before_context: usize,
    after_context: usize,
) -> WorktreeGrepParams {
    WorktreeGrepParams {
        pattern: "needle".to_string(),
        subdir: subdir.map(str::to_string),
        globs: glob.map(str::to_string).into_iter().collect(),
        max_per_file: None,
        output_mode: output_mode.to_string(),
        case_insensitive: None,
        before_context,
        after_context,
        show_line_numbers: true,
    }
}

/// Render a served outcome the way the live seam does, asserting the index
/// covered the whole query itself.
fn render_fully_covered(
    outcome: WorktreeGrepOutcome,
    params: &WorktreeGrepParams,
    native_output: bool,
) -> String {
    assert!(
        outcome.uncovered.is_empty(),
        "expected full index coverage, got uncovered {:?}",
        outcome.uncovered
    );
    render_grep_lines(
        outcome.lines,
        &params.output_mode,
        params.show_line_numbers,
        native_output,
        params.before_context > 0 || params.after_context > 0,
    )
}

fn warm_grep(
    search: &WorktreeSearch,
    subdir: Option<&str>,
    glob: Option<&str>,
    output_mode: &str,
    before_context: usize,
    after_context: usize,
    deny_read: &[PathBuf],
) -> String {
    let params = warm_params(subdir, glob, output_mode, before_context, after_context);
    let outcome = search
        .try_grep(&params, deny_read)
        .expect("warm grep served");
    render_fully_covered(outcome, &params, false)
}

fn cold_grep(
    root: &Path,
    glob: Option<&str>,
    output_mode: &str,
    before_context: usize,
    after_context: usize,
    deny_read: Vec<PathBuf>,
) -> String {
    let mut payload = grep_payload("needle", glob, output_mode);
    payload.before_context = (before_context > 0).then_some(before_context as u32);
    payload.after_context = (after_context > 0).then_some(after_context as u32);
    grep_search(payload, root, output_mode, true, deny_read).unwrap()
}

/// The warm grep rendered the way ripgrep renders it, for comparison against
/// the real binary's stdout.
fn warm_grep_native(
    search: &WorktreeSearch,
    before_context: usize,
    after_context: usize,
) -> String {
    let mut params = warm_params(None, None, "content", before_context, after_context);
    params.case_insensitive = Some(false);
    let outcome = search.try_grep(&params, &[]).expect("warm grep served");
    render_fully_covered(outcome, &params, true)
}

fn cold_grep_native(root: &Path, before_context: usize, after_context: usize) -> String {
    cold_grep_native_capped(root, before_context, after_context, None)
}

fn cold_grep_native_capped(
    root: &Path,
    before_context: usize,
    after_context: usize,
    max_per_file: Option<usize>,
) -> String {
    let mut payload = grep_payload("needle", None, "content");
    payload.case_insensitive = Some(false);
    payload.before_context = (before_context > 0).then_some(before_context as u32);
    payload.after_context = (after_context > 0).then_some(after_context as u32);
    grep_search_native(
        payload,
        root,
        "content",
        true,
        Vec::new(),
        GrepWalkLimits {
            globs: Vec::new(),
            max_per_file,
            timeout: Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        },
    )
    .unwrap()
}

/// ripgrep's `-m N` is not "stop at the Nth match". It keeps printing that
/// match's after-context window, and a line inside the window that itself
/// matches renders as a *match*, without counting toward the cap or extending
/// the window. The walk must reproduce all of that; fff's per-file cap can
/// express none of it, so the index declines the combination outright rather
/// than diverging from the engine it falls back to.
#[test]
fn a_capped_search_with_after_context_matches_real_rg() {
    if Command::new("rg").arg("--version").output().is_err() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    // `needle-two` is itself a match sitting inside the first match's window.
    write_file(
        root,
        "a.txt",
        "needle one\nplain\nneedle three\nplain two\n",
    );
    write_file(root, "b.txt", "needle-one\nneedle-two\ntail\n");
    write_file(
        root,
        "c.txt",
        "pre\nneedle a\nmid\nneedle b\nmid two\nneedle c\npost\n",
    );

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for (max, before, after) in [(1, 0, 1), (1, 0, 2), (1, 1, 1), (2, 1, 1), (2, 0, 1)] {
        let mut params = warm_params(None, None, "content", before, after);
        params.case_insensitive = Some(false);
        params.max_per_file = Some(max);
        assert!(
            search.try_grep(&params, &[]).is_none(),
            "the index must decline -m{max} with after-context rather than diverge"
        );

        let (stdout, stderr, status) = native_search(
            root,
            "rg",
            &[
                "-n",
                "--sort",
                "path",
                "-m",
                &max.to_string(),
                "-B",
                &before.to_string(),
                "-A",
                &after.to_string(),
                "needle",
            ],
        );
        assert!(stderr.is_empty());
        assert_eq!(status, 0);
        assert_eq!(
            raw_warm_stdout(cold_grep_native_capped(root, before, after, Some(max))),
            stdout,
            "-m{max} -B{before} -A{after}"
        );
    }

    // Without after-context the cap is a plain stop, and the index serves it.
    let mut params = warm_params(None, None, "content", 1, 0);
    params.case_insensitive = Some(false);
    params.max_per_file = Some(1);
    assert!(
        search.try_grep(&params, &[]).is_some(),
        "-m N with only before-context stays index-servable"
    );
}

/// `rg -m 0` asks for no matches at all: nothing on stdout, exit 1. Each
/// output mode's sink records a match *before* consulting the cap, so a zero
/// cap has to be short-circuited ahead of the search rather than counted down
/// to — and `files_with_matches` stops at the first hit without consulting the
/// cap at all.
#[test]
fn a_zero_cap_yields_no_matches_in_every_output_mode() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "a.txt", "needle one\nneedle two\n");

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for output_mode in ["content", "count", "files_with_matches"] {
        // Sanity: without the cap this fixture does match, so an empty result
        // below is the cap's doing and not an empty fixture.
        let mut params = warm_params(None, None, output_mode, 0, 0);
        params.case_insensitive = Some(false);
        assert!(
            !render_fully_covered(
                search.try_grep(&params, &[]).expect("served"),
                &params,
                true
            )
            .is_empty(),
            "{output_mode} matches without a cap"
        );

        params.max_per_file = Some(0);
        assert!(
            search.try_grep(&params, &[]).is_none(),
            "{output_mode}: the index declines a zero cap so the shim passes through"
        );

        let mut payload = grep_payload("needle", None, output_mode);
        payload.case_insensitive = Some(false);
        let walked = grep_search_native(
            payload,
            root,
            output_mode,
            true,
            Vec::new(),
            GrepWalkLimits {
                globs: Vec::new(),
                max_per_file: Some(0),
                timeout: Duration::from_secs(30),
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        )
        .unwrap();
        assert_eq!(walked, "", "{output_mode}: a zero cap yields no output");

        if Command::new("rg").arg("--version").output().is_ok() {
            let flag = match output_mode {
                "content" => "-n",
                "count" => "-c",
                _ => "-l",
            };
            let (stdout, _, status) = native_search(root, "rg", &[flag, "-m", "0", "needle"]);
            assert!(stdout.is_empty(), "{output_mode}: rg prints nothing");
            assert_eq!(status, 1, "{output_mode}: rg exits 1");
        }
    }
}

/// A tree whose matches sit on consecutive lines, within one context window of
/// each other, and far apart — every arrangement where per-match context
/// windows touch, overlap, or leave a gap.
fn write_dense_context_fixture(root: &Path) {
    git_init(root);
    write_file(
        root,
        "src/dense.txt",
        "one\nneedle two\nneedle three\nfour\nfive\nneedle six\nseven\n\
         eight\nnine\nten\neleven\nneedle twelve\nthirteen\n",
    );
    write_file(root, "src/edge.txt", "needle first\nsecond\n");
    write_file(root, "other.txt", "alpha\nbeta\nneedle gamma\n");
}

/// Overlapping context windows are where the two engines diverged: the index
/// rendered each match's window independently, so lines shared by two nearby
/// matches printed twice, while the walk's sink emitted them once. Both now
/// render from the same coordinate-deduped line model.
#[test]
fn overlapping_and_asymmetric_context_windows_match_the_walk() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_dense_context_fixture(root);

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for (before, after) in [(1, 1), (2, 2), (3, 3), (0, 3), (3, 0), (1, 4), (5, 1)] {
        let warm = warm_grep(&search, None, None, "content", before, after, &[]);
        let cold = cold_grep(root, None, "content", before, after, Vec::new());
        assert_eq!(warm, cold, "-B{before} -A{after}");
        // Equality alone would not catch the duplication bug if both engines
        // duplicated identically, so assert the coordinates are distinct too.
        let mut coordinates: Vec<String> = warm
            .lines()
            .filter(|line| *line != "--")
            .map(coordinate_of)
            .collect();
        let emitted = coordinates.len();
        coordinates.sort();
        coordinates.dedup();
        assert_eq!(
            coordinates.len(),
            emitted,
            "a line was emitted twice with -B{before} -A{after}: {warm}"
        );
    }
}

/// The `path:N` coordinate of one rendered grep line, in either separator form
/// (`path:N:text` for a match, `path:N-text` for context).
fn coordinate_of(line: &str) -> String {
    let (path, rest) = line.split_once(':').unwrap_or((line, ""));
    let number: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    format!("{path}:{number}")
}

/// Three-way byte equality on the case that used to differ. Skipped when `rg`
/// is not installed.
#[test]
fn context_output_is_byte_identical_across_both_engines_and_real_rg() {
    if Command::new("rg").arg("--version").output().is_err() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    write_dense_context_fixture(root);

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for (before, after) in [(1, 1), (2, 2), (0, 2), (2, 0)] {
        // `--sort path` makes rg's own file ordering deterministic; both Cairn
        // engines already order by path through the shared renderer.
        let (stdout, stderr, status) = native_search(
            root,
            "rg",
            &[
                "-n",
                "--sort",
                "path",
                "-B",
                &before.to_string(),
                "-A",
                &after.to_string(),
                "needle",
            ],
        );
        assert!(stderr.is_empty());
        assert_eq!(status, 0);
        assert_eq!(
            raw_warm_stdout(warm_grep_native(&search, before, after)),
            stdout,
            "warm -B{before} -A{after}"
        );
        assert_eq!(
            raw_warm_stdout(cold_grep_native(root, before, after)),
            stdout,
            "cold -B{before} -A{after}"
        );
    }
}

fn glob_paths_from_walk(root: &Path, pattern: &str, deny_read: Vec<PathBuf>) -> Vec<PathBuf> {
    glob_matched_paths_walk(root.to_path_buf(), pattern.to_string(), deny_read)
        .unwrap()
        .0
}

fn path_set(paths: Vec<PathBuf>) -> HashSet<String> {
    paths
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

/// A non-binary file above the mmap-safe cap is one fff never searches
/// (CAIRN-2574 crash-proofing). It used to discard the whole query; now the
/// index serves everything else and names just that file, so the caller
/// re-greps one file instead of re-walking the tree.
#[test]
fn oversized_file_is_named_uncovered_and_the_rest_is_served() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "small.rs", "needle small\n");
    let big = format!("needle big\n{}\n", "abcd ".repeat(80_000));
    write_file(root, "big.rs", &big);

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    let params = warm_params(None, None, "files_with_matches", 0, 0);
    let outcome = search
        .try_grep(&params, &[])
        .expect("an oversized file no longer discards the query");
    assert_eq!(outcome.uncovered, vec!["big.rs".to_string()]);
    assert_eq!(
        render_grep_lines(outcome.lines, "files_with_matches", true, false, false),
        "small.rs"
    );

    // The cold walk remains the reference for the complete answer.
    let cold = cold_grep(root, None, "files_with_matches", 0, 0, Vec::new());
    let files: HashSet<&str> = cold.lines().collect();
    assert!(
        files.contains("small.rs"),
        "cold walk finds small: {cold:?}"
    );
    assert!(files.contains("big.rs"), "cold walk finds big: {cold:?}");
}

/// A file whose matching line is past fff's display cap cannot be rendered
/// faithfully from the index, so only that file is handed back — and its
/// already-drained matches are dropped so the caller's re-grep does not
/// duplicate them.
#[test]
fn a_truncating_line_makes_only_its_own_file_uncovered() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "short.rs", "let needle = 1;\n");
    write_file(
        root,
        "long.rs",
        &format!("let needle = \"{}\";\n", "a".repeat(700)),
    );

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    let params = warm_params(None, None, "content", 0, 0);
    let outcome = search.try_grep(&params, &[]).expect("partially served");
    assert_eq!(outcome.uncovered, vec!["long.rs".to_string()]);
    assert_eq!(
        render_grep_lines(outcome.lines, "content", true, false, false),
        "short.rs:1:let needle = 1;",
        "the truncating file's drained matches are dropped for the re-grep"
    );

    // Display truncation does not change whether a file matched or how many
    // matches it holds, so `-l` and `-c` stay fully covered.
    for output_mode in ["files_with_matches", "count"] {
        let params = warm_params(None, None, output_mode, 0, 0);
        let warm = search
            .try_grep(&params, &[])
            .expect("a long line does not poison -l/-c");
        assert!(
            warm.uncovered.is_empty(),
            "{output_mode} should be fully covered: {:?}",
            warm.uncovered
        );
        assert_eq!(
            render_grep_lines(warm.lines, output_mode, true, false, false),
            cold_grep(root, None, output_mode, 0, 0, Vec::new()),
            "{output_mode}"
        );
    }
}

/// The merged answer — index lines plus the caller's re-grep of the uncovered
/// files — must equal the cold walk and real `rg`, ordering and all. This is
/// the whole point of returning lines rather than rendered bytes.
#[test]
fn partial_coverage_merges_into_the_same_bytes_as_the_walk_and_real_rg() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "a-small.txt", "alpha\nneedle one\nbeta\n");
    write_file(root, "z-small.txt", "needle last\n");
    // Sorts between the two small files, so a merge that simply appended the
    // re-grepped lines would order them wrongly.
    write_file(
        root,
        "m-big.txt",
        &format!("filler\nneedle big\n{}\n", "abcd ".repeat(80_000)),
    );

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for (before, after) in [(0, 0), (1, 1)] {
        let mut params = warm_params(None, None, "content", before, after);
        params.case_insensitive = Some(false);
        let outcome = search.try_grep(&params, &[]).expect("partially served");
        assert_eq!(outcome.uncovered, vec!["m-big.txt".to_string()]);

        // Stand in for the caller's merge: re-grep the uncovered file and
        // render the combined line set once.
        let mut lines = outcome.lines;
        let mut payload = grep_payload("needle", None, "content");
        payload.case_insensitive = Some(false);
        payload.before_context = (before > 0).then_some(before as u32);
        payload.after_context = (after > 0).then_some(after as u32);
        lines.extend(
            grep_uncovered_files(
                &payload,
                root,
                &outcome.uncovered,
                "content",
                None,
                Duration::from_secs(30),
            )
            .unwrap(),
        );
        let merged = render_grep_lines(lines, "content", true, true, before > 0 || after > 0);

        assert_eq!(
            merged,
            cold_grep_native(root, before, after),
            "merged -B{before} -A{after} equals the walk"
        );
        if Command::new("rg").arg("--version").output().is_ok() {
            let (stdout, _, _) = native_search(
                root,
                "rg",
                &[
                    "-n",
                    "--sort",
                    "path",
                    "-B",
                    &before.to_string(),
                    "-A",
                    &after.to_string(),
                    "needle",
                ],
            );
            assert_eq!(raw_warm_stdout(merged), stdout);
        }
    }

    assert!(
        search.served_grep_query_count() > 0,
        "the index served these queries rather than bailing"
    );
}

#[test]
fn index_grep_glob_filter_matches_walk_override_semantics() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "src/main.rs", "needle root rust\n");
    write_file(root, "src/lib.RS", "needle upper rust\n");
    write_file(root, "src/nested/deep.rs", "needle nested rust\n");
    write_file(root, "logs/error.log", "needle log\n");
    write_file(root, "notes.txt", "needle text\n");

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for glob in ["*.RS", "!*.log", "src/*.rs"] {
        let warm = warm_grep(&search, None, Some(glob), "files_with_matches", 0, 0, &[]);
        let cold = cold_grep(root, Some(glob), "files_with_matches", 0, 0, Vec::new());
        assert_eq!(warm, cold, "root glob {glob:?} matches cold walk");
    }

    for glob in ["*.RS", "!*.log", "nested/*.rs"] {
        let warm = warm_grep(
            &search,
            Some("src"),
            Some(glob),
            "files_with_matches",
            0,
            0,
            &[],
        );
        let cold = cold_grep(
            &root.join("src"),
            Some(glob),
            "files_with_matches",
            0,
            0,
            Vec::new(),
        );
        assert_eq!(warm, cold, "subdir glob {glob:?} matches cold walk");
    }
}

#[test]
fn index_grep_subdir_matches_walk_and_rebases_paths() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "src/visible.rs", "before\nneedle\nafter\n");
    write_file(root, "src/visible.txt", "needle text\n");
    write_file(root, "src/secret/hidden.rs", "needle hidden\n");
    write_file(root, "other.rs", "needle outside\n");

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for output_mode in ["content", "files_with_matches", "count"] {
        let warm = warm_grep(&search, Some("src"), Some("*.rs"), output_mode, 0, 0, &[]);
        let cold = cold_grep(
            &root.join("src"),
            Some("*.rs"),
            output_mode,
            0,
            0,
            Vec::new(),
        );
        assert_eq!(warm, cold, "subdir {output_mode} matches cold walk");
        assert!(
            !warm.contains("src/"),
            "subdir results are relative to the subdir root: {warm:?}"
        );
    }

    let warm_context = warm_grep(&search, Some("src"), Some("*.rs"), "content", 1, 1, &[]);
    let cold_context = cold_grep(&root.join("src"), Some("*.rs"), "content", 1, 1, Vec::new());
    assert_eq!(
        warm_context, cold_context,
        "subdir context matches cold walk"
    );

    let fenced = warm_grep(
        &search,
        Some("src"),
        Some("*.rs"),
        "files_with_matches",
        0,
        0,
        &[root.join("src/secret")],
    );
    assert!(fenced.contains("visible.rs"));
    assert!(!fenced.contains("hidden.rs"));
}

#[test]
fn index_glob_subdir_matches_walk_and_rebases_paths() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "src/visible.rs", "\n");
    write_file(root, "src/nested/deep.rs", "\n");
    write_file(root, "src/hidden.log", "\n");
    write_file(root, "outside.rs", "\n");

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    let walk = path_set(glob_paths_from_walk(&root.join("src"), "*.rs", Vec::new()));
    let indexed = path_set(search.try_glob("*.rs", Some("src"), &[]).unwrap());
    assert_eq!(indexed, walk);
    assert!(indexed.contains("visible.rs"));
    assert!(indexed.contains("nested/deep.rs"));
    assert!(!indexed.contains("src/visible.rs"));

    // A slash-containing pattern is relative to the subdir: `nested/*.rs`
    // under `src` must match `src/nested/deep.rs` (rebased to `nested/deep.rs`),
    // exactly as the walk rooted at `src` does. This is the case that regresses
    // if fff is handed the subdir-relative pattern against worktree-root paths.
    let slash_walk = path_set(glob_paths_from_walk(
        &root.join("src"),
        "nested/*.rs",
        Vec::new(),
    ));
    let slash_indexed = path_set(search.try_glob("nested/*.rs", Some("src"), &[]).unwrap());
    assert_eq!(slash_indexed, slash_walk);
    assert_eq!(slash_indexed, HashSet::from(["nested/deep.rs".to_string()]));

    let fenced = path_set(
        search
            .try_glob("*.rs", Some("src"), &[root.join("src/nested")])
            .unwrap(),
    );
    assert_eq!(fenced, HashSet::from(["visible.rs".to_string()]));
}

#[test]
fn index_glob_matches_walk_sets_with_exact_refiltering() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "alpha.rs", "root\n");
    write_file(root, "src/beta.rs", "nested\n");
    write_file(root, "src/nested/gamma.rs", "nested\n");
    write_file(root, "src/Upper.RS", "case\n");
    write_file(root, "src/upper.rs", "lower\n");
    write_file(root, "ignored.rs", "ignored\n");
    write_file(root, ".gitignore", "ignored.rs\n");

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    for pattern in ["**/*.rs", "*.rs", "src/*.RS"] {
        let walk = path_set(glob_paths_from_walk(root, pattern, Vec::new()));
        let indexed = path_set(
            search
                .try_glob(pattern, None, &[])
                .expect("warm glob served"),
        );
        assert_eq!(indexed, walk, "pattern {pattern:?} matches walk exactly");
    }

    let upper = search.try_glob("src/*.RS", None, &[]).unwrap();
    assert_eq!(upper, vec![PathBuf::from("src/Upper.RS")]);
}

fn native_search(root: &Path, program: &str, args: &[&str]) -> (Vec<u8>, Vec<u8>, i32) {
    let output = Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .unwrap();
    (
        output.stdout,
        output.stderr,
        output.status.code().unwrap_or(-1),
    )
}

fn raw_warm_stdout(body: String) -> Vec<u8> {
    if body.is_empty() {
        Vec::new()
    } else {
        format!("{body}\n").into_bytes()
    }
}

#[test]
fn warm_content_stdout_and_status_match_native_rg_and_recursive_grep() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "src/a.txt", "Needle upper\nneedle lower\n");
    write_file(root, "src/b.txt", "absent\n");

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    let rg_body = warm_grep_native(&search, 0, 0);
    let (stdout, stderr, status) = native_search(root, "rg", &["-n", "needle"]);
    assert_eq!(raw_warm_stdout(rg_body), stdout);
    assert!(stderr.is_empty());
    assert_eq!(status, 0);

    let grep_body = warm_grep_native(&search, 0, 0)
        .lines()
        .map(|line| format!("./{line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let (stdout, stderr, status) = native_search(root, "grep", &["-rn", "needle", "."]);
    assert_eq!(raw_warm_stdout(grep_body), stdout);
    assert!(stderr.is_empty());
    assert_eq!(status, 0);

    let mut params = warm_params(None, None, "content", 0, 0);
    params.pattern = "missing".to_string();
    params.case_insensitive = Some(false);
    params.show_line_numbers = false;
    let no_match = render_fully_covered(search.try_grep(&params, &[]).unwrap(), &params, true);
    let (stdout, stderr, status) = native_search(root, "rg", &["missing"]);
    assert_eq!(raw_warm_stdout(no_match), stdout);
    assert!(stderr.is_empty());
    assert_eq!(status, 1);
}
