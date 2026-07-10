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
use std::time::Duration;

use crate::mcp::handlers::search::{glob_matched_paths_walk, grep_search, GrepPayload};
use crate::worktree_search::{WorktreeGrepParams, WorktreeSearch};

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

fn warm_grep(
    search: &WorktreeSearch,
    subdir: Option<&str>,
    glob: Option<&str>,
    output_mode: &str,
    before_context: usize,
    after_context: usize,
    deny_read: &[PathBuf],
) -> String {
    search
        .try_grep(
            &WorktreeGrepParams {
                pattern: "needle".to_string(),
                subdir: subdir.map(str::to_string),
                globs: glob.map(str::to_string).into_iter().collect(),
                max_per_file: None,
                output_mode: output_mode.to_string(),
                case_insensitive: None,
                before_context,
                after_context,
                show_line_numbers: true,
            },
            deny_read,
        )
        .expect("warm grep served")
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

/// A non-binary file above the mmap-safe cap makes the warm index bail
/// (CAIRN-2574 crash-proofing), so the ripgrep walk the caller falls through to
/// must still find every match the warm path declines to serve. Proves the
/// completeness contract holds across the bail boundary.
#[test]
fn oversized_file_bails_warm_but_cold_walk_still_matches() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    git_init(root);
    write_file(root, "small.rs", "needle small\n");
    let big = format!("needle big\n{}\n", "abcd ".repeat(80_000));
    write_file(root, "big.rs", &big);

    let search = WorktreeSearch::new(root).unwrap();
    assert!(search.wait_for_scan(Duration::from_secs(15)));

    // Warm path: the oversized in-scope file forces a bail to the fallback.
    assert!(
        search
            .try_grep(
                &WorktreeGrepParams {
                    pattern: "needle".to_string(),
                    subdir: None,
                    globs: Vec::new(),
                    max_per_file: None,
                    output_mode: "files_with_matches".to_string(),
                    case_insensitive: None,
                    before_context: 0,
                    after_context: 0,
                    show_line_numbers: true,
                },
                &[],
            )
            .is_none(),
        "oversized in-scope file bails the warm index to the ripgrep fallback"
    );

    // Cold walk (the fallback the caller uses on None) still finds both files.
    let cold = cold_grep(root, None, "files_with_matches", 0, 0, Vec::new());
    let files: HashSet<&str> = cold.lines().collect();
    assert!(
        files.contains("small.rs"),
        "cold walk finds small: {cold:?}"
    );
    assert!(files.contains("big.rs"), "cold walk finds big: {cold:?}");
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

    let rg_body = search
        .try_grep(
            &WorktreeGrepParams {
                pattern: "needle".to_string(),
                subdir: None,
                globs: Vec::new(),
                max_per_file: None,
                output_mode: "content".to_string(),
                case_insensitive: Some(false),
                before_context: 0,
                after_context: 0,
                show_line_numbers: true,
            },
            &[],
        )
        .unwrap();
    let (stdout, stderr, status) = native_search(root, "rg", &["-n", "needle"]);
    assert_eq!(raw_warm_stdout(rg_body), stdout);
    assert!(stderr.is_empty());
    assert_eq!(status, 0);

    let grep_body = search
        .try_grep(
            &WorktreeGrepParams {
                pattern: "needle".to_string(),
                subdir: None,
                globs: Vec::new(),
                max_per_file: None,
                output_mode: "content".to_string(),
                case_insensitive: Some(false),
                before_context: 0,
                after_context: 0,
                show_line_numbers: true,
            },
            &[],
        )
        .unwrap();
    let grep_body = grep_body
        .lines()
        .map(|line| format!("./{line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let (stdout, stderr, status) = native_search(root, "grep", &["-rn", "needle", "."]);
    assert_eq!(raw_warm_stdout(grep_body), stdout);
    assert!(stderr.is_empty());
    assert_eq!(status, 0);

    let no_match = search
        .try_grep(
            &WorktreeGrepParams {
                pattern: "missing".to_string(),
                subdir: None,
                globs: Vec::new(),
                max_per_file: None,
                output_mode: "content".to_string(),
                case_insensitive: Some(false),
                before_context: 0,
                after_context: 0,
                show_line_numbers: false,
            },
            &[],
        )
        .unwrap();
    let (stdout, stderr, status) = native_search(root, "rg", &["missing"]);
    assert_eq!(raw_warm_stdout(no_match), stdout);
    assert!(stderr.is_empty());
    assert_eq!(status, 1);
}
