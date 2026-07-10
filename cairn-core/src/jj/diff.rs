//! Changed-file derivation from the live jj graph and the `git diff --git`
//! parser it depends on.
use super::*;
use std::path::Path;

/// One changed file derived from the live sealed jj graph: its repo-relative
/// path, status, and `+`/`-` line counts, plus the previous path for a rename.
/// The substrate for the node `/changed` projection, which derives the changed
/// set from the graph ([`node_changed_files`]) rather than the best-effort
/// `file_changes` cache, so a just-sealed commit's file is never omitted the way
/// the decoupled async cache insert could lag or drop it (CAIRN-2101).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphFileChange {
    pub path: String,
    pub previous_path: Option<String>,
    /// `added` | `modified` | `deleted` | `renamed` — the same vocabulary the
    /// `file_changes` cache records, so the rendered table reads identically
    /// whichever source produced it.
    pub status: String,
    pub additions: i32,
    pub deletions: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCommit {
    pub commit_id: String,
    pub change_id: String,
    pub description: String,
    pub author: String,
    pub timestamp: String,
    pub working_copy: bool,
}

/// Cumulative changed files of a workspace against its recorded base, read from
/// the live sealed jj graph rather than the side-channel `file_changes` cache.
///
/// Runs `jj diff --git -r '<base>..@'` from `ws`. The range revset is what makes
/// this both correct and base-advance-resilient:
///
/// - It spans every sealed commit on the node's branch AND the loose edits in
///   `@` (jj snapshots the working copy into `@`), so a just-sealed file can
///   never lag the way the async cache insert could — the bug this fixes.
/// - `base..@` is the node's OWN commits even when the base advanced and `@` has
///   not yet rebased onto the new tip; a `--from base --to @` tree diff would
///   instead pollute the result with the base-advance deltas (verified against
///   jj 0.42).
///
/// `--ignore-working-copy` reads the last-recorded `@` without taking the
/// working-copy lock, so this read-only projection never contends with the live
/// agent's own jj operations (the same trade-off as [`list_files`]: an edit made
/// since the last jj op won't show until the next snapshot, which the agent
/// takes on nearly every operation).
///
/// Returns `None` when `ws` is not a jj workspace or neither base anchor
/// resolves, so the caller falls back to the recorded cache (e.g. a torn-down
/// workspace whose only surviving record is the DB).
pub fn node_changed_files(
    jj: &JjEnv,
    ws: &Path,
    base_branch: Option<&str>,
    base_commit: Option<&str>,
) -> Option<Vec<GraphFileChange>> {
    if !is_jj_dir(ws) {
        return None;
    }
    node_range_patch(jj, ws, base_branch, base_commit).map(|patch| parse_git_diff(&patch))
}

/// Git-format cumulative patch for the node's effective `fork..@` range.
pub fn node_range_patch(
    jj: &JjEnv,
    ws: &Path,
    base_branch: Option<&str>,
    base_commit: Option<&str>,
) -> Option<String> {
    if !is_jj_dir(ws) {
        return None;
    }
    let fork = resolve_node_fork_point(jj, ws, base_branch, base_commit)?;
    let revset = format!("{fork}..@");
    jj.run(
        ws,
        &["diff", "--ignore-working-copy", "--git", "-r", &revset],
        "jj diff --git (node range)",
    )
    .ok()
}

const RANGE_COMMIT_TEMPLATE: &str = "commit_id.short() ++ \"\\x1f\" ++ change_id.short() ++ \"\\x1f\" ++ description.first_line() ++ \"\\x1f\" ++ author.name() ++ \" <\" ++ author.email() ++ \">\" ++ \"\\x1f\" ++ author.timestamp() ++ \"\\x1f\" ++ if(empty, \"1\", \"0\") ++ \"\\n\"";

/// Commits in `fork..@`, including a non-empty working-copy commit.
pub fn range_commits(jj: &JjEnv, ws: &Path, fork: &str) -> Result<Vec<RangeCommit>, String> {
    let working_copy_id = jj
        .run(
            ws,
            &[
                "log",
                "--ignore-working-copy",
                "-r",
                "@",
                "--no-graph",
                "-T",
                "commit_id.short()",
            ],
            "jj log working copy id",
        )?
        .trim()
        .to_string();
    let revset = format!("{fork}..@");
    let output = jj.run(
        ws,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            &revset,
            "--no-graph",
            "-T",
            RANGE_COMMIT_TEMPLATE,
        ],
        "jj log node range commits",
    )?;
    Ok(parse_range_commits(&output, &working_copy_id))
}

fn parse_range_commits(output: &str, working_copy_id: &str) -> Vec<RangeCommit> {
    output
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\u{1f}');
            let commit_id = fields.next()?.trim().to_string();
            let change_id = fields.next()?.trim().to_string();
            let description = fields.next().unwrap_or_default().trim().to_string();
            let author = fields.next().unwrap_or_default().trim().to_string();
            let timestamp = fields.next().unwrap_or_default().trim().to_string();
            let empty = fields.next().unwrap_or_default().trim() == "1";
            let working_copy = commit_id == working_copy_id;
            (!(commit_id.is_empty() || working_copy && empty)).then_some(RangeCommit {
                commit_id,
                change_id,
                description,
                author,
                timestamp,
                working_copy,
            })
        })
        .collect()
}

/// Resolve the node's current effective fork point from the live jj graph.
///
/// The recorded `base_commit`/`pack_anchor` is the original fork point. That is
/// not necessarily where the workspace is currently based: default-branch
/// reconciliation can rebase the node onto `<base>@origin`, while local/manual
/// advancement can move the local bookmark first. Rather than trusting one stale
/// reference, resolve every base form that exists and choose the newest commit
/// common to `@` and any of those bases. That keeps `/changed` and live PR diffs
/// measuring only the node's own commits whether the node was rebased or the base
/// advanced without the node.
///
/// Returns `None` when no base candidate resolves, so callers keep their existing
/// cache or anchor fallback rather than diffing against an empty revset, which
/// would dump the workspace's entire history. Lock-free via
/// `--ignore-working-copy`.
pub fn resolve_node_fork_point(
    jj: &JjEnv,
    ws: &Path,
    base_branch: Option<&str>,
    base_commit: Option<&str>,
) -> Option<String> {
    let mut candidates: Vec<String> = Vec::new();
    if let Some(branch) = base_branch.filter(|s| !s.is_empty()) {
        candidates.push(format!("{branch}@origin"));
        candidates.push(format!("bookmarks(exact:{branch:?})"));
    }
    if let Some(commit) = base_commit.filter(|s| !s.is_empty()) {
        candidates.push(commit.to_string());
    }

    let resolved: Vec<String> = candidates
        .into_iter()
        .filter(|rev| changed_base_resolves(jj, ws, rev))
        .collect();
    if resolved.is_empty() {
        return None;
    }

    let union = resolved.join(" | ");
    let revset = format!("heads(::@ & ::({union}))");
    // A criss-cross graph can produce multiple heads here. Taking the first is
    // intentionally git-like: callers need a stable base, and any merge-base is
    // a valid common ancestor for this defensive diff.
    jj.run(
        ws,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            &revset,
            "--no-graph",
            "-T",
            "commit_id ++ \"\n\"",
        ],
        "jj resolve node fork point",
    )
    .ok()
    .and_then(|s| {
        s.lines()
            .map(str::trim)
            .find(|line| !line.is_empty())
            .map(str::to_string)
    })
}

/// Whether `rev` resolves to a commit in the store, read lock-free. An exact
/// bookmark that does not exist resolves to the empty set (empty stdout, exit
/// 0), which this reports as unresolved.
fn changed_base_resolves(jj: &JjEnv, ws: &Path, rev: &str) -> bool {
    jj.run(
        ws,
        &[
            "log",
            "--ignore-working-copy",
            "-r",
            rev,
            "--no-graph",
            "-T",
            "commit_id",
        ],
        "jj resolve (node changed base)",
    )
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// Parse `jj diff --git` (standard git unified-diff) output into structured
/// per-file changes. Status comes from the rename markers and the `/dev/null`
/// side of the `---`/`+++` headers; `+`/`-` lines inside hunks are counted for
/// the line totals. Pure (no jj invocation), so the risky bit carries its own
/// unit tests.
pub(crate) fn parse_git_diff(diff: &str) -> Vec<GraphFileChange> {
    let mut files: Vec<GraphFileChange> = Vec::new();
    let mut block: Option<DiffBlock> = None;
    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            if let Some(done) = block.take() {
                files.push(done.finish());
            }
            block = Some(DiffBlock::new(rest));
            continue;
        }
        let Some(b) = block.as_mut() else { continue };
        if line.starts_with("@@") {
            // First hunk header: everything after is content, where a leading
            // `+`/`-` is an added/removed line rather than a file header.
            b.in_hunk = true;
            continue;
        }
        if b.in_hunk {
            if line.starts_with('+') {
                b.additions += 1;
            } else if line.starts_with('-') {
                b.deletions += 1;
            }
            continue;
        }
        // Header region (before the first hunk): file-level metadata only.
        if let Some(p) = line.strip_prefix("rename from ") {
            b.renamed = true;
            b.old_path = Some(unquote_diff_path(p));
        } else if let Some(p) = line.strip_prefix("rename to ") {
            b.renamed = true;
            b.new_path = Some(unquote_diff_path(p));
        } else if line.starts_with("new file mode") {
            b.added = true;
        } else if line.starts_with("deleted file mode") {
            b.deleted = true;
        } else if let Some(p) = line.strip_prefix("--- ") {
            if p == "/dev/null" {
                b.added = true;
            } else {
                b.old_path = Some(strip_diff_prefix(p));
            }
        } else if let Some(p) = line.strip_prefix("+++ ") {
            if p == "/dev/null" {
                b.deleted = true;
            } else {
                b.new_path = Some(strip_diff_prefix(p));
            }
        }
    }
    if let Some(done) = block.take() {
        files.push(done.finish());
    }
    files
}

/// Public wrapper over [`parse_git_diff`]: turn a captured `git`/`jj diff --git`
/// patch into structured [`GraphFileChange`] rows. Lets callers outside `jj`
/// (the run-path commit barrier) record a just-sealed commit's file changes from
/// the working-copy patch captured before the seal, feeding the same
/// `file_changes` cache the write path records into.
pub fn parse_git_patch(diff: &str) -> Vec<GraphFileChange> {
    parse_git_diff(diff)
}

/// Accumulator for one `diff --git` file block while [`parse_git_diff`] scans.
struct DiffBlock {
    header_old: Option<String>,
    header_new: Option<String>,
    old_path: Option<String>,
    new_path: Option<String>,
    renamed: bool,
    added: bool,
    deleted: bool,
    in_hunk: bool,
    additions: i32,
    deletions: i32,
}

impl DiffBlock {
    fn new(header: &str) -> Self {
        let (header_old, header_new) = parse_diff_header_paths(header);
        DiffBlock {
            header_old,
            header_new,
            old_path: None,
            new_path: None,
            renamed: false,
            added: false,
            deleted: false,
            in_hunk: false,
            additions: 0,
            deletions: 0,
        }
    }

    fn finish(self) -> GraphFileChange {
        let new_path = self.new_path.or(self.header_new);
        let old_path = self.old_path.or(self.header_old);
        let (status, path, previous_path) = if self.renamed {
            (
                "renamed",
                new_path.or_else(|| old_path.clone()).unwrap_or_default(),
                old_path,
            )
        } else if self.added {
            ("added", new_path.or(old_path).unwrap_or_default(), None)
        } else if self.deleted {
            ("deleted", old_path.or(new_path).unwrap_or_default(), None)
        } else {
            ("modified", new_path.or(old_path).unwrap_or_default(), None)
        };
        GraphFileChange {
            path,
            previous_path,
            status: status.to_string(),
            additions: self.additions,
            deletions: self.deletions,
        }
    }
}

/// Split a `diff --git a/X b/Y` header tail into (old, new) paths with the
/// `a/`/`b/` prefixes stripped. Whitespace-split is unambiguous for the common
/// no-space case; quoted/spaced paths fall back on the more reliable
/// `---`/`+++`/`rename` lines, so this is only a backstop for hunkless entries
/// (binary or pure mode changes).
fn parse_diff_header_paths(header: &str) -> (Option<String>, Option<String>) {
    let tokens: Vec<&str> = header.split_whitespace().collect();
    if tokens.len() == 2 {
        (
            Some(strip_diff_prefix(tokens[0])),
            Some(strip_diff_prefix(tokens[1])),
        )
    } else {
        (None, None)
    }
}

/// Strip a leading `a/`/`b/` diff prefix, then any surrounding quotes git adds
/// for paths with special characters.
fn strip_diff_prefix(path: &str) -> String {
    let path = path
        .strip_prefix("a/")
        .or_else(|| path.strip_prefix("b/"))
        .unwrap_or(path);
    unquote_diff_path(path)
}

/// Drop surrounding double quotes git adds around a path with special
/// characters. C-escapes inside are left as-is (rare; the path still renders
/// recognizably).
fn unquote_diff_path(path: &str) -> String {
    let trimmed = path.trim();
    trimmed
        .strip_prefix('"')
        .and_then(|p| p.strip_suffix('"'))
        .unwrap_or(trimmed)
        .to_string()
}

#[cfg(test)]
mod range_commit_tests {
    use super::*;

    #[test]
    fn range_commit_parser_labels_and_skips_empty_working_copy() {
        let input = "abc123\u{1f}change1\u{1f}sealed\u{1f}A <a@b>\u{1f}2026-01-01\u{1f}0\nwc123\u{1f}change2\u{1f}\u{1f}A <a@b>\u{1f}2026-01-02\u{1f}1\n";
        let commits = parse_range_commits(input, "wc123");
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].commit_id, "abc123");
        assert!(!commits[0].working_copy);
    }

    #[test]
    fn range_commit_parser_keeps_dirty_working_copy() {
        let input = "wc123\u{1f}change2\u{1f}work\u{1f}A <a@b>\u{1f}2026-01-02\u{1f}0\n";
        let commits = parse_range_commits(input, "wc123");
        assert_eq!(commits.len(), 1);
        assert!(commits[0].working_copy);
    }
}
