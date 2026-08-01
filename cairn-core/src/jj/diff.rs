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
    pub(crate) path: String,
    pub(crate) previous_path: Option<String>,
    /// `added` | `modified` | `deleted` | `renamed` — the same vocabulary the
    /// `file_changes` cache records, so the rendered table reads identically
    /// whichever source produced it.
    pub(crate) status: String,
    pub(crate) additions: i32,
    pub(crate) deletions: i32,
}

/// Changed files between two immutable logical coordinates. Unlike
/// [`node_changed_files`], this never consults the workspace `@` commit.
pub(crate) fn logical_changed_files(
    jj: &JjEnv,
    repository: &Path,
    base: &str,
    head: &str,
) -> Option<Vec<GraphFileChange>> {
    if !is_jj_dir(repository) {
        return None;
    }
    let revset = format!("{base}..{head}");
    jj.run(
        repository,
        &["diff", "--ignore-working-copy", "--git", "-r", &revset],
        "jj diff --git (logical range)",
    )
    .ok()
    .map(|patch| parse_git_diff(&patch))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCommit {
    pub(crate) commit_id: String,
    pub(crate) change_id: String,
    pub(crate) description: String,
    pub(crate) author: String,
    pub(crate) timestamp: String,
    pub(crate) working_copy: bool,
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
pub(crate) fn parse_git_patch(diff: &str) -> Vec<GraphFileChange> {
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
