//! Reconstruct a batch's unified diff from its archived commit.
//!
//! A `gitcoord` write event drops the heavy inline diff and keeps only the batch
//! commit sha. Reconstruction regenerates a `git show <sha>`-equivalent unified
//! diff from the commit's tree against its first parent, resolved entirely
//! through the in-memory [`ObjectStore`] (no `.git`, no shelling out). The output
//! is byte-compatible with `git show --format= <sha>` for the file-content diff:
//! `diff --git` envelope, abbreviated `index` lines, `--- / +++` headers, and
//! context-grouped `@@` hunks.
//!
//! The line differ is a linear-space Myers alignment that, like git's xdiff,
//! marks changed lines per side and emits each change block's deletions before
//! its insertions, with difflib-style context grouping (3 lines, hunks merged
//! when their context windows touch). For the unambiguous edits an agent write
//! batch produces — added files, deleted files, and localized in-file edits —
//! this reproduces git's hunks exactly, verified against shelled `git show` in
//! tests. Memory is O(n+m) per file, and a work budget bounds the time on a file
//! that changed nearly everywhere (see [`ALIGN_WORK_BUDGET`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use gix_hash::{oid, ObjectId};
use gix_object::{CommitRefIter, Kind as ObjectKind, TreeRefIter};

use super::objects::ObjectStore;

const HASH_KIND: gix_hash::Kind = gix_hash::Kind::Sha1;

/// Unified-diff context lines, matching git's default.
const CONTEXT: usize = 3;

struct TreeEntry {
    mode: u16,
    oid: ObjectId,
    is_tree: bool,
}

/// List commits on the first-parent chain from `base` (exclusive) to `tip`
/// (inclusive), oldest first. Archived Git objects do not preserve jj change
/// ids, so callers render that field as unavailable.
pub fn list_range_commits(
    store: &ObjectStore,
    base_hex: &str,
    tip_hex: &str,
) -> Result<Vec<RangeCommit>, String> {
    const CAP: usize = 100_000;
    let base =
        ObjectId::from_hex(base_hex.as_bytes()).map_err(|e| format!("invalid base sha: {e}"))?;
    let mut current =
        ObjectId::from_hex(tip_hex.as_bytes()).map_err(|e| format!("invalid tip sha: {e}"))?;
    let mut commits = Vec::new();
    while current != base && commits.len() < CAP {
        let (kind, bytes) = store
            .resolve_object(&current)
            .ok_or_else(|| format!("missing commit {current}"))?;
        if kind != ObjectKind::Commit {
            return Err(format!("object {current} is not a commit"));
        }
        let iter = CommitRefIter::from_bytes(&bytes, HASH_KIND);
        let author = iter
            .author()
            .map_err(|e| format!("decoding commit author: {e}"))?;
        let author_name = String::from_utf8_lossy(author.name.as_ref())
            .trim()
            .to_string();
        let author_email = String::from_utf8_lossy(author.email.as_ref())
            .trim()
            .to_string();
        let timestamp = author.time().unwrap_or_default().seconds;
        let message = iter
            .message()
            .map_err(|e| format!("decoding commit message: {e}"))?;
        let summary = String::from_utf8_lossy(message)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        commits.push(RangeCommit {
            sha: current.to_string(),
            summary,
            author: if author_email.is_empty() {
                author_name
            } else {
                format!("{author_name} <{author_email}>")
            },
            timestamp,
        });
        let Some(parent) = iter.parent_ids().next() else {
            return Err(format!(
                "base {base} is not on tip {tip_hex}'s first-parent chain"
            ));
        };
        current = parent;
    }
    if current != base {
        return Err(format!("base {base} was not reached from tip {tip_hex}"));
    }
    commits.reverse();
    Ok(commits)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeCommit {
    pub sha: String,
    pub summary: String,
    pub author: String,
    pub timestamp: i64,
}

enum ChangeKind {
    Add,
    Delete,
    Modify,
}

impl ChangeKind {
    fn status(&self) -> &'static str {
        match self {
            ChangeKind::Add => "added",
            ChangeKind::Delete => "removed",
            ChangeKind::Modify => "modified",
        }
    }
}

struct FileChange {
    path: String,
    kind: ChangeKind,
    old: Option<(u16, ObjectId)>,
    new: Option<(u16, ObjectId)>,
}

/// Render the unified diff of `commit_oid` against its first parent, equivalent
/// to `git show --format= <commit_oid>`.
pub fn render_commit_diff(store: &ObjectStore, commit_hex: &str) -> Result<String, String> {
    let commit_oid = ObjectId::from_hex(commit_hex.as_bytes())
        .map_err(|e| format!("invalid commit sha: {e}"))?;
    let (kind, commit) = store
        .resolve_object(&commit_oid)
        .ok_or_else(|| format!("missing commit {commit_oid}"))?;
    if kind != ObjectKind::Commit {
        return Err(format!("object {commit_oid} is not a commit"));
    }

    let tree_id = CommitRefIter::from_bytes(&commit, HASH_KIND)
        .tree_id()
        .map_err(|e| format!("decoding commit tree id: {e}"))?;
    let parent_id = CommitRefIter::from_bytes(&commit, HASH_KIND)
        .parent_ids()
        .next();

    let new_tree = read_tree(store, &tree_id)?;
    let old_tree = match parent_id {
        Some(parent) => {
            let (pk, pbytes) = store
                .resolve_object(&parent)
                .ok_or_else(|| format!("missing parent commit {parent}"))?;
            if pk != ObjectKind::Commit {
                return Err(format!("parent {parent} is not a commit"));
            }
            let parent_tree = CommitRefIter::from_bytes(&pbytes, HASH_KIND)
                .tree_id()
                .map_err(|e| format!("decoding parent tree id: {e}"))?;
            read_tree(store, &parent_tree)?
        }
        None => BTreeMap::new(),
    };

    let mut changes: Vec<FileChange> = Vec::new();
    diff_trees(store, &old_tree, &new_tree, "", &mut changes)?;
    changes.sort_by(|a, b| a.path.cmp(&b.path));

    let mut out = String::new();
    for change in &changes {
        out.push_str(&render_file_change(store, change)?);
    }
    Ok(out)
}

/// A structured per-file entry of a range diff, surfaced in the node-tab diff
/// facet. `patch` holds just the unified hunks (starting at the first `@@`),
/// matching the shape the frontend `DiffViewer` parses; the `diff --git`
/// envelope is intentionally omitted here. Rename detection is out of scope
/// (a rename shows as a delete plus an add, git's default `-M`-off behavior),
/// so `previous_path` is always `None` from this renderer.
#[derive(Debug, Clone)]
pub struct NodeDiffFile {
    pub path: String,
    pub previous_path: Option<String>,
    pub status: String,
    pub additions: u32,
    pub deletions: u32,
    pub patch: String,
}

/// Resolve a commit's root tree into a flat name → entry map.
fn resolve_commit_tree(
    store: &ObjectStore,
    commit_oid: &oid,
) -> Result<BTreeMap<Vec<u8>, TreeEntry>, String> {
    let (kind, commit) = store
        .resolve_object(commit_oid)
        .ok_or_else(|| format!("missing commit {commit_oid}"))?;
    if kind != ObjectKind::Commit {
        return Err(format!("object {commit_oid} is not a commit"));
    }
    let tree_id = CommitRefIter::from_bytes(&commit, HASH_KIND)
        .tree_id()
        .map_err(|e| format!("decoding commit tree id: {e}"))?;
    read_tree(store, &tree_id)
}

/// Collect the sorted set of file changes between two commit trees.
fn range_changes(
    store: &ObjectStore,
    base_oid: &oid,
    tip_oid: &oid,
) -> Result<Vec<FileChange>, String> {
    let old_tree = resolve_commit_tree(store, base_oid)?;
    let new_tree = resolve_commit_tree(store, tip_oid)?;
    let mut changes: Vec<FileChange> = Vec::new();
    diff_trees(store, &old_tree, &new_tree, "", &mut changes)?;
    changes.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(changes)
}

/// One changed path of a [`RangePatch`]: its status and `+`/`-` line counts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeFileSummary {
    pub path: String,
    pub status: String,
    pub additions: u32,
    pub deletions: u32,
}

/// A range's unified diff together with its per-file summary, rendered in one
/// pass so each file's lines are aligned once.
#[derive(Debug, Clone, Default)]
pub struct RangePatch {
    pub patch: String,
    pub files: Vec<RangeFileSummary>,
}

/// Render the unified diff of `tip_oid`'s tree against `base_oid`'s tree,
/// equivalent to `git diff <base_oid> <tip_oid>` (two-dot). When `base_oid` is
/// the recorded fork point this equals git's three-dot `<base>...<tip>`, so a
/// cumulative branch diff matches.
///
/// Only paths `include` accepts are rendered, and an excluded path's blobs are
/// never read: the tree walk is cheap, while aligning a large file is not, so a
/// caller that wants one file of a wide range must say so here rather than
/// filtering the finished patch.
pub fn render_range_patch(
    store: &ObjectStore,
    base_hex: &str,
    tip_hex: &str,
    include: &dyn Fn(&str) -> bool,
) -> Result<RangePatch, String> {
    let base_oid =
        ObjectId::from_hex(base_hex.as_bytes()).map_err(|e| format!("invalid base sha: {e}"))?;
    let tip_oid =
        ObjectId::from_hex(tip_hex.as_bytes()).map_err(|e| format!("invalid tip sha: {e}"))?;
    let changes = range_changes(store, &base_oid, &tip_oid)?;
    let mut out = RangePatch::default();
    for change in changes.iter().filter(|change| include(&change.path)) {
        let hunks = change_hunks(store, change)?;
        let (additions, deletions) = count_changes(&hunks);
        out.patch.push_str(&file_envelope(change));
        out.patch.push_str(&hunks);
        out.files.push(RangeFileSummary {
            path: change.path.clone(),
            status: change.kind.status().to_string(),
            additions,
            deletions,
        });
    }
    Ok(out)
}

/// Structured variant of [`render_range_diff`]: one [`NodeDiffFile`] per changed
/// path, carrying its hunk patch and `+`/`-` counts so the frontend needn't
/// re-parse a concatenated diff.
pub fn render_range_file_diffs(
    store: &ObjectStore,
    base_hex: &str,
    tip_hex: &str,
) -> Result<Vec<NodeDiffFile>, String> {
    let base_oid =
        ObjectId::from_hex(base_hex.as_bytes()).map_err(|e| format!("invalid base sha: {e}"))?;
    let tip_oid =
        ObjectId::from_hex(tip_hex.as_bytes()).map_err(|e| format!("invalid tip sha: {e}"))?;
    let changes = range_changes(store, &base_oid, &tip_oid)?;
    let mut files = Vec::with_capacity(changes.len());
    for change in &changes {
        files.push(render_node_diff_file(store, change)?);
    }
    Ok(files)
}

/// Count commits on the first-parent chain from `tip` back to (but not
/// including) `base`, resolved through the layered [`ObjectStore`]. Bounded so a
/// missing base or a cycle can't spin; an unparseable coordinate yields 0.
pub fn count_commits_ahead(store: &ObjectStore, base_hex: &str, tip_hex: &str) -> i32 {
    const CAP: i32 = 100_000;
    let (Ok(base), Ok(tip)) = (
        ObjectId::from_hex(base_hex.as_bytes()),
        ObjectId::from_hex(tip_hex.as_bytes()),
    ) else {
        return 0;
    };

    let mut count = 0;
    let mut current = tip;
    while current != base && count < CAP {
        let Some((kind, bytes)) = store.resolve_object(&current) else {
            break;
        };
        if kind != ObjectKind::Commit {
            break;
        }
        let Some(parent) = CommitRefIter::from_bytes(&bytes, HASH_KIND)
            .parent_ids()
            .next()
        else {
            break;
        };
        count += 1;
        current = parent;
    }
    count
}

fn render_node_diff_file(store: &ObjectStore, change: &FileChange) -> Result<NodeDiffFile, String> {
    let patch = change_hunks(store, change)?;
    let (additions, deletions) = count_changes(&patch);
    Ok(NodeDiffFile {
        path: change.path.clone(),
        previous_path: None,
        status: change.kind.status().to_string(),
        additions,
        deletions,
        patch,
    })
}

/// Count added/removed lines in a unified-hunk body. The body has no `+++`/`---`
/// file headers (those live in the `diff --git` envelope), so a leading `+`/`-`
/// is always a content line; `@@` hunk headers and `\ No newline` markers are
/// ignored.
fn count_changes(patch: &str) -> (u32, u32) {
    let mut additions = 0u32;
    let mut deletions = 0u32;
    for line in patch.lines() {
        if line.starts_with('+') {
            additions += 1;
        } else if line.starts_with('-') {
            deletions += 1;
        }
    }
    (additions, deletions)
}

fn read_tree(store: &ObjectStore, tree: &oid) -> Result<BTreeMap<Vec<u8>, TreeEntry>, String> {
    let (kind, bytes) = store
        .resolve_object(tree)
        .ok_or_else(|| format!("missing tree {tree}"))?;
    if kind != ObjectKind::Tree {
        return Err(format!("object {tree} is not a tree"));
    }
    let mut map = BTreeMap::new();
    for entry in TreeRefIter::from_bytes(&bytes, HASH_KIND) {
        let entry = entry.map_err(|error| format!("decoding tree {tree}: {error}"))?;
        map.insert(
            entry.filename.to_vec(),
            TreeEntry {
                mode: entry.mode.value(),
                oid: entry.oid.to_owned(),
                is_tree: entry.mode.is_tree(),
            },
        );
    }
    Ok(map)
}

fn diff_trees(
    store: &ObjectStore,
    old: &BTreeMap<Vec<u8>, TreeEntry>,
    new: &BTreeMap<Vec<u8>, TreeEntry>,
    prefix: &str,
    out: &mut Vec<FileChange>,
) -> Result<(), String> {
    let names: BTreeSet<&Vec<u8>> = old.keys().chain(new.keys()).collect();
    for name in names {
        let name_str = String::from_utf8_lossy(name);
        let path = if prefix.is_empty() {
            name_str.to_string()
        } else {
            format!("{prefix}/{name_str}")
        };
        match (old.get(name), new.get(name)) {
            (Some(o), Some(n)) if o.oid == n.oid && o.mode == n.mode => {}
            (Some(o), Some(n)) => {
                if o.is_tree && n.is_tree {
                    let os = read_tree(store, &o.oid)?;
                    let ns = read_tree(store, &n.oid)?;
                    diff_trees(store, &os, &ns, &path, out)?;
                } else if o.is_tree {
                    collect_subtree(store, &o.oid, &path, ChangeKind::Delete, out)?;
                    out.push(added(&path, n));
                } else if n.is_tree {
                    out.push(deleted(&path, o));
                    collect_subtree(store, &n.oid, &path, ChangeKind::Add, out)?;
                } else {
                    out.push(FileChange {
                        path,
                        kind: ChangeKind::Modify,
                        old: Some((o.mode, o.oid)),
                        new: Some((n.mode, n.oid)),
                    });
                }
            }
            (Some(o), None) => {
                if o.is_tree {
                    collect_subtree(store, &o.oid, &path, ChangeKind::Delete, out)?;
                } else {
                    out.push(deleted(&path, o));
                }
            }
            (None, Some(n)) => {
                if n.is_tree {
                    collect_subtree(store, &n.oid, &path, ChangeKind::Add, out)?;
                } else {
                    out.push(added(&path, n));
                }
            }
            (None, None) => {}
        }
    }
    Ok(())
}

fn collect_subtree(
    store: &ObjectStore,
    tree: &oid,
    prefix: &str,
    kind: ChangeKind,
    out: &mut Vec<FileChange>,
) -> Result<(), String> {
    let entries = read_tree(store, tree)?;
    for (name, entry) in entries {
        let name_str = String::from_utf8_lossy(&name);
        let path = format!("{prefix}/{name_str}");
        if entry.is_tree {
            let next_kind = match kind {
                ChangeKind::Add => ChangeKind::Add,
                _ => ChangeKind::Delete,
            };
            collect_subtree(store, &entry.oid, &path, next_kind, out)?;
        } else {
            match kind {
                ChangeKind::Add => out.push(added(&path, &entry)),
                _ => out.push(deleted(&path, &entry)),
            }
        }
    }
    Ok(())
}

fn added(path: &str, entry: &TreeEntry) -> FileChange {
    FileChange {
        path: path.to_string(),
        kind: ChangeKind::Add,
        old: None,
        new: Some((entry.mode, entry.oid)),
    }
}

fn deleted(path: &str, entry: &TreeEntry) -> FileChange {
    FileChange {
        path: path.to_string(),
        kind: ChangeKind::Delete,
        old: Some((entry.mode, entry.oid)),
        new: None,
    }
}

fn short(oid: &ObjectId) -> String {
    oid.to_hex().to_string()[..7].to_string()
}

fn blob_bytes(store: &ObjectStore, oid: &ObjectId) -> Result<Vec<u8>, String> {
    let (kind, bytes) = store
        .resolve_object(oid)
        .ok_or_else(|| format!("missing blob {oid}"))?;
    if kind != ObjectKind::Blob {
        return Err(format!("object {oid} is not a blob"));
    }
    Ok(bytes)
}

fn render_file_change(store: &ObjectStore, change: &FileChange) -> Result<String, String> {
    let mut out = file_envelope(change);
    out.push_str(&change_hunks(store, change)?);
    Ok(out)
}

/// A change's unified hunks (from its first `@@`), aligned once.
fn change_hunks(store: &ObjectStore, change: &FileChange) -> Result<String, String> {
    let old_bytes = match &change.old {
        Some((_, oid)) => blob_bytes(store, oid)?,
        None => Vec::new(),
    };
    let new_bytes = match &change.new {
        Some((_, oid)) => blob_bytes(store, oid)?,
        None => Vec::new(),
    };
    Ok(unified_hunks(&old_bytes, &new_bytes))
}

/// A change's `diff --git` envelope through its `+++` line.
fn file_envelope(change: &FileChange) -> String {
    let old_hex = change
        .old
        .as_ref()
        .map(|(_, oid)| short(oid))
        .unwrap_or_else(|| "0000000".to_string());
    let new_hex = change
        .new
        .as_ref()
        .map(|(_, oid)| short(oid))
        .unwrap_or_else(|| "0000000".to_string());

    let mut out = String::new();
    out.push_str(&format!("diff --git a/{} b/{}\n", change.path, change.path));
    match change.kind {
        ChangeKind::Add => {
            out.push_str(&format!(
                "new file mode {:o}\n",
                change.new.as_ref().unwrap().0
            ));
            out.push_str(&format!("index {old_hex}..{new_hex}\n"));
        }
        ChangeKind::Delete => {
            out.push_str(&format!(
                "deleted file mode {:o}\n",
                change.old.as_ref().unwrap().0
            ));
            out.push_str(&format!("index {old_hex}..{new_hex}\n"));
        }
        ChangeKind::Modify => {
            out.push_str(&format!(
                "index {old_hex}..{new_hex} {:o}\n",
                change.new.as_ref().unwrap().0
            ));
        }
    }
    let a_label = match change.kind {
        ChangeKind::Add => "/dev/null".to_string(),
        _ => format!("a/{}", change.path),
    };
    let b_label = match change.kind {
        ChangeKind::Delete => "/dev/null".to_string(),
        _ => format!("b/{}", change.path),
    };
    out.push_str(&format!("--- {a_label}\n"));
    out.push_str(&format!("+++ {b_label}\n"));
    out
}

/// One aligned diff step over interned line indices.
enum Op {
    Equal(usize, usize),
    Del(usize),
    Ins(usize),
}

/// Split content into lines, reporting whether it ended with a newline.
fn split_lines(text: &str) -> (Vec<&str>, bool) {
    if text.is_empty() {
        return (Vec::new(), true);
    }
    let ends_with_newline = text.ends_with('\n');
    let mut lines: Vec<&str> = text.split('\n').collect();
    if ends_with_newline {
        // Trailing empty element from the final newline is not a line.
        lines.pop();
    }
    (lines, ends_with_newline)
}

/// Work one file's alignment may spend, in snake steps plus diagonal visits,
/// before the regions still unaligned are reported as wholly replaced.
///
/// Myers runs in O((n+m)·D) time for D differing lines, so the memory bound
/// alone does not bound a file that changed almost everywhere (a regenerated
/// lockfile or JSON dump): D approaches n and the time is quadratic again. A
/// few-edit change to a 100k-line file costs a few million units; past the
/// budget the result is still a correct diff, only no longer minimal.
const ALIGN_WORK_BUDGET: u64 = 16_000_000;

/// Align two line sequences into an edit script.
///
/// Linear-space Myers (divide and conquer on the middle snake) over interned
/// line ids, so memory is O(n+m) however large the file: a full LCS table is
/// 4·n·m bytes, which is 66 GB for a 128k-line file edited in place. Like
/// git's xdiff it marks which lines of each side changed and then reads the
/// script off the marks, so each change block emits its deletions before its
/// insertions.
fn align_ops(a: &[&str], b: &[&str]) -> Vec<Op> {
    let (a, b) = intern_lines(a, b);
    let mut changed_a = vec![false; a.len()];
    let mut changed_b = vec![false; b.len()];
    let mut budget = ALIGN_WORK_BUDGET;
    mark_changes(&a, &b, &mut changed_a, &mut changed_b, &mut budget);
    ops_from_marks(&changed_a, &changed_b)
}

/// Map each distinct line to a small integer so comparisons are word-sized.
fn intern_lines<'t>(a: &[&'t str], b: &[&'t str]) -> (Vec<u32>, Vec<u32>) {
    let mut ids: HashMap<&'t str, u32> = HashMap::with_capacity(a.len() + b.len());
    let mut intern = |line: &'t str| -> u32 {
        let next = ids.len() as u32;
        *ids.entry(line).or_insert(next)
    };
    let a_ids: Vec<u32> = a.iter().map(|line| intern(line)).collect();
    let b_ids: Vec<u32> = b.iter().map(|line| intern(line)).collect();
    (a_ids, b_ids)
}

/// Mark the lines of `a` and `b` that a shortest edit script deletes or
/// inserts. `changed_a`/`changed_b` are index-aligned with `a`/`b`.
fn mark_changes(
    a: &[u32],
    b: &[u32],
    changed_a: &mut [bool],
    changed_b: &mut [bool],
    budget: &mut u64,
) {
    let prefix = a.iter().zip(b).take_while(|(x, y)| x == y).count();
    let (a, b) = (&a[prefix..], &b[prefix..]);
    let suffix = a
        .iter()
        .rev()
        .zip(b.iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    let (a, b) = (&a[..a.len() - suffix], &b[..b.len() - suffix]);
    let changed_a = &mut changed_a[prefix..prefix + a.len()];
    let changed_b = &mut changed_b[prefix..prefix + b.len()];
    if a.is_empty() || b.is_empty() {
        changed_a.fill(true);
        changed_b.fill(true);
        return;
    }
    match middle_snake(a, b, budget) {
        // A split at either corner would recurse on the whole region; the
        // theory rules it out once the ends are trimmed, and the guard keeps a
        // bug from becoming unbounded recursion.
        Some((x, y)) if (x, y) != (0, 0) && (x, y) != (a.len(), b.len()) => {
            let (head_a, tail_a) = changed_a.split_at_mut(x);
            let (head_b, tail_b) = changed_b.split_at_mut(y);
            mark_changes(&a[..x], &b[..y], head_a, head_b, budget);
            mark_changes(&a[x..], &b[y..], tail_a, tail_b, budget);
        }
        _ => {
            changed_a.fill(true);
            changed_b.fill(true);
        }
    }
}

/// Find a point `(x, y)` on a shortest edit path from `(0, 0)` to
/// `(a.len(), b.len())`, searching forward and backward at once until the two
/// frontiers overlap (Myers 1986, section 4b). Returns `None` when the work
/// budget runs out first.
fn middle_snake(a: &[u32], b: &[u32], budget: &mut u64) -> Option<(usize, usize)> {
    let (n, m) = (a.len() as isize, b.len() as isize);
    let max_d = (n + m + 1) / 2;
    let offset = max_d;
    let width = (2 * max_d + 2) as usize;
    // Furthest x reached on each diagonal k = x - y, forward from the start and
    // backward from the end (the backward array counts x from the far edge).
    let mut forward = vec![-1isize; width];
    let mut backward = vec![-1isize; width];
    forward[(offset + 1) as usize] = 0;
    backward[(offset + 1) as usize] = 0;
    let delta = n - m;
    // With an odd delta the frontiers can first overlap on a forward step,
    // with an even one on a backward step.
    let overlap_forward = delta % 2 != 0;
    let (mut k1_start, mut k1_end, mut k2_start, mut k2_end) = (0isize, 0isize, 0isize, 0isize);

    for d in 0..max_d {
        if *budget == 0 {
            return None;
        }
        let mut k1 = -d + k1_start;
        while k1 <= d - k1_end {
            let at = (offset + k1) as usize;
            let mut x1 = if k1 == -d || (k1 != d && forward[at - 1] < forward[at + 1]) {
                forward[at + 1]
            } else {
                forward[at - 1] + 1
            };
            let mut y1 = x1 - k1;
            let start = x1;
            while x1 < n && y1 < m && a[x1 as usize] == b[y1 as usize] {
                x1 += 1;
                y1 += 1;
            }
            *budget = budget.saturating_sub(1 + (x1 - start) as u64);
            forward[at] = x1;
            if x1 > n {
                k1_end += 2;
            } else if y1 > m {
                k1_start += 2;
            } else if overlap_forward {
                let other = offset + delta - k1;
                if other >= 0 && (other as usize) < width && backward[other as usize] != -1 {
                    let x2 = n - backward[other as usize];
                    if x1 >= x2 {
                        return Some((x1 as usize, y1 as usize));
                    }
                }
            }
            k1 += 2;
        }

        let mut k2 = -d + k2_start;
        while k2 <= d - k2_end {
            let at = (offset + k2) as usize;
            let mut x2 = if k2 == -d || (k2 != d && backward[at - 1] < backward[at + 1]) {
                backward[at + 1]
            } else {
                backward[at - 1] + 1
            };
            let mut y2 = x2 - k2;
            let start = x2;
            while x2 < n && y2 < m && a[(n - x2 - 1) as usize] == b[(m - y2 - 1) as usize] {
                x2 += 1;
                y2 += 1;
            }
            *budget = budget.saturating_sub(1 + (x2 - start) as u64);
            backward[at] = x2;
            if x2 > n {
                k2_end += 2;
            } else if y2 > m {
                k2_start += 2;
            } else if !overlap_forward {
                let other = offset + delta - k2;
                if other >= 0 && (other as usize) < width && forward[other as usize] != -1 {
                    let x1 = forward[other as usize];
                    let y1 = offset + x1 - other;
                    if x1 >= n - x2 {
                        return Some((x1 as usize, y1 as usize));
                    }
                }
            }
            k2 += 2;
        }
    }
    None
}

/// Read an edit script off per-side change marks: kept lines pair up in order,
/// and each block of changes emits its deletions, then its insertions.
fn ops_from_marks(changed_a: &[bool], changed_b: &[bool]) -> Vec<Op> {
    let (n, m) = (changed_a.len(), changed_b.len());
    let mut ops = Vec::with_capacity(n.max(m));
    let (mut i, mut j) = (0usize, 0usize);
    while i < n || j < m {
        if i < n && j < m && !changed_a[i] && !changed_b[j] {
            ops.push(Op::Equal(i, j));
            i += 1;
            j += 1;
            continue;
        }
        let before = (i, j);
        while i < n && changed_a[i] {
            ops.push(Op::Del(i));
            i += 1;
        }
        while j < m && changed_b[j] {
            ops.push(Op::Ins(j));
            j += 1;
        }
        if (i, j) == before {
            // Kept lines left on one side only: marks no alignment produces.
            // Close the remainder out as replaced rather than loop.
            ops.extend((i..n).map(Op::Del));
            ops.extend((j..m).map(Op::Ins));
            break;
        }
    }
    ops
}

fn unified_hunks(old: &[u8], new: &[u8]) -> String {
    let old_text = String::from_utf8_lossy(old);
    let new_text = String::from_utf8_lossy(new);
    let (old_lines, old_nl) = split_lines(&old_text);
    let (new_lines, new_nl) = split_lines(&new_text);

    let ops = align_ops(&old_lines, &new_lines);
    let change_positions: Vec<usize> = ops
        .iter()
        .enumerate()
        .filter(|(_, op)| !matches!(op, Op::Equal(..)))
        .map(|(i, _)| i)
        .collect();
    if change_positions.is_empty() {
        return String::new();
    }

    // Group changes into hunks: a context window of CONTEXT equal lines on each
    // side, with adjacent groups merged when their windows touch.
    let mut hunks: Vec<(usize, usize)> = Vec::new();
    let mut start = change_positions[0].saturating_sub(CONTEXT);
    let mut last = change_positions[0];
    for &pos in &change_positions[1..] {
        if pos - last <= 2 * CONTEXT + 1 {
            last = pos;
        } else {
            hunks.push((start, (last + CONTEXT).min(ops.len() - 1)));
            start = pos.saturating_sub(CONTEXT);
            last = pos;
        }
    }
    hunks.push((start, (last + CONTEXT).min(ops.len() - 1)));

    let mut out = String::new();
    for (hs, he) in hunks {
        out.push_str(&render_hunk(
            &ops[hs..=he],
            &old_lines,
            old_nl,
            &new_lines,
            new_nl,
        ));
    }
    out
}

fn render_hunk(
    ops: &[Op],
    old_lines: &[&str],
    old_nl: bool,
    new_lines: &[&str],
    new_nl: bool,
) -> String {
    let mut old_start: Option<usize> = None;
    let mut new_start: Option<usize> = None;
    let mut old_count = 0usize;
    let mut new_count = 0usize;
    let mut body = String::new();

    for op in ops {
        match op {
            Op::Equal(oi, ni) => {
                old_start.get_or_insert(oi + 1);
                new_start.get_or_insert(ni + 1);
                old_count += 1;
                new_count += 1;
                push_line(&mut body, ' ', old_lines[*oi], *oi, old_lines.len(), old_nl);
            }
            Op::Del(oi) => {
                old_start.get_or_insert(oi + 1);
                old_count += 1;
                push_line(&mut body, '-', old_lines[*oi], *oi, old_lines.len(), old_nl);
            }
            Op::Ins(ni) => {
                new_start.get_or_insert(ni + 1);
                new_count += 1;
                push_line(&mut body, '+', new_lines[*ni], *ni, new_lines.len(), new_nl);
            }
        }
    }

    let old_start = old_start.unwrap_or(0);
    let new_start = new_start.unwrap_or(0);
    format!(
        "@@ -{} +{} @@\n{}",
        fmt_range(old_start, old_count),
        fmt_range(new_start, new_count),
        body
    )
}

fn push_line(body: &mut String, tag: char, text: &str, index: usize, total: usize, ends_nl: bool) {
    body.push(tag);
    body.push_str(text);
    body.push('\n');
    if !ends_nl && index + 1 == total {
        body.push_str("\\ No newline at end of file\n");
    }
}

fn fmt_range(start: usize, count: usize) -> String {
    if count == 1 {
        format!("{start}")
    } else {
        format!("{start},{count}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objects::ObjectStore;
    use crate::testutil::{commit_all, git, init_repo, write_file};
    use std::path::Path;

    /// Build a repo with a base commit and several feature commits exercising
    /// add / modify / delete, then assert the object-store range renderer matches
    /// shelled `git diff <base> <tip>` byte-for-byte.
    #[test]
    fn render_range_patch_matches_git_diff() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        init_repo(dir);

        write_file(dir, "keep.txt", b"line one\nline two\nline three\n");
        write_file(dir, "gone.txt", b"delete me\n");
        write_file(dir, "edit.txt", b"alpha\nbeta\ngamma\n");
        let base = commit_all(dir, "base");

        // A run of commits: modify edit.txt, add new.txt, delete gone.txt.
        write_file(dir, "edit.txt", b"alpha\nBETA\ngamma\ndelta\n");
        commit_all(dir, "edit");
        write_file(dir, "nested/new.txt", b"fresh\ncontent\n");
        commit_all(dir, "add nested");
        std::fs::remove_file(dir.join("gone.txt")).unwrap();
        let tip = commit_all(dir, "remove");

        let store = ObjectStore::new(Path::new(dir), None).unwrap();
        let rendered = render_range_patch(&store, &base, &tip, &|_| true).unwrap();
        let expected = git(dir, &["diff", "--no-color", &base, &tip]);
        assert_eq!(rendered.patch, expected);
        let summary: Vec<(&str, &str, u32, u32)> = rendered
            .files
            .iter()
            .map(|file| {
                (
                    file.path.as_str(),
                    file.status.as_str(),
                    file.additions,
                    file.deletions,
                )
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                ("edit.txt", "modified", 2, 1),
                ("gone.txt", "removed", 0, 1),
                ("nested/new.txt", "added", 2, 0),
            ]
        );

        // A filter renders only what it names; the rest is never aligned.
        let one = render_range_patch(&store, &base, &tip, &|path| path == "edit.txt").unwrap();
        assert_eq!(one.files.len(), 1);
        assert_eq!(
            one.patch,
            git(dir, &["diff", "--no-color", &base, &tip, "--", "edit.txt"])
        );
    }

    /// The structured variant carries the same per-file content as the string
    /// renderer: status, hunk patch, and accurate `+`/`-` counts.
    #[test]
    fn render_range_file_diffs_reports_status_and_counts() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        init_repo(dir);

        write_file(dir, "edit.txt", b"one\ntwo\nthree\n");
        write_file(dir, "gone.txt", b"bye\n");
        let base = commit_all(dir, "base");

        write_file(dir, "edit.txt", b"one\nTWO\nthree\nfour\n");
        write_file(dir, "added.txt", b"new line a\nnew line b\n");
        std::fs::remove_file(dir.join("gone.txt")).unwrap();
        let tip = commit_all(dir, "changes");

        let store = ObjectStore::new(Path::new(dir), None).unwrap();
        let files = render_range_file_diffs(&store, &base, &tip).unwrap();

        let by_path: std::collections::HashMap<&str, &NodeDiffFile> =
            files.iter().map(|f| (f.path.as_str(), f)).collect();
        assert_eq!(files.len(), 3, "added, edit, gone");

        let added = by_path["added.txt"];
        assert_eq!(added.status, "added");
        assert_eq!((added.additions, added.deletions), (2, 0));

        let edit = by_path["edit.txt"];
        assert_eq!(edit.status, "modified");
        // one line replaced (TWO) plus one appended (four) => +2 / -1
        assert_eq!((edit.additions, edit.deletions), (2, 1));
        assert!(edit.patch.starts_with("@@"), "patch holds hunks only");

        let gone = by_path["gone.txt"];
        assert_eq!(gone.status, "removed");
        assert_eq!((gone.additions, gone.deletions), (0, 1));
    }
    /// LCS length by the full quadratic table: the reference the alignment's
    /// minimality is checked against, on inputs small enough to afford it.
    fn reference_lcs_len(a: &[&str], b: &[&str]) -> usize {
        let mut table = vec![vec![0usize; b.len() + 1]; a.len() + 1];
        for i in (0..a.len()).rev() {
            for j in (0..b.len()).rev() {
                table[i][j] = if a[i] == b[j] {
                    table[i + 1][j + 1] + 1
                } else {
                    table[i + 1][j].max(table[i][j + 1])
                };
            }
        }
        table[0][0]
    }

    /// Every random pair aligns into a script that consumes both sides in
    /// order, pairs only equal lines, keeps as many lines as an LCS (so it is a
    /// shortest script), and emits each change block's deletions first.
    #[test]
    fn alignment_is_consistent_and_minimal() {
        const ALPHABET: [&str; 4] = ["a", "b", "c", "d"];
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound) as usize
        };
        for _ in 0..3_000 {
            let a: Vec<&str> = (0..next(40)).map(|_| ALPHABET[next(4)]).collect();
            let b: Vec<&str> = (0..next(40)).map(|_| ALPHABET[next(4)]).collect();
            let ops = align_ops(&a, &b);

            let (mut i, mut j, mut kept) = (0usize, 0usize, 0usize);
            let mut previous_was_insert = false;
            for op in &ops {
                match *op {
                    Op::Equal(oi, ni) => {
                        assert_eq!((oi, ni), (i, j), "script skips lines: {a:?} -> {b:?}");
                        assert_eq!(a[oi], b[ni], "pairs unequal lines: {a:?} -> {b:?}");
                        i += 1;
                        j += 1;
                        kept += 1;
                        previous_was_insert = false;
                    }
                    Op::Del(oi) => {
                        assert_eq!(oi, i, "deletes out of order: {a:?} -> {b:?}");
                        assert!(
                            !previous_was_insert,
                            "insertion before deletion in one block: {a:?} -> {b:?}"
                        );
                        i += 1;
                    }
                    Op::Ins(ni) => {
                        assert_eq!(ni, j, "inserts out of order: {a:?} -> {b:?}");
                        j += 1;
                        previous_was_insert = true;
                    }
                }
            }
            assert_eq!(
                (i, j),
                (a.len(), b.len()),
                "script stops short: {a:?} -> {b:?}"
            );
            assert_eq!(
                kept,
                reference_lcs_len(&a, &b),
                "not minimal: {a:?} -> {b:?}"
            );
        }
    }

    fn numbered_lines(count: usize) -> Vec<String> {
        (0..count)
            .map(|index| format!("    \"entry-{index}\": {},", index * 7))
            .collect()
    }

    fn file_text(lines: &[String]) -> Vec<u8> {
        let mut text = lines.join("\n");
        text.push('\n');
        text.into_bytes()
    }

    /// Scattered replacements, an inserted block, and a deleted block in a
    /// file large enough that the old table cost gigabytes still render
    /// byte-for-byte as git renders them.
    #[test]
    fn scattered_edits_in_a_large_file_match_git_diff() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        init_repo(dir);
        let old = numbered_lines(30_000);
        write_file(dir, "data.json", &file_text(&old));
        let base = commit_all(dir, "base");

        let mut new = old.clone();
        new[3] = "    \"entry-3\": \"replaced\",".to_string();
        new[12_000] = "    \"entry-12000\": null,".to_string();
        new.drain(20_000..20_005);
        for (offset, line) in ["    \"inserted-a\": 1,", "    \"inserted-b\": 2,"]
            .into_iter()
            .enumerate()
        {
            new.insert(25_000 + offset, line.to_string());
        }
        let last = new.len() - 1;
        new[last] = "    \"entry-last\": 0".to_string();
        write_file(dir, "data.json", &file_text(&new));
        let tip = commit_all(dir, "edit");

        let store = ObjectStore::new(Path::new(dir), None).unwrap();
        let rendered = render_range_patch(&store, &base, &tip, &|_| true).unwrap();
        let expected = git(dir, &["diff", "--no-color", &base, &tip]);
        assert_eq!(rendered.patch, expected);
        assert_eq!(
            (rendered.files[0].additions, rendered.files[0].deletions),
            (5, 8)
        );
    }

    /// The production case: a 128k-line file edited in a few places, whose
    /// full LCS table was 66 GB and 16 billion compares. Linear-space
    /// alignment finishes it well inside the bound even in a debug build.
    #[test]
    fn a_large_file_with_few_edits_aligns_quickly() {
        let old = numbered_lines(128_000);
        let mut new = old.clone();
        new[10] = "    \"entry-10\": -1,".to_string();
        new[64_000] = "    \"entry-64000\": -1,".to_string();
        new.insert(127_000, "    \"added\": true,".to_string());

        let started = std::time::Instant::now();
        let hunks = unified_hunks(&file_text(&old), &file_text(&new));
        let elapsed = started.elapsed();
        assert_eq!(count_changes(&hunks), (3, 2));
        assert_eq!(hunks.matches("@@ -").count(), 3, "one hunk per edit");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "took {elapsed:?}"
        );
    }

    /// A file that changed on every line drives Myers toward its quadratic
    /// worst case; the work budget reports it as replaced wholesale instead.
    #[test]
    fn a_file_rewritten_everywhere_stays_within_the_work_budget() {
        let old = numbered_lines(50_000);
        let new: Vec<String> = (0..50_000)
            .map(|index| format!("rewritten {index}"))
            .collect();

        let started = std::time::Instant::now();
        let hunks = unified_hunks(&file_text(&old), &file_text(&new));
        let elapsed = started.elapsed();
        assert_eq!(count_changes(&hunks), (50_000, 50_000));
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "took {elapsed:?}"
        );
    }

    #[test]
    fn list_range_commits_returns_oldest_first_with_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path();
        init_repo(dir);
        write_file(dir, "a.txt", b"base\n");
        let base = commit_all(dir, "base");
        write_file(dir, "a.txt", b"one\n");
        let first = commit_all(dir, "first change");
        write_file(dir, "b.txt", b"two\n");
        let tip = commit_all(dir, "second change");
        let store = ObjectStore::new(Path::new(dir), None).unwrap();
        let commits = list_range_commits(&store, &base, &tip).unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].sha, first);
        assert_eq!(commits[0].summary, "first change");
        assert_eq!(commits[1].sha, tip);
        assert_eq!(commits[1].summary, "second change");
        assert!(!commits[0].author.is_empty());
    }
}
