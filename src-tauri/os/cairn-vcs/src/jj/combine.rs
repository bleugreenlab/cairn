//! How the versions of a conflicted branch name combine.
//!
//! Combining two versions is only ever about the commits the BRANCH owns.
//! Anything another name already holds, the base branch above all, is shared
//! history: a replay that sweeps it up rewrites someone else's branch, and a
//! version that sits on more of it than the other is on a newer base rather than
//! carrying extra work.
//!
//! Two versions can also hold the SAME work: one is the other's commits replayed
//! onto a newer base (same change ids, the same content once the base advance is
//! accounted for). Replaying either onto the other then copies every commit a
//! third time. The combination that keeps everything is the version that already
//! carries the other's work, with the other's copies abandoned: a [`TwinStack`].
//!
//! Everything here reads. The settlements that act on it live with the name
//! repair in `bookmark.rs`.

use super::*;
use std::collections::HashMap;
use std::path::Path;

/// Commits some OTHER name already holds: the ancestry of every local and
/// remote bookmark not named `branch`. Matched by name rather than by commit,
/// so a base bookmark sitting exactly on one of this branch's versions still
/// protects that commit.
pub(crate) fn foreign_ancestry(branch: &str) -> String {
    format!("::(bookmarks(~exact:{branch:?}) | remote_bookmarks(~exact:{branch:?}))")
}

/// The commits of `from..tip` that `branch` alone holds.
pub(crate) fn own_commits_revset(branch: &str, from: &str, tip: &str) -> String {
    format!("({from}..{tip}) ~ {}", foreign_ancestry(branch))
}

/// The store's parked `@`: an empty, undescribed working-copy commit. Never
/// counted as work built on anything, because jj carries it along with its
/// parent and it holds nothing.
const PARKED_WORKING_COPY: &str = "(working_copies() & empty() & description(exact:\"\"))";

pub(crate) fn log_lines(
    jj: &JjEnv,
    store: &Path,
    revset: &str,
    field: &str,
) -> Result<Vec<String>, String> {
    Ok(jj
        .run(
            store,
            &[
                "log",
                "-r",
                revset,
                "--no-graph",
                "-T",
                &format!("{field} ++ \"\\n\""),
                "--ignore-working-copy",
            ],
            "jj log (conflicted name settlement)",
        )?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// One commit of a version's history, as a replay sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GraphCommit {
    pub commit: String,
    pub change: String,
    pub parents: Vec<String>,
    /// The commit records a jj conflict, so its git tree is jj's conflict
    /// encoding rather than content a three-way merge can reason about.
    pub conflicted: bool,
}

/// `revset`'s commits, parents before children.
pub(crate) fn graph(jj: &JjEnv, store: &Path, revset: &str) -> Result<Vec<GraphCommit>, String> {
    let out = jj.run(
        store,
        &[
            "log",
            "-r",
            revset,
            "--reversed",
            "--no-graph",
            "-T",
            "commit_id ++ \"\\t\" ++ change_id ++ \"\\t\" ++ \
             parents.map(|p| p.commit_id()).join(\",\") ++ \"\\t\" ++ \
             if(self.conflict(), \"conflict\", \"\") ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "jj log (branch version graph)",
    )?;
    Ok(out
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let commit = fields.next()?.trim();
            if commit.is_empty() {
                return None;
            }
            Some(GraphCommit {
                commit: commit.to_string(),
                change: fields.next().unwrap_or_default().trim().to_string(),
                parents: fields
                    .next()
                    .unwrap_or_default()
                    .split(',')
                    .map(str::trim)
                    .filter(|parent| !parent.is_empty())
                    .map(ToOwned::to_owned)
                    .collect(),
                conflicted: fields.next().is_some_and(|flag| flag.trim() == "conflict"),
            })
        })
        .collect())
}

enum MergedTree {
    Clean(String),
    Conflicted,
}

/// git's three-way merge of `theirs` against `base`, onto `ours`. Any of the
/// three may be a tree, so a replay can chain onto its own intermediate
/// results. Writes loose tree objects and nothing else. `None` when git could
/// not merge (an older git that takes no `--merge-base`, a missing object).
fn merge_tree(checkout: &Path, base: &str, ours: &str, theirs: &str) -> Option<MergedTree> {
    let output = cairn_platform::env::git()
        .args([
            "merge-tree",
            "--write-tree",
            &format!("--merge-base={base}"),
            ours,
            theirs,
        ])
        .current_dir(checkout)
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    match output.status.code() {
        Some(0) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let tree = stdout.lines().next()?.trim();
            (!tree.is_empty()).then(|| MergedTree::Clean(tree.to_string()))
        }
        Some(1) => Some(MergedTree::Conflicted),
        _ => None,
    }
}

/// Replay `commits` (parents first) onto `onto` in memory, commit by commit,
/// the way `jj rebase -r` re-parents them: a commit whose parent is not among
/// them lands on `onto`, the rest on their replayed parent.
///
/// `Some(true)` when some commit conflicts, `Some(false)` when every one
/// merged, `None` when the replay could not be tried: no commits, a merge
/// commit, a commit that already records a jj conflict, or git unable to merge.
pub(crate) fn replay_in_memory(store: &Path, commits: &[GraphCommit], onto: &str) -> Option<bool> {
    if commits.is_empty() || commits.iter().any(|commit| commit.conflicted) {
        return None;
    }
    let checkout = super::export::resolve_backing_checkout(store)?;
    let mut replayed: HashMap<&str, String> = HashMap::new();
    for commit in commits {
        let [parent] = commit.parents.as_slice() else {
            return None;
        };
        let ours = replayed
            .get(parent.as_str())
            .cloned()
            .unwrap_or_else(|| onto.to_string());
        match merge_tree(&checkout, parent, &ours, &commit.commit)? {
            MergedTree::Clean(tree) => {
                replayed.insert(&commit.commit, tree);
            }
            MergedTree::Conflicted => return Some(true),
        }
    }
    Some(false)
}

/// [`replay_in_memory`] of `revset`'s commits onto `onto`, read from the store.
/// `None` (not tried) when the store cannot be read or `onto` itself records a
/// conflict.
pub(crate) fn replay_revset_in_memory(
    jj: &JjEnv,
    store: &Path,
    revset: &str,
    onto: &str,
) -> Option<bool> {
    if super::conflict::revset_has_conflict(jj, store, onto).unwrap_or(true) {
        return None;
    }
    replay_in_memory(store, &graph(jj, store, revset).ok()?, onto)
}

/// Whether a maintainer ordering (keep `keep`, replay `replay`'s commits since
/// `from` onto it) is safe to print, and if so how its replay went in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrderingCheck {
    /// Print it. `conflicts` is the in-memory replay's outcome, `None` when it
    /// was not tried.
    Offer { conflicts: Option<bool> },
    /// Do not print it, for `why` (a clause that completes "because ...").
    Withhold { why: String },
}

/// Judge one ordering. A replay range may only ever hold commits this branch
/// alone owns, and nothing else may be built on them; an ordering that cannot
/// meet that is withheld with the reason rather than printed.
pub(crate) fn check_ordering(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    from: &str,
    keep: &str,
    replay: &str,
) -> OrderingCheck {
    check_ordering_reading(jj, store, branch, from, keep, replay).unwrap_or_else(|error| {
        OrderingCheck::Withhold {
            why: format!(
                "the store could not say which of `{replay}`'s commits are its own ({error})"
            ),
        }
    })
}

fn check_ordering_reading(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    from: &str,
    keep: &str,
    replay: &str,
) -> Result<OrderingCheck, String> {
    let foreign = foreign_ancestry(branch);
    let own_revset = own_commits_revset(branch, from, replay);
    let held = log_lines(
        jj,
        store,
        &format!("heads(({from}..{replay}) & {foreign})"),
        "commit_id",
    )?;
    let own = graph(jj, store, &own_revset)?;
    if own.is_empty() {
        return Ok(OrderingCheck::Withhold {
            why: if held.is_empty() {
                format!("`{replay}` adds no commits over `{keep}`")
            } else {
                format!(
                    "every commit `{replay}` has over `{keep}` is already held by another branch \
                     (newest `{}`), so it has none of its own to replay",
                    held.join("`, `")
                )
            },
        });
    }
    let built_on = log_lines(
        jj,
        store,
        &format!("descendants({own_revset}) ~ ({own_revset}) ~ {PARKED_WORKING_COPY}"),
        "commit_id",
    )?;
    if !built_on.is_empty() {
        return Ok(OrderingCheck::Withhold {
            why: format!(
                "other work is built on `{replay}`'s own commits ({}), and the rebase would move it",
                built_on.join(", ")
            ),
        });
    }
    if !held.is_empty() {
        return Ok(OrderingCheck::Withhold {
            why: format!(
                "`{replay}` sits on a newer base than `{keep}`: its history includes `{}`, which \
                 another branch already holds. Replaying only its own commits would leave that \
                 base behind, and replaying the base too would rewrite another branch",
                held.join("`, `")
            ),
        });
    }
    let conflicts = if super::conflict::revset_has_conflict(jj, store, keep)? {
        None
    } else {
        replay_in_memory(store, &own, keep)
    };
    Ok(OrderingCheck::Offer { conflicts })
}

/// Two versions holding the same work: every commit `drop` has over `keep` has
/// a twin in `keep` (same change id, same description, and the same content
/// once replayed onto the twin's parent). Keeping `keep` loses nothing; the
/// twins are copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TwinStack {
    pub keep: String,
    pub drop: String,
    /// `drop`'s own commits, each with a twin in `keep`, parents first.
    pub twins: Vec<String>,
    /// What besides this branch's own names references the twins: other
    /// bookmarks, work built on them, a working copy. Empty when the store may
    /// abandon them itself. The branch's own remote-tracking bookmark is never
    /// listed, because the next publish moves it.
    pub referenced_by: Vec<String>,
}

impl TwinStack {
    /// The `jj abandon` revset naming exactly the twins.
    pub fn twins_revset(&self) -> String {
        format!("{}..{}", self.keep, self.drop)
    }
}

/// Find the twin stack between a conflicted name's two versions, if they form
/// one. A version that sits on shared history the other lacks (a newer base) is
/// only ever the one kept.
pub(crate) fn find_twin_stack(
    jj: &JjEnv,
    store: &Path,
    conflict: &ConflictedBookmark,
) -> Result<Option<TwinStack>, String> {
    let [first, second] = conflict.added.as_slice() else {
        return Ok(None);
    };
    for (keep, drop) in [(first, second), (second, first)] {
        if let Some(stack) =
            twin_stack_keeping(jj, store, &conflict.branch, &keep.commit, &drop.commit)?
        {
            return Ok(Some(stack));
        }
    }
    Ok(None)
}

fn twin_stack_keeping(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    keep: &str,
    drop: &str,
) -> Result<Option<TwinStack>, String> {
    let dropped = format!("{keep}..{drop}");
    let foreign = foreign_ancestry(branch);
    if !log_lines(jj, store, &format!("({dropped}) & {foreign}"), "commit_id")?.is_empty() {
        return Ok(None);
    }
    let drop_own = graph(jj, store, &dropped)?;
    if drop_own.is_empty() {
        return Ok(None);
    }
    let keep_own = graph(jj, store, &format!("{drop}..{keep}"))?;
    let mut by_change: HashMap<&str, Vec<&GraphCommit>> = HashMap::new();
    for commit in &keep_own {
        by_change.entry(&commit.change).or_default().push(commit);
    }
    let mut pairs = Vec::with_capacity(drop_own.len());
    for original in &drop_own {
        let [twin] = by_change
            .get(original.change.as_str())
            .map(Vec::as_slice)
            .unwrap_or_default()
        else {
            return Ok(None);
        };
        if original.conflicted || twin.conflicted {
            return Ok(None);
        }
        let ([original_parent], [twin_parent]) =
            (original.parents.as_slice(), twin.parents.as_slice())
        else {
            return Ok(None);
        };
        pairs.push((original, *twin, original_parent, twin_parent));
    }
    let Some(checkout) = super::export::resolve_backing_checkout(store) else {
        return Ok(None);
    };
    let objects = commit_objects(
        &checkout,
        pairs
            .iter()
            .flat_map(|(original, twin, ..)| [original.commit.as_str(), twin.commit.as_str()]),
    )?;
    for (original, twin, original_parent, twin_parent) in pairs {
        let (Some(original_object), Some(twin_object)) =
            (objects.get(&original.commit), objects.get(&twin.commit))
        else {
            return Ok(None);
        };
        if original_object.message != twin_object.message {
            return Ok(None);
        }
        match merge_tree(&checkout, original_parent, twin_parent, &original.commit) {
            Some(MergedTree::Clean(tree)) if tree == twin_object.tree => {}
            _ => return Ok(None),
        }
    }
    Ok(Some(TwinStack {
        keep: keep.to_string(),
        drop: drop.to_string(),
        twins: drop_own.into_iter().map(|commit| commit.commit).collect(),
        referenced_by: twin_references(jj, store, branch, &dropped)?,
    }))
}

struct CommitObject {
    tree: String,
    message: String,
}

/// Each commit's tree and full message, in one git call.
fn commit_objects<'a>(
    checkout: &Path,
    commits: impl Iterator<Item = &'a str>,
) -> Result<HashMap<String, CommitObject>, String> {
    let output = cairn_platform::env::git()
        .args(["log", "--no-walk=unsorted", "--format=%H%x00%T%x00%B%x00"])
        .args(commits)
        .args(["--"])
        .current_dir(checkout)
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|error| format!("git log (twin commit objects): {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git log (twin commit objects): {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut fields = stdout.split('\0');
    let mut objects = HashMap::new();
    // `--format` terminates each record with a newline, which lands at the
    // start of the next record's hash field.
    while let (Some(commit), Some(tree), Some(message)) =
        (fields.next(), fields.next(), fields.next())
    {
        let commit = commit.trim();
        if commit.is_empty() {
            break;
        }
        objects.insert(
            commit.to_string(),
            CommitObject {
                tree: tree.trim().to_string(),
                message: message.to_string(),
            },
        );
    }
    Ok(objects)
}

/// What besides this branch's own names references the commits of `twins`.
fn twin_references(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
    twins: &str,
) -> Result<Vec<String>, String> {
    let names = jj.run(
        store,
        &[
            "log",
            "-r",
            twins,
            "--no-graph",
            "-T",
            "local_bookmarks.map(|b| \"local\\t\" ++ b.name() ++ \"\\n\").join(\"\") ++ \
             remote_bookmarks.map(|b| \"remote\\t\" ++ b.name() ++ \"\\t\" ++ b.remote() ++ \"\\n\").join(\"\")",
            "--ignore-working-copy",
        ],
        "jj log (twin references)",
    )?;
    let mut references = Vec::new();
    for line in names.lines() {
        let mut fields = line.split('\t');
        let (kind, name) = (
            fields.next().unwrap_or_default(),
            fields.next().unwrap_or_default(),
        );
        if name.is_empty() || name == branch {
            continue;
        }
        let reference = match kind {
            "remote" => format!("bookmark `{name}@{}`", fields.next().unwrap_or_default()),
            _ => format!("bookmark `{name}`"),
        };
        if !references.contains(&reference) {
            references.push(reference);
        }
    }
    for commit in log_lines(
        jj,
        store,
        &format!("descendants({twins}) ~ ({twins}) ~ {PARKED_WORKING_COPY}"),
        "commit_id",
    )? {
        references.push(format!("commit `{commit}`, built on them"));
    }
    for commit in log_lines(
        jj,
        store,
        &format!("({twins}) & working_copies()"),
        "commit_id",
    )? {
        references.push(format!("a working copy at `{commit}`"));
    }
    Ok(references)
}

/// The nearest base-branch commit a version is built on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionBase {
    pub commit: String,
    /// The first line of the base commit's description.
    pub description: String,
    /// The pull request that commit landed, when its description names one.
    pub pull_request: Option<u64>,
    /// This base is newer than some other version's and older than none.
    pub newest: bool,
}

/// Each of `conflict.added`'s nearest `base_branch` ancestor, in order. `None`
/// where the store could not say.
pub(crate) fn version_bases(
    jj: &JjEnv,
    store: &Path,
    conflict: &ConflictedBookmark,
    base_branch: &str,
) -> Vec<Option<VersionBase>> {
    let base =
        format!("(bookmarks(exact:{base_branch:?}) | remote_bookmarks(exact:{base_branch:?}))");
    let mut bases = conflict
        .added
        .iter()
        .map(|version| {
            let out = jj
                .run(
                    store,
                    &[
                        "log",
                        "-r",
                        &format!("latest(heads(::{} & ::{base}))", version.commit),
                        "--no-graph",
                        "-T",
                        "commit_id ++ \"\\t\" ++ description.first_line() ++ \"\\n\"",
                        "--ignore-working-copy",
                    ],
                    "jj log (version base)",
                )
                .ok()?;
            let (commit, description) = out.lines().next()?.split_once('\t')?;
            Some(VersionBase {
                commit: commit.trim().to_string(),
                description: description.trim().to_string(),
                pull_request: pull_request_number(description),
                newest: false,
            })
        })
        .collect::<Vec<_>>();
    let commits = bases
        .iter()
        .map(|base| base.as_ref().map(|base| base.commit.clone()))
        .collect::<Vec<_>>();
    for (index, base) in bases.iter_mut().enumerate() {
        let Some(base) = base else { continue };
        let others = commits
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .filter_map(|(_, commit)| commit.as_deref())
            .collect::<Vec<_>>();
        base.newest = others.iter().any(|other| *other != base.commit)
            && others
                .iter()
                .all(|other| revset_descends_from(jj, store, &base.commit, other));
    }
    bases
}

/// The last `#123` a description names: the squash-merge suffix GitHub writes.
fn pull_request_number(description: &str) -> Option<u64> {
    description
        .match_indices('#')
        .filter_map(|(at, _)| {
            let digits = description[at + 1..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            digits.parse().ok()
        })
        .last()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pull_request_number_is_the_last_one_a_description_names() {
        assert_eq!(pull_request_number("Fix the page (#4167)"), Some(4167));
        assert_eq!(pull_request_number("Follow up #12 for (#4138)"), Some(4138));
        assert_eq!(pull_request_number("no number # here"), None);
        assert_eq!(pull_request_number(""), None);
    }

    #[test]
    fn foreign_ancestry_excludes_this_branch_by_name() {
        assert_eq!(
            foreign_ancestry("agent/x"),
            "::(bookmarks(~exact:\"agent/x\") | remote_bookmarks(~exact:\"agent/x\"))"
        );
    }
}
