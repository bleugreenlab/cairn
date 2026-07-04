//! Conflict detection over commits/branches and divergent-change collapse.
use super::*;
use std::path::Path;

/// Whether any commit in `revset` carries a recorded conflict. `revset` is any
/// revset string — a bare bookmark name (`integration`), a remote ref
/// (`main@origin`), or a `bookmarks(exact:...)` expression. Used to vet a rebase
/// DEST before handing it to clean siblings: a conflicted dest must never
/// propagate down to children.
pub fn revset_has_conflict(jj: &JjEnv, store: &Path, revset: &str) -> Result<bool, String> {
    let out = jj.run(
        store,
        &["log", "-r", revset, "--no-graph", "-T", "self.conflict()"],
        "jj dest conflict check",
    )?;
    Ok(out.contains("true"))
}

/// Whether a bookmark's commit carries a recorded conflict. GitHub reports a
/// jj-conflicted commit as mergeable (and renders it as garbage), so the merge
/// gate trusts this over the GitHub mergeable bit for jj projects.
pub fn branch_has_conflict(jj: &JjEnv, store: &Path, branch: &str) -> Result<bool, String> {
    revset_has_conflict(jj, store, &format!("bookmarks(exact:{:?})", branch))
}

/// Enumerate the conflicting file paths in a workspace whose conflict markers are
/// already materialized on disk (callers run [`update_stale`] first). Runs IN the
/// workspace, not the bare store: `jj resolve --list` is working-copy-scoped, so
/// it must see the materialized `@`. Each output line is `<path>  <N-sided
/// conflict>`; the leading whitespace-delimited token is the path. Returns empty
/// on no conflicts (jj exits non-zero with "No conflicts found", mapped to an Err
/// and swallowed) or any other error — the file list is advisory detail on a note,
/// never load-bearing.
pub fn conflicted_files(jj: &JjEnv, ws_path: &Path) -> Vec<String> {
    let Ok(out) = jj.run(ws_path, &["resolve", "--list"], "jj resolve --list") else {
        return Vec::new();
    };
    out.lines()
        .filter_map(|line| line.split_whitespace().next())
        .map(|token| token.to_string())
        .collect()
}

/// A commit carrying a recorded conflict, with the paths whose merge did not
/// resolve. Store-side detail for *reporting* which commits and files block a
/// merge — advisory enumeration only; the boolean [`branch_has_conflict`] stays
/// the load-bearing gate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictedCommit {
    /// Short commit id.
    pub commit_id: String,
    /// Short change id.
    pub change_id: String,
    /// First line of the commit description.
    pub description: String,
    /// Conflicted file paths recorded in this commit.
    pub files: Vec<String>,
}

/// Enumerate the conflicted commits in `range_revset` (e.g. `"a..b"`, or a bare
/// `bookmarks(exact:...)`) with their conflicted file paths — store-side, no
/// workspace needed. Returns an empty Vec on no conflicts or any jj error: this
/// is advisory detail for a diagnostic, never load-bearing (mirrors
/// [`conflicted_files`]).
///
/// In jj a conflict in an ancestor propagates to every descendant until
/// resolved, so `<dest>..<branch> & conflicts()` names precisely the offending
/// commits. Fields are emitted unit-separated (`\x1f`) and the file list
/// record-separated (`\x1e`) so the parse stays robust against arbitrary
/// descriptions and paths.
pub fn conflicted_commits(jj: &JjEnv, store: &Path, range_revset: &str) -> Vec<ConflictedCommit> {
    // jj template, verified against jj 0.42: `\x1f`/`\x1e` are jj string escapes
    // that emit the literal control bytes we split on; `conflicts()` is the
    // revset and `self.conflicted_files()` yields store-side paths (no workspace).
    const TEMPLATE: &str = "commit_id.short() ++ \"\\x1f\" ++ change_id.short() ++ \"\\x1f\" ++ description.first_line() ++ \"\\x1f\" ++ self.conflicted_files().map(|f| f.path()).join(\"\\x1e\") ++ \"\\n\"";
    let revset = format!("({range_revset}) & conflicts()");
    let Ok(out) = jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", TEMPLATE],
        "jj conflicted commits",
    ) else {
        return Vec::new();
    };
    out.lines()
        .filter_map(|line| {
            let mut parts = line.split('\u{1f}');
            let commit_id = parts.next()?.trim().to_string();
            if commit_id.is_empty() {
                return None;
            }
            let change_id = parts.next().unwrap_or_default().trim().to_string();
            let description = parts.next().unwrap_or_default().trim().to_string();
            let files = parts
                .next()
                .map(|field| {
                    field
                        .split('\u{1e}')
                        .map(str::trim)
                        .filter(|p| !p.is_empty())
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            Some(ConflictedCommit {
                commit_id,
                change_id,
                description,
                files,
            })
        })
        .collect()
}

/// Classification of a branch (already rebased onto its dest) for flatten
/// recovery. A base advance that re-applies a whole feature lineage onto a new
/// base can record conflicts in INTERMEDIATE commits while a later commit
/// resolves them, leaving the NET tip clean. jj still refuses to push or fold a
/// branch whose history contains ANY conflicted commit, so a mechanically-clean
/// tip is blocked by its own conflicted ancestors. This three-way split drives
/// the guarded flatten that clears them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlattenState {
    /// No recorded conflict anywhere in `dest..branch` — nothing to recover.
    Clean,
    /// The branch TIP itself carries a recorded conflict. A flatten preserves the
    /// tip tree, so it would NOT clear this — the agent must resolve the markers
    /// and re-seal. Kept distinct so callers escalate rather than flatten.
    TipConflicted,
    /// The tip is clean but an INTERMEDIATE commit in `dest..branch` carries a
    /// recorded conflict — flatten-recoverable: collapsing to one commit whose
    /// tree equals the clean tip discards the conflicted history without changing
    /// the net tree.
    IntermediateOnly,
}

/// Classify a branch for flatten recovery against `dest_revset` (any revset that
/// resolves to the dest tip — a bookmark name, `<default>@origin`, or a concrete
/// commit id). The tip is checked FIRST so a conflicted tip is never misread as a
/// recoverable intermediate: `conflicted_commits(dest..branch)` would include the
/// tip too, but a conflicted tip needs manual resolution, not a flatten.
pub fn flatten_state(
    jj: &JjEnv,
    store: &Path,
    dest_revset: &str,
    branch: &str,
) -> Result<FlattenState, String> {
    if branch_has_conflict(jj, store, branch)? {
        return Ok(FlattenState::TipConflicted);
    }
    let range = format!("({dest_revset})..bookmarks(exact:{branch:?})");
    if conflicted_commits(jj, store, &range).is_empty() {
        Ok(FlattenState::Clean)
    } else {
        Ok(FlattenState::IntermediateOnly)
    }
}

/// True when `tip_revset` resolves to a commit that already descends from (or
/// equals) `dest_commit`. `tip_revset` is any single-revision revset — a
/// `bookmarks(...)` expression, a `<name>@` workspace ref, etc. Implemented with
/// the DAG-range operator `dest::(tip)`: that range is non-empty exactly when
/// `tip` is reachable forward from `dest`, i.e. `tip` descends from or equals
/// `dest`. A resolve failure (a ref that does not exist) falls to `false` so the
/// caller rebases rather than wrongly skipping.
pub(crate) fn revset_descends_from(
    jj: &JjEnv,
    store: &Path,
    tip_revset: &str,
    dest_commit: &str,
) -> bool {
    let revset = format!("{dest_commit}::({tip_revset})");
    jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", "commit_id"],
        "jj descends check",
    )
    .ok()
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// True when `branch`'s tip already descends from (or equals) `dest_commit`.
/// Lets a reconcile skip re-rebasing an already-rebased branch so repeated or
/// serialized passes never re-rewrite an already-rebased commit (clean or
/// conflicted) — the structural idempotence that, paired with single-writer
/// serialization, stops new divergent conflicted copies from accumulating and
/// keeps a resolved clean bookmark from being dragged back onto a conflicted
/// twin.
pub fn branch_descends_from(jj: &JjEnv, store: &Path, branch: &str, dest_commit: &str) -> bool {
    revset_descends_from(
        jj,
        store,
        &format!("bookmarks(exact:{branch:?})"),
        dest_commit,
    )
}

/// True when `branch`'s tip is an ancestor of (or equals) `dest_commit`.
/// A resolve failure falls to `false`, matching [`revset_descends_from`]: callers
/// should proceed with the normal rebase rather than wrongly classifying a branch
/// as safely fast-forwardable.
pub fn branch_is_ancestor_of(jj: &JjEnv, store: &Path, branch: &str, dest_commit: &str) -> bool {
    let branch_revset = format!("bookmarks(exact:{branch:?})");
    let revset = format!("({branch_revset})::{dest_commit}");
    jj.run(
        store,
        &["log", "-r", &revset, "--no-graph", "-T", "commit_id"],
        "jj ancestor check",
    )
    .ok()
    .map(|s| !s.trim().is_empty())
    .unwrap_or(false)
}

/// The change-id of a bookmark's tip over the store, or empty when the bookmark
/// does not resolve. A jj *divergent* change's twins all share ONE change-id,
/// and the bookmark points at exactly one of them, so this returns that shared
/// id even mid-divergence. `--ignore-working-copy` keeps it store-driven.
pub fn change_id_of(jj: &JjEnv, store: &Path, branch: &str) -> String {
    jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("bookmarks(exact:{branch:?})"),
            "--no-graph",
            "-T",
            "change_id",
            "--ignore-working-copy",
        ],
        "change id of bookmark",
    )
    .map(|s| s.trim().to_string())
    .unwrap_or_default()
}

/// Every visible commit id sharing one change-id over the store. A healthy change
/// resolves to exactly one commit; a jj *divergent* change (the `<id>/0 /1 ...`
/// accumulation) resolves to several. The `change_id(...)` revset function is
/// used (not the bare id) because jj refuses a bare divergent change-id symbol.
pub fn visible_commit_ids_for_change(jj: &JjEnv, store: &Path, change_id: &str) -> Vec<String> {
    jj.run(
        store,
        &[
            "log",
            "-r",
            &format!("change_id({change_id})"),
            "--no-graph",
            "-T",
            "commit_id ++ \"\\n\"",
            "--ignore-working-copy",
        ],
        "visible commits for change",
    )
    .unwrap_or_default()
    .lines()
    .map(|l| l.trim().to_string())
    .filter(|l| !l.is_empty())
    .collect()
}

/// Count the visible commits sharing one change-id over the store. Thin wrapper
/// over [`visible_commit_ids_for_change`].
pub fn visible_commits_for_change(jj: &JjEnv, store: &Path, change_id: &str) -> usize {
    visible_commit_ids_for_change(jj, store, change_id).len()
}

/// One twin of a (possibly) divergent change: its commit id and whether it
/// carries a recorded conflict.
struct Twin {
    commit: String,
    conflicted: bool,
}

/// The result of collapsing a (possibly) divergent change on a bookmark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollapseOutcome {
    /// The change resolves to 0 or 1 visible commit — nothing to collapse.
    NotDivergent,
    /// Exactly one non-conflicted twin survived: the bookmark now points at it
    /// and every other twin was abandoned.
    Collapsed {
        kept: String,
        abandoned: Vec<String>,
    },
    /// Canonicalization is ambiguous — every twin conflicts, or more than one
    /// carries edits. The store is left UNTOUCHED for a human to resolve.
    Ambiguous {
        change_id: String,
        twins: Vec<String>,
    },
}

/// Collapse a *pre-existing* divergent change on `branch` over the shared store.
/// New divergence is already prevented upstream (the per-store mutex plus the
/// idempotent [`reconcile_siblings`] skip); this heals a twin that forked BEFORE
/// that serialization landed, or via an external `jj` op outside Cairn's lock —
/// the cleanup `reconcile_siblings` never did, because it operates on each
/// bookmark's single tip and never touches the orphaned twin sharing the
/// change-id, so [`sealed_commit_is_lost`] keeps firing on every re-seal.
///
/// Resolves the bookmark tip's change-id, lists every visible commit sharing it,
/// and:
/// - 0 or 1 visible commit -> [`CollapseOutcome::NotDivergent`] (no-op);
/// - exactly one NON-conflicted twin -> keep it, `bookmark set` the bookmark to
///   it, abandon the conflicted twin(s) -> [`CollapseOutcome::Collapsed`];
/// - otherwise (every twin conflicts, or more than one clean twin) ->
///   [`CollapseOutcome::Ambiguous`] with NO mutation.
///
/// The single-clean-twin rule is the deliberate hybrid: the lone resolved commit
/// the agent produced is the unambiguous keep, but choosing among several clean
/// twins (or among only-conflicted ones) would be a guess on the shared store
/// that could lose work, so those tangles surface to a human instead. There is
/// deliberately NO descendancy tiebreak among multiple clean twins.
///
/// Mutations use `--ignore-working-copy` (store-driven, like `reconcile_siblings`).
/// The caller MUST hold the per-store lock so the collapse cannot itself fork.
pub fn collapse_divergent_bookmark(
    jj: &JjEnv,
    store: &Path,
    branch: &str,
) -> Result<CollapseOutcome, String> {
    // A bookmark that does not resolve to a local tip (missing, or a
    // remote-tracking dest like `main@origin`) has nothing to collapse here.
    if bookmark_commit(jj, store, branch).is_none() {
        return Ok(CollapseOutcome::NotDivergent);
    }
    let change_id = change_id_of(jj, store, branch);
    if change_id.is_empty() {
        return Ok(CollapseOutcome::NotDivergent);
    }

    let commit_ids = visible_commit_ids_for_change(jj, store, &change_id);
    if commit_ids.len() <= 1 {
        return Ok(CollapseOutcome::NotDivergent);
    }

    // Classify each twin by whether it carries a recorded conflict. A transient
    // check error falls to `false` (treat as clean) — the same
    // liveness-over-strictness convention as the reconcile guards; the
    // single-clean-twin rule below still surfaces anything genuinely ambiguous.
    let twins: Vec<Twin> = commit_ids
        .into_iter()
        .map(|commit| {
            let conflicted = revset_has_conflict(jj, store, &commit).unwrap_or(false);
            Twin { commit, conflicted }
        })
        .collect();

    let clean: Vec<&Twin> = twins.iter().filter(|twin| !twin.conflicted).collect();
    if clean.len() != 1 {
        return Ok(CollapseOutcome::Ambiguous {
            change_id,
            twins: twins.into_iter().map(|twin| twin.commit).collect(),
        });
    }

    let kept = clean[0].commit.clone();
    let abandoned: Vec<String> = twins
        .iter()
        .filter(|twin| twin.commit != kept)
        .map(|twin| twin.commit.clone())
        .collect();

    // Point the bookmark at the surviving clean twin FIRST (so it never strands
    // on a commit we are about to abandon), then drop every other twin.
    jj.run(
        store,
        &[
            "bookmark",
            "set",
            branch,
            "-r",
            &kept,
            "--ignore-working-copy",
        ],
        "collapse: point bookmark at the clean twin",
    )?;
    for commit in &abandoned {
        jj.run(
            store,
            &["abandon", commit, "--ignore-working-copy"],
            "collapse: abandon divergent twin",
        )?;
    }
    Ok(CollapseOutcome::Collapsed { kept, abandoned })
}
