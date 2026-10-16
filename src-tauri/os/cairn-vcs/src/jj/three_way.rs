//! The advisory three-way content merge behind `cairn:~/rebase`.
//!
//! A conflict diagnostic stores three immutable commits and no patches, so both
//! sides of the merge are recomputable on demand. This module goes one step
//! further and computes the MERGE itself, line-wise and in this process, which
//! answers two questions with one primitive:
//!
//! 1. What the merged file actually looks like — base, ours, and theirs
//!    projected together, with everything non-overlapping already resolved.
//! 2. Whether restoring a path WHOLE from the branch's committed tip (what
//!    `resolution:"take-committed-tip"` does) would discard incoming work.
//!
//! The second is the load-bearing one. `jj restore --from <tip> -- <path>` is
//! whole-file, so every incoming hunk in a conflicting file that lives OUTSIDE
//! the conflicting region is discarded along with the region the agent resolved.
//! The invariant that detects it is one line:
//!
//! > The whole-file restore of a path is lossless exactly when
//! > `completion_candidate(base, ours, theirs) == ours`.
//!
//! Taking `ours` at every conflict reproduces the agent's own resolution, and
//! the merge's clean regions carry the incoming hunks. So a candidate that still
//! equals the committed tip proves the tip already holds both sides; a candidate
//! that differs names, precisely, the incoming work the restore would drop.
//!
//! Everything here is ADVISORY, and says so wherever it is rendered. The store's
//! replay is judged by jj's own merge, never by this one. This exists so an
//! agent can see the merge before requesting the replay, and so a replay that
//! would silently throw work away is refused instead.

use super::*;
use std::collections::BTreeSet;
use std::path::Path;

/// The conflict-marker run length `diffy::merge` writes with its default
/// options. Asserted by a unit test rather than assumed, because the collapse
/// below recognizes regions by exactly this run and would silently pass a whole
/// conflict through as content if diffy ever changed it.
const MARKER_LEN: usize = 7;

/// What a path's content is at one immutable revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileContent {
    /// Present, and valid UTF-8.
    Text(String),
    /// Not present at that revision — this side added or deleted the path.
    /// Established by asking jj which paths exist, never by pattern-matching an
    /// error string, so a real failure stays a failure.
    Absent,
    /// Present, but not valid UTF-8. A line-wise text merge cannot say anything
    /// true about it, so it says nothing rather than mangling it.
    Binary,
}

/// Resolve every overlapping region by retaining both sides, destination
/// (`theirs`) first and branch (`ours`) second. This is intentionally narrower
/// than [`completion_candidate`]: it is used only for registry tail appends whose
/// entries are independent and commute.
pub fn union_completion_candidate(base: &str, ours: &str, theirs: &str) -> String {
    match merge_preview(base, ours, theirs) {
        MergePreview::Clean(text) => text,
        MergePreview::Conflicted { text, .. } => collapse_conflicts_to_union(&text),
    }
}

fn collapse_conflicts_to_union(merged: &str) -> String {
    #[derive(Clone, Copy)]
    enum Region {
        Body,
        Ours,
        Original,
        Theirs,
    }
    let mut out = String::with_capacity(merged.len());
    let mut ours = String::new();
    let mut theirs = String::new();
    let mut state = Region::Body;
    for line in merged.split_inclusive('\n') {
        let bare = line.trim_end_matches(['\n', '\r']);
        match state {
            Region::Body => {
                if is_marker(bare, '<') {
                    ours.clear();
                    theirs.clear();
                    state = Region::Ours;
                } else {
                    out.push_str(line);
                }
            }
            Region::Ours => {
                if is_marker(bare, '|') {
                    state = Region::Original;
                } else if is_marker(bare, '=') {
                    state = Region::Theirs;
                } else {
                    ours.push_str(line);
                }
            }
            Region::Original => {
                if is_marker(bare, '=') {
                    state = Region::Theirs;
                }
            }
            Region::Theirs => {
                if is_marker(bare, '>') {
                    out.push_str(&theirs);
                    out.push_str(&ours);
                    state = Region::Body;
                } else {
                    theirs.push_str(line);
                }
            }
        }
    }
    out
}

/// Every file below one repo-relative directory at an immutable revision.
/// This is the directory companion to [`paths_present_at`]: callers discovering
/// branch-added migrations cannot name the files before asking the tree.
pub fn paths_under_at(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
    directory: &str,
) -> Result<BTreeSet<String>, String> {
    let listed = jj.run(
        store,
        &[
            "file",
            "list",
            "--ignore-working-copy",
            "-r",
            rev,
            "--",
            directory,
        ],
        "jj file list (directory at revision)",
    )?;
    Ok(listed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

impl FileContent {
    /// The mergeable text for this side. An absent path merges as empty content,
    /// which is what an add on the other side means; binary content has no text
    /// answer at all.
    pub fn mergeable(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Absent => Some(""),
            Self::Binary => None,
        }
    }
}

/// Which of `paths` exist at `rev`, in one call.
///
/// Presence is its own question, asked directly. Inferring it from a failed
/// `jj file show` would mean reading jj's error prose, which conflates "this
/// path was added on the other side" with "this commit was garbage-collected" —
/// two answers that must never be confused in a resource whose whole point is
/// not to substitute one fact for another.
pub fn paths_present_at(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
    paths: &[String],
) -> Result<BTreeSet<String>, String> {
    if paths.is_empty() {
        return Ok(BTreeSet::new());
    }
    let mut args = vec!["file", "list", "--ignore-working-copy", "-r", rev, "--"];
    args.extend(paths.iter().map(String::as_str));
    let listed = jj.run(store, &args, "jj file list (merge preview presence)")?;
    Ok(listed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

/// One path's content at one immutable revision. Read-only: nothing is
/// materialized in any checkout and the store is not mutated.
///
/// `present` is the answer [`paths_present_at`] already gave for this revision,
/// so a caller reading many paths pays one presence probe rather than one per
/// file.
pub fn file_at_revision(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
    path: &str,
    present: &BTreeSet<String>,
) -> Result<FileContent, String> {
    if !present.contains(path) {
        return Ok(FileContent::Absent);
    }
    // `run_bytes` rather than `run`: the trimming runner would eat leading and
    // trailing whitespace, and a merge that silently rewrites the file's last
    // byte is worse than no merge at all.
    let bytes = jj.run_bytes(
        store,
        &[
            "file",
            "show",
            "--ignore-working-copy",
            "-r",
            rev,
            "--",
            path,
        ],
        "jj file show (merge preview)",
    )?;
    Ok(match String::from_utf8(bytes) {
        Ok(text) => FileContent::Text(text),
        Err(_) => FileContent::Binary,
    })
}

/// One path's UTF-8 text at one revision, or `None` when the path does not
/// exist there. A binary file is an error: every caller wants to read the text.
pub fn text_at_revision(
    jj: &JjEnv,
    store: &Path,
    rev: &str,
    path: &str,
) -> Result<Option<String>, String> {
    let present = paths_present_at(jj, store, rev, &[path.to_string()])?;
    match file_at_revision(jj, store, rev, path, &present)? {
        FileContent::Text(text) => Ok(Some(text)),
        FileContent::Absent => Ok(None),
        FileContent::Binary => Err(format!("`{path}` is not UTF-8 text at revision `{rev}`")),
    }
}

/// The three sides of one path's merge, read from immutable commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeSides {
    pub base: FileContent,
    pub ours: FileContent,
    pub theirs: FileContent,
}

/// A diff3-style merge of one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergePreview {
    /// The two sides did not overlap: this is the whole merged file, ready to
    /// commit as-is.
    Clean(String),
    /// Overlapping regions remain, rendered with `<<<<<<< ours`,
    /// `||||||| original`, `=======`, `>>>>>>> theirs` markers.
    Conflicted { text: String, regions: usize },
}

impl MergePreview {
    pub fn text(&self) -> &str {
        match self {
            Self::Clean(text) => text,
            Self::Conflicted { text, .. } => text,
        }
    }

    pub fn regions(&self) -> usize {
        match self {
            Self::Clean(_) => 0,
            Self::Conflicted { regions, .. } => *regions,
        }
    }
}

/// The classic three-way projection: base, ours, and theirs merged line-wise,
/// with overlapping regions left as diff3 markers.
pub fn merge_preview(base: &str, ours: &str, theirs: &str) -> MergePreview {
    match diffy::merge(base, ours, theirs) {
        Ok(text) => MergePreview::Clean(text),
        Err(text) => {
            let regions = count_conflict_regions(&text);
            MergePreview::Conflicted { text, regions }
        }
    }
}

/// The same merge with every conflict region collapsed to its `ours` side: the
/// complete file the branch would need to commit for a whole-file restore to be
/// lossless.
pub fn completion_candidate(base: &str, ours: &str, theirs: &str) -> String {
    match merge_preview(base, ours, theirs) {
        MergePreview::Clean(text) => text,
        MergePreview::Conflicted { text, .. } => collapse_conflicts_to_ours(&text),
    }
}

/// Strip every conflict region in a diff3-style merge down to its `ours` side.
///
/// A state machine rather than a marker scan, deliberately: content that merely
/// LOOKS like an opening marker cannot open a region while one is already open,
/// and each state accepts only the one marker that legitimately follows it. That
/// keeps a file whose own text contains conflict scaffolding — a doc example, a
/// test fixture — from being silently rewritten.
///
/// Load-bearing beyond legibility: [`completion_candidate`] is what the loss
/// invariant compares against, so a region read wrongly turns a safe replay into
/// a refusal, or a lossy one into a silent drop.
fn collapse_conflicts_to_ours(merged: &str) -> String {
    #[derive(Clone, Copy)]
    enum Region {
        /// Outside any conflict: merged content, kept.
        Body,
        /// Inside the `ours` side: kept, because taking ours at every conflict
        /// is what a whole-file restore of the committed tip does.
        Ours,
        /// Inside the `||||||| original` side: dropped.
        Original,
        /// Inside the `theirs` side: dropped.
        Theirs,
    }

    let mut out = String::with_capacity(merged.len());
    let mut state = Region::Body;
    // `split_inclusive` keeps each line's terminator, so a file with no trailing
    // newline survives the round trip unchanged.
    for line in merged.split_inclusive('\n') {
        let bare = line.trim_end_matches(['\n', '\r']);
        match state {
            Region::Body => {
                if is_marker(bare, '<') {
                    state = Region::Ours;
                } else {
                    out.push_str(line);
                }
            }
            Region::Ours => {
                if is_marker(bare, '|') {
                    state = Region::Original;
                } else if is_marker(bare, '=') {
                    // Only reachable under the non-diff3 conflict style, which
                    // has no original section. Accepted so the collapse does not
                    // depend on which style produced its input.
                    state = Region::Theirs;
                } else {
                    out.push_str(line);
                }
            }
            Region::Original => {
                if is_marker(bare, '=') {
                    state = Region::Theirs;
                }
            }
            Region::Theirs => {
                if is_marker(bare, '>') {
                    state = Region::Body;
                }
            }
        }
    }
    out
}

/// Whether a line is a conflict marker of the given kind: exactly [`MARKER_LEN`]
/// of the marker character, then end-of-line or the space before a label.
fn is_marker(line: &str, marker: char) -> bool {
    let mut chars = line.chars();
    for _ in 0..MARKER_LEN {
        if chars.next() != Some(marker) {
            return false;
        }
    }
    matches!(chars.next(), None | Some(' '))
}

fn count_conflict_regions(merged: &str) -> usize {
    merged
        .lines()
        .filter(|line| is_marker(line.trim_end_matches('\r'), '<'))
        .count()
}

/// The incoming work a whole-file restore would discard, and the file that would
/// keep it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedWork {
    /// The COMPLETE file that would make the restore lossless: the merge with
    /// every conflict region taken from the committed tip. Marker-free, so it is
    /// directly committable.
    pub candidate: String,
    /// Unified diff from the committed tip to that candidate — exactly the work
    /// the restore drops, and nothing else.
    pub diff: String,
    /// Lines that diff adds.
    pub added_lines: usize,
    /// Hunks that diff carries.
    pub hunks: usize,
}

/// Whether restoring one path WHOLE from the branch's committed tip keeps
/// everything both sides have.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestoreVerdict {
    /// The committed tip already contains every incoming hunk in this path, so
    /// the whole-file restore is exactly right.
    Lossless,
    /// The restore would discard incoming work. Positive evidence, and the
    /// evidence itself is the remedy: committing `candidate` makes the same path
    /// lossless.
    Lossy(DroppedWork),
    /// Not assessable line-wise. Carries the reason, written for the agent.
    ///
    /// This is never a refusal ground. Refusal is reserved for positive evidence
    /// of loss, mirroring how a base-drift classification is only ever concluded
    /// from positive evidence that the sides agree — an instrument that stops
    /// work because it could not see is worse than one that names what it could
    /// not see.
    NotAssessed(String),
}

/// Apply the loss invariant to one path's three sides.
pub fn assess_whole_file_restore(sides: &MergeSides) -> RestoreVerdict {
    if let FileContent::Absent = sides.theirs {
        return RestoreVerdict::NotAssessed(
            "the incoming change deletes this path; keeping your committed version is a decision \
             no line-wise merge can make for you"
                .to_string(),
        );
    }
    if let FileContent::Absent = sides.ours {
        return RestoreVerdict::NotAssessed(
            "your branch deletes this path while the incoming change still edits it; the replay \
             would restore your deletion whole"
                .to_string(),
        );
    }
    let (Some(base), Some(ours), Some(theirs)) = (
        sides.base.mergeable(),
        sides.ours.mergeable(),
        sides.theirs.mergeable(),
    ) else {
        return RestoreVerdict::NotAssessed(
            "one side of this path is not UTF-8 text, so it cannot be merged line-wise".to_string(),
        );
    };
    let candidate = completion_candidate(base, ours, theirs);
    if same_ignoring_one_trailing_newline(&candidate, ours) {
        return RestoreVerdict::Lossless;
    }
    RestoreVerdict::Lossy(describe_drop(ours, &candidate))
}

/// diffy's marker writer inserts a newline before a marker when the side above
/// it did not end with one, so a conflict that runs to end-of-file can come back
/// carrying a trailing newline the committed tip does not have. That single byte
/// is an artifact of the preview, not incoming work, so it is normalized away
/// rather than reported as a dropped hunk.
fn same_ignoring_one_trailing_newline(a: &str, b: &str) -> bool {
    a == b || a.strip_suffix('\n') == Some(b) || b.strip_suffix('\n') == Some(a)
}

/// A unified diff between two texts, without diffy's `--- original` /
/// `+++ modified` headers: they name nothing real here, and whatever renders the
/// diff states which two things it actually runs between.
pub fn create_patch_body(from: &str, to: &str) -> String {
    diffy::create_patch(from, to)
        .to_string()
        .lines()
        .skip_while(|line| line.starts_with("--- ") || line.starts_with("+++ "))
        .collect::<Vec<_>>()
        .join("\n")
}

fn describe_drop(ours: &str, candidate: &str) -> DroppedWork {
    let diff = create_patch_body(ours, candidate);
    let body: Vec<&str> = diff.lines().collect();
    let added_lines = body
        .iter()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
        .count();
    let hunks = body.iter().filter(|line| line.starts_with("@@")).count();
    DroppedWork {
        candidate: candidate.to_string(),
        diff,
        added_lines,
        hunks,
    }
}

/// One path's assessment, ready to render or to refuse on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathAssessment {
    pub path: String,
    pub verdict: RestoreVerdict,
}

/// Read the three sides of every named path, in as few jj calls as the question
/// allows: one presence probe per side for the whole set, then one content read
/// per side per present path.
///
/// A path whose CONTENT cannot be read degrades to an `Err` for that path alone.
/// This is an instrument on a read path; one unreadable file must not take the
/// page down with it.
///
/// A failed PRESENCE probe is different in kind and is never degraded, because a
/// probe answers for a whole revision at once. Substituting an empty set for a
/// failed one would say "absent everywhere" — which is not missing evidence but
/// manufactured evidence, and it manufactures it in the dangerous direction:
/// merging against an empty base makes both sides look like whole-file additions,
/// the whole file becomes one conflict region, collapsing it to `ours` reproduces
/// `ours`, and a lossy restore is certified lossless on the strength of a store
/// hiccup. `an_empty_base_would_falsely_certify_a_lossy_restore` holds that
/// specific corruption, so the reason this is an error survives the next reader.
pub fn read_sides_for_paths(
    jj: &JjEnv,
    store: &Path,
    base_rev: &str,
    ours_rev: &str,
    theirs_rev: &str,
    paths: &[String],
) -> Vec<(String, Result<MergeSides, String>)> {
    let probe = |rev: &str| {
        paths_present_at(jj, store, rev, paths)
            .map_err(|error| format!("could not list files at {rev}: {error}"))
    };
    let (base_present, ours_present, theirs_present) =
        match combine_presence(probe(base_rev), probe(ours_rev), probe(theirs_rev)) {
            Ok(present) => present,
            Err(error) => {
                return paths
                    .iter()
                    .map(|path| (path.clone(), Err(error.clone())))
                    .collect()
            }
        };

    paths
        .iter()
        .map(|path| {
            let sides = (|| {
                Ok(MergeSides {
                    base: file_at_revision(jj, store, base_rev, path, &base_present)?,
                    ours: file_at_revision(jj, store, ours_rev, path, &ours_present)?,
                    theirs: file_at_revision(jj, store, theirs_rev, path, &theirs_present)?,
                })
            })();
            (path.clone(), sides)
        })
        .collect()
}

/// Require all three per-revision presence probes to have answered.
///
/// Pure, and split out so the refusal can be proven without a store: a partial
/// answer here is indistinguishable downstream from a real one, so this is the
/// seam where it has to be caught. Every failure is reported, not just the
/// first — when a store is unwell it is usually unwell for more than one read,
/// and naming one of three sends the reader looking in the wrong place.
#[allow(clippy::type_complexity)]
fn combine_presence(
    base: Result<BTreeSet<String>, String>,
    ours: Result<BTreeSet<String>, String>,
    theirs: Result<BTreeSet<String>, String>,
) -> Result<(BTreeSet<String>, BTreeSet<String>, BTreeSet<String>), String> {
    match (base, ours, theirs) {
        (Ok(base), Ok(ours), Ok(theirs)) => Ok((base, ours, theirs)),
        (base, ours, theirs) => Err(format!(
            "this path could not be merged: {}",
            [base.err(), ours.err(), theirs.err()]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("; ")
        )),
    }
}

/// Apply the loss invariant to every named path.
///
/// The single computation behind three surfaces: the summary's advance warning
/// about clean hunks inside conflicting files, the resolution assessment, and
/// the guard that refuses a lossy `take-committed-tip`. One answer, so those
/// three can never disagree with each other.
pub fn assess_paths(
    jj: &JjEnv,
    store: &Path,
    base_rev: &str,
    ours_rev: &str,
    theirs_rev: &str,
    paths: &[String],
) -> Vec<PathAssessment> {
    read_sides_for_paths(jj, store, base_rev, ours_rev, theirs_rev, paths)
        .into_iter()
        .map(|(path, sides)| PathAssessment {
            path,
            verdict: match sides {
                Ok(sides) => assess_whole_file_restore(&sides),
                Err(error) => RestoreVerdict::NotAssessed(error),
            },
        })
        .collect()
}

/// What carrying one path's committed resolution onto a newer destination
/// produces.
///
/// A resolution is made against one incoming side and committed linearly, so it
/// holds that side's content without that side in its ancestry. When the base
/// has advanced again by replay time, restoring the committed file whole would
/// overwrite everything the newer destination added to it. Carrying it instead
/// merges the newer destination's changes in, three-way, with the side the
/// resolution was made against as the base: the agent's resolution is what
/// changed on one side, and the destination's further work on the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Carried {
    /// The destination holds exactly what the resolution was made against, so
    /// the committed file is already the complete answer and a whole-file
    /// restore is exact. Also the only arm a binary path can reach.
    Unchanged,
    /// The destination changed this path only where the resolution did not:
    /// the committed resolution with those changes merged in.
    Merged(String),
    /// The destination changed the region the resolution covers. Nobody has
    /// resolved this pair of sides yet.
    Conflicted { text: String, regions: usize },
    /// Not judgeable line-wise. Carries the reason, written for the agent.
    Unjudged(String),
}

/// Carry one path's committed resolution from the side it was resolved against
/// onto a newer destination. Pure; see [`Carried`] for the arms.
pub fn carry_resolution(
    resolved_against: &FileContent,
    committed: &FileContent,
    destination: &FileContent,
) -> Carried {
    match (resolved_against, destination) {
        (FileContent::Text(before), FileContent::Text(now)) if before == now => {
            return Carried::Unchanged
        }
        (FileContent::Absent, FileContent::Absent) => return Carried::Unchanged,
        _ => {}
    }
    if let FileContent::Absent = destination {
        return Carried::Unjudged(
            "the base deleted this path after your resolution was made, so there is no newer \
             version to carry your resolution onto"
                .to_string(),
        );
    }
    if let FileContent::Absent = committed {
        return Carried::Unjudged(
            "your resolution deletes this path, and the base has changed it since that \
             resolution was made"
                .to_string(),
        );
    }
    let (Some(base), Some(ours), Some(theirs)) = (
        resolved_against.mergeable(),
        committed.mergeable(),
        destination.mergeable(),
    ) else {
        return Carried::Unjudged(
            "one side of this path is not UTF-8 text, and the base has changed it since your \
             resolution was made"
                .to_string(),
        );
    };
    match merge_preview(base, ours, theirs) {
        MergePreview::Clean(text) => Carried::Merged(text),
        MergePreview::Conflicted { text, regions } => Carried::Conflicted { text, regions },
    }
}

/// Carry every named path's resolution from `resolved_against` onto
/// `destination`, reading the three sides from the store.
///
/// A binary path cannot be compared through [`FileContent`], so whether it
/// changed between the two sides is asked of jj directly. Any path that probe
/// mentions at all counts as changed: "unchanged" licenses a whole-file
/// restore, so the doubtful direction is the one that refuses it.
pub fn carry_paths(
    jj: &JjEnv,
    store: &Path,
    resolved_against: &str,
    committed: &str,
    destination: &str,
    paths: &[String],
) -> Vec<(String, Result<Carried, String>)> {
    let sides = read_sides_for_paths(jj, store, resolved_against, committed, destination, paths);
    let needs_probe: Vec<String> = sides
        .iter()
        .filter(|(_, sides)| {
            sides.as_ref().is_ok_and(|sides| {
                matches!(sides.base, FileContent::Binary)
                    || matches!(sides.theirs, FileContent::Binary)
            })
        })
        .map(|(path, _)| path.clone())
        .collect();
    let binary_changed = if needs_probe.is_empty() {
        Ok(BTreeSet::new())
    } else {
        paths_mentioned_in_diff(jj, store, resolved_against, destination, &needs_probe)
    };
    sides
        .into_iter()
        .map(|(path, sides)| {
            let carried = sides.and_then(|sides| {
                if needs_probe.contains(&path) {
                    let changed = binary_changed.as_ref().map_err(Clone::clone)?;
                    if !changed.contains(&path) {
                        return Ok(Carried::Unchanged);
                    }
                }
                Ok(carry_resolution(&sides.base, &sides.ours, &sides.theirs))
            });
            (path, carried)
        })
        .collect()
}

/// Which of `paths` differ in content between two commits, by exact name.
///
/// The per-path evidence that a branch re-resolved a file: a commit that only
/// touched other files leaves every one of these unchanged. A path renamed away
/// is reported under its new name and so reads as unchanged here, which keeps
/// the earlier resolution's claim rather than inventing a new one.
pub fn paths_changed_between(
    jj: &JjEnv,
    store: &Path,
    from: &str,
    to: &str,
    paths: &[String],
) -> Result<BTreeSet<String>, String> {
    if paths.is_empty() || from == to {
        return Ok(BTreeSet::new());
    }
    let mut args = vec![
        "diff",
        "--ignore-working-copy",
        "--name-only",
        "--from",
        from,
        "--to",
        to,
        "--",
    ];
    args.extend(paths.iter().map(String::as_str));
    let listed = jj.run(store, &args, "jj diff --name-only (re-resolved paths)")?;
    let named: BTreeSet<&str> = listed.lines().map(str::trim).collect();
    Ok(paths
        .iter()
        .filter(|path| named.contains(path.as_str()))
        .cloned()
        .collect())
}

/// Which of `paths` a `from`..`to` summary diff mentions, by substring, so a
/// rename's rendering still counts as a mention of both its ends.
fn paths_mentioned_in_diff(
    jj: &JjEnv,
    store: &Path,
    from: &str,
    to: &str,
    paths: &[String],
) -> Result<BTreeSet<String>, String> {
    let mut args = vec![
        "diff",
        "--ignore-working-copy",
        "--summary",
        "--from",
        from,
        "--to",
        to,
        "--",
    ];
    args.extend(paths.iter().map(String::as_str));
    let summary = jj
        .run(store, &args, "jj diff --summary (resolution carry probe)")
        .map_err(|error| format!("could not tell whether the base changed this path: {error}"))?;
    Ok(paths
        .iter()
        .filter(|path| summary.lines().any(|line| line.contains(path.as_str())))
        .cloned()
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_tail_union_keeps_destination_then_branch_and_is_lossless() {
        let base = "        migration!(\"0227_base\"),\n    ]\n";
        let ours =
            "        migration!(\"0227_base\"),\n        migration!(\"0228_ours\"),\n    ]\n";
        let theirs =
            "        migration!(\"0227_base\"),\n        migration!(\"0228_theirs\"),\n    ]\n";
        let candidate = union_completion_candidate(base, ours, theirs);
        assert_eq!(
            candidate,
            "        migration!(\"0227_base\"),\n        migration!(\"0228_theirs\"),\n        migration!(\"0228_ours\"),\n    ]\n",
            "the destination's established tail stays first"
        );
        assert_eq!(
            assess_whole_file_restore(&sides(base, &candidate, theirs)),
            RestoreVerdict::Lossless,
            "committing the union makes whole-file replay lossless"
        );
    }

    #[test]
    fn registry_union_preserves_non_overlapping_changes_outside_the_tail() {
        let base = "header\nentry:a\nfooter\n";
        let ours = "branch-header\nentry:a\nentry:ours\nfooter\n";
        let theirs = "header\nentry:a\nentry:theirs\nfooter\nincoming-footer\n";
        let candidate = union_completion_candidate(base, ours, theirs);
        assert!(candidate.contains("branch-header"), "{candidate}");
        assert!(
            candidate.contains("entry:theirs\nentry:ours"),
            "{candidate}"
        );
        assert!(candidate.contains("incoming-footer"), "{candidate}");
    }

    fn text(value: &str) -> FileContent {
        FileContent::Text(value.to_string())
    }

    #[test]
    fn a_destination_still_at_the_resolved_side_takes_the_resolution_whole() {
        let resolved_against = text("head\nA\n");
        assert_eq!(
            carry_resolution(&resolved_against, &text("head\nA\nX\n"), &resolved_against),
            Carried::Unchanged
        );
        assert_eq!(
            carry_resolution(&FileContent::Absent, &text("new\n"), &FileContent::Absent),
            Carried::Unchanged
        );
    }

    #[test]
    fn a_destination_change_away_from_the_resolution_is_merged_in() {
        // Resolved against A at the tail; the newer base added B at the top.
        let carried = carry_resolution(
            &text("head\nbody\nA\n"),
            &text("head\nbody\nA\nX\n"),
            &text("B\nhead\nbody\nA\n"),
        );
        assert_eq!(
            carried,
            Carried::Merged("B\nhead\nbody\nA\nX\n".to_string())
        );
    }

    #[test]
    fn a_destination_change_in_the_resolved_region_is_a_new_conflict() {
        // Resolved A-then-X at the tail; the newer base appended B at the same spot.
        let carried = carry_resolution(
            &text("head\nA\n"),
            &text("head\nA\nX\n"),
            &text("head\nA\nB\n"),
        );
        let Carried::Conflicted { text, regions } = carried else {
            panic!("same-spot appends cannot both be kept by a line merge: {carried:?}");
        };
        assert_eq!(regions, 1);
        assert!(text.contains("X") && text.contains("B"), "{text}");
    }

    #[test]
    fn a_deletion_or_binary_side_that_changed_is_unjudged() {
        assert!(matches!(
            carry_resolution(&text("a\n"), &text("a\nb\n"), &FileContent::Absent),
            Carried::Unjudged(_)
        ));
        assert!(matches!(
            carry_resolution(&text("a\n"), &FileContent::Absent, &text("a\nc\n")),
            Carried::Unjudged(_)
        ));
        // Binary content carries no bytes to compare, so the pure function can
        // never call it unchanged; `carry_paths` asks the store instead.
        assert!(matches!(
            carry_resolution(
                &FileContent::Binary,
                &FileContent::Binary,
                &FileContent::Binary
            ),
            Carried::Unjudged(_)
        ));
    }

    fn sides(base: &str, ours: &str, theirs: &str) -> MergeSides {
        MergeSides {
            base: FileContent::Text(base.to_string()),
            ours: FileContent::Text(ours.to_string()),
            theirs: FileContent::Text(theirs.to_string()),
        }
    }

    /// The collapse recognizes regions by an exact run of marker characters. If
    /// diffy ever wrote a different length, every conflict region would sail
    /// through as ordinary content and the invariant would call a lossy restore
    /// lossless — the exact failure this whole module exists to prevent.
    #[test]
    fn the_marker_length_this_module_parses_is_the_one_diffy_writes() {
        let merged = diffy::merge("a\n", "x\n", "y\n").unwrap_err();
        assert!(
            merged.contains(&format!("{} ours\n", "<".repeat(MARKER_LEN))),
            "unexpected marker shape: {merged}"
        );
        assert!(merged.contains(&format!("{} original\n", "|".repeat(MARKER_LEN))));
        assert!(merged.contains(&format!("{}\n", "=".repeat(MARKER_LEN))));
        assert!(merged.contains(&format!("{} theirs\n", ">".repeat(MARKER_LEN))));
    }

    /// The common case, and the one that must not produce a false refusal: the
    /// agent resolved by keeping both sides, so their committed file already
    /// contains everything and the whole-file restore is exactly right.
    #[test]
    fn a_union_resolution_is_lossless() {
        let verdict = assess_whole_file_restore(&sides("a\n", "a\nx\ny\n", "a\ny\n"));
        assert_eq!(verdict, RestoreVerdict::Lossless, "union kept both sides");
    }

    /// The CAIRN-3610 shape, and the test that would have caught the defect: a
    /// small conflict at the top, and a large incoming block far below it that
    /// the branch never touched. The whole-file restore keeps the branch's file
    /// and the block simply vanishes.
    #[test]
    fn incoming_work_outside_the_conflict_is_named_as_dropped() {
        let base = "header\nbody\nbody\nbody\n";
        let ours = "header\nours-resolution\nbody\nbody\nbody\n";
        let theirs = "header\ntheirs-line\nbody\nbody\nbody\ntimeout_secs: 30\nretries: 3\n";

        let RestoreVerdict::Lossy(dropped) = assess_whole_file_restore(&sides(base, ours, theirs))
        else {
            panic!("a restore that discards the incoming block must be reported as lossy");
        };
        assert!(
            dropped.candidate.contains("timeout_secs: 30"),
            "the candidate carries the incoming block: {}",
            dropped.candidate
        );
        assert!(
            dropped.candidate.contains("ours-resolution")
                && !dropped.candidate.contains("theirs-line"),
            "and keeps the branch's own resolution inside the conflict: {}",
            dropped.candidate
        );
        assert!(
            !dropped.candidate.contains("<<<<<<<"),
            "the candidate is directly committable, so it carries no markers: {}",
            dropped.candidate
        );
        assert!(
            dropped.diff.contains("+timeout_secs: 30") && dropped.diff.contains("+retries: 3"),
            "the diff names exactly the dropped work: {}",
            dropped.diff
        );
        assert_eq!(dropped.added_lines, 2);
        assert_eq!(dropped.hunks, 1);
    }

    /// Committing the candidate is the documented remedy, so it has to actually
    /// work: the same assessment run against it must come back lossless.
    #[test]
    fn committing_the_candidate_makes_the_same_restore_lossless() {
        let base = "header\nbody\n";
        let ours = "header\nours-resolution\nbody\n";
        let theirs = "header\ntheirs-line\nbody\ntail\n";

        let RestoreVerdict::Lossy(dropped) = assess_whole_file_restore(&sides(base, ours, theirs))
        else {
            panic!("expected a lossy verdict to remedy");
        };
        assert_eq!(
            assess_whole_file_restore(&sides(base, &dropped.candidate, theirs)),
            RestoreVerdict::Lossless,
            "the remedy the resource hands out must satisfy the invariant it is measured by"
        );
    }

    /// A resolution that took ONLY the incoming side is still the agent's
    /// judgment about the conflicting region, and nothing outside it is lost.
    #[test]
    fn taking_only_theirs_inside_the_conflict_is_still_lossless() {
        let verdict = assess_whole_file_restore(&sides("a\n", "theirs-won\n", "theirs-won\n"));
        assert_eq!(verdict, RestoreVerdict::Lossless);
    }

    #[test]
    fn multiple_conflict_regions_all_collapse_to_ours() {
        let base = "1\n2\n3\n4\n5\n";
        let ours = "1\nOURS-A\n3\nOURS-B\n5\n";
        let theirs = "1\nTHEIRS-A\n3\nTHEIRS-B\n5\n";
        let merged = merge_preview(base, ours, theirs);
        assert_eq!(merged.regions(), 2, "two disjoint regions: {merged:?}");
        assert_eq!(collapse_conflicts_to_ours(merged.text()), ours);
    }

    /// A file whose own content contains conflict scaffolding — documentation,
    /// or a test fixture — must not have that scaffolding read as a region
    /// boundary and rewritten.
    #[test]
    fn marker_shaped_content_inside_a_side_is_preserved() {
        let scaffolding = format!("{} example\n", "<".repeat(MARKER_LEN));
        let merged = format!(
            "keep\n{} ours\n{scaffolding}mine\n{} original\nbase\n{}\ntheirs\n{} theirs\ntail\n",
            "<".repeat(MARKER_LEN),
            "|".repeat(MARKER_LEN),
            "=".repeat(MARKER_LEN),
            ">".repeat(MARKER_LEN),
        );
        assert_eq!(
            collapse_conflicts_to_ours(&merged),
            format!("keep\n{scaffolding}mine\ntail\n")
        );
    }

    /// diffy inserts a newline before a marker when the side above it lacks one,
    /// so a conflict at end-of-file would otherwise report a one-byte "dropped
    /// hunk" that is purely an artifact of the preview.
    #[test]
    fn a_conflict_at_end_of_file_without_a_trailing_newline_is_not_a_false_positive() {
        let verdict = assess_whole_file_restore(&sides("a\nb", "a\nours", "a\nours"));
        assert_eq!(verdict, RestoreVerdict::Lossless);
    }

    #[test]
    fn a_region_running_to_end_of_file_collapses_to_ours() {
        let merged = format!(
            "head\n{} ours\nmine\n{} original\nbase\n{}\ntheirs\n{} theirs\n",
            "<".repeat(MARKER_LEN),
            "|".repeat(MARKER_LEN),
            "=".repeat(MARKER_LEN),
            ">".repeat(MARKER_LEN),
        );
        assert_eq!(collapse_conflicts_to_ours(&merged), "head\nmine\n");
    }

    /// A merge with no overlap is a complete, marker-free file — the auto-merge
    /// candidate and the three-way projection are the same artifact.
    #[test]
    fn non_overlapping_edits_merge_clean_and_carry_both_sides() {
        let merged = merge_preview("a\nb\nc\n", "OURS\nb\nc\n", "a\nb\nTHEIRS\n");
        let MergePreview::Clean(text) = &merged else {
            panic!("non-overlapping edits do not conflict: {merged:?}");
        };
        assert_eq!(text, "OURS\nb\nTHEIRS\n");
    }

    /// A branch that has not merged anything yet: the untouched tip is missing
    /// every incoming hunk, and the invariant says so.
    #[test]
    fn an_unresolved_tip_is_lossy() {
        let RestoreVerdict::Lossy(dropped) =
            assess_whole_file_restore(&sides("a\nb\nc\n", "a\nb\nc\n", "a\nb\nc\nincoming\n"))
        else {
            panic!("a tip carrying none of the incoming change cannot be lossless");
        };
        assert!(dropped.diff.contains("+incoming"), "{}", dropped.diff);
    }

    #[test]
    fn an_absent_or_binary_side_is_reported_rather_than_mangled() {
        let deleted_by_them = MergeSides {
            base: FileContent::Text("a\n".into()),
            ours: FileContent::Text("a\nmine\n".into()),
            theirs: FileContent::Absent,
        };
        assert!(matches!(
            assess_whole_file_restore(&deleted_by_them),
            RestoreVerdict::NotAssessed(reason) if reason.contains("deletes this path")
        ));

        let deleted_by_us = MergeSides {
            base: FileContent::Text("a\n".into()),
            ours: FileContent::Absent,
            theirs: FileContent::Text("a\ntheirs\n".into()),
        };
        assert!(matches!(
            assess_whole_file_restore(&deleted_by_us),
            RestoreVerdict::NotAssessed(reason) if reason.contains("your branch deletes")
        ));

        let binary = MergeSides {
            base: FileContent::Binary,
            ours: FileContent::Text("a\n".into()),
            theirs: FileContent::Text("b\n".into()),
        };
        assert!(matches!(
            assess_whole_file_restore(&binary),
            RestoreVerdict::NotAssessed(reason) if reason.contains("not UTF-8")
        ));
    }

    /// The corruption that makes a failed presence probe unsafe to degrade.
    ///
    /// With the real base, ours inserts near the top and theirs appends at the
    /// bottom: the merge is clean, carries both, and the whole-file restore is
    /// LOSSY. Read the base as absent — which is exactly what an empty presence
    /// set means — and both sides look like whole-file additions, the file
    /// becomes one conflict region, collapsing it to ours reproduces ours, and
    /// the same restore certifies as LOSSLESS. A store hiccup would have
    /// manufactured a proof of safety.
    #[test]
    fn an_empty_base_would_falsely_certify_a_lossy_restore() {
        let ours = "a\nMINE\nb\nc\n";
        let theirs = "a\nb\nc\nINCOMING\n";

        let truthful = assess_whole_file_restore(&sides("a\nb\nc\n", ours, theirs));
        let RestoreVerdict::Lossy(dropped) = &truthful else {
            panic!("against the real base this restore drops the appended line: {truthful:?}");
        };
        assert!(dropped.diff.contains("+INCOMING"), "{}", dropped.diff);

        let with_lost_base = assess_whole_file_restore(&MergeSides {
            base: FileContent::Absent,
            ours: FileContent::Text(ours.to_string()),
            theirs: FileContent::Text(theirs.to_string()),
        });
        assert_eq!(
            with_lost_base,
            RestoreVerdict::Lossless,
            "a lost base silently flips the verdict, which is why a failed presence probe must \
             never degrade to an empty set"
        );
    }

    /// The seam that keeps the above from being reachable: a probe that did not
    /// answer is an error, not an empty answer.
    #[test]
    fn a_failed_presence_probe_refuses_rather_than_reporting_an_empty_revision() {
        let ok = || Ok(BTreeSet::from(["a.rs".to_string()]));

        let base_failed = combine_presence(
            Err("could not list files at base: store unavailable".to_string()),
            ok(),
            ok(),
        );
        let error = base_failed.expect_err("a failed base probe cannot yield file state");
        assert!(error.contains("store unavailable"), "{error}");

        // Every failure is named, not just the first.
        let two_failed = combine_presence(
            Err("base probe died".to_string()),
            ok(),
            Err("theirs probe died".to_string()),
        )
        .expect_err("still an error");
        assert!(
            two_failed.contains("base probe died") && two_failed.contains("theirs probe died"),
            "{two_failed}"
        );

        let all_answered = combine_presence(ok(), ok(), ok()).expect("three answers combine");
        assert_eq!(all_answered.0, BTreeSet::from(["a.rs".to_string()]));
    }

    /// A genuinely absent path is still absent — the refusal above must not have
    /// turned every add or delete into an error.
    #[test]
    fn an_empty_but_successful_probe_still_means_absent() {
        let empty: Result<BTreeSet<String>, String> = Ok(BTreeSet::new());
        let combined = combine_presence(empty, Ok(BTreeSet::new()), Ok(BTreeSet::new()))
            .expect("an empty answer is an answer");
        assert!(combined.0.is_empty());
    }

    /// A path both sides added: there is no base, so the whole file overlaps and
    /// the branch's committed content is by definition its own resolution.
    #[test]
    fn a_path_added_on_both_sides_merges_against_an_empty_base() {
        let both_added = MergeSides {
            base: FileContent::Absent,
            ours: FileContent::Text("mine\nshared\n".into()),
            theirs: FileContent::Text("theirs\nshared\n".into()),
        };
        assert_eq!(
            assess_whole_file_restore(&both_added),
            RestoreVerdict::Lossless
        );
    }
}
