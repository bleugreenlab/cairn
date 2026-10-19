//! Verify-then-rewrite of one execution's events into their durable archived
//! form — the writer counterpart to [`crate::storage::events::reconstruct`].
//!
//! [`archive_execution`] is the single writer. Issue teardown calls it with the
//! execution's live branch range before the branch is deleted; the recurring
//! sweep ([`super::sweep`]) calls it with no range for leftovers. Every event
//! still recorded `full` becomes one of these forms:
//!
//! - **gitcoord** (range present only): content addressed by a git coordinate
//!   (`content_commit`) instead of stored verbatim, in two shapes distinguished
//!   by `data_blob`:
//!   - **read** (`data_blob` NULL) — a read whose every target is a single
//!     in-repo tracked file. The heavy rendered `toolResult` is dropped — along
//!     with Claude's verbatim `raw.tool_use_result` duplicate of it — and `data`
//!     keeps a stub pinning `tool_input.paths` (the reconstruction contract).
//!     Rewritten only after a **render-and-compare** against the stored
//!     agent-seen bytes succeeds, so a coordinate read can never silently diverge
//!     from what the agent actually saw, and `content_render_sha` records that
//!     agreement. A mixed batch archives as a **hybrid read**: its reproducible
//!     `file:` sections are git-addressed and the rest stays verbatim in a
//!     placeholder skeleton.
//!   - **write** (`data_blob` present) — the *assistant* event of a write batch
//!     that committed. The heavy `toolUses[].input.changes[*].payload` bytes
//!     are stripped; the remainder (model text, plus the change skeletons keyed
//!     by target/mode) is zstd-compressed into `data_blob` while
//!     `content_commit` pins the batch commit and `data` holds a label stub.
//!     Reconstruction regenerates the per-change payload *view* from the commit,
//!     so presentation-equivalence (not byte identity) is the contract and
//!     `content_render_sha` does not apply. The paired short write `tool_result`
//!     IS the change summary, so it is a plain zstd row that round-trips
//!     byte-for-byte.
//! - **blobbed**: assembled `system:prompt` and near-constant `system:init`
//!   events, independent of git. Their constant parts are content-hashed into
//!   `archival_blobs` once and shared across executions; the per-run remainder
//!   is inlined; a byte-exact verify gates each one. Blob rows are deliberately
//!   not foreign-keyed to executions, so deleting one execution never cascades
//!   a blob another still references; collecting unreferenced blobs is not
//!   implemented.
//! - **zstd**: everything else (run output, directory/glob reads, web/PDF/cairn
//!   resource reads, out-of-repo paths, model text, thinking, artifacts), any
//!   read that fails the compare, and every git-shaped row when no range is
//!   given. The original `data` is compressed verbatim.
//!
//! # Store-native coordinates
//!
//! No checkout is consulted. Objects resolve from the project repository's git
//! object database — the same `.git` that backs the project's jj store, so every
//! sealed commit is already there — and coordinates are proven, not mapped:
//!
//! - A **range** (`base`, `tip`, `target_head`) is one branch's live fork
//!   point, head, and integration-target head, resolved by the caller through
//!   [`crate::diff::live_branch_range`]. An execution spans one branch per node
//!   (a planner's and a builder's, say), so it is archived with the range of
//!   every branch that still resolves.
//! - The **pack** holds every object reachable from any range's `tip` but not
//!   from the project default branch's head (`durable_head`), so every branch's
//!   history survives branch deletion and `git gc`. Only the default branch is
//!   excluded: a child issue's integration target is its parent's agent branch,
//!   which is rebased and deleted in turn, so the child's pack runs down through
//!   the parent's commits to the default branch. Empty ranges (true merges,
//!   commit-less branches) store a NULL pack. `execution_history` keeps one base/tip pair for the
//!   node-diff fallback: the latest range with commits of its own.
//! - **Recorded commits are packed too.** A branch is rebased whenever its base
//!   advances, so by teardown most commits a transcript recorded are no longer
//!   on it: still in the store, but unreachable once the branch is deleted and
//!   collected. Every commit a write or run reported that still resolves is
//!   therefore an extra pack tip. A write's coordinate stays the exact commit it
//!   landed (reconstruction diffs it against its own parent), and a read's
//!   replayed state stays the exact tree the agent read. A write whose commit
//!   the store no longer knows stays zstd. Every coordinate is thus a branch
//!   tip, a recorded commit, a fork point, or an ancestor of one, and all of
//!   those outlive the branch: in the pack, or on the integration branch.
//! - **Reads** stamp no commit, so each read tries its own job's branch first —
//!   the replayed tracker commit (advanced by each committed write or run on
//!   that branch), then `base`, then `tip` — and then the other branches the
//!   same way. The first byte-exact match wins: any surviving commit that
//!   reproduces the agent-seen bytes is a valid coordinate.
//!
//! An execution that already has an `execution_history` row (archived at an
//! earlier teardown, then reopened) is archived without a range: replacing its
//! pack could orphan the coordinates the first archival wrote.
//!
//! # Applying
//!
//! Blobs and the `execution_history` row commit first, then the event UPDATEs
//! in bounded chunks, so a gitcoord row can never reference an absent pack and
//! no single transaction grows with the transcript. Every UPDATE only touches a
//! row still `full`, so a teardown and a sweep racing on one execution can
//! never archive a row twice. A team replica offloads its pack and blobs to the
//! team content store (put before commit) and keeps only hash pointers.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::models::Event;
use crate::runs::queries::{event_from_row, EVENT_COLUMNS};
use crate::storage::content_store::ContentStore;
use crate::storage::events::encoding::{
    init_placeholder, ArchivedShape, ARCHIVED_SYSTEM_INIT, ARCHIVED_SYSTEM_PROMPT, INIT_TOOLS_TAG,
};
use crate::storage::events::reconstruct;
use crate::storage::{
    build_execution_pack, compress, decompress, DbResult, LocalDb, ObjectStore, RowExt,
    CODEC_ZSTD_V1,
};

mod apply;
mod coordinates;
mod event_shape;
mod read;
mod system_blob;
mod write;

use apply::*;
use coordinates::*;
use event_shape::*;
use read::*;
use system_blob::{build_system_init_shape, build_system_prompt_shape};
use write::*;

#[cfg(test)]
mod tests_classify;
#[cfg(test)]
mod tests_durability;
#[cfg(test)]
mod tests_offload;
#[cfg(test)]
mod tests_roundtrip;
#[cfg(test)]
mod tests_system_blob;
#[cfg(test)]
mod testsupport;

/// One content-addressed blob to insert into `archival_blobs`: its content hash
/// and the zstd-compressed bytes.
pub(crate) type SegmentBlob = (String, Vec<u8>);

/// A hash-addressed [`ArchivedShape::Blobbed`] plus the blobs it references
/// (a system prompt's static segments, or a system init's skeleton + tool set).
pub(crate) type BlobbedShape = (ArchivedShape, Vec<SegmentBlob>);

/// Which hash-addressed system-event consumer a blobbed rewrite serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SystemBlobKind {
    Prompt,
    Init,
}

/// A builder for one of the two hash-addressed system-event shapes.
type SystemBlobBuilder = fn(&Event) -> Result<Option<BlobbedShape>, String>;

/// The live range one of an execution's branches occupies, and the repository
/// its objects resolve from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveRange {
    /// The branch, matched against `jobs.branch` to find which range an event's
    /// run worked on.
    pub branch: String,
    /// The project repository. It is the git backend of the project's jj store,
    /// so every sealed commit is in its object database, and reconstruction
    /// later builds its [`ObjectStore`] over the same path.
    pub repo_path: PathBuf,
    /// The branch's fork point from its integration target.
    pub base: String,
    /// The branch head.
    pub tip: String,
    /// The integration target's head the fork point was taken against.
    pub target_head: String,
    /// The project default branch's head: the one ref guaranteed to outlive
    /// the branch, and so the only one the pack excludes. It equals
    /// `target_head` unless the integration target is another agent branch.
    pub durable_head: String,
}

impl ArchiveRange {
    pub fn from_branch_range(
        repo_path: PathBuf,
        branch: String,
        range: crate::diff::BranchRange,
        durable_head: String,
    ) -> Self {
        Self {
            branch,
            repo_path,
            base: range.base,
            tip: range.tip,
            target_head: range.target_head,
            durable_head,
        }
    }
}

/// One execution to archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchiveTarget {
    pub execution_id: String,
    /// The live range of each of the execution's branches, oldest job first,
    /// all in one repository. Empty archives zstd and blobbed shapes only and
    /// writes no `execution_history`.
    pub ranges: Vec<ArchiveRange>,
}

/// Per-execution archival outcome. `mismatch_fallback` counts reads that
/// resolved bytes at a durable commit but failed the render-and-compare at every
/// candidate: the signature of replay drift, an uncommitted edit, or a
/// divergence between the live and archived read composition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ArchiveSummary {
    pub gitcoord_read: usize,
    pub gitcoord_write: usize,
    /// Mixed-target reads archived per-section: the reproducible `file:` sections
    /// git-addressed, the rest stored verbatim in a placeholder skeleton.
    pub hybrid_read: usize,
    pub zstd: usize,
    /// Assembled `system:prompt` events rewritten to content-addressed segments.
    pub system_prompt: usize,
    /// Near-constant `system:init` events rewritten to a content-addressed
    /// skeleton plus a deduped tool-set and the inlined varying fields.
    pub system_init: usize,
    pub mismatch_fallback: usize,
    /// Committed writes whose recorded commit the repository no longer knows
    /// (collected before archival, or sealed somewhere never exported), and so
    /// fell to zstd.
    pub unresolved_write: usize,
    pub bytes_before: usize,
    pub bytes_after: usize,
}

impl ArchiveSummary {
    /// Rows this pass rewrote.
    pub fn rows(&self) -> usize {
        self.gitcoord_read
            + self.gitcoord_write
            + self.hybrid_read
            + self.zstd
            + self.system_prompt
            + self.system_init
    }

    /// Fold another execution's outcome into a running total.
    pub fn absorb(&mut self, other: &ArchiveSummary) {
        self.gitcoord_read += other.gitcoord_read;
        self.gitcoord_write += other.gitcoord_write;
        self.hybrid_read += other.hybrid_read;
        self.zstd += other.zstd;
        self.system_prompt += other.system_prompt;
        self.system_init += other.system_init;
        self.mismatch_fallback += other.mismatch_fallback;
        self.unresolved_write += other.unresolved_write;
        self.bytes_before += other.bytes_before;
        self.bytes_after += other.bytes_after;
    }

    /// Reads resolved bytes but none reproduced at any durable commit. The
    /// expected signature of a live/archived read-composition divergence, which
    /// must never hide behind a green run.
    pub fn all_reads_mismatched(&self) -> bool {
        self.mismatch_fallback > 0 && self.gitcoord_read == 0 && self.hybrid_read == 0
    }
}

impl fmt::Display for ArchiveSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} gitcoord-read, {} hybrid-read, {} gitcoord-write, {} system-prompt, \
             {} system-init, {} zstd, {} mismatch-fallback, {} unresolved-write, \
             {} -> {} bytes",
            self.gitcoord_read,
            self.hybrid_read,
            self.gitcoord_write,
            self.system_prompt,
            self.system_init,
            self.zstd,
            self.mismatch_fallback,
            self.unresolved_write,
            self.bytes_before,
            self.bytes_after,
        )
    }
}

/// Archive one execution's still-`full` events.
///
/// With a range, the writer packs the branch's objects, writes
/// `execution_history`, and attempts git coordinates; without one it writes only
/// zstd and blobbed shapes. On error nothing past the last committed chunk is
/// written and the remaining rows stay `full` for a later pass to retry.
pub async fn archive_execution(
    db: &LocalDb,
    target: ArchiveTarget,
) -> Result<ArchiveSummary, String> {
    let Some(loaded) = load(db, &target.execution_id)
        .await
        .map_err(|e| format!("loading execution events for archival: {e}"))?
    else {
        return Ok(ArchiveSummary::default());
    };
    if !loaded.events.iter().any(is_full) {
        return Ok(ArchiveSummary::default());
    }
    let ranges = if loaded.has_history && !target.ranges.is_empty() {
        log::info!(
            "archival: execution {} already has archived history; archiving its remaining rows without a range",
            loaded.execution_id
        );
        Vec::new()
    } else {
        target.ranges
    };
    if let Some(first) = ranges.first() {
        if ranges
            .iter()
            .any(|range| range.repo_path != first.repo_path)
        {
            return Err(format!(
                "archival: execution {} has branch ranges in more than one repository",
                loaded.execution_id
            ));
        }
    }

    // Classification shells out to git, builds the execution pack, walks git
    // objects, and compresses fallback payloads. Keep that synchronous unit off
    // the async runtime; the non-Send `ObjectStore` is created and dropped
    // entirely inside the blocking worker before the apply await.
    let Classified {
        updates,
        summary,
        history,
        blobs,
    } = tokio::task::spawn_blocking(move || classify(&loaded, &ranges))
        .await
        .map_err(|error| format!("archival classification task failed: {error}"))??;

    if updates.is_empty() && history.is_none() && blobs.is_empty() {
        return Ok(summary);
    }
    // A team replica offloads its heavy pack and system blobs to the shared team
    // store and writes only pointers into the synced replica. The private
    // database keeps them inline.
    if db.team_id().is_some() {
        apply_offloaded(db, db.content_store().as_ref(), updates, history, blobs).await?;
    } else {
        apply(db, updates, history, blobs).await?;
    }
    Ok(summary)
}

fn is_full(event: &Event) -> bool {
    event.storage_mode.as_deref().unwrap_or("full") == "full"
}

// ---------------------------------------------------------------------------
// Phase A: gather everything via async DB reads (no ObjectStore alive here).
// ---------------------------------------------------------------------------

struct Loaded {
    execution_id: String,
    project_id: String,
    repository_id: String,
    /// An earlier archival already wrote this execution's pack.
    has_history: bool,
    /// Each run's job branch, for runs whose job has one.
    run_branches: HashMap<String, String>,
    /// Every event across all the execution's runs (including sub-agent task
    /// runs) in global chronological order, so interleaved task commits replay
    /// in the order they actually landed on the shared branch.
    events: Vec<Event>,
}

async fn load(db: &LocalDb, execution_id: &str) -> DbResult<Option<Loaded>> {
    let execution_id = execution_id.to_string();
    db.read(move |conn| {
        let execution_id = execution_id.clone();
        Box::pin(async move {
            let (project_id, repository_id) = {
                let mut rows = conn
                    .query(
                        "SELECT p.id, COALESCE(p.repository_id, p.id)
                         FROM jobs j JOIN projects p ON j.project_id = p.id
                         WHERE j.execution_id = ?1 LIMIT 1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                (row.text(0)?, row.text(1)?)
            };

            let has_history = conn
                .query(
                    "SELECT 1 FROM execution_history WHERE execution_id = ?1 LIMIT 1",
                    (execution_id.as_str(),),
                )
                .await?
                .next()
                .await?
                .is_some();

            let mut run_branches = HashMap::new();
            let mut rows = conn
                .query(
                    "SELECT r.id, j.branch FROM runs r JOIN jobs j ON r.job_id = j.id
                     WHERE j.execution_id = ?1 AND j.branch IS NOT NULL AND j.branch != ''",
                    (execution_id.as_str(),),
                )
                .await?;
            while let Some(row) = rows.next().await? {
                run_branches.insert(row.text(0)?, row.text(1)?);
            }

            // Filter by a run subquery rather than joining, so the unqualified
            // EVENT_COLUMNS `id` stays unambiguous against runs.id/jobs.id.
            let sql = format!(
                "SELECT {EVENT_COLUMNS} FROM events
                 WHERE run_id IN (
                     SELECT r.id FROM runs r JOIN jobs j ON r.job_id = j.id
                     WHERE j.execution_id = ?1
                 )
                 ORDER BY created_at ASC, sequence ASC"
            );
            let mut rows = conn.query(&sql, (execution_id.as_str(),)).await?;
            let mut events = Vec::new();
            while let Some(row) = rows.next().await? {
                events.push(event_from_row(&row)?);
            }

            Ok(Some(Loaded {
                execution_id,
                project_id,
                repository_id,
                has_history,
                run_branches,
                events,
            }))
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// Phase B: synchronous classification (builds + drops the ObjectStore here).
// ---------------------------------------------------------------------------

/// One event's rewrite: the row `id` and the [`ArchivedShape`] it becomes. The
/// shape's [`ArchivedShape::encode`] yields the column values `apply` binds, so
/// the writer can never set a combination the reader's `decode` would reject.
#[derive(Clone)]
struct EventUpdate {
    id: String,
    shape: ArchivedShape,
}

#[derive(Clone)]
struct ExecHistory {
    execution_id: String,
    project_id: String,
    repository_id: String,
    base_sha: String,
    tip_sha: String,
    pack: Option<(Vec<u8>, Vec<u8>)>,
}

struct Classified {
    updates: Vec<EventUpdate>,
    summary: ArchiveSummary,
    history: Option<ExecHistory>,
    /// Content-addressed segment blobs to insert (hash, zstd bytes). Shared across
    /// executions via `INSERT OR IGNORE`; duplicates are harmless.
    blobs: Vec<SegmentBlob>,
}

/// The git half of classification, present only when ranges were given.
struct GitContext<'a> {
    store: ObjectStore,
    ranges: &'a [ArchiveRange],
    run_branches: &'a HashMap<String, String>,
    /// Per range, the replay tracker: the commit that branch most plausibly sat
    /// at when a read ran. It starts at the fork point and advances on each
    /// committed write or run on the branch.
    current: Vec<String>,
}

impl GitContext<'_> {
    /// The range a run worked on: its job's branch, or the only range when the
    /// job's branch is unknown.
    fn range_of(&self, run_id: &str) -> Option<usize> {
        let own = self
            .run_branches
            .get(run_id)
            .and_then(|branch| self.ranges.iter().position(|range| &range.branch == branch));
        own.or_else(|| (self.ranges.len() == 1).then_some(0))
    }

    /// Advance the replay tracker of the branch `run_id` worked on.
    fn advance(&mut self, run_id: &str, commit: String) {
        if let Some(index) = self.range_of(run_id) {
            self.current[index] = commit;
        }
    }

    /// The commits a read is tried at, in order: on the run's own branch the
    /// replayed tracker, the fork point, and the head, then the same for every
    /// other branch — de-duplicated. Every candidate outlives the branch by
    /// construction (a recorded commit is packed). Extra candidates cost only a
    /// render: the byte compare decides.
    fn read_candidates(&self, run_id: &str) -> Vec<String> {
        let own = self.range_of(run_id);
        let order = own
            .into_iter()
            .chain((0..self.ranges.len()).filter(|index| Some(*index) != own));
        let mut candidates: Vec<String> = Vec::new();
        for index in order {
            let range = &self.ranges[index];
            for commit in [&self.current[index], &range.base, &range.tip] {
                if !candidates.contains(commit) {
                    candidates.push(commit.clone());
                }
            }
        }
        candidates
    }
}

/// The range `execution_history` records for the node-diff fallback, which
/// keeps one base/tip pair per execution: the latest branch with commits of its
/// own, else the latest branch.
fn primary_range(ranges: &[ArchiveRange]) -> &ArchiveRange {
    ranges
        .iter()
        .rev()
        .find(|range| range.base != range.tip)
        .unwrap_or(&ranges[ranges.len() - 1])
}

fn classify(loaded: &Loaded, ranges: &[ArchiveRange]) -> Result<Classified, String> {
    let tool_map = build_tool_map(&loaded.events);
    let mut history: Option<ExecHistory> = None;
    let mut git: Option<GitContext<'_>> = None;
    let mut summary = ArchiveSummary::default();

    // Pre-pass: resolve every commit a write or run recorded. A write's commit
    // sha lives on the *later* `tool_result`, but the heavy payload it
    // coordinates lives on the *earlier* assistant event, so the strip decision
    // needs the pairing up front; and the pack must hold every recorded commit.
    let (write_commits, run_commits) = match ranges.first() {
        Some(first) => {
            let mut commits = CommitResolver::new(&first.repo_path);
            let (writes, unresolved) = build_write_commits(&mut commits, &loaded.events, &tool_map);
            summary.unresolved_write = unresolved;
            let runs = build_run_commits(&mut commits, &loaded.events, &tool_map);
            (writes, runs)
        }
        None => (HashMap::new(), HashMap::new()),
    };

    if let Some(first) = ranges.first() {
        let repo_path = &first.repo_path;
        let mut tips: Vec<&str> = ranges.iter().map(|range| range.tip.as_str()).collect();
        tips.extend(write_commits.values().map(|write| write.commit.as_str()));
        tips.extend(run_commits.values().map(String::as_str));
        tips.sort_unstable();
        tips.dedup();
        let mut exclude: Vec<&str> = ranges
            .iter()
            .map(|range| range.durable_head.as_str())
            .collect();
        exclude.sort_unstable();
        exclude.dedup();
        let pack = build_execution_pack(repo_path, &tips, &exclude)?;
        let store = ObjectStore::new(repo_path, pack.clone())
            .map_err(|e| format!("building archival object store: {e}"))?;
        let primary = primary_range(ranges);
        history = Some(ExecHistory {
            execution_id: loaded.execution_id.clone(),
            project_id: loaded.project_id.clone(),
            repository_id: loaded.repository_id.clone(),
            base_sha: primary.base.clone(),
            tip_sha: primary.tip.clone(),
            pack,
        });
        git = Some(GitContext {
            store,
            ranges,
            run_branches: &loaded.run_branches,
            current: ranges.iter().map(|range| range.base.clone()).collect(),
        });
    }

    let mut updates: Vec<EventUpdate> = Vec::new();
    let mut blobs: Vec<SegmentBlob> = Vec::new();
    // Per write commit, the repo paths its diff touches (`None`: unresolvable).
    let mut diff_paths: HashMap<String, Option<HashSet<String>>> = HashMap::new();

    for event in &loaded.events {
        // Idempotency: never re-archive an already-rewritten row (a stub would be
        // re-read as if it were original content).
        if !is_full(event) {
            continue;
        }
        summary.bytes_before += event.data.len();

        // Assembled system prompts and near-constant system inits archive to
        // content-addressed blobs independent of git, so handle them before any
        // tool classification and regardless of the range.
        let system_blob: Option<(SystemBlobKind, SystemBlobBuilder)> =
            match event.event_type.as_str() {
                "system:prompt" => Some((SystemBlobKind::Prompt, build_system_prompt_shape)),
                "system:init" => Some((SystemBlobKind::Init, build_system_init_shape)),
                _ => None,
            };
        if let Some((kind, build)) = system_blob {
            match build(event)? {
                Some((shape, shape_blobs)) => {
                    if let ArchivedShape::Blobbed { ref data, .. } = shape {
                        summary.bytes_after += data.len();
                    }
                    match kind {
                        SystemBlobKind::Prompt => summary.system_prompt += 1,
                        SystemBlobKind::Init => summary.system_init += 1,
                    }
                    blobs.extend(shape_blobs);
                    updates.push(EventUpdate {
                        id: event.id.clone(),
                        shape,
                    });
                }
                None => push_zstd(event, None, &mut updates, &mut summary)?,
            }
            continue;
        }

        let tool = event_tool_use_id(&event.data).and_then(|id| tool_map.get(&id));
        let tool_name = tool.map(|(name, _)| normalize_tool_name(name));
        let is_result = matches!(event.event_type.as_str(), "tool_result" | "result");

        if !is_result {
            // An assistant event whose write batch committed to a commit the
            // store still knows is gitcoord-stripped; everything else (model
            // text, user, thinking, an uncommitted write's assistant event) is
            // plain zstd.
            try_assistant_write(
                event,
                tool_name,
                &write_commits,
                git.as_ref().map(|git| &git.store),
                &mut diff_paths,
                &mut updates,
                &mut summary,
            )?;
            continue;
        }

        match (tool_name, git.as_mut()) {
            (Some("read"), Some(git)) => {
                let candidates = git.read_candidates(&event.run_id);
                try_read(
                    event,
                    tool,
                    &candidates,
                    &git.store,
                    &mut updates,
                    &mut summary,
                )?;
            }
            (Some("write"), git) => {
                // The short write result IS the change summary: nothing heavy to
                // strip, and reconstruction returns it verbatim. A committed batch
                // advances its branch's replay tracker.
                if let (Some(commit), Some(git)) = (
                    event_tool_use_id(&event.data).and_then(|id| write_commits.get(&id)),
                    git,
                ) {
                    git.advance(&event.run_id, commit.commit.clone());
                }
                push_zstd(event, tool_name, &mut updates, &mut summary)?;
            }
            (Some("run"), Some(git)) => {
                // Run output is never reconstructable, but a run that committed
                // (commit-barrier outcome) advances the tracker.
                if let Some(full) = run_commits.get(&event.id) {
                    git.advance(&event.run_id, full.clone());
                }
                push_zstd(event, tool_name, &mut updates, &mut summary)?;
            }
            _ => push_zstd(event, tool_name, &mut updates, &mut summary)?,
        }
    }

    // Drop the non-Send store before returning to the async caller.
    drop(git);
    Ok(Classified {
        updates,
        summary,
        history,
        blobs,
    })
}
