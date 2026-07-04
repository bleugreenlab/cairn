//! Verify-then-rewrite of an execution's events to git coordinates at worktree
//! teardown — the writer counterpart to [`crate::storage::events::reconstruct`].
//!
//! At teardown the worktree and its branch are about to disappear, so every
//! event recorded `full` on the hot path is rewritten into one of two durable
//! forms before the objects it references can be garbage-collected:
//!
//! - **gitcoord**: content addressed by a git coordinate (`content_commit`)
//!   instead of stored verbatim, in two shapes distinguished by `data_blob`:
//!   - **read** (`data_blob` NULL) — a read whose every target is a single
//!     in-repo tracked file. The heavy rendered `toolResult` is dropped — along
//!     with Claude's verbatim `raw.tool_use_result` duplicate of it — and `data`
//!     keeps a stub pinning `tool_input.paths` (the reconstruction contract).
//!     Rewritten only after a **render-and-compare** against the stored
//!     agent-seen bytes succeeds, so a coordinate read can never silently diverge
//!     from what the agent actually saw, and `content_render_sha` records that
//!     agreement.
//!   - **write** (`data_blob` present) — the *assistant* event of a write batch
//!     that committed. The heavy `toolUses[].input.changes[*].payload` bytes
//!     (old_string/new_string/diff/content) are stripped; the remainder of the
//!     event (model text, plus the change skeletons keyed by target/mode) is
//!     zstd-compressed into `data_blob` while `content_commit` pins the batch
//!     commit and `data` holds a label stub. (Presence of `data_blob` on a
//!     gitcoord row is what distinguishes a write from a read; the full
//!     column-level encode/decode contract these shapes serialize to lives in
//!     [`crate::storage::events::encoding`], shared with
//!     [`crate::storage::events::reconstruct`].)
//!     The paired short write `tool_result` carries no heavy bytes — it IS the
//!     change summary — so it is stored as a plain **zstd** row that round-trips
//!     byte-for-byte and is never replaced by a regenerated diff.
//!     [`crate::storage::events::reconstruct`] regenerates the per-change payload
//!     *view* from the commit ([`crate::storage::events::diff`]) so the
//!     transcript shows the committed change
//!     where the payload was. Payload byte-identity is impossible (old_string
//!     anchors aren't recoverable from a commit), so presentation-equivalence is
//!     the accepted contract and `content_render_sha` does not apply to writes.
//! - **zstd**: everything else (run output, directory/glob reads, web/PDF/cairn
//!   resource reads, out-of-repo paths, model text, thinking, artifacts) and any
//!   read that fails the compare (replay drift, a dirty worktree, or cairn-cmd's
//!   over-budget composition seam). The original `data` is compressed verbatim.
//!
//! The whole pass is one transaction per execution: the `execution_history`
//! insert and every event UPDATE commit together, or nothing does and the rows
//! stay `full`.
//!
//! Assembled `system:prompt` events are a third path, independent of git: their
//! static segments (recorded as a boundary map at assembly time) are
//! content-hashed and stored once in `archival_blobs`, the per-run dynamic tail
//! is inlined, and the full prompt is discarded only after a byte-exact verify
//! (any mismatch keeps the whole event as zstd). Blob rows are shared across
//! executions and are deliberately not foreign-keyed to executions, so deleting
//! one execution never cascades a blob another still references. Garbage
//! collecting unreferenced blobs is out of scope and not yet implemented.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::jj::JjEnv;
use crate::models::Event;
use crate::runs::queries::{event_from_row, EVENT_COLUMNS};
use crate::storage::content_store::{content_hash, frame_pack, ContentStore};
use crate::storage::events::encoding::{
    init_placeholder, ArchivedShape, ARCHIVED_SYSTEM_INIT, ARCHIVED_SYSTEM_PROMPT, INIT_TOOLS_TAG,
};
use crate::storage::events::reconstruct;
use crate::storage::{
    build_execution_pack, compress, decompress, DbResult, LocalDb, ObjectStore, RowExt,
    CODEC_ZSTD_V1,
};

mod apply;
mod event_shape;
mod read;
mod system_blob;
mod write;

#[cfg(test)]
mod tests_classify;
#[cfg(test)]
mod tests_jj;
#[cfg(test)]
mod tests_offload;
#[cfg(test)]
mod tests_roundtrip;
#[cfg(test)]
mod tests_system_blob;
#[cfg(test)]
mod testsupport;

use apply::*;
use event_shape::*;
use read::*;
use write::*;

pub(crate) use event_shape::{build_tool_map, event_tool_use_id, normalize_tool_name, zstd_stub};
#[cfg(test)]
pub(crate) use system_blob::classify_system_blob_columns_for_test;
pub(crate) use system_blob::{build_system_init_shape, build_system_prompt_shape};

/// One content-addressed blob to insert into `archival_blobs`: its content hash
/// and the zstd-compressed bytes.
pub(crate) type SegmentBlob = (String, Vec<u8>);

/// A hash-addressed [`ArchivedShape::Blobbed`] plus the blobs it references
/// (a system prompt's static segments, or a system init's skeleton + tool set).
pub(crate) type BlobbedShape = (ArchivedShape, Vec<SegmentBlob>);

/// Which hash-addressed system-event consumer a blobbed rewrite serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SystemBlobKind {
    Prompt,
    Init,
}

/// Minimal writer-specific receiver for the shared "blobbed else zstd" branch.
///
/// Teardown archival and historical backfill use different update and summary
/// structs, but their two hash-addressed system-event paths must make the same
/// choice and store the same shape. This sink keeps the common branch here while
/// leaving each writer to account for its own counters.
pub(crate) trait SystemBlobSink {
    fn push_blobbed(
        &mut self,
        event: &Event,
        kind: SystemBlobKind,
        shape: ArchivedShape,
        blobs: Vec<SegmentBlob>,
    );

    fn push_zstd(&mut self, event: &Event) -> Result<(), String>;
}

/// Try the hash-addressed system-event shape and fall back to whole-row zstd.
pub(crate) fn push_blobbed_or_zstd<S, F>(
    event: &Event,
    kind: SystemBlobKind,
    build: F,
    sink: &mut S,
) -> Result<(), String>
where
    S: SystemBlobSink,
    F: FnOnce(&Event) -> Result<Option<BlobbedShape>, String>,
{
    match build(event)? {
        Some((shape, blobs)) => sink.push_blobbed(event, kind, shape, blobs),
        None => sink.push_zstd(event)?,
    }
    Ok(())
}

/// Per-execution archival outcome: the measurement that later decides whether
/// extracting cairn-cmd's read-composition algorithm into cairn-common is worth
/// it. `mismatch_fallback` counts reads that resolved bytes but failed the
/// render-and-compare (replay drift, a dirty worktree, the composition seam).
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
    pub bytes_before: usize,
    pub bytes_after: usize,
}

/// Archive one teardown target's execution: pack its at-risk commits and rewrite
/// its events to git coordinates (+ zstd backstop), verifying every read before
/// discarding its bytes. `worktree_path` and `repo_path` are the still-present
/// worktree and the canonical project repo; `job_ids` are every job referencing
/// the worktree (inheritance fan-out), which all share one execution.
///
/// Returns a per-execution [`ArchiveSummary`]. On any error nothing is written —
/// the caller logs and proceeds, leaving the rows `full`.
pub async fn archive_target(
    db: &LocalDb,
    worktree_path: &str,
    repo_path: &str,
    job_ids: &[String],
    jj: Option<&JjEnv>,
) -> Result<ArchiveSummary, String> {
    let Some(loaded) = load(db, worktree_path, job_ids)
        .await
        .map_err(|e| format!("loading execution events for archival: {e}"))?
    else {
        return Ok(ArchiveSummary::default());
    };
    if loaded.events.is_empty() {
        return Ok(ArchiveSummary::default());
    }

    // Classify synchronously: the non-Send `ObjectStore` lives only inside this
    // call and is dropped before the apply await.
    let Classified {
        updates,
        summary,
        history,
        blobs,
    } = classify(worktree_path, repo_path, &loaded, jj)?;

    if updates.is_empty() && history.is_none() && blobs.is_empty() {
        return Ok(summary);
    }
    // A team replica carries a content store: offload the heavy pack and system
    // blobs to the shared per-team store by hash, and write only pointers into
    // the synced replica. The private DB has no store, so a local run keeps the
    // inline path byte-for-byte. Detection is on the resolved handle itself, not
    // a separate team lookup.
    match db.content_store() {
        Some(store) => apply_offloaded(db, store.as_ref(), updates, history, blobs).await?,
        None => apply(db, updates, history, blobs).await?,
    }
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Phase A: gather everything via async DB reads (no ObjectStore alive here).
// ---------------------------------------------------------------------------

struct Loaded {
    execution_id: String,
    /// Replay anchor: the worktree HEAD when the root job was created. `None`
    /// makes the whole execution zstd-only.
    base_commit: Option<String>,
    /// Durable pack anchor (reachable from the default branch). `None` makes the
    /// whole execution zstd-only — 1540's contract never guesses an anchor.
    pack_anchor: Option<String>,
    default_branch: String,
    base_branch: Option<String>,
    /// Every event across all the execution's runs (including sub-agent task
    /// runs) in global chronological order, so interleaved task commits replay
    /// in the order they actually landed on the shared branch.
    events: Vec<Event>,
}

async fn load(db: &LocalDb, worktree_path: &str, job_ids: &[String]) -> DbResult<Option<Loaded>> {
    let worktree_path = worktree_path.to_string();
    let job_ids = job_ids.to_vec();
    db.read(move |conn| {
        let worktree_path = worktree_path.clone();
        let job_ids = job_ids.clone();
        Box::pin(async move {
            // Distinct executions referenced by the worktree's jobs. The fan-out
            // (builder/create-pr/review/tasks) shares exactly one; warn otherwise.
            let mut exec_ids: Vec<String> = Vec::new();
            for job_id in &job_ids {
                let mut rows = conn
                    .query("SELECT execution_id FROM jobs WHERE id = ?1", (job_id.as_str(),))
                    .await?;
                if let Some(row) = rows.next().await? {
                    if let Some(exec) = row.opt_text(0)? {
                        if !exec_ids.contains(&exec) {
                            exec_ids.push(exec);
                        }
                    }
                }
            }
            let Some(execution_id) = exec_ids.first().cloned() else {
                return Ok(None);
            };
            if exec_ids.len() != 1 {
                log::warn!(
                    "archival: worktree {worktree_path} spans {} executions; archiving {execution_id}",
                    exec_ids.len()
                );
            }

            // Replay + pack anchors from the root job for this worktree (earliest
            // created; the inheriting children and tasks come later).
            let (base_commit, pack_anchor, base_branch) = {
                let mut rows = conn
                    .query(
                        "SELECT base_commit, pack_anchor, base_branch FROM jobs
                         WHERE execution_id = ?1 AND worktree_path = ?2
                         ORDER BY created_at ASC LIMIT 1",
                        (execution_id.as_str(), worktree_path.as_str()),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => (row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?),
                    None => (None, None, None),
                }
            };

            let default_branch = {
                let mut rows = conn
                    .query(
                        "SELECT p.default_branch FROM jobs j
                         JOIN projects p ON j.project_id = p.id
                         WHERE j.execution_id = ?1 LIMIT 1",
                        (execution_id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .and_then(|row| row.opt_text(0).ok().flatten())
                    .unwrap_or_else(|| "main".to_string())
            };

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
                base_commit,
                pack_anchor,
                default_branch,
                base_branch,
                events,
            }))
        })
    })
    .await
}

// ---------------------------------------------------------------------------
// Phase B: synchronous classification (builds + drops the ObjectStore here).
// ---------------------------------------------------------------------------

/// A resolved archival coordinate: the in-pack git commit-id plus, under jj, the
/// stable change-id it was forward-mapped to. The change-id is `None` for
/// plain-git worktrees and for any commit jj could not resolve.
type Coord = (String, Option<String>);

/// One event's rewrite: the row `id` and the [`ArchivedShape`] it becomes. The
/// shape's [`ArchivedShape::encode`] yields the column values `apply` binds, so
/// the writer can never set a combination the reader's `decode` would reject.
#[derive(Clone)]
struct EventUpdate {
    id: String,
    shape: ArchivedShape,
    /// Durable jj change-id provenance for git-addressed shapes (gitcoord
    /// read/write, hybrid read); `None` for zstd/blobbed/full and plain git.
    /// Orthogonal to the six-column encode/decode contract.
    change_id: Option<String>,
}

#[derive(Clone)]
struct ExecHistory {
    execution_id: String,
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

fn classify(
    worktree_path: &str,
    repo_path: &str,
    loaded: &Loaded,
    jj: Option<&JjEnv>,
) -> Result<Classified, String> {
    let worktree = Path::new(worktree_path);
    let tool_map = build_tool_map(&loaded.events);

    // Eligibility: both anchors present, else the whole execution is zstd-only
    // (no pack, no execution_history) — never guess a coordinate.
    let eligible = loaded.base_commit.is_some() && loaded.pack_anchor.is_some();

    // When a committed execution archives as all-zstd (0 gitcoord), the root
    // cause is almost always an ineligible anchor pair: this line names the
    // execution and which anchor is missing so a live teardown is conclusive
    // without a second instrumentation pass.
    if !eligible {
        log::info!(
            "archival ineligible (all-zstd): execution={} worktree={} base_commit={} pack_anchor={} events={}",
            loaded.execution_id,
            worktree_path,
            loaded.base_commit.is_some(),
            loaded.pack_anchor.is_some(),
            loaded.events.len()
        );
    }

    let mut store: Option<ObjectStore> = None;
    let mut history: Option<ExecHistory> = None;
    let mut current: Option<Coord> = None;

    if eligible {
        // Refresh the workspace's backing git refs from jj's current (post-rebase)
        // state before packing, so the pack reflects any out-of-workspace
        // auto-rebase and every forward-mapped commit lands inside it.
        if let Some(jj) = jj {
            if crate::jj::is_jj_dir(worktree) {
                if let Err(e) = crate::jj::export_git(jj, worktree) {
                    log::warn!("archival: jj git export before pack build failed: {e}");
                }
            }
        }
        let pack_anchor = loaded.pack_anchor.as_deref().unwrap();
        let history_base = jj
            .filter(|_| crate::jj::is_jj_dir(worktree))
            .and_then(|jj| {
                crate::jj::resolve_node_fork_point(
                    jj,
                    worktree,
                    loaded
                        .base_branch
                        .as_deref()
                        .or(Some(loaded.default_branch.as_str())),
                    loaded
                        .pack_anchor
                        .as_deref()
                        .or(loaded.base_commit.as_deref()),
                )
            })
            .unwrap_or_else(|| pack_anchor.to_string());
        // `history_base` may intentionally differ from `pack_anchor` after a jj
        // auto-rebase onto an advanced base. That remains reachable after
        // teardown: if it is on the base-branch lineage, the project object DB
        // keeps it durably; if it is below the execution tip and not excluded by
        // the base branch, `build_execution_pack(pack_anchor..tip)` includes it.
        // This lets archived diffs use the same effective fork point as live
        // diffs without requiring the live jj graph later.
        // A production jj workspace is `.jj`-only (no `.git`), so git cannot run
        // in the worktree; after the export above the execution's objects live in
        // the project repo's object database, and the tip is jj's `@-`. Plain git
        // resolves both from the worktree itself.
        let (git_dir, tip_sha) = pack_source(worktree, repo_path, jj)?;
        let pack_pair =
            build_execution_pack(&git_dir, &tip_sha, pack_anchor, &loaded.default_branch)?;
        store = Some(
            ObjectStore::new(Path::new(repo_path), pack_pair.clone())
                .map_err(|e| format!("building archival object store: {e}"))?,
        );
        current = loaded
            .base_commit
            .as_deref()
            .map(|base| forward_map(worktree, jj, base));
        history = Some(ExecHistory {
            execution_id: loaded.execution_id.clone(),
            base_sha: history_base,
            tip_sha,
            pack: pack_pair,
        });
    }

    let store_ref = store.as_ref();
    let mut updates: Vec<EventUpdate> = Vec::new();
    let mut summary = ArchiveSummary::default();
    let mut blobs: Vec<SegmentBlob> = Vec::new();
    let mut sha_cache: HashMap<String, String> = HashMap::new();

    // Pre-pass: pair each committed write to its assistant event. A write's
    // commit sha lives on the *later* `tool_result`, but the heavy payload it
    // coordinates lives on the *earlier* assistant event, so the strip decision
    // needs the pairing up front. Maps every committed write's `tool_use_id` to
    // the resolved full commit sha (eligible executions only).
    let write_commits = if eligible {
        build_write_commits(worktree, jj, &loaded.events, &tool_map, &mut sha_cache)
    } else {
        HashMap::new()
    };

    for event in &loaded.events {
        // Idempotency: never re-archive an already-rewritten row (a stub would be
        // re-read as if it were original content).
        if event.storage_mode.as_deref().unwrap_or("full") != "full" {
            continue;
        }
        summary.bytes_before += event.data.len();

        // Assembled system prompts archive to content-addressed segments (in
        // `archival_blobs`) independent of git anchors, so handle them before any
        // tool classification and regardless of `eligible`.
        if event.event_type == "system:prompt" {
            let mut sink = RewriteSystemBlobSink {
                updates: &mut updates,
                summary: &mut summary,
                blobs: &mut blobs,
            };
            push_blobbed_or_zstd(
                event,
                SystemBlobKind::Prompt,
                build_system_prompt_shape,
                &mut sink,
            )?;
            continue;
        }

        // Near-constant `system:init` handshakes content-address their constant
        // inventory once (the machine config) and inline only the per-run varying
        // fields. Same blob mechanism as the system prompt, independent of git.
        if event.event_type == "system:init" {
            let mut sink = RewriteSystemBlobSink {
                updates: &mut updates,
                summary: &mut summary,
                blobs: &mut blobs,
            };
            push_blobbed_or_zstd(
                event,
                SystemBlobKind::Init,
                build_system_init_shape,
                &mut sink,
            )?;
            continue;
        }

        let tool = event_tool_use_id(&event.data).and_then(|id| tool_map.get(&id));
        let tool_name = tool.map(|(name, _)| normalize_tool_name(name));
        let is_result = matches!(event.event_type.as_str(), "tool_result" | "result");

        if is_result {
            match tool_name {
                Some("read") => try_read(
                    event,
                    tool,
                    current.as_ref(),
                    store_ref,
                    &mut updates,
                    &mut summary,
                )?,
                Some("write") => {
                    // The short write result IS the change summary: nothing heavy
                    // to strip, and reconstruction must return it verbatim, so it
                    // round-trips as a plain zstd row. A committed batch advances
                    // the replay tracker (its assistant event carries the
                    // coordinate); the pre-pass already resolved the full sha.
                    if let Some(coord) =
                        event_tool_use_id(&event.data).and_then(|id| write_commits.get(&id))
                    {
                        current = Some(coord.clone());
                    }
                    push_zstd(event, tool_name, &mut updates, &mut summary)?;
                }
                Some("run") => {
                    // Run output is never reconstructable, but a run that
                    // committed (commit-barrier outcome) advances the tracker.
                    if eligible {
                        if let Some(short) =
                            tool_result_text(&event.data).and_then(|tr| run_commit_sha(&tr))
                        {
                            if let Some(coord) = resolve_coord(worktree, jj, &short, &mut sha_cache)
                            {
                                current = Some(coord);
                            }
                        }
                    }
                    push_zstd(event, tool_name, &mut updates, &mut summary)?;
                }
                _ => push_zstd(event, tool_name, &mut updates, &mut summary)?,
            }
        } else {
            // Non-result events. An assistant event whose write batch committed is
            // gitcoord-stripped (its heavy change payloads dropped); everything
            // else (model text, user, thinking, an uncommitted write's assistant
            // event) falls to plain zstd.
            try_assistant_write(event, tool_name, &write_commits, &mut updates, &mut summary)?;
        }
    }

    // Drop the non-Send store before returning to the async caller.
    drop(store);
    Ok(Classified {
        updates,
        summary,
        history,
        blobs,
    })
}

struct RewriteSystemBlobSink<'a> {
    updates: &'a mut Vec<EventUpdate>,
    summary: &'a mut ArchiveSummary,
    blobs: &'a mut Vec<SegmentBlob>,
}

impl SystemBlobSink for RewriteSystemBlobSink<'_> {
    fn push_blobbed(
        &mut self,
        event: &Event,
        kind: SystemBlobKind,
        shape: ArchivedShape,
        blobs: Vec<SegmentBlob>,
    ) {
        if let ArchivedShape::Blobbed { ref data, .. } = shape {
            self.summary.bytes_after += data.len();
        }
        match kind {
            SystemBlobKind::Prompt => self.summary.system_prompt += 1,
            SystemBlobKind::Init => self.summary.system_init += 1,
        }
        self.blobs.extend(blobs);
        self.updates.push(EventUpdate {
            id: event.id.clone(),
            shape,
            change_id: None,
        });
    }

    fn push_zstd(&mut self, event: &Event) -> Result<(), String> {
        push_zstd(event, None, self.updates, self.summary)
    }
}
