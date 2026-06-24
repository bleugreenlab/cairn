//! Verify-then-rewrite of an execution's events to git coordinates at worktree
//! teardown — the writer counterpart to [`super::reconstruct`].
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
//!     [`super::encoding`], shared with [`super::reconstruct`].)
//!     The paired short write `tool_result` carries no heavy bytes — it IS the
//!     change summary — so it is stored as a plain **zstd** row that round-trips
//!     byte-for-byte and is never replaced by a regenerated diff.
//!     [`super::reconstruct`] regenerates the per-change payload *view* from the
//!     commit ([`super::diff`]) so the transcript shows the committed change
//!     where the payload was. Payload byte-identity is impossible (old_string
//!     anchors aren't recoverable from a commit), so presentation-equivalence is
//!     the accepted contract and `content_render_sha` does not apply to writes.
//! - **zstd**: everything else (run output, directory/glob reads, web/PDF/cairn
//!   resource reads, out-of-repo paths, model text, thinking, artifacts) and any
//!   read that fails the compare (replay drift, a dirty worktree, or cairn-cli's
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

use super::encoding::{
    init_placeholder, ArchivedShape, ARCHIVED_SYSTEM_INIT, ARCHIVED_SYSTEM_PROMPT, INIT_TOOLS_TAG,
};
use super::{build_execution_pack, compress, reconstruct, ObjectStore};
use crate::jj::JjEnv;
use crate::models::Event;
use crate::runs::queries::{event_from_row, EVENT_COLUMNS};
use crate::storage::{DbResult, LocalDb, RowExt};

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
/// extracting cairn-cli's read-composition algorithm into cairn-common is worth
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
    apply(db, updates, history, blobs).await?;
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
            let (base_commit, pack_anchor) = {
                let mut rows = conn
                    .query(
                        "SELECT base_commit, pack_anchor FROM jobs
                         WHERE execution_id = ?1 AND worktree_path = ?2
                         ORDER BY created_at ASC LIMIT 1",
                        (execution_id.as_str(), worktree_path.as_str()),
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => (row.opt_text(0)?, row.opt_text(1)?),
                    None => (None, None),
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
            base_sha: pack_anchor.to_string(),
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

#[cfg(test)]
pub(crate) fn classify_system_blob_columns_for_test(
    event: &Event,
    kind: SystemBlobKind,
) -> Result<
    (
        super::encoding::EventColumns,
        Vec<SegmentBlob>,
        ArchiveSummary,
    ),
    String,
> {
    let mut updates = Vec::new();
    let mut summary = ArchiveSummary::default();
    summary.bytes_before += event.data.len();
    let mut blobs = Vec::new();
    let mut sink = RewriteSystemBlobSink {
        updates: &mut updates,
        summary: &mut summary,
        blobs: &mut blobs,
    };
    match kind {
        SystemBlobKind::Prompt => {
            push_blobbed_or_zstd(event, kind, build_system_prompt_shape, &mut sink)?
        }
        SystemBlobKind::Init => {
            push_blobbed_or_zstd(event, kind, build_system_init_shape, &mut sink)?
        }
    }
    let update = updates
        .into_iter()
        .next()
        .ok_or_else(|| "system blob classifier produced no update".to_string())?;
    Ok((update.shape.encode(), blobs, summary))
}

/// Build the segmented shape and its segment blobs from an event's recorded
/// boundary map (`raw.segments`, an ordered list of `{kind, byteOffset,
/// byteLen}`). Returns `None` when the content or map is absent, a span is out of
/// range, or the reconstructed concatenation does not byte-match the full prompt
/// — every such case keeps the event whole (the caller falls to zstd).
pub(crate) fn build_system_prompt_shape(event: &Event) -> Result<Option<BlobbedShape>, String> {
    let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
        return Ok(None);
    };
    let Some(content) = value.get("content").and_then(|v| v.as_str()) else {
        return Ok(None);
    };
    let Some(seg_map) = value
        .get("raw")
        .and_then(|raw| raw.get("segments"))
        .and_then(|segments| segments.as_array())
        .filter(|segments| !segments.is_empty())
    else {
        return Ok(None);
    };

    let mut archived_segments: Vec<Value> = Vec::with_capacity(seg_map.len());
    let mut blobs: Vec<SegmentBlob> = Vec::new();
    let mut rebuilt = String::with_capacity(content.len());

    for seg in seg_map {
        let kind = seg.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        let offset = seg.get("byteOffset").and_then(|v| v.as_u64());
        let len = seg.get("byteLen").and_then(|v| v.as_u64());
        let (Some(offset), Some(len)) = (offset, len) else {
            return Ok(None);
        };
        let Some(slice) = (offset as usize)
            .checked_add(len as usize)
            .and_then(|end| content.get(offset as usize..end))
        else {
            return Ok(None);
        };
        rebuilt.push_str(slice);
        if kind == crate::orchestrator::session::SEGMENT_KIND_DYNAMIC {
            archived_segments.push(json!({ "kind": kind, "inline": slice }));
        } else {
            let hash = sha256_hex(slice.as_bytes());
            blobs.push((hash.clone(), compress(slice.as_bytes())?));
            archived_segments.push(json!({ "kind": kind, "hash": hash }));
        }
    }

    // Verify-then-discard: the recorded spans must tile the full prompt exactly,
    // or the boundary metadata is untrustworthy and the event stays whole.
    if rebuilt != content {
        return Ok(None);
    }

    let render_sha = sha256_hex(content.as_bytes());
    let mut map = value.as_object().cloned().unwrap_or_default();
    map.remove("content");
    // `raw.segments` (the {kind, byteOffset, byteLen} boundary map) and `raw.hash`
    // are consumed at archival time and redundant with the archived `segments`
    // list below; the byte offsets are meaningless once `content` is dropped, so
    // strip both from the stub.
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("segments");
        raw.remove("hash");
    }
    map.insert("archived".to_string(), json!(ARCHIVED_SYSTEM_PROMPT));
    map.insert("segments".to_string(), Value::Array(archived_segments));
    let data = Value::Object(map).to_string();

    Ok(Some((ArchivedShape::Blobbed { render_sha, data }, blobs)))
}

/// Locate the byte span of `value`'s serialized form in `data`, anchored at
/// `key_token` (e.g. `"\"sessionId\":"`). The bytes after the key are exactly
/// what serde wrote, so matching against [`serde_json::to_string`] of the parsed
/// value is escape-correct. Returns `Ok(None)` when the key is absent or the
/// following bytes do not match — an untrustworthy anchor the caller declines to
/// rewrite (keeping the event whole).
fn locate_value_span(
    data: &str,
    key_token: &str,
    value: &Value,
) -> Result<Option<(usize, usize)>, String> {
    let serialized = serde_json::to_string(value).map_err(|e| e.to_string())?;
    let Some(pos) = data.find(key_token) else {
        return Ok(None);
    };
    let start = pos + key_token.len();
    if !data[start..].starts_with(&serialized) {
        return Ok(None);
    }
    Ok(Some((start, start + serialized.len())))
}

/// Build the skeleton + blobs for a `system:init` event, or `None` to keep it
/// whole. Substitutes the varying spans (`sessionId`, `raw.session_id`,
/// `raw.uuid`, `raw.cwd`, `raw.tools`) with placeholders into a constant
/// skeleton, dedupes the sorted tool set into a shared blob, and verifies the
/// reconstruction is byte-exact before returning. Never re-serializes the event,
/// so it is agnostic to the recording app's struct version: a row whose field set
/// differs simply fails the verify and falls to zstd.
pub(crate) fn build_system_init_shape(event: &Event) -> Result<Option<BlobbedShape>, String> {
    let data = event.data.as_str();
    let Ok(value) = serde_json::from_str::<Value>(data) else {
        return Ok(None);
    };
    let Some(obj) = value.as_object() else {
        return Ok(None);
    };

    // (start, end, placeholder) spans to splice out of `data`, plus the `vars`
    // map (tag -> raw value) the stub inlines for the scalar fields.
    let mut spans: Vec<(usize, usize, String)> = Vec::new();
    let mut vars = serde_json::Map::new();
    let mut blobs: Vec<SegmentBlob> = Vec::new();

    let raw = obj.get("raw");
    // Each scalar varying field: (vars tag, key anchor, value). The tag doubles as
    // the placeholder body and the `vars` key. Absent or non-string fields are
    // skipped — a Codex init carries only `sessionId`.
    let scalars: [(&str, &str, Option<&Value>); 4] = [
        ("sessionId", "\"sessionId\":", obj.get("sessionId")),
        (
            "raw.session_id",
            "\"session_id\":",
            raw.and_then(|r| r.get("session_id")),
        ),
        ("raw.uuid", "\"uuid\":", raw.and_then(|r| r.get("uuid"))),
        ("raw.cwd", "\"cwd\":", raw.and_then(|r| r.get("cwd"))),
    ];
    for (tag, key_token, val) in scalars {
        let Some(val) = val else { continue };
        if !val.is_string() {
            continue;
        }
        let Some((start, end)) = locate_value_span(data, key_token, val)? else {
            return Ok(None);
        };
        spans.push((start, end, init_placeholder(tag)));
        vars.insert(tag.to_string(), val.clone());
    }

    // Tool set: dedupe the sorted unique names into a shared blob and keep only
    // the order permutation that restores this run's shuffled order.
    let mut toolset: Option<(String, Vec<usize>)> = None;
    let mut toolset_json: Option<String> = None;
    if let Some(tools_val) = raw.and_then(|r| r.get("tools")) {
        if let Some(arr) = tools_val.as_array() {
            if let Some(names) = arr
                .iter()
                .map(|t| t.as_str())
                .collect::<Option<Vec<&str>>>()
            {
                let Some((start, end)) = locate_value_span(data, "\"tools\":", tools_val)? else {
                    return Ok(None);
                };
                let mut sorted = names.clone();
                sorted.sort_unstable();
                sorted.dedup();
                let Some(order) = names
                    .iter()
                    .map(|n| sorted.binary_search(n).ok())
                    .collect::<Option<Vec<usize>>>()
                else {
                    return Ok(None);
                };
                let sorted_json = serde_json::to_string(&sorted).map_err(|e| e.to_string())?;
                let hash = sha256_hex(sorted_json.as_bytes());
                blobs.push((hash.clone(), compress(sorted_json.as_bytes())?));
                spans.push((start, end, init_placeholder(INIT_TOOLS_TAG)));
                toolset = Some((hash, order));
                toolset_json = Some(sorted_json);
            }
        }
    }

    // Splice placeholders over the located spans to form the constant skeleton.
    spans.sort_by_key(|(start, _, _)| *start);
    let mut skeleton = String::with_capacity(data.len());
    let mut cursor = 0usize;
    for (start, end, placeholder) in &spans {
        if *start < cursor {
            return Ok(None); // overlapping spans: untrustworthy, keep whole
        }
        skeleton.push_str(&data[cursor..*start]);
        skeleton.push_str(placeholder);
        cursor = *end;
    }
    skeleton.push_str(&data[cursor..]);
    let skeleton_hash = sha256_hex(skeleton.as_bytes());

    let mut stub = serde_json::Map::new();
    stub.insert("archived".to_string(), json!(ARCHIVED_SYSTEM_INIT));
    stub.insert("eventType".to_string(), json!(event.event_type));
    stub.insert("skeleton".to_string(), json!(skeleton_hash));
    if !vars.is_empty() {
        stub.insert("vars".to_string(), Value::Object(vars));
    }
    if let Some((hash, order)) = &toolset {
        stub.insert("toolset".to_string(), json!(hash));
        stub.insert("order".to_string(), json!(order));
    }
    let stub_data = Value::Object(stub).to_string();
    let render_sha = sha256_hex(data.as_bytes());

    // Verify-then-discard: reconstruct byte-exactly from the stub and the exact
    // blob texts the reader will load, and bail to zstd on any mismatch. Sharing
    // the reader's reassembly guarantees the verify exercises the real path.
    let mut verify_blobs: HashMap<String, String> = HashMap::new();
    verify_blobs.insert(skeleton_hash.clone(), skeleton.clone());
    if let (Some((hash, _)), Some(json)) = (&toolset, toolset_json) {
        verify_blobs.insert(hash.clone(), json);
    }
    if reconstruct::reassemble_system_init(&stub_data, &verify_blobs).as_deref() != Some(data) {
        return Ok(None);
    }

    blobs.push((skeleton_hash, compress(skeleton.as_bytes())?));
    Ok(Some((
        ArchivedShape::Blobbed {
            render_sha,
            data: stub_data,
        },
        blobs,
    )))
}

/// Verify a read against the stored agent-seen bytes and rewrite it.
///
/// Computes the candidate rendering with the exact same code reconstruction will
/// use ([`reconstruct::reconstruct_read`]) so a byte match guarantees a faithful
/// round-trip. An unresolvable target (resource/glob/out-of-repo) renders a stub
/// and falls to plain zstd; a resolved-but-differing render (drift, dirty tree,
/// composition seam) falls to zstd and counts as a mismatch.
fn try_read(
    event: &Event,
    tool: Option<&(String, Value)>,
    current: Option<&Coord>,
    store: Option<&ObjectStore>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let paths = tool.and_then(|(_, input)| read_paths(input));
    let stored = tool_result_text(&event.data);
    match (paths, current, stored) {
        (Some(paths), Some((commit, change_id)), Some(stored)) => {
            let candidate = reconstruct::reconstruct_read(commit, &paths, store);
            if candidate == stored {
                // Pure-file fast path: every target resolved and the batch
                // reproduced byte-for-byte.
                let render_sha = sha256_hex(stored.as_bytes());
                let data = read_stub(&event.data, &paths);
                summary.gitcoord_read += 1;
                summary.bytes_after += data.len();
                updates.push(EventUpdate {
                    id: event.id.clone(),
                    shape: ArchivedShape::GitcoordRead {
                        commit: commit.to_string(),
                        render_sha,
                        data,
                    },
                    change_id: change_id.clone(),
                });
            } else if candidate.contains(reconstruct::STUB_PREFIX) {
                // At least one target could not resolve from git (a resource,
                // glob, directory, or out-of-repo path in a mixed batch).
                // Coordinatize the reproducible file sections per-section and
                // store the rest verbatim, falling to plain zstd if none qualify.
                try_hybrid_read(
                    event,
                    &paths,
                    commit,
                    change_id.clone(),
                    store,
                    &stored,
                    updates,
                    summary,
                )?;
            } else {
                // Every target resolved but the render differs (replay drift,
                // dirty tree, composition seam): the coordinate can't be trusted.
                push_zstd(event, Some("read"), updates, summary)?;
                summary.mismatch_fallback += 1;
            }
        }
        _ => push_zstd(event, Some("read"), updates, summary)?,
    }
    Ok(())
}

/// Per-section hybrid archival for a mixed read batch whose pure-file fast path
/// failed because some target could not resolve from git. Coordinatize each
/// reproducible `file:` section and store the remaining sections (and the
/// separators, footers, and affordances) verbatim in a NUL-placeholder skeleton.
///
/// A file section that does not appear verbatim in the stored result — budget-
/// truncated in the mixed batch, a glob/directory the file producer rejects, or an
/// unresolvable blob — is left in the skeleton: per-section degradation, never a
/// batch-wide fallback. With no coordinatizable section the whole event falls to
/// plain zstd (the prior behavior). Otherwise the shape is verified by splicing
/// the skeleton back and comparing to the stored bytes before they are discarded.
#[allow(clippy::too_many_arguments)]
fn try_hybrid_read(
    event: &Event,
    paths: &[String],
    commit: &str,
    change_id: Option<String>,
    store: Option<&ObjectStore>,
    stored: &str,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let mut skeleton = String::new();
    let mut cursor = 0usize;
    let mut indices: Vec<usize> = Vec::new();
    for (index, target) in paths.iter().enumerate() {
        if reconstruct::repo_relative_path(target).is_none() {
            continue; // non-file target: stays verbatim in the skeleton.
        }
        let section = reconstruct::render_archived_file_section(commit, target, store);
        if section.contains(reconstruct::STUB_PREFIX) {
            continue; // glob/directory/unresolvable: stays verbatim.
        }
        // Ordered cursor scan in path order: a verbatim match both proves the
        // section reproduces byte-exactly and locates its span to excise.
        if let Some(relative) = stored[cursor..].find(&section) {
            let start = cursor + relative;
            let end = start + section.len();
            skeleton.push_str(&stored[cursor..start]);
            skeleton.push_str(&reconstruct::section_placeholder(index));
            cursor = end;
            indices.push(index);
        }
        // No verbatim match (truncated in the mixed batch): left verbatim.
    }
    skeleton.push_str(&stored[cursor..]);

    if indices.is_empty() {
        // Nothing reproducible (e.g. a pure-resource batch): the prior zstd path.
        return push_zstd(event, Some("read"), updates, summary);
    }

    // Verify-then-discard: splice the skeleton back with the same renderer that
    // built it and bail to zstd on any mismatch. Passes by construction; the guard
    // is against future render drift, exactly as the pure-file read's compare.
    let rebuilt = reconstruct::splice_hybrid_skeleton(&skeleton, commit, paths, &indices, store);
    if rebuilt != stored {
        return push_zstd(event, Some("read"), updates, summary);
    }

    let render_sha = sha256_hex(stored.as_bytes());
    let data = hybrid_stub(&event.data, paths, &indices);
    let blob = compress(skeleton.as_bytes())?;
    summary.hybrid_read += 1;
    summary.bytes_after += data.len() + blob.len();
    updates.push(EventUpdate {
        id: event.id.clone(),
        shape: ArchivedShape::HybridRead {
            commit: commit.to_string(),
            render_sha,
            data,
            blob,
        },
        change_id,
    });
    Ok(())
}

/// Pair every committed write to its assistant event. A write's commit sha is
/// reported on its `tool_result`, but the heavy payload it coordinates lives on
/// the assistant event that issued the call; this maps the shared `tool_use_id`
/// to the resolved full commit sha so the assistant pass can find it. A batch
/// that did not commit (preview, resource-only, failure) is absent from the map.
fn build_write_commits(
    worktree: &Path,
    jj: Option<&JjEnv>,
    events: &[Event],
    tool_map: &HashMap<String, (String, Value)>,
    sha_cache: &mut HashMap<String, String>,
) -> HashMap<String, Coord> {
    let mut commits = HashMap::new();
    for event in events {
        if !matches!(event.event_type.as_str(), "tool_result" | "result") {
            continue;
        }
        let Some(id) = event_tool_use_id(&event.data) else {
            continue;
        };
        let is_write = tool_map
            .get(&id)
            .map(|(name, _)| normalize_tool_name(name) == "write")
            .unwrap_or(false);
        if !is_write {
            continue;
        }
        if let Some(coord) = tool_result_text(&event.data)
            .and_then(|tr| change_commit_sha(&tr))
            .and_then(|short| resolve_coord(worktree, jj, &short, sha_cache))
        {
            commits.insert(id, coord);
        }
    }
    commits
}

/// Gitcoord-strip the assistant event of a write batch that committed: drop the
/// heavy `toolUses[].input.changes[*].payload` bytes, keep the rest of the event
/// (model text + change skeletons) zstd-compressed in `data_blob`, and pin the
/// batch commit in `content_commit`. A non-assistant event, an assistant event
/// with no committed write, or one bundling writes to several distinct commits
/// (ambiguous coordinate) falls to plain zstd, which preserves it verbatim.
fn try_assistant_write(
    event: &Event,
    tool_name: Option<&str>,
    write_commits: &HashMap<String, Coord>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    match committed_write_for_event(&event.data, write_commits) {
        Some((commit, change_id)) => {
            let remainder = strip_change_payloads(&event.data);
            let blob = compress(remainder.as_bytes())?;
            let data = gitcoord_write_stub(event);
            summary.gitcoord_write += 1;
            summary.bytes_after += data.len() + blob.len();
            updates.push(EventUpdate {
                id: event.id.clone(),
                shape: ArchivedShape::GitcoordWrite { commit, data, blob },
                change_id,
            });
        }
        None => push_zstd(event, tool_name, updates, summary)?,
    }
    Ok(())
}

/// The single commit an assistant event's write batch landed, or `None` when no
/// tool use committed or several did to distinct commits (a single
/// `content_commit` cannot address them, so the row is kept verbatim as zstd).
fn committed_write_for_event(data: &str, write_commits: &HashMap<String, Coord>) -> Option<Coord> {
    let value: Value = serde_json::from_str(data).ok()?;
    let tool_uses = value.get("toolUses")?.as_array()?;
    let mut found: Option<Coord> = None;
    for tool in tool_uses {
        let Some(id) = tool.get("id").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(coord) = write_commits.get(id) else {
            continue;
        };
        match &found {
            None => found = Some(coord.clone()),
            // Ambiguity is judged on the commit alone: a single content_commit
            // cannot address two distinct commits.
            Some(existing) if existing.0 == coord.0 => {}
            Some(_) => return None,
        }
    }
    found
}

/// Drop every `changes[*].payload` from an assistant event's tool call, keeping
/// target/mode and the rest of the call shell. The committed content is
/// regenerated from the coordinate on reconstruction.
///
/// Strips both copies the backends record: the authoritative
/// `toolUses[].input.changes[*].payload`, and the single-tool-use backwards-compat
/// duplicate `toolInput.changes[*].payload` (`agent_process::stream` mirrors a
/// lone tool call's input into a top-level `toolInput`). The renderer and
/// reconstruction key off `toolUses`, so the `toolInput` copy is vestigial — but
/// it is just as heavy, so an archived `data_blob` must retain neither.
fn strip_change_payloads(data: &str) -> String {
    let mut value = serde_json::from_str::<Value>(data).unwrap_or(Value::Null);
    if let Some(tool_uses) = value.get_mut("toolUses").and_then(|v| v.as_array_mut()) {
        for tool in tool_uses {
            strip_changes_payloads(tool.get_mut("input"));
        }
    }
    strip_changes_payloads(value.get_mut("toolInput"));
    value.to_string()
}

/// Remove `payload` from every element of an input value's `changes` array.
/// Both `toolUses[].input` and the duplicate top-level `toolInput` carry the same
/// `{ changes, commit_msg }` shape, so one helper services both.
fn strip_changes_payloads(input: Option<&mut Value>) {
    if let Some(changes) = input
        .and_then(|v| v.as_object_mut())
        .and_then(|input| input.get_mut("changes"))
        .and_then(|v| v.as_array_mut())
    {
        for change in changes {
            if let Some(obj) = change.as_object_mut() {
                obj.remove("payload");
            }
        }
    }
}

/// The gitcoord-write stub kept in `data`: just enough for a list-row label
/// before reconstruction restores the remainder from `data_blob`.
fn gitcoord_write_stub(event: &Event) -> String {
    json!({ "eventType": event.event_type, "archived": "gitcoord_write" }).to_string()
}

fn push_zstd(
    event: &Event,
    tool_name: Option<&str>,
    updates: &mut Vec<EventUpdate>,
    summary: &mut ArchiveSummary,
) -> Result<(), String> {
    let blob = compress(event.data.as_bytes())?;
    let data = zstd_stub(event, tool_name);
    summary.zstd += 1;
    summary.bytes_after += data.len() + blob.len();
    updates.push(EventUpdate {
        id: event.id.clone(),
        shape: ArchivedShape::Zstd { data, blob },
        change_id: None,
    });
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase C: one transaction — execution_history insert + all event UPDATEs.
// ---------------------------------------------------------------------------

async fn apply(
    db: &LocalDb,
    updates: Vec<EventUpdate>,
    history: Option<ExecHistory>,
    blobs: Vec<SegmentBlob>,
) -> Result<(), String> {
    db.write(move |conn| {
        let updates = updates.clone();
        let history = history.clone();
        let blobs = blobs.clone();
        Box::pin(async move {
            // Content-addressed segment blobs first. Shared across executions, so
            // INSERT OR IGNORE: a hash already present (this or another execution)
            // is left untouched and the event simply references it.
            for (hash, content) in &blobs {
                conn.execute(
                    "INSERT OR IGNORE INTO archival_blobs(hash, content, created_at)
                     VALUES (?1, ?2, unixepoch())",
                    (hash.as_str(), turso::Value::Blob(content.clone())),
                )
                .await?;
            }
            if let Some(history) = history {
                match history.pack {
                    Some((pack, idx)) => {
                        conn.execute(
                            "INSERT OR REPLACE INTO execution_history
                             (execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            (
                                history.execution_id.as_str(),
                                history.base_sha.as_str(),
                                history.tip_sha.as_str(),
                                pack,
                                idx,
                            ),
                        )
                        .await?;
                    }
                    None => {
                        conn.execute(
                            "INSERT OR REPLACE INTO execution_history
                             (execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES (?1, ?2, ?3, NULL, NULL)",
                            (
                                history.execution_id.as_str(),
                                history.base_sha.as_str(),
                                history.tip_sha.as_str(),
                            ),
                        )
                        .await?;
                    }
                }
            }
            for EventUpdate {
                id,
                shape,
                change_id,
            } in &updates
            {
                // The shape encodes to its exact six column values; one uniform
                // UPDATE binds them so the writer can never set a combination the
                // reader's `decode` would reject. `content_change_id` rides along
                // as orthogonal provenance, outside that six-column contract.
                let cols = shape.encode();
                let blob_value = match &cols.data_blob {
                    Some(blob) => turso::Value::Blob(blob.clone()),
                    None => turso::Value::Null,
                };
                conn.execute(
                    "UPDATE events SET storage_mode = ?1, content_commit = ?2,
                         content_render_sha = ?3, data = ?4, data_blob = ?5, codec = ?6,
                         content_change_id = ?7
                     WHERE id = ?8",
                    (
                        cols.storage_mode.as_deref(),
                        cols.content_commit.as_deref(),
                        cols.content_render_sha.as_deref(),
                        cols.data.as_str(),
                        blob_value,
                        cols.codec.as_deref(),
                        change_id.as_deref(),
                        id.as_str(),
                    ),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|e| format!("archival transaction failed: {e}"))
}

// ---------------------------------------------------------------------------
// Event-shape helpers. The stored `data` is a serialized `TranscriptEvent`
// (camelCase): tool calls carry `toolUses: [{id, name, input}]` on the assistant
// event; the paired `tool_result` carries `toolUseId` + `toolResult` but no
// `toolInput`, so reads are paired back to their call to recover `paths`.
// ---------------------------------------------------------------------------

/// Map every tool-call id to its `(name, input)` from the assistant events, so a
/// `tool_result` can recover the tool name and (for reads) the requested paths.
pub(crate) fn build_tool_map(events: &[Event]) -> HashMap<String, (String, Value)> {
    let mut map = HashMap::new();
    for event in events {
        if event.event_type != "assistant" {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        let Some(tool_uses) = value.get("toolUses").and_then(|v| v.as_array()) else {
            continue;
        };
        for tool in tool_uses {
            let id = tool.get("id").and_then(|v| v.as_str());
            let name = tool.get("name").and_then(|v| v.as_str());
            if let (Some(id), Some(name)) = (id, name) {
                let input = tool.get("input").cloned().unwrap_or(Value::Null);
                map.insert(id.to_string(), (name.to_string(), input));
            }
        }
    }
    map
}

/// Normalize a recorded tool name to the bare MCP tool name the classifier
/// dispatches on (`read`, `write`, `run`). Both backends record MCP calls
/// server-prefixed and identically shaped: the Claude CLI emits `mcp__cairn__read`
/// and Codex builds `format!("mcp__{server}__{tool}")` over `[mcp_servers.cairn]`
/// (backends/codex/runtime.rs), so a real session's `toolUses[].name` is
/// `mcp__cairn__write`, never the bare `write` the classifier matched on before.
/// Strip the `mcp__<server>` prefix and return the trailing tool segment,
/// tolerating either a `__` or `.` delimiter before the tool name; a non-MCP
/// name passes through unchanged.
pub(crate) fn normalize_tool_name(name: &str) -> &str {
    let Some(rest) = name.strip_prefix("mcp__") else {
        return name;
    };
    // `rest` is `<server>__<tool>`; the tool is the final `__`-delimited segment.
    // Peel a trailing `.`-delimited tail too, so a dot-joined server/tool pairing
    // still resolves to the bare tool name.
    let after_underscores = rest.rsplit("__").next().unwrap_or(rest);
    after_underscores
        .rsplit('.')
        .next()
        .unwrap_or(after_underscores)
}

pub(crate) fn event_tool_use_id(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    value
        .get("toolUseId")
        .or_else(|| value.get("tool_use_id"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn tool_result_text(data: &str) -> Option<String> {
    let value: Value = serde_json::from_str(data).ok()?;
    value
        .get("toolResult")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Read targets from a read tool call's input (`{ "paths": [...] }`). `None` if
/// any element is not a string — such a read cannot be gitcoord-addressed.
fn read_paths(input: &Value) -> Option<Vec<String>> {
    input
        .get("paths")?
        .as_array()?
        .iter()
        .map(|v| v.as_str().map(str::to_string))
        .collect()
}

/// The committed sha from a change report's `commit.sha` (a short sha to be
/// resolved to full against the live worktree). `None` when nothing committed.
fn change_commit_sha(tool_result: &str) -> Option<String> {
    let value: Value = serde_json::from_str(tool_result).ok()?;
    value
        .get("commit")?
        .get("sha")?
        .as_str()
        .map(str::to_string)
}

/// The short sha a run's commit-barrier reported (`✓ Committed changes (<sha>)`).
fn run_commit_sha(tool_result: &str) -> Option<String> {
    const MARKER: &str = "Committed changes (";
    let start = tool_result.find(MARKER)? + MARKER.len();
    let rest = &tool_result[start..];
    let end = rest.find(')')?;
    let sha = rest[..end].trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
}

/// The gitcoord-read stub: drop the heavy rendered `toolResult`, pin
/// `toolInput.paths` (the contract reconstruction dispatches on). Other fields
/// (eventType, toolUseId) are preserved for a list-row label.
fn read_stub(data: &str, paths: &[String]) -> String {
    let mut map = serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    map.insert("toolInput".to_string(), json!({ "paths": paths }));
    map.insert("toolResult".to_string(), Value::Null);
    // Claude records the rendered read a SECOND time under `raw.tool_use_result`
    // (content blocks whose `text` duplicates `toolResult` byte for byte). Nulling
    // `toolResult` alone would leave the whole read in the stub, so drop the
    // duplicate here. Nothing in the codebase reads `tool_use_result` (verified
    // by grep across Rust + frontend), so reconstruction does not re-inject it.
    // Codex never duplicates here: its reader keeps `raw` only for non-text MCP
    // content, which a read never produces.
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("tool_use_result");
    }
    Value::Object(map).to_string()
}

/// The hybrid-read stub: like [`read_stub`] (drop the heavy `toolResult`, pin
/// `toolInput.paths`, strip the duplicate `raw.tool_use_result`) plus the
/// `hybrid_read` marker and the indices of the git-addressed file sections.
/// [`super::encoding::decode`] discriminates on columns (`content_render_sha`
/// presence), not this marker; the marker is for list-row labels and debugging.
fn hybrid_stub(data: &str, paths: &[String], indices: &[usize]) -> String {
    let mut map = serde_json::from_str::<Value>(data)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    map.insert("toolInput".to_string(), json!({ "paths": paths }));
    map.insert("toolResult".to_string(), Value::Null);
    map.insert("archived".to_string(), json!("hybrid_read"));
    map.insert("sections".to_string(), json!(indices));
    if let Some(Value::Object(raw)) = map.get_mut("raw") {
        raw.remove("tool_use_result");
    }
    Value::Object(map).to_string()
}

/// The zstd stub: just enough to render a list-row label. The full original
/// `data` lives compressed in `data_blob` and is restored on read.
pub(crate) fn zstd_stub(event: &Event, tool_name: Option<&str>) -> String {
    let mut map = serde_json::Map::new();
    map.insert("eventType".to_string(), json!(event.event_type));
    if let Some(name) = tool_name {
        map.insert("toolName".to_string(), json!(name));
    }
    if let Ok(Value::Object(original)) = serde_json::from_str::<Value>(&event.data) {
        if let Some(id) = original.get("toolUseId") {
            map.insert("toolUseId".to_string(), id.clone());
        }
    }
    map.insert("archived".to_string(), json!("zstd"));
    Value::Object(map).to_string()
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

/// Resolve a (possibly short) sha to its full 40-hex form against the live
/// worktree, peeling tags to the commit. Cached: an execution often re-reports
/// the same commit across events.
/// Map an already-full git sha to its archival coordinate. Under jj, forward to
/// the current in-pack commit-id plus the stable change-id (so the coordinate
/// survives jj's auto-rebase); under plain git (or when jj cannot resolve it),
/// identity with no change-id.
fn forward_map(worktree: &Path, jj: Option<&JjEnv>, full: &str) -> Coord {
    match jj.and_then(|jj| crate::jj::forward_resolve_commit(jj, worktree, full)) {
        Some((change_id, current)) => (current, Some(change_id)),
        None => (full.to_string(), None),
    }
}

/// Resolve a recorded (possibly short, possibly jj-rewritten) sha to an archival
/// coordinate. Under jj the recorded id is resolved and forward-mapped through jj
/// **directly** — a production jj workspace is non-colocated (`.jj`, no `.git`),
/// so `git rev-parse` in the worktree is not available, and jj resolves a short or
/// already-hidden commit-id all the same. Plain git (or a jj id jj cannot resolve)
/// falls back to expanding the short sha via `git rev-parse`. `None` only when
/// neither layer resolves it.
fn resolve_coord(
    worktree: &Path,
    jj: Option<&JjEnv>,
    short: &str,
    cache: &mut HashMap<String, String>,
) -> Option<Coord> {
    if let Some(jj) = jj {
        if let Some((change_id, current)) = crate::jj::forward_resolve_commit(jj, worktree, short) {
            return Some((current, Some(change_id)));
        }
    }
    let full = resolve_full(worktree, short, cache)?;
    Some((full, None))
}

fn resolve_full(worktree: &Path, sha: &str, cache: &mut HashMap<String, String>) -> Option<String> {
    if let Some(full) = cache.get(sha) {
        return Some(full.clone());
    }
    let output = Command::new("git")
        .current_dir(worktree)
        .args(["rev-parse", "--verify", &format!("{sha}^{{commit}}")])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let full = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if full.is_empty() {
        return None;
    }
    cache.insert(sha.to_string(), full.clone());
    Some(full)
}

/// Resolve the git repository that holds the execution's objects and the tip the
/// archival range is built against. A production jj workspace is `.jj`-only with
/// no `.git`, so git cannot run there: after `export_git` the objects live in the
/// project repo's object database and the tip is jj's `@-` (its `git HEAD`
/// analogue). Plain git resolves both from the worktree itself.
fn pack_source(
    worktree: &Path,
    repo_path: &str,
    jj: Option<&JjEnv>,
) -> Result<(PathBuf, String), String> {
    if let Some(jj) = jj {
        if crate::jj::is_jj_dir(worktree) {
            let tip = crate::jj::head_commit(jj, worktree)
                .map_err(|e| format!("resolving jj tip for archival: {e}"))?;
            return Ok((PathBuf::from(repo_path), tip));
        }
    }
    let tip =
        git_head(worktree).ok_or_else(|| "resolving worktree HEAD for archival".to_string())?;
    Ok((worktree.to_path_buf(), tip))
}

fn git_head(worktree: &Path) -> Option<String> {
    let output = Command::new("git")
        .current_dir(worktree)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::archival::event_fixture::{
        assistant_read, assistant_run, assistant_text, assistant_thinking, assistant_tool,
        assistant_write, assistant_write_dup_tool_input, render_targets as rendered, system_event,
        system_init_claude, system_init_codex, system_prompt, tool_result as read_result,
        user_text, write_result,
    };
    use crate::archival::reconstruct::{reconstruct_events, STUB_PREFIX};
    use crate::archival::testutil::{commit_all, git, init_repo, write_file};
    use crate::storage::migrated_test_db;
    use serde_json::json;
    use std::path::{Path, PathBuf};
    use turso::params;

    // Event-`data` builders (`assistant_read`/`assistant_write`/`assistant_run`/
    // `assistant_tool`/`read_result`/`write_result`) and the rendered-batch
    // expectation (`rendered`) come from the shared real-anatomy fixture imported
    // above, so a wrong-shape assumption fails in exactly one place.

    #[allow(clippy::too_many_arguments)]
    async fn insert_event(
        db: &LocalDb,
        id: &str,
        run_id: &str,
        seq: i64,
        created_at: i64,
        event_type: &str,
        data: &str,
    ) {
        let id = id.to_string();
        let run_id = run_id.to_string();
        let event_type = event_type.to_string();
        let data = data.to_string();
        db.write(move |conn| {
            let (id, run_id, event_type, data) =
                (id.clone(), run_id.clone(), event_type.clone(), data.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events (id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        id.as_str(),
                        run_id.as_str(),
                        seq,
                        created_at,
                        event_type.as_str(),
                        data.as_str(),
                        created_at
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Seed the workspace → project → execution → jobs → runs chain. `repo_path`
    /// is the canonical repo (ObjectStore ODB); `worktree_path` is the live
    /// worktree (git + pack). A child task job + run share the worktree.
    #[allow(clippy::too_many_arguments)]
    async fn seed_chain(
        db: &LocalDb,
        repo_path: &str,
        worktree_path: &str,
        base_commit: Option<&str>,
        pack_anchor: Option<&str>,
        with_task: bool,
    ) {
        let repo_path = repo_path.to_string();
        let worktree_path = worktree_path.to_string();
        let base_commit = base_commit.map(str::to_string);
        let pack_anchor = pack_anchor.map(str::to_string);
        db.write(move |conn| {
            let repo_path = repo_path.clone();
            let worktree_path = worktree_path.clone();
            let base_commit = base_commit.clone();
            let pack_anchor = pack_anchor.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj','ws','p','P',?1,'main',1,1)",
                    (repo_path.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, project_id, status, worktree_path,
                         base_commit, pack_anchor, created_at, updated_at)
                     VALUES ('job','exec','proj','complete',?1,?2,?3,1,1)",
                    params![
                        worktree_path.as_str(),
                        base_commit.as_deref(),
                        pack_anchor.as_deref()
                    ],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
                     VALUES ('run','job','proj','exited',1,1)",
                    (),
                )
                .await?;
                if with_task {
                    conn.execute(
                        "INSERT INTO jobs(id, execution_id, project_id, parent_job_id, status,
                             worktree_path, base_commit, pack_anchor, created_at, updated_at)
                         VALUES ('taskjob','exec','proj','job','complete',?1,?2,?3,2,2)",
                        params![
                            worktree_path.as_str(),
                            base_commit.as_deref(),
                            pack_anchor.as_deref()
                        ],
                    )
                    .await?;
                    conn.execute(
                        "INSERT INTO runs(id, job_id, project_id, status, created_at, updated_at)
                         VALUES ('taskrun','taskjob','proj','exited',2,2)",
                        (),
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn load_events(db: &LocalDb) -> Vec<Event> {
        db.read(|conn| {
            Box::pin(async move {
                let sql = format!(
                    "SELECT {EVENT_COLUMNS} FROM events
                     WHERE run_id IN (
                         SELECT r.id FROM runs r JOIN jobs j ON r.job_id = j.id
                         WHERE j.execution_id = 'exec'
                     )
                     ORDER BY created_at ASC, sequence ASC"
                );
                let mut rows = conn.query(&sql, ()).await?;
                let mut events = Vec::new();
                while let Some(row) = rows.next().await? {
                    events.push(event_from_row(&row)?);
                }
                DbResult::Ok(events)
            })
        })
        .await
        .unwrap()
    }

    fn tool_result_of(event: &Event) -> String {
        let value: Value = serde_json::from_str(&event.data).unwrap();
        value["toolResult"].as_str().unwrap().to_string()
    }

    struct Fixture {
        _origin_dir: tempfile::TempDir,
        _clone_dir: tempfile::TempDir,
        origin: PathBuf,
        clone: PathBuf,
        anchor: String,
        w1: String,
    }

    /// origin holds main at the anchor; the clone makes the work commit (W1) and
    /// a later out-of-band "drift" commit, so W1's objects live only in the range
    /// pack and the anchor resolves from the repo layer.
    fn build_fixture() -> Fixture {
        let origin_dir = tempfile::tempdir().unwrap();
        let origin = origin_dir.path().to_path_buf();
        init_repo(&origin);
        write_file(&origin, "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
        write_file(&origin, "dir/b.txt", b"unchanged-keep\n");
        let anchor = commit_all(&origin, "base");

        let clone_dir = tempfile::tempdir().unwrap();
        let clone = clone_dir.path().to_path_buf();
        git(
            &origin,
            &[
                "clone",
                "-q",
                origin.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );
        git(&clone, &["checkout", "-q", "-b", "work"]);
        write_file(&clone, "a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n");
        write_file(&clone, "c.txt", b"new file\n");
        let w1 = commit_all(&clone, "work");
        // Out-of-band drift commit: not reported through any event result.
        write_file(&clone, "a.txt", b"DRIFT\nbeta\ngamma\ndelta\nepsilon\n");
        commit_all(&clone, "drift");

        Fixture {
            _origin_dir: origin_dir,
            _clone_dir: clone_dir,
            origin,
            clone,
            anchor,
            w1,
        }
    }

    fn short(worktree: &Path, sha: &str) -> String {
        git(worktree, &["rev-parse", "--short", sha])
            .trim()
            .to_string()
    }

    /// The heart of the PR: a realistic event sequence is archived, then
    /// reconstructed, and every classification is asserted end to end.
    #[tokio::test]
    async fn keystone_roundtrip() {
        let fx = build_fixture();
        let db = migrated_test_db("archival-keystone.db").await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            fx.clone.to_str().unwrap(),
            Some(&fx.anchor),
            Some(&fx.anchor),
            true,
        )
        .await;

        let v1 = b"alpha\nbeta\ngamma\ndelta\n";
        let v2 = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n";
        let v3 = b"DRIFT\nbeta\ngamma\ndelta\nepsilon\n";
        let b_txt = b"unchanged-keep\n";
        let w1_short = short(&fx.clone, &fx.w1);

        // Anchor read: full file + single-file grep + an unchanged file (repo
        // layer). current == base == anchor.
        let anchor_targets: Vec<(&str, &[u8])> = vec![
            ("file:a.txt", v1),
            ("file:a.txt?grep=beta", v1),
            ("file:dir/b.txt", b_txt),
        ];
        let stored_anchor = rendered(&anchor_targets);
        // Post-write read at W1.
        let stored_w1 = rendered(&[("file:a.txt", v2)]);
        // Drift read: stored bytes are the real (drifted) HEAD; the tracker is
        // stale at W1, so render-and-compare must fail.
        let stored_drift = rendered(&[("file:a.txt", v3)]);
        // Dirty read: bytes match no committed state at W1.
        let dirty = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\nuncommitted\n";
        let stored_dirty = rendered(&[("file:a.txt", dirty)]);
        // Task read of the unchanged file at W1 (multi-run; repo-layer blob).
        let stored_task = rendered(&[("file:dir/b.txt", b_txt)]);

        // run "run": chronological created_at order.
        insert_event(
            &db,
            "a-r1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read(
                "r1",
                &["file:a.txt", "file:a.txt?grep=beta", "file:dir/b.txt"],
            ),
        )
        .await;
        insert_event(
            &db,
            "e-r1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored_anchor),
        )
        .await;
        insert_event(
            &db,
            "a-w1",
            "run",
            3,
            3,
            "assistant",
            &assistant_write("w1"),
        )
        .await;
        insert_event(
            &db,
            "e-w1",
            "run",
            4,
            4,
            "tool_result",
            &write_result("w1", &w1_short),
        )
        .await;
        // Task run interleaves here (current == W1).
        insert_event(
            &db,
            "a-t1",
            "taskrun",
            1,
            5,
            "assistant",
            &assistant_read("t1", &["file:dir/b.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-t1",
            "taskrun",
            2,
            6,
            "tool_result",
            &read_result("t1", &stored_task),
        )
        .await;
        insert_event(
            &db,
            "a-r2",
            "run",
            5,
            7,
            "assistant",
            &assistant_read("r2", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-r2",
            "run",
            6,
            8,
            "tool_result",
            &read_result("r2", &stored_w1),
        )
        .await;
        // Drift run: no "Committed changes" marker; the tracker stays at W1.
        insert_event(
            &db,
            "a-run",
            "run",
            7,
            9,
            "assistant",
            &assistant_run("run1"),
        )
        .await;
        insert_event(
            &db,
            "e-run",
            "run",
            8,
            10,
            "tool_result",
            &read_result("run1", "ran a command"),
        )
        .await;
        insert_event(
            &db,
            "a-r3",
            "run",
            9,
            11,
            "assistant",
            &assistant_read("r3", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-r3",
            "run",
            10,
            12,
            "tool_result",
            &read_result("r3", &stored_drift),
        )
        .await;
        insert_event(
            &db,
            "a-r4",
            "run",
            11,
            13,
            "assistant",
            &assistant_read("r4", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-r4",
            "run",
            12,
            14,
            "tool_result",
            &read_result("r4", &stored_dirty),
        )
        .await;
        insert_event(
            &db,
            "a-r5",
            "run",
            13,
            15,
            "assistant",
            &assistant_read("r5", &["file:outside.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-r5",
            "run",
            14,
            16,
            "tool_result",
            &read_result("r5", "out of repo content"),
        )
        .await;
        insert_event(
            &db,
            "a-text",
            "run",
            15,
            17,
            "assistant",
            &assistant_text("thinking out loud"),
        )
        .await;
        // Extended-thinking and backend system events fall to plain zstd and must
        // round-trip verbatim.
        insert_event(
            &db,
            "a-think",
            "run",
            16,
            18,
            "assistant",
            &assistant_thinking("planning the next step"),
        )
        .await;
        insert_event(
            &db,
            "sys",
            "run",
            17,
            19,
            "system:init",
            &system_event("init"),
        )
        .await;

        let summary = archive_target(
            &db,
            fx.clone.to_str().unwrap(),
            fx.origin.to_str().unwrap(),
            &["job".to_string(), "taskjob".to_string()],
            None,
        )
        .await
        .unwrap();

        assert_eq!(summary.gitcoord_read, 3, "anchor + W1 + task reads");
        assert_eq!(summary.gitcoord_write, 1);
        assert_eq!(summary.mismatch_fallback, 2, "drift + dirty reads");
        assert!(summary.bytes_before > 0 && summary.bytes_after > 0);

        // The heavy rendered payload is dropped from a gitcoord-read stub (the
        // storage win; net byte totals only shrink at real payload sizes, where
        // zstd's per-frame overhead on many small events no longer dominates).
        let stub_r2 = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT data FROM events WHERE id = 'e-r2'", ())
                        .await?;
                    DbResult::Ok(rows.next().await?.unwrap().text(0)?)
                })
            })
            .await
            .unwrap();
        assert!(
            stub_r2.len() < read_result("r2", &stored_w1).len(),
            "gitcoord stub drops the rendered bytes"
        );
        assert!(
            !stub_r2.contains("epsilon"),
            "the gitcoord stub retains no read content bytes — not even Claude's \
             raw.tool_use_result duplicate of the rendered read"
        );

        // Reconstruct everything and index by id.
        let events = load_events(&db).await;
        let reconstructed = reconstruct_events(&db, events).await;
        let by_id: HashMap<&str, &Event> =
            reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();

        // gitcoord reads are byte-identical with matching render shas.
        assert_eq!(tool_result_of(by_id["e-r1"]), stored_anchor);
        assert_eq!(tool_result_of(by_id["e-r2"]), stored_w1);
        assert_eq!(tool_result_of(by_id["e-t1"]), stored_task);

        // The short write result (e-w1) round-trips byte-for-byte: it IS the
        // change summary, never replaced by a regenerated diff.
        assert_eq!(by_id["e-w1"].data, write_result("w1", &w1_short));

        // The assistant event (a-w1) had its heavy change payloads stripped: the
        // remainder stored in data_blob no longer carries them.
        let remainder_a_w1 = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT data_blob, codec FROM events WHERE id = 'a-w1'", ())
                        .await?;
                    let row = rows.next().await?.unwrap();
                    let blob = row.opt_blob(0)?.unwrap();
                    let codec: String = row.text(1)?;
                    DbResult::Ok((blob, codec))
                })
            })
            .await
            .unwrap();
        let decompressed = String::from_utf8(
            crate::archival::decompress(&remainder_a_w1.1, &remainder_a_w1.0).unwrap(),
        )
        .unwrap();
        assert!(
            !decompressed.contains("HEAVYPAYLOAD"),
            "archived assistant remainder drops the heavy payloads"
        );

        // Reconstruction re-injects the committed per-change diff where each
        // payload was; concatenated in change order they reproduce `git show W1`.
        let recon_w1: Value = serde_json::from_str(&by_id["a-w1"].data).unwrap();
        let changes = recon_w1["toolUses"][0]["input"]["changes"]
            .as_array()
            .unwrap();
        let combined: String = changes
            .iter()
            .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
            .collect();
        let expected_diff = git(&fx.clone, &["show", "--format=", "--no-color", &fx.w1]);
        let expected_diff = expected_diff.trim_start_matches('\n');
        assert_eq!(combined, expected_diff);

        // drift + dirty reads landed zstd and round-trip their stored bytes.
        assert_eq!(tool_result_of(by_id["e-r3"]), stored_drift);
        assert_eq!(tool_result_of(by_id["e-r4"]), stored_dirty);
        // out-of-repo read fell to plain zstd (not a mismatch) and round-trips.
        assert_eq!(tool_result_of(by_id["e-r5"]), "out of repo content");
        // run output round-trips.
        assert_eq!(tool_result_of(by_id["e-run"]), "ran a command");
        // model text, thinking, and system events round-trip exactly.
        assert_eq!(by_id["a-text"].data, assistant_text("thinking out loud"));
        assert_eq!(
            by_id["a-think"].data,
            assistant_thinking("planning the next step")
        );
        assert_eq!(by_id["sys"].data, system_event("init"));

        // The render-sha tripwire was captured for a gitcoord read.
        let render_sha = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT content_render_sha FROM events WHERE id = 'e-r2'",
                            (),
                        )
                        .await?;
                    DbResult::Ok(
                        rows.next()
                            .await?
                            .and_then(|r| r.opt_text(0).ok().flatten()),
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(render_sha, Some(sha256_hex(stored_w1.as_bytes())));

        // A second archival pass is a no-op (rows already archived).
        let again = archive_target(
            &db,
            fx.clone.to_str().unwrap(),
            fx.origin.to_str().unwrap(),
            &["job".to_string(), "taskjob".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(again.gitcoord_read, 0);
        assert_eq!(again.zstd, 0);
    }

    /// The genuine de-circularization: the stored `toolResult` is produced by the
    /// REAL [`handle_read_batch`](crate::mcp::handlers::read::handle_read_batch) over
    /// real worktree files — not the `render_targets` fixture — so this proves the
    /// live producer + shared `assemble` and the archival reconstruction agree
    /// byte-for-byte across the rewrite/replay round trip. Covers a full small
    /// file, a windowed read with a continue footer, a single-file grep, an empty
    /// file, and a multi-target batch in one event — plus a file edited after the
    /// read, which must fall to zstd rather than a false gitcoord match.
    #[tokio::test]
    async fn live_read_batch_round_trips_through_archival() {
        use crate::db::DbState;
        use crate::mcp::handlers::read::handle_read_batch;
        use crate::mcp::types::McpCallbackRequest;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use cairn_common::read::ReadBatchEnvelope;
        use std::collections::HashMap as Map;
        use std::sync::{Arc, Mutex};

        // A real repo at one clean commit; the live worktree is a fresh clone
        // checked out at that commit, so every live file read resolves to the
        // committed blob and archival can address it at `anchor`.
        let origin_dir = tempfile::tempdir().unwrap();
        let origin = origin_dir.path().to_path_buf();
        init_repo(&origin);
        let big: String = (1..=40)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n");
        write_file(&origin, "big.rs", big.as_bytes());
        write_file(&origin, "small.rs", b"hello\nworld\n");
        write_file(
            &origin,
            "needle.rs",
            b"alpha\nNEEDLE\ngamma\nNEEDLE again\n",
        );
        write_file(&origin, "empty.rs", b"");
        let anchor = commit_all(&origin, "base");

        let clone_dir = tempfile::tempdir().unwrap();
        let clone = clone_dir.path().to_path_buf();
        git(
            &origin,
            &[
                "clone",
                "-q",
                origin.to_str().unwrap(),
                clone.to_str().unwrap(),
            ],
        );

        // Build a throwaway orchestrator whose only job is to run the live read
        // over the worktree files (file reads never touch the DB).
        let live_db = migrated_test_db("archival-live-orch.db").await;
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(live_db), search));
        let orch = OrchestratorBuilder::new(
            db_state,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        let cursors = Mutex::new(Map::new());
        let live_text = |paths: serde_json::Value| {
            let orch = &orch;
            let cursors = &cursors;
            let clone = clone.clone();
            async move {
                let request = McpCallbackRequest {
                    cwd: clone.display().to_string(),
                    run_id: None,
                    tool: "read_batch".to_string(),
                    payload: json!({ "paths": paths }),
                    tool_use_id: None,
                };
                let raw = handle_read_batch(orch, &request, cursors).await;
                serde_json::from_str::<ReadBatchEnvelope>(&raw)
                    .unwrap()
                    .text
            }
        };

        let batch_paths = [
            "file:small.rs",
            "file:big.rs?offset=2&limit=3",
            "file:needle.rs?grep=NEEDLE",
            "file:empty.rs",
        ];
        let clean_text = live_text(json!(batch_paths)).await;
        // Sanity: the live envelope carries the enriched suffixes/footers we expect.
        assert!(clean_text.contains("=== file:small.rs [lines 1\u{2013}2 of 2] ==="));
        assert!(
            clean_text.contains("=== file:big.rs?offset=2&limit=3 [lines 3\u{2013}5 of 40] ===")
        );
        assert!(clean_text.contains("continue: file:big.rs?offset=5"));
        assert!(clean_text.contains("=== file:needle.rs?grep=NEEDLE [2 matches] ==="));
        assert!(clean_text.contains("=== file:empty.rs ==="));

        // A read whose bytes drift from the committed blob (here: an uncommitted
        // edit on disk) cannot be addressed by the coordinate and must fall to
        // zstd, round-tripping its stored bytes verbatim — never a false match.
        std::fs::write(clone.join("small.rs"), b"hello\nDIRTY\nworld\n").unwrap();
        let dirty_text = live_text(json!(["file:small.rs"])).await;
        assert_ne!(clean_text, dirty_text);

        // Archive the reads against the committed worktree state.
        let db = migrated_test_db("archival-live-roundtrip.db").await;
        seed_chain(
            &db,
            origin.to_str().unwrap(),
            clone.to_str().unwrap(),
            Some(&anchor),
            Some(&anchor),
            false,
        )
        .await;
        insert_event(
            &db,
            "a-r1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read("r1", &batch_paths),
        )
        .await;
        insert_event(
            &db,
            "e-r1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &clean_text),
        )
        .await;
        insert_event(
            &db,
            "a-r2",
            "run",
            3,
            3,
            "assistant",
            &assistant_read("r2", &["file:small.rs"]),
        )
        .await;
        insert_event(
            &db,
            "e-r2",
            "run",
            4,
            4,
            "tool_result",
            &read_result("r2", &dirty_text),
        )
        .await;

        let summary = archive_target(
            &db,
            clone.to_str().unwrap(),
            origin.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            summary.gitcoord_read, 1,
            "the clean multi-target live read archives to a git coordinate, not mismatch_fallback"
        );
        assert_eq!(
            summary.mismatch_fallback, 1,
            "the post-edit read cannot match its coordinate and falls to zstd"
        );

        // Reconstruct and assert byte equality with the live reads.
        let events = load_events(&db).await;
        let reconstructed = reconstruct_events(&db, events).await;
        let by_id: HashMap<&str, &Event> =
            reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(
            tool_result_of(by_id["e-r1"]),
            clean_text,
            "reconstruction is byte-identical to the live multi-target read"
        );
        assert_eq!(
            tool_result_of(by_id["e-r2"]),
            dirty_text,
            "the zstd-backstopped read round-trips its stored bytes verbatim"
        );
    }

    /// A mixed read batch — a reproducible in-repo file plus a verbatim resource
    /// section — archives per-section: the file section is git-addressed, the
    /// resource stays verbatim in the skeleton, and reconstruction is byte-exact.
    #[tokio::test]
    async fn mixed_read_batch_archives_hybrid() {
        use crate::archival::event_fixture::{mixed_render_targets, MixedSection};
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"alpha\nbeta\ngamma\n");
        let anchor = commit_all(repo, "base");

        let db = migrated_test_db("archival-hybrid-mixed.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            Some(&anchor),
            Some(&anchor),
            false,
        )
        .await;

        let stored = mixed_render_targets(&[
            MixedSection::File("file:a.txt", b"alpha\nbeta\ngamma\n"),
            MixedSection::Resource("cairn://p/P/1", "Issue overview\nrelevant context"),
        ]);
        insert_event(
            &db,
            "a1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read("r1", &["file:a.txt", "cairn://p/P/1"]),
        )
        .await;
        insert_event(
            &db,
            "e1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored),
        )
        .await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            summary.hybrid_read, 1,
            "the mixed batch coordinatizes its file section"
        );
        assert_eq!(summary.gitcoord_read, 0);

        let events = load_events(&db).await;
        let row = events.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(row.storage_mode.as_deref(), Some("gitcoord"));
        assert!(row.content_commit.is_some());
        assert!(
            row.content_render_sha.is_some(),
            "the drift sha over the full original bytes"
        );
        assert!(row.data_blob.is_some(), "the skeleton lives in data_blob");
        assert!(
            row.data_blob.as_ref().unwrap().len() < stored.len(),
            "the compressed skeleton is smaller than the original composed result"
        );

        let reconstructed = reconstruct_events(&db, events).await;
        let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(
            tool_result_of(read),
            stored,
            "hybrid reconstruction is byte-identical to the live mixed read"
        );
    }

    /// Per-section degradation: a batch with one committed file and one file-shaped
    /// target absent from the commit coordinatizes only the resolvable section and
    /// leaves the other verbatim, recording exactly that section index.
    #[tokio::test]
    async fn hybrid_read_degrades_unresolvable_file_sections_verbatim() {
        use crate::archival::event_fixture::{mixed_render_targets, MixedSection};
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"alpha\nbeta\n");
        let anchor = commit_all(repo, "base");

        let db = migrated_test_db("archival-hybrid-degrade.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            Some(&anchor),
            Some(&anchor),
            false,
        )
        .await;

        // ghost.txt is absent from the commit, so its section cannot be regenerated
        // and must stay verbatim in the skeleton.
        let stored = mixed_render_targets(&[
            MixedSection::File("file:a.txt", b"alpha\nbeta\n"),
            MixedSection::File("file:ghost.txt", b"phantom\nrows\n"),
        ]);
        insert_event(
            &db,
            "a1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read("r1", &["file:a.txt", "file:ghost.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored),
        )
        .await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.hybrid_read, 1);

        let events = load_events(&db).await;
        let row = events.iter().find(|e| e.id == "e1").unwrap();
        let data: Value = serde_json::from_str(&row.data).unwrap();
        let sections: Vec<u64> = data["sections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert_eq!(
            sections,
            vec![0],
            "only the committed file section is coordinatized"
        );

        let reconstructed = reconstruct_events(&db, events).await;
        let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(
            tool_result_of(read),
            stored,
            "byte-identical despite the verbatim degraded section"
        );
    }

    /// A pure-resource batch (no reproducible file section) still falls to plain
    /// zstd, never hybrid, and round-trips its stored bytes verbatim.
    #[tokio::test]
    async fn pure_resource_batch_falls_to_zstd() {
        use crate::archival::event_fixture::{mixed_render_targets, MixedSection};
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"alpha\n");
        let anchor = commit_all(repo, "base");

        let db = migrated_test_db("archival-pure-resource.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            Some(&anchor),
            Some(&anchor),
            false,
        )
        .await;

        let stored = mixed_render_targets(&[
            MixedSection::Resource("cairn://p/P/1", "issue one"),
            MixedSection::Resource("cairn://p/P/2", "issue two"),
        ]);
        insert_event(
            &db,
            "a1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read("r1", &["cairn://p/P/1", "cairn://p/P/2"]),
        )
        .await;
        insert_event(
            &db,
            "e1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored),
        )
        .await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.hybrid_read, 0);
        assert_eq!(summary.gitcoord_read, 0);

        let events = load_events(&db).await;
        let row = events.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(
            row.storage_mode.as_deref(),
            Some("zstd"),
            "the pure-resource result falls to plain zstd"
        );

        let reconstructed = reconstruct_events(&db, events).await;
        let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(tool_result_of(read), stored);
    }

    /// Regression for CAIRN-1538: real sessions record MCP-prefixed tool names
    /// (`mcp__cairn__read`, `mcp__cairn__write`, `mcp__cairn__run`), not the bare
    /// verbs the classifier matched on. Without prefix normalization every result
    /// falls to zstd and the gitcoord counts stay zero. Both backends emit the
    /// `__`-joined form; the dotted `mcp__cairn.read` variant is exercised too so
    /// the normalizer's delimiter tolerance is covered.
    #[tokio::test]
    async fn mcp_prefixed_names_classify_to_gitcoord() {
        let fx = build_fixture();
        let db = migrated_test_db("archival-mcp-prefixed.db").await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            fx.clone.to_str().unwrap(),
            Some(&fx.anchor),
            Some(&fx.anchor),
            false,
        )
        .await;

        let b_txt = b"unchanged-keep\n";
        let stored_b = rendered(&[("file:dir/b.txt", b_txt)]);
        let w1_short = short(&fx.clone, &fx.w1);

        // Claude-form read, then the dotted Codex-style variant, both at the
        // anchor (current == base), then a prefixed write that committed W1.
        insert_event(
            &db,
            "a1",
            "run",
            1,
            1,
            "assistant",
            &assistant_tool(
                "r1",
                "mcp__cairn__read",
                json!({ "paths": ["file:dir/b.txt"] }),
            ),
        )
        .await;
        insert_event(
            &db,
            "e1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored_b),
        )
        .await;
        insert_event(
            &db,
            "a2",
            "run",
            3,
            3,
            "assistant",
            &assistant_tool(
                "r2",
                "mcp__cairn.read",
                json!({ "paths": ["file:dir/b.txt"] }),
            ),
        )
        .await;
        insert_event(
            &db,
            "e2",
            "run",
            4,
            4,
            "tool_result",
            &read_result("r2", &stored_b),
        )
        .await;
        insert_event(
            &db,
            "a3",
            "run",
            5,
            5,
            "assistant",
            &assistant_tool(
                "w1",
                "mcp__cairn__write",
                json!({
                "changes": [{ "target": "file:a.txt", "mode": "patch",
                    "payload": { "diff": "@@ heavy diff payload @@" } }], "commit_msg": "w" }),
            ),
        )
        .await;
        insert_event(
            &db,
            "e3",
            "run",
            6,
            6,
            "tool_result",
            &write_result("w1", &w1_short),
        )
        .await;

        let summary = archive_target(
            &db,
            fx.clone.to_str().unwrap(),
            fx.origin.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            summary.gitcoord_read, 2,
            "both prefix forms normalize to a read"
        );
        assert_eq!(
            summary.gitcoord_write, 1,
            "prefixed write normalizes and classifies"
        );
        assert_eq!(summary.mismatch_fallback, 0);

        // Reconstruct: the short result is verbatim, the assistant payload regen'd.
        let events = load_events(&db).await;
        let reconstructed = reconstruct_events(&db, events).await;
        let by_id: HashMap<&str, &Event> =
            reconstructed.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(by_id["e3"].data, write_result("w1", &w1_short));
        let recon: Value = serde_json::from_str(&by_id["a3"].data).unwrap();
        let diff = recon["toolUses"][0]["input"]["changes"][0]["payload"]["diff"]
            .as_str()
            .unwrap();
        assert!(!diff.contains("heavy diff payload"), "heavy payload gone");
        assert!(
            diff.contains("diff --git a/a.txt b/a.txt"),
            "committed diff re-injected, got: {diff}"
        );
    }

    /// A single-tool-use write carries its input twice (`toolUses[0].input` and
    /// the backwards-compat top-level `toolInput`). The archived `data_blob` must
    /// retain neither heavy payload copy, while reconstruction still regenerates
    /// the committed diff from the coordinate.
    #[tokio::test]
    async fn duplicate_tool_input_archives_without_either_payload() {
        let fx = build_fixture();
        let db = migrated_test_db("archival-dup-tool-input.db").await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            fx.clone.to_str().unwrap(),
            Some(&fx.anchor),
            Some(&fx.anchor),
            false,
        )
        .await;

        let w1_short = short(&fx.clone, &fx.w1);
        insert_event(
            &db,
            "a-w1",
            "run",
            1,
            1,
            "assistant",
            &assistant_write_dup_tool_input("w1"),
        )
        .await;
        insert_event(
            &db,
            "e-w1",
            "run",
            2,
            2,
            "tool_result",
            &write_result("w1", &w1_short),
        )
        .await;

        let summary = archive_target(
            &db,
            fx.clone.to_str().unwrap(),
            fx.origin.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.gitcoord_write, 1);

        // The stored remainder (zstd in data_blob) carries neither payload copy.
        let (blob, codec) = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT data_blob, codec FROM events WHERE id = 'a-w1'", ())
                        .await?;
                    let row = rows.next().await?.unwrap();
                    DbResult::Ok((row.opt_blob(0)?.unwrap(), row.text(1)?))
                })
            })
            .await
            .unwrap();
        let remainder =
            String::from_utf8(crate::archival::decompress(&codec, &blob).unwrap()).unwrap();
        assert!(
            !remainder.contains("HEAVYPAYLOAD"),
            "archived remainder drops both payload copies"
        );
        let stored: Value = serde_json::from_str(&remainder).unwrap();
        assert!(
            stored["toolUses"][0]["input"]["changes"][0]
                .get("payload")
                .is_none(),
            "toolUses payload stripped"
        );
        assert!(
            stored["toolInput"]["changes"][0].get("payload").is_none(),
            "duplicate toolInput payload stripped"
        );

        // Reconstruction regenerates the committed diff into the toolUses changes.
        let events = load_events(&db).await;
        let reconstructed = reconstruct_events(&db, events).await;
        let recon = reconstructed.iter().find(|e| e.id == "a-w1").unwrap();
        let value: Value = serde_json::from_str(&recon.data).unwrap();
        let combined: String = value["toolUses"][0]["input"]["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
            .collect();
        let expected = git(&fx.clone, &["show", "--format=", "--no-color", &fx.w1]);
        assert_eq!(combined, expected.trim_start_matches('\n'));
    }

    #[test]
    fn normalize_tool_name_strips_prefixes() {
        assert_eq!(normalize_tool_name("mcp__cairn__read"), "read");
        assert_eq!(normalize_tool_name("mcp__cairn__write"), "write");
        assert_eq!(normalize_tool_name("mcp__cairn__run"), "run");
        // Dotted server/tool join still resolves to the bare tool.
        assert_eq!(normalize_tool_name("mcp__cairn.write"), "write");
        // A non-MCP name passes through untouched.
        assert_eq!(normalize_tool_name("read"), "read");
        assert_eq!(normalize_tool_name("bash"), "bash");
    }

    /// An execution whose range is empty (tip == anchor == default branch)
    /// archives reads with a NULL pack and still reconstructs from the repo ODB.
    #[tokio::test]
    async fn empty_range_archives_with_null_pack() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"alpha\nbeta\n");
        let anchor = commit_all(repo, "base");

        let db = migrated_test_db("archival-empty-range.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            Some(&anchor),
            Some(&anchor),
            false,
        )
        .await;

        let stored = rendered(&[("file:a.txt", b"alpha\nbeta\n")]);
        insert_event(
            &db,
            "a1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read("r1", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored),
        )
        .await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.gitcoord_read, 1);

        // The execution_history row exists with a NULL pack.
        let pack_is_null = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT pack FROM execution_history WHERE execution_id = 'exec'",
                            (),
                        )
                        .await?;
                    let row = rows.next().await?.unwrap();
                    DbResult::Ok(row.opt_blob(0)?.is_none())
                })
            })
            .await
            .unwrap();
        assert!(pack_is_null, "empty range stores a NULL pack");

        let events = load_events(&db).await;
        let reconstructed = reconstruct_events(&db, events).await;
        let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(tool_result_of(read), stored);
    }

    /// No base_commit recorded → the whole execution is zstd-only, no pack, no
    /// execution_history; every event still round-trips.
    #[tokio::test]
    async fn missing_anchor_is_zstd_only() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"alpha\n");
        commit_all(repo, "base");

        let db = migrated_test_db("archival-zstd-only.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            None,
            None,
            false,
        )
        .await;

        let stored = rendered(&[("file:a.txt", b"alpha\n")]);
        insert_event(
            &db,
            "a1",
            "run",
            1,
            1,
            "assistant",
            &assistant_read("r1", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e1",
            "run",
            2,
            2,
            "tool_result",
            &read_result("r1", &stored),
        )
        .await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.gitcoord_read, 0);
        assert_eq!(summary.zstd, 2);

        let has_history = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT 1 FROM execution_history WHERE execution_id = 'exec'",
                            (),
                        )
                        .await?;
                    DbResult::Ok(rows.next().await?.is_some())
                })
            })
            .await
            .unwrap();
        assert!(
            !has_history,
            "zstd-only execution writes no execution_history row"
        );

        let events = load_events(&db).await;
        let reconstructed = reconstruct_events(&db, events).await;
        let read = reconstructed.iter().find(|e| e.id == "e1").unwrap();
        assert_eq!(tool_result_of(read), stored);
    }

    /// Full-text search still finds an archived session's text: the writer
    /// rewrites a `user` event to a zstd stub (its needle living in `data_blob`),
    /// and the index rebuild reconstructs it before indexing.
    #[tokio::test]
    async fn archived_session_text_is_still_searchable() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"x\n");
        let anchor = commit_all(repo, "base");

        let db = migrated_test_db("archival-fts.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            Some(&anchor),
            Some(&anchor),
            false,
        )
        .await;
        insert_event(
            &db,
            "u1",
            "run",
            1,
            1,
            "user",
            &user_text("zephyr archival needle"),
        )
        .await;

        archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();

        // The user event is now a zstd stub; its text survives only in data_blob.
        let stub_data = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT data FROM events WHERE id = 'u1'", ())
                        .await?;
                    DbResult::Ok(rows.next().await?.unwrap().text(0)?)
                })
            })
            .await
            .unwrap();
        assert!(
            !stub_data.contains("zephyr"),
            "stub no longer carries the needle inline"
        );

        let index_dir = tempfile::tempdir().unwrap();
        let index = crate::storage::SearchIndex::open_or_create(index_dir.path()).unwrap();
        index.rebuild(&db).await.unwrap();
        assert_eq!(
            index.search("zephyr", None).unwrap().len(),
            1,
            "archived session text is reconstructed and indexed"
        );
    }

    #[test]
    fn run_commit_sha_extracts_barrier_sha() {
        assert_eq!(
            run_commit_sha("output\n\n\u{2713} Committed changes (abc1234) updated PR#5")
                .as_deref(),
            Some("abc1234")
        );
        assert_eq!(run_commit_sha("no commit here"), None);
    }

    #[test]
    fn read_stub_pins_paths_and_drops_result() {
        let data = read_result("t1", "heavy rendered bytes");
        let stub = read_stub(&data, &["file:a.txt".to_string()]);
        let value: Value = serde_json::from_str(&stub).unwrap();
        assert_eq!(value["toolInput"]["paths"][0], "file:a.txt");
        assert!(value["toolResult"].is_null());
        // The Claude `raw.tool_use_result` duplicate of the rendered read is
        // dropped too — nulling `toolResult` alone would leave it behind.
        assert!(value["raw"]["tool_use_result"].is_null());
        assert!(!stub.contains("heavy rendered bytes"));
        assert!(!stub.contains(STUB_PREFIX));
    }

    /// The heavy bytes live on the *assistant* event's `toolUses[].input` (real
    /// events shape — the paired tool_result carries no `toolInput`). Stripping
    /// drops every change payload while keeping the call skeleton.
    #[test]
    fn strip_change_payloads_drops_payload_keeps_skeleton() {
        let stripped = strip_change_payloads(&assistant_write("w1"));
        assert!(
            !stripped.contains("HEAVYPAYLOAD"),
            "heavy payloads stripped"
        );
        let value: Value = serde_json::from_str(&stripped).unwrap();
        let input = &value["toolUses"][0]["input"];
        assert_eq!(input["changes"][0]["target"], "file:a.txt");
        assert_eq!(input["changes"][0]["mode"], "patch");
        assert!(
            input["changes"][0].get("payload").is_none(),
            "payload dropped"
        );
        assert!(
            input["changes"][1].get("payload").is_none(),
            "payload dropped"
        );
        assert_eq!(input["commit_msg"], "w", "call skeleton kept");
    }

    // ---- system-prompt segment archival ----

    async fn blob_count(db: &LocalDb) -> i64 {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT COUNT(*) FROM archival_blobs", ())
                    .await?;
                DbResult::Ok(rows.next().await?.unwrap().i64(0)?)
            })
        })
        .await
        .unwrap()
    }

    async fn event_data_of(db: &LocalDb, id: &'static str) -> String {
        db.read(move |conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query("SELECT data FROM events WHERE id = ?1", (id,))
                    .await?;
                DbResult::Ok(rows.next().await?.unwrap().text(0)?)
            })
        })
        .await
        .unwrap()
    }

    /// A realistic assembled prompt round-trips byte-identical through segment
    /// archival; identical static segments across two runs collapse to shared blob
    /// rows (only refs added the second time); a re-archival pass is a no-op.
    #[tokio::test]
    async fn system_prompt_segments_round_trip_and_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"x\n");
        commit_all(repo, "base");

        // No git anchors: system-prompt segmentation is independent of git, so it
        // must still fire on an otherwise zstd-only execution.
        let db = migrated_test_db("archival-sysprompt-roundtrip.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            None,
            None,
            false,
        )
        .await;

        let backend_base = "CLAUDE-BASE ".repeat(800);
        let cairn = format!("\n\n{}", "CAIRN-PROMPT ".repeat(700));
        let workspace = "\n\n## Workspace Instructions\n\nworkspace doctrine".to_string();
        let agent = "\n\n<agent_role>\nbuilder role body".to_string();
        let dyn1 = "\n\n## Orientation\n\ncwd=/work/run-1\n</agent_role>".to_string();
        let dyn2 = "\n\n## Orientation\n\ncwd=/work/run-2\n</agent_role>".to_string();

        let statics: [(&str, &str); 4] = [
            ("backend_base", &backend_base),
            ("cairn", &cairn),
            ("workspace", &workspace),
            ("agent", &agent),
        ];
        let (data1, content1) = system_prompt(
            &statics
                .iter()
                .copied()
                .chain([("dynamic", dyn1.as_str())])
                .collect::<Vec<_>>(),
        );
        let (data2, content2) = system_prompt(
            &statics
                .iter()
                .copied()
                .chain([("dynamic", dyn2.as_str())])
                .collect::<Vec<_>>(),
        );
        insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data1).await;
        insert_event(&db, "sp2", "run", 2, 2, "system:prompt", &data2).await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.system_prompt, 2);
        // Four distinct static segments shared by both events — not eight rows.
        assert_eq!(blob_count(&db).await, 4, "identical static segments dedup");

        let stored1 = event_data_of(&db, "sp1").await;
        assert!(
            !stored1.contains("CLAUDE-BASE"),
            "static bytes moved to archival_blobs"
        );
        assert!(
            stored1.contains("cwd=/work/run-1"),
            "dynamic tail stays inline"
        );
        // The redundant `raw.segments` byte map and `raw.hash` are stripped from
        // the archived stub (the archived top-level `segments` list supersedes
        // them; byte offsets are meaningless once `content` is gone).
        let stored1_value: Value = serde_json::from_str(&stored1).unwrap();
        assert!(
            stored1_value["raw"].get("segments").is_none(),
            "redundant raw.segments stripped from the archived stub"
        );
        assert!(
            stored1_value["raw"].get("hash").is_none(),
            "redundant raw.hash stripped from the archived stub"
        );
        assert!(
            stored1_value["segments"].is_array(),
            "archived segments list is preserved"
        );
        eprintln!(
            "SYS_PROMPT_BYTES full={} archived_stub={} (blobs one-time, 4 rows)",
            content1.len(),
            stored1.len()
        );

        let events = load_events(&db).await;
        let recon = reconstruct_events(&db, events).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        let recon1: Value = serde_json::from_str(&by_id["sp1"].data).unwrap();
        let recon2: Value = serde_json::from_str(&by_id["sp2"].data).unwrap();
        assert_eq!(recon1["content"].as_str().unwrap(), content1);
        assert_eq!(recon2["content"].as_str().unwrap(), content2);

        // Re-archival is a no-op: rows are already gitcoord, no blobs added.
        let again = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(again.system_prompt, 0);
        assert_eq!(blob_count(&db).await, 4, "second pass adds zero blob rows");
    }

    /// Corrupted boundary metadata (spans that no longer tile the content) fails
    /// the byte-exact verify and the event keeps its whole bytes as zstd.
    #[tokio::test]
    async fn system_prompt_corrupt_boundary_falls_back_to_zstd() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"x\n");
        commit_all(repo, "base");
        let db = migrated_test_db("archival-sysprompt-corrupt.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            None,
            None,
            false,
        )
        .await;

        let (data, content) = system_prompt(&[
            ("backend_base", "AAAA"),
            ("cairn", "BBBB"),
            ("dynamic", "CCCC"),
        ]);
        // Shorten the first recorded span so the spans drop a byte.
        let mut value: Value = serde_json::from_str(&data).unwrap();
        value["raw"]["segments"][0]["byteLen"] = json!(3);
        let data = value.to_string();
        insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data).await;

        let summary = archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(summary.system_prompt, 0, "verify fails, no segmentation");
        assert_eq!(summary.zstd, 1, "falls back to whole-event zstd");
        assert_eq!(blob_count(&db).await, 0);

        let events = load_events(&db).await;
        let recon = reconstruct_events(&db, events).await;
        let sp = recon.iter().find(|e| e.id == "sp1").unwrap();
        let value: Value = serde_json::from_str(&sp.data).unwrap();
        assert_eq!(value["content"].as_str().unwrap(), content);
    }

    /// A missing blob row (e.g. a dropped segment) degrades to a labeled stub in
    /// place; the resolvable segments and the inline tail still reconstruct.
    #[tokio::test]
    async fn system_prompt_missing_blob_degrades_to_stub() {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path();
        init_repo(repo);
        write_file(repo, "a.txt", b"x\n");
        commit_all(repo, "base");
        let db = migrated_test_db("archival-sysprompt-missing.db").await;
        seed_chain(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            None,
            None,
            false,
        )
        .await;

        let (data, _content) = system_prompt(&[
            ("backend_base", "BACKENDBASEDATA"),
            ("cairn", "CAIRNDATA"),
            ("dynamic", "DYNTAIL"),
        ]);
        insert_event(&db, "sp1", "run", 1, 1, "system:prompt", &data).await;
        archive_target(
            &db,
            repo.to_str().unwrap(),
            repo.to_str().unwrap(),
            &["job".to_string()],
            None,
        )
        .await
        .unwrap();
        assert_eq!(blob_count(&db).await, 2);

        // Drop the cairn segment's blob.
        let cairn_hash = sha256_hex(b"CAIRNDATA");
        db.write(move |conn| {
            let cairn_hash = cairn_hash.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM archival_blobs WHERE hash = ?1",
                    (cairn_hash.as_str(),),
                )
                .await?;
                DbResult::Ok(())
            })
        })
        .await
        .unwrap();

        let events = load_events(&db).await;
        let recon = reconstruct_events(&db, events).await;
        let sp = recon.iter().find(|e| e.id == "sp1").unwrap();
        let value: Value = serde_json::from_str(&sp.data).unwrap();
        let content = value["content"].as_str().unwrap();
        assert!(
            content.contains("BACKENDBASEDATA"),
            "resolvable segment intact"
        );
        assert!(
            content.contains(STUB_PREFIX),
            "missing segment becomes a labeled stub"
        );
        assert!(content.contains("DYNTAIL"), "inline dynamic tail intact");
    }

    // ---- system-init skeleton archival ----

    /// Set up a git repo + seeded chain so `archive_target` can run, returning the
    /// db and the repo path string. System-init archival is independent of git,
    /// but `archive_target` still needs the chain and a present worktree.
    async fn init_db_for(name: &'static str) -> (LocalDb, tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().to_str().unwrap().to_string();
        init_repo(dir.path());
        write_file(dir.path(), "a.txt", b"x\n");
        commit_all(dir.path(), "base");
        let db = migrated_test_db(name).await;
        seed_chain(&db, &repo, &repo, None, None, false).await;
        (db, dir, repo)
    }

    /// Two Claude `system:init` events from different runs of the same machine —
    /// differing only in session ids, cwd, and the CLI's per-run tool shuffle —
    /// round-trip byte-exact and collapse to one shared skeleton blob plus one
    /// shared (sorted) tool-set blob. A re-archival pass adds nothing.
    #[tokio::test]
    async fn system_init_round_trips_and_dedup() {
        let (db, _dir, repo) = init_db_for("archival-sysinit-roundtrip.db").await;

        // Same tool SET, shuffled order; different session/uuid/cwd.
        let tools_a = ["mcp__cairn__read", "Glob", "Grep", "mcp__cairn__write"];
        let tools_b = ["Grep", "mcp__cairn__write", "mcp__cairn__read", "Glob"];
        let init1 = system_init_claude("sess-1", "uuid-1", "/work/run-1", &tools_a);
        let init2 = system_init_claude("sess-2", "uuid-2", "/work/run-2", &tools_b);
        insert_event(&db, "si1", "run", 1, 1, "system:init", &init1).await;
        insert_event(&db, "si2", "run", 2, 2, "system:init", &init2).await;

        let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        assert_eq!(summary.system_init, 2);
        assert_eq!(summary.zstd, 0, "both inits content-addressed, none zstd");
        // One skeleton (identical config) + one tool-set (same sorted set) — not
        // four rows.
        assert_eq!(
            blob_count(&db).await,
            2,
            "skeleton + tool set dedup across runs"
        );

        let stored1 = event_data_of(&db, "si1").await;
        assert!(
            !stored1.contains("claude-sonnet"),
            "constant inventory moved to the skeleton blob"
        );
        assert!(
            !stored1.contains("Glob"),
            "tool names live in the shared tool-set blob, not the per-row stub"
        );
        assert!(stored1.contains("/work/run-1"), "cwd inlined in the stub");
        assert!(stored1.contains("sess-1"), "session id inlined in the stub");

        // Byte-exact reconstruction, including each run's original tool order.
        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(by_id["si1"].data, init1, "run 1 reconstructs byte-exact");
        assert_eq!(by_id["si2"].data, init2, "run 2 reconstructs byte-exact");

        // Re-archival is a no-op: rows are already blobbed, no blobs added.
        let again = archive_target(&db, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        assert_eq!(again.system_init, 0);
        assert_eq!(blob_count(&db).await, 2, "second pass adds zero blob rows");
    }

    /// Codex's fixed init (only `sessionId` varies, no tools) is the degenerate
    /// case: two runs collapse to one shared skeleton blob and reconstruct exactly.
    #[tokio::test]
    async fn system_init_codex_round_trips_and_dedups() {
        let (db, _dir, repo) = init_db_for("archival-sysinit-codex.db").await;
        let init1 = system_init_codex("codex-sess-1");
        let init2 = system_init_codex("codex-sess-2");
        insert_event(&db, "ci1", "run", 1, 1, "system:init", &init1).await;
        insert_event(&db, "ci2", "run", 2, 2, "system:init", &init2).await;

        let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        assert_eq!(summary.system_init, 2);
        assert_eq!(blob_count(&db).await, 1, "one shared skeleton, no tool set");

        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(by_id["ci1"].data, init1);
        assert_eq!(by_id["ci2"].data, init2);
    }

    /// Version-agnostic: an init recorded by a different app version (here, an
    /// extra `usage` field the current struct lacks) still round-trips byte-exact,
    /// because the skeleton is the literal recorded bytes with the varying spans
    /// substituted — never a re-serialization of the current struct. This is what
    /// lets the backfill reclaim the historical backlog.
    #[tokio::test]
    async fn system_init_foreign_version_round_trips() {
        let (db, _dir, repo) = init_db_for("archival-sysinit-foreign.db").await;
        let base = system_init_claude("sess-x", "uuid-x", "/work/x", &["Glob", "Grep"]);
        // Simulate an older recorder that emitted a `usage` field.
        let init = base.replace("\"isError\":false,", "\"isError\":false,\"usage\":null,");
        assert!(init.contains("\"usage\":null"));
        insert_event(&db, "sf1", "run", 1, 1, "system:init", &init).await;

        let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        assert_eq!(
            summary.system_init, 1,
            "foreign-version init still archives"
        );

        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let si = recon.iter().find(|e| e.id == "sf1").unwrap();
        assert_eq!(
            si.data, init,
            "foreign-version init reconstructs byte-exact"
        );
    }

    /// A `system:init` whose `data` is not the expected object falls back to whole-
    /// event zstd and round-trips verbatim — the safety net for any shape the
    /// builder cannot prove byte-exact.
    #[tokio::test]
    async fn system_init_unexpected_shape_falls_back_to_zstd() {
        let (db, _dir, repo) = init_db_for("archival-sysinit-fallback.db").await;
        let data = "\"a bare json string, not an init object\"";
        insert_event(&db, "sb1", "run", 1, 1, "system:init", data).await;

        let summary = archive_target(&db, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        assert_eq!(summary.system_init, 0);
        assert_eq!(summary.zstd, 1, "falls back to whole-event zstd");
        assert_eq!(blob_count(&db).await, 0);

        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let sb = recon.iter().find(|e| e.id == "sb1").unwrap();
        assert_eq!(sb.data, data, "verbatim round-trip");
    }

    /// A dropped skeleton blob degrades to a labeled stub object (still valid JSON),
    /// rather than corrupting the transcript.
    #[tokio::test]
    async fn system_init_missing_skeleton_degrades_to_stub() {
        let (db, _dir, repo) = init_db_for("archival-sysinit-missing.db").await;
        let init = system_init_claude("sess-m", "uuid-m", "/work/m", &["Glob", "Grep"]);
        insert_event(&db, "sm1", "run", 1, 1, "system:init", &init).await;
        archive_target(&db, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        assert_eq!(blob_count(&db).await, 2);

        // Drop every blob so the skeleton can't resolve.
        db.write(|conn| {
            Box::pin(async move {
                conn.execute("DELETE FROM archival_blobs", ()).await?;
                DbResult::Ok(())
            })
        })
        .await
        .unwrap();

        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let sm = recon.iter().find(|e| e.id == "sm1").unwrap();
        let value: Value = serde_json::from_str(&sm.data).expect("degraded stub is valid json");
        assert_eq!(value["eventType"], "system:init");
        assert!(
            value["content"].as_str().unwrap().contains(STUB_PREFIX),
            "missing skeleton becomes a labeled stub"
        );
    }

    // ---------------------------------------------------------------------
    // jj forward-mapping (CAIRN-1964): the coordinate a write event is
    // archived under must be the CURRENT in-pack commit, not the commit-id
    // jj rewrote out from under it.
    // ---------------------------------------------------------------------

    fn jj_bin() -> Option<String> {
        let bin = std::env::var("CAIRN_JJ_BIN")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "jj".to_string());
        Command::new(&bin)
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
            .then_some(bin)
    }

    /// Run a jj command in `dir` against an isolated config; assert success.
    fn jj_raw(bin: &str, cfg: &Path, dir: &Path, args: &[&str]) -> String {
        let out = Command::new(bin)
            .current_dir(dir)
            .env("JJ_CONFIG", cfg)
            .env("EDITOR", "true")
            .env("JJ_EDITOR", "true")
            .args(args)
            .output()
            .expect("spawn jj");
        assert!(
            out.status.success(),
            "jj {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    async fn event_coord(db: &LocalDb, id: &str) -> (Option<String>, Option<String>) {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT content_commit, content_change_id FROM events WHERE id = ?1",
                        (id.as_str(),),
                    )
                    .await?;
                let row = rows.next().await?.unwrap();
                DbResult::Ok((row.opt_text(0)?, row.opt_text(1)?))
            })
        })
        .await
        .unwrap()
    }

    async fn exec_pack_oids(db: &LocalDb) -> std::collections::BTreeSet<String> {
        let idx = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT pack_idx FROM execution_history WHERE execution_id = 'exec'",
                            (),
                        )
                        .await?;
                    DbResult::Ok(
                        rows.next()
                            .await?
                            .and_then(|r| r.opt_blob(0).ok().flatten()),
                    )
                })
            })
            .await
            .unwrap()
            .expect("execution_history pack_idx present");
        let index = gix_pack::index::File::from_data(
            idx,
            std::path::PathBuf::from("<memory>.idx"),
            gix_hash::Kind::Sha1,
        )
        .unwrap();
        index.iter().map(|e| e.oid.to_hex().to_string()).collect()
    }

    /// The non-colocated regression the colocated keystone masks: a production jj
    /// workspace (`jj workspace add`) carries `.jj` and NO `.git`, so the
    /// `resolve_full`-first path could never resolve a recorded write/run commit
    /// there and silently dropped the coordinate. `resolve_coord` must forward-map
    /// the recorded SHORT id through jj directly, with no git in the worktree.
    #[test]
    #[serial_test::serial(jj)]
    fn resolve_coord_forward_maps_short_id_in_noncolocated_jj_workspace() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping resolve_coord_forward_maps_short_id_in_noncolocated_jj_workspace: jj not resolvable");
            return;
        };
        let home = tempfile::tempdir().unwrap();
        let proj = tempfile::tempdir().unwrap();
        let wts = tempfile::tempdir().unwrap();
        init_repo(proj.path());
        write_file(proj.path(), "a.txt", b"base\n");
        commit_all(proj.path(), "base");

        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj.path()).unwrap();
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None)
            .unwrap();

        // The real workspace shape: a `.jj` dir and no `.git`.
        assert!(ws.join(".jj").exists());
        assert!(
            !ws.join(".git").exists(),
            "a jj workspace is non-colocated (no .git)"
        );

        // Seal a parent then a write commit; record the write's SHORT, original id.
        std::fs::write(ws.join("p.txt"), "p\n").unwrap();
        crate::jj::seal(&jj, &ws, "P", None).unwrap();
        std::fs::write(ws.join("w.txt"), "w\n").unwrap();
        crate::jj::seal(&jj, &ws, "W", None).unwrap();

        let cfg = home.path().join("probe-config.toml");
        std::fs::write(
            &cfg,
            "ui.paginate = \"never\"\n[user]\nname = \"t\"\nemail = \"t@e.com\"\n",
        )
        .unwrap();
        let w_short = jj_raw(
            &bin,
            &cfg,
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id.short()"],
        );
        let w_full = jj_raw(
            &bin,
            &cfg,
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
        );
        let w_change = jj_raw(
            &bin,
            &cfg,
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
        );

        // Reword the parent: auto-rebase churns the write's commit-id.
        jj_raw(
            &bin,
            &cfg,
            &ws,
            &["describe", "-r", "@--", "-m", "P reworded"],
        );
        let w_new = jj_raw(
            &bin,
            &cfg,
            &ws,
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
        );
        assert_ne!(
            w_full, w_new,
            "precondition: the rebase churned the write commit-id"
        );

        // The fix: resolve_coord forward-maps the short, now-hidden id with no git
        // in the worktree. The plain-git path genuinely cannot resolve it here.
        let mut cache = HashMap::new();
        assert!(
            resolve_full(&ws, &w_short, &mut cache).is_none(),
            "git rev-parse cannot resolve the id in a .jj-only worktree"
        );
        let coord = resolve_coord(&ws, Some(&jj), &w_short, &mut cache)
            .expect("jj resolves the recorded short commit without git in the worktree");
        assert_eq!(
            coord,
            (w_new, Some(w_change)),
            "the coordinate is the forward-mapped commit plus its stable change-id"
        );
    }

    /// The keystone of CAIRN-1964: a write commit whose commit-id jj churned via
    /// auto-rebase is archived under its CURRENT (in-pack) commit-id with the
    /// stable change-id recorded, while the bare-git control pins the stale
    /// commit-id that the durable pack no longer contains. A drift read after the
    /// write proves the unchanged render-and-compare verify still refuses to
    /// substitute a forward-mapped commit's bytes.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn jj_forward_maps_churned_write_coordinate() {
        let Some(bin) = jj_bin() else {
            eprintln!("skipping jj_forward_maps_churned_write_coordinate: jj not resolvable");
            return;
        };

        // A colocated jj repo (git + jj in one dir), so the worktree's git HEAD —
        // which `build_execution_pack` shells out to — tracks jj's post-rebase
        // state and the pack spans the forward-mapped commit.
        let dir = tempfile::tempdir().unwrap();
        let repo = dir.path().to_str().unwrap().to_string();
        init_repo(dir.path());
        write_file(dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
        let m = commit_all(dir.path(), "base");

        let cfg = dir.path().join("jj-config.toml");
        std::fs::write(
            &cfg,
            "ui.paginate = \"never\"\n[user]\nname = \"t\"\nemail = \"t@e.com\"\n",
        )
        .unwrap();
        jj_raw(&bin, &cfg, dir.path(), &["git", "init", "--colocate"]);

        // Parent commit P (the one we churn), then the write commit W whose change
        // matches `assistant_write` (a.txt patched, c.txt created).
        write_file(dir.path(), "p.txt", b"p\n");
        jj_raw(&bin, &cfg, dir.path(), &["commit", "-m", "P"]);
        write_file(dir.path(), "a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n");
        write_file(dir.path(), "c.txt", b"new file\n");
        jj_raw(&bin, &cfg, dir.path(), &["commit", "-m", "W"]);
        let w_old = jj_raw(
            &bin,
            &cfg,
            dir.path(),
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
        );
        let w_change = jj_raw(
            &bin,
            &cfg,
            dir.path(),
            &["log", "-r", "@-", "--no-graph", "-T", "change_id"],
        );

        // Reword P (@--): auto-rebase churns W's commit-id, preserving its
        // change-id; colocated jj advances git HEAD to the rebased W.
        jj_raw(
            &bin,
            &cfg,
            dir.path(),
            &["describe", "-r", "@--", "-m", "P reworded"],
        );
        let w_new = jj_raw(
            &bin,
            &cfg,
            dir.path(),
            &["log", "-r", "@-", "--no-graph", "-T", "commit_id"],
        );
        assert_ne!(
            w_old, w_new,
            "precondition: the rebase churned W's commit-id"
        );

        let drift = rendered(&[("file:a.txt", b"DRIFTED CONTENT THE COMMIT NEVER HAD\n")]);

        // Shared event sequence: the committed write (recording W's ORIGINAL id)
        // plus a read pinned to the post-write commit whose recorded bytes drift.
        async fn seed_events(db: &LocalDb, w_old: &str, drift: &str) {
            insert_event(db, "a-w1", "run", 1, 1, "assistant", &assistant_write("w1")).await;
            insert_event(
                db,
                "e-w1",
                "run",
                2,
                2,
                "tool_result",
                &write_result("w1", w_old),
            )
            .await;
            insert_event(
                db,
                "a-rd",
                "run",
                3,
                3,
                "assistant",
                &assistant_read("rd", &["file:a.txt"]),
            )
            .await;
            insert_event(
                db,
                "e-rd",
                "run",
                4,
                4,
                "tool_result",
                &read_result("rd", drift),
            )
            .await;
        }

        let jj = crate::jj::JjEnv::resolve(&bin, dir.path());

        // ---- jj path: forward-map the churned write to the current commit. ----
        let db_ok = migrated_test_db("archival-jj-forward-ok.db").await;
        seed_chain(&db_ok, &repo, &repo, Some(&m), Some(&m), false).await;
        seed_events(&db_ok, &w_old, &drift).await;
        let summary = archive_target(&db_ok, &repo, &repo, &["job".to_string()], Some(&jj))
            .await
            .unwrap();
        assert_eq!(summary.gitcoord_write, 1, "the write is git-addressed");
        assert!(
            summary.mismatch_fallback >= 1,
            "the drift read fails the verify"
        );

        let (commit, change_id) = event_coord(&db_ok, "a-w1").await;
        assert_eq!(
            commit.as_deref(),
            Some(w_new.as_str()),
            "content_commit is the forward-mapped (current) commit, not the churned-away one"
        );
        assert_eq!(
            change_id.as_deref(),
            Some(w_change.as_str()),
            "the stable change-id is persisted as provenance"
        );

        let oids = exec_pack_oids(&db_ok).await;
        assert!(
            oids.contains(&w_new),
            "the forward-mapped commit is inside the durable pack"
        );
        assert!(
            !oids.contains(&w_old),
            "the churned-away commit is excluded from the durable pack"
        );

        let recon = reconstruct_events(&db_ok, load_events(&db_ok).await).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        assert!(
            !by_id["a-w1"].data.contains(STUB_PREFIX),
            "the forward-mapped write reconstructs its committed diff, no stub"
        );
        // The drift read is preserved verbatim (zstd), never silently replaced by
        // the forward-mapped commit's bytes — the verify guardrail holds.
        assert_eq!(
            tool_result_of(by_id["e-rd"]),
            drift,
            "a drifted forward-mapped read keeps its recorded bytes, not the commit's"
        );
        let (rd_commit, _) = event_coord(&db_ok, "e-rd").await;
        assert!(rd_commit.is_none(), "the drifted read is not git-addressed");

        // ---- bare-git control: no forward-map, so the stale id is pinned. ----
        let db_ctrl = migrated_test_db("archival-jj-forward-ctrl.db").await;
        seed_chain(&db_ctrl, &repo, &repo, Some(&m), Some(&m), false).await;
        seed_events(&db_ctrl, &w_old, &drift).await;
        archive_target(&db_ctrl, &repo, &repo, &["job".to_string()], None)
            .await
            .unwrap();
        let (ctrl_commit, ctrl_change) = event_coord(&db_ctrl, "a-w1").await;
        assert_eq!(
            ctrl_commit.as_deref(),
            Some(w_old.as_str()),
            "without forward-mapping the coordinate pins the churned-away commit"
        );
        assert!(
            ctrl_change.is_none(),
            "no change-id is recorded on the plain-git path"
        );
    }

    /// CAIRN-1956: a production jj workspace is `.jj`-only (no `.git`), so the
    /// teardown packfile build — which shelled `git rev-parse HEAD` / `rev-list` /
    /// `pack-objects` in the worktree — failed with `bad object` and dropped the
    /// WHOLE execution to zstd (no gitcoord rows, no `execution_history`). The
    /// colocated keystone above masked this because it carries a `.git`. With the
    /// pack routed to the project repo (where `jj git export` lands the objects)
    /// and the tip resolved from jj's `@-`, a merged jj session archives: the
    /// write is git-addressed with its change-id, the durable pack holds the
    /// commit, and reconstruction renders the committed diff.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn jj_noncolocated_workspace_archives_via_project_repo() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping jj_noncolocated_workspace_archives_via_project_repo: jj not resolvable"
            );
            return;
        };

        let home = tempfile::tempdir().unwrap();
        let proj_dir = tempfile::tempdir().unwrap();
        let wts = tempfile::tempdir().unwrap();
        let proj = proj_dir.path().to_str().unwrap().to_string();

        init_repo(proj_dir.path());
        write_file(proj_dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
        let anchor = commit_all(proj_dir.path(), "base");

        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
        let ws = wts.path().join("job");
        crate::jj::add_workspace(&jj, &store, &ws, "agent/CAIRN-1-builder-0", "main", None)
            .unwrap();

        // The real production shape: a `.jj` dir and NO `.git`. This is exactly
        // where the old worktree-local git invocations had no repository.
        assert!(ws.join(".jj").exists());
        assert!(
            !ws.join(".git").exists(),
            "a jj workspace is non-colocated (no .git)"
        );

        // The write commit matching `assistant_write` (a.txt patched, c.txt added).
        std::fs::write(ws.join("a.txt"), "ALPHA\nbeta\ngamma\ndelta\nepsilon\n").unwrap();
        std::fs::write(ws.join("c.txt"), "new file\n").unwrap();
        crate::jj::seal(&jj, &ws, "W", None).unwrap();
        let w_full = crate::jj::head_commit(&jj, &ws).unwrap();

        let ws_str = ws.to_str().unwrap().to_string();
        let db = migrated_test_db("archival-jj-noncolocated.db").await;
        seed_chain(&db, &proj, &ws_str, Some(&anchor), Some(&anchor), false).await;
        insert_event(
            &db,
            "a-w1",
            "run",
            1,
            1,
            "assistant",
            &assistant_write("w1"),
        )
        .await;
        insert_event(
            &db,
            "e-w1",
            "run",
            2,
            2,
            "tool_result",
            &write_result("w1", &w_full),
        )
        .await;

        // The regression: before the fix this returned Err ("bad object") because
        // git ran in the `.jj`-only worktree. It must now succeed.
        let summary = archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
            .await
            .expect("archival succeeds for a non-colocated jj workspace");
        assert_eq!(
            summary.gitcoord_write, 1,
            "the write is git-addressed, not dropped to zstd"
        );

        let (commit, change_id) = event_coord(&db, "a-w1").await;
        assert_eq!(
            commit.as_deref(),
            Some(w_full.as_str()),
            "content_commit pins the write commit resolved through the project repo"
        );
        assert!(
            change_id.is_some(),
            "the durable jj change-id is recorded once the pack builds (CAIRN-1964 path)"
        );

        let oids = exec_pack_oids(&db).await;
        assert!(
            oids.contains(&w_full),
            "the write commit is inside the durable execution pack"
        );

        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        assert!(
            !by_id["a-w1"].data.contains(STUB_PREFIX),
            "reconstruction renders the committed diff, not a 'content no longer resolvable' stub"
        );
    }

    // ---------------------------------------------------------------------
    // CAIRN-1988: no-pack (empty-range) archival durability under git gc for
    // MERGED jj executions. The read path resolves purely by `content_commit`
    // against the project ODB; with a NULL `execution_history.pack` there is no
    // pack to fall back on, so reconstruction depends entirely on the git object
    // surviving the project repo's `git gc`. These tests lock that down.
    // ---------------------------------------------------------------------

    /// True when the execution's `execution_history` row stores a NULL `pack`
    /// (the empty-range classification — everything reachable from the tip is
    /// already durable on the default branch, so reconstruction is ODB-only).
    async fn exec_pack_is_null(db: &LocalDb) -> bool {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT pack FROM execution_history WHERE execution_id = 'exec'",
                        (),
                    )
                    .await?;
                let row = rows.next().await?.expect("execution_history row present");
                DbResult::Ok(row.opt_blob(0)?.is_none())
            })
        })
        .await
        .unwrap()
    }

    /// Run git in `dir` returning `(success, trimmed stdout)` WITHOUT panicking on
    /// a non-zero exit, so a boolean query (`merge-base --is-ancestor`, `cat-file
    /// -t`) is usable where `testutil::git` would abort the test on exit 1.
    fn git_check(dir: &Path, args: &[&str]) -> (bool, String) {
        let out = Command::new("git")
            .current_dir(dir)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .args(args)
            .output()
            .expect("spawn git");
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
        )
    }

    /// The dominant production case end to end: an empty-range (NULL `pack`)
    /// MERGED jj execution still reconstructs its read content AND its write diff
    /// from the project ODB ALONE after its `.jj` workspace is forgotten/removed
    /// AND the project repo is `git gc --prune=now`-ed. The store-owns-merge fold
    /// folds the seal onto the default branch, so the archival range is empty (no
    /// pack); the durable branch ref then keeps `content_commit` alive across gc.
    /// No predicate fix is required — this regression test is the deliverable, and
    /// the at-risk path (NULL pack + workspace-forget + gc) had zero coverage.
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn no_pack_merged_execution_reconstructs_after_workspace_forget_and_git_gc() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping no_pack_merged_execution_reconstructs_after_workspace_forget_and_git_gc: jj not resolvable"
            );
            return;
        };

        let home = tempfile::tempdir().unwrap();
        let proj_dir = tempfile::tempdir().unwrap();
        let wts = tempfile::tempdir().unwrap();
        let proj = proj_dir.path().to_str().unwrap().to_string();

        init_repo(proj_dir.path());
        write_file(proj_dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
        let anchor = commit_all(proj_dir.path(), "base");

        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
        let ws = wts.path().join("job");
        let branch = "agent/CAIRN-1-builder-0";
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();
        // True production shape: a `.jj` workspace, no `.git` in the worktree.
        assert!(ws.join(".jj").exists());
        assert!(
            !ws.join(".git").exists(),
            "a jj workspace is non-colocated (no .git)"
        );

        // The write commit matching `assistant_write` (a.txt patched, c.txt added).
        std::fs::write(ws.join("a.txt"), "ALPHA\nbeta\ngamma\ndelta\nepsilon\n").unwrap();
        std::fs::write(ws.join("c.txt"), "new file\n").unwrap();
        crate::jj::seal(&jj, &ws, "W", None).unwrap();
        let w = crate::jj::head_commit(&jj, &ws).unwrap();

        // Store-owns-merge fold: FF `main` to the child's seal and export it to the
        // project git, so `rev-list w --not anchor main` is empty → NULL pack.
        crate::jj::merge_into_bookmark(&jj, &store, "main", branch).unwrap();

        let ws_str = ws.to_str().unwrap().to_string();
        let db = migrated_test_db("archival-nopack-merged.db").await;
        seed_chain(&db, &proj, &ws_str, Some(&anchor), Some(&anchor), false).await;

        // A committed write, then a read of the committed file (git-addressed to w).
        let stored = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
        insert_event(
            &db,
            "a-w1",
            "run",
            1,
            1,
            "assistant",
            &assistant_write("w1"),
        )
        .await;
        insert_event(
            &db,
            "e-w1",
            "run",
            2,
            2,
            "tool_result",
            &write_result("w1", &w),
        )
        .await;
        insert_event(
            &db,
            "a-rd",
            "run",
            3,
            3,
            "assistant",
            &assistant_read("rd", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-rd",
            "run",
            4,
            4,
            "tool_result",
            &read_result("rd", &stored),
        )
        .await;

        let summary = archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
            .await
            .unwrap();

        // The NULL-pack precondition this test exists for: both events are git
        // coordinates, and `execution_history.pack` is NULL (ODB-only).
        assert_eq!(summary.gitcoord_read, 1, "the read is git-addressed");
        assert_eq!(summary.gitcoord_write, 1, "the write is git-addressed");
        assert!(
            exec_pack_is_null(&db).await,
            "an empty-range merged execution stores a NULL pack (ODB-only reconstruction)"
        );
        let (w_commit, _) = event_coord(&db, "a-w1").await;
        assert_eq!(
            w_commit.as_deref(),
            Some(w.as_str()),
            "the write pins the seal commit"
        );
        let (rd_commit, _) = event_coord(&db, "e-rd").await;
        assert_eq!(
            rd_commit.as_deref(),
            Some(w.as_str()),
            "the read pins the seal commit"
        );

        // Teardown the way a merged execution's worktree is reclaimed: forget the
        // workspace, remove it, and `git gc --prune=now` the project store.
        crate::jj::forget_workspace(&jj, &store, branch).unwrap();
        std::fs::remove_dir_all(&ws).unwrap();
        git(proj_dir.path(), &["gc", "--prune=now"]);

        // The crux: reconstruct from the project ODB ALONE (no pack, no workspace).
        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        assert!(
            !tool_result_of(by_id["e-rd"]).contains(STUB_PREFIX),
            "the read reconstructs from the post-gc ODB, not a 'no longer resolvable' stub"
        );
        assert_eq!(
            tool_result_of(by_id["e-rd"]),
            stored,
            "the read returns its exact original bytes after workspace-forget + git gc"
        );
        assert!(
            !by_id["a-w1"].data.contains(STUB_PREFIX),
            "the write diff still renders from the durable seal commit"
        );
    }

    /// Hardening for the non-obvious guarantee: a `content_commit` that churned to
    /// a HIDDEN commit-id AFTER teardown (the base advanced and the change
    /// auto-rebased, so the recorded commit is reachable from NO `refs/heads/*`)
    /// still reconstructs after `git gc --prune=now`, because jj pins it with a
    /// `refs/jj/keep/*` ref in the backing git repo. If this ever fails it is the
    /// early-warning that the NULL-pack read path needs a change-id fallback
    /// (resolve `content_change_id` → current commit via the still-present store).
    #[tokio::test]
    #[serial_test::serial(jj)]
    async fn no_pack_churned_content_commit_survives_git_gc_via_keep_refs() {
        let Some(bin) = jj_bin() else {
            eprintln!(
                "skipping no_pack_churned_content_commit_survives_git_gc_via_keep_refs: jj not resolvable"
            );
            return;
        };

        let home = tempfile::tempdir().unwrap();
        let proj_dir = tempfile::tempdir().unwrap();
        let wts = tempfile::tempdir().unwrap();
        let proj = proj_dir.path().to_str().unwrap().to_string();

        init_repo(proj_dir.path());
        write_file(proj_dir.path(), "a.txt", b"alpha\nbeta\ngamma\ndelta\n");
        let anchor = commit_all(proj_dir.path(), "base");

        let jj = crate::jj::JjEnv::resolve(&bin, home.path());
        let store = home.path().join("jj-stores").join("proj");
        crate::jj::ensure_project_store(&jj, &store, proj_dir.path()).unwrap();
        let ws = wts.path().join("job");
        let branch = "agent/CAIRN-1-builder-0";
        crate::jj::add_workspace(&jj, &store, &ws, branch, "main", None).unwrap();

        std::fs::write(ws.join("a.txt"), "ALPHA\nbeta\ngamma\ndelta\nepsilon\n").unwrap();
        std::fs::write(ws.join("c.txt"), "new file\n").unwrap();
        crate::jj::seal(&jj, &ws, "W", None).unwrap();
        let w = crate::jj::head_commit(&jj, &ws).unwrap();

        // Fold main → w so the archival range is empty (NULL pack).
        crate::jj::merge_into_bookmark(&jj, &store, "main", branch).unwrap();

        let ws_str = ws.to_str().unwrap().to_string();
        let db = migrated_test_db("archival-nopack-churn.db").await;
        seed_chain(&db, &proj, &ws_str, Some(&anchor), Some(&anchor), false).await;
        let stored = rendered(&[("file:a.txt", b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n")]);
        insert_event(
            &db,
            "a-w1",
            "run",
            1,
            1,
            "assistant",
            &assistant_write("w1"),
        )
        .await;
        insert_event(
            &db,
            "e-w1",
            "run",
            2,
            2,
            "tool_result",
            &write_result("w1", &w),
        )
        .await;
        insert_event(
            &db,
            "a-rd",
            "run",
            3,
            3,
            "assistant",
            &assistant_read("rd", &["file:a.txt"]),
        )
        .await;
        insert_event(
            &db,
            "e-rd",
            "run",
            4,
            4,
            "tool_result",
            &read_result("rd", &stored),
        )
        .await;

        let summary = archive_target(&db, &ws_str, &proj, &["job".to_string()], Some(&jj))
            .await
            .unwrap();
        assert_eq!(summary.gitcoord_read, 1);
        assert_eq!(summary.gitcoord_write, 1);
        assert!(exec_pack_is_null(&db).await, "NULL pack precondition");
        let (_, change_id) = event_coord(&db, "a-w1").await;
        let change_id = change_id.expect("the durable jj change-id is recorded");

        // Teardown: forget + remove the workspace.
        crate::jj::forget_workspace(&jj, &store, branch).unwrap();
        std::fs::remove_dir_all(&ws).unwrap();

        // Churn AFTER teardown: reword the write change in the store so its
        // commit-id is rewritten (w → w_new). Both `main` and the child bookmark
        // advance to w_new, so the recorded `content_commit` (w) is now reachable
        // from no branch — exactly the worst case the worry named.
        let cfg = home.path().join("probe-config.toml");
        std::fs::write(
            &cfg,
            "ui.paginate = \"never\"\n[user]\nname = \"t\"\nemail = \"t@e.com\"\n",
        )
        .unwrap();
        jj_raw(
            &bin,
            &cfg,
            &store,
            &[
                "describe",
                "-r",
                &change_id,
                "-m",
                "W reworded",
                "--ignore-working-copy",
            ],
        );
        let w_new = jj_raw(
            &bin,
            &cfg,
            &store,
            &[
                "log",
                "-r",
                &change_id,
                "--no-graph",
                "-T",
                "commit_id",
                "--ignore-working-copy",
            ],
        );
        assert_ne!(
            w, w_new,
            "precondition: the post-teardown churn rewrote the write commit-id"
        );
        // Export the rewritten bookmarks so the project git heads advance off w.
        jj_raw(
            &bin,
            &cfg,
            &store,
            &["git", "export", "--ignore-working-copy"],
        );
        let (hidden, _) = git_check(
            proj_dir.path(),
            &["merge-base", "--is-ancestor", &w, "main"],
        );
        assert!(
            !hidden,
            "precondition: the recorded content_commit is hidden (no longer on any branch)"
        );

        // gc the project store: only jj's keep-ref can save the hidden commit now.
        git(proj_dir.path(), &["gc", "--prune=now"]);
        let (alive, kind) = git_check(proj_dir.path(), &["cat-file", "-t", &w]);
        assert!(
            alive && kind == "commit",
            "jj's keep-refs pin the hidden content_commit across git gc (got alive={alive} kind={kind:?})"
        );

        // The durability guarantee: reconstruction still returns the original bytes
        // from the ODB alone, with no pack and the commit reachable from no branch.
        let recon = reconstruct_events(&db, load_events(&db).await).await;
        let by_id: HashMap<&str, &Event> = recon.iter().map(|e| (e.id.as_str(), e)).collect();
        assert_eq!(
            tool_result_of(by_id["e-rd"]),
            stored,
            "a churned-then-gc'd content_commit still reconstructs its original bytes"
        );
        assert!(
            !by_id["a-w1"].data.contains(STUB_PREFIX),
            "the write diff renders from the keep-ref-pinned hidden commit"
        );
    }
}
