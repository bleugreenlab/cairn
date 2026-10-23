//! Transparent reconstruction of archived events on the read path.
//!
//! Events are recorded `full` on the hot path and rewritten to git coordinates
//! (with a zstd backstop) at issue teardown (and by the recurring sweep), the
//! immutability boundary. This
//! module restores them on read so every event consumer — transcripts, search,
//! the frontend — sees the original rendered content without knowing archival
//! exists. [`reconstruct_events`] is strict identity when every row is `full`
//! (the common case: ~90% of reads are live monitoring), so the hot path pays
//! only one cheap scan.
//!
//! The column-level contract — which columns each archived shape sets, and how a
//! row's columns are classified back into a shape (including the `data_blob`
//! read-vs-write discriminator) — lives in [`super::encoding`]. This module asks
//! [`super::encoding::decode`] for the shape and acts on it:
//! - `Full`: identity.
//! - `Zstd`: decompress `data_blob` with its codec marker, restoring `data`.
//! - `GitcoordRead`: for each target in `tool_input.paths`, resolve the file's
//!   blob at `content_commit` through the layered [`ObjectStore`], render it
//!   through the live `render_*_from_bytes` pipeline (no frozen copy), and
//!   reassemble the batch under the live renderer's enriched headers. If a target
//!   degrades to a stub, its header is `=== <uri> [error] ===`; degraded stubs are
//!   explicitly exempt from the byte-exact drift comparison.
//! - `GitcoordWrite`: decompress the payload-stripped assistant remainder from
//!   `data_blob`, regenerate the batch's unified diff from `content_commit` vs
//!   its parent ([`super::diff`]), and re-inject each file's section into the
//!   matching `changes[*].payload` so the transcript shows the committed change
//!   where the dropped payload was. The paired short write `tool_result` is a
//!   plain `Zstd` row and round-trips verbatim.
//!
//! Any unresolvable coordinate — a dropped pack, a missing object, drift —
//! degrades to a labeled coordinate stub. Reconstruction never errors out of the
//! event list.
//!
//! # The quarantine gate
//!
//! This module decides what `data` an event reader sees, which makes it the one
//! seam where a record known to carry a disclosed credential can be withheld
//! (CAIRN-3828). The check runs *after* reconstruction, on every return path
//! including the identity fast path — see [`withheld`] for why running it first
//! would silently fail on exactly the archived events that need it most.
//!
//! Withholding substitutes a labeled stub, the same shape an unresolvable
//! coordinate degrades to, rather than dropping the event: a hole in a
//! transcript that reads as a rendering bug teaches an operator nothing. The row
//! on disk is never rewritten — it is the record of what happened, and repairing
//! it is an explicit operator action, not something a read performs.
//! See `cairn_core::security::remediation`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use cairn_common::read::ReadSegment;
use cairn_common::token_meter::{Cut, TokenMeter};
use serde_json::Value;

struct LegacyCharMeter;
impl TokenMeter for LegacyCharMeter {
    fn count(&self, text: &str) -> u32 {
        text.len().try_into().unwrap_or(u32::MAX)
    }
    fn cut(&self, text: &str, budget: u32) -> Cut {
        let mut byte_offset = (budget as usize).min(text.len());
        while !text.is_char_boundary(byte_offset) {
            byte_offset -= 1;
        }
        Cut {
            byte_offset,
            tokens: byte_offset as u32,
        }
    }
}
static LEGACY_CHAR_METER: LegacyCharMeter = LegacyCharMeter;
use sha2::{Digest, Sha256};

use super::encoding::{
    decode, init_placeholder, ArchivedShape, EventColumns, ARCHIVED_SYSTEM_INIT, INIT_TOOLS_TAG,
};
use super::{decompress, diff, ObjectStore, CODEC_NONE, CODEC_ZSTD_V1};
use crate::models::Event;
use crate::storage::content_store::{content_hash, unframe_pack, ContentStore};
use crate::storage::render::ArchivedFileRenderer;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Prefix of the stub substituted for an unresolvable coordinate.
pub const STUB_PREFIX: &str = "content no longer resolvable";

/// Reconstruct any archived events in `events` back to their full rendered form.
///
/// Identity (a single scan, no allocation) when every row is `full`. See the
/// module docs for per-mode behavior.
pub async fn reconstruct_events(db: &LocalDb, events: Vec<Event>) -> Vec<Event> {
    if !events.iter().any(is_archived) {
        return withheld(events);
    }
    let coords = load_coords(db, &events).await;
    withheld(finish(coords, events))
}

/// What reconstruction made of one event: whether the `data` it now carries is
/// the content an agent saw, and if not, why not.
///
/// The read path never needs this — a transcript renders a degraded stub the
/// same way it renders content — but a consumer that hands events onward as a
/// record (the project export) must be able to say which rows are not, rather
/// than guess from the text: a stub prefix also appears verbatim in any
/// transcript that read this file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconstruction {
    /// Stored `full`: `data` is the recorded content, untouched.
    Stored,
    /// Archived, and restored completely from its compressed form, its
    /// content-addressed blobs, or its git coordinate.
    Restored,
    /// Archived, and at least one part of it could not be resolved (a dropped
    /// pack, a commit or blob the store no longer has): `data` carries the rest,
    /// with each unresolvable part replaced by a [`STUB_PREFIX`] stub.
    Degraded,
    /// Archived, but the stored form itself could not be decoded (an invalid
    /// column shape, corrupt compression, a stub that is not JSON): `data` is
    /// still the stored form, not content.
    Undecodable,
    /// Withheld by the disclosure quarantine: `data` is the withholding notice,
    /// and the stored form must not be served either.
    Withheld,
}

impl Reconstruction {
    /// Whether `data` is anything less than the complete content an agent saw.
    pub fn is_unresolved(self) -> bool {
        matches!(self, Reconstruction::Degraded | Reconstruction::Undecodable)
    }
}

/// [`reconstruct_events`], also reporting what reconstruction made of each
/// event (index-aligned with the input). Same reader, same quarantine gate, run
/// last; the only difference is that the outcome each shape handler already
/// knows is returned instead of dropped.
pub async fn reconstruct_events_reporting(
    db: &LocalDb,
    events: Vec<Event>,
) -> Vec<(Event, Reconstruction)> {
    let reported = if events.iter().any(is_archived) {
        let coords = load_coords(db, &events).await;
        finish_reporting(coords, events)
    } else {
        events
            .into_iter()
            .map(|event| (event, Reconstruction::Stored))
            .collect()
    };
    let (mut events, outcomes): (Vec<Event>, Vec<Reconstruction>) = reported.into_iter().unzip();
    // The quarantine gate runs last here exactly as in `withheld`, for the same
    // reason: reconstruction regenerates `data` and would overwrite a stub laid
    // down before it.
    let withheld = crate::storage::quarantine::withhold_quarantined_reporting(&mut events);
    events
        .into_iter()
        .zip(outcomes)
        .zip(withheld)
        .map(|((event, outcome), incident)| match incident {
            Some(_) => (event, Reconstruction::Withheld),
            None => (event, outcome),
        })
        .collect()
}

/// Gather every coordinate, pack, and blob a batch's archived events need.
///
/// Two phases so the `ObjectStore` (which holds a non-Send `Rc`) is never
/// alive across an await: gather all coordinates and pack bytes via async DB
/// reads under one read connection first, then build the layered stores and
/// reconstruct synchronously.
async fn load_coords(db: &LocalDb, events: &[Event]) -> Coords {
    let run_ids = gitcoord_run_ids(events);
    let hashes = blobbed_hashes(events);
    // A team replica carries a content store; the private DB returns `None`. When
    // present, an offloaded pack/blob whose bytes are absent locally is fetched
    // from the shared per-team store (the store read-through caches into the
    // private `cas_cache`, so repeat reconstructs are offline). The fetch happens
    // in this async phase, before the non-Send `ObjectStore` is built.
    let store = db.team_id().map(|_| db.content_store().clone());
    let private_route_db = db.private_route_db().cloned();
    db.read(move |conn| {
        let private_route_db = private_route_db.clone();
        Box::pin(async move {
            DbResult::Ok(
                Coords::load(
                    conn,
                    &run_ids,
                    &hashes,
                    store.as_deref(),
                    private_route_db.as_deref(),
                )
                .await,
            )
        })
    })
    .await
    .unwrap_or_default()
}

/// Connection-scoped reconstruction for callers that already hold a read
/// connection (e.g. the transcript resource), avoiding a nested pool checkout.
pub async fn reconstruct_events_with_conn_and_routes(
    conn: &turso::Connection,
    events: Vec<Event>,
    store: Option<&dyn ContentStore>,
    private_route_db: Option<&LocalDb>,
) -> Vec<Event> {
    if !events.iter().any(is_archived) {
        return withheld(events);
    }
    let run_ids = gitcoord_run_ids(&events);
    let hashes = blobbed_hashes(&events);
    let coords = Coords::load(conn, &run_ids, &hashes, store, private_route_db).await;
    withheld(finish(coords, events))
}

/// Apply the disclosure quarantine (CAIRN-3828) to fully reconstructed events.
///
/// Last, and that ordering is the whole correctness argument. Withholding
/// rewrites `data`, while reconstruction *regenerates* `data` from the
/// compressed blob or the git coordinate and pays no attention to what was
/// there before. A gate that ran first would therefore have its stub
/// overwritten for precisely the archived events whose stored bytes carry the
/// credential — the gate would appear to work on plain rows and fail silently
/// on the ones already moved to cold storage.
///
/// Every return path funnels through here, including the identity fast path for
/// an all-`full` batch, so there is no way to leave this function reachable
/// only on one branch.
fn withheld(mut events: Vec<Event>) -> Vec<Event> {
    crate::storage::quarantine::withhold_quarantined(&mut events);
    events
}

/// Build the layered stores from gathered coordinates and reconstruct each
/// archived event in place. Synchronous: no non-Send store crosses an await.
fn finish(coords: Coords, mut events: Vec<Event>) -> Vec<Event> {
    let ctx = coords.into_ctx();
    for event in events.iter_mut() {
        if is_archived(event) {
            reconstruct_one(event, &ctx);
        }
    }
    events
}

/// [`finish`], keeping each event's outcome.
fn finish_reporting(coords: Coords, events: Vec<Event>) -> Vec<(Event, Reconstruction)> {
    let ctx = coords.into_ctx();
    events
        .into_iter()
        .map(|mut event| {
            let outcome = if is_archived(&event) {
                reconstruct_one(&mut event, &ctx)
            } else {
                Reconstruction::Stored
            };
            (event, outcome)
        })
        .collect()
}

fn is_archived(event: &Event) -> bool {
    matches!(
        event.storage_mode.as_deref(),
        Some("zstd") | Some("gitcoord")
    )
}

/// Distinct run ids of the gitcoord events (zstd events need no coordinates).
fn gitcoord_run_ids(events: &[Event]) -> Vec<String> {
    let set: HashSet<&str> = events
        .iter()
        .filter(|event| event.storage_mode.as_deref() == Some("gitcoord"))
        .map(|event| event.run_id.as_str())
        .collect();
    set.into_iter().map(str::to_string).collect()
}

/// Whether an event is a hash-addressed `blobbed` row: a `gitcoord` row with no
/// commit (see [`super::encoding`]). Covers both the system-prompt and
/// system-init consumers.
fn is_blobbed(event: &Event) -> bool {
    event.storage_mode.as_deref() == Some("gitcoord") && event.content_commit.is_none()
}

/// Distinct content-addressed blob hashes referenced by the archived `blobbed`
/// rows in `events`, so they load once up front. Collects both a system prompt's
/// `segments[].hash` and a system init's `skeleton`/`toolset` hashes — harmless
/// to scan for all keys regardless of which marker a row carries.
fn blobbed_hashes(events: &[Event]) -> Vec<String> {
    let mut set: HashSet<String> = HashSet::new();
    for event in events.iter().filter(|event| is_blobbed(event)) {
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            continue;
        };
        if let Some(segments) = value.get("segments").and_then(|s| s.as_array()) {
            for seg in segments {
                if let Some(hash) = seg.get("hash").and_then(|v| v.as_str()) {
                    set.insert(hash.to_string());
                }
            }
        }
        for key in ["skeleton", "toolset"] {
            if let Some(hash) = value.get(key).and_then(|v| v.as_str()) {
                set.insert(hash.to_string());
            }
        }
    }
    set.into_iter().collect()
}

/// Resolved coordinates for one batch's archived events, gathered entirely
/// through async DB reads so no non-Send [`ObjectStore`] is alive across an
/// await. [`Coords::into_ctx`] turns this into the synchronous reconstruction
/// context.
#[derive(Default)]
struct Coords {
    /// run_id -> (execution_id, project repo path)
    run_meta: HashMap<String, RunMeta>,
    /// execution_id -> repo path + optional range pack bytes
    packs: HashMap<String, ExecPack>,
    /// content hash -> decompressed system-prompt segment text. Missing entries
    /// (dropped blob) degrade to a labeled stub at reconstruction.
    blobs: HashMap<String, String>,
}

struct RunMeta {
    execution_id: String,
    repo_path: PathBuf,
}

struct ExecPack {
    repo_path: PathBuf,
    /// `Some((pack, idx))` when the execution has a non-empty range pack; `None`
    /// for an empty range (or no `execution_history` row), where the layered
    /// lookup falls through entirely to the project repo's ODB.
    pack: Option<(Vec<u8>, Vec<u8>)>,
}

/// Per-execution archival coordinates resolved to layered stores for one call.
///
/// Events in one batch may span many executions; each execution's store is built
/// once. `zstd` events need none of this.
struct ReconstructCtx {
    run_meta: HashMap<String, RunMeta>,
    /// execution_id -> layered store (None when the store could not be built;
    /// gitcoord events for it then degrade to stubs)
    stores: HashMap<String, Option<ObjectStore>>,
    /// content hash -> decompressed system-prompt segment text.
    blobs: HashMap<String, String>,
}

impl Coords {
    async fn load(
        conn: &turso::Connection,
        run_ids: &[String],
        hashes: &[String],
        store: Option<&dyn ContentStore>,
        private_route_db: Option<&LocalDb>,
    ) -> Coords {
        let mut run_meta: HashMap<String, RunMeta> = HashMap::new();

        for run_id in run_ids {
            if let Some(meta) = load_run_meta(conn, run_id, private_route_db).await {
                run_meta.insert(run_id.clone(), meta);
            }
        }

        let exec_ids: HashSet<String> = run_meta
            .values()
            .map(|meta| meta.execution_id.clone())
            .collect();
        let mut packs: HashMap<String, ExecPack> = HashMap::new();
        for exec_id in exec_ids {
            let Some(repo_path) = run_meta
                .values()
                .find(|meta| meta.execution_id == exec_id)
                .map(|meta| meta.repo_path.clone())
            else {
                continue;
            };
            let pack = load_pack_bytes(conn, &exec_id, store).await;
            packs.insert(exec_id, ExecPack { repo_path, pack });
        }

        let mut blobs: HashMap<String, String> = HashMap::new();
        for hash in hashes {
            if let Some(text) = load_blob(conn, hash, store).await {
                blobs.insert(hash.clone(), text);
            }
        }

        Coords {
            run_meta,
            packs,
            blobs,
        }
    }

    /// Build the layered stores synchronously (no awaits, so no non-Send value
    /// crosses a suspend point).
    fn into_ctx(self) -> ReconstructCtx {
        let stores = self
            .packs
            .into_iter()
            .map(|(exec_id, ep)| (exec_id, ObjectStore::new(&ep.repo_path, ep.pack).ok()))
            .collect();
        ReconstructCtx {
            run_meta: self.run_meta,
            stores,
            blobs: self.blobs,
        }
    }
}

impl ReconstructCtx {
    fn store_for(&self, run_id: &str) -> Option<&ObjectStore> {
        let meta = self.run_meta.get(run_id)?;
        self.stores.get(&meta.execution_id)?.as_ref()
    }
}

async fn load_run_meta(
    conn: &turso::Connection,
    run_id: &str,
    private_route_db: Option<&LocalDb>,
) -> Option<RunMeta> {
    let mut rows = conn
        .query(
            "SELECT j.execution_id, p.id, p.key, p.repo_path
             FROM runs r
             JOIN jobs j ON r.job_id = j.id
             JOIN projects p ON j.project_id = p.id
             WHERE r.id = ?1
             LIMIT 1",
            (run_id,),
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    let execution_id: Option<String> = row.opt_text(0).ok()?;
    let project_id: String = row.text(1).ok()?;
    let project_key: String = row.text(2).ok()?;
    let synced_repo_path: String = row.text(3).ok()?;
    let repo_path = match cairn_common::ids::parse_route_scope(&project_id) {
        Ok(cairn_common::ids::RouteScope::Team(_)) => {
            load_private_local_repo_path(private_route_db?, &project_key)
                .await
                .unwrap_or_default()
        }
        _ => synced_repo_path,
    };
    Some(RunMeta {
        execution_id: execution_id?,
        repo_path: PathBuf::from(repo_path),
    })
}

async fn load_private_local_repo_path(private_db: &LocalDb, project_key: &str) -> Option<String> {
    let key = cairn_common::uri::canonical_project(project_key);
    private_db
        .query_opt_text(
            "SELECT local_repo_path FROM project_routes WHERE project_key = ?1",
            (key,),
        )
        .await
        .ok()
        .flatten()
}

/// Load an execution's range pack bytes, returning `None` for an empty range
/// (NULL pack columns and no `pack_hash`) or a missing `execution_history` row.
///
/// Local run: `pack`/`pack_idx` are inline. Team run: those columns are NULL and
/// `pack_hash` points at the framed pack object in the content store, fetched
/// here by hash and split back into `(pack, idx)`. A missing pointer, absent
/// store, fetch error, or malformed object yields `None` (degrade to stub).
async fn load_pack_bytes(
    conn: &turso::Connection,
    execution_id: &str,
    store: Option<&dyn ContentStore>,
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut rows = conn
        .query(
            "SELECT pack, pack_idx, pack_hash FROM execution_history WHERE execution_id = ?1 LIMIT 1",
            (execution_id,),
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    if let (Some(pack), Some(idx)) = (row.opt_blob(0).ok()?, row.opt_blob(1).ok()?) {
        return Some((pack, idx));
    }
    let pack_hash = row.opt_text(2).ok()??;
    let bytes = store?.get(&pack_hash).await.ok()??;
    // Content-addressed integrity: the fetched object MUST hash to the pointer
    // that named it. A mismatch (corrupt/stale object, broker or backend bug,
    // tampering) is treated as absent so reconstruction degrades to a stub rather
    // than restoring from bytes that are not the object `pack_hash` names.
    if content_hash(&bytes) != pack_hash {
        log::warn!(
            "content-store pack object for {pack_hash} failed integrity check; treating as absent"
        );
        return None;
    }
    unframe_pack(&bytes)
}

/// Restore one content-addressed segment blob's text. `None` (missing
/// row/object, integrity/utf-8 failure) leaves the segment to degrade to a
/// labeled stub.
///
/// The two scopes store the blob DIFFERENTLY under the same hash. Team conn
/// (`store` present): the `archival_blobs` table is private-only and absent here,
/// so the bytes are fetched from the content store — where they are stored
/// UNCOMPRESSED (their sha256 IS the key, so a fetch can verify integrity), hence
/// used directly after the hash check, no decompress. Private conn (`store` is
/// `None`): the bytes come from `archival_blobs`, which holds the
/// zstd-COMPRESSED form, so the local path decompresses.
async fn load_blob(
    conn: &turso::Connection,
    hash: &str,
    store: Option<&dyn ContentStore>,
) -> Option<String> {
    // A segment blob is addressed by its own hash and read only here, so this is
    // its quarantine gate. It needs one of its own rather than inheriting the
    // event's: a blob is shared by every event whose segment hashes the same, so
    // a credential in a system prompt reaches transcripts whose own rows are
    // clean (CAIRN-3828).
    if let Some(incident) = crate::storage::quarantine::quarantine()
        .snapshot()
        .withheld_by(crate::storage::quarantine::ARCHIVAL_BLOB_SINK, hash)
    {
        return Some(crate::storage::quarantine::withheld_notice(incident));
    }
    match store {
        // Team conn: the store object is the UNCOMPRESSED segment bytes, keyed by
        // their sha256. Verify the fetched bytes hash to the requested key before
        // trusting them (content-addressed integrity over an untrusted network); a
        // mismatch is treated as absent and degrades to a stub.
        Some(store) => {
            let bytes = store.get(hash).await.ok()??;
            if content_hash(&bytes) != hash {
                log::warn!(
                    "content-store blob object for {hash} failed integrity check; treating as absent"
                );
                return None;
            }
            String::from_utf8(bytes).ok()
        }
        // Private conn: `archival_blobs` holds the zstd-compressed bytes.
        None => {
            let content = load_blob_local(conn, hash).await?;
            let bytes = decompress(CODEC_ZSTD_V1, &content).ok()?;
            String::from_utf8(bytes).ok()
        }
    }
}

/// Read one content-addressed blob's compressed bytes from the private
/// `archival_blobs` table.
async fn load_blob_local(conn: &turso::Connection, hash: &str) -> Option<Vec<u8>> {
    let mut rows = conn
        .query(
            "SELECT content FROM archival_blobs WHERE hash = ?1 LIMIT 1",
            (hash,),
        )
        .await
        .ok()?;
    let row = rows.next().await.ok()??;
    row.opt_blob(0).ok()?
}

fn reconstruct_one(event: &mut Event, ctx: &ReconstructCtx) -> Reconstruction {
    let shape = match decode(&columns_of(event)) {
        Ok(shape) => shape,
        Err(error) => {
            log::warn!(
                "archived event {} has invalid column shape: {error}",
                event.id
            );
            return Reconstruction::Undecodable;
        }
    };
    match shape {
        ArchivedShape::Full { .. } => Reconstruction::Stored,
        ArchivedShape::Zstd { .. } => reconstruct_zstd(event),
        ArchivedShape::GitcoordRead { commit, .. } => {
            reconstruct_gitcoord_read(event, &commit, ctx.store_for(&event.run_id))
        }
        ArchivedShape::GitcoordWrite { commit, .. } => {
            reconstruct_gitcoord_write(event, &commit, ctx.store_for(&event.run_id))
        }
        ArchivedShape::HybridRead { commit, .. } => {
            reconstruct_hybrid_read(event, &commit, ctx.store_for(&event.run_id))
        }
        ArchivedShape::Blobbed { ref data, .. } => {
            // The `archived` marker inside the stub selects which reassembly the
            // shared blob mechanism applies.
            if blobbed_marker(data).as_deref() == Some(ARCHIVED_SYSTEM_INIT) {
                reconstruct_system_init(event, &ctx.blobs)
            } else {
                reconstruct_system_prompt(event, &ctx.blobs)
            }
        }
    }
}

/// Read the `archived` discriminator from a `blobbed` stub's `data`.
fn blobbed_marker(data: &str) -> Option<String> {
    serde_json::from_str::<Value>(data)
        .ok()?
        .get("archived")?
        .as_str()
        .map(str::to_string)
}

/// Restore an archived `system:prompt` event: resolve each segment (a content
/// hash into `archival_blobs`, or an inlined dynamic span), concatenate in order
/// into the full prompt, and put it back under `content`. A segment whose blob is
/// missing degrades to a labeled stub in place, so the rest still reconstructs.
fn reconstruct_system_prompt(event: &mut Event, blobs: &HashMap<String, String>) -> Reconstruction {
    let mut value: Value = match serde_json::from_str(&event.data) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("system prompt {} data is not json: {error}", event.id);
            return Reconstruction::Undecodable;
        }
    };
    let Some(segments) = value.get("segments").and_then(|s| s.as_array()) else {
        log::warn!("system prompt {} has no segments", event.id);
        return Reconstruction::Undecodable;
    };

    let mut full = String::new();
    let mut outcome = Reconstruction::Restored;
    for seg in segments {
        if let Some(inline) = seg.get("inline").and_then(|v| v.as_str()) {
            full.push_str(inline);
        } else if let Some(hash) = seg.get("hash").and_then(|v| v.as_str()) {
            match blobs.get(hash) {
                Some(text) => full.push_str(text),
                None => {
                    let kind = seg
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .unwrap_or("segment");
                    full.push_str(&stub(hash, kind));
                    outcome = Reconstruction::Degraded;
                }
            }
        }
    }

    if let Value::Object(ref mut map) = value {
        map.insert("content".to_string(), Value::String(full));
        map.remove("archived");
        map.remove("segments");
    }
    match serde_json::to_string(&value) {
        Ok(serialized) => {
            event.data = serialized;
            outcome
        }
        Err(error) => {
            log::warn!(
                "system prompt {} could not be re-serialized: {error}",
                event.id
            );
            Reconstruction::Undecodable
        }
    }
}

/// Restore an archived `system:init` event from its content-addressed skeleton
/// plus the inlined varying fields. A missing skeleton blob (or any structural
/// failure) degrades to a labeled stub so the transcript stays valid JSON; the
/// rest of the row still reconstructs when only the tool-set blob is missing.
fn reconstruct_system_init(event: &mut Event, blobs: &HashMap<String, String>) -> Reconstruction {
    // `reassemble_system_init` substitutes a stub array for a missing tool-set
    // blob and still succeeds, so whether it did is read off the stub up front.
    let toolset_missing = serde_json::from_str::<Value>(&event.data)
        .ok()
        .and_then(|value| {
            value
                .get("toolset")
                .and_then(|hash| hash.as_str())
                .map(|hash| !blobs.contains_key(hash))
        })
        .unwrap_or(false);
    match reassemble_system_init(&event.data, blobs) {
        Some(data) => {
            event.data = data;
            if toolset_missing {
                Reconstruction::Degraded
            } else {
                Reconstruction::Restored
            }
        }
        None => {
            let hash = serde_json::from_str::<Value>(&event.data)
                .ok()
                .and_then(|v| {
                    v.get("skeleton")
                        .and_then(|s| s.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_default();
            event.data = serde_json::json!({
                "eventType": event.event_type,
                "archived": ARCHIVED_SYSTEM_INIT,
                "content": stub(&hash, "skeleton"),
            })
            .to_string();
            Reconstruction::Degraded
        }
    }
}

/// Reassemble a `system:init` event's original `data` from its stub and the
/// loaded blob texts: substitute each placeholder in the skeleton with its inline
/// value, rebuilding the tool-set array in its original (shuffled) order from the
/// deduped sorted set and the stored permutation. Returns `None` on a structural
/// problem or a missing skeleton blob (the caller degrades). A missing tool-set
/// blob substitutes a labeled stub array so the rest still reconstructs. Shared
/// with the writer's verify-then-discard so the two can never drift.
pub fn reassemble_system_init(stub_data: &str, blobs: &HashMap<String, String>) -> Option<String> {
    let value: Value = serde_json::from_str(stub_data).ok()?;
    let obj = value.as_object()?;
    let skeleton_hash = obj.get("skeleton")?.as_str()?;
    let skeleton = blobs.get(skeleton_hash)?;
    let mut out = skeleton.clone();

    if let Some(vars) = obj.get("vars").and_then(|v| v.as_object()) {
        for (tag, val) in vars {
            let serialized = serde_json::to_string(val).ok()?;
            out = out.replace(&init_placeholder(tag), &serialized);
        }
    }

    if let Some(toolset_hash) = obj.get("toolset").and_then(|v| v.as_str()) {
        let order: Vec<usize> = obj
            .get("order")?
            .as_array()?
            .iter()
            .map(|v| v.as_u64().map(|n| n as usize))
            .collect::<Option<_>>()?;
        let tools_text = match blobs.get(toolset_hash) {
            Some(sorted_json) => {
                let sorted: Vec<String> = serde_json::from_str(sorted_json).ok()?;
                let original = order
                    .iter()
                    .map(|&i| sorted.get(i))
                    .collect::<Option<Vec<&String>>>()?;
                serde_json::to_string(&original).ok()?
            }
            None => serde_json::to_string(&[stub(toolset_hash, "toolset")]).ok()?,
        };
        out = out.replace(&init_placeholder(INIT_TOOLS_TAG), &tools_text);
    }

    Some(out)
}

/// Snapshot an event's archival columns for [`decode`].
fn columns_of(event: &Event) -> EventColumns {
    EventColumns {
        storage_mode: event.storage_mode.clone(),
        content_commit: event.content_commit.clone(),
        content_render_sha: event.content_render_sha.clone(),
        data: event.data.clone(),
        data_blob: event.data_blob.clone(),
        codec: event.codec.clone(),
    }
}

fn reconstruct_zstd(event: &mut Event) -> Reconstruction {
    let codec = event.codec.as_deref().unwrap_or(CODEC_NONE);
    let Some(blob) = event.data_blob.as_deref() else {
        log::warn!("zstd archived event {} has no data_blob", event.id);
        return Reconstruction::Undecodable;
    };
    match decompress(codec, blob) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => {
                event.data = text;
                Reconstruction::Restored
            }
            Err(error) => {
                log::warn!(
                    "zstd archived event {} data_blob is not valid utf-8: {error}",
                    event.id
                );
                Reconstruction::Undecodable
            }
        },
        Err(error) => {
            log::warn!(
                "failed to decompress zstd archived event {}: {error}",
                event.id
            );
            Reconstruction::Undecodable
        }
    }
}

fn reconstruct_gitcoord_read(
    event: &mut Event,
    commit_hex: &str,
    store: Option<&ObjectStore>,
) -> Reconstruction {
    let mut data: Value = match serde_json::from_str(&event.data) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("gitcoord read {} data is not json: {error}", event.id);
            return Reconstruction::Undecodable;
        }
    };

    let paths = data
        .get("toolInput")
        .and_then(|input| input.get("paths"))
        .and_then(|paths| paths.as_array())
        .map(|array| {
            array
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect::<Vec<_>>()
        });

    let (rendered, resolved) = match paths {
        Some(paths) => reconstruct_read_resolving(commit_hex, &paths, store),
        None => {
            log::warn!("gitcoord read {} has no tool_input.paths", event.id);
            (stub(commit_hex, "<read>"), false)
        }
    };

    // Drift tripwire: the verify-then-discard rewrite recorded the sha of the
    // exact bytes the agent saw. The shared composition is now a stability
    // contract — a future view-layer change would silently rewrite reconstructed
    // history — so a mismatch here means the live/archival read composition has
    // diverged since archival. We still inject `rendered`: it is the best
    // available reconstruction. A degraded (stub) reconstruction legitimately
    // differs and already logged its own warning, so skip the compare there.
    if !rendered.contains(STUB_PREFIX) {
        if let Some(expected) = event.content_render_sha.as_deref() {
            let actual = format!("{:x}", Sha256::digest(rendered.as_bytes()));
            if actual != expected {
                log::warn!(
                    "gitcoord read {} reconstruction sha mismatch (recorded {expected}, got {actual}); \
                     the shared read composition may have diverged since archival",
                    event.id
                );
            }
        }
    }

    if let Value::Object(ref mut map) = data {
        map.insert("toolResult".to_string(), Value::String(rendered));
    }
    match serde_json::to_string(&data) {
        Ok(serialized) => {
            event.data = serialized;
            resolved_outcome(resolved)
        }
        Err(error) => {
            log::warn!(
                "gitcoord read {} could not be re-serialized: {error}",
                event.id
            );
            Reconstruction::Undecodable
        }
    }
}

fn resolved_outcome(resolved: bool) -> Reconstruction {
    if resolved {
        Reconstruction::Restored
    } else {
        Reconstruction::Degraded
    }
}

/// Restore a mixed-target hybrid read: decompress the placeholder skeleton, splice
/// each git-addressed file section back into its placeholder, and inject the
/// composed result into `toolResult`. `content_render_sha` is the drift tripwire
/// over the full original bytes (skipped when a section degraded to a stub, which
/// legitimately differs).
fn reconstruct_hybrid_read(
    event: &mut Event,
    commit_hex: &str,
    store: Option<&ObjectStore>,
) -> Reconstruction {
    let codec = event.codec.as_deref().unwrap_or(CODEC_NONE);
    let Some(blob) = event.data_blob.as_deref() else {
        log::warn!("hybrid read {} has no data_blob", event.id);
        return Reconstruction::Undecodable;
    };
    let skeleton = match decompress(codec, blob) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => {
                log::warn!("hybrid read {} skeleton is not utf-8: {error}", event.id);
                return Reconstruction::Undecodable;
            }
        },
        Err(error) => {
            log::warn!("failed to decompress hybrid read {}: {error}", event.id);
            return Reconstruction::Undecodable;
        }
    };

    let mut data: Value = match serde_json::from_str(&event.data) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("hybrid read {} data is not json: {error}", event.id);
            return Reconstruction::Undecodable;
        }
    };

    let paths: Vec<String> = data
        .get("toolInput")
        .and_then(|input| input.get("paths"))
        .and_then(|paths| paths.as_array())
        .map(|array| {
            array
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let sections: Vec<usize> = data
        .get("sections")
        .and_then(|value| value.as_array())
        .map(|array| {
            array
                .iter()
                .filter_map(|value| value.as_u64().map(|n| n as usize))
                .collect()
        })
        .unwrap_or_default();

    let (rendered, resolved) =
        splice_hybrid_skeleton_resolving(&skeleton, commit_hex, &paths, &sections, store);

    // Same drift tripwire as the pure-file read: a mismatch means the live and
    // archival read composition diverged since archival. A degraded (stub) section
    // legitimately differs and already logged, so skip the compare there.
    if !rendered.contains(STUB_PREFIX) {
        if let Some(expected) = event.content_render_sha.as_deref() {
            let actual = format!("{:x}", Sha256::digest(rendered.as_bytes()));
            if actual != expected {
                log::warn!(
                    "hybrid read {} reconstruction sha mismatch (recorded {expected}, got {actual}); \
                     the shared read composition may have diverged since archival",
                    event.id
                );
            }
        }
    }

    if let Value::Object(ref mut map) = data {
        map.insert("toolResult".to_string(), Value::String(rendered));
        map.remove("archived");
        map.remove("sections");
    }
    match serde_json::to_string(&data) {
        Ok(serialized) => {
            event.data = serialized;
            resolved_outcome(resolved)
        }
        Err(error) => {
            log::warn!(
                "hybrid read {} could not be re-serialized: {error}",
                event.id
            );
            Reconstruction::Undecodable
        }
    }
}

/// Restore a write assistant event: decompress the payload-stripped remainder,
/// regenerate the committed diff, and re-inject each file's section into its
/// `changes[*].payload`. Presentation-equivalence, not byte-identity: the
/// reconstructed payload is the committed change, not the original anchors.
fn reconstruct_gitcoord_write(
    event: &mut Event,
    commit_hex: &str,
    store: Option<&ObjectStore>,
) -> Reconstruction {
    let codec = event.codec.as_deref().unwrap_or(CODEC_NONE);
    let Some(blob) = event.data_blob.as_deref() else {
        log::warn!("gitcoord write {} has no data_blob", event.id);
        return Reconstruction::Undecodable;
    };
    let remainder = match decompress(codec, blob) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => {
                log::warn!(
                    "gitcoord write {} remainder is not utf-8: {error}",
                    event.id
                );
                return Reconstruction::Undecodable;
            }
        },
        Err(error) => {
            log::warn!("failed to decompress gitcoord write {}: {error}", event.id);
            return Reconstruction::Undecodable;
        }
    };
    let mut value: Value = match serde_json::from_str(&remainder) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("gitcoord write {} remainder is not json: {error}", event.id);
            return Reconstruction::Undecodable;
        }
    };

    let sections = commit_diff_sections(commit_hex, store);
    let mut stubbed = false;
    if let Some(tool_uses) = value.get_mut("toolUses").and_then(|v| v.as_array_mut()) {
        for tool in tool_uses {
            stubbed |= inject_change_diffs(tool, commit_hex, &sections);
        }
    }
    match serde_json::to_string(&value) {
        Ok(serialized) => {
            event.data = serialized;
            resolved_outcome(!stubbed)
        }
        Err(error) => {
            log::warn!(
                "gitcoord write {} could not be re-serialized: {error}",
                event.id
            );
            Reconstruction::Undecodable
        }
    }
}

/// The committed unified diff split into per-file sections keyed by repo path, or
/// `None` when the coordinate is unresolvable (dropped pack, missing object).
fn commit_diff_sections(
    commit_hex: &str,
    store: Option<&ObjectStore>,
) -> Option<HashMap<String, String>> {
    let store = store?;
    match diff::render_commit_diff(store, commit_hex) {
        Ok(diff) => Some(split_diff_by_path(&diff)),
        Err(error) => {
            log::warn!("archived write diff unresolvable {commit_hex}: {error}");
            None
        }
    }
}

/// The repo paths a commit's diff touches, split exactly as write
/// reconstruction splits it, or `None` when the commit is unresolvable. The
/// archival writer drops a change's payload only for a path in this set, so
/// every payload it drops is one [`inject_change_diffs`] fills back in.
pub fn commit_diff_paths(commit_hex: &str, store: &ObjectStore) -> Option<HashSet<String>> {
    commit_diff_sections(commit_hex, Some(store)).map(|sections| sections.into_keys().collect())
}

/// Inject the regenerated diff into each payload-free file change's
/// `payload.diff`. A change that kept its payload (a resource target, a rename,
/// a `unified_patch` envelope, a failed item) is left as stored, and a file
/// absent from the diff stays payload-free; when the coordinate is wholly
/// unresolvable, each payload-free file change gets a labeled stub instead.
/// Returns whether any change got a stub.
fn inject_change_diffs(
    tool: &mut Value,
    commit_hex: &str,
    sections: &Option<HashMap<String, String>>,
) -> bool {
    let Some(changes) = tool
        .get_mut("input")
        .and_then(|v| v.as_object_mut())
        .and_then(|input| input.get_mut("changes"))
        .and_then(|v| v.as_array_mut())
    else {
        return false;
    };
    let mut stubbed = false;
    for change in changes {
        let Some(obj) = change.as_object_mut() else {
            continue;
        };
        if obj.contains_key("payload") {
            continue;
        }
        let Some(target) = obj
            .get("target")
            .and_then(|v| v.as_str())
            .map(str::to_string)
        else {
            continue;
        };
        let Some(path) = repo_relative_path(&target) else {
            continue;
        };
        let diff_text = match sections {
            Some(map) => match map.get(&path) {
                Some(section) => section.clone(),
                None => continue,
            },
            None => {
                stubbed = true;
                stub(commit_hex, &path)
            }
        };
        obj.insert(
            "payload".to_string(),
            serde_json::json!({ "diff": diff_text }),
        );
    }
    stubbed
}

/// Split a unified diff into per-file sections keyed by their `b/<path>`. Each
/// section begins at a `diff --git a/<path> b/<path>` header and runs to the next
/// header (matching [`super::diff`]'s output shape).
fn split_diff_by_path(diff: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut current_path: Option<String> = None;
    let mut current = String::new();
    for line in diff.split_inclusive('\n') {
        if let Some(rest) = line.strip_prefix("diff --git a/") {
            if let Some(path) = current_path.take() {
                map.insert(path, std::mem::take(&mut current));
            }
            current_path = rest.find(" b/").map(|idx| rest[..idx].to_string());
            if current_path.is_none() {
                current.clear();
            }
        }
        if current_path.is_some() {
            current.push_str(line);
        }
    }
    if let Some(path) = current_path.take() {
        map.insert(path, current);
    }
    map
}

/// Reconstruct one archived `gitcoord` read's `toolResult` by rebuilding each
/// target's [`ReadSegment`] from its resolved blob and running the **same**
/// `assemble` the live `read_batch` ran.
///
/// Reproducing the historical composition here is load-bearing: the archival
/// rewrite that created these git-coordinate rows discarded the original rendered
/// bytes only after a byte-exact compare succeeded. This shared path is the only
/// thing that can reproduce their enriched headers, continue footers, fair-share
/// budgets, and empty-body header collapse. A target
/// that cannot be resolved or rendered degrades to an `Error` segment
/// (`=== uri [error] ===` header + a `STUB_PREFIX` body), so reconstruction never
/// errors out of the batch. This degraded output is not byte-exact history: the
/// `STUB_PREFIX` guard in [`reconstruct_gitcoord_read`] explicitly exempts it from
/// the recorded-render SHA comparison (as does the equivalent hybrid-read guard).
pub fn reconstruct_read(commit_hex: &str, paths: &[String], store: Option<&ObjectStore>) -> String {
    reconstruct_read_resolving(commit_hex, paths, store).0
}

/// [`reconstruct_read`], also reporting whether every target resolved.
fn reconstruct_read_resolving(
    commit_hex: &str,
    paths: &[String],
    store: Option<&ObjectStore>,
) -> (String, bool) {
    let renderer = crate::storage::render::archived_file_renderer();
    let mut resolved = true;
    let segments: Vec<ReadSegment> = paths
        .iter()
        .map(
            |target| match build_archived_segment(commit_hex, target, store, renderer) {
                Ok(segment) => segment,
                Err(stub) => {
                    resolved = false;
                    *stub
                }
            },
        )
        .collect();
    let text = crate::storage::render::assemble_legacy_chars(segments, &LEGACY_CHAR_METER).text;
    (text, resolved)
}

/// Render one archived file target's standalone section text — the exact bytes it
/// contributed to the composed batch — by rebuilding its segment from `commit_hex`
/// and running the shared single-section renderer. An unresolvable target renders
/// an `Error` segment carrying a `STUB_PREFIX` body. Shared by the hybrid-read
/// write (its verbatim needle) and reconstruction (its skeleton fill).
pub fn render_archived_file_section(
    commit_hex: &str,
    target: &str,
    store: Option<&ObjectStore>,
) -> String {
    render_archived_file_section_resolving(commit_hex, target, store).0
}

/// [`render_archived_file_section`], also reporting whether the target resolved.
fn render_archived_file_section_resolving(
    commit_hex: &str,
    target: &str,
    store: Option<&ObjectStore>,
) -> (String, bool) {
    let renderer = crate::storage::render::archived_file_renderer();
    let (seg, resolved) = match build_archived_segment(commit_hex, target, store, renderer) {
        Ok(segment) => (segment, true),
        Err(stub) => (*stub, false),
    };
    (
        crate::storage::render::render_section_text(seg, &LEGACY_CHAR_METER),
        resolved,
    )
}

/// The NUL-delimited placeholder a hybrid-read skeleton carries in place of an
/// excised file section. A raw NUL byte can never appear in rendered UTF-8/JSON
/// text, so a placeholder can never collide with content (the same invariant the
/// system-init skeleton relies on).
pub fn section_placeholder(index: usize) -> String {
    format!("\0{index}\0")
}

/// Splice the git-addressed file sections back into a hybrid-read skeleton:
/// substitute each indexed placeholder with its standalone render. Shared by the
/// write's verify-then-discard and reconstruction so the two cannot drift. A
/// rendered section can never contain a placeholder (NUL-free), so substitutions
/// never cascade and order is irrelevant.
pub fn splice_hybrid_skeleton(
    skeleton: &str,
    commit_hex: &str,
    paths: &[String],
    sections: &[usize],
    store: Option<&ObjectStore>,
) -> String {
    splice_hybrid_skeleton_resolving(skeleton, commit_hex, paths, sections, store).0
}

/// [`splice_hybrid_skeleton`], also reporting whether every excised section
/// resolved. A section index with no target is a placeholder left unfilled, so
/// it counts as unresolved too.
fn splice_hybrid_skeleton_resolving(
    skeleton: &str,
    commit_hex: &str,
    paths: &[String],
    sections: &[usize],
    store: Option<&ObjectStore>,
) -> (String, bool) {
    let mut out = skeleton.to_string();
    let mut resolved = true;
    for &index in sections {
        let Some(target) = paths.get(index) else {
            resolved = false;
            continue;
        };
        let (section, section_resolved) =
            render_archived_file_section_resolving(commit_hex, target, store);
        resolved &= section_resolved;
        out = out.replace(&section_placeholder(index), &section);
    }
    (out, resolved)
}

/// Build the [`ReadSegment`] for one archived read target from its blob, or an
/// `Error` segment carrying a labeled coordinate stub when the blob cannot be
/// resolved or rendered. The registered [`ArchivedFileRenderer`] (the mcp read
/// layer's byte renderers) guarantees the segment is identical to the one the
/// live read built from the same bytes; when no renderer is registered the
/// target degrades to the same coordinate stub as an unresolvable one.
///
/// `Err` carries that stub segment, so a caller that only renders treats both
/// arms alike and one that reports can tell them apart.
fn build_archived_segment(
    commit_hex: &str,
    target: &str,
    store: Option<&ObjectStore>,
    renderer: Option<&dyn ArchivedFileRenderer>,
) -> Result<ReadSegment, Box<ReadSegment>> {
    let stub_segment = |label: &str| {
        Box::new(crate::storage::render::error_segment(
            target,
            stub(commit_hex, label),
        ))
    };
    let Some(store) = store else {
        return Err(stub_segment(target));
    };
    let Some(path) = repo_relative_path(target) else {
        return Err(stub_segment(target));
    };
    match store.resolve_path_at_commit(commit_hex, &path) {
        Ok(bytes) => render_archived_bytes(renderer, target, &bytes, || *stub_segment(&path)),
        Err(error) => {
            warn_unresolvable_once(commit_hex, &path, &error);
            Err(stub_segment(&path))
        }
    }
}

/// Render archived file bytes through the registered renderer, degrading to
/// `stub` when no renderer is registered or the render fails. Pure: the renderer
/// and stub are both injected, so neither the global registry nor a DB is needed
/// to exercise both branches. A missing renderer yields the same labeled
/// coordinate stub as an unresolvable coordinate.
fn render_archived_bytes(
    renderer: Option<&dyn ArchivedFileRenderer>,
    target: &str,
    bytes: &[u8],
    stub: impl Fn() -> ReadSegment,
) -> Result<ReadSegment, Box<ReadSegment>> {
    match renderer {
        Some(renderer) => match renderer.produce_archived_file_segment(target, bytes) {
            Ok(segment) => Ok(segment),
            Err(error) => {
                log::warn!("archived read render failed for {target}: {error}");
                Err(Box::new(stub()))
            }
        },
        None => Err(Box::new(stub())),
    }
}

pub fn repo_relative_path(target: &str) -> Option<String> {
    let split = cairn_common::query::split_target_query(target).ok()?;
    let rel = split.identity.strip_prefix("file:")?;
    Some(rel.trim_start_matches('/').to_string())
}

fn stub(commit_hex: &str, label: &str) -> String {
    format!("{STUB_PREFIX}: {commit_hex}:{label}")
}

/// An unresolvable archived-read coordinate is a permanent, expected condition
/// for legacy rows written before the render-and-compare verify shipped: their
/// original rendered bytes were discarded at teardown and can never be
/// reconstructed. The same handful of coordinates would otherwise warn on every
/// transcript view (CAIRN-1675 observed ~1,854 lines for ~6 coordinates), so
/// warn once per coordinate per process and drop the repeats to debug.
fn warn_unresolvable_once(commit_hex: &str, path: &str, error: &impl std::fmt::Display) {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let key = format!("{commit_hex}:{path}");
    let first = SEEN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .map(|mut seen| seen.insert(key.clone()))
        .unwrap_or(true);
    if first {
        log::warn!("archived read coordinate unresolvable {key}: {error}");
    } else {
        log::debug!("archived read coordinate unresolvable {key}: {error}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::event_fixture::assistant_write_remainder;
    use crate::storage::events::reconstruct_fixture::{
        build_fixture, make_event, migrated_db, read_event, seed_chain, tool_result,
    };
    use crate::storage::events::testutil::git;
    use crate::storage::{compress, CODEC_ZSTD_V1};

    #[tokio::test]
    async fn identity_when_all_full() {
        let db = migrated_db().await;
        let events = vec![
            make_event(
                "e1",
                "assistant",
                "{\"content\":\"hi\"}",
                None,
                None,
                None,
                None,
            ),
            make_event(
                "e2",
                "user",
                "{\"content\":\"yo\"}",
                Some("full"),
                None,
                None,
                None,
            ),
        ];
        let out = reconstruct_events(&db, events.clone()).await;
        assert_eq!(out[0].data, events[0].data);
        assert_eq!(out[1].data, events[1].data);
    }

    #[tokio::test]
    async fn zstd_round_trips_data() {
        let db = migrated_db().await;
        let original = "{\"eventType\":\"user\",\"content\":\"hello archived world\"}";
        let blob = compress(original.as_bytes()).unwrap();
        let event = make_event(
            "z1",
            "user",
            "{\"_archived\":true}",
            Some("zstd"),
            None,
            Some(blob),
            Some(CODEC_ZSTD_V1),
        );
        let out = reconstruct_events(&db, vec![event]).await;
        assert_eq!(out[0].data, original);
    }

    #[tokio::test]
    async fn unresolvable_coordinate_degrades_to_stub() {
        let fx = build_fixture();
        let db = migrated_db().await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            Some((fx.pack.clone(), fx.idx.clone())),
        )
        .await;

        let bogus = "0".repeat(40);
        let event = read_event(&bogus, &["file:a.txt"]);
        let out = reconstruct_events(&db, vec![event]).await;
        let body = tool_result(&out[0]);
        assert!(
            body.contains(STUB_PREFIX),
            "expected coordinate stub, got: {body}"
        );
        // The live renderer labels the degraded section as non-content.
        assert!(body.contains("=== file:a.txt [error] ==="));
    }

    /// A gitcoord-write assistant event (payload-stripped remainder in data_blob,
    /// commit coordinate) reconstructs by re-injecting the committed per-file diff
    /// into each change's `payload.diff`. Concatenated in change order they
    /// reproduce `git show <tip>`.
    #[tokio::test]
    async fn gitcoord_write_reinjects_committed_diff() {
        let fx = build_fixture();
        let db = migrated_db().await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            Some((fx.pack.clone(), fx.idx.clone())),
        )
        .await;

        // The tip commit modifies a.txt and adds c.txt; the change skeletons name
        // both (sorted order), payloads already stripped at archival.
        let remainder = assistant_write_remainder("w1");
        let blob = compress(remainder.as_bytes()).unwrap();
        let event = make_event(
            "w1",
            "assistant",
            "{\"eventType\":\"assistant\",\"archived\":\"gitcoord_write\"}",
            Some("gitcoord"),
            Some(&fx.tip),
            Some(blob),
            Some(CODEC_ZSTD_V1),
        );
        let out = reconstruct_events(&db, vec![event]).await;
        let value: Value = serde_json::from_str(&out[0].data).unwrap();
        let changes = value["toolUses"][0]["input"]["changes"].as_array().unwrap();
        let combined: String = changes
            .iter()
            .map(|c| c["payload"]["diff"].as_str().unwrap().to_string())
            .collect();

        // `git show --format=` prints the bare diff (a leading blank line from the
        // empty commit format, then the patch).
        let expected = git(&fx.clone, &["show", "--format=", "--no-color", &fx.tip]);
        let expected = expected.trim_start_matches('\n');
        assert_eq!(combined, expected);
    }

    fn outcomes(reported: &[(Event, Reconstruction)]) -> Vec<Reconstruction> {
        reported.iter().map(|(_, outcome)| *outcome).collect()
    }

    #[tokio::test]
    async fn reporting_names_plain_and_restored_rows() {
        let db = migrated_db().await;
        let original = "{\"eventType\":\"user\",\"content\":\"hello archived world\"}";
        let blob = compress(original.as_bytes()).unwrap();
        let events = vec![
            make_event(
                "p1",
                "user",
                "{\"content\":\"plain\"}",
                None,
                None,
                None,
                None,
            ),
            make_event(
                "z1",
                "user",
                "{\"_archived\":true}",
                Some("zstd"),
                None,
                Some(blob),
                Some(CODEC_ZSTD_V1),
            ),
        ];
        let reported = reconstruct_events_reporting(&db, events).await;
        assert_eq!(
            outcomes(&reported),
            [Reconstruction::Stored, Reconstruction::Restored]
        );
        assert_eq!(reported[1].0.data, original);
    }

    /// A coordinate the store cannot resolve is reported, not left to be
    /// inferred from the stub text: that text is also what a transcript that
    /// read this very file contains.
    #[tokio::test]
    async fn reporting_names_an_unresolvable_coordinate_degraded() {
        let fx = build_fixture();
        let db = migrated_db().await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            Some((fx.pack.clone(), fx.idx.clone())),
        )
        .await;

        let bogus = "0".repeat(40);
        let reported =
            reconstruct_events_reporting(&db, vec![read_event(&bogus, &["file:a.txt"])]).await;
        assert_eq!(outcomes(&reported), [Reconstruction::Degraded]);
        assert!(reported[0].1.is_unresolved());
        assert!(tool_result(&reported[0].0).contains(STUB_PREFIX));
    }

    #[tokio::test]
    async fn reporting_names_a_missing_prompt_segment_degraded() {
        let db = migrated_db().await;
        let stub_data = serde_json::json!({
            "eventType": "system",
            "archived": "system_prompt",
            "segments": [{"inline": "head "}, {"hash": "f".repeat(64), "kind": "skills"}],
        })
        .to_string();
        let event = make_event(
            "sp1",
            "system",
            &stub_data,
            Some("gitcoord"),
            None,
            None,
            None,
        );
        let reported = reconstruct_events_reporting(&db, vec![event]).await;
        assert_eq!(outcomes(&reported), [Reconstruction::Degraded]);
        let value: Value = serde_json::from_str(&reported[0].0.data).unwrap();
        assert!(value["content"].as_str().unwrap().starts_with("head "));
    }

    #[tokio::test]
    async fn reporting_names_a_corrupt_blob_undecodable_and_keeps_the_stored_form() {
        let db = migrated_db().await;
        let event = make_event(
            "bad-zstd",
            "user",
            "{\"_archived\":true}",
            Some("zstd"),
            None,
            Some(b"not a zstd frame".to_vec()),
            Some(CODEC_ZSTD_V1),
        );
        let reported = reconstruct_events_reporting(&db, vec![event]).await;
        assert_eq!(outcomes(&reported), [Reconstruction::Undecodable]);
        assert_eq!(reported[0].0.data, "{\"_archived\":true}");
    }

    /// The quarantine gate still runs last on the reporting path, so an archived
    /// credential-bearing row comes back withheld rather than restored.
    #[tokio::test]
    async fn reporting_names_a_quarantined_row_withheld_after_restoring_it() {
        use crate::storage::quarantine::{quarantine, QuarantineSet, TRANSCRIPT_EVENT_SINK};
        let db = migrated_db().await;
        let blob = compress(b"{\"content\":\"sk-secret\"}").unwrap();
        let event = make_event(
            "reporting-withheld-event",
            "user",
            "{\"_archived\":true}",
            Some("zstd"),
            None,
            Some(blob),
            Some(CODEC_ZSTD_V1),
        );
        let source = std::path::Path::new("reporting-withheld-test-source");
        quarantine().install_for(
            source,
            QuarantineSet::from_entries([(
                TRANSCRIPT_EVENT_SINK.to_string(),
                "reporting-withheld-event".to_string(),
                "incident-1".to_string(),
            )]),
        );
        let reported = reconstruct_events_reporting(&db, vec![event]).await;
        quarantine().install_for(source, QuarantineSet::default());
        assert_eq!(outcomes(&reported), [Reconstruction::Withheld]);
        assert!(!reported[0].0.data.contains("sk-secret"));
    }

    /// The trait-fallback branch: with no renderer registered, archived file bytes
    /// degrade to the injected stub. Pure — no DB, no global, no mcp dependency.
    #[test]
    fn render_archived_bytes_without_renderer_degrades_to_stub() {
        let stub =
            || crate::storage::render::error_segment("file:a.txt", stub("deadbeef", "file:a.txt"));
        let segment = render_archived_bytes(None, "file:a.txt", b"ignored", stub).unwrap_err();
        assert!(segment.body.contains(STUB_PREFIX));
    }
}
