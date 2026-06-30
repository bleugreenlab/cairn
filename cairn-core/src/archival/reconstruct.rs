//! Transparent reconstruction of archived events on the read path.
//!
//! Events are recorded `full` on the hot path and rewritten to git coordinates
//! (with a zstd backstop) at worktree teardown, the immutability boundary. This
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
//!   reassemble the batch under `=== <uri> ===` headers.
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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use cairn_common::read::ReadSegment;
use gix_hash::ObjectId;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::encoding::{
    decode, init_placeholder, ArchivedShape, EventColumns, ARCHIVED_SYSTEM_INIT, INIT_TOOLS_TAG,
};
use super::store::{content_hash, unframe_pack, ContentStore};
use super::{decompress, diff, ObjectStore, CODEC_NONE, CODEC_ZSTD_V1};
use crate::models::Event;
use crate::storage::{DbResult, LocalDb, RowExt};

/// Prefix of the stub substituted for an unresolvable coordinate.
pub(crate) const STUB_PREFIX: &str = "content no longer resolvable";

/// Reconstruct any archived events in `events` back to their full rendered form.
///
/// Identity (a single scan, no allocation) when every row is `full`. See the
/// module docs for per-mode behavior.
pub async fn reconstruct_events(db: &LocalDb, events: Vec<Event>) -> Vec<Event> {
    if !events.iter().any(is_archived) {
        return events;
    }
    // Two phases so the `ObjectStore` (which holds a non-Send `Rc`) is never
    // alive across an await: gather all coordinates and pack bytes via async DB
    // reads under one read connection first, then build the layered stores and
    // reconstruct synchronously.
    let run_ids = gitcoord_run_ids(&events);
    let hashes = blobbed_hashes(&events);
    // A team replica carries a content store; the private DB returns `None`. When
    // present, an offloaded pack/blob whose bytes are absent locally is fetched
    // from the shared per-team store (the store read-through caches into the
    // private `cas_cache`, so repeat reconstructs are offline). The fetch happens
    // in this async phase, before the non-Send `ObjectStore` is built.
    let store = db.content_store().cloned();
    let private_route_db = db.private_route_db().cloned();
    let coords = db
        .read(move |conn| {
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
        .unwrap_or_default();
    finish(coords, events)
}

/// Connection-scoped reconstruction for callers that already hold a read
/// connection (e.g. the transcript resource), avoiding a nested pool checkout.
pub async fn reconstruct_events_with_conn(
    conn: &turso::Connection,
    events: Vec<Event>,
    store: Option<&dyn ContentStore>,
) -> Vec<Event> {
    reconstruct_events_with_conn_and_routes(conn, events, store, None).await
}

pub async fn reconstruct_events_with_conn_and_routes(
    conn: &turso::Connection,
    events: Vec<Event>,
    store: Option<&dyn ContentStore>,
    private_route_db: Option<&LocalDb>,
) -> Vec<Event> {
    if !events.iter().any(is_archived) {
        return events;
    }
    let run_ids = gitcoord_run_ids(&events);
    let hashes = blobbed_hashes(&events);
    let coords = Coords::load(conn, &run_ids, &hashes, store, private_route_db).await;
    finish(coords, events)
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
    let key = project_key.to_uppercase();
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

fn reconstruct_one(event: &mut Event, ctx: &ReconstructCtx) {
    let shape = match decode(&columns_of(event)) {
        Ok(shape) => shape,
        Err(error) => {
            log::warn!(
                "archived event {} has invalid column shape: {error}",
                event.id
            );
            return;
        }
    };
    match shape {
        ArchivedShape::Full { .. } => {}
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
fn reconstruct_system_prompt(event: &mut Event, blobs: &HashMap<String, String>) {
    let mut value: Value = match serde_json::from_str(&event.data) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("system prompt {} data is not json: {error}", event.id);
            return;
        }
    };
    let Some(segments) = value.get("segments").and_then(|s| s.as_array()) else {
        log::warn!("system prompt {} has no segments", event.id);
        return;
    };

    let mut full = String::new();
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
        Ok(serialized) => event.data = serialized,
        Err(error) => {
            log::warn!(
                "system prompt {} could not be re-serialized: {error}",
                event.id
            )
        }
    }
}

/// Restore an archived `system:init` event from its content-addressed skeleton
/// plus the inlined varying fields. A missing skeleton blob (or any structural
/// failure) degrades to a labeled stub so the transcript stays valid JSON; the
/// rest of the row still reconstructs when only the tool-set blob is missing.
fn reconstruct_system_init(event: &mut Event, blobs: &HashMap<String, String>) {
    match reassemble_system_init(&event.data, blobs) {
        Some(data) => event.data = data,
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
pub(crate) fn reassemble_system_init(
    stub_data: &str,
    blobs: &HashMap<String, String>,
) -> Option<String> {
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

fn reconstruct_zstd(event: &mut Event) {
    let codec = event.codec.as_deref().unwrap_or(CODEC_NONE);
    let Some(blob) = event.data_blob.as_deref() else {
        log::warn!("zstd archived event {} has no data_blob", event.id);
        return;
    };
    match decompress(codec, blob) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => event.data = text,
            Err(error) => log::warn!(
                "zstd archived event {} data_blob is not valid utf-8: {error}",
                event.id
            ),
        },
        Err(error) => log::warn!(
            "failed to decompress zstd archived event {}: {error}",
            event.id
        ),
    }
}

fn reconstruct_gitcoord_read(event: &mut Event, commit_hex: &str, store: Option<&ObjectStore>) {
    let mut data: Value = match serde_json::from_str(&event.data) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("gitcoord read {} data is not json: {error}", event.id);
            return;
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

    let rendered = match paths {
        Some(paths) => reconstruct_read(commit_hex, &paths, store),
        None => {
            log::warn!("gitcoord read {} has no tool_input.paths", event.id);
            stub(commit_hex, "<read>")
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
        Ok(serialized) => event.data = serialized,
        Err(error) => log::warn!(
            "gitcoord read {} could not be re-serialized: {error}",
            event.id
        ),
    }
}

/// Restore a mixed-target hybrid read: decompress the placeholder skeleton, splice
/// each git-addressed file section back into its placeholder, and inject the
/// composed result into `toolResult`. `content_render_sha` is the drift tripwire
/// over the full original bytes (skipped when a section degraded to a stub, which
/// legitimately differs).
fn reconstruct_hybrid_read(event: &mut Event, commit_hex: &str, store: Option<&ObjectStore>) {
    let codec = event.codec.as_deref().unwrap_or(CODEC_NONE);
    let Some(blob) = event.data_blob.as_deref() else {
        log::warn!("hybrid read {} has no data_blob", event.id);
        return;
    };
    let skeleton = match decompress(codec, blob) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => {
                log::warn!("hybrid read {} skeleton is not utf-8: {error}", event.id);
                return;
            }
        },
        Err(error) => {
            log::warn!("failed to decompress hybrid read {}: {error}", event.id);
            return;
        }
    };

    let mut data: Value = match serde_json::from_str(&event.data) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("hybrid read {} data is not json: {error}", event.id);
            return;
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

    let rendered = splice_hybrid_skeleton(&skeleton, commit_hex, &paths, &sections, store);

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
        Ok(serialized) => event.data = serialized,
        Err(error) => log::warn!(
            "hybrid read {} could not be re-serialized: {error}",
            event.id
        ),
    }
}

/// Restore a write assistant event: decompress the payload-stripped remainder,
/// regenerate the committed diff, and re-inject each file's section into its
/// `changes[*].payload`. Presentation-equivalence, not byte-identity: the
/// reconstructed payload is the committed change, not the original anchors.
fn reconstruct_gitcoord_write(event: &mut Event, commit_hex: &str, store: Option<&ObjectStore>) {
    let codec = event.codec.as_deref().unwrap_or(CODEC_NONE);
    let Some(blob) = event.data_blob.as_deref() else {
        return;
    };
    let remainder = match decompress(codec, blob) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => text,
            Err(error) => {
                log::warn!(
                    "gitcoord write {} remainder is not utf-8: {error}",
                    event.id
                );
                return;
            }
        },
        Err(error) => {
            log::warn!("failed to decompress gitcoord write {}: {error}", event.id);
            return;
        }
    };
    let mut value: Value = match serde_json::from_str(&remainder) {
        Ok(value) => value,
        Err(error) => {
            log::warn!("gitcoord write {} remainder is not json: {error}", event.id);
            return;
        }
    };

    let sections = commit_diff_sections(commit_hex, store);
    if let Some(tool_uses) = value.get_mut("toolUses").and_then(|v| v.as_array_mut()) {
        for tool in tool_uses {
            inject_change_diffs(tool, commit_hex, &sections);
        }
    }
    match serde_json::to_string(&value) {
        Ok(serialized) => event.data = serialized,
        Err(error) => log::warn!(
            "gitcoord write {} could not be re-serialized: {error}",
            event.id
        ),
    }
}

/// The committed unified diff split into per-file sections keyed by repo path, or
/// `None` when the coordinate is unresolvable (dropped pack, missing object).
fn commit_diff_sections(
    commit_hex: &str,
    store: Option<&ObjectStore>,
) -> Option<HashMap<String, String>> {
    let store = store?;
    let commit = ObjectId::from_hex(commit_hex.as_bytes()).ok()?;
    match diff::render_commit_diff(store, &commit) {
        Ok(diff) => Some(split_diff_by_path(&diff)),
        Err(error) => {
            log::warn!("archived write diff unresolvable {commit_hex}: {error}");
            None
        }
    }
}

/// Inject the regenerated diff into each file change's `payload.diff`. A change
/// whose file is absent from the diff (resource target, or no textual change) is
/// left payload-free; when the coordinate is wholly unresolvable, each file
/// change gets a labeled stub instead.
fn inject_change_diffs(
    tool: &mut Value,
    commit_hex: &str,
    sections: &Option<HashMap<String, String>>,
) {
    let Some(changes) = tool
        .get_mut("input")
        .and_then(|v| v.as_object_mut())
        .and_then(|input| input.get_mut("changes"))
        .and_then(|v| v.as_array_mut())
    else {
        return;
    };
    for change in changes {
        let Some(obj) = change.as_object_mut() else {
            continue;
        };
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
            None => stub(commit_hex, &path),
        };
        obj.insert(
            "payload".to_string(),
            serde_json::json!({ "diff": diff_text }),
        );
    }
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
/// Reproducing the live composition here is load-bearing: the verify-then-discard
/// rewrite ([`super::rewrite::try_read`]) discards the original rendered bytes
/// once a byte-exact compare succeeds, so this shared path is the only thing that
/// can reproduce them — enriched headers, continue footers, fair-share budgets,
/// and the empty-body header collapse all fall out of the one assembler. A target
/// that cannot be resolved or rendered degrades to an `Error` segment (bare
/// `=== uri ===` header + a `STUB_PREFIX` body), so reconstruction never errors
/// out of the batch and [`super::rewrite::try_read`]'s stub contract still holds.
pub(crate) fn reconstruct_read(
    commit_hex: &str,
    paths: &[String],
    store: Option<&ObjectStore>,
) -> String {
    let commit = ObjectId::from_hex(commit_hex.as_bytes()).ok();
    let segments: Vec<ReadSegment> = paths
        .iter()
        .map(|target| build_archived_segment(commit.as_deref(), commit_hex, target, store))
        .collect();
    crate::mcp::handlers::read::view::assemble(segments).text
}

/// Render one archived file target's standalone section text — the exact bytes it
/// contributed to the composed batch — by rebuilding its segment from `commit_hex`
/// and running the shared single-section renderer. An unresolvable target renders
/// an `Error` segment carrying a `STUB_PREFIX` body. Shared by the hybrid-read
/// write (its verbatim needle) and reconstruction (its skeleton fill).
pub(crate) fn render_archived_file_section(
    commit_hex: &str,
    target: &str,
    store: Option<&ObjectStore>,
) -> String {
    let commit = ObjectId::from_hex(commit_hex.as_bytes()).ok();
    let seg = build_archived_segment(commit.as_deref(), commit_hex, target, store);
    crate::mcp::handlers::read::view::render_section_text(seg)
}

/// The NUL-delimited placeholder a hybrid-read skeleton carries in place of an
/// excised file section. A raw NUL byte can never appear in rendered UTF-8/JSON
/// text, so a placeholder can never collide with content (the same invariant the
/// system-init skeleton relies on).
pub(crate) fn section_placeholder(index: usize) -> String {
    format!("\0{index}\0")
}

/// Splice the git-addressed file sections back into a hybrid-read skeleton:
/// substitute each indexed placeholder with its standalone render. Shared by the
/// write's verify-then-discard and reconstruction so the two cannot drift. A
/// rendered section can never contain a placeholder (NUL-free), so substitutions
/// never cascade and order is irrelevant.
pub(crate) fn splice_hybrid_skeleton(
    skeleton: &str,
    commit_hex: &str,
    paths: &[String],
    sections: &[usize],
    store: Option<&ObjectStore>,
) -> String {
    let mut out = skeleton.to_string();
    for &index in sections {
        let Some(target) = paths.get(index) else {
            continue;
        };
        let section = render_archived_file_section(commit_hex, target, store);
        out = out.replace(&section_placeholder(index), &section);
    }
    out
}

/// Build the [`ReadSegment`] for one archived read target from its blob, or an
/// `Error` segment carrying a labeled coordinate stub when the blob cannot be
/// resolved or rendered. The shared producer
/// ([`crate::mcp::handlers::files::produce_archived_file_segment`]) guarantees the
/// segment is identical to the one the live read built from the same bytes.
fn build_archived_segment(
    commit: Option<&gix_hash::oid>,
    commit_hex: &str,
    target: &str,
    store: Option<&ObjectStore>,
) -> ReadSegment {
    let stub_segment =
        |label: &str| crate::mcp::handlers::read::error_segment(target, stub(commit_hex, label));
    let (Some(commit), Some(store)) = (commit, store) else {
        return stub_segment(target);
    };
    let Some(path) = repo_relative_path(target) else {
        return stub_segment(target);
    };
    match store.resolve_path_at_commit(commit, &path) {
        Ok(bytes) => {
            match crate::mcp::handlers::files::produce_archived_file_segment(target, &bytes) {
                Ok(segment) => segment,
                Err(error) => {
                    log::warn!("archived read render failed for {target}: {error}");
                    stub_segment(&path)
                }
            }
        }
        Err(error) => {
            warn_unresolvable_once(commit_hex, &path, &error);
            stub_segment(&path)
        }
    }
}

pub(crate) fn repo_relative_path(target: &str) -> Option<String> {
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
    use crate::archival::event_fixture::{
        assistant_write_remainder, read_stub, render_targets as expected_read,
    };
    use crate::archival::testutil::{commit_all, git, init_repo, write_file};
    use crate::archival::{build_execution_pack, compress, CODEC_ZSTD_V1};
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use std::path::PathBuf;
    use std::sync::Arc;
    use turso::params;

    async fn migrated_db() -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.keep().join("cairn-archival-reconstruct.db");
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    /// Seed the runs → jobs → projects chain plus an `execution_history` row so
    /// `reconstruct_events` can resolve coordinates for run `run` / execution
    /// `exec`. `pack` is the range pack bytes, or `None` for an empty range.
    async fn seed_chain(db: &LocalDb, repo_path: &str, pack: Option<(Vec<u8>, Vec<u8>)>) {
        let repo_path = repo_path.to_string();
        db.write(move |conn| {
            let repo_path = repo_path.clone();
            let pack = pack.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj','ws','p','P',?1,1,1)",
                    (repo_path.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, project_id, status, created_at, updated_at)
                     VALUES ('job','exec','proj','complete',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, status, created_at, updated_at)
                     VALUES ('run','job','exited',1,1)",
                    (),
                )
                .await?;
                match pack {
                    Some((pack, idx)) => {
                        conn.execute(
                            "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES ('exec','base','tip',?1,?2)",
                            params![pack, idx],
                        )
                        .await?;
                    }
                    None => {
                        conn.execute(
                            "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack, pack_idx)
                             VALUES ('exec','base','tip',NULL,NULL)",
                            (),
                        )
                        .await?;
                    }
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn seed_team_chain(
        db: &LocalDb,
        synced_repo_path: &str,
        pack: Option<(Vec<u8>, Vec<u8>)>,
    ) {
        let synced_repo_path = synced_repo_path.to_string();
        db.write(move |conn| {
            let synced_repo_path = synced_repo_path.clone();
            let pack = pack.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES ('ws','w',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('teamABC123~00000000-0000-4000-8000-000000000001','ws','p','P',?1,1,1)",
                    (synced_repo_path.as_str(),),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, status, started_at) VALUES ('exec','r','running',1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, execution_id, project_id, status, created_at, updated_at)
                     VALUES ('job','exec','teamABC123~00000000-0000-4000-8000-000000000001','complete',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, status, created_at, updated_at)
                     VALUES ('run','job','exited',1,1)",
                    (),
                )
                .await?;
                if let Some((pack, idx)) = pack {
                    conn.execute(
                        "INSERT INTO execution_history(execution_id, base_sha, tip_sha, pack, pack_idx)
                         VALUES ('exec','base','tip',?1,?2)",
                        params![pack, idx],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    fn make_event(
        id: &str,
        event_type: &str,
        data: &str,
        storage_mode: Option<&str>,
        content_commit: Option<&str>,
        data_blob: Option<Vec<u8>>,
        codec: Option<&str>,
    ) -> Event {
        Event {
            id: id.to_string(),
            run_id: "run".to_string(),
            session_id: None,
            sequence: 1,
            timestamp: 1,
            event_type: event_type.to_string(),
            data: data.to_string(),
            parent_tool_use_id: None,
            created_at: 1,
            input_tokens: None,
            cache_read_tokens: None,
            cache_create_tokens: None,
            output_tokens: None,
            turn_id: None,
            thinking_tokens: None,
            cost_usd: None,
            storage_mode: storage_mode.map(str::to_string),
            content_commit: content_commit.map(str::to_string),
            content_change_id: None,
            content_render_sha: None,
            data_blob,
            codec: codec.map(str::to_string),
            read_segments: None,
        }
    }

    struct Fixture {
        _origin_dir: tempfile::TempDir,
        _clone_dir: tempfile::TempDir,
        origin: PathBuf,
        clone: PathBuf,
        anchor: String,
        tip: String,
        pack: Vec<u8>,
        idx: Vec<u8>,
    }

    /// origin holds `main` at the anchor; a clone makes a branch commit (tip)
    /// whose new objects live only in the range pack — so a tip read mixes
    /// pack-layer (commit/tree/modified blob) and repo-layer (unchanged blob)
    /// resolution.
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
        let tip = commit_all(&clone, "work");

        let (pack, idx) = build_execution_pack(&clone, &tip, &anchor, "main")
            .unwrap()
            .unwrap();

        Fixture {
            _origin_dir: origin_dir,
            _clone_dir: clone_dir,
            origin,
            clone,
            anchor,
            tip,
            pack,
            idx,
        }
    }

    // The expected rendered batch body (`expected_read`) and the archived
    // gitcoord-read stub (`read_stub`) come from the shared real-anatomy fixture
    // imported above; a wrong-shape assumption fails in exactly one place.

    fn read_event(content_commit: &str, paths: &[&str]) -> Event {
        let data = read_stub("t1", paths);
        make_event(
            "read-ev",
            "tool_result",
            &data,
            Some("gitcoord"),
            Some(content_commit),
            None,
            None,
        )
    }

    fn tool_result(event: &Event) -> String {
        let value: serde_json::Value = serde_json::from_str(&event.data).unwrap();
        value["toolResult"].as_str().unwrap().to_string()
    }

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
    async fn gitcoord_reads_are_byte_identical() {
        let fx = build_fixture();
        let db = migrated_db().await;
        seed_chain(
            &db,
            fx.origin.to_str().unwrap(),
            Some((fx.pack.clone(), fx.idx.clone())),
        )
        .await;

        // Tip read: full file, line window, tail, single-file grep (all pack-layer
        // a.txt) plus an unchanged file (pack-layer tree, repo-layer blob).
        let tip_a = b"ALPHA\nbeta\ngamma\ndelta\nepsilon\n";
        let b_txt = b"unchanged-keep\n";
        let targets: Vec<(&str, &[u8])> = vec![
            ("file:a.txt", tip_a),
            ("file:a.txt?offset=2&limit=2", tip_a),
            ("file:a.txt?offset=-1", tip_a),
            ("file:a.txt?grep=beta", tip_a),
            ("file:dir/b.txt", b_txt),
        ];
        let paths: Vec<&str> = targets.iter().map(|(t, _)| *t).collect();
        let event = read_event(&fx.tip, &paths);
        let out = reconstruct_events(&db, vec![event]).await;
        assert_eq!(tool_result(&out[0]), expected_read(&targets));

        // Anchor read resolves entirely from the repo layer (anchor is not in the
        // range pack).
        let anchor_a = b"alpha\nbeta\ngamma\ndelta\n";
        let anchor_targets: Vec<(&str, &[u8])> = vec![("file:a.txt", anchor_a)];
        let anchor_event = read_event(&fx.anchor, &["file:a.txt"]);
        let anchor_out = reconstruct_events(&db, vec![anchor_event]).await;
        assert_eq!(tool_result(&anchor_out[0]), expected_read(&anchor_targets));
    }

    #[tokio::test]
    async fn team_gitcoord_read_stubs_until_private_local_repo_path_is_set() {
        let fx = build_fixture();
        let team_db = migrated_db().await;
        seed_team_chain(
            &team_db,
            "/creator/path/not/on/this/machine",
            Some((fx.pack.clone(), fx.idx.clone())),
        )
        .await;
        let private_db = Arc::new(migrated_db().await);
        private_db
            .execute(
                "INSERT INTO teams(id, name, sync_url, replica_path, created_at) VALUES ('teamABC123', 'Team', 'http://sync', '/tmp/team.db', 1)",
                (),
            )
            .await
            .unwrap();
        private_db
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('P', 'teamABC123', 1)",
                (),
            )
            .await
            .unwrap();

        let event = read_event(&fx.tip, &["file:a.txt"]);
        let without_clone = team_db
            .read(|conn| {
                let event = event.clone();
                let private_db = private_db.clone();
                Box::pin(async move {
                    DbResult::Ok(
                        reconstruct_events_with_conn_and_routes(
                            conn,
                            vec![event],
                            None,
                            Some(private_db.as_ref()),
                        )
                        .await,
                    )
                })
            })
            .await
            .unwrap();
        assert!(tool_result(&without_clone[0]).contains(STUB_PREFIX));

        private_db
            .execute(
                "UPDATE project_routes SET local_repo_path = ?1 WHERE project_key = 'P'",
                (fx.origin.to_str().unwrap(),),
            )
            .await
            .unwrap();
        let event = read_event(&fx.tip, &["file:a.txt"]);
        let with_clone = team_db
            .read(|conn| {
                let event = event.clone();
                let private_db = private_db.clone();
                Box::pin(async move {
                    DbResult::Ok(
                        reconstruct_events_with_conn_and_routes(
                            conn,
                            vec![event],
                            None,
                            Some(private_db.as_ref()),
                        )
                        .await,
                    )
                })
            })
            .await
            .unwrap();
        assert_eq!(
            tool_result(&with_clone[0]),
            expected_read(&[(
                "file:a.txt",
                b"ALPHA
beta
gamma
delta
epsilon
" as &[u8],
            )])
        );
    }

    #[tokio::test]
    async fn null_pack_reconstructs_via_repo_layer() {
        let fx = build_fixture();
        let db = migrated_db().await;
        // No range pack: an anchor read still resolves from the project repo ODB.
        seed_chain(&db, fx.origin.to_str().unwrap(), None).await;

        let anchor_a = b"alpha\nbeta\ngamma\ndelta\n";
        let event = read_event(&fx.anchor, &["file:a.txt"]);
        let out = reconstruct_events(&db, vec![event]).await;
        assert_eq!(
            tool_result(&out[0]),
            expected_read(&[("file:a.txt", anchor_a)])
        );
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
        // The header is still emitted around the stub body.
        assert!(body.contains("=== file:a.txt ==="));
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
}
