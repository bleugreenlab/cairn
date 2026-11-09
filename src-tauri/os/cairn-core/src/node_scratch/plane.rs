//! The object channel's `scratch/*` routes: how every machine brings a node
//! scratch current and writes it back.
//!
//! Mounted beside the CAS object routes on the loopback router and on the
//! executor listener, so a colocated machine and a remote one sync the same
//! way. Each request is authenticated by the executor's object-channel
//! credential and authorized for one `scratch_id`, which the runner grants an
//! executor connection when it hands that connection work carrying the
//! scratch, or when the connection advertises a replica of it.
//!
//! Transfers move blobs by identity, in packs of at most
//! [`NODE_SCRATCH_TRANSFER_BATCH_BYTES`]. Every installed pack is kept, so an
//! interrupted transfer resumes by asking only for what is still missing.
//!
//! A write-back is three calls. `upload/begin` plans the merge from the change
//! list alone and checks the ceiling against the merged size before any byte
//! moves; `upload/pack` stages blob packs; `upload/complete` installs them and,
//! under the scratch's write lock, re-plans against the head as it is now,
//! writes the result, and advances the authority by compare-and-swap.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use cairn_common::executor_protocol::{
    ScratchBlobsRequest, ScratchChange, ScratchSkeletonRequest, ScratchUploadBegin,
    ScratchUploadComplete, ScratchUploadPlan, ScratchUploadResult, NODE_SCRATCH_MAX_FILE_BYTES,
    NODE_SCRATCH_TRANSFER_BATCH_BYTES, SCRATCH_SERVED_HEADER, SCRATCH_UPLOAD_HEADER,
};

use super::owner::{self, NodeScratch, Replica};
use super::{merge, store};
use crate::orchestrator::Orchestrator;

/// An upload begun and not completed within this long is abandoned.
const UPLOAD_TTL_MS: i64 = 60 * 60 * 1000;
/// The most paths one write-back may name.
const MAX_CHANGES: usize = 1_000_000;

struct UploadSession {
    scratch_id: String,
    executor_id: String,
    generation: u64,
    changes: Vec<ScratchChange>,
    writer: String,
    staging: PathBuf,
    packs: Vec<PathBuf>,
    created_at_ms: i64,
}

/// The scratch plane's live state: which executor connections may touch
/// which scratches, the write-backs in flight, and one write lock per scratch.
#[derive(Clone, Default)]
pub struct ScratchPlaneState {
    grants: Arc<Mutex<HashSet<(String, u64, String)>>>,
    uploads: Arc<Mutex<HashMap<String, UploadSession>>>,
    locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    subscribed: super::lifecycle::Subscribed,
    /// Each machine's measured sync throughput, bytes per second, as an
    /// exponentially weighted moving average of what its reports moved.
    throughput: Arc<Mutex<HashMap<String, f64>>>,
}

/// Throughput assumed for a machine nothing has measured yet.
pub const UNMEASURED_REMOTE_BYTES_PER_SECOND: f64 = 10.0 * 1024.0 * 1024.0;
pub const UNMEASURED_COLOCATED_BYTES_PER_SECOND: f64 = 100.0 * 1024.0 * 1024.0;
/// How much one observation moves the average.
const THROUGHPUT_WEIGHT: f64 = 0.3;
/// Transfers too small to say anything about a link are not observed.
const THROUGHPUT_MIN_BYTES: u64 = 256 * 1024;

impl ScratchPlaneState {
    pub fn observe_throughput(&self, executor_id: &str, bytes: u64, ms: u64) {
        if bytes < THROUGHPUT_MIN_BYTES || ms == 0 {
            return;
        }
        let observed = bytes as f64 / (ms as f64 / 1000.0);
        let mut throughput = self.throughput.lock().unwrap();
        let average = throughput.entry(executor_id.to_owned()).or_insert(observed);
        *average = *average * (1.0 - THROUGHPUT_WEIGHT) + observed * THROUGHPUT_WEIGHT;
    }

    /// What placement prices a machine's sync at.
    pub fn throughput(&self, executor_id: &str, colocated: bool) -> f64 {
        self.throughput
            .lock()
            .unwrap()
            .get(executor_id)
            .copied()
            .unwrap_or(if colocated {
                UNMEASURED_COLOCATED_BYTES_PER_SECOND
            } else {
                UNMEASURED_REMOTE_BYTES_PER_SECOND
            })
    }

    pub(super) fn subscribed(&self) -> &super::lifecycle::Subscribed {
        &self.subscribed
    }

    pub fn authorize(&self, executor_id: &str, generation: u64, scratch_id: &str) {
        self.grants.lock().unwrap().insert((
            executor_id.to_owned(),
            generation,
            scratch_id.to_owned(),
        ));
    }

    pub fn authorizes(&self, executor_id: &str, generation: u64, scratch_id: &str) -> bool {
        self.grants.lock().unwrap().contains(&(
            executor_id.to_owned(),
            generation,
            scratch_id.to_owned(),
        ))
    }

    /// A connection ended: nothing it was granted survives it.
    pub fn revoke_connection(&self, executor_id: &str, generation: u64) {
        self.grants
            .lock()
            .unwrap()
            .retain(|(executor, held, _)| executor != executor_id || *held != generation);
        let abandoned: Vec<UploadSession> = {
            let mut uploads = self.uploads.lock().unwrap();
            let ids: Vec<String> = uploads
                .iter()
                .filter(|(_, upload)| {
                    upload.executor_id == executor_id && upload.generation == generation
                })
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| uploads.remove(&id))
                .collect()
        };
        for upload in abandoned {
            let _ = std::fs::remove_dir_all(upload.staging);
        }
    }

    /// A scratch closed: no connection may touch it again.
    pub fn revoke_scratch(&self, scratch_id: &str) {
        self.grants
            .lock()
            .unwrap()
            .retain(|(_, _, held)| held != scratch_id);
        self.locks.lock().unwrap().remove(scratch_id);
    }

    /// The write lock of one scratch. Held from a write-back's re-plan through
    /// its compare-and-swap, its ref update, and any gc.
    pub fn lock(&self, scratch_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap()
            .entry(scratch_id.to_owned())
            .or_default()
            .clone()
    }

    fn expire_uploads(&self, now_ms: i64) {
        let expired: Vec<UploadSession> = {
            let mut uploads = self.uploads.lock().unwrap();
            let ids: Vec<String> = uploads
                .iter()
                .filter(|(_, upload)| upload.created_at_ms + UPLOAD_TTL_MS < now_ms)
                .map(|(id, _)| id.clone())
                .collect();
            ids.into_iter()
                .filter_map(|id| uploads.remove(&id))
                .collect()
        };
        for upload in expired {
            let _ = std::fs::remove_dir_all(upload.staging);
        }
    }
}

/// The scratch routes, to be mounted at `<object channel>/scratch`.
pub fn router(orch: Orchestrator) -> Router {
    Router::new()
        .route(
            "/skeleton",
            post(skeleton).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .route(
            "/blobs",
            post(blobs).layer(DefaultBodyLimit::max(8 * 1024 * 1024)),
        )
        .route(
            "/upload/begin",
            post(begin).layer(DefaultBodyLimit::max(256 * 1024 * 1024)),
        )
        .route(
            "/upload/pack",
            post(upload_pack).layer(DefaultBodyLimit::max(
                (NODE_SCRATCH_MAX_FILE_BYTES + 16 * 1024 * 1024) as usize,
            )),
        )
        .route(
            "/upload/complete",
            post(complete).layer(DefaultBodyLimit::max(64 * 1024)),
        )
        .with_state(orch)
}

type Refusal = (StatusCode, String);

fn refuse(status: StatusCode, detail: impl Into<String>) -> Refusal {
    (status, detail.into())
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn authenticate(
    orch: &Orchestrator,
    route: &str,
    headers: &HeaderMap,
) -> Result<(String, u64), Refusal> {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| refuse(StatusCode::UNAUTHORIZED, "missing object-channel bearer"))?;
    let session = orch
        .object_plane
        .authenticate(token, now_ms().max(0) as u64)
        .map_err(|refusal| {
            orch.object_plane.report_refusal(route, None, &refusal);
            refuse(
                StatusCode::UNAUTHORIZED,
                "invalid object-channel credential",
            )
        })?;
    Ok((session.executor_id, session.generation))
}

/// The open scratch `scratch_id`, if this connection may touch it.
async fn authorized_scratch(
    orch: &Orchestrator,
    executor_id: &str,
    generation: u64,
    scratch_id: &str,
) -> Result<NodeScratch, Refusal> {
    if !orch
        .object_plane
        .scratch
        .authorizes(executor_id, generation, scratch_id)
    {
        return Err(refuse(
            StatusCode::FORBIDDEN,
            format!("this connection holds no grant for node scratch {scratch_id}"),
        ));
    }
    match owner::load(&orch.db.local, scratch_id).await {
        Ok(Some(scratch)) if scratch.closed_at.is_none() => Ok(scratch),
        Ok(Some(_)) => Err(refuse(
            StatusCode::GONE,
            format!("node scratch {scratch_id} is closed"),
        )),
        Ok(None) => Err(refuse(
            StatusCode::NOT_FOUND,
            format!("no node scratch {scratch_id}"),
        )),
        Err(error) => Err(refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("read node scratch {scratch_id}: {error}"),
        )),
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, Refusal> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| refuse(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .map_err(|error| refuse(StatusCode::UNPROCESSABLE_ENTITY, error))
}

fn pack_response(pack: Vec<u8>, served: Option<usize>) -> Response {
    let hash = crate::orchestrator::object_plane::content_sha256(&pack);
    let mut response = Response::new(axum::body::Body::from(pack));
    let headers = response.headers_mut();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/x-git-packed-objects"),
    );
    if let Ok(value) = HeaderValue::from_str(&hash) {
        headers.insert("x-cairn-content-sha256", value);
    }
    if let Some(served) = served {
        headers.insert(SCRATCH_SERVED_HEADER, HeaderValue::from(served));
    }
    response
}

fn into_response<T: IntoResponse>(result: Result<T, Refusal>) -> Response {
    match result {
        Ok(response) => response.into_response(),
        Err(refusal) => refusal.into_response(),
    }
}

async fn skeleton(
    State(orch): State<Orchestrator>,
    headers: HeaderMap,
    Json(request): Json<ScratchSkeletonRequest>,
) -> Response {
    into_response(
        async {
            let (executor, generation) = authenticate(&orch, "/scratch/skeleton", &headers)?;
            let scratch =
                authorized_scratch(&orch, &executor, generation, &request.scratch_id).await?;
            let config_dir = orch.config_dir.clone();
            let pack = blocking(move || {
                let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
                if !repo.has_commit(&request.want_commit) {
                    return Err(format!(
                        "node scratch {} holds no commit {}",
                        scratch.id, request.want_commit
                    ));
                }
                let objects =
                    repo.skeleton_objects(&request.want_commit, request.have_commit.as_deref())?;
                if objects.is_empty() {
                    return Ok(None);
                }
                repo.pack_of(&objects).map(Some)
            })
            .await?;
            Ok(match pack {
                Some(pack) => pack_response(pack, None),
                None => StatusCode::NO_CONTENT.into_response(),
            })
        }
        .await,
    )
}

async fn blobs(
    State(orch): State<Orchestrator>,
    headers: HeaderMap,
    Json(request): Json<ScratchBlobsRequest>,
) -> Response {
    into_response(
        async {
            let (executor, generation) = authenticate(&orch, "/scratch/blobs", &headers)?;
            let scratch =
                authorized_scratch(&orch, &executor, generation, &request.scratch_id).await?;
            if request.oids.is_empty() {
                return Err(refuse(StatusCode::BAD_REQUEST, "no blobs requested"));
            }
            let config_dir = orch.config_dir.clone();
            let (pack, served) = blocking(move || {
                let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
                let sizes = repo.sizes(&request.oids)?;
                let mut take = Vec::new();
                let mut bytes = 0u64;
                for (oid, size) in request.oids.iter().zip(sizes) {
                    let size = size.ok_or_else(|| {
                        format!("node scratch {} holds no object {oid}", scratch.id)
                    })?;
                    if !take.is_empty() && bytes + size > NODE_SCRATCH_TRANSFER_BATCH_BYTES {
                        break;
                    }
                    bytes += size;
                    take.push(oid.clone());
                }
                let served = take.len();
                repo.pack_of(&take).map(|pack| (pack, served))
            })
            .await?;
            Ok(pack_response(pack, Some(served)))
        }
        .await,
    )
}

fn validate_changes(changes: &[ScratchChange]) -> Result<(), Refusal> {
    if changes.len() > MAX_CHANGES {
        return Err(refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("a write-back may name at most {MAX_CHANGES} paths"),
        ));
    }
    for change in changes {
        let path = &change.path.0;
        let bad_path = path.is_empty()
            || path.contains(&0)
            || path.starts_with(b"/")
            || path
                .split(|byte| *byte == b'/')
                .any(|part| part.is_empty() || part == b"." || part == b".." || part == b".git");
        let bad_blob = [change.base.as_ref(), change.new.as_ref()]
            .into_iter()
            .flatten()
            .any(|blob| {
                !cairn_codec::scratch_tree::valid_oid(&blob.oid)
                    || !matches!(blob.mode, 0o100644 | 0o100755 | 0o120000)
            });
        if bad_path || bad_blob {
            return Err(refuse(
                StatusCode::BAD_REQUEST,
                format!("unacceptable scratch change at {:?}", change.path.display()),
            ));
        }
    }
    Ok(())
}

async fn begin(
    State(orch): State<Orchestrator>,
    headers: HeaderMap,
    Json(request): Json<ScratchUploadBegin>,
) -> Response {
    into_response(
        async {
            let (executor, generation) = authenticate(&orch, "/scratch/upload/begin", &headers)?;
            let scratch =
                authorized_scratch(&orch, &executor, generation, &request.scratch_id).await?;
            validate_changes(&request.changes)?;
            orch.object_plane.scratch.expire_uploads(now_ms());
            let ceiling = owner::ceiling_bytes(&orch, &scratch.project_id).await;
            let config_dir = orch.config_dir.clone();
            let planned = {
                let scratch = scratch.clone();
                let changes = request.changes.clone();
                let writer = request.writer.clone();
                blocking(move || {
                    let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
                    let head = store::tree(&repo, scratch.head_commit.as_deref())?;
                    let plan = merge::plan(&head, &changes, &writer);
                    if let Some(refusal) =
                        merge::refusal(&plan, scratch.head_bytes, ceiling, &scratch.owner_uri)
                    {
                        return Ok(Err(refusal));
                    }
                    let incoming: Vec<String> = plan
                        .incoming
                        .iter()
                        .map(|(_, blob)| blob.oid.clone())
                        .collect();
                    repo.missing(&incoming).map(Ok)
                })
                .await?
            };
            let missing = match planned {
                Ok(missing) => missing,
                Err(refusal) => {
                    if let Err(error) = owner::record_refusal(
                        &orch.db.local,
                        &scratch.id,
                        &executor,
                        refusal.merged_bytes,
                    )
                    .await
                    {
                        log::warn!("record node scratch refusal {}: {error}", scratch.id);
                    }
                    return Ok(Json(ScratchUploadPlan::Refused(refusal)));
                }
            };
            let upload_id = uuid::Uuid::new_v4().to_string();
            let staging = store::store_root(&orch.config_dir)
                .join(".uploads")
                .join(&upload_id);
            orch.object_plane.scratch.uploads.lock().unwrap().insert(
                upload_id.clone(),
                UploadSession {
                    scratch_id: scratch.id,
                    executor_id: executor,
                    generation,
                    changes: request.changes,
                    writer: request.writer,
                    staging,
                    packs: Vec::new(),
                    created_at_ms: now_ms(),
                },
            );
            Ok(Json(ScratchUploadPlan::Proceed { upload_id, missing }))
        }
        .await,
    )
}

/// The session `upload_id`, checked against the connection presenting it.
fn session_paths(
    orch: &Orchestrator,
    upload_id: &str,
    executor: &str,
    generation: u64,
) -> Result<(String, PathBuf), Refusal> {
    let uploads = orch.object_plane.scratch.uploads.lock().unwrap();
    let session = uploads.get(upload_id).ok_or_else(|| {
        refuse(
            StatusCode::NOT_FOUND,
            format!("no write-back {upload_id} is in flight; begin again"),
        )
    })?;
    if session.executor_id != executor || session.generation != generation {
        return Err(refuse(
            StatusCode::FORBIDDEN,
            "that write-back belongs to another connection",
        ));
    }
    Ok((session.scratch_id.clone(), session.staging.clone()))
}

async fn upload_pack(
    State(orch): State<Orchestrator>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    into_response(
        async {
            let (executor, generation) = authenticate(&orch, "/scratch/upload/pack", &headers)?;
            let upload_id = headers
                .get(SCRATCH_UPLOAD_HEADER)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| refuse(StatusCode::BAD_REQUEST, "missing upload id header"))?
                .to_owned();
            let (scratch_id, staging) = session_paths(&orch, &upload_id, &executor, generation)?;
            authorized_scratch(&orch, &executor, generation, &scratch_id).await?;
            let path = blocking(move || {
                // Validated on arrival so a corrupt pack is refused here, on
                // the call that sent it, rather than at complete.
                cairn_codec::transfer::validate_pack(
                    &body,
                    cairn_codec::transfer::PackLimits::default(),
                )?;
                std::fs::create_dir_all(&staging)
                    .map_err(|error| format!("stage scratch upload: {error}"))?;
                let path = staging.join(format!("{}.pack", uuid::Uuid::new_v4().simple()));
                std::fs::write(&path, &body)
                    .map_err(|error| format!("stage scratch pack: {error}"))?;
                Ok(path)
            })
            .await?;
            if let Some(session) = orch
                .object_plane
                .scratch
                .uploads
                .lock()
                .unwrap()
                .get_mut(&upload_id)
            {
                session.packs.push(path);
            }
            Ok(StatusCode::NO_CONTENT)
        }
        .await,
    )
}

async fn complete(
    State(orch): State<Orchestrator>,
    headers: HeaderMap,
    Json(request): Json<ScratchUploadComplete>,
) -> Response {
    into_response(
        async {
            let (executor, generation) = authenticate(&orch, "/scratch/upload/complete", &headers)?;
            let (scratch_id, _) = session_paths(&orch, &request.upload_id, &executor, generation)?;
            let lock = orch.object_plane.scratch.lock(&scratch_id);
            let _held = lock.lock().await;
            // Re-read under the lock: the head this write-back folds into is the
            // head as it is now, not as it was at begin.
            let scratch = authorized_scratch(&orch, &executor, generation, &scratch_id).await?;
            let ceiling = owner::ceiling_bytes(&orch, &scratch.project_id).await;
            let (changes, writer, packs) = {
                let uploads = orch.object_plane.scratch.uploads.lock().unwrap();
                let session = uploads.get(&request.upload_id).ok_or_else(|| {
                    refuse(
                        StatusCode::NOT_FOUND,
                        "that write-back is no longer in flight",
                    )
                })?;
                (
                    session.changes.clone(),
                    session.writer.clone(),
                    session.packs.clone(),
                )
            };
            let config_dir = orch.config_dir.clone();
            enum Folded {
                Refused(cairn_common::executor_protocol::ScratchRefusal),
                Missing(Vec<String>),
                Ready {
                    head: String,
                    head_bytes: u64,
                    added: u64,
                    conflicts: Vec<cairn_common::executor_protocol::ScratchConflict>,
                },
            }
            let folded = {
                let scratch = scratch.clone();
                blocking(move || {
                    let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
                    for pack in &packs {
                        let bytes = std::fs::read(pack)
                            .map_err(|error| format!("read staged scratch pack: {error}"))?;
                        repo.install_pack(&bytes)?;
                    }
                    let head = store::tree(&repo, scratch.head_commit.as_deref())?;
                    let plan = merge::plan(&head, &changes, &writer);
                    if let Some(refusal) =
                        merge::refusal(&plan, scratch.head_bytes, ceiling, &scratch.owner_uri)
                    {
                        return Ok(Folded::Refused(refusal));
                    }
                    let incoming: Vec<String> = plan
                        .incoming
                        .iter()
                        .map(|(_, blob)| blob.oid.clone())
                        .collect();
                    let missing = repo.missing(&incoming)?;
                    if !missing.is_empty() {
                        return Ok(Folded::Missing(missing));
                    }
                    let new_head = match (&scratch.head_commit, plan.changed) {
                        (Some(head), false) => head.clone(),
                        _ => store::commit(&repo, &plan.result)?,
                    };
                    Ok(Folded::Ready {
                        head: new_head,
                        head_bytes: plan.merged_bytes,
                        added: plan.added_bytes(&head),
                        conflicts: plan.conflicts,
                    })
                })
                .await?
            };
            let (head, head_bytes, added, conflicts) = match folded {
                Folded::Refused(refusal) => {
                    let _ = owner::record_refusal(
                        &orch.db.local,
                        &scratch.id,
                        &executor,
                        refusal.merged_bytes,
                    )
                    .await;
                    finish_upload(&orch, &request.upload_id);
                    return Ok(Json(ScratchUploadResult::Refused(refusal)));
                }
                Folded::Missing(missing) => {
                    return Ok(Json(ScratchUploadResult::Missing { missing }));
                }
                Folded::Ready {
                    head,
                    head_bytes,
                    added,
                    conflicts,
                } => (head, head_bytes, added, conflicts),
            };
            let advanced = if Some(&head) == scratch.head_commit.as_ref() {
                true
            } else {
                owner::advance_head(
                    &orch.db.local,
                    &scratch.id,
                    scratch.head_commit.as_deref(),
                    &head,
                    head_bytes,
                    added,
                    &executor,
                )
                .await
                .map_err(|error| refuse(StatusCode::SERVICE_UNAVAILABLE, error.to_string()))?
            };
            if !advanced {
                // Only another runner process could move the head while this
                // one holds the lock; complete again folds into the new head.
                return Err(refuse(
                    StatusCode::CONFLICT,
                    "the node scratch head moved during this write-back; complete again",
                ));
            }
            let written_bytes = scratch.written_bytes + added;
            if let Err(error) = owner::record_replica(
                &orch.db.local,
                &Replica {
                    scratch_id: scratch.id.clone(),
                    executor_id: executor.clone(),
                    synced_commit: Some(head.clone()),
                    synced_written_bytes: written_bytes,
                    dirty: false,
                    refused_bytes: None,
                },
            )
            .await
            {
                log::warn!("record node scratch replica {}: {error}", scratch.id);
            }
            let config_dir = orch.config_dir.clone();
            let maintained = {
                let scratch = scratch.clone();
                let head = head.clone();
                tokio::task::spawn_blocking(move || {
                    let repo = store::open(&config_dir, &scratch.project_id, &scratch.id)?;
                    store::maintain(&repo, Some(&head), head_bytes)
                })
                .await
            };
            if let Ok(Err(error)) | Err(error) = maintained.map_err(|e| e.to_string()) {
                log::warn!("maintain node scratch store {}: {error}", scratch.id);
            }
            finish_upload(&orch, &request.upload_id);
            let head_seq = if Some(&head) == scratch.head_commit.as_ref() {
                scratch.head_seq
            } else {
                scratch.head_seq + 1
            };
            Ok(Json(ScratchUploadResult::Applied {
                head,
                head_bytes,
                head_seq,
                conflicts,
            }))
        }
        .await,
    )
}

fn finish_upload(orch: &Orchestrator, upload_id: &str) {
    let session = orch
        .object_plane
        .scratch
        .uploads
        .lock()
        .unwrap()
        .remove(upload_id);
    if let Some(session) = session {
        let _ = std::fs::remove_dir_all(session.staging);
    }
}
