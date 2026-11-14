use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use cairn_common::executor_protocol::{
    object_fetch_stream, CellRequest, CloudObjectGrantRequest, CloudObjectOperation,
    DeltaUploadReceipt, ObjectFetchFailure, ObjectFetchFailureClass, ObjectFetchFailureCode,
    ObjectTransferCoordinate,
};
use sha2::{Digest, Sha256};

use crate::storage::content_store::ContentStore;
use crate::storage::pack_catalog::PackCatalogPublication;
use crate::storage::{DbResult, LocalDb, RowExt};

const CREDENTIAL_TTL_MS: u64 = 15 * 60 * 1000;
const STAGED_UPLOAD_TTL_MS: u64 = 60 * 60 * 1000;
/// How many retired token hashes are kept to name a refused credential. A
/// retired token outlives its usefulness once it would have expired anyway.
const RETIRED_CREDENTIAL_CAP: usize = 256;
/// At most one refusal line per executor in this interval; the rest are
/// counted into the next.
const REFUSAL_REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct ValidatedObjectPack {
    pub pack: Vec<u8>,
    pub(crate) index: Vec<u8>,
    pub(crate) framed: Vec<u8>,
    pub(crate) content_hash: String,
    pub pack_checksum: String,
    pub(crate) object_count: u64,
}

pub fn validate_pack_bytes(pack: Vec<u8>) -> Result<ValidatedObjectPack, String> {
    let validated = cairn_codec::transfer::validate_pack(
        &pack,
        cairn_codec::transfer::PackLimits::default(),
        &std::env::temp_dir(),
    )
    .map_err(|error| format!("validate object pack: {error}"))?;
    let index = validated.index.clone();
    let framed = cairn_codec::transfer::frame_pack(&pack, &index);
    Ok(ValidatedObjectPack {
        content_hash: content_sha256(&framed),
        pack_checksum: validated.manifest.pack_checksum,
        object_count: validated.manifest.object_count,
        pack,
        index,
        framed,
    })
}

pub fn validate_framed_pack(framed: Vec<u8>) -> Result<ValidatedObjectPack, String> {
    let expected_hash = content_sha256(&framed);
    let (pack, supplied_index) = cairn_codec::transfer::unframe_pack(&framed)
        .map_err(|error| format!("unframe object pack: {error}"))?;
    let validated = validate_pack_bytes(pack)?;
    if validated.index != supplied_index {
        return Err("object pack index disagrees with independent validation".into());
    }

    if validated.content_hash != expected_hash {
        return Err("object pack framing changed during validation".into());
    }
    Ok(validated)
}

/// Put validated bytes before publishing their catalog owner reference. An
/// absent content store is a complete local-only mode, not an error.
pub(crate) async fn publish_validated_pack(
    db: &LocalDb,
    store: Option<&dyn ContentStore>,
    pack: &ValidatedObjectPack,
    mut publication: PackCatalogPublication,
) -> Result<(), String> {
    publication.content_hash = pack.content_hash.clone();
    publication.byte_count = pack.framed.len() as u64;
    publication.pack_checksum = pack.pack_checksum.clone();
    publication.object_count = pack.object_count;
    if let Some(store) = store {
        store
            .put(&pack.content_hash, &pack.framed)
            .await
            .map_err(|error| format!("put object pack {}: {error}", pack.content_hash))?;
        publish_validated_reference(db, pack, publication).await?;
    }
    Ok(())
}

/// Publish a reference for bytes already durably present in the content store.
/// This is the completion seam for direct executor uploads: callers must first
/// fetch and validate the addressed bytes.
pub(crate) async fn publish_validated_reference(
    db: &LocalDb,
    pack: &ValidatedObjectPack,
    mut publication: PackCatalogPublication,
) -> Result<(), String> {
    publication.content_hash = pack.content_hash.clone();
    publication.byte_count = pack.framed.len() as u64;
    publication.pack_checksum = pack.pack_checksum.clone();
    publication.object_count = pack.object_count;
    crate::storage::pack_catalog::publish_pack(db, publication)
        .await
        .map_err(|error| format!("publish object pack catalog: {error}"))
}

/// Resolve durable catalog truth. Local object presence is deliberately
/// irrelevant; only referenced, published entries participate.
pub async fn resolve_catalog_chain(
    db: &LocalDb,
    project_id: &str,
    repository_id: &str,
    want_commit: &str,
    have_commits: &[String],
) -> DbResult<Vec<(String, u64, String, Option<String>, String)>> {
    let mut tip = want_commit.to_owned();
    let mut chain = Vec::new();
    for _ in 0..128 {
        if have_commits.iter().any(|have| have == &tip) {
            break;
        }
        let Some(row) = db
            .query_opt(
                "SELECT content_hash, byte_count, pack_checksum, base_commit, tip_commit
                 FROM pack_catalog c WHERE c.project_id = ?1 AND c.repository_id = ?2
                   AND c.object_format = 'sha1' AND c.tip_commit = ?3
                   AND c.publication_state = 'published'
                   AND EXISTS (SELECT 1 FROM pack_catalog_references r
                     WHERE r.content_hash = c.content_hash AND r.project_id = c.project_id
                       AND r.repository_id = c.repository_id
                       AND r.object_format = c.object_format)
                 ORDER BY CASE kind WHEN 'reachable' THEN 0 ELSE 1 END,
                          created_at DESC LIMIT 1",
                (project_id.to_owned(), repository_id.to_owned(), tip.clone()),
                |row| {
                    Ok((
                        row.text(0)?,
                        row.i64(1)? as u64,
                        row.text(2)?,
                        row.opt_text(3)?,
                        row.text(4)?,
                    ))
                },
            )
            .await?
        else {
            return Ok(Vec::new());
        };
        tip = match &row.3 {
            Some(base) => base.clone(),
            None => {
                chain.push(row);
                break;
            }
        };
        chain.push(row);
    }
    chain.reverse();
    Ok(chain)
}

/// What an executor's object fetch asks this runner for.
pub struct RunnerFetchRequest {
    /// The project's store repository, read without the project-store lock.
    pub repository: PathBuf,
    /// Where the reachable listing and the pack being sent are spooled. Only
    /// one pack is ever there at once.
    pub scratch: PathBuf,
    pub coordinate: ObjectTransferCoordinate,
    pub want_commit: String,
    pub have_commits: Vec<String>,
    pub limits: cairn_codec::transfer::PackLimits,
}

/// How many chunks of pack data wait between the builder and the network.
/// The builder blocks once they are full, so a slow executor slows the build
/// rather than filling memory.
const FETCH_STREAM_CHANNEL_CHUNKS: usize = 4;
const FETCH_STREAM_CHUNK_BYTES: usize = 1024 * 1024;

/// A runner object fetch that has begun: the closure, as a fetch stream
/// (see [`cairn_common::executor_protocol::object_fetch_stream`]) produced on
/// a blocking thread one pack at a time. Dropping it (the executor went away)
/// stops the producer at its next chunk and removes its scratch.
pub struct RunnerFetchStream {
    chunks: tokio::sync::mpsc::Receiver<Bytes>,
}

impl RunnerFetchStream {
    pub fn into_response(self) -> axum::response::Response {
        let body = futures_util::stream::unfold(self.chunks, |mut chunks| async move {
            chunks
                .recv()
                .await
                .map(|chunk| (Ok::<_, std::io::Error>(chunk), chunks))
        });
        let mut response = axum::response::Response::new(axum::body::Body::from_stream(body));
        response.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static(object_fetch_stream::CONTENT_TYPE),
        );
        response
    }
}

/// The HTTP answer to a refused object request: the status its class names,
/// with the typed failure as the body, which is what the executor reads.
pub fn object_fetch_refusal(failure: &ObjectFetchFailure) -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    let status = match failure.class {
        ObjectFetchFailureClass::Deterministic | ObjectFetchFailureClass::Integrity => {
            StatusCode::UNPROCESSABLE_ENTITY
        }
        ObjectFetchFailureClass::NotApplicable => StatusCode::CONFLICT,
        ObjectFetchFailureClass::Unavailable => StatusCode::NOT_FOUND,
        ObjectFetchFailureClass::Transient => StatusCode::SERVICE_UNAVAILABLE,
        ObjectFetchFailureClass::StaleAuthorization => StatusCode::FORBIDDEN,
    };
    (status, axum::Json(failure.clone())).into_response()
}

/// Begin answering an object fetch.
///
/// Resolves once the first pack is validated (or the closure is known to
/// need none), so a refusal decided before any byte is sent (an unknown
/// commit, an object no pack can hold among the first) is an ordinary typed
/// error response. A failure after that ends the stream with its Error
/// record. Call without the project-store lock.
///
/// A have this store does not hold narrows nothing and is dropped, rather
/// than failing the fetch: an executor offers its checkouts' heads, and one
/// may stand on a commit it sealed that has not reached this runner.
pub async fn open_runner_fetch(
    request: RunnerFetchRequest,
) -> Result<RunnerFetchStream, ObjectFetchFailure> {
    let (started, answer) = tokio::sync::oneshot::channel();
    let (sender, chunks) = tokio::sync::mpsc::channel(FETCH_STREAM_CHANNEL_CHUNKS);
    tokio::task::spawn_blocking(move || produce_runner_fetch(request, started, sender));
    match answer.await {
        Ok(Ok(())) => Ok(RunnerFetchStream { chunks }),
        Ok(Err(failure)) => Err(failure),
        Err(_) => Err(ObjectFetchFailure::new(
            ObjectFetchFailureCode::PackConstruction,
            "the runner's object fetch producer stopped without answering",
        )),
    }
}

type FetchStarted = tokio::sync::oneshot::Sender<Result<(), ObjectFetchFailure>>;

fn produce_runner_fetch(
    request: RunnerFetchRequest,
    started: FetchStarted,
    sender: tokio::sync::mpsc::Sender<Bytes>,
) {
    use cairn_codec::transfer::{build_bounded_packs, known_commits, shallow_boundary};
    let repository = &request.repository;
    let want = &request.want_commit;
    let prepared = (|| {
        let unresolvable = || {
            ObjectFetchFailure::new(
                ObjectFetchFailureCode::CommitUnresolvable,
                format!("the runner cannot resolve commit {want}"),
            )
        };
        if known_commits(repository, std::slice::from_ref(want))
            .map_err(|_| unresolvable())?
            .is_empty()
        {
            return Err(unresolvable());
        }
        let shallow = shallow_boundary(repository, want).map_err(|error| {
            ObjectFetchFailure::new(ObjectFetchFailureCode::PackConstruction, error)
        })?;
        if shallow.len() > object_fetch_stream::MAX_SHALLOW_COMMITS {
            return Err(ObjectFetchFailure::new(
                ObjectFetchFailureCode::ShallowBoundaryTooWide,
                format!(
                    "the source's shallow boundary for {want} is {} commits, over the transfer limit of {}",
                    shallow.len(),
                    object_fetch_stream::MAX_SHALLOW_COMMITS
                ),
            ));
        }
        let haves = known_commits(repository, &request.have_commits).map_err(|error| {
            ObjectFetchFailure::new(ObjectFetchFailureCode::PackConstruction, error)
        })?;
        std::fs::create_dir_all(&request.scratch).map_err(|error| {
            ObjectFetchFailure::new(
                ObjectFetchFailureCode::PackConstruction,
                format!("creating object fetch scratch: {error}"),
            )
        })?;
        reclaim_stale_fetch_scratch(&request.scratch);
        Ok((shallow, haves))
    })();
    let (shallow, haves) = match prepared {
        Ok(prepared) => prepared,
        Err(failure) => {
            let _ = started.send(Err(failure));
            return;
        }
    };

    let mut encoder = object_fetch_stream::StreamEncoder::new();
    let opening = encoder
        .open(&request.coordinate, want, &shallow)
        .expect("the boundary was bounded above");
    let mut pending_start = Some((started, opening));
    let send = |bytes: Vec<u8>| {
        sender
            .blocking_send(Bytes::from(bytes))
            .map_err(|_| "the executor stopped reading the object fetch".to_string())
    };
    // Nothing is sent until there is something to send, so a refusal
    // decided before the first pack is an ordinary error response.
    let begin = |pending: &mut Option<(FetchStarted, Vec<u8>)>| -> Result<(), String> {
        if let Some((started, opening)) = pending.take() {
            let _ = started.send(Ok(()));
            send(opening)?;
        }
        Ok(())
    };
    let built = build_bounded_packs(
        repository,
        std::slice::from_ref(want),
        &haves,
        request.limits,
        &request.scratch,
        &mut |pack| {
            begin(&mut pending_start)?;
            let sha256 = sha256_file(&pack.pack_path)?;
            send(encoder.pack(object_fetch_stream::PackDescriptor {
                byte_count: pack.manifest.pack_bytes,
                sha256,
                pack_checksum: pack.manifest.pack_checksum.clone(),
                object_count: pack.manifest.object_count,
            }))?;
            let mut file = std::fs::File::open(&pack.pack_path)
                .map_err(|error| format!("reading built pack: {error}"))?;
            let mut sent = 0u64;
            loop {
                let mut chunk = vec![0u8; FETCH_STREAM_CHUNK_BYTES];
                let read = std::io::Read::read(&mut file, &mut chunk)
                    .map_err(|error| format!("reading built pack: {error}"))?;
                if read == 0 {
                    break;
                }
                chunk.truncate(read);
                sent += read as u64;
                send(chunk)?;
            }
            if sent != pack.manifest.pack_bytes {
                return Err(format!(
                    "built pack changed while it was sent: {sent} of {} bytes",
                    pack.manifest.pack_bytes
                ));
            }
            Ok(())
        },
    );
    let failure = match built {
        Ok(summary) => {
            if begin(&mut pending_start)
                .and_then(|()| send(encoder.end()))
                .is_ok()
            {
                log::info!(
                    "served object fetch for {want} to executor {} request {}: {} packs, {} objects, {} bytes",
                    request.coordinate.executor_id,
                    request.coordinate.request_id,
                    summary.packs,
                    summary.objects,
                    summary.pack_bytes,
                );
            }
            return;
        }
        Err(cairn_codec::transfer::BoundedPackError::Stopped(reason)) => {
            log::info!(
                "object fetch for {want} to executor {} request {} stopped: {reason}",
                request.coordinate.executor_id,
                request.coordinate.request_id,
            );
            return;
        }
        Err(error) => bounded_pack_failure(error),
    };
    log::warn!(
        "managed object fetch for {want} to executor {} request {} attempt {} refused: {failure}",
        request.coordinate.executor_id,
        request.coordinate.request_id,
        request.coordinate.attempt_id,
    );
    match pending_start.take() {
        Some((started, _)) => {
            let _ = started.send(Err(failure));
        }
        None => {
            let _ = send(object_fetch_stream::StreamEncoder::error(&failure));
        }
    }
}

/// How old an entry in the fetch scratch must be before a later fetch
/// removes it. Every fetch removes its own spool on every return, so an older
/// one was left by a runner that died mid-fetch; a live fetch is never this
/// old, because the executor's whole-request deadline is ten minutes.
const STALE_FETCH_SCRATCH: Duration = Duration::from_secs(24 * 60 * 60);

fn reclaim_stale_fetch_scratch(scratch: &Path) {
    let Ok(entries) = std::fs::read_dir(scratch) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|modified| modified.elapsed().ok())
            .is_some_and(|age| age > STALE_FETCH_SCRATCH);
        if stale {
            let path = entry.path();
            let removed = if path.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            if let Err(error) = removed {
                log::warn!(
                    "could not remove stale object fetch scratch {}: {error}",
                    path.display()
                );
            }
        }
    }
}

/// The typed failure a bounded pack build ended in.
fn bounded_pack_failure(error: cairn_codec::transfer::BoundedPackError) -> ObjectFetchFailure {
    use cairn_codec::transfer::{BoundedPackError, LimitDimension};
    use cairn_common::executor_protocol::{
        OversizedObject, PackLimitDimension, PackLimitViolation,
    };
    match error {
        BoundedPackError::Oversized { object, violation } => ObjectFetchFailure::oversized_object(
            OversizedObject {
                oid: object.oid,
                kind: object.kind,
                path: object.path,
                canonical_bytes: object.canonical_bytes,
                packed_bytes: object.packed_bytes,
            },
            PackLimitViolation {
                dimension: match violation.dimension {
                    LimitDimension::PackBytes => PackLimitDimension::PackBytes,
                    LimitDimension::ObjectCount => PackLimitDimension::ObjectCount,
                    LimitDimension::InflatedBytes => PackLimitDimension::InflatedBytes,
                },
                actual: violation.actual,
                limit: violation.limit,
            },
        ),
        BoundedPackError::SourceMissing(message) => {
            ObjectFetchFailure::new(ObjectFetchFailureCode::SourceObjectsMissing, message)
        }
        BoundedPackError::Failed(message) | BoundedPackError::Stopped(message) => {
            ObjectFetchFailure::new(ObjectFetchFailureCode::PackConstruction, message)
        }
    }
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file =
        std::fs::File::open(path).map_err(|error| format!("reading built pack: {error}"))?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)
        .map_err(|error| format!("hashing built pack: {error}"))?;
    Ok(hex_hash(hasher.finalize().into()))
}

#[derive(Clone, Debug)]
struct ObjectCredentialSession {
    device_id: String,
    runner_device_id: String,
    generation: u64,
    token_hash: [u8; 32],
    expires_at_unix_ms: u64,
    previous_token: Option<PreviousObjectCredential>,
}

#[derive(Clone, Debug)]
struct PreviousObjectCredential {
    token_hash: [u8; 32],
    expires_at_unix_ms: u64,
}

/// Why a token stopped being accepted before its expiry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CredentialRetirement {
    /// A newer connection generation of the same executor was issued.
    Superseded { by: u64 },
    /// Its own generation rotated it out and its grace passed.
    Rotated,
    /// Its connection ended.
    Revoked,
}

impl std::fmt::Display for CredentialRetirement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Superseded { by } => write!(f, "superseded by generation {by}"),
            Self::Rotated => f.write_str("rotated"),
            Self::Revoked => f.write_str("revoked"),
        }
    }
}

/// The session an executor holds now, named beside a refusal so the line says
/// what the presented token lost to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredObjectSession {
    pub generation: u64,
    pub expires_at_unix_ms: u64,
}

/// Why an object-channel token was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectAuthRefusal {
    /// Not a token this runner issued, or one retired long enough ago that it
    /// is no longer remembered.
    Unknown,
    Expired {
        executor_id: String,
        generation: u64,
        expired_at_unix_ms: u64,
        stored: Option<StoredObjectSession>,
    },
    Retired {
        executor_id: String,
        generation: u64,
        retired_at_unix_ms: u64,
        cause: CredentialRetirement,
        stored: Option<StoredObjectSession>,
    },
}

impl ObjectAuthRefusal {
    pub fn executor_id(&self) -> Option<&str> {
        match self {
            Self::Unknown => None,
            Self::Expired { executor_id, .. } | Self::Retired { executor_id, .. } => {
                Some(executor_id)
            }
        }
    }

    /// What counts as the same refusal when repeats are folded together.
    fn signature(&self) -> String {
        match self {
            Self::Unknown => "unknown".into(),
            Self::Expired { generation, .. } => format!("expired:{generation}"),
            Self::Retired {
                generation, cause, ..
            } => format!("retired:{generation}:{cause}"),
        }
    }
}

impl std::fmt::Display for ObjectAuthRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stored =
            |f: &mut std::fmt::Formatter<'_>, stored: &Option<StoredObjectSession>| match stored {
                Some(stored) => write!(
                    f,
                    " stored_generation={} stored_expires_at_unix_ms={}",
                    stored.generation, stored.expires_at_unix_ms
                ),
                None => f.write_str(" stored=none"),
            };
        match self {
            Self::Unknown => f.write_str("unknown token"),
            Self::Expired {
                executor_id,
                generation,
                expired_at_unix_ms,
                stored: current,
            } => {
                write!(
                    f,
                    "expired executor_id={executor_id} generation={generation} expired_at_unix_ms={expired_at_unix_ms}"
                )?;
                stored(f, current)
            }
            Self::Retired {
                executor_id,
                generation,
                retired_at_unix_ms,
                cause,
                stored: current,
            } => {
                write!(
                    f,
                    "retired executor_id={executor_id} generation={generation} retired_at_unix_ms={retired_at_unix_ms} cause=\"{cause}\""
                )?;
                stored(f, current)
            }
        }
    }
}

#[derive(Clone, Debug)]
struct RetiredObjectCredential {
    token_hash: [u8; 32],
    executor_id: String,
    generation: u64,
    retired_at_unix_ms: u64,
    cause: CredentialRetirement,
}

/// Every executor's live session and the newest connection generation this
/// runner has issued it, under one lock so the two never disagree.
///
/// The newest generation is a one-way mark. A connection task can outlive the
/// connection that replaced it (a runner pump stuck across a link bounce woke
/// to a refresh tick after the executor had redialled), and a session keyed
/// only by executor let that task's refresh replace the live generation's
/// token and its teardown delete it. Generations only rise within a runner
/// process, so an issue below the mark is always a superseded task, and the
/// mark outlives `revoke` so one cannot plant a session after the live one
/// ends either.
#[derive(Default)]
struct CredentialBook {
    sessions: HashMap<String, ObjectCredentialSession>,
    newest_generation: HashMap<String, u64>,
    /// Hashes only, never tokens: enough to name a refused one.
    retired: VecDeque<RetiredObjectCredential>,
}

impl CredentialBook {
    fn retire(
        &mut self,
        token_hash: [u8; 32],
        executor_id: &str,
        generation: u64,
        now_unix_ms: u64,
        cause: CredentialRetirement,
    ) {
        self.retired.push_back(RetiredObjectCredential {
            token_hash,
            executor_id: executor_id.to_owned(),
            generation,
            retired_at_unix_ms: now_unix_ms,
            cause,
        });
        while self.retired.len() > RETIRED_CREDENTIAL_CAP {
            self.retired.pop_front();
        }
    }

    fn prune_retired(&mut self, now_unix_ms: u64) {
        self.retired.retain(|retired| {
            retired.retired_at_unix_ms.saturating_add(CREDENTIAL_TTL_MS) >= now_unix_ms
        });
    }

    fn stored(&self, executor_id: &str) -> Option<StoredObjectSession> {
        self.sessions
            .get(executor_id)
            .map(|session| StoredObjectSession {
                generation: session.generation,
                expires_at_unix_ms: session.expires_at_unix_ms,
            })
    }
}

/// One refusal line per executor per interval, so a burst of retries against
/// a dead token stays readable.
struct RefusalReports(Mutex<crate::repeat_report::KeyedRepeatReporter<String>>);

impl Default for RefusalReports {
    fn default() -> Self {
        Self(Mutex::new(crate::repeat_report::KeyedRepeatReporter::new(
            REFUSAL_REPORT_INTERVAL,
        )))
    }
}

#[derive(Clone, Debug)]
pub struct AuthenticatedObjectSession {
    pub executor_id: String,
    pub device_id: String,
    pub runner_device_id: String,
    pub generation: u64,
}

#[derive(Clone, Debug)]
pub struct StagedDeltaUpload {
    receipt: DeltaUploadReceipt,
    pub path: PathBuf,
    staged_at_unix_ms: u64,
}

#[derive(Clone, Default)]
pub struct ObjectPlaneState {
    credentials: Arc<Mutex<CredentialBook>>,
    refusal_reports: Arc<RefusalReports>,
    authorized: Arc<Mutex<HashMap<(String, String), AuthorizedTransfer>>>,
    staged: Arc<Mutex<HashMap<String, StagedDeltaUpload>>>,
    /// The node scratch routes' grants and in-flight write-backs, revoked with
    /// the connection that holds them.
    pub scratch: crate::node_scratch::plane::ScratchPlaneState,
}

#[derive(Clone, Debug)]
struct AuthorizedTransfer {
    coordinate: ObjectTransferCoordinate,
    base_commit: String,
}

impl ObjectPlaneState {
    /// Issue (or rotate) `generation`'s object-channel credential. `None` when
    /// a newer generation of this executor has already been issued: the caller
    /// is a superseded connection and must not touch the live session.
    pub fn issue_credential(
        &self,
        executor_id: &str,
        device_id: &str,
        runner_device_id: &str,
        generation: u64,
        now_unix_ms: u64,
    ) -> Option<(String, u64)> {
        let mut book = self.credentials.lock().unwrap();
        book.prune_retired(now_unix_ms);
        if let Some(newest) = book
            .newest_generation
            .get(executor_id)
            .copied()
            .filter(|newest| generation < *newest)
        {
            log::warn!(
                "object-channel credential issue refused: executor_id={executor_id} generation={generation} is superseded by generation={newest}"
            );
            return None;
        }
        book.newest_generation
            .insert(executor_id.to_owned(), generation);
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let expires_at_unix_ms = now_unix_ms.saturating_add(CREDENTIAL_TTL_MS);
        let previous_token = match book.sessions.remove(executor_id) {
            Some(session) if session.generation == generation => {
                if let Some(previous) = session.previous_token {
                    book.retire(
                        previous.token_hash,
                        executor_id,
                        generation,
                        now_unix_ms,
                        CredentialRetirement::Rotated,
                    );
                }
                if session.expires_at_unix_ms >= now_unix_ms {
                    Some(PreviousObjectCredential {
                        token_hash: session.token_hash,
                        expires_at_unix_ms: session.expires_at_unix_ms,
                    })
                } else {
                    book.retire(
                        session.token_hash,
                        executor_id,
                        generation,
                        now_unix_ms,
                        CredentialRetirement::Rotated,
                    );
                    None
                }
            }
            Some(session) => {
                let cause = CredentialRetirement::Superseded { by: generation };
                book.retire(
                    session.token_hash,
                    executor_id,
                    session.generation,
                    now_unix_ms,
                    cause,
                );
                if let Some(previous) = session.previous_token {
                    book.retire(
                        previous.token_hash,
                        executor_id,
                        session.generation,
                        now_unix_ms,
                        cause,
                    );
                }
                None
            }
            None => None,
        };
        book.sessions.insert(
            executor_id.to_owned(),
            ObjectCredentialSession {
                device_id: device_id.to_owned(),
                runner_device_id: runner_device_id.to_owned(),
                generation,
                token_hash: hash_bytes(token.as_bytes()),
                expires_at_unix_ms,
                previous_token,
            },
        );
        Some((token, expires_at_unix_ms))
    }

    /// Whether `generation` holds an unexpired session for this executor. A
    /// live connection that finds it does not has lost its session to
    /// something it did not do, and re-issues.
    pub fn holds_session(&self, executor_id: &str, generation: u64, now_unix_ms: u64) -> bool {
        self.credentials
            .lock()
            .unwrap()
            .sessions
            .get(executor_id)
            .is_some_and(|session| {
                session.generation == generation && session.expires_at_unix_ms >= now_unix_ms
            })
    }

    pub fn revoke(&self, executor_id: &str, generation: u64) {
        let mut book = self.credentials.lock().unwrap();
        if book
            .sessions
            .get(executor_id)
            .is_some_and(|session| session.generation == generation)
        {
            let now = unix_time_ms();
            if let Some(session) = book.sessions.remove(executor_id) {
                book.retire(
                    session.token_hash,
                    executor_id,
                    generation,
                    now,
                    CredentialRetirement::Revoked,
                );
                if let Some(previous) = session.previous_token {
                    book.retire(
                        previous.token_hash,
                        executor_id,
                        generation,
                        now,
                        CredentialRetirement::Rotated,
                    );
                }
            }
            self.scratch.revoke_connection(executor_id, generation);
            self.authorized.lock().unwrap().retain(|_, transfer| {
                transfer.coordinate.executor_id != executor_id
                    || transfer.coordinate.connection_generation != generation
            });
            let abandoned = {
                let mut staged = self.staged.lock().unwrap();
                let ids = staged
                    .iter()
                    .filter(|(_, upload)| {
                        upload.receipt.coordinate.executor_id == executor_id
                            && upload.receipt.coordinate.connection_generation == generation
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>();
                ids.into_iter()
                    .filter_map(|id| staged.remove(&id))
                    .collect::<Vec<_>>()
            };
            for upload in abandoned {
                let _ = std::fs::remove_file(upload.path);
            }
        }
    }

    pub fn authorize_request(&self, request: &CellRequest, executor_id: &str, generation: u64) {
        let coordinate = ObjectTransferCoordinate {
            repository: request.repository.identity(),
            request_id: request.request_id.clone(),
            attempt_id: request.attempt_id.clone(),
            executor_id: executor_id.to_owned(),
            connection_generation: generation,
        };
        self.authorized.lock().unwrap().insert(
            (request.request_id.clone(), request.attempt_id.clone()),
            AuthorizedTransfer {
                coordinate,
                base_commit: request.base_commit.clone(),
            },
        );
    }

    pub fn revoke_request(
        &self,
        request_id: &str,
        attempt_id: &str,
        executor_id: &str,
        generation: u64,
    ) {
        let key = (request_id.to_owned(), attempt_id.to_owned());
        let mut authorized = self.authorized.lock().unwrap();
        if authorized.get(&key).is_some_and(|transfer| {
            transfer.coordinate.executor_id == executor_id
                && transfer.coordinate.connection_generation == generation
        }) {
            authorized.remove(&key);
        }
    }

    pub fn authorizes(&self, coordinate: &ObjectTransferCoordinate, base_commit: &str) -> bool {
        self.authorized
            .lock()
            .unwrap()
            .get(&(coordinate.request_id.clone(), coordinate.attempt_id.clone()))
            .is_some_and(|transfer| {
                transfer.coordinate == *coordinate && transfer.base_commit == base_commit
            })
    }

    /// Validate the execution/generation fence before the runner asks the API to
    /// mint bearer material. Hash and length checks happen here as well as at the
    /// API boundary so malformed executor input never reaches the grant minter.
    pub(crate) fn authorizes_cloud_grant(
        &self,
        request: &CloudObjectGrantRequest,
        base_commit: &str,
    ) -> bool {
        Self::valid_cloud_grant_shape(request) && self.authorizes(&request.coordinate, base_commit)
    }

    pub(crate) fn valid_cloud_grant_shape(request: &CloudObjectGrantRequest) -> bool {
        let valid_hash = request.content_hash.len() == 64
            && request
                .content_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        let valid_size = match request.operation {
            CloudObjectOperation::Get => request.byte_count.is_none(),
            CloudObjectOperation::Put => request.byte_count.is_some_and(|bytes| bytes > 0),
        };
        valid_hash && valid_size
    }

    pub fn authenticate(
        &self,
        token: &str,
        now_unix_ms: u64,
    ) -> Result<AuthenticatedObjectSession, ObjectAuthRefusal> {
        let candidate = hash_bytes(token.as_bytes());
        let book = self.credentials.lock().unwrap();
        let mut expired = None;
        for (executor_id, session) in &book.sessions {
            let matched_expiry = if constant_time_eq(&candidate, &session.token_hash) {
                Some(session.expires_at_unix_ms)
            } else {
                session
                    .previous_token
                    .as_ref()
                    .filter(|previous| constant_time_eq(&candidate, &previous.token_hash))
                    .map(|previous| previous.expires_at_unix_ms)
            };
            match matched_expiry {
                Some(expires_at) if expires_at >= now_unix_ms => {
                    return Ok(AuthenticatedObjectSession {
                        executor_id: executor_id.clone(),
                        device_id: session.device_id.clone(),
                        runner_device_id: session.runner_device_id.clone(),
                        generation: session.generation,
                    });
                }
                Some(expires_at) => expired = Some((executor_id, session.generation, expires_at)),
                None => {}
            }
        }
        if let Some((executor_id, generation, expired_at_unix_ms)) = expired {
            return Err(ObjectAuthRefusal::Expired {
                executor_id: executor_id.clone(),
                generation,
                expired_at_unix_ms,
                stored: book.stored(executor_id),
            });
        }
        let retired = book
            .retired
            .iter()
            .rev()
            .find(|retired| constant_time_eq(&candidate, &retired.token_hash));
        Err(match retired {
            Some(retired) => ObjectAuthRefusal::Retired {
                executor_id: retired.executor_id.clone(),
                generation: retired.generation,
                retired_at_unix_ms: retired.retired_at_unix_ms,
                cause: retired.cause,
                stored: book.stored(&retired.executor_id),
            },
            None => ObjectAuthRefusal::Unknown,
        })
    }

    /// Log one refused object-channel request, at most one line per executor
    /// per interval with the rest counted into the next. Every route that
    /// authenticates an object-channel token reports through here so they all
    /// say the same thing. `presented` is the executor and generation the
    /// request claimed, when it carried a coordinate. Returns the line logged.
    pub fn report_refusal(
        &self,
        route: &str,
        presented: Option<(&str, u64)>,
        refusal: &ObjectAuthRefusal,
    ) -> Option<String> {
        let presented_text = match presented {
            Some((executor_id, generation)) => {
                format!("executor_id={executor_id} generation={generation}")
            }
            None => "none".into(),
        };
        let message = format!(
            "object-channel credential refused route={route} presented {presented_text} refusal={refusal}"
        );
        let key = presented
            .map(|(executor_id, _)| executor_id)
            .or_else(|| refusal.executor_id())
            .unwrap_or("<unidentified>")
            .to_owned();
        let line = self.refusal_reports.0.lock().unwrap().record(
            Instant::now(),
            key,
            &format!("{route}:{}", refusal.signature()),
            &message,
        )?;
        log::warn!("{line}");
        Some(line)
    }

    pub fn stage_delta(
        &self,
        staging_dir: &Path,
        receipt: DeltaUploadReceipt,
        pack: &[u8],
    ) -> Result<StagedDeltaUpload, String> {
        self.cleanup_expired_staged(unix_time_ms());
        let mut staged = self.staged.lock().unwrap();
        if let Some(existing) = staged.get(&receipt.receipt_id) {
            if existing.receipt == receipt {
                return Ok(existing.clone());
            }
            return Err("receipt id is already bound to different content".into());
        }
        std::fs::create_dir_all(staging_dir)
            .map_err(|error| format!("creating object upload staging directory: {error}"))?;
        let final_path = staging_dir.join(format!("{}.pack", receipt.receipt_id));
        let temp_path = staging_dir.join(format!(
            ".{}.{}.tmp",
            receipt.receipt_id,
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&temp_path, pack)
            .map_err(|error| format!("writing staged delta pack: {error}"))?;
        std::fs::rename(&temp_path, &final_path)
            .map_err(|error| format!("publishing staged delta pack: {error}"))?;
        let upload = StagedDeltaUpload {
            receipt,
            path: final_path,
            staged_at_unix_ms: unix_time_ms(),
        };
        staged.insert(upload.receipt.receipt_id.clone(), upload.clone());
        Ok(upload)
    }

    /// Resolve a staged upload only while its executor generation is still the
    /// live authenticated session and every receipt field matches. The locked
    /// commit-barrier consumer uses this instead of trusting executor JSON.
    pub fn staged_delta(&self, receipt: &DeltaUploadReceipt) -> Option<StagedDeltaUpload> {
        let session_is_current = self
            .credentials
            .lock()
            .unwrap()
            .sessions
            .get(&receipt.coordinate.executor_id)
            .is_some_and(|session| {
                session.generation == receipt.coordinate.connection_generation
                    && session.expires_at_unix_ms >= unix_time_ms()
            });
        if !session_is_current {
            return None;
        }
        self.staged
            .lock()
            .unwrap()
            .get(&receipt.receipt_id)
            .filter(|upload| upload.receipt == *receipt)
            .cloned()
    }

    pub(crate) fn consume_staged_delta(&self, receipt: &DeltaUploadReceipt) -> Result<(), String> {
        let upload = {
            let mut staged = self.staged.lock().unwrap();
            match staged.get(&receipt.receipt_id) {
                Some(upload) if upload.receipt == *receipt => staged.remove(&receipt.receipt_id),
                Some(_) => return Err("staged delta receipt content changed".into()),
                None => return Ok(()),
            }
        };
        if let Some(upload) = upload {
            match std::fs::remove_file(&upload.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(format!("remove consumed staged delta pack: {error}")),
            }
        }
        Ok(())
    }

    fn cleanup_expired_staged(&self, now_unix_ms: u64) {
        let expired = {
            let mut staged = self.staged.lock().unwrap();
            let ids = staged
                .iter()
                .filter(|(_, upload)| {
                    upload
                        .staged_at_unix_ms
                        .saturating_add(STAGED_UPLOAD_TTL_MS)
                        < now_unix_ms
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| staged.remove(&id))
                .collect::<Vec<_>>()
        };
        for upload in expired {
            let _ = std::fs::remove_file(upload.path);
        }
    }
}

pub fn content_sha256(bytes: &[u8]) -> String {
    hex_hash(hash_bytes(bytes))
}

fn hash_bytes(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex_hash(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::executor_protocol::{GitObjectFormat, RepositoryIdentity};

    /// An executor's checkout can stand on a commit it sealed that has not
    /// reached the runner yet. Offered as a have, that commit used to fail the
    /// whole fetch with a 422; it now narrows nothing, and a have the runner
    /// does hold still narrows the pack to what the executor lacks.
    /// What a fetch served: the packs' object counts, in order.
    async fn served(
        repository: &Path,
        tip: &str,
        haves: &[String],
        limits: cairn_codec::transfer::PackLimits,
    ) -> Result<Vec<u64>, ObjectFetchFailure> {
        let scratch = tempfile::tempdir().unwrap();
        let stream = open_runner_fetch(RunnerFetchRequest {
            repository: repository.to_path_buf(),
            scratch: scratch.path().join("fetch"),
            coordinate: coordinate(1),
            want_commit: tip.to_owned(),
            have_commits: haves.to_vec(),
            limits,
        })
        .await?;
        let body = axum::body::to_bytes(stream.into_response().into_body(), usize::MAX)
            .await
            .unwrap();
        let mut decoder = object_fetch_stream::StreamDecoder::new(u64::MAX);
        let mut packs = Vec::new();
        let mut ended = false;
        let mut failed = None;
        decoder
            .push(&body, &mut |event| {
                match event {
                    object_fetch_stream::StreamEvent::PackStart(pack) => {
                        packs.push(pack.object_count)
                    }
                    object_fetch_stream::StreamEvent::End(_) => ended = true,
                    object_fetch_stream::StreamEvent::Error(failure) => failed = Some(failure),
                    _ => {}
                }
                Ok::<(), ()>(())
            })
            .unwrap();
        decoder.finish().unwrap();
        assert_eq!(
            std::fs::read_dir(scratch.path().join("fetch"))
                .unwrap()
                .count(),
            0,
            "a served fetch leaves no scratch behind"
        );
        // A refusal after the first pack arrives as the stream's Error record,
        // which the executor takes as the fetch's failure.
        if let Some(failure) = failed {
            return Err(failure);
        }
        assert!(ended, "the stream ended with neither End nor Error");
        assert_eq!(
            std::fs::read_dir(scratch.path().join("fetch"))
                .unwrap()
                .count(),
            0,
            "a served fetch leaves no scratch behind"
        );
        Ok(packs)
    }

    fn git_in(repository: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(["-c", "user.email=test@example.com", "-c", "user.name=Test"])
            .args(args)
            .current_dir(repository)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    #[tokio::test]
    async fn a_closure_past_one_pack_is_served_as_several_and_an_unpackable_object_is_named() {
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path();
        git_in(repository, &["init", "-b", "main", "-q"]);
        for index in 0..6u8 {
            // Incompressible enough that six of them cannot share 8 KiB.
            let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ u64::from(index + 1);
            let bytes: Vec<u8> = (0..4096)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect();
            std::fs::write(repository.join(format!("blob-{index}")), bytes).unwrap();
            git_in(repository, &["add", "."]);
            git_in(repository, &["commit", "-qm", &format!("blob {index}")]);
        }
        let tip = git_in(repository, &["rev-parse", "HEAD"]);
        let small = cairn_codec::transfer::PackLimits::new(8 * 1024, 1_000_000, u64::MAX);
        let packs = served(repository, &tip, &[], small).await.unwrap();
        assert!(packs.len() > 1, "{packs:?}");

        let tiny = cairn_codec::transfer::PackLimits::new(1024, 1_000_000, u64::MAX);
        let refused = served(repository, &tip, &[], tiny).await.unwrap_err();
        assert_eq!(refused.code, ObjectFetchFailureCode::ObjectExceedsPackLimit);
        assert_eq!(refused.class, ObjectFetchFailureClass::Deterministic);
        let object = refused.object.as_ref().unwrap();
        assert!(object.path.as_deref().unwrap().starts_with("blob-"));
        assert!(refused.detail.contains(&object.oid), "{}", refused.detail);

        let unknown = served(repository, &"2".repeat(40), &[], small)
            .await
            .unwrap_err();
        assert_eq!(unknown.code, ObjectFetchFailureCode::CommitUnresolvable);
    }

    #[tokio::test]
    async fn an_unknown_have_does_not_fail_the_fetch_and_a_known_one_narrows_it() {
        const UNKNOWN: &str = "1111111111111111111111111111111111111111";
        let temp = tempfile::tempdir().unwrap();
        let repository = temp.path();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(["-c", "user.email=test@example.com", "-c", "user.name=Test"])
                .args(args)
                .current_dir(repository)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8(output.stdout).unwrap().trim().to_string()
        };
        git(&["init", "-b", "main", "-q"]);
        std::fs::write(repository.join("base"), "base\n").unwrap();
        git(&["add", "base"]);
        git(&["commit", "-qm", "base"]);
        let parent = git(&["rev-parse", "HEAD"]);
        std::fs::write(repository.join("next"), "next\n").unwrap();
        git(&["add", "next"]);
        git(&["commit", "-qm", "next"]);
        let tip = git(&["rev-parse", "HEAD"]);

        let count = |haves: Vec<String>| {
            let tip = tip.clone();
            async move {
                served(repository, &tip, &haves, Default::default())
                    .await
                    .unwrap()
                    .iter()
                    .sum::<u64>()
            }
        };
        let full = count(vec![]).await;
        assert_eq!(count(vec![UNKNOWN.to_string()]).await, full);
        let narrowed = count(vec![UNKNOWN.to_string(), parent]).await;
        assert!(
            narrowed < full,
            "{narrowed} objects is not fewer than {full}"
        );
    }

    #[test]
    fn canonical_validator_rejects_unframed_and_corrupt_pack_bytes() {
        assert!(validate_pack_bytes(b"not a git pack".to_vec()).is_err());
        assert!(validate_framed_pack(b"not framed".to_vec()).is_err());
    }

    fn coordinate(generation: u64) -> ObjectTransferCoordinate {
        ObjectTransferCoordinate {
            repository: RepositoryIdentity {
                project_id: "project".into(),
                repository_id: "repository".into(),
                object_format: GitObjectFormat::Sha1,
            },
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            executor_id: "executor".into(),
            connection_generation: generation,
        }
    }

    #[test]
    fn reconnect_invalidates_old_credential_and_disconnect_is_generation_guarded() {
        let state = ObjectPlaneState::default();
        let (old, _) = state
            .issue_credential("executor", "device", "runner", 1, 100)
            .unwrap();
        let (new, _) = state
            .issue_credential("executor", "device", "runner", 2, 100)
            .unwrap();
        assert!(state.authenticate(&old, 101).is_err());
        assert_eq!(state.authenticate(&new, 101).unwrap().generation, 2);
        state.revoke("executor", 1);
        assert!(state.authenticate(&new, 101).is_ok());
        state.revoke("executor", 2);
        assert!(state.authenticate(&new, 101).is_err());
    }

    /// The 11-08 specimen: a superseded connection task's late refresh and
    /// teardown ran after the executor's next generation attached.
    #[test]
    fn a_stale_refresh_after_reconnect_leaves_the_live_credential_valid() {
        let state = ObjectPlaneState::default();
        state
            .issue_credential("executor", "device", "runner", 3, 100)
            .unwrap();
        let (live, _) = state
            .issue_credential("executor", "device", "runner", 4, 100)
            .unwrap();
        assert!(
            state
                .issue_credential("executor", "device", "runner", 3, 101)
                .is_none(),
            "a superseded generation must not be issued a credential"
        );
        assert!(
            state.authenticate(&live, 102).is_ok(),
            "a stale generation's refresh replaced the live credential"
        );
        state.revoke("executor", 3);
        assert!(
            state.authenticate(&live, 103).is_ok(),
            "a stale generation's teardown deleted the live session"
        );
        assert!(state.holds_session("executor", 4, 103));
        assert!(!state.holds_session("executor", 3, 103));
    }

    #[test]
    fn a_stale_generation_cannot_issue_after_the_live_session_ends() {
        let state = ObjectPlaneState::default();
        state
            .issue_credential("executor", "device", "runner", 4, 100)
            .unwrap();
        state.revoke("executor", 4);
        assert!(state
            .issue_credential("executor", "device", "runner", 3, 101)
            .is_none());
        assert!(state.credentials.lock().unwrap().sessions.is_empty());
        // The next generation is still welcome.
        assert!(state
            .issue_credential("executor", "device", "runner", 5, 102)
            .is_some());
    }

    #[test]
    fn a_superseded_token_is_refused_naming_both_generations() {
        let state = ObjectPlaneState::default();
        let (old, _) = state
            .issue_credential("executor", "device", "runner", 3, 100)
            .unwrap();
        let (live, live_expires) = state
            .issue_credential("executor", "device", "runner", 4, 200)
            .unwrap();
        assert_eq!(
            state.authenticate(&old, 201).unwrap_err(),
            ObjectAuthRefusal::Retired {
                executor_id: "executor".into(),
                generation: 3,
                retired_at_unix_ms: 200,
                cause: CredentialRetirement::Superseded { by: 4 },
                stored: Some(StoredObjectSession {
                    generation: 4,
                    expires_at_unix_ms: live_expires,
                }),
            }
        );
        assert_eq!(
            state.authenticate("not a token", 201).unwrap_err(),
            ObjectAuthRefusal::Unknown
        );
        assert_eq!(
            state.authenticate(&live, live_expires + 1).unwrap_err(),
            ObjectAuthRefusal::Expired {
                executor_id: "executor".into(),
                generation: 4,
                expired_at_unix_ms: live_expires,
                stored: Some(StoredObjectSession {
                    generation: 4,
                    expires_at_unix_ms: live_expires,
                }),
            }
        );
        state.revoke("executor", 4);
        assert!(matches!(
            state.authenticate(&live, 201).unwrap_err(),
            ObjectAuthRefusal::Retired {
                generation: 4,
                cause: CredentialRetirement::Revoked,
                stored: None,
                ..
            }
        ));
    }

    #[test]
    fn a_refusal_is_reported_once_per_executor_per_interval_naming_what_it_lost_to() {
        let state = ObjectPlaneState::default();
        let (old, _) = state
            .issue_credential("executor", "device", "runner", 3, 100)
            .unwrap();
        state
            .issue_credential("executor", "device", "runner", 4, 200)
            .unwrap();
        let refusal = state.authenticate(&old, 201).unwrap_err();
        let line = state
            .report_refusal("/fetch", Some(("executor", 3)), &refusal)
            .expect("the first refusal is reported");
        for expected in [
            "object-channel credential refused route=/fetch",
            "presented executor_id=executor generation=3",
            "cause=\"superseded by generation 4\"",
            "stored_generation=4",
        ] {
            assert!(line.contains(expected), "{line} lacks {expected}");
        }
        assert!(
            state
                .report_refusal("/fetch", Some(("executor", 3)), &refusal)
                .is_none(),
            "an identical repeat inside the interval is folded into the next line"
        );
    }

    #[test]
    fn request_authorization_binds_every_coordinate_dimension() {
        use cairn_common::executor_protocol::{CellPriority, MutationPolicy, RepositoryLocator};
        let state = ObjectPlaneState::default();
        let request = CellRequest {
            request_id: "request".into(),
            attempt_id: "attempt".into(),
            project_id: "project".into(),
            repository: RepositoryLocator::ManagedObjects {
                project_id: "project".into(),
                repository_id: "repository".into(),
                object_format: GitObjectFormat::Sha1,
            },
            base_commit: "a".repeat(40),
            command: "check".into(),
            command_class: cairn_common::executor_protocol::CellCommandClass::Other,
            placement_work_class:
                cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
            owner: None,
            cwd: String::new(),
            env: Vec::new(),
            priority: CellPriority::ReviewCheck,
            wait_horizon_unix_ms: 1,
            waiting_since_unix_ms: 0,
            timeout_ms: 1,
            mutation_policy: MutationPolicy::PureVerdict,
            requesting_job_id: None,
            affinity_key: None,
            executor: None,
            pinned_executor_id: None,
            placement_mobility: Default::default(),
            verdict_platforms: Vec::new(),
            avoided_executor_ids: Vec::new(),
            project_requirements: Default::default(),
            command_resource_identity: None,
            resource_reservation: Default::default(),
            learned_estimate: None,
            scratch_sync: None,
            home_affinity: None,
            publication: None,
        };
        state.authorize_request(&request, "executor", 1);
        assert!(state.authorizes(&coordinate(1), &request.base_commit));
        assert!(!state.authorizes(&coordinate(2), &request.base_commit));
        assert!(!state.authorizes(&coordinate(1), &"b".repeat(40)));
        let get = CloudObjectGrantRequest {
            coordinate: coordinate(1),
            content_hash: "a".repeat(64),
            operation: CloudObjectOperation::Get,
            byte_count: None,
        };
        assert!(state.authorizes_cloud_grant(&get, &request.base_commit));
        let mut wrong_generation = get.clone();
        wrong_generation.coordinate.connection_generation = 2;
        assert!(!state.authorizes_cloud_grant(&wrong_generation, &request.base_commit));
        let mut malformed_put = get;
        malformed_put.operation = CloudObjectOperation::Put;
        assert!(!state.authorizes_cloud_grant(&malformed_put, &request.base_commit));

        state.revoke_request("request", "attempt", "executor", 1);
        assert!(!state.authorizes(&coordinate(1), &request.base_commit));
    }

    #[test]
    fn credential_rotation_keeps_a_live_generation_authenticated_past_the_old_ttl() {
        let state = ObjectPlaneState::default();
        let (old, old_expires) = state
            .issue_credential("executor", "device", "runner", 1, 100)
            .unwrap();
        let (rotated, rotated_expires) = state
            .issue_credential(
                "executor",
                "device",
                "runner",
                1,
                old_expires.saturating_sub(1),
            )
            .unwrap();
        assert_eq!(
            state
                .authenticate(&old, old_expires.saturating_sub(1))
                .unwrap()
                .generation,
            1,
            "the previous token must remain valid while the update frame is in flight"
        );
        assert!(state
            .authenticate(&old, old_expires.saturating_add(1))
            .is_err());
        assert_eq!(
            state
                .authenticate(&rotated, old_expires.saturating_add(1))
                .unwrap()
                .generation,
            1
        );
        assert!(rotated_expires > old_expires);
    }

    #[test]
    fn staging_is_idempotent_only_for_the_same_receipt() {
        let state = ObjectPlaneState::default();
        let dir = tempfile::tempdir().unwrap();
        let receipt = DeltaUploadReceipt {
            receipt_id: "receipt".into(),
            coordinate: coordinate(1),
            base_commit: "a".repeat(40),
            delta_commit: "b".repeat(40),
            content_hash: content_sha256(b"pack"),
            pack_checksum: "c".repeat(40),
        };
        let first = state
            .stage_delta(dir.path(), receipt.clone(), b"pack")
            .unwrap();
        let second = state
            .stage_delta(dir.path(), receipt.clone(), b"pack")
            .unwrap();
        assert_eq!(first.path, second.path);
        assert!(state.staged_delta(&receipt).is_none());
        state
            .issue_credential("executor", "device", "runner", 1, unix_time_ms())
            .unwrap();
        assert_eq!(state.staged_delta(&receipt).unwrap().path, first.path);
        state
            .issue_credential("executor", "device", "runner", 2, unix_time_ms())
            .unwrap();
        assert!(state.staged_delta(&receipt).is_none());
        let mut conflicting = receipt;
        conflicting.content_hash = "different".into();
        assert!(state
            .stage_delta(dir.path(), conflicting, b"other")
            .is_err());
    }

    #[test]
    fn consumed_and_expired_staged_uploads_remove_their_files() {
        let state = ObjectPlaneState::default();
        let dir = tempfile::tempdir().unwrap();
        let receipt = DeltaUploadReceipt {
            receipt_id: "consumed".into(),
            coordinate: coordinate(1),
            base_commit: "a".repeat(40),
            delta_commit: "b".repeat(40),
            content_hash: content_sha256(b"pack"),
            pack_checksum: "c".repeat(40),
        };
        let upload = state
            .stage_delta(dir.path(), receipt.clone(), b"pack")
            .unwrap();
        assert!(upload.path.is_file());
        state.consume_staged_delta(&receipt).unwrap();
        assert!(!upload.path.exists());
        assert!(state.staged.lock().unwrap().is_empty());

        let mut expired_receipt = receipt;
        expired_receipt.receipt_id = "expired".into();
        let expired = state
            .stage_delta(dir.path(), expired_receipt, b"pack")
            .unwrap();
        state.cleanup_expired_staged(
            expired
                .staged_at_unix_ms
                .saturating_add(STAGED_UPLOAD_TTL_MS)
                .saturating_add(1),
        );
        assert!(!expired.path.exists());
        assert!(state.staged.lock().unwrap().is_empty());
    }
}
