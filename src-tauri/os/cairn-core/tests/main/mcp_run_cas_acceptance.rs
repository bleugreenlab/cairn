//! Production-shaped managed object acceptance coverage for CAIRN-2795.

use crate::common;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::body::{Body, Bytes};
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use cairn_common::executor_protocol::{
    CatalogFetchResponse, CatalogPackDescriptor, CellCommandClass, CellOutcome, CellPriority,
    CellRequest, CloudObjectGrant, CloudObjectGrantRequest, CloudObjectOperation,
    DeltaUploadReceipt, MutationDeltaUploadRequest, MutationPolicy, ObjectFetchFailure,
    ObjectFetchFailureCode, ObjectTransferCoordinate, RepositoryLocator,
    CLOUD_OBJECT_GRANT_VERSION,
};
use cairn_core::internal::orchestrator::object_plane::{
    content_sha256, object_fetch_refusal, open_runner_fetch, RunnerFetchRequest,
};
use cairn_core::internal::orchestrator::Orchestrator;
use futures_util::StreamExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[derive(Clone)]
struct ObjectServerState {
    orch: Orchestrator,
    repository: PathBuf,
    staging: PathBuf,
    fetches: Arc<AtomicUsize>,
    uploads: Arc<AtomicUsize>,
    fail_upload: Arc<AtomicBool>,
    cloud_only: Arc<AtomicBool>,
    corrupt_catalog_suffix: Arc<AtomicBool>,
    runner_object_bytes: Arc<AtomicUsize>,
    cloud_gets: Arc<AtomicUsize>,
    cloud_puts: Arc<AtomicUsize>,
    cloud: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    server_url: Arc<OnceLock<String>>,
}

fn files_named(root: &Path, suffix: &str, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files_named(&path, suffix, found);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(suffix))
        {
            found.push(path);
        }
    }
}

#[tokio::test]
async fn corrupt_catalog_suffix_publishes_no_validated_prefix() {
    if common::skip_if_fenced("corrupt_catalog_suffix_publishes_no_validated_prefix") {
        return;
    }
    let fixture = fixture().await;
    fixture.state.cloud_only.store(true, Ordering::SeqCst);
    fixture
        .state
        .corrupt_catalog_suffix
        .store(true, Ordering::SeqCst);

    let outcome = submit(
        &fixture,
        request(
            &fixture,
            "corrupt-suffix",
            "printf must-not-run",
            MutationPolicy::PureVerdict,
        ),
    )
    .await;
    assert!(!matches!(outcome, CellOutcome::Completed { .. }));
    assert_eq!(fixture.state.runner_object_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.cloud_gets.load(Ordering::SeqCst), 2);

    let mut manifests = Vec::new();
    files_named(
        &fixture.executor_home,
        "cache-manifest.json",
        &mut manifests,
    );
    assert_eq!(manifests.len(), 1);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifests[0]).unwrap()).unwrap();
    assert_eq!(manifest["packs"].as_array().unwrap().len(), 0);

    let mut published_packs = Vec::new();
    files_named(&fixture.executor_home, ".pack", &mut published_packs);
    assert!(
        published_packs.is_empty(),
        "validated catalog prefix leaked into executable ODB: {published_packs:?}"
    );
}

async fn catalog(
    State(state): State<ObjectServerState>,
    headers: HeaderMap,
    Json(request): Json<FetchRequest>,
) -> Response {
    if !authenticated(&state, &headers)
        || !state
            .orch
            .object_plane
            .authorizes(&request.coordinate, &request.want_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    // Production pack construction. Only the storage around it is simulated: a
    // second implementation of what a runner actually serves would let this
    // fixture stay green while the real route drifted.
    let haves = cairn_codec::transfer::known_commits(&state.repository, &request.have_commits)
        .unwrap_or_default();
    let Some((pack, _)) = cairn_codec::transfer::build_reachable_pack(
        &state.repository,
        std::slice::from_ref(&request.want_commit),
        &haves,
    )
    .ok()
    .flatten() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let validated = cairn_codec::transfer::validate_pack(
        &pack,
        cairn_codec::transfer::PackLimits::default(),
        &std::env::temp_dir(),
    )
    .unwrap();
    let framed = cairn_codec::transfer::frame_pack(&pack, &validated.index);
    let hash = content_sha256(&framed);
    state
        .cloud
        .lock()
        .unwrap()
        .insert(hash.clone(), framed.clone());
    let url = format!("{}/s3/{hash}", state.server_url.get().unwrap());
    let mut packs = vec![CatalogPackDescriptor {
        catalog_id: format!("acceptance:{hash}"),
        content_hash: hash.clone(),
        byte_count: framed.len() as u64,
        pack_checksum: validated.manifest.pack_checksum,
        base_commit: None,
        tip_commit: request.want_commit.clone(),
        grant: grant(hash, CloudObjectOperation::Get, url),
    }];
    if state.corrupt_catalog_suffix.load(Ordering::SeqCst) {
        let bytes = b"not-a-framed-pack".to_vec();
        let hash = content_sha256(&bytes);
        state
            .cloud
            .lock()
            .unwrap()
            .insert(hash.clone(), bytes.clone());
        packs.push(CatalogPackDescriptor {
            catalog_id: format!("acceptance:corrupt:{hash}"),
            content_hash: hash.clone(),
            byte_count: bytes.len() as u64,
            pack_checksum: "invalid".into(),
            base_commit: None,
            tip_commit: request.want_commit,
            grant: grant(
                hash.clone(),
                CloudObjectOperation::Get,
                format!("{}/s3/{hash}", state.server_url.get().unwrap()),
            ),
        });
    }
    Json(CatalogFetchResponse { packs }).into_response()
}

fn grant(hash: String, operation: CloudObjectOperation, url: String) -> CloudObjectGrant {
    CloudObjectGrant {
        version: CLOUD_OBJECT_GRANT_VERSION,
        content_hash: hash,
        operation,
        url,
        method: match operation {
            CloudObjectOperation::Get => "GET",
            CloudObjectOperation::Put => "PUT",
        }
        .into(),
        expires_at: "2099-01-01T00:00:00Z".into(),
        headers: Default::default(),
    }
}

async fn cloud_grant(
    State(state): State<ObjectServerState>,
    headers: HeaderMap,
    Json(request): Json<CloudObjectGrantRequest>,
) -> Response {
    if !authenticated(&state, &headers)
        || request.operation != CloudObjectOperation::Put
        || !state.orch.object_plane.authorizes(
            &request.coordinate,
            headers["x-cairn-base-commit"].to_str().unwrap(),
        )
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let url = format!(
        "{}/s3/{}",
        state.server_url.get().unwrap(),
        request.content_hash
    );
    Json(grant(request.content_hash, request.operation, url)).into_response()
}

async fn cloud_get(
    State(state): State<ObjectServerState>,
    AxumPath(hash): AxumPath<String>,
) -> Response {
    state.cloud_gets.fetch_add(1, Ordering::SeqCst);
    match state.cloud.lock().unwrap().get(&hash).cloned() {
        Some(bytes) => (StatusCode::OK, Bytes::from(bytes)).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn cloud_put(
    State(state): State<ObjectServerState>,
    AxumPath(hash): AxumPath<String>,
    body: Bytes,
) -> Response {
    state.uploads.fetch_add(1, Ordering::SeqCst);
    state.cloud_puts.fetch_add(1, Ordering::SeqCst);
    if state.fail_upload.load(Ordering::SeqCst) || content_sha256(&body) != hash {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    state.cloud.lock().unwrap().insert(hash, body.to_vec());
    StatusCode::OK.into_response()
}

async fn delta_complete(
    State(state): State<ObjectServerState>,
    headers: HeaderMap,
    Json(request): Json<MutationDeltaUploadRequest>,
) -> Response {
    if !authenticated(&state, &headers)
        || !state
            .orch
            .object_plane
            .authorizes(&request.coordinate, &request.base_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some(framed) = state
        .cloud
        .lock()
        .unwrap()
        .get(&request.content_hash)
        .cloned()
    else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if framed.len() as u64 != request.byte_count || content_sha256(&framed) != request.content_hash
    {
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    }
    let (pack, index) = cairn_codec::transfer::unframe_pack(&framed).unwrap();
    let validated = cairn_codec::transfer::validate_pack(
        &pack,
        cairn_codec::transfer::PackLimits::default(),
        &std::env::temp_dir(),
    )
    .unwrap();
    if validated.index != index
        || validated.manifest.pack_checksum != request.pack_checksum
        || !validated
            .manifest
            .objects
            .iter()
            .any(|object| object.oid == request.delta_commit)
    {
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    }
    let receipt = DeltaUploadReceipt {
        receipt_id: format!("cloud-receipt-{}", request.coordinate.request_id),
        coordinate: request.coordinate,
        base_commit: request.base_commit,
        delta_commit: request.delta_commit,
        content_hash: request.content_hash,
        pack_checksum: request.pack_checksum,
    };
    state
        .orch
        .object_plane
        .stage_delta(&state.staging, receipt.clone(), &pack)
        .unwrap();
    Json(receipt).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FetchRequest {
    coordinate: ObjectTransferCoordinate,
    want_commit: String,
    #[serde(default)]
    have_commits: Vec<String>,
}

struct Fixture {
    _temp: TempDir,
    executor_home: PathBuf,
    orch: Orchestrator,
    project_id: String,
    repository_id: String,
    base: String,
    state: ObjectServerState,
}

fn git(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn init_repo(repo: &Path) -> String {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "acceptance@example.com"]);
    git(repo, &["config", "user.name", "Acceptance"]);
    std::fs::write(repo.join("README.md"), "cold objects\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "base"]);
    git(repo, &["rev-parse", "HEAD"])
}

async fn fixture() -> Fixture {
    fixture_from(init_repo).await
}

async fn fixture_from(make_repository: fn(&Path) -> String) -> Fixture {
    let (temp, db) = common::migrated_db().await;
    let repository = temp.path().join("runner-repository");
    let base = make_repository(&repository);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "cas", &repository).await;
    let repository_id = "acceptance-repository".to_owned();
    let orch = common::orchestrator(&temp, db);
    common::load_fleet_settings(&orch);
    let executor_home = temp.path().join("isolated-executor-home");
    let state = ObjectServerState {
        orch: orch.clone(),
        repository,
        staging: temp.path().join("object-staging"),
        fetches: Arc::new(AtomicUsize::new(0)),
        uploads: Arc::new(AtomicUsize::new(0)),
        fail_upload: Arc::new(AtomicBool::new(false)),
        cloud_only: Arc::new(AtomicBool::new(false)),
        corrupt_catalog_suffix: Arc::new(AtomicBool::new(false)),
        runner_object_bytes: Arc::new(AtomicUsize::new(0)),
        cloud_gets: Arc::new(AtomicUsize::new(0)),
        cloud_puts: Arc::new(AtomicUsize::new(0)),
        cloud: Arc::new(Mutex::new(HashMap::new())),
        server_url: Arc::new(OnceLock::new()),
    };
    let app = Router::new()
        .route("/fetch", post(fetch))
        .route("/catalog", post(catalog))
        .route("/cloud-grant", post(cloud_grant))
        .route("/delta/complete", post(delta_complete))
        .route("/delta", post(upload))
        .route("/s3/{hash}", get(cloud_get).put(cloud_put))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    state.server_url.set(format!("http://{address}")).unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    common::attach_isolated_test_executor(
        &orch,
        executor_home.clone(),
        format!("http://{address}"),
        project_id.clone(),
    );
    Fixture {
        _temp: temp,
        executor_home,
        orch,
        project_id,
        repository_id,
        base,
        state,
    }
}

fn request(
    fixture: &Fixture,
    suffix: &str,
    command: impl Into<String>,
    mutation_policy: MutationPolicy,
) -> CellRequest {
    let base = fixture.base.clone();
    request_at(fixture, suffix, command, mutation_policy, &base)
}

/// A request for one exact commit, which is the only thing a managed executor is
/// ever allowed to run against.
fn request_at(
    fixture: &Fixture,
    suffix: &str,
    command: impl Into<String>,
    mutation_policy: MutationPolicy,
    base_commit: &str,
) -> CellRequest {
    let command = command.into();
    CellRequest {
        request_id: format!("cas-{suffix}"),
        attempt_id: "attempt-1".into(),
        project_id: fixture.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: fixture.project_id.clone(),
            repository_id: fixture.repository_id.clone(),
            absolute_path: fixture.state.repository.display().to_string(),
        },
        base_commit: base_commit.to_owned(),
        command_class: CellCommandClass::classify(&command),
        placement_work_class: cairn_common::executor_protocol::PlacementWorkClass::AgentSessions,
        command,
        owner: None,
        cwd: String::new(),
        env: Vec::new(),
        priority: CellPriority::AgentInteractive,
        wait_horizon_unix_ms: u64::MAX,
        waiting_since_unix_ms: 0,
        timeout_ms: 30_000,
        mutation_policy,
        requesting_job_id: None,
        affinity_key: Some("cas-acceptance".into()),
        executor: None,
        pinned_executor_id: Some("isolated-test-executor".into()),
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
    }
}

async fn submit(fixture: &Fixture, request: CellRequest) -> CellOutcome {
    fixture.orch.fleet.submit(&fixture.orch, request).await
}

async fn fetch(
    State(state): State<ObjectServerState>,
    headers: HeaderMap,
    Json(request): Json<FetchRequest>,
) -> Response {
    state.fetches.fetch_add(1, Ordering::SeqCst);
    if state.cloud_only.load(Ordering::SeqCst) {
        // Typed, as the real route answers: an untyped refusal is outside the
        // protocol and fails closed without trying the cloud.
        return object_fetch_refusal(&ObjectFetchFailure::new(
            ObjectFetchFailureCode::RepositoryUnavailable,
            "the acceptance runner serves this repository from the cloud only",
        ));
    }
    if !authenticated(&state, &headers)
        || !state
            .orch
            .object_plane
            .authorizes(&request.coordinate, &request.want_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    // Production's fetch stream, and production's own typed answer for a
    // commit this runner cannot resolve: never a silently empty success.
    let stream = match open_runner_fetch(RunnerFetchRequest {
        repository: state.repository.clone(),
        scratch: state.staging.join("fetch-scratch"),
        coordinate: request.coordinate,
        want_commit: request.want_commit,
        have_commits: request.have_commits,
        limits: Default::default(),
    })
    .await
    {
        Ok(stream) => stream,
        Err(failure) => return object_fetch_refusal(&failure),
    };
    let (parts, body) = stream.into_response().into_parts();
    let served = state.runner_object_bytes.clone();
    let body = body.into_data_stream().inspect(move |chunk| {
        if let Ok(chunk) = chunk {
            served.fetch_add(chunk.len(), Ordering::SeqCst);
        }
    });
    Response::from_parts(parts, Body::from_stream(body))
}

async fn upload(
    State(state): State<ObjectServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.uploads.fetch_add(1, Ordering::SeqCst);
    if state.fail_upload.load(Ordering::SeqCst) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    if !authenticated(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let coordinate: ObjectTransferCoordinate = serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(headers["x-cairn-coordinate"].as_bytes())
            .unwrap(),
    )
    .unwrap();
    let base_commit = headers["x-cairn-base-commit"].to_str().unwrap().to_owned();
    let delta_commit = headers["x-cairn-delta-commit"].to_str().unwrap().to_owned();
    if !state
        .orch
        .object_plane
        .authorizes(&coordinate, &base_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let validated = cairn_codec::transfer::validate_pack(
        &body,
        cairn_codec::transfer::PackLimits::default(),
        &std::env::temp_dir(),
    )
    .unwrap();
    let content_hash = format!("{:x}", Sha256::digest(&body));
    let receipt = DeltaUploadReceipt {
        receipt_id: format!("receipt-{}", coordinate.request_id),
        coordinate,
        base_commit,
        delta_commit,
        content_hash,
        pack_checksum: validated.manifest.pack_checksum,
    };
    state
        .orch
        .object_plane
        .stage_delta(&state.staging, receipt.clone(), &body)
        .unwrap();
    Json(receipt).into_response()
}

fn authenticated(state: &ObjectServerState, headers: &HeaderMap) -> bool {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(|token| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            state.orch.object_plane.authenticate(token, now).ok()
        })
        .is_some()
}
