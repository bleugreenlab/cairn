//! Production-shaped managed object acceptance coverage for CAIRN-2795.

use crate::common;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use base64::Engine;
use cairn_common::executor_protocol::{
    BuildSlotOutcome, BuildSlotPriority, BuildSlotRequest, DeltaUploadReceipt, MutationPolicy,
    ObjectTransferCoordinate, PlacementConstraints, RepositoryLocator,
};
use cairn_core::internal::orchestrator::object_plane::content_sha256;
use cairn_core::internal::orchestrator::Orchestrator;
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
    interrupt_fetch: Arc<AtomicBool>,
    fail_upload: Arc<AtomicBool>,
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
    git(repo, &["init"]);
    git(repo, &["config", "user.email", "acceptance@example.com"]);
    git(repo, &["config", "user.name", "Acceptance"]);
    std::fs::write(repo.join("README.md"), "cold objects\n").unwrap();
    git(repo, &["add", "."]);
    git(repo, &["commit", "-m", "base"]);
    git(repo, &["rev-parse", "HEAD"])
}

async fn fixture() -> Fixture {
    let (temp, db) = common::migrated_db().await;
    let repository = temp.path().join("runner-repository");
    let base = init_repo(&repository);
    let db = Arc::new(db);
    let project_id = common::insert_project_with_repo(&db, "CAS", &repository).await;
    let repository_id = "acceptance-repository".to_owned();
    let orch = common::orchestrator(&temp, db);
    let executor_home = temp.path().join("isolated-executor-home");
    let state = ObjectServerState {
        orch: orch.clone(),
        repository,
        staging: temp.path().join("object-staging"),
        fetches: Arc::new(AtomicUsize::new(0)),
        uploads: Arc::new(AtomicUsize::new(0)),
        interrupt_fetch: Arc::new(AtomicBool::new(false)),
        fail_upload: Arc::new(AtomicBool::new(false)),
    };
    let app = Router::new()
        .route("/fetch", post(fetch))
        .route("/delta", post(upload))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
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
) -> BuildSlotRequest {
    BuildSlotRequest {
        request_id: format!("cas-{suffix}"),
        attempt_id: "attempt-1".into(),
        project_id: fixture.project_id.clone(),
        repository: RepositoryLocator::ColocatedPath {
            project_id: fixture.project_id.clone(),
            repository_id: fixture.repository_id.clone(),
            absolute_path: fixture.state.repository.display().to_string(),
        },
        base_commit: fixture.base.clone(),
        command: command.into(),
        cwd: String::new(),
        env: Vec::new(),
        priority: BuildSlotPriority::AgentInteractive,
        deadline_unix_ms: u64::MAX,
        timeout_ms: 30_000,
        mutation_policy,
        requesting_job_id: None,
        affinity_key: Some("cas-acceptance".into()),
        constraints: Some(PlacementConstraints {
            executor_id: Some("isolated-test-executor".into()),
            ..PlacementConstraints::default()
        }),
    }
}

async fn submit(fixture: &Fixture, request: BuildSlotRequest) -> BuildSlotOutcome {
    fixture
        .orch
        .build_slots
        .submit(&fixture.orch, request)
        .await
}

async fn fetch(
    State(state): State<ObjectServerState>,
    headers: HeaderMap,
    Json(request): Json<FetchRequest>,
) -> Response {
    state.fetches.fetch_add(1, Ordering::SeqCst);
    if !authenticated(&state, &headers)
        || !state
            .orch
            .object_plane
            .authorizes(&request.coordinate, &request.want_commit)
    {
        return StatusCode::FORBIDDEN.into_response();
    }
    let Some((pack, _)) = cairn_codec::transfer::build_reachable_pack(
        &state.repository,
        &[request.want_commit],
        &request.have_commits,
    )
    .unwrap() else {
        return StatusCode::NO_CONTENT.into_response();
    };
    if state.interrupt_fetch.swap(false, Ordering::SeqCst) {
        return (StatusCode::OK, Bytes::from_static(b"PACK interrupted")).into_response();
    }
    let validated =
        cairn_codec::transfer::validate_pack(&pack, cairn_codec::transfer::PackLimits::default())
            .unwrap();
    let mut response = Response::new(Body::from(pack));
    response.headers_mut().insert(
        "x-cairn-content-sha256",
        HeaderValue::from_str(&content_sha256(&validated.pack)).unwrap(),
    );
    response.headers_mut().insert(
        "x-cairn-pack-checksum",
        HeaderValue::from_str(&validated.manifest.pack_checksum).unwrap(),
    );
    response
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
    let validated =
        cairn_codec::transfer::validate_pack(&body, cairn_codec::transfer::PackLimits::default())
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
            state.orch.object_plane.authenticate(token, now)
        })
        .is_some()
}

fn assert_completed(outcome: &BuildSlotOutcome, expected: &str) {
    match outcome {
        BuildSlotOutcome::Completed {
            output, exit_code, ..
        } => {
            assert_eq!(*exit_code, Some(0));
            assert!(output.contains(expected), "{outcome:?}");
        }
        _ => panic!("expected completed outcome, got {outcome:?}"),
    }
}

#[tokio::test]
async fn cold_fetch_materializes_verdict_then_warm_run_avoids_object_io() {
    if common::skip_if_fenced("cold_fetch_materializes_verdict_then_warm_run_avoids_object_io") {
        return;
    }
    let fixture = fixture().await;
    assert_ne!(fixture.executor_home, fixture.orch.config_dir);
    let first = submit(
        &fixture,
        request(
            &fixture,
            "cold",
            "git cat-file -e HEAD^{commit} && printf cold-ok",
            MutationPolicy::PureVerdict,
        ),
    )
    .await;
    assert_completed(&first, "cold-ok");
    assert_eq!(fixture.state.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.uploads.load(Ordering::SeqCst), 0);

    let second = submit(
        &fixture,
        request(
            &fixture,
            "warm",
            "git cat-file -e HEAD^{commit} && printf warm-ok",
            MutationPolicy::PureVerdict,
        ),
    )
    .await;
    assert_completed(&second, "warm-ok");
    assert_eq!(
        fixture.state.fetches.load(Ordering::SeqCst),
        1,
        "verified warm closure should be reused"
    );
    assert_eq!(fixture.state.uploads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn colocated_execution_performs_zero_object_operations() {
    if common::skip_if_fenced("colocated_execution_performs_zero_object_operations") {
        return;
    }
    let fixture = fixture().await;
    common::attach_test_executor(&fixture.orch);
    let mut colocated = request(
        &fixture,
        "colocated",
        "git cat-file -e HEAD^{commit} && printf colocated-ok",
        MutationPolicy::PureVerdict,
    );
    colocated.constraints = None;
    let outcome = submit(&fixture, colocated).await;
    assert_completed(&outcome, "colocated-ok");
    assert_eq!(fixture.state.fetches.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.state.uploads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn interrupted_fetch_publishes_nothing_and_retry_fetches_again() {
    if common::skip_if_fenced("interrupted_fetch_publishes_nothing_and_retry_fetches_again") {
        return;
    }
    let fixture = fixture().await;
    fixture.state.interrupt_fetch.store(true, Ordering::SeqCst);
    let failed = submit(
        &fixture,
        request(
            &fixture,
            "interrupted",
            "printf must-not-run",
            MutationPolicy::PureVerdict,
        ),
    )
    .await;
    assert!(
        !matches!(failed, BuildSlotOutcome::Completed { .. }),
        "{failed:?}"
    );
    assert_eq!(fixture.state.fetches.load(Ordering::SeqCst), 1);

    let retry = submit(
        &fixture,
        request(
            &fixture,
            "retry",
            "printf retry-ok",
            MutationPolicy::PureVerdict,
        ),
    )
    .await;
    assert_completed(&retry, "retry-ok");
    assert_eq!(
        fixture.state.fetches.load(Ordering::SeqCst),
        2,
        "a partial fetch must not become a warm root"
    );
}

#[tokio::test]
async fn managed_allow_delta_uploads_and_stages_a_receipt() {
    if common::skip_if_fenced("managed_allow_delta_uploads_and_stages_a_receipt") {
        return;
    }
    let fixture = fixture().await;
    let outcome = submit(
        &fixture,
        request(
            &fixture,
            "delta",
            "printf managed-change > managed.txt",
            MutationPolicy::AllowDelta,
        ),
    )
    .await;
    let receipt = match outcome {
        BuildSlotOutcome::Completed {
            mutation_delta: Some(delta),
            ..
        } => delta.upload_receipt.expect("managed delta receipt"),
        other => panic!("expected uploaded managed delta, got {other:?}"),
    };
    assert_eq!(fixture.state.fetches.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.state.uploads.load(Ordering::SeqCst), 1);
    assert!(fixture.orch.object_plane.staged_delta(&receipt).is_some());
}

#[tokio::test]
async fn failed_upload_does_not_rerun_the_command() {
    if common::skip_if_fenced("failed_upload_does_not_rerun_the_command") {
        return;
    }
    let fixture = fixture().await;
    fixture.state.fail_upload.store(true, Ordering::SeqCst);
    let marker = fixture._temp.path().join("execution-count");
    let command = format!(
        "printf x >> '{}'; printf change > changed.txt",
        marker.display()
    );
    let outcome = submit(
        &fixture,
        request(
            &fixture,
            "upload-failure",
            command,
            MutationPolicy::AllowDelta,
        ),
    )
    .await;
    assert!(
        matches!(outcome, BuildSlotOutcome::FailedAfterExecution { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        std::fs::read_to_string(marker).unwrap(),
        "x",
        "upload failure must not rerun executed work"
    );
    assert_eq!(fixture.state.uploads.load(Ordering::SeqCst), 1);
}
