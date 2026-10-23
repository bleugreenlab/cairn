//! Shared integration test helpers.

#![allow(dead_code)]

pub mod sync_server;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};

use cairn_common::executor_protocol::{
    DeclineReason, ExecutorAdvertisement, ExecutorCapabilities, ExecutorIdentity, ExecutorMessage,
    ObjectChannelCredential, OfferSummary, ResidencyRuntimeConfig,
};
use cairn_core::internal::db::DbState;
use cairn_core::internal::dispatch::{dispatch_tool, DispatchOutput};
use cairn_core::internal::jj;
use cairn_core::internal::mcp::handlers::issue_resources::handle_read_issue_resource;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::{McpCallbackRequest, McpCallbackResponse};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{DbError, DbResult, LocalDb, RowExt, SearchIndex};
use cairn_db::turso::params;
use cairn_exec::blocking::offload;
use cairn_exec::{AdmissionLimits, ExecutorRuntime, Fleet as ExecutorPool};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;

pub async fn migrated_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open_migrated_for_test(temp.path().join("cairn.turso.db"))
        .await
        .unwrap();
    (temp, db)
}

/// The exact jj executable this host's integration suite should use, as a file
/// path the snapshot below can copy.
///
/// The resolution itself is `cairn_vcs::jj::testing::resolve_jj`, the one
/// definition every suite shares: the build-slot artifact first, then
/// `CAIRN_JJ_BIN`, then PATH, with Cairn's own jj shim unwrapped to the binary
/// it forwards to (a shim here would exec itself until timeout). That answer
/// may be a bare program name found on PATH, which is located to a path here.
fn jj_candidate() -> Option<PathBuf> {
    let bin = cairn_vcs::jj::testing::resolve_jj()?;
    let path = PathBuf::from(&bin);
    if path.components().count() > 1 {
        Some(path)
    } else {
        cairn_common::toolchain_path::locate_program(&bin)
    }
}

/// A process-lifetime snapshot of the real jj executable.
///
/// Whole-workspace checks can replace their shared sidecar while this integration
/// binary is running. Resolving that live path once per command creates a TOCTOU
/// race: `jj --version` succeeds, then `jj git init` fails because the path was
/// replaced between the two spawns. Copying the executable into an owned tempdir
/// makes the fixture's toolchain as immutable as its repository and config.
fn pinned_jj_bin() -> &'static Path {
    struct PinnedJj {
        _temp: TempDir,
        bin: PathBuf,
    }

    static PINNED: OnceLock<PinnedJj> = OnceLock::new();
    PINNED
        .get_or_init(|| {
            let source = jj_candidate()
                .expect("jj must be provisioned in the build slot or available on PATH");

            // Open before copying. The open handle remains a stable coordinate
            // even if a platform permits the source directory entry to change.
            let mut source_file =
                std::fs::File::open(&source).expect("open jj executable snapshot");
            let source_permissions = source_file
                .metadata()
                .expect("read jj executable metadata")
                .permissions();
            let temp = tempdir().expect("create pinned jj directory");
            let bin = temp.path().join(source.file_name().unwrap_or_default());
            let mut pinned_file = std::fs::File::create(&bin).expect("create pinned jj executable");
            std::io::copy(&mut source_file, &mut pinned_file).expect("snapshot jj executable");
            std::fs::set_permissions(&bin, source_permissions)
                .expect("preserve jj executable permissions");
            // Close the destination before executing it. Linux refuses to exec a
            // file any process still holds open for writing (ETXTBSY, "Text file
            // busy"), so leaving this handle in scope failed the probe — and with
            // it every jj-backed test in the binary — on Linux executors.
            drop(pinned_file);
            let probe = Command::new(&bin)
                .arg("--version")
                .output()
                .expect("run pinned jj executable");
            assert!(probe.status.success(), "pinned jj --version must succeed");

            // Production paths exercised after fixture setup resolve jj again.
            // Every test thread observes the same OnceLock value, so this pins
            // those calls to the same immutable executable without per-test env
            // mutation.
            std::env::set_var("CAIRN_JJ_BIN", &bin);
            PinnedJj { _temp: temp, bin }
        })
        .bin
        .as_path()
}

/// A fixture orchestrator that owns every run in its database.
///
/// `boot_at(0)` is what makes that true: fixtures seed `runs` rows with epoch
/// timestamps, and the CAIRN-3287 ownership fence refuses tool calls for runs
/// that predate the host's boot. A test orchestrator boots at the epoch, so its
/// seeded runs are its own. Tests OF the fence build their own orchestrator with
/// an explicit `boot_at` (see `run_ownership_fence`).
pub fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
    orchestrator_booted_at(temp, db, 0)
}

/// [`orchestrator`] with an explicit host boot time, for placing seeded runs on
/// either side of the ownership fence.
pub fn orchestrator_booted_at(temp: &TempDir, db: Arc<LocalDb>, boot_at: i64) -> Orchestrator {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(db_state, services, temp.path().join("config"))
        .boot_at(boot_at)
        .build()
}

/// [`orchestrator`] wired to a permission-response broadcast the test holds, so
/// it can observe what an answer delivers to a waiting provider poll.
pub fn orchestrator_with_permission_responses(
    temp: &TempDir,
    db: Arc<LocalDb>,
) -> (
    Orchestrator,
    tokio::sync::broadcast::Sender<(String, String)>,
) {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    let responses = tokio::sync::broadcast::channel(16).0;
    let orch = Orchestrator::builder(db_state, services, temp.path().join("config"))
        .permission_responses(responses.clone())
        .build();
    (orch, responses)
}

/// The stdin of an in-memory agent process. The backend writes one
/// newline-terminated JSON message per wake, so the delivered prompts are the
/// lines of what accumulates here.
pub struct RecordingStdin(pub Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for RecordingStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Register an in-memory process for `session_id` on `run_id` (owned by
/// `job-1`), so a resume takes the warm path instead of spawning a CLI. Returns
/// the log of prompts delivered to it.
pub fn register_recording_process(
    orch: &Orchestrator,
    run_id: &str,
    session_id: &str,
) -> Arc<std::sync::Mutex<Vec<u8>>> {
    let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut processes = orch.process_state.processes.lock().unwrap();
    let handle = cairn_core::internal::agent_process::process::RunHandle::new(
        Arc::new(std::sync::Mutex::new(None)),
        Arc::new(std::sync::Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(RecordingStdin(
                delivered.clone(),
            ))),
        ))),
        Some(session_id.to_string()),
        Some("job-1".to_string()),
    );
    processes.register(run_id.to_string(), handle);
    delivered
}

/// Stdin of a process whose pipe has closed: every write fails.
pub struct BrokenStdin;

impl std::io::Write for BrokenStdin {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe))
    }
}

/// [`register_recording_process`] for a process whose stdin is broken, so a
/// warm launch starts its turn and then fails to deliver the prompt.
pub fn register_broken_process(orch: &Orchestrator, run_id: &str, session_id: &str) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let handle = cairn_core::internal::agent_process::process::RunHandle::new(
        Arc::new(std::sync::Mutex::new(None)),
        Arc::new(std::sync::Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(BrokenStdin)),
        ))),
        Some(session_id.to_string()),
        Some("job-1".to_string()),
    );
    processes.register(run_id.to_string(), handle);
}

/// Attach the production executor runtime through the same advertised-message
/// facade used by enrolled WebSocket executors.
pub fn attached_executor_home(orch: &Orchestrator) -> PathBuf {
    orch.config_dir.clone()
}

pub fn attach_test_executor(orch: &Orchestrator) {
    attach_executor(
        orch,
        attached_executor_home(orch),
        None,
        true,
        "test-executor",
        Vec::new(),
        AdmissionLimits::default(),
    );
}

/// Attach the colocated test executor and keep a handle on its pool, so a test
/// can put the machine into a state through its own admission path (holding a
/// project's provisioning ticket, say) and later take it out again.
pub fn attach_test_executor_pool(orch: &Orchestrator) -> ExecutorPool {
    attach_executor(
        orch,
        attached_executor_home(orch),
        None,
        true,
        "test-executor",
        Vec::new(),
        AdmissionLimits::default(),
    )
}

/// Attach an executor with room for only `concurrency_units` commands at once
/// and `max_queue_entries` waiting behind them, so a test can genuinely fill the
/// machine and watch what a batch with nowhere to run does about it.
pub fn attach_capacity_limited_test_executor(
    orch: &Orchestrator,
    memory_budget_bytes: u64,
    max_queue_entries: usize,
) {
    attach_executor(
        orch,
        attached_executor_home(orch),
        None,
        true,
        "test-executor",
        Vec::new(),
        AdmissionLimits {
            memory_budget_bytes: Some(memory_budget_bytes),
            max_queue_entries,
            ..AdmissionLimits::default()
        },
    );
}

/// Attach a production executor whose object cache and workspaces live under a
/// CAIRN_HOME that is physically separate from the runner. Managed-object tests
/// use this instead of sharing the runner's jj store, so an execution can only
/// obtain Git objects through the configured HTTP object channel.
pub fn attach_isolated_test_executor(
    orch: &Orchestrator,
    executor_home: PathBuf,
    object_base_url: String,
    project_id: String,
) {
    const EXECUTOR_ID: &str = "isolated-test-executor";
    let (bearer_token, expires_at_unix_ms) = orch.object_plane.issue_credential(
        EXECUTOR_ID,
        "isolated-test-device",
        "test-runner-device",
        1,
        unix_time_ms(),
    );
    attach_executor(
        orch,
        executor_home,
        Some(ObjectChannelCredential {
            base_url: object_base_url,
            bearer_token,
            expires_at_unix_ms,
        }),
        false,
        EXECUTOR_ID,
        vec![project_id],
        AdmissionLimits::default(),
    );
}

/// Answer an offer in the production link handler's order: claim first, then
/// bind the request identity. A decline therefore never leaves an identity
/// bound, and a claim whose identity is already live on this machine gives its
/// capacity back rather than keeping a ticket nothing will grant. Binding
/// failure is not answered by releasing the identity, because the live binding
/// belongs to whatever already holds it.
pub fn answer_offer(
    pool: &ExecutorPool,
    entry_id: String,
    summary: &OfferSummary,
) -> ExecutorMessage {
    match pool.try_claim(&entry_id, summary) {
        Ok(()) if pool.bind_request_identity(&summary.request_id) => {
            ExecutorMessage::Claim { entry_id }
        }
        Ok(()) => {
            pool.release_claim(&entry_id);
            cairn_exec::AdmissionRefusal::new(
                DeclineReason::Constraint,
                Some("this machine already holds that request identity".into()),
            )
            .into_decline(entry_id)
        }
        Err(refusal) => refusal.into_decline(entry_id),
    }
}

/// A claim the runner reports lost was never granted, so nothing is running:
/// give back the capacity it charged and the identity it bound, so the next
/// offer for the same request is answered from admission rather than declined
/// as already held.
pub fn forget_lost_claim(pool: &ExecutorPool, entry_id: &str) {
    if let Some(request_id) = pool.release_claim(entry_id) {
        pool.release_request_identity(&request_id);
    }
}

/// The per-batch MCP callback relay a real executor runs, so a test's child
/// processes can call back into the test's own host.
///
/// Production splits this across the link: the `cairn-exec` binary binds a
/// loopback `/api/mcp`, injects it into each item of a batch that carries a
/// runner callback context, and forwards whatever arrives bearing that context
/// as an `McpRelayRequest` frame; the runner's link handler authorizes the frame
/// against the executor session and the run (`Fleet::authorize_mcp_relay`) and
/// dispatches it. The test executor has no link to carry frames over, so this
/// does both halves in one place, with the same checks in the same order.
///
/// Unlike a real executor it rewrites `CAIRN_CALLBACK_URL` on every batch, not
/// only those with a context. A colocated test executor's children inherit the
/// test process's environment, and a test run from an agent shell carries the
/// LIVE host's callback URL and secret there; without the rewrite, a child that
/// called the SDK or the CLI would act on that host.
#[derive(Clone)]
struct TestMcpRelay {
    url: String,
    live_contexts: Arc<Mutex<HashSet<String>>>,
    orch: Orchestrator,
    executor_id: &'static str,
    generation: Arc<AtomicU64>,
    read_cursors: Arc<Mutex<HashMap<String, usize>>>,
}

/// Revokes a batch's callback context on the executor side when the batch ends.
struct LiveContext {
    id: String,
    live_contexts: Arc<Mutex<HashSet<String>>>,
}

impl Drop for LiveContext {
    fn drop(&mut self) {
        self.live_contexts.lock().unwrap().remove(&self.id);
    }
}

impl TestMcpRelay {
    fn start(orch: &Orchestrator, executor_id: &'static str, generation: Arc<AtomicU64>) -> Self {
        let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .expect("bind the test executor's callback relay");
        listener
            .set_nonblocking(true)
            .expect("make the callback relay non-blocking");
        let address = listener
            .local_addr()
            .expect("read the callback relay address");
        let relay = Self {
            url: format!("http://{address}/api/mcp"),
            live_contexts: Arc::default(),
            orch: orch.clone(),
            executor_id,
            generation,
            read_cursors: Arc::default(),
        };
        let router = Router::new()
            .route("/api/mcp", post(relay_test_callback))
            .layer(DefaultBodyLimit::max(
                cairn_common::paths::MCP_CALLBACK_MAX_BODY_BYTES,
            ))
            .with_state(relay.clone());
        let listener =
            tokio::net::TcpListener::from_std(listener).expect("serve the callback relay");
        tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });
        relay
    }

    /// Point every item of `batch` at this relay, and admit its callback
    /// context for as long as the returned guard lives.
    fn admit(
        &self,
        batch: Option<&mut cairn_common::executor_protocol::ProcessBatch>,
    ) -> Option<LiveContext> {
        let batch = batch?;
        let context = batch.runner_context_id.clone();
        for item in &mut batch.items {
            item.env.retain(|(name, _)| {
                name != "CAIRN_CALLBACK_URL" && (context.is_none() || name != "CAIRN_MCP_SECRET")
            });
            item.env
                .push(("CAIRN_CALLBACK_URL".into(), self.url.clone()));
            if let Some(context) = &context {
                item.env.push(("CAIRN_MCP_SECRET".into(), context.clone()));
            }
        }
        let id = context?;
        self.live_contexts.lock().unwrap().insert(id.clone());
        Some(LiveContext {
            id,
            live_contexts: self.live_contexts.clone(),
        })
    }
}

async fn relay_test_callback(
    State(relay): State<TestMcpRelay>,
    headers: HeaderMap,
    Json(request): Json<McpCallbackRequest>,
) -> Response {
    let context = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|context| relay.live_contexts.lock().unwrap().contains(*context));
    let Some(context) = context else {
        return (
            StatusCode::UNAUTHORIZED,
            "invalid or expired callback context",
        )
            .into_response();
    };
    if let Err(diagnostic) = relay.orch.fleet.authorize_mcp_relay(
        relay.executor_id,
        relay.generation.load(Ordering::Acquire),
        context,
        &request,
    ) {
        return (StatusCode::FORBIDDEN, diagnostic).into_response();
    }
    let DispatchOutput { content, reminders } =
        dispatch_tool(&relay.orch, &request, &relay.read_cursors)
            .await
            .into_inner();
    Json(McpCallbackResponse {
        result: content,
        artifact_uri: None,
        reminders,
    })
    .into_response()
}

fn attach_executor(
    orch: &Orchestrator,
    executor_home: PathBuf,
    object_channel: Option<ObjectChannelCredential>,
    colocated: bool,
    executor_id: &'static str,
    projects_served: Vec<String>,
    admission: AdmissionLimits,
) -> ExecutorPool {
    let projects_served = Arc::new(projects_served);
    let core_pool = orch.fleet.clone();
    let snapshot_pool = core_pool.clone();
    let generation = Arc::new(AtomicU64::new(0));
    let snapshot_generation = generation.clone();
    let callback_pool = core_pool.clone();
    let callback_orch = orch.clone();
    let resident_event_pool = core_pool.clone();
    let resident_event_generation = generation.clone();
    let runtime = ExecutorRuntime::new(executor_home)
        .with_snapshot_callback(move |_reason, snapshot, health| {
            snapshot_pool.handle_executor_message(
                executor_id,
                snapshot_generation.load(Ordering::Acquire),
                ExecutorMessage::SnapshotUpdated { snapshot, health },
            );
        })
        .with_runner_callback(move |callback| {
            let pool = callback_pool.clone();
            let orch = callback_orch.clone();
            Box::pin(async move { pool.handle_runner_callback(&orch, callback).await })
        })
        .with_resident_process_event_callback(move |event| {
            let pool = resident_event_pool.clone();
            let generation = resident_event_generation.clone();
            Box::pin(async move {
                pool.handle_executor_message(
                    executor_id,
                    generation.load(Ordering::Acquire),
                    ExecutorMessage::ResidentProcessEvent { event },
                );
            })
        });
    let executor_pool = ExecutorPool::new(runtime.with_admission_limits(admission));
    let (tx, mut rx) = mpsc::unbounded_channel();
    let attached_generation = core_pool
        .attach_advertised_executor(
            ExecutorAdvertisement {
                identity: ExecutorIdentity {
                    device_id: if colocated {
                        "test-device"
                    } else {
                        "isolated-test-device"
                    }
                    .into(),
                    executor_id: executor_id.into(),
                    display_name: if colocated {
                        "Test executor"
                    } else {
                        "Isolated test executor"
                    }
                    .into(),
                },
                capabilities: ExecutorCapabilities {
                    os: std::env::consts::OS.into(),
                    arch: std::env::consts::ARCH.into(),
                    logical_cores: 1,
                    concurrency_capacity: None,
                    toolchains: Vec::new(),
                    projects_served: projects_served.as_ref().clone(),
                    disk_budget_bytes: None,
                    memory_budget_bytes: None,
                    sandbox: None,
                    toolchain_detection: None,
                    build_prerequisites: None,
                    containment: None,
                },
                current_load: 0,
                warm_roots: Vec::new(),
                observed_at_unix_ms: 0,
                liveness_observed_at_unix_ms: None,
                facts_stale_refresh_blocked: None,
                last_refresh: None,
            },
            tx,
            colocated,
            None,
            None,
        )
        .expect("attach the test executor");
    generation.store(attached_generation, Ordering::Release);
    let relay = TestMcpRelay::start(orch, executor_id, generation.clone());
    executor_pool.configure_object_channel(
        object_channel,
        executor_id.to_owned(),
        attached_generation,
    );
    let handle = executor_pool.clone();

    // Each frame's synchronous pool work runs on the blocking pool, awaited in
    // arrival order, as the executor's link-control worker runs it. The pool's
    // blocking guard therefore sees here what it sees in production, and the
    // runner's half of each exchange stays on this task.
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            match message {
                // Answer the offer the way a real machine does: from admission,
                // before the next frame is read. Every integration test
                // therefore exercises the whole offer -> claim -> grant path
                // rather than a dispatch that skips it.
                ExecutorMessage::Offer { entry_id, summary } => {
                    let pool = executor_pool.clone();
                    let answer = offload(move || answer_offer(&pool, entry_id, &summary)).await;
                    core_pool.handle_executor_message(executor_id, attached_generation, answer);
                }
                ExecutorMessage::Lost { entry_id } => {
                    let pool = executor_pool.clone();
                    offload(move || forget_lost_claim(&pool, &entry_id)).await;
                }
                ExecutorMessage::Granted {
                    entry_id,
                    config,
                    request,
                    mut batch,
                } => {
                    let pool = executor_pool.clone();
                    let reservation = request.resource_reservation.clone();
                    offload(move || {
                        pool.configure(config);
                        pool.consume_claim(&entry_id, &reservation);
                    })
                    .await;
                    let executor_pool = executor_pool.clone();
                    let core_pool = core_pool.clone();
                    let projects_served = projects_served.clone();
                    let relay = relay.clone();
                    tokio::spawn(async move {
                        let request_id = request.request_id.clone();
                        let attempt_id = request.attempt_id.clone();
                        let context = relay.admit(batch.as_mut());
                        let outcome = executor_pool.run_granted(request, batch).await;
                        drop(context);
                        // The link handler revokes a batch's callback contexts
                        // when its result arrives, before applying the result.
                        core_pool.settle_runner_contexts_for_request(&request_id, &attempt_id);
                        core_pool.handle_executor_message(
                            executor_id,
                            attached_generation,
                            ExecutorMessage::Result {
                                request_id,
                                attempt_id,
                                outcome,
                            },
                        );
                        core_pool.handle_executor_message(
                            executor_id,
                            attached_generation,
                            ExecutorMessage::AdvertisementUpdated {
                                advertisement: ExecutorAdvertisement {
                                    identity: ExecutorIdentity {
                                        device_id: if colocated {
                                            "test-device"
                                        } else {
                                            "isolated-test-device"
                                        }
                                        .into(),
                                        executor_id: executor_id.into(),
                                        display_name: if colocated {
                                            "Test executor"
                                        } else {
                                            "Isolated test executor"
                                        }
                                        .into(),
                                    },
                                    capabilities: ExecutorCapabilities {
                                        os: std::env::consts::OS.into(),
                                        arch: std::env::consts::ARCH.into(),
                                        logical_cores: 1,
                                        concurrency_capacity: None,
                                        toolchains: Vec::new(),
                                        projects_served: projects_served.as_ref().clone(),
                                        disk_budget_bytes: None,
                                        memory_budget_bytes: None,
                                        sandbox: None,
                                        toolchain_detection: None,
                                        build_prerequisites: None,
                                        containment: None,
                                    },
                                    current_load: 0,
                                    warm_roots: executor_pool.warm_roots(),
                                    observed_at_unix_ms: unix_time_ms(),
                                    liveness_observed_at_unix_ms: None,
                                    facts_stale_refresh_blocked: None,
                                    last_refresh: None,
                                },
                            },
                        );
                    });
                }
                ExecutorMessage::Cancel { request_id, .. } => {
                    let pool = executor_pool.clone();
                    offload(move || pool.cancel_request(&request_id)).await;
                }
                ExecutorMessage::CancelJob { job_id } => {
                    let pool = executor_pool.clone();
                    offload(move || pool.cancel_job_requests(&job_id)).await;
                }
                ExecutorMessage::ResidencyRequest {
                    correlation_id,
                    config,
                    operation,
                    granted_entry_id,
                } => {
                    if let ResidencyRuntimeConfig::Install(config) = config {
                        let pool = executor_pool.clone();
                        offload(move || pool.configure(config)).await;
                    }
                    let executor_pool = executor_pool.clone();
                    let core_pool = core_pool.clone();
                    tokio::spawn(async move {
                        let result = executor_pool
                            .operate_residency(operation, granted_entry_id)
                            .await;
                        core_pool.handle_executor_message(
                            executor_id,
                            attached_generation,
                            ExecutorMessage::ResidencyResponse {
                                correlation_id,
                                result,
                            },
                        );
                    });
                }
                ExecutorMessage::SnapshotRequest { correlation_id } => {
                    let pool = executor_pool.clone();
                    let (snapshot, health) =
                        offload(move || (pool.snapshot(), pool.substrate_report())).await;
                    core_pool.handle_executor_message(
                        executor_id,
                        attached_generation,
                        ExecutorMessage::SnapshotResponse {
                            correlation_id,
                            snapshot,
                            health,
                        },
                    );
                }
                ExecutorMessage::Shutdown { .. } => break,
                _ => {}
            }
        }
    });
    handle
}

pub fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn config_dir(temp: &TempDir) -> PathBuf {
    temp.path().join("config")
}

pub async fn resource_orchestrator_fixture() -> (TempDir, Arc<LocalDb>, Orchestrator) {
    let (temp, db) = migrated_db().await;
    let db = Arc::new(db);
    let orch = orchestrator(&temp, db.clone());
    (temp, db, orch)
}

pub async fn insert_project_with_repo(db: &LocalDb, key: &str, repo_path: &Path) -> String {
    let id = format!("project-{key}");
    let key = key.to_string();
    let repo = repo_path.to_string_lossy().to_string();
    let id_ret = id.clone();
    db.execute(
        "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
         VALUES (?1, 'default', 'Test Project', ?2, ?3, 1, 1)",
        params![id.as_str(), key.as_str(), repo.as_str()],
    )
    .await
    .unwrap();
    id_ret
}

pub async fn project_resource_fixture(
    project_key: &str,
) -> (TempDir, Arc<LocalDb>, Orchestrator, PathBuf) {
    let (temp, db) = migrated_db().await;
    let db = Arc::new(db);
    let repo = temp.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    insert_project_with_repo(&db, project_key, &repo).await;
    let orch = orchestrator(&temp, db.clone());
    (temp, db, orch, repo)
}

pub async fn read_resource(orch: &Orchestrator, uri: impl AsRef<str>) -> String {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "read_issue_resource".to_string(),
        payload: json!({ "uri": uri.as_ref() }),
        tool_use_id: None,
    };
    handle_read_issue_resource(orch, &request).await
}

pub async fn change_resource(orch: &Orchestrator, changes: Value) -> String {
    change_resource_as_run(orch, changes, None).await
}

pub async fn change_resource_as_run(
    orch: &Orchestrator,
    changes: Value,
    run_id: Option<&str>,
) -> String {
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: run_id.map(str::to_string),
        tool: "write".to_string(),
        payload: json!({ "changes": changes }),
        tool_use_id: None,
    };
    handle_write(orch, &request).await
}

pub async fn seed_authenticated_thread_run(
    db: &LocalDb,
    project_id: &str,
    thread_name: &str,
    run_id: &str,
) {
    let thread_id = format!("thread-{run_id}");
    let job_id = format!("job-{run_id}");
    db.execute(
        "INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'active', 'none', 1, 1)",
        params![thread_id.as_str(), project_id, thread_name],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO jobs(id, project_id, thread_id, node_name, status, uri_segment, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'Thread', 'running', 'thread', 1, 1)",
        params![job_id.as_str(), project_id, thread_id.as_str()],
    )
    .await
    .unwrap();
    db.execute(
        "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'live', 1, 1)",
        params![run_id, project_id, job_id.as_str()],
    )
    .await
    .unwrap();
}

pub async fn test_orchestrator() -> (tempfile::TempDir, Orchestrator) {
    let temp = tempfile::tempdir().unwrap();
    let (_db_temp, db) = migrated_db().await;
    let orch = orchestrator(&temp, Arc::new(db));
    (temp, orch)
}

pub async fn query_i64(db: &LocalDb, sql: &'static str) -> DbResult<i64> {
    db.read(|conn| {
        Box::pin(async move {
            let mut rows = conn.query(sql, ()).await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("missing integer row".to_string()))?;
            row.i64(0)
        })
    })
    .await
}

pub async fn scalar_text_by_id(db: &LocalDb, sql: &'static str, id: &str) -> Option<String> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![id.as_str()]).await?;
            rows.next().await?.map(|row| row.opt_text(0)).transpose()
        })
    })
    .await
    .unwrap()
    .flatten()
}

pub async fn scalar_i64_by_id(db: &LocalDb, sql: &'static str, id: &str) -> i64 {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let mut rows = conn.query(sql, params![id.as_str()]).await?;
            let row = rows.next().await?.expect("count row");
            row.i64(0)
        })
    })
    .await
    .unwrap()
}

pub async fn execute(db: &LocalDb, sql: &'static str) -> DbResult<()> {
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(sql, ()).await?;
            Ok(())
        })
    })
    .await
}

pub async fn create_project(db: &LocalDb, key: &str) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let key = key.to_string();
    let now = chrono::Utc::now().timestamp();

    db.execute(
        "
        INSERT INTO projects (
            id, workspace_id, name, key, repo_path, created_at, updated_at
        )
        VALUES (?1, 'default', 'Test Project', ?2, '/tmp/test-repo', ?3, ?4)
        ",
        params![id.as_str(), key.as_str(), now, now],
    )
    .await
    .unwrap();

    id
}

pub async fn create_job(
    db: &LocalDb,
    parent_job_id: Option<&str>,
    task_index: Option<i64>,
) -> String {
    let id = uuid::Uuid::new_v4().to_string();
    let project_key = format!("JT{}", &id[..8]);
    let project_id = create_project(db, &project_key).await;
    let parent_job_id = parent_job_id.map(str::to_string);
    let task_index = task_index.unwrap_or(0);
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let project_id = project_id.clone();
        let parent_job_id = parent_job_id.clone();
        Box::pin(async move {
            conn.execute(
                "
                INSERT INTO jobs (
                    id, project_id, status, parent_job_id, task_index, created_at, updated_at
                )
                VALUES (?1, ?2, 'running', ?3, ?4, ?5, ?6)
                ",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    parent_job_id.as_deref(),
                    task_index,
                    now,
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();

    id
}

/// The jj binary for the test, or `None` once the calling test has been recorded
/// as a `jj-unavailable` skip (`cairn_vcs::jj::testing::skip_unless_jj`). The
/// guard behind every `let Some(bin) = common::jj_bin() else { return }`.
pub fn jj_bin() -> Option<String> {
    cairn_vcs::jj::testing::skip_unless_jj(resolve_jj(), record_skip)
}

/// The jj binary on this machine as a path, with no skip recorded, for a caller
/// that is not a skip guard. Resolved (and probed) by [`jj_candidate`].
pub fn resolve_jj() -> Option<String> {
    jj_candidate().map(|bin| bin.to_string_lossy().into_owned())
}

/// The git `HEAD` commit sha of `repo`, used to base a jj workspace on the
/// project repo's current tip.
pub fn head_sha(repo: &Path) -> String {
    let out = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(repo)
        .output()
        .unwrap();
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

#[derive(Debug, Clone)]
pub struct ProvisionedJjWorkspace {
    pub project_repository: PathBuf,
    pub git_common_dir: PathBuf,
    pub store_dir: PathBuf,
    pub workspace: PathBuf,
}

fn canonical_git_common_dir(repository: &Path) -> PathBuf {
    let output = Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
        .current_dir(repository)
        .output()
        .unwrap();
    assert!(output.status.success());
    let common = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    std::fs::canonicalize(if common.is_absolute() {
        common
    } else {
        repository.join(common)
    })
    .unwrap()
}

/// Provision a NON-COLOCATED `.jj` workspace at `ws` over a shared store backed
/// by `project_repo`, mirroring production worktree provisioning. The returned
/// identity makes the one runner Git backend/shared-store pair explicit to tests.
pub fn provision_jj_workspace(
    config_dir: &Path,
    project_repo: &Path,
    ws: &Path,
    branch: &str,
) -> ProvisionedJjWorkspace {
    assert!(project_repo.join(".git").exists());
    assert!(!project_repo.join(".jj").exists());

    let jj = jj::environment_with_binary(pinned_jj_bin().to_string_lossy(), config_dir);
    let store = jj::project_store_dir(config_dir, project_repo);
    jj::ensure_project_store(&jj, &store, project_repo).unwrap();
    let base = head_sha(project_repo);
    jj::add_workspace(&jj, &store, ws, branch, &base, None).unwrap();
    jj::write_base_marker(ws, "main", &base).unwrap();
    jj::write_project_root_marker(ws, project_repo).unwrap();

    assert!(ws.join(".jj").exists());
    assert!(!ws.join(".git").exists());
    assert_eq!(store, jj::project_store_dir(config_dir, project_repo));
    let git_common_dir = canonical_git_common_dir(project_repo);
    let git_target = std::fs::read_to_string(store.join(".jj/repo/store/git_target")).unwrap();
    assert_eq!(
        std::fs::canonicalize(git_target.trim()).unwrap(),
        git_common_dir
    );

    ProvisionedJjWorkspace {
        project_repository: std::fs::canonicalize(project_repo).unwrap(),
        git_common_dir,
        store_dir: std::fs::canonicalize(store).unwrap(),
        workspace: std::fs::canonicalize(ws).unwrap(),
    }
}

/// Drive `primary_ws` into the OP-LOG stale state that reproduces the commit
/// barrier's data-loss bug. A sibling workspace over the same store seals a
/// commit, then the primary branch is rebased onto it from the store
/// (`--ignore-working-copy`, WITHOUT an `update_stale`) — rewriting the primary
/// workspace's own `@` out from under it. This is the production shape where BOTH
/// the seal and the restore are blocked by staleness, distinct from a mere
/// bookmark advance (which leaves `jj restore` working). The primary workspace
/// must already exist (via [`provision_jj_workspace`]); the sibling is created
/// next to it, coupled only through the shared store.
pub fn stale_sibling_advance(
    config_dir: &Path,
    project_repo: &Path,
    primary_ws: &Path,
    primary_branch: &str,
) {
    let jj = jj::environment_with_binary(pinned_jj_bin().to_string_lossy(), config_dir);
    let store = jj::project_store_dir(config_dir, project_repo);
    let primary_name = primary_ws
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("primary");
    let sibling_ws = primary_ws
        .parent()
        .unwrap()
        .join(format!("{primary_name}-sibling-advance-ws"));
    let sibling_branch = "agent/SIB-advance-0";
    let base = head_sha(project_repo);
    jj::add_workspace(&jj, &store, &sibling_ws, sibling_branch, &base, None).unwrap();
    std::fs::write(sibling_ws.join("sibling-advance.txt"), "sibling advance\n").unwrap();
    jj::seal(&jj, &sibling_ws, "sibling advance", None).unwrap();
    // Rebase the whole primary branch (its working-copy commit included) onto the
    // sibling tip from the store. Rewriting `@` from outside is what makes the
    // primary workspace stale; we deliberately skip the `update_stale` that
    // `advance_workspace_onto` would normally run, leaving it stale for the test.
    jj::rebase_branch_onto(&jj, &store, primary_branch, sibling_branch).unwrap();
}

/// True when this process carries the Cairn worktree fence's marker
/// (`CAIRN_SANDBOXED=1`, set on every fenced spawn), in which case a test that
/// needs an undisturbed subprocess or execution-cell lifecycle records a
/// declared skip instead of running.
///
/// This is a narrow last resort, not a resolution. The fence permits spawning a
/// child, binding loopback, and writing temp files, so a test doing only those
/// belongs in the ordinary lane; the remaining callers need a *nested* execution
/// cell, which CAIRN-3112 owns. Every skip recorded here must be declared in
/// `src-tauri/skip-manifest.toml` — `scripts/test-rust.ts` fails the run, and
/// therefore the `rust-full` verdict, on one that is not.
///
/// The marker is the ONLY trigger. An earlier version also inferred confinement
/// from the MCP run-tool envelope (`CAIRN_CALLBACK_URL` + `CAIRN_RUN_ID` +
/// `CAIRN_WORKTREE` all present), which is false in an agent terminal: it
/// silently skipped a whole cross-surface suite that, once run for real,
/// immediately failed on a genuine defect its green had hidden (CAIRN-3112). An
/// inference that costs coverage and buys none is deleted rather than deprecated.
pub fn skip_if_fenced(test: &str) -> bool {
    if std::env::var_os("CAIRN_SANDBOXED").is_none() {
        return false;
    }
    eprintln!("skipping {test}: cannot run nested inside a Cairn worktree fence");
    record_skip(test, "worktree-fence");
    true
}

pub use cairn_common::skip_ledger::record_skip;
