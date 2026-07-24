//! Shared integration test helpers.

#![allow(dead_code)]

pub mod sync_server;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cairn_common::executor_protocol::{
    ExecutorAdvertisement, ExecutorCapabilities, ExecutorIdentity, ExecutorMessage,
    ObjectChannelCredential,
};
use cairn_core::internal::db::DbState;
use cairn_core::internal::jj::{self, JjEnv};
use cairn_core::internal::mcp::handlers::issue_resources::handle_read_issue_resource;
use cairn_core::internal::mcp::handlers::write::handle_write;
use cairn_core::internal::mcp::types::McpCallbackRequest;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::services::testing::TestServicesBuilder;
use cairn_core::internal::storage::{
    DbError, DbResult, LocalDb, MigrationRunner, RowExt, SearchIndex, TURSO_MIGRATIONS,
};
use cairn_db::turso::params;
use cairn_executor::{ExecutorRuntime, Fleet as ExecutorPool};
use serde_json::{json, Value};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;

pub async fn migrated_db() -> (TempDir, LocalDb) {
    let temp = tempdir().unwrap();
    let db = LocalDb::open(temp.path().join("cairn.turso.db"))
        .await
        .unwrap();
    MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
        .run(&db)
        .await
        .unwrap();
    (temp, db)
}

pub fn orchestrator(temp: &TempDir, db: Arc<LocalDb>) -> Orchestrator {
    let search_index = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
    let db_state = Arc::new(DbState::new(db, search_index));
    let services = Arc::new(TestServicesBuilder::new().build());
    Orchestrator::builder(db_state, services, temp.path().join("config")).build()
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
    );
}

fn attach_executor(
    orch: &Orchestrator,
    executor_home: PathBuf,
    object_channel: Option<ObjectChannelCredential>,
    colocated: bool,
    executor_id: &'static str,
    projects_served: Vec<String>,
) {
    let projects_served = Arc::new(projects_served);
    let core_pool = orch.fleet.clone();
    let snapshot_pool = core_pool.clone();
    let generation = Arc::new(AtomicU64::new(0));
    let snapshot_generation = generation.clone();
    let callback_pool = core_pool.clone();
    let callback_orch = orch.clone();
    let lifetime_event_pool = core_pool.clone();
    let lifetime_event_generation = generation.clone();
    let runtime = ExecutorRuntime::new(executor_home)
        .with_snapshot_callback(move |snapshot, health| {
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
        .with_lifetime_process_event_callback(move |event| {
            let pool = lifetime_event_pool.clone();
            let generation = lifetime_event_generation.clone();
            Box::pin(async move {
                pool.handle_executor_message(
                    executor_id,
                    generation.load(Ordering::Acquire),
                    ExecutorMessage::LifetimeProcessEvent { event },
                );
            })
        });
    let executor_pool = ExecutorPool::new(runtime);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let attached_generation = core_pool.attach_advertised_executor(
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
                toolchains: Vec::new(),
                projects_served: projects_served.as_ref().clone(),
                disk_budget_bytes: None,
                memory_budget_bytes: None,
            },
            current_load: 0,
            warm_roots: Vec::new(),
            observed_at_unix_ms: 0,
        },
        tx,
        colocated,
        None,
    );
    generation.store(attached_generation, Ordering::Release);
    executor_pool.configure_object_channel(
        object_channel,
        executor_id.to_owned(),
        attached_generation,
    );

    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            match message {
                ExecutorMessage::Configure { config } => executor_pool.configure(config),
                ExecutorMessage::Submit { request, batch } => {
                    let executor_pool = executor_pool.clone();
                    let core_pool = core_pool.clone();
                    let projects_served = projects_served.clone();
                    tokio::spawn(async move {
                        let request_id = request.request_id.clone();
                        let attempt_id = request.attempt_id.clone();
                        let outcome = match batch {
                            Some(batch) => executor_pool.submit_run_batch(request, batch).await,
                            None => executor_pool.submit(request).await,
                        };
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
                                        toolchains: Vec::new(),
                                        projects_served: projects_served.as_ref().clone(),
                                        disk_budget_bytes: None,
                                        memory_budget_bytes: None,
                                    },
                                    current_load: 0,
                                    warm_roots: executor_pool.warm_roots(),
                                    observed_at_unix_ms: unix_time_ms(),
                                },
                            },
                        );
                    });
                }
                ExecutorMessage::Cancel { request_id, .. } => {
                    executor_pool.cancel_request(&request_id);
                }
                ExecutorMessage::CancelJob { job_id } => {
                    executor_pool.cancel_job_requests(&job_id);
                }
                ExecutorMessage::LifetimeLeaseRequest {
                    correlation_id,
                    operation,
                } => {
                    let executor_pool = executor_pool.clone();
                    let core_pool = core_pool.clone();
                    tokio::spawn(async move {
                        let result = executor_pool.operate_lifetime_lease(operation).await;
                        core_pool.handle_executor_message(
                            executor_id,
                            attached_generation,
                            ExecutorMessage::LifetimeLeaseResponse {
                                correlation_id,
                                result,
                            },
                        );
                    });
                }
                ExecutorMessage::SnapshotRequest { correlation_id } => {
                    core_pool.handle_executor_message(
                        executor_id,
                        attached_generation,
                        ExecutorMessage::SnapshotResponse {
                            correlation_id,
                            snapshot: executor_pool.snapshot(),
                            health: executor_pool.substrate_report(),
                        },
                    );
                }
                ExecutorMessage::Shutdown => break,
                _ => {}
            }
        }
    });
}

fn unix_time_ms() -> u64 {
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
    let request = McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "write".to_string(),
        payload: json!({ "changes": changes }),
        tool_use_id: None,
    };
    handle_write(orch, &request).await
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

/// True when this test process is itself confined by a Cairn worktree fence
/// (`CAIRN_SANDBOXED=1`, set on every fenced `run`). Integration tests that
/// spawn a real `sandbox-exec` or depend on an undisturbed subprocess lifecycle
/// cannot run nested inside the agent fence, so they call this to skip rather
/// than fail. Unfenced CI still runs them for real coverage.
/// The jj binary for the test, or `None` to self-skip when jj is unavailable.
/// jj-backed integration tests gate on this and print a skip note rather than
/// fail when jj cannot be resolved (honoring `CAIRN_JJ_BIN` when set).
pub fn jj_bin() -> Option<String> {
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

    let jj = JjEnv::resolve("jj", config_dir);
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
    let jj = JjEnv::resolve("jj", config_dir);
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

/// True when the unfenced sync CI lane has demanded these tests actually RUN.
/// Set via `CAIRN_REQUIRE_SYNC_TESTS=1` by `.github/workflows/sync-tests.yml`.
/// In that mode a would-be skip — a fenced process (here) or a missing `tursodb`
/// (see `sync_server::SyncServer::locate_or_spawn`) — is a HARD FAILURE rather
/// than a vacuous green, because the whole point of that lane is to exercise the
/// Turso-sync integration tests for real. (CAIRN-2170: these self-skips are
/// exactly how a sync misdiagnosis stood unrefuted — a skip is NOT a pass.)
pub fn sync_tests_required() -> bool {
    std::env::var_os("CAIRN_REQUIRE_SYNC_TESTS").is_some()
}

pub fn skip_if_fenced(test: &str) -> bool {
    let explicitly_sandboxed = std::env::var_os("CAIRN_SANDBOXED").is_some();
    // Cargo can be launched from an agent run-tool without preserving the
    // explicit sandbox marker into the test process. The MCP run-tool envelope is
    // still enough evidence that this process is nested under the worktree fence.
    let run_tool_context = std::env::var_os("CAIRN_CALLBACK_URL").is_some()
        && std::env::var_os("CAIRN_RUN_ID").is_some()
        && std::env::var_os("CAIRN_WORKTREE").is_some();

    if explicitly_sandboxed || run_tool_context {
        // In the unfenced sync lane a fenced process can't run these tests, so
        // skipping would read green while proving nothing — fail loudly instead.
        assert!(
            !sync_tests_required(),
            "{test}: CAIRN_REQUIRE_SYNC_TESTS is set but this process is fenced — the \
             unfenced sync lane must run with the CAIRN_SANDBOXED / CAIRN_CALLBACK_URL / \
             CAIRN_RUN_ID / CAIRN_WORKTREE fence vars UNSET. A skip is NOT a pass."
        );
        eprintln!("skipping {test}: cannot run nested inside a Cairn worktree fence");
        record_fence_skip(test);
        true
    } else {
        false
    }
}

// Best-effort: append a self-skipped test name to `$CAIRN_SKIP_LOG` (set by
// `scripts/test-rust.ts`) so the runner can report how many tests skipped under
// the fence. libtest swallows the skip message of a passing test, so without
// this the skip is indistinguishable from a real pass. (#157)
pub fn record_fence_skip(test: &str) {
    if let Some(log) = std::env::var_os("CAIRN_SKIP_LOG") {
        use std::io::Write as _;
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
        {
            // One write_all (vs writeln!'s multiple syscalls) keeps the append
            // atomic when parallel test threads all skip at once.
            let _ = f.write_all(format!("{test}\n").as_bytes());
        }
    }
}
