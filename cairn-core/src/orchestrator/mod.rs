//! Orchestrator — bundles all runtime state needed for agent execution.
//!
//! Both the Tauri desktop app and cairn-server create their own `Orchestrator`.
//! All orchestration functions take `&Orchestrator` instead of framework-specific
//! handles (e.g. `&AppHandle`).

pub mod account_manager;
pub mod agents;
pub mod conflict_resolution;
pub mod docs;
pub mod identity;
pub mod lifecycle;
pub mod recipes;
pub mod session;
pub mod settings;
pub mod skills;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use tokio::sync::broadcast;

use crate::agent_process::process::AgentProcessState;
use crate::api::ApiConfig;
use crate::backends::{backend_for_name, ProviderModelCatalog};
use crate::db::DbState;
use crate::effects::executor::EffectExecutor;
use crate::effects::types::WorkflowEffect;
use crate::embeddings::{EmbeddingEngine, VibeState};
use crate::identity::IdentityStore;
use crate::mcp::McpAuthState;
use crate::models::{ProviderUsageSnapshot, TriggerEvent};
use crate::notify::Notifier;
use crate::services::{PtyState, Services};
use crate::sync::SyncMessage;

pub use account_manager::AccountManager;

pub struct OrchestratorBuilder {
    db: Arc<DbState>,
    services: Arc<Services>,
    process_state: Arc<AgentProcessState>,
    mcp_auth: Arc<McpAuthState>,
    warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    pty_state: Arc<PtyState>,
    permission_responses: broadcast::Sender<(String, String)>,
    run_completions: broadcast::Sender<String>,
    prompt_responses: broadcast::Sender<(String, String)>,
    trigger_events: broadcast::Sender<TriggerEvent>,
    session_allowed_tools: Arc<Mutex<HashSet<String>>>,
    identity_store: Arc<Mutex<Option<IdentityStore>>>,
    mcp_binary_path: String,
    config_dir: PathBuf,
    schema_dir: Option<PathBuf>,
    mcp_callback_port: u16,
    embedding_engine: Option<Arc<std::sync::Mutex<EmbeddingEngine>>>,
    vibe_state: Option<Arc<VibeState>>,
    account_manager: Arc<AccountManager>,
    sync_tx: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<SyncMessage>>>>,
    notifier: Notifier,
    api_config: ApiConfig,
    effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
    model_catalog: Arc<RwLock<HashMap<String, ProviderModelCatalog>>>,
    provider_usage_snapshots: Arc<RwLock<HashMap<String, ProviderUsageSnapshot>>>,
}

impl OrchestratorBuilder {
    pub fn new(db: Arc<DbState>, services: Arc<Services>, config_dir: PathBuf) -> Self {
        let process_state = Arc::new(AgentProcessState::default());
        let mcp_auth = Arc::new(McpAuthState::new(config_dir.clone()));
        let pty_state = Arc::new(PtyState::default());
        let permission_responses = broadcast::channel(16).0;
        let run_completions = broadcast::channel(64).0;
        let prompt_responses = broadcast::channel(16).0;
        let trigger_events = broadcast::channel(256).0;
        let session_allowed_tools = Arc::new(Mutex::new(HashSet::new()));
        let identity_store = Arc::new(Mutex::new(None));
        let account_manager = Arc::new(AccountManager::new(db.clone(), services.emitter.clone()));
        let sync_tx = Arc::new(std::sync::Mutex::new(None));
        let notifier = Notifier::new(sync_tx.clone(), services.emitter.clone());

        Self {
            db,
            services,
            process_state,
            mcp_auth,
            warm_gc: None,
            pty_state,
            permission_responses,
            run_completions,
            prompt_responses,
            trigger_events,
            session_allowed_tools,
            identity_store,
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir,
            schema_dir: None,
            mcp_callback_port: 0,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx,
            notifier,
            api_config: ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(RwLock::new(HashMap::new())),
            provider_usage_snapshots: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn process_state(mut self, process_state: Arc<AgentProcessState>) -> Self {
        self.process_state = process_state;
        self
    }

    pub fn mcp_auth(mut self, mcp_auth: Arc<McpAuthState>) -> Self {
        self.mcp_auth = mcp_auth;
        self
    }

    pub fn warm_gc(
        mut self,
        warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    ) -> Self {
        self.warm_gc = warm_gc;
        self
    }

    pub fn pty_state(mut self, pty_state: Arc<PtyState>) -> Self {
        self.pty_state = pty_state;
        self
    }

    pub fn permission_responses(
        mut self,
        permission_responses: broadcast::Sender<(String, String)>,
    ) -> Self {
        self.permission_responses = permission_responses;
        self
    }

    pub fn run_completions(mut self, run_completions: broadcast::Sender<String>) -> Self {
        self.run_completions = run_completions;
        self
    }

    pub fn prompt_responses(
        mut self,
        prompt_responses: broadcast::Sender<(String, String)>,
    ) -> Self {
        self.prompt_responses = prompt_responses;
        self
    }

    pub fn trigger_events(mut self, trigger_events: broadcast::Sender<TriggerEvent>) -> Self {
        self.trigger_events = trigger_events;
        self
    }

    pub fn identity_store(mut self, identity_store: Option<IdentityStore>) -> Self {
        self.identity_store = Arc::new(Mutex::new(identity_store));
        self
    }

    pub fn mcp_binary_path(mut self, mcp_binary_path: impl Into<String>) -> Self {
        self.mcp_binary_path = mcp_binary_path.into();
        self
    }

    pub fn schema_dir(mut self, schema_dir: Option<PathBuf>) -> Self {
        self.schema_dir = schema_dir;
        self
    }

    pub fn mcp_callback_port(mut self, mcp_callback_port: u16) -> Self {
        self.mcp_callback_port = mcp_callback_port;
        self
    }

    pub fn embedding_engine(
        mut self,
        embedding_engine: Option<Arc<std::sync::Mutex<EmbeddingEngine>>>,
    ) -> Self {
        self.embedding_engine = embedding_engine;
        self
    }

    pub fn vibe_state(mut self, vibe_state: Option<Arc<VibeState>>) -> Self {
        self.vibe_state = vibe_state;
        self
    }

    pub fn account_manager(mut self, account_manager: Arc<AccountManager>) -> Self {
        self.account_manager = account_manager;
        self
    }

    pub fn sync_tx(
        mut self,
        sync_tx: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<SyncMessage>>>>,
    ) -> Self {
        self.sync_tx = sync_tx;
        self
    }

    pub fn notifier(mut self, notifier: Notifier) -> Self {
        self.notifier = notifier;
        self
    }

    pub fn api_config(mut self, api_config: ApiConfig) -> Self {
        self.api_config = api_config;
        self
    }

    pub fn effect_tx(
        mut self,
        effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
    ) -> Self {
        self.effect_tx = effect_tx;
        self
    }

    pub fn build(self) -> Orchestrator {
        Orchestrator {
            db: self.db,
            services: self.services,
            process_state: self.process_state,
            mcp_auth: self.mcp_auth,
            warm_gc: self.warm_gc,
            pty_state: self.pty_state,
            permission_responses: self.permission_responses,
            run_completions: self.run_completions,
            prompt_responses: self.prompt_responses,
            trigger_events: self.trigger_events,
            session_allowed_tools: self.session_allowed_tools,
            identity_store: self.identity_store,
            mcp_binary_path: self.mcp_binary_path,
            config_dir: self.config_dir,
            schema_dir: self.schema_dir,
            mcp_callback_port: self.mcp_callback_port,
            embedding_engine: self.embedding_engine,
            vibe_state: self.vibe_state,
            account_manager: self.account_manager,
            sync_tx: self.sync_tx,
            notifier: self.notifier,
            api_config: self.api_config,
            effect_tx: self.effect_tx,
            executor: Arc::new(OnceLock::new()),
            model_catalog: self.model_catalog,
            provider_usage_snapshots: self.provider_usage_snapshots,
        }
    }
}

/// Central runtime state for agent orchestration.
///
/// Created once at startup by each host (Tauri app, cairn-server).
/// Passed to all orchestration functions as `&Orchestrator`.
#[derive(Clone)]
pub struct Orchestrator {
    /// Database connection state (SQLite + Diesel)
    pub db: Arc<DbState>,
    /// Service abstractions (event emitter, process spawner, clock, filesystem)
    pub services: Arc<Services>,
    /// Active Claude CLI process tracking
    pub process_state: Arc<AgentProcessState>,
    /// MCP authentication (shared secret for TOTP passcodes)
    pub mcp_auth: Arc<McpAuthState>,
    /// Warm process GC (optional — hosts may not enable warm processes)
    pub warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    /// Active PTY sessions (terminals)
    pub pty_state: Arc<PtyState>,

    // === Broadcast channels for cross-component communication ===
    /// Permission response broadcast: (request_id, response_json)
    /// Hosts send on this channel when a user responds to a permission prompt.
    pub permission_responses: broadcast::Sender<(String, String)>,
    /// Run completion broadcast: run_id
    /// Emitted when a run finishes (used by sub-agent handlers to unblock).
    pub run_completions: broadcast::Sender<String>,
    /// Prompt response broadcast: (prompt_id, response_text)
    /// Hosts send on this channel when a user responds to an ask_user prompt.
    pub prompt_responses: broadcast::Sender<(String, String)>,

    /// Trigger event channel for event-driven recipe dispatch.
    /// Emission sites send lean `TriggerEvent` values; each host subscribes
    /// and dispatches through `process_trigger_event`.
    pub trigger_events: broadcast::Sender<TriggerEvent>,

    /// Tools auto-allowed via "Allow for Session" permission responses.
    /// Checked in handle_permission_prompt before showing UI.
    pub session_allowed_tools: Arc<Mutex<HashSet<String>>>,

    /// Multi-account identity store. None = anonymous/legacy mode.
    /// Populated from local identity store (desktop) or JWT claims (server).
    pub identity_store: Arc<Mutex<Option<IdentityStore>>>,

    // === Host-specific paths (set by Tauri or cairn-server) ===
    /// Path to the cairn-mcp binary
    pub mcp_binary_path: String,
    /// Directory for writing MCP config files
    pub config_dir: PathBuf,
    /// Directory containing bundled preset schemas (None if not available)
    pub schema_dir: Option<PathBuf>,
    /// Port for the MCP callback server
    pub mcp_callback_port: u16,
    /// Embedding engine for inline computation (None if init failed)
    pub embedding_engine: Option<Arc<std::sync::Mutex<EmbeddingEngine>>>,
    /// Vibe state for embedding-based color assignment (None if embeddings unavailable)
    pub vibe_state: Option<Arc<VibeState>>,

    // === Team connection state (multi-team) ===
    /// Multi-team manager: handles DB-backed team configs, JWT refresh, credential resolution

    // === Account connection (replaces teams for individual users) ===
    /// Account manager: device code auth, single-account JWT lifecycle
    pub account_manager: Arc<AccountManager>,

    // === Remote sync channel ===
    /// Sync sender for dual-writing to cloud. None when not connected or no plan.
    /// Wrapped in Arc<Mutex> so it can be set after account connection.
    pub sync_tx: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<SyncMessage>>>>,

    // === Unified notification ===
    /// Combined sync + emit for write operations. Shares `sync_tx` and `emitter`.
    pub notifier: Notifier,

    // === Cloud API ===
    /// Cloud API endpoint configuration (account, sync, bug reports)
    pub api_config: ApiConfig,

    /// Typed effect queue for async draining.
    ///
    /// Sync callers (e.g. `finalize_run`) push `WorkflowEffect`s here.
    /// An async drainer task (spawned by each host) receives effects and
    /// calls `execute_effects`. This replaces the ad-hoc `"dag-advance"`
    /// event → listener → async handler chain.
    ///
    /// `None` when the host hasn't set up an effect drainer yet (backward compat).
    pub effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,

    /// Host-specific effect executor for `StartAgentJobs` and `ExecuteAction`.
    ///
    /// Wrapped in `Arc<OnceLock<...>>` so it's Clone-compatible (Orchestrator
    /// derives Clone) and settable after construction via `&self`. The executor
    /// may need a reference to the host's wrapper (e.g. `Arc<ServerState>`)
    /// which isn't available until after the Orchestrator is built.
    pub executor: Arc<OnceLock<Arc<dyn EffectExecutor>>>,

    /// Cached provider model catalog loaded at startup and refreshed on demand.
    pub model_catalog: Arc<RwLock<HashMap<String, ProviderModelCatalog>>>,
    /// Latest provider/account usage snapshots keyed by backend name.
    pub provider_usage_snapshots: Arc<RwLock<HashMap<String, ProviderUsageSnapshot>>>,
}

impl Orchestrator {
    pub fn builder(
        db: Arc<DbState>,
        services: Arc<Services>,
        config_dir: PathBuf,
    ) -> OrchestratorBuilder {
        OrchestratorBuilder::new(db, services, config_dir)
    }

    pub fn set_executor(
        &self,
        executor: Arc<dyn EffectExecutor>,
    ) -> Result<(), Arc<dyn EffectExecutor>> {
        self.executor.set(executor)
    }

    /// Send a sync message to the cloud (no-op if sync not active).
    pub fn sync(&self, msg: SyncMessage) {
        if let Ok(guard) = self.sync_tx.lock() {
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(msg);
            }
        }
    }

    /// Prepare the sync task for a connected paid account.
    /// Returns a future to spawn and the sender, or None if not eligible.
    /// Caller must spawn the future on a runtime (e.g. `tauri::async_runtime::spawn`).
    pub fn prepare_sync(
        &self,
    ) -> Option<(
        impl std::future::Future<Output = ()>,
        tokio::sync::mpsc::UnboundedSender<SyncMessage>,
        String, // email for logging
    )> {
        let conn = match self.account_manager.get_connection() {
            Ok(Some(c)) if c.plan != "free" => c,
            _ => return None,
        };

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let am = self.account_manager.clone();
        let jwt_provider = Arc::new(move || am.get_jwt().ok().flatten());
        let device_id = conn.device_id.clone();
        let email = conn.email.clone();

        let task =
            crate::sync::SyncTask::new(rx, jwt_provider, device_id, self.api_config.clone()).run();
        Some((task, tx, email))
    }

    /// Activate sync by storing the sender. Call after spawning the task future.
    pub fn activate_sync(&self, tx: tokio::sync::mpsc::UnboundedSender<SyncMessage>) {
        if let Ok(mut guard) = self.sync_tx.lock() {
            *guard = Some(tx);
        }
    }

    /// Evict a warm process if needed to make room for a new one.
    /// Returns the run_id of the evicted process, if any.
    pub fn collect_warm_if_needed(&self) -> Option<String> {
        let gc = self.warm_gc.as_ref()?;
        let eviction_candidate = {
            let mut conn = self.db.conn.lock().ok()?;
            gc.find_eviction_candidate(&self.process_state, &mut conn)
        };

        if let Some(ref run_id) = eviction_candidate {
            log::info!(
                "GC: evicting warm process {}",
                &run_id[..run_id.len().min(8)]
            );
            if let Err(e) = lifecycle::kill_session_with_reason(self, run_id, "warm_evict") {
                log::error!("GC: failed to kill evicted process {}: {}", run_id, e);
            }
        }

        eviction_candidate
    }

    pub fn get_model_catalog(&self) -> Vec<ProviderModelCatalog> {
        let Ok(catalog) = self.model_catalog.read() else {
            return Vec::new();
        };
        let mut providers: Vec<_> = catalog.values().cloned().collect();
        providers.sort_by(|a, b| a.backend.cmp(&b.backend));
        providers
    }

    pub fn refresh_model_catalog(&self) {
        for backend_name in ["claude", "codex"] {
            let backend = backend_for_name(Some(backend_name));
            let entry = match backend.discover_models() {
                Ok(models) => ProviderModelCatalog {
                    backend: backend_name.to_string(),
                    models,
                    refreshed_at: Some(chrono::Utc::now().timestamp()),
                    error: None,
                },
                Err(error) => ProviderModelCatalog {
                    backend: backend_name.to_string(),
                    models: Vec::new(),
                    refreshed_at: Some(chrono::Utc::now().timestamp()),
                    error: Some(error),
                },
            };
            if let Ok(mut catalog) = self.model_catalog.write() {
                catalog.insert(backend_name.to_string(), entry);
            }
        }
    }

    pub fn spawn_model_catalog_refresh(&self) {
        let orch = self.clone();
        std::thread::spawn(move || {
            orch.refresh_model_catalog();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::gc::WarmProcessGC;
    use crate::agent_process::process::RunHandle;
    use crate::diesel_models::NewRun;
    use crate::services::testing::{MockChildProcess, TestServicesBuilder};
    use crate::test_utils::test_diesel_conn;
    use diesel::prelude::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn test_orchestrator(conn: diesel::sqlite::SqliteConnection, max_warm: usize) -> Orchestrator {
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());

        Orchestrator::builder(db, services, std::path::PathBuf::from("/tmp"))
            .warm_gc(Some(Arc::new(WarmProcessGC::new(max_warm))))
            .mcp_callback_port(3847)
            .build()
    }

    #[test]
    fn builder_defaults_match_expected_runtime_defaults() {
        let db = Arc::new(DbState {
            conn: Mutex::new(test_diesel_conn()),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = Orchestrator::builder(
            db.clone(),
            services.clone(),
            std::path::PathBuf::from("/tmp/config"),
        )
        .build();

        assert!(Arc::ptr_eq(&orch.db, &db));
        assert!(Arc::ptr_eq(&orch.services, &services));
        assert!(orch.warm_gc.is_none());
        assert_eq!(orch.mcp_binary_path, "cairn-mcp");
        assert_eq!(orch.config_dir, std::path::PathBuf::from("/tmp/config"));
        assert!(orch.schema_dir.is_none());
        assert_eq!(orch.mcp_callback_port, 0);
        assert!(orch.embedding_engine.is_none());
        assert!(orch.vibe_state.is_none());
        assert!(orch.effect_tx.is_none());
        assert!(orch.executor.get().is_none());
        assert!(orch.sync_tx.lock().unwrap().is_none());
        assert!(orch.identity_store.lock().unwrap().is_none());
        assert!(orch.model_catalog.read().unwrap().is_empty());
    }

    #[test]
    fn builder_applies_explicit_overrides() {
        let db = Arc::new(DbState {
            conn: Mutex::new(test_diesel_conn()),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let process_state = Arc::new(AgentProcessState::default());
        let mcp_auth = Arc::new(crate::mcp::McpAuthState::new(std::path::PathBuf::from(
            "/tmp/auth",
        )));
        let pty_state = Arc::new(crate::services::PtyState::default());
        let permission_responses = tokio::sync::broadcast::channel(16).0;
        let run_completions = tokio::sync::broadcast::channel(64).0;
        let prompt_responses = tokio::sync::broadcast::channel(16).0;
        let trigger_events = tokio::sync::broadcast::channel(256).0;
        let identity_store = crate::identity::IdentityStore {
            user_id: "user-1".to_string(),
            accounts: vec![],
            git_identities: vec![],
            project_overrides: Default::default(),
        };
        let schema_dir = std::path::PathBuf::from("/tmp/schemas");
        let api_config = crate::api::ApiConfig {
            base_url: "http://localhost:9000".to_string(),
        };
        let (effect_tx, _effect_rx) = tokio::sync::mpsc::unbounded_channel();

        let mut permission_rx = permission_responses.subscribe();
        let mut run_rx = run_completions.subscribe();
        let mut prompt_rx = prompt_responses.subscribe();
        let mut trigger_rx = trigger_events.subscribe();

        let orch = Orchestrator::builder(
            db.clone(),
            services.clone(),
            std::path::PathBuf::from("/tmp/config"),
        )
        .process_state(process_state.clone())
        .mcp_auth(mcp_auth.clone())
        .pty_state(pty_state.clone())
        .permission_responses(permission_responses)
        .run_completions(run_completions)
        .prompt_responses(prompt_responses)
        .trigger_events(trigger_events)
        .identity_store(Some(identity_store.clone()))
        .mcp_binary_path("custom-mcp")
        .schema_dir(Some(schema_dir.clone()))
        .mcp_callback_port(9123)
        .api_config(api_config.clone())
        .effect_tx(Some(effect_tx))
        .build();

        assert!(Arc::ptr_eq(&orch.process_state, &process_state));
        assert!(Arc::ptr_eq(&orch.mcp_auth, &mcp_auth));
        assert!(Arc::ptr_eq(&orch.pty_state, &pty_state));
        assert_eq!(orch.mcp_binary_path, "custom-mcp");
        assert_eq!(orch.schema_dir, Some(schema_dir));
        assert_eq!(orch.mcp_callback_port, 9123);
        assert_eq!(orch.api_config.base_url, api_config.base_url);
        assert!(orch.effect_tx.is_some());
        assert_eq!(
            orch.identity_store
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .user_id,
            identity_store.user_id
        );

        orch.permission_responses
            .send(("req-1".to_string(), "yes".to_string()))
            .unwrap();
        orch.run_completions.send("run-1".to_string()).unwrap();
        orch.prompt_responses
            .send(("run-1".to_string(), "reply".to_string()))
            .unwrap();
        orch.trigger_events
            .send(TriggerEvent::JobEnded {
                job_id: "job-1".to_string(),
                status: "complete".to_string(),
                execution_id: Some("exec-1".to_string()),
                issue_id: Some("issue-1".to_string()),
                project_id: "proj-1".to_string(),
            })
            .unwrap();

        assert_eq!(
            permission_rx.try_recv().unwrap(),
            ("req-1".to_string(), "yes".to_string())
        );
        assert_eq!(run_rx.try_recv().unwrap(), "run-1".to_string());
        assert_eq!(
            prompt_rx.try_recv().unwrap(),
            ("run-1".to_string(), "reply".to_string())
        );
        assert!(matches!(
            trigger_rx.try_recv().unwrap(),
            TriggerEvent::JobEnded {
                job_id,
                status,
                execution_id,
                issue_id,
                project_id,
            } if job_id == "job-1"
                && status == "complete"
                && execution_id.as_deref() == Some("exec-1")
                && issue_id.as_deref() == Some("issue-1")
                && project_id == "proj-1"
        ));
    }

    fn insert_live_run(conn: &mut diesel::sqlite::SqliteConnection, run_id: &str) {
        let now = chrono::Utc::now().timestamp() as i32;
        diesel::insert_into(crate::schema::runs::table)
            .values(&NewRun {
                id: run_id,
                issue_id: None,
                project_id: None,
                job_id: None,
                status: Some("live"),
                session_id: Some("session-1"),
                error_message: None,
                started_at: Some(now),
                exited_at: None,
                created_at: now,
                updated_at: now,
                backend: None,
                exit_reason: None,
                start_mode: None,
                chat_id: None,
            })
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn collect_warm_if_needed_evicts_without_deadlocking() {
        let mut conn = test_diesel_conn();
        insert_live_run(&mut conn, "run-1");
        let orch = test_orchestrator(conn, 1);

        let child = Arc::new(Mutex::new(Some(Box::new(MockChildProcess::with_stdout(
            999_999,
            vec![],
        ))
            as Box<dyn crate::services::ChildProcess>)));
        let stdin = Arc::new(Mutex::new(None));
        let mut handle = RunHandle::new(child, stdin, Some("session-1".to_string()), None);
        handle.transition_to_warm();

        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        let orch_for_thread = orch.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = orch_for_thread.collect_warm_if_needed();
            let _ = tx.send(result);
        });

        let evicted = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("collect_warm_if_needed deadlocked during warm eviction");
        assert_eq!(evicted.as_deref(), Some("run-1"));

        assert!(!orch
            .process_state
            .processes
            .lock()
            .unwrap()
            .contains_key("run-1"));

        let run_status: Option<String> = crate::schema::runs::table
            .find("run-1")
            .select(crate::schema::runs::status)
            .first(&mut *orch.db.conn.lock().unwrap())
            .unwrap();
        assert_eq!(run_status.as_deref(), Some("exited"));
    }
}
