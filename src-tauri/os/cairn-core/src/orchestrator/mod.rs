//! Orchestrator — bundles all runtime state needed for agent execution.
//!
//! Both the Tauri desktop app and cairn-server create their own `Orchestrator`.
//! All orchestration functions take `&Orchestrator` instead of framework-specific
//! handles (e.g. `&AppHandle`).

pub mod account_events;
pub mod account_manager;
pub mod agents;
pub mod attention;
pub mod attention_delivery;
pub mod attention_push;
pub mod attention_retirement;
pub mod automations;
pub mod base_advance;
pub mod build_change;
pub mod build_services;
mod claude_credentials;
pub mod config_resource;
pub mod conflict_session;
pub mod contended_identifiers;
pub mod custom_providers;
mod decide;
mod subscription_periods;
#[cfg(test)]
pub(crate) use decide::tests as decide_fixtures;
pub(crate) use decide::{DecideRequest, DecisionFailure};
pub(crate) mod base_branch_sync;
mod embed;
pub use decide::DecisionDiagnostic;
pub mod device_presence;
pub mod docs;
pub mod executor_registry; // Synced visibility; live placement remains in fleet.
pub(crate) mod generation_cache;
pub mod identity;
pub mod inference;
pub(crate) mod landing;
pub mod lifecycle;
pub mod model_defaults;
pub mod models;
pub mod object_plane;
pub mod parent_wake;
pub mod provider_pool;
pub mod recipes;
pub mod replay_base;
pub mod session;
mod settings;
pub mod skills;
mod store_lock;
use store_lock::summarize_bounded;
#[cfg(test)]
use store_lock::JJ_STORE_LOCK_SAMPLE_CAPACITY;
pub(crate) use store_lock::{
    branch_preparation_operation, describe_preparation_wait, StoreLockStatus,
};
pub use store_lock::{
    JjStoreGuard, JjStoreLockMetrics, JjStoreOperationTrace, JjStorePhaseTimer, StoreLockClass,
};
mod team_attention_push;
pub mod usage_refresh;
pub mod wakes;
pub mod workflow_configs;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use tokio::sync::broadcast;
use tokio::sync::Mutex as TokioMutex;

use crate::agent_process::process::AgentProcessState;
use crate::api::ApiConfig;
use crate::backends::backend_for_name;
use crate::credentials::{CredentialStore, CredentialStoreHandle};
use crate::db::DbState;
use crate::effects::executor::EffectExecutor;
use crate::effects::types::WorkflowEffect;
use crate::embeddings::{
    spawn_embed_worker, turns::SemanticSearch, EmbedGate, EmbedJob, PositionConfig, PositionKind,
    PositionMeta, VibeState,
};
use crate::mcp::gateway::McpGateway;
use crate::mcp::McpAuthState;
use crate::models::{get_latest_context_token_event, ContextTokenState, TriggerEvent};
use crate::notify::Notifier;
use crate::services::{ChildProcess, PtyState, Services};
use cairn_models::types::catalog::ProviderModelCatalog;
use cairn_models::types::usage::ProviderUsageSnapshot;

pub use crate::account::AnonDeviceManager;
pub use account_manager::AccountManager;
pub use attention::{AttentionEvent, AttentionFact, AttentionFactKey};

type ProviderUsageSnapshots = Arc<cairn_models::usage::book::UsageBook>;
pub(crate) type ProviderUsageCaptures = Arc<Mutex<HashSet<(String, String)>>>;

pub struct OrchestratorBuilder {
    db: Arc<DbState>,
    services: Arc<Services>,
    process_state: Arc<AgentProcessState>,
    mcp_auth: Arc<McpAuthState>,
    warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    pty_state: Arc<PtyState>,
    repl_state: Arc<crate::mcp::handlers::repl::ReplState>,
    permission_responses: broadcast::Sender<(String, String)>,
    run_completions: broadcast::Sender<String>,
    prompt_responses: broadcast::Sender<(String, String)>,
    browser_bridge_responses: broadcast::Sender<(String, String)>,
    browser_nav_events: broadcast::Sender<crate::browsers::BrowserNavEvent>,
    trigger_events: broadcast::Sender<TriggerEvent>,
    attention_changed: broadcast::Sender<AttentionEvent>,
    session_allowed_tools: Arc<Mutex<HashSet<String>>>,
    session_allowed_crossings: Arc<Mutex<HashSet<String>>>,
    credential_store: Option<CredentialStore>,
    subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    models_runtime: Option<models::ModelRuntime>,
    mcp_binary_path: String,
    jj_binary_path: String,
    config_dir: PathBuf,
    schema_dir: Option<PathBuf>,
    mcp_callback_port: u16,
    vibe_state: Option<Arc<VibeState>>,
    account_manager: Arc<AccountManager>,
    notifier: Notifier,
    thread_status_cache: Arc<generation_cache::ThreadStatusCache>,
    issue_status_cache: Arc<generation_cache::IssueStatusCache>,
    api_config: ApiConfig,
    effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
    browser_command_tx: Option<crate::browsers::BrowserCommandTx>,
    model_catalog: Arc<RwLock<HashMap<String, ProviderModelCatalog>>>,
    catalog_rediscovery: bool,
    provider_usage_snapshots: ProviderUsageSnapshots,
    provider_usage_captures: ProviderUsageCaptures,
    reconciled_subscriptions: subscription_periods::ReconciledSubscriptions,
    context_token_snapshots: Arc<RwLock<HashMap<String, ContextTokenState>>>,
    team_attention_sender: Arc<dyn team_attention_push::TeamAttentionSender>,
    recent_turn_end_check_requests: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    artifact_revision_quiet: std::time::Duration,
    boot_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TurnEndCancelReason {
    IssueResolved = 0,
    BranchAdvanced = 1,
    UserRequested = 2,
    OperatorCompact = 3,
}

impl TurnEndCancelReason {
    pub(crate) fn describe(self) -> &'static str {
        match self {
            Self::IssueResolved => "its issue resolved",
            Self::BranchAdvanced => "its branch advanced",
            Self::UserRequested => "a user requested cancellation",
            Self::OperatorCompact => "the operator compacted its session",
        }
    }

    // Keep this exhaustive and adjacent to the decoder: encoding a new variant
    // without decoding it below would make `cancelled()` park forever after the
    // cancellation had already won its one notification.
    fn to_discriminant(self) -> u8 {
        match self {
            Self::IssueResolved => 0,
            Self::BranchAdvanced => 1,
            Self::UserRequested => 2,
            Self::OperatorCompact => 3,
        }
    }

    fn from_discriminant(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::IssueResolved),
            1 => Some(Self::BranchAdvanced),
            2 => Some(Self::UserRequested),
            3 => Some(Self::OperatorCompact),
            NOT_CANCELLED => None,
            _ => None,
        }
    }
}

const NOT_CANCELLED: u8 = u8::MAX;

/// The jj an orchestrator drives when its host names none.
///
/// The runtime always names the bundled sidecar, so outside tests this is only a
/// bare `jj` for PATH. A test build defaults to the sidecar the suites provision
/// and gate on (`cairn_vcs::jj::testing::provisioned_jj`): a fixture that builds
/// its store with that sidecar and then publishes through this orchestrator must
/// drive the same binary, or the outcome depends on whether the machine happens
/// to have its own jj on PATH. `JjEnv::resolve` still honours `CAIRN_JJ_BIN`
/// ahead of this path, so a suite that pins a private copy keeps it.
fn default_jj_binary_path() -> String {
    #[cfg(any(test, feature = "test-utils"))]
    if let Some(sidecar) = cairn_vcs::jj::testing::provisioned_jj() {
        return sidecar.to_string_lossy().into_owned();
    }
    "jj".to_string()
}

impl OrchestratorBuilder {
    pub(crate) fn new(db: Arc<DbState>, services: Arc<Services>, config_dir: PathBuf) -> Self {
        let process_state = Arc::new(AgentProcessState::default());
        let mcp_auth = Arc::new(McpAuthState::new(config_dir.clone()));
        let pty_state = Arc::new(PtyState::default());
        let repl_state = Arc::new(crate::mcp::handlers::repl::ReplState::default());
        let permission_responses = broadcast::channel(16).0;
        let run_completions = broadcast::channel(64).0;
        let prompt_responses = broadcast::channel(16).0;
        let browser_bridge_responses = broadcast::channel(16).0;
        let browser_nav_events = broadcast::channel(64).0;
        let trigger_events = broadcast::channel(256).0;
        let attention_changed = broadcast::channel(64).0;
        let session_allowed_tools = Arc::new(Mutex::new(HashSet::new()));
        let session_allowed_crossings = Arc::new(Mutex::new(HashSet::new()));
        let credential_store = None;
        // Wrap the host's emitter once, here, so that every notification the
        // runtime produces — through `Notifier`, through `services.emitter`, from
        // anywhere in the crate — passes the projection generations on its way
        // out. Wrapping in `build()` instead would leave `Notifier` bound to the
        // raw emitter and silently miss the job and thread mutations it carries.
        let thread_status_cache = Arc::new(generation_cache::ThreadStatusCache::with_coalescing(
            "thread status projection",
            generation_cache::THREAD_STATUS_COALESCE_FLOOR,
        ));
        let issue_status_cache = Arc::new(generation_cache::IssueStatusCache::new(
            "issue status projection",
        ));
        let services = Arc::new(Services {
            process: services.process.clone(),
            git: services.git.clone(),
            http: services.http.clone(),
            emitter: Arc::new(generation_cache::ProjectionInvalidatingEmitter::new(
                services.emitter.clone(),
                thread_status_cache.clone(),
                issue_status_cache.clone(),
            )),
            clock: services.clock.clone(),
            fs: services.fs.clone(),
            pty_factory: services.pty_factory.clone(),
        });
        let account_manager = Arc::new(AccountManager::new(db.clone(), services.emitter.clone()));
        let notifier = Notifier::new(services.emitter.clone());
        let recent_turn_end_check_requests = Arc::new(Mutex::new(HashMap::new()));

        Self {
            db,
            services,
            process_state,
            mcp_auth,
            warm_gc: None,
            pty_state,
            repl_state,
            permission_responses,
            run_completions,
            prompt_responses,
            browser_bridge_responses,
            browser_nav_events,
            trigger_events,
            attention_changed,
            session_allowed_tools,
            session_allowed_crossings,
            credential_store,
            subscriptions: Arc::new(cairn_models::subscription::Unsubscribed),
            models_runtime: None,
            mcp_binary_path: "cairn-cmd".to_string(),
            jj_binary_path: default_jj_binary_path(),
            config_dir,
            schema_dir: None,
            mcp_callback_port: 0,
            vibe_state: None,
            account_manager,
            notifier,
            thread_status_cache,
            issue_status_cache,
            api_config: ApiConfig::default(),
            effect_tx: None,
            browser_command_tx: None,
            model_catalog: Arc::new(RwLock::new(HashMap::new())),
            // Off in test builds, with no way to turn it off in any other; see
            // `Orchestrator::catalog_rediscovery`.
            catalog_rediscovery: !cfg!(any(test, feature = "test-utils")),
            provider_usage_snapshots: Arc::new(cairn_models::usage::book::UsageBook::new()),
            provider_usage_captures: Arc::new(Mutex::new(HashSet::new())),
            reconciled_subscriptions: Arc::default(),
            context_token_snapshots: Arc::new(RwLock::new(HashMap::new())),
            team_attention_sender: Arc::new(team_attention_push::HttpTeamAttentionSender::default()),
            recent_turn_end_check_requests,
            artifact_revision_quiet: crate::execution::artifact_revisions::DEFAULT_QUIET_PERIOD,
            boot_at: chrono::Utc::now().timestamp(),
        }
    }

    /// Override how long an artifact must go unwritten before its revision is
    /// delivered to the jobs launched with an earlier version. Tests shorten it
    /// so the real write path delivers within the test.
    pub fn artifact_revision_quiet(mut self, quiet: std::time::Duration) -> Self {
        self.artifact_revision_quiet = quiet;
        self
    }

    /// Override when this host began owning runs. See [`Orchestrator::boot_at`].
    ///
    /// Only test fixtures set this: they seed `runs` rows with epoch timestamps,
    /// so a fixture orchestrator passes `0` to state that it owns every run in
    /// its fixture database. The ownership fence's own tests set it explicitly to
    /// place seeded runs on either side of boot.
    pub fn boot_at(mut self, boot_at: i64) -> Self {
        self.boot_at = boot_at;
        self
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

    pub fn repl_state(mut self, repl_state: Arc<crate::mcp::handlers::repl::ReplState>) -> Self {
        self.repl_state = repl_state;
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

    pub fn attention_changed(
        mut self,
        attention_changed: broadcast::Sender<AttentionEvent>,
    ) -> Self {
        self.attention_changed = attention_changed;
        self
    }

    pub fn credential_store(mut self, credential_store: Option<CredentialStore>) -> Self {
        self.credential_store = credential_store;
        self
    }

    /// Subscription mechanics (managed account homes, subscription usage,
    /// Codex refresh). Defaults to `Unsubscribed`: a runtime that installs
    /// nothing answers every subscription question with "no support".
    pub fn subscriptions(
        mut self,
        subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    ) -> Self {
        self.subscriptions = subscriptions;
        self
    }

    /// The facet implementations launches go through. Defaults to the ones
    /// that reach providers, with sessions on this machine; tests install
    /// recorders here.
    pub fn models_runtime(mut self, runtime: models::ModelRuntime) -> Self {
        self.models_runtime = Some(runtime);
        self
    }

    pub fn mcp_binary_path(mut self, mcp_binary_path: impl Into<String>) -> Self {
        self.mcp_binary_path = mcp_binary_path.into();
        self
    }

    pub fn jj_binary_path(mut self, jj_binary_path: impl Into<String>) -> Self {
        self.jj_binary_path = jj_binary_path.into();
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

    pub fn vibe_state(mut self, vibe_state: Option<Arc<VibeState>>) -> Self {
        self.vibe_state = vibe_state;
        self
    }

    pub fn account_manager(mut self, account_manager: Arc<AccountManager>) -> Self {
        self.account_manager = account_manager;
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

    /// Let this test orchestrator rediscover every enabled provider's models
    /// the way production does: after each credential change and when startup
    /// maintenance asks. Test builds leave it off (see
    /// [`Orchestrator::catalog_rediscovery`]); a test that turns it on must
    /// enable only providers that answer without leaving the machine.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn catalog_rediscovery(mut self, enabled: bool) -> Self {
        self.catalog_rediscovery = enabled;
        self
    }

    #[cfg(test)]
    fn team_attention_sender(
        mut self,
        sender: Arc<dyn team_attention_push::TeamAttentionSender>,
    ) -> Self {
        self.team_attention_sender = sender;
        self
    }

    pub fn effect_tx(
        mut self,
        effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
    ) -> Self {
        self.effect_tx = effect_tx;
        self
    }

    pub fn browser_command_tx(
        mut self,
        browser_command_tx: Option<crate::browsers::BrowserCommandTx>,
    ) -> Self {
        self.browser_command_tx = browser_command_tx;
        self
    }

    /// Construct the orchestrator. Must not spawn any task that holds a strong
    /// `Orchestrator` clone: such a task keeps every channel the orchestrator owns
    /// (e.g. `browser_command_tx`) open after the caller drops it. Long-lived
    /// workers start through the host-called `start_*` methods instead.
    pub fn build(self) -> Orchestrator {
        // Install the process-wide rustls CryptoProvider at the single startup
        // chokepoint every sync-capable host passes through before it can open a
        // team replica. Turso's sync IO builds its hyper-rustls client the
        // instant the `turso-sync-io` thread spawns (inside
        // turso's `sync::Builder::build()`), so the provider must already be
        // installed by then — doing it lazily per-open races that thread spawn
        // (CAIRN-2196). Idempotent and `Once`-guarded; see
        // `storage::install_crypto_provider`.
        crate::storage::install_crypto_provider();

        // Register the archived-file renderer so archival reconstruction (in
        // `storage`, below the mcp read layer) can reproduce archived reads
        // byte-for-byte. One of two idempotent registration sites: this covers
        // orchestrator-owning construction (including tests that build an
        // orchestrator directly), while `cairn_transport::state::init_database`
        // registers earlier to cover the startup search-index refresh, which
        // reconstructs archived events before the orchestrator is built.
        crate::mcp::handlers::read::register_archived_file_renderer();

        let anon_device_manager = Arc::new(AnonDeviceManager::new(
            self.db.clone(),
            self.api_config.clone(),
        ));
        let fleet = Arc::new(crate::fleet::Fleet::with_residency_route_path(
            self.config_dir.join("build-slot-residency-routes.json"),
        ));
        // One identity for this process in every journal it writes. The
        // runner-event journal attaches first, so its `runner_started` row is
        // the incarnation's first: that row is where this process's connection
        // generations begin, which is what makes them joinable at all.
        let runner_incarnation = format!("runner-{}", crate::fleet::unix_time_ms());
        fleet.attach_runner_event_journal(self.db.local.clone(), runner_incarnation.clone());
        // The execution queue explains its own bindings durably, keyed by this
        // process. Entries a previous incarnation left open are closed rather
        // than resurrected: the futures waiting on them ended with that process.
        fleet.attach_queue_journal(self.db.local.clone(), runner_incarnation);
        // Machines that could not prepare a project's cells stay excluded for
        // that project across a runner restart, until their evidence changes.
        fleet.attach_preparation_store(self.db.local.clone());
        // Work granted to a machine before a restart is still running there.
        // Its debts reload before any purpose item is admitted, so nothing
        // dispatches beside one.
        fleet.attach_purpose_debt_journal(self.db.local.clone());
        let models_runtime = self.models_runtime.unwrap_or_else(|| {
            models::ModelRuntime::local(self.services.process.clone(), self.process_state.clone())
        });
        let credential_store =
            CredentialStoreHandle::new(self.config_dir.clone(), self.credential_store);
        let orchestrator = Orchestrator {
            db: self.db,
            services: self.services,
            process_state: self.process_state,
            mcp_auth: self.mcp_auth,
            run_bearers: Arc::new(crate::mcp::run_bearer::RunBearerBook::default()),
            warm_gc: self.warm_gc,
            pty_state: self.pty_state,
            repl_state: self.repl_state,
            project_overlays: Arc::new(
                crate::mcp::handlers::read::overlay::ProjectOverlayRegistry::default(),
            ),
            park_slots: Arc::new(crate::orchestrator::lifecycle::ParkSlots::default()),
            private_lanes: Arc::new(crate::execution::jobs::PrivateLaneLedger::default()),
            fleet,
            object_plane: Arc::new(crate::orchestrator::object_plane::ObjectPlaneState::default()),
            permission_responses: self.permission_responses,
            run_completions: self.run_completions,
            prompt_responses: self.prompt_responses,
            browser_bridge_responses: self.browser_bridge_responses,
            browser_nav_events: self.browser_nav_events,
            browser_network: Arc::new(crate::browser_network::BrowserNetworkArchive::default()),
            fact_log: crate::routes::FactLogHandle::default(),
            trigger_events: self.trigger_events,
            attention_changed: self.attention_changed,
            session_allowed_tools: self.session_allowed_tools,
            session_allowed_crossings: self.session_allowed_crossings,
            claude_feeds: cairn_models::claude::feeds::ClaudeCredentialFeeds::new(
                credential_store.clone(),
            ),
            credential_store,
            subscriptions: self.subscriptions,
            models_runtime,
            decision_observations: Arc::default(),
            mcp_binary_path: self.mcp_binary_path,
            jj_binary_path: self.jj_binary_path,
            config_dir: self.config_dir,
            schema_dir: self.schema_dir,
            mcp_callback_port: self.mcp_callback_port,
            boot_at: self.boot_at,
            runtime: tokio::runtime::Handle::try_current().ok(),
            attached_ui: Arc::new(Mutex::new(AttachedUi::default())),
            vibe_state: self.vibe_state,
            embed_tx: Arc::new(Mutex::new(None)),
            era_summary_tx: crate::threads::era_worker::sender_slot(),
            tenet_evidence_hint: Arc::new(tokio::sync::Notify::new()),
            embed_gate: Arc::new(EmbedGate::default()),
            semantic_search: std::sync::OnceLock::new(),
            anon_device_manager,
            account_manager: self.account_manager,
            notifier: self.notifier,
            api_config: self.api_config,
            effect_tx: self.effect_tx,
            browser_command_tx: self.browser_command_tx,
            browser_loading: Arc::new(Mutex::new(HashMap::new())),
            executor: Arc::new(OnceLock::new()),
            mcp_gateway: Arc::new(OnceLock::new()),
            desktop_facades: Arc::default(),
            model_catalog: self.model_catalog,
            catalog_rediscovery: self.catalog_rediscovery,
            provider_usage_snapshots: self.provider_usage_snapshots,
            provider_usage_captures: self.provider_usage_captures,
            reconciled_subscriptions: self.reconciled_subscriptions,
            sidebar_active_issues: Arc::new(std::sync::RwLock::new(None)),
            sidebar_active_issues_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            sidebar_active_issues_rebuild: Arc::new(TokioMutex::new(())),
            sidebar_active_issues_rebuild_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            context_token_snapshots: self.context_token_snapshots,
            team_attention_sender: self.team_attention_sender,
            team_attention_push_dedupe: Arc::new(TokioMutex::new(HashMap::new())),
            execution_locks: Arc::new(Mutex::new(HashMap::new())),
            job_launch_locks: Arc::new(Mutex::new(HashMap::new())),
            job_launch_claims: Arc::new(Mutex::new(HashMap::new())),
            jj_store_locks: Arc::new(Mutex::new(HashMap::new())),
            jj_store_lock_metrics: Arc::new(Mutex::new(HashMap::new())),
            base_branch_sync: Arc::default(),
            build_service_children: Arc::new(Mutex::new(HashMap::new())),
            build_service_reconcile: Arc::new(Mutex::new(())),
            build_service_runtime: Arc::new(Mutex::new(HashMap::new())),
            compile_cache_service: Arc::new(RwLock::new(None)),
            agent_completion_attention_dedupe: Arc::new(Mutex::new(HashSet::new())),
            codemap_refresh_in_flight: Arc::new(Mutex::new(HashSet::new())),
            session_reseed_fallback_attempted: Arc::new(Mutex::new(HashSet::new())),
            turn_end_checks_in_flight: Arc::new(Mutex::new(HashMap::new())),
            recent_turn_end_check_requests: self.recent_turn_end_check_requests,
            main_cadence: Arc::new(Mutex::new(Default::default())),
            artifact_revisions: Arc::new(
                crate::execution::artifact_revisions::RevisionDeliveries::new(
                    self.artifact_revision_quiet,
                ),
            ),
            write_checks_in_flight: Arc::new(Mutex::new(HashMap::new())),
            node_check_status_cache: Arc::new(
                crate::execution::checks_status::NodeCheckStatusCache::default(),
            ),
            pr_refresh_cache: Arc::new(
                crate::orchestrator::generation_cache::GenerationCache::new(
                    "pr refresh projection",
                ),
            ),
            thread_status_cache: self.thread_status_cache,
            issue_status_cache: self.issue_status_cache,
            codex_pool: Arc::new(crate::backends::codex::pool::CodexAppServerPool::default()),
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut pulls = crate::storage::subscribe_team_pull_applied();
            let snapshot = Arc::downgrade(&orchestrator.sidebar_active_issues);
            let generation = Arc::downgrade(&orchestrator.sidebar_active_issues_generation);
            handle.spawn(async move {
                while pulls.recv().await.is_ok() {
                    let (Some(snapshot), Some(generation)) =
                        (snapshot.upgrade(), generation.upgrade())
                    else {
                        break;
                    };
                    generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                    if let Ok(mut snapshot) = snapshot.write() {
                        *snapshot = None;
                    };
                }
            });
        }
        orchestrator
    }
}

/// Three heartbeat intervals: startup silence becomes evidence only after the
/// fleet has had ample opportunity to report into this runner session.
const FLEET_STARTUP_GRACE_SECS: i64 = 90;

fn fleet_is_warming(executor_count: usize, boot_at: i64, captured_at_unix_ms: u64) -> bool {
    executor_count == 0
        && captured_at_unix_ms / 1_000
            < boot_at.saturating_add(FLEET_STARTUP_GRACE_SECS).max(0) as u64
}

/// Everything one executor has to say for itself, as reasons.
///
/// Free-standing so the mapping can be tested against a machine's shape
/// directly. The rule it has to hold to is that a healthy machine produces
/// nothing: a reason is a statement that something needs attention, and one that
/// a correctly working platform emits forever is noise that makes the whole
/// vocabulary unreadable.
fn executor_health_reasons(
    executor: &cairn_common::executor_protocol::ExecutorHealthSnapshot,
) -> Vec<cairn_common::executor_protocol::SubstrateHealthReason> {
    use cairn_common::executor_protocol::{
        DiskHealthStatus, ExecutorHealthStatus, SubstrateHealthReason,
    };

    let id = executor.identity.executor_id.clone();
    let mut reasons = Vec::new();
    if executor.status == ExecutorHealthStatus::Stale {
        reasons.push(SubstrateHealthReason::StaleExecutor {
            executor_id: id.clone(),
        });
    }
    if executor.host.pressure.is_some() {
        reasons.push(SubstrateHealthReason::HostPressure {
            executor_id: id.clone(),
        });
    }
    // The link is fine but its facts have aged out. Named separately so an
    // operator is not sent looking for a dead machine.
    if executor.telemetry_stale {
        reasons.push(SubstrateHealthReason::StaleTelemetry {
            executor_id: id.clone(),
            age_ms: executor.liveness_age_ms.unwrap_or_default(),
        });
    }
    match executor.disk.status {
        DiskHealthStatus::Pressured => reasons.push(SubstrateHealthReason::DiskPressured {
            executor_id: id.clone(),
        }),
        DiskHealthStatus::Full => reasons.push(SubstrateHealthReason::DiskFull {
            executor_id: id.clone(),
        }),
        // Unknown disk pressure is exactly the volume reading being a gap, and
        // the gap loop below names it with its own reason. Tight is the
        // pressure tier at work above the admission watermark: it prices
        // placement and never degrades substrate health, so it gates nothing.
        DiskHealthStatus::Unknown | DiskHealthStatus::Ok | DiskHealthStatus::Tight => {}
    }
    // Bytes this executor has admitted it cannot reclaim. Independent of the
    // physical verdict above: a volume with room to spare is still degraded when
    // the janitor is permanently stuck, and reporting only the physical half is
    // how two unreclaimable trees read as `Ok` for sixteen days (CAIRN-4217).
    // A peer that does not report reclamation at all says nothing here, because
    // silence is not evidence in either direction.
    if executor
        .disk
        .reclamation
        .as_ref()
        .is_some_and(|reclamation| reclamation.is_outstanding())
    {
        reasons.push(SubstrateHealthReason::StorageCleanupFailed {
            executor_id: id.clone(),
        });
    }
    // Each reading this machine needs and cannot take, named. Only placement
    // inputs qualify: the daemon's own size is unavailable by construction off
    // macOS, and letting that permanent diagnostic gap become a reason would
    // hold every healthy Windows and Linux executor at unknown forever. The
    // classification lives on the protocol type so the panel and the placement
    // seam cannot drift apart about it.
    for (measurement, reason) in executor.machine.placement_gaps() {
        reasons.push(SubstrateHealthReason::MeasurementUnavailable {
            executor_id: id.clone(),
            measurement,
            reason,
        });
    }
    if executor.disk.cleanup_blocked {
        reasons.push(SubstrateHealthReason::StorageCleanupFailed {
            executor_id: id.clone(),
        });
    }
    // A partial walk is a measured result that is visibly incomplete, so it
    // carries the bounded evidence rather than only a count.
    if let Some(accounting) = executor
        .machine
        .disk_accounting
        .value()
        .filter(|accounting| accounting.is_partial())
    {
        reasons.push(SubstrateHealthReason::DiskAccountingPartial {
            executor_id: id.clone(),
            skipped_entries: accounting.skipped_count(),
            skipped: accounting.skipped.clone(),
        });
    }
    reasons
}

fn substrate_health_status(
    reasons: &[cairn_common::executor_protocol::SubstrateHealthReason],
) -> cairn_common::executor_protocol::SubstrateHealthStatus {
    use cairn_common::executor_protocol::{SubstrateHealthReason, SubstrateHealthStatus};

    if reasons.iter().any(|reason| {
        matches!(
            reason,
            SubstrateHealthReason::NoExecutors
                | SubstrateHealthReason::DiskFull { .. }
                | SubstrateHealthReason::StorageCleanupFailed { .. }
        )
    }) {
        SubstrateHealthStatus::Blocked
    } else if reasons.is_empty() {
        SubstrateHealthStatus::Healthy
    } else if reasons
        .iter()
        .all(|reason| matches!(reason, SubstrateHealthReason::MeasurementUnavailable { .. }))
    {
        SubstrateHealthStatus::Unknown
    } else {
        SubstrateHealthStatus::Degraded
    }
}

/// A one-shot cancellation handle for an in-flight turn-end (`when:review`) check
/// suite. Cloneable so the runner holds one clone while the in-flight registry
/// holds another; an issue resolution, branch advance, or user request pulling
/// [`Orchestrator::cancel_turn_end_checks`] wakes the runner's `select!`, which
/// drops the suite (killing any running check through its process's kill-on-drop
/// guard). The atomic reason is the source of truth — re-checked around the wait
/// — so the `Notify` wakeup can never be lost.
#[derive(Clone)]
pub struct TurnEndCancel {
    inner: Arc<TurnEndCancelInner>,
}

struct TurnEndCancelInner {
    reason: AtomicU8,
    notify: tokio::sync::Notify,
    status: Mutex<Option<TurnEndCheckRuntimeStatus>>,
}

/// What a running review wave publishes once its planning finishes: the plan,
/// which is everything the live `/checks` projection reads, so a poll while the
/// wave runs does no VCS or config work of its own, and the fabric request each
/// lane has been submitted as.
///
/// A base replay that keeps the branch's patch does not withdraw the wave, so
/// the node's head can move past `plan.head` while the wave still runs; the
/// status poll names that head on the lanes the wave answers.
#[derive(Clone)]
pub(crate) struct TurnEndCheckRuntimeStatus {
    pub plan: Arc<crate::execution::checks_turn_end::WavePlan>,
    pub request_ids: HashMap<String, String>,
}

impl Default for TurnEndCancel {
    fn default() -> Self {
        Self {
            inner: Arc::new(TurnEndCancelInner {
                reason: AtomicU8::new(NOT_CANCELLED),
                notify: tokio::sync::Notify::new(),
                status: Mutex::new(None),
            }),
        }
    }
}

impl TurnEndCancel {
    /// Signal cancellation. Idempotent.
    fn cancel(&self, reason: TurnEndCancelReason) {
        if self
            .inner
            .reason
            .compare_exchange(
                NOT_CANCELLED,
                reason.to_discriminant(),
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_ok()
        {
            // `notify_one` stores a permit when no waiter is parked, so a cancel
            // that races just ahead of the runner's `cancelled().await` is not lost.
            self.inner.notify.notify_one();
        }
    }

    /// Whether cancellation has been signalled.
    fn is_cancelled(&self) -> bool {
        self.inner.reason.load(Ordering::SeqCst) != NOT_CANCELLED
    }

    /// Resolve as soon as cancellation is signalled — immediately when already
    /// cancelled, otherwise when [`Self::cancel`] next fires.
    pub(crate) async fn cancelled(&self) -> TurnEndCancelReason {
        loop {
            if let Some(reason) =
                TurnEndCancelReason::from_discriminant(self.inner.reason.load(Ordering::SeqCst))
            {
                return reason;
            }
            self.inner.notify.notified().await;
        }
    }

    pub(crate) fn set_runtime_status(
        &self,
        plan: Arc<crate::execution::checks_turn_end::WavePlan>,
    ) {
        if let Ok(mut status) = self.inner.status.lock() {
            *status = Some(TurnEndCheckRuntimeStatus {
                plan,
                request_ids: HashMap::new(),
            });
        }
    }

    fn runtime_status(&self) -> Option<TurnEndCheckRuntimeStatus> {
        self.inner.status.lock().ok()?.clone()
    }

    fn set_request_id(&self, check_name: String, request_id: String) {
        if let Ok(mut status) = self.inner.status.lock() {
            if let Some(status) = status.as_mut() {
                status.request_ids.insert(check_name, request_id);
            }
        }
    }
}

/// The desktop UI attached over the runner's `/ws` channel, tracked so
/// `process_info` (and thus `cairn://dev/pid`) can report the *window* pid.
///
/// Each `/ws` connection that carries a `ui_pid` takes a monotonic generation
/// token when it registers. A connection detaches only when it still holds the
/// current generation, so a same-pid reconnect or in-process reload — where the
/// desktop process id is unchanged — is not clobbered by the old socket's late
/// disconnect cleanup running after the new socket has already re-registered.
#[derive(Debug, Default)]
struct AttachedUi {
    /// Next generation token to hand out; increments on every registration.
    next_generation: u64,
    /// The current registration as `(generation, pid)`, or `None` when no
    /// desktop is attached.
    current: Option<(u64, u32)>,
}

#[derive(Clone, Debug)]
pub enum CloudObjectGrantMintError {
    InvalidRequest,
    StaleExecution,
    MissingProjectAssociation { project_id: String },
    Unavailable(String),
}

impl std::fmt::Display for CloudObjectGrantMintError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequest => formatter.write_str("cloud object grant request is malformed"),
            Self::StaleExecution => {
                formatter.write_str("cloud object grant execution authorization is stale")
            }
            Self::MissingProjectAssociation { project_id } => write!(
                formatter,
                "project '{project_id}' is not connected to a team; cloud object grants require a team association and retrying cannot repair this project state"
            ),
            Self::Unavailable(error) => formatter.write_str(error),
        }
    }
}

/// Central runtime state for agent orchestration.
///
/// Created once at startup by each host (Tauri app, cairn-server).
/// Passed to all orchestration functions as `&Orchestrator`.
#[derive(Clone)]
pub struct Orchestrator {
    /// Database connection state.
    pub db: Arc<DbState>,
    /// Service abstractions (event emitter, process spawner, clock, filesystem)
    pub services: Arc<Services>,
    /// Active Claude CLI process tracking
    pub process_state: Arc<AgentProcessState>,
    /// MCP authentication (shared secret for TOTP passcodes)
    pub mcp_auth: Arc<McpAuthState>,
    /// Per-run bearers for the streamable-HTTP MCP endpoint. Process-local, so
    /// a restart drops them all (see `mcp::run_bearer`).
    pub run_bearers: Arc<crate::mcp::run_bearer::RunBearerBook>,
    /// Warm process GC (optional — hosts may not enable warm processes)
    pub warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    /// Active PTY sessions (terminals)
    pub pty_state: Arc<PtyState>,
    pub repl_state: Arc<crate::mcp::handlers::repl::ReplState>,
    /// Runner-owned immutable base tables and SHA-keyed corrections for
    /// store-native branch tree projections.
    pub(crate) project_overlays: Arc<crate::mcp::handlers::read::overlay::ProjectOverlayRegistry>,

    /// Per-run debounce for the deferred durable-wait park, so a turn suspending
    /// several of its calls at once is interrupted once, after the last of them
    /// has had its handoff grace. Shared across clones behind the `Arc`.
    pub(crate) park_slots: Arc<crate::orchestrator::lifecycle::ParkSlots>,
    /// The last admission decision each private ordinary-session lane made:
    /// the budget it measured and why its queue head waits, if it does.
    pub(crate) private_lanes: Arc<crate::execution::jobs::PrivateLaneLedger>,

    // === Broadcast channels for cross-component communication ===
    /// Permission response broadcast: (request_id, response_json)
    /// Hosts send on this channel when a user responds to a permission prompt.
    pub(crate) permission_responses: broadcast::Sender<(String, String)>,
    /// Run completion broadcast: run_id
    /// Emitted when a run finishes (used by sub-agent handlers to unblock).
    pub(crate) run_completions: broadcast::Sender<String>,
    /// Prompt response broadcast: (prompt_id, response_text)
    /// Hosts send on this channel when a user responds to an ask_user prompt.
    pub prompt_responses: broadcast::Sender<(String, String)>,

    /// Inline-browser bridge response broadcast: (request_id, payload_json).
    /// The app-side `browser_bridge_message` command sends on this channel when
    /// a webview's content script posts back an extract/interaction result; the
    /// synchronous read/write awaiter filters by `request_id`. Lives in the
    /// orchestrator default; hosts without a webview never publish on it.
    pub browser_bridge_responses: broadcast::Sender<(String, String)>,

    /// Inline-browser navigation lifecycle broadcast. The app's `on_navigation`
    /// and `on_page_load` handlers publish a [`BrowserNavEvent`](crate::browsers::BrowserNavEvent)
    /// on nav start / load finish; the interaction path subscribes BEFORE a
    /// click (or submit-typing) to confirm whether it navigated, and the
    /// `waitForNavigation`/`waitForLoad` actions await the next event. Hosts
    /// without a webview never publish on it.
    pub browser_nav_events: broadcast::Sender<crate::browsers::BrowserNavEvent>,

    /// Runtime-only, bounded archive of sanitized browser network captures. It
    /// survives page navigation and is cleared when the browser closes or the
    /// runner restarts; payloads are never written to the database.
    pub browser_network: Arc<crate::browser_network::BrowserNetworkArchive>,

    /// The durable fact log's lane signals and trigger index. Producers mint
    /// rows and poke it; the fact log worker consumes them.
    pub(crate) fact_log: crate::routes::FactLogHandle,

    /// Trigger event channel for event-driven recipe dispatch.
    /// Emission sites send lean `TriggerEvent` values; each host subscribes
    /// and dispatches through `process_trigger_event`.
    pub trigger_events: broadcast::Sender<TriggerEvent>,

    /// Attention-changed broadcast: payload = typed [`AttentionEvent`].
    ///
    /// Each emission corresponds to a discrete actionable fact (a question is
    /// stored, a permission requested, an artifact written, the agent's turn
    /// terminalized with work remaining, a PR state changed, an issue
    /// resolved). The event carries enough content for the `watch` long-poll
    /// handler to build a response without a follow-up `read`. Emit through
    /// [`Orchestrator::emit_attention_event`] so the dedupe cache collapses
    /// repeated-fact bursts (e.g. an artifact patched five times in a 500ms
    /// window emits once with the freshest content).
    pub attention_changed: broadcast::Sender<AttentionEvent>,

    /// Tools auto-allowed via "Allow for Session" permission responses.
    /// Checked in the Codex permission flow before prompting the user.
    pub(crate) session_allowed_tools: Arc<Mutex<HashSet<String>>>,

    /// Worktree-fence crossings auto-allowed via an `allow` + `scope: session`
    /// permission answer. Keyed by the crossing's canonical descriptor (the
    /// `descriptor` field of a [`crate::mcp::handlers::fence::Crossing`]).
    /// Session-scoped tools cannot be reused here because a fenced verb's tool
    /// name is always `read`/`write`/`run`; the descriptor (path or command)
    /// is what distinguishes one crossing from another. Checked in
    /// `raise_fence` before suspending.
    pub session_allowed_crossings: Arc<Mutex<HashSet<String>>>,

    /// Multi-account identity store. None = anonymous/legacy mode.
    /// Populated from local identity store (desktop) or JWT claims (server).
    credential_store: CredentialStoreHandle,
    /// Keeps live Claude processes on their account's current credential,
    /// reading `credential_store`. See `claude_credentials.rs`.
    claude_feeds: cairn_models::claude::feeds::ClaudeCredentialFeeds,

    /// Subscription mechanics, installed by the host. See
    /// [`cairn_models::subscription`].
    subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    /// The facet implementations every launch goes through. See
    /// [`models::ModelRuntime`].
    models_runtime: models::ModelRuntime,
    /// Which logprobs offers were seen to answer decisions, and which could
    /// not. See [`decide::DecisionObservations`].
    decision_observations: Arc<decide::DecisionObservations>,

    // === Host-specific paths (set by Tauri or cairn-server) ===
    /// Path to the cairn-cmd binary
    pub(crate) mcp_binary_path: String,
    /// Path to the jj binary (bundled sidecar; falls back to PATH `jj`).
    pub jj_binary_path: String,
    /// Directory for writing MCP config files
    pub config_dir: PathBuf,
    /// Job ids with an in-flight turn-end (`when:idle`/`when:review`) check run,
    /// each mapped to its cancellation handle. Single-flight guard so a rapid
    /// re-idle does not stack suites; the signal the PR-node / `/checks` render
    /// reads to show a "running" state; and the lever an issue resolution, branch
    /// advance, or user request pulls to quit a minutes-long suite mid-flight
    /// ([`Orchestrator::cancel_turn_end_checks`]).
    /// Runtime-only; never persisted.
    build_service_runtime: Arc<
        Mutex<HashMap<String, crate::orchestrator::build_services::BuildServiceRuntimeDiagnostic>>,
    >,
    turn_end_checks_in_flight: Arc<Mutex<HashMap<String, TurnEndCancel>>>,
    /// Request identities from the newest review wave for each job. Kept after
    /// settlement so the equally short-lived Work history can join a completed
    /// executor row back to its exact check lane without guessing by command.
    recent_turn_end_check_requests: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
    /// The main cadence's per-project slot: one pass in flight, at most one
    /// queued successor.
    pub(crate) main_cadence: Arc<Mutex<crate::execution::checks_main::CadenceSlots>>,
    /// Per-artifact quiet-period debouncer for delivering a revised artifact to
    /// the jobs launched with an earlier version. Runtime-only.
    pub(crate) artifact_revisions: Arc<crate::execution::artifact_revisions::RevisionDeliveries>,
    /// Job ids with a synchronous when:write batch in flight. Runtime-only.
    write_checks_in_flight: Arc<Mutex<HashMap<String, usize>>>,
    /// Exact settled node-check projection, generation-fenced and single-flight.
    pub(crate) node_check_status_cache: Arc<crate::execution::checks_status::NodeCheckStatusCache>,
    /// The one PR projection, keyed by the job that owns the merge request.
    ///
    /// The PR-owner job, not the action run that happens to be asking: several
    /// action runs render the same pull request, and keying by the asker would
    /// give each its own copy of one remote fact and its own `jj` fan-out.
    pub(crate) pr_refresh_cache: Arc<
        crate::orchestrator::generation_cache::GenerationCache<
            Arc<crate::pr_data::actions::refresh::PrSnapshot>,
        >,
    >,
    /// Per-project thread activity rollup, keyed by project id. Shared with the
    /// emitter decorator that advances its generation.
    pub(crate) thread_status_cache: Arc<generation_cache::ThreadStatusCache>,
    /// Per-project issue activity/PR rollup, keyed by project id. Shares the
    /// emitter decorator that advances the thread rollup's generation.
    pub(crate) issue_status_cache: Arc<generation_cache::IssueStatusCache>,
    /// Runner-owned persistent commit-addressed execution workspaces.
    pub fleet: Arc<crate::fleet::Fleet>,
    /// Runtime-only object-channel credentials and staged managed-executor uploads.
    pub object_plane: Arc<crate::orchestrator::object_plane::ObjectPlaneState>,
    /// Pool of long-lived Codex app-servers for EPHEMERAL CALLS (CAIRN-2549).
    /// Each pooled call is a `thread/start` on a shared process rather than a
    /// process of its own. Deliberately owned HERE and NOT in
    /// `process_state.processes`, so warm-process GC never treats it as an
    /// evictable per-session process. Empty and inert until the first Codex
    /// call.
    pub(crate) codex_pool: Arc<crate::backends::codex::pool::CodexAppServerPool>,
    /// Directory containing bundled preset schemas (None if not available)
    pub(crate) schema_dir: Option<PathBuf>,
    /// Port for the MCP callback server
    pub mcp_callback_port: u16,
    /// Unix seconds at which this host process began owning runs.
    ///
    /// The orchestrator is constructed once per host process, so this is that
    /// process's boot time. It is the fence line in `dispatch::dispatch_tool`
    /// (CAIRN-3287): a `runs` row timestamped before it belongs to a predecessor
    /// runner, and an agent still calling tools for such a run is a zombie whose
    /// transcript reader died with the process that spawned it. Runtime-only;
    /// never persisted.
    pub boot_at: i64,
    /// The Tokio runtime this orchestrator was built on, if any.
    ///
    /// Work is often triggered from plain OS threads (process reader and exit
    /// watcher callbacks, attention delivery) that carry no runtime context, so
    /// background work the orchestrator schedules must not depend on the
    /// caller's thread. See [`Orchestrator::spawn_blocking_detached`].
    runtime: Option<tokio::runtime::Handle>,
    /// Desktop UI attached over the runner's `/ws` channel. The window-owning
    /// Tauri process registers its own `std::process::id()` on connect and
    /// detaches on disconnect; the `process_info` tool reads the current pid so
    /// `cairn://dev/pid` reports the *window* pid rather than this windowless
    /// runner daemon's. Generation-tracked (see [`AttachedUi`]) so a same-pid
    /// reconnect/reload is not clobbered by the old socket's late close.
    /// Runtime-only; never persisted.
    attached_ui: Arc<Mutex<AttachedUi>>,
    /// Vibe state for embedding-based color assignment (None if centroids unavailable)
    vibe_state: Option<Arc<VibeState>>,
    /// Sender into the async event-embed worker. None until the worker is started.
    embed_tx: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<EmbedJob>>>>,
    /// Bounded ingress for fallback-era summary upgrades. None only before startup wiring.
    era_summary_tx: crate::threads::era_worker::EraSummarySender,
    /// Wakes the tenet evidence worker after a conclusion commits. Pending
    /// event rows are the source of truth; this only shortens the wait.
    tenet_evidence_hint: Arc<tokio::sync::Notify>,
    /// The embedding health gate every consumer shares, so one outage pauses
    /// all of them and is logged once. See `embeddings::embedder`.
    embed_gate: Arc<EmbedGate>,
    /// Search's semantic lane: the resident copy of the span vectors it scans.
    semantic_search: std::sync::OnceLock<Arc<SemanticSearch>>,

    // === Team connection state (multi-team) ===
    /// Multi-team manager: handles DB-backed team configs, JWT refresh, credential resolution

    // === Account connection (replaces teams for individual users) ===
    /// Account manager: device code auth, single-account JWT lifecycle
    pub account_manager: Arc<AccountManager>,

    /// Anonymous device manager: user-less JWT for the `/embed` gateway so
    /// embedding works logged-out. Account JWT takes precedence when connected.
    pub anon_device_manager: Arc<AnonDeviceManager>,

    // === Unified notification ===
    /// Emits frontend `db-change` events for write operations. Shares `emitter`.
    pub(crate) notifier: Notifier,

    // === Cloud API ===
    /// Cloud API endpoint configuration (account, sync, bug reports)
    api_config: ApiConfig,
    team_attention_sender: Arc<dyn team_attention_push::TeamAttentionSender>,
    team_attention_push_dedupe: Arc<TokioMutex<HashMap<String, String>>>,

    /// Typed effect queue for async draining.
    ///
    /// Sync callers (e.g. `finalize_run`) push `WorkflowEffect`s here.
    /// An async drainer task (spawned by each host) receives effects and
    /// calls `execute_effects`. This replaces the ad-hoc `"dag-advance"`
    /// event → listener → async handler chain.
    ///
    /// `None` when the host hasn't set up an effect drainer yet (backward compat).
    pub effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,

    /// Sender into the app-side browser drain task. Core dispatch and execution
    /// teardown push [`BrowserCommand`](crate::browsers::BrowserCommand)s here;
    /// the app applies them to its `BrowserRegistry` (which holds the live
    /// `Webview` handles — a Tauri type cairn-core cannot name). `None` on hosts
    /// without a webview layer (headless/server).
    pub browser_command_tx: Option<crate::browsers::BrowserCommandTx>,

    /// In-memory per-browser page-loading flag, keyed by browser id. The
    /// app-side nav handlers set it `true` on navigation start / page-load start
    /// and `false` on page-load finish. The browser read path consults it to
    /// distinguish "the page is still loading / not yet interactive" from "the
    /// page loaded but the content-script bridge failed", so a slow first compile
    /// reads as still-loading rather than a misleading hard timeout. Volatile by
    /// nature: in-memory, never persisted, reset cleanly on restart.
    pub(crate) browser_loading: Arc<Mutex<HashMap<String, bool>>>,

    /// Host-specific effect executor for `StartAgentJobs` and `ExecuteAction`.
    ///
    /// Wrapped in `Arc<OnceLock<...>>` so it's Clone-compatible (Orchestrator
    /// derives Clone) and settable after construction via `&self`. The executor
    /// may need a reference to the host's wrapper (e.g. `Arc<ServerState>`)
    /// which isn't available until after the Orchestrator is built.
    pub(crate) executor: Arc<OnceLock<Arc<dyn EffectExecutor>>>,

    /// Host-specific gateway to external MCP servers (the `cairn://mcp/...`
    /// family). `None` until a host sets it; `read`/`run` MCP dispatch returns a
    /// clear error when unset. Wrapped like `executor` so it's Clone-compatible
    /// and settable after construction via `&self`.
    mcp_gateway: Arc<OnceLock<Arc<dyn McpGateway>>>,

    /// One desktop-automation facade per machine, each owning that machine's
    /// service residency. Held here because the residency outlives any single
    /// probe or verb call, and a second facade for a machine is a second
    /// claimant on a holder that has room for exactly one.
    pub(crate) desktop_facades: Arc<crate::fleet::desktop::DesktopFacadeRegistry>,

    /// Cached provider model catalog loaded at startup and refreshed on demand.
    model_catalog: Arc<RwLock<HashMap<String, ProviderModelCatalog>>>,
    /// Whether [`Self::refresh_model_catalog`] rediscovers anything. Always on
    /// outside test builds. Test builds default it off, because a full refresh
    /// is the one discovery nobody asked for by name: it fires from
    /// `add_account` and the other credential paths on a detached thread,
    /// reaches every enabled provider over the network, and on failure stamps
    /// an error onto each entry, which hides whatever catalog the test seeded.
    /// A scoped single-provider refresh is an explicit request and is not
    /// gated.
    catalog_rediscovery: bool,
    /// Latest usage snapshots keyed by backend and requested account.
    pub provider_usage_snapshots: ProviderUsageSnapshots,
    /// Account probes are single-flight so concurrent session starts cannot
    /// duplicate provider traffic or race health writes for one account.
    pub(crate) provider_usage_captures: ProviderUsageCaptures,
    /// What the credential store held when `subscription_periods` was last
    /// reconciled.
    pub(crate) reconciled_subscriptions: subscription_periods::ReconciledSubscriptions,
    /// Published Nav-equivalent active issue projection. Mutations invalidate it;
    /// the next tray/sidebar read rebuilds it with bounded per-database queries.
    pub sidebar_active_issues:
        Arc<std::sync::RwLock<Option<Vec<crate::models::SidebarActiveIssue>>>>,
    sidebar_active_issues_generation: Arc<std::sync::atomic::AtomicU64>,
    /// Single-flight gate for cold snapshot rebuilds. Readers recheck the cache
    /// after acquiring it so one query pass serves an entire concurrent burst.
    pub sidebar_active_issues_rebuild: Arc<TokioMutex<()>>,
    sidebar_active_issues_rebuild_count: Arc<std::sync::atomic::AtomicU64>,
    /// Latest normalized context-token snapshots keyed by durable session id.
    context_token_snapshots: Arc<RwLock<HashMap<String, ContextTokenState>>>,

    /// Per-execution locks to serialize read-modify-write operations on snapshots.
    /// Prevents concurrent `persist_task_packet` calls from losing packets.
    execution_locks: Arc<Mutex<HashMap<String, Arc<TokioMutex<()>>>>>,

    /// Per-job locks serializing the decide-to-resume-and-launch critical
    /// section. Deciding a job is idle, inserting its run, allocating its turn,
    /// spawning the backend process and registering that process are five
    /// separate steps; two wake facts routed concurrently for one job each
    /// passed the idle check before either had created a turn, and each launched
    /// a process against the same session — two independent agent contexts on
    /// one session, racing every side effect of the turn (CAIRN-3283).
    ///
    /// `std::sync::Mutex`, not `TokioMutex`: the launch funnels
    /// (`continue_job_impl_with_intent`, `prepare_job`) are synchronous
    /// functions that already block on `run_db`. Not reentrant — acquire it at
    /// exactly one level.
    job_launch_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,

    /// Jobs whose launch has been admitted but whose process has not registered
    /// yet, mapped to the run that launch minted.
    ///
    /// The launch lock alone cannot cover a cold start. `prepare_job` is
    /// synchronous and returns before the transport spawns and registers the
    /// process, so its guard is gone during exactly the interval when the job
    /// has a run and a turn but no serving process — the shape a resume reads as
    /// idle. Holding the lock across that gap is not possible: the spawn happens
    /// after an await, and a `std::sync::MutexGuard` cannot cross one.
    ///
    /// So admission is carried instead of locked. `prepare_job` claims its job
    /// here under the launch lock and hands the claim to the transport, which
    /// holds it until the process is registered or the start has failed; a
    /// resume that acquires the launch lock meanwhile sees the claim and
    /// attaches to its run rather than launching a second process against the
    /// same session (CAIRN-3283).
    ///
    /// Deliberately in memory and RAII-released: a claim cannot outlive the
    /// process holding it, and unwinding drops it, so no crash or panic can
    /// leave a job permanently unable to resume the way a durable marker could.
    job_launch_claims: Arc<Mutex<HashMap<String, String>>>,

    /// Per-store locks serializing base-advance reconcile and merge-fold
    /// mutations on a shared jj project store, keyed by the store directory.
    /// Concurrent jj rebase/import ops on one store from forked operation logs
    /// mint divergent conflicted copies of the same change-id; this single-writer
    /// discipline closes that window. Keyed by store path (not execution/job) so
    /// reconciles from different executions on the same project store serialize.
    jj_store_locks: Arc<Mutex<HashMap<String, Arc<store_lock::PriorityLock>>>>,
    jj_store_lock_metrics: Arc<Mutex<HashMap<String, JjStoreLockMetrics>>>,

    /// What this process knows about origin's copy of each project's base
    /// branches (the default branch and every tracked integration branch), the
    /// single-flight sync toward each, and the per-repository fetch locks.
    /// See `orchestrator::base_branch_sync`.
    pub(crate) base_branch_sync: Arc<crate::orchestrator::base_branch_sync::BaseBranchSyncRegistry>,

    /// Launcher handles for supervised Managed Build Service daemons, keyed by
    /// service name. The default sccache daemon runs in the FOREGROUND as Cairn's
    /// child (`SCCACHE_NO_DAEMON`), so its handle controls the server directly:
    /// held so the supervisor can kill-then-relaunch a wedged daemon and so the
    /// server is stopped on shutdown. See `orchestrator::build_services`.
    build_service_children: Arc<Mutex<HashMap<String, Box<dyn ChildProcess>>>>,

    /// Serializes build-service reconciliation within this process. A settings
    /// restart, the periodic supervisor tick, and a startup launch can all
    /// arrive at once; without this they race on one port and produce competing
    /// daemons instead of a recovery. See `orchestrator::build_services`.
    build_service_reconcile: Arc<Mutex<()>>,

    /// What the supervisor last observed about this machine's compile-cache
    /// service: which service it is, whether settings enable it, and whether
    /// its launch program resolves.
    ///
    /// Held here because establishing it is not free — it parses the settings
    /// file and resolves the launch program against PATH, which shells out —
    /// while the substrate-health snapshot that reports it is read on every
    /// fleet change. Reconciliation already visits all three facts on every
    /// pass, so it publishes them here and readers only ever consume them.
    /// `None` until this process has reconciled once.
    /// See `orchestrator::build_services`.
    compile_cache_service:
        Arc<RwLock<Option<crate::orchestrator::build_services::ObservedCompileCacheService>>>,

    /// Per-run dedupe for legacy `agent-attention` terminal toasts.
    ///
    /// The completion toast now fires at the turn idle boundary while the same
    /// run may later EOF/finalize. Keying by run id suppresses cleanup-path
    /// duplicates and repeated crash finalization attempts.
    agent_completion_attention_dedupe: Arc<Mutex<HashSet<String>>>,

    /// Projects whose code map is being recomputed right now. A code map is a
    /// whole-tree walk, so a second request while one is in flight is dropped
    /// rather than queued: the running pass re-reads the base after each round
    /// and already covers whatever advanced.
    pub(crate) codemap_refresh_in_flight: Arc<Mutex<HashSet<String>>>,

    /// Session identities whose crashed native resume was already handed to the
    /// digest-reseed fallback.
    ///
    /// One attempt per session: the fallback rotates to a fresh session, which
    /// passes no `--resume` flag and therefore can never report itself
    /// unresolvable. A second attempt on the same id is either a duplicate
    /// finalize or a failure that retrying will not fix.
    pub(crate) session_reseed_fallback_attempted: Arc<Mutex<HashSet<String>>>,
}

/// Resolve the `/embed` gateway token: account JWT preferred, anonymous device
/// JWT as fallback. An *expired* account JWT is treated as absent (see
/// `AccountManager::get_jwt`), so it falls through to the anonymous token rather
/// than shadowing it — keeping vibe coloring and the recommender alive when the
/// account token lapses. Single source of truth for the gateway's token
/// precedence so it can be unit-tested without a full Orchestrator.
fn resolve_embed_token(account: &AccountManager, anon: &AnonDeviceManager) -> Option<String> {
    account
        .get_jwt()
        .ok()
        .flatten()
        .or_else(|| anon.get_anon_jwt().ok().flatten())
}

/// Ownership of a job's launch across the gap between admission and process
/// registration. Dropping it re-opens the job to resumes, so it is held by
/// whoever is responsible for the spawn. See `Orchestrator::job_launch_claims`.
pub struct JobLaunchClaim {
    claims: Arc<Mutex<HashMap<String, String>>>,
    job_id: String,
}

impl Drop for JobLaunchClaim {
    fn drop(&mut self) {
        self.claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.job_id);
    }
}

impl Orchestrator {
    /// Reclaim and promote durable ordinary session requests after host restart.
    pub async fn recover_ordinary_admissions_on_startup(&self) {
        crate::execution::jobs::recover_ordinary_admissions_on_startup(self).await;
    }

    /// Each private ordinary-session lane's occupancy and the budget its
    /// promoter last decided against. Cached decisions, never a fresh probe.
    pub async fn private_lane_budgets(
        &self,
    ) -> crate::storage::DbResult<Vec<crate::storage::LaneBudget>> {
        crate::execution::jobs::private_lane_budgets(self).await
    }

    pub fn runtime_admission_api_config(&self) -> ApiConfig {
        self.api_config.clone()
    }

    /// Run blocking `task` in the background regardless of the caller's thread.
    ///
    /// The task lands on the blocking pool of the runtime this orchestrator was
    /// built on, so it has runtime context for anything it spawns in turn (a
    /// launched session's admission heartbeat, for one). An orchestrator built
    /// off any runtime has no pool to offer, and the task gets its own thread.
    pub(crate) fn spawn_blocking_detached(&self, task: impl FnOnce() + Send + 'static) {
        match &self.runtime {
            Some(runtime) => {
                runtime.spawn_blocking(task);
            }
            None => {
                std::thread::spawn(task);
            }
        }
    }
    /// Run async `task` in the background on the runtime this orchestrator was
    /// built on, regardless of the caller's thread, so whatever it spawns in turn
    /// outlives the caller. Off any runtime it gets a thread and a runtime of
    /// its own.
    ///
    /// This is how work that starts on a thread with no runtime of its own (an
    /// agent backend's stdout loop firing a turn-end hook) gets one: never a
    /// runtime built for the occasion, which would drop every task the work
    /// spawned for others the moment the work itself returned.
    pub(crate) fn spawn_detached(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
    ) {
        self.spawn_detached_or(task, || {});
    }

    /// [`Self::spawn_detached`], calling `on_spawn_failure` in place of `task`
    /// when the task cannot be started at all, so a caller holding something the
    /// task would have released (a single-flight slot) can release it.
    pub(crate) fn spawn_detached_or(
        &self,
        task: impl std::future::Future<Output = ()> + Send + 'static,
        on_spawn_failure: impl FnOnce() + Send + 'static,
    ) {
        spawn_onto(self.runtime.as_ref(), task, on_spawn_failure);
    }

    /// Queue a persisted fallback era for its one-way generated-summary upgrade.
    /// Returns false when startup has not wired the worker or its bounded queue
    /// is full; persistence remains authoritative and the startup sweep retries it.
    pub fn enqueue_thread_era_summary(&self, job_id: impl Into<String>, seq: i32) -> bool {
        crate::threads::era_worker::enqueue(&self.era_summary_tx, job_id, seq)
    }

    /// Tell the tenet evidence worker a conclusion's event just committed.
    pub fn notify_tenet_evidence(&self) {
        self.tenet_evidence_hint.notify_one();
    }

    pub(crate) fn tenet_evidence_hint(&self) -> Arc<tokio::sync::Notify> {
        self.tenet_evidence_hint.clone()
    }

    pub fn invalidate_sidebar_active_issues(&self) {
        self.sidebar_active_issues_generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Ok(mut snapshot) = self.sidebar_active_issues.write() {
            *snapshot = None;
        }
    }

    pub fn sidebar_active_issues_generation(&self) -> u64 {
        self.sidebar_active_issues_generation
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn begin_sidebar_active_issues_rebuild(&self) {
        self.sidebar_active_issues_rebuild_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn sidebar_active_issues_rebuild_count(&self) -> u64 {
        self.sidebar_active_issues_rebuild_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn publish_sidebar_active_issues(
        &self,
        generation: u64,
        issues: Vec<crate::models::SidebarActiveIssue>,
    ) -> Result<(), String> {
        let mut snapshot = self
            .sidebar_active_issues
            .write()
            .map_err(|error| error.to_string())?;
        if self.sidebar_active_issues_generation() == generation {
            *snapshot = Some(issues);
        }
        Ok(())
    }

    /// Whether the fleet's silence is still explained by this runner's own
    /// startup rather than by an absent fleet, given a count of the executors
    /// the caller has already projected.
    ///
    /// This is the only thing `cairn://executors` needs from the substrate
    /// health snapshot, and it takes the count rather than going and finding
    /// one because that resource has already projected the fleet: building the
    /// whole health snapshot to read one boolean off it would repeat a fleet
    /// clone and a per-executor projection, and add the compile-cache and
    /// store-lock projections besides.
    pub fn fleet_warming_up(&self, executor_count: usize, captured_at_unix_ms: u64) -> bool {
        fleet_is_warming(executor_count, self.boot_at, captured_at_unix_ms)
    }

    pub fn substrate_health_snapshot(
        &self,
    ) -> cairn_common::executor_protocol::SubstrateHealthSnapshot {
        use cairn_common::executor_protocol::{
            CellInventoryHealth, CellLifecycleCensus, ExecutorHealthStatus,
            InventoryAuthorityState, PersistentCellLifecycle, StoreLockHealth,
            SubstrateHealthReason, SubstrateHealthSnapshot, SUBSTRATE_HEALTH_SCHEMA_VERSION,
        };
        let captured_at_unix_ms = self.services.clock.now_u64().saturating_mul(1_000);
        let fleet = self.fleet.snapshot();
        let executors = self.fleet.executor_health(captured_at_unix_ms);
        let mut occupancy = CellLifecycleCensus {
            total: fleet.cells.len(),
            ..CellLifecycleCensus::default()
        };
        for cell in &fleet.cells {
            match cell.lifecycle {
                PersistentCellLifecycle::Materializing => occupancy.provisioning += 1,
                PersistentCellLifecycle::Idle => occupancy.idle += 1,
                PersistentCellLifecycle::Active => occupancy.running += 1,
                PersistentCellLifecycle::Deleting => occupancy.retired += 1,
                PersistentCellLifecycle::Quarantined => occupancy.quarantined += 1,
            }
        }
        let mut inventory = CellInventoryHealth {
            authority: if executors.is_empty()
                || executors.iter().any(|executor| {
                    executor.status != ExecutorHealthStatus::Online
                        || executor.inventory.authority != InventoryAuthorityState::Authoritative
                }) {
                InventoryAuthorityState::Unknown
            } else {
                InventoryAuthorityState::Authoritative
            },
            ..CellInventoryHealth::default()
        };
        for executor in &executors {
            inventory.checked_out_count += executor.inventory.checked_out_count;
            inventory.idle_count += executor.inventory.idle_count;
            inventory.idle_retention_budget_total = inventory
                .idle_retention_budget_total
                .saturating_add(executor.inventory.idle_retention_budget_total);
            inventory.idle_retention_pressured |= executor.inventory.idle_retention_pressured;
            inventory.excess_idle_count = inventory
                .excess_idle_count
                .saturating_add(executor.inventory.excess_idle_count);
            inventory.transient_occupancy += executor.inventory.transient_occupancy;
            inventory.resident_occupancy += executor.inventory.resident_occupancy;
            inventory.active_transient_reservation.memory_bytes = inventory
                .active_transient_reservation
                .memory_bytes
                .saturating_add(executor.inventory.active_transient_reservation.memory_bytes);
            inventory.active_transient_reservation.disk_growth_bytes = inventory
                .active_transient_reservation
                .disk_growth_bytes
                .saturating_add(
                    executor
                        .inventory
                        .active_transient_reservation
                        .disk_growth_bytes,
                );
            inventory.active_transient_reservation.concurrency_units = inventory
                .active_transient_reservation
                .concurrency_units
                .saturating_add(
                    executor
                        .inventory
                        .active_transient_reservation
                        .concurrency_units,
                );
            inventory.active_resident_reservation.memory_bytes = inventory
                .active_resident_reservation
                .memory_bytes
                .saturating_add(executor.inventory.active_resident_reservation.memory_bytes);
            inventory.active_resident_reservation.disk_growth_bytes = inventory
                .active_resident_reservation
                .disk_growth_bytes
                .saturating_add(
                    executor
                        .inventory
                        .active_resident_reservation
                        .disk_growth_bytes,
                );
            inventory.active_resident_reservation.concurrency_units = inventory
                .active_resident_reservation
                .concurrency_units
                .saturating_add(
                    executor
                        .inventory
                        .active_resident_reservation
                        .concurrency_units,
                );
            inventory.retirement_in_progress |= executor.inventory.retirement_in_progress;
            if executor.inventory.sweep_status
                == cairn_common::executor_protocol::StorageSweepStatus::InFlight
            {
                inventory.sweep_status =
                    cairn_common::executor_protocol::StorageSweepStatus::InFlight;
            } else if inventory.sweep_status
                == cairn_common::executor_protocol::StorageSweepStatus::NotStarted
            {
                inventory.sweep_status = executor.inventory.sweep_status;
            }
            if executor.inventory.last_reclaimed_at_unix_ms > inventory.last_reclaimed_at_unix_ms {
                inventory.last_reclaimed_cell_id =
                    executor.inventory.last_reclaimed_cell_id.clone();
                inventory.last_reclaimed_at_unix_ms = executor.inventory.last_reclaimed_at_unix_ms;
                inventory.last_reclaimed_bytes = executor.inventory.last_reclaimed_bytes;
            }
        }
        let store_locks = self
            .jj_store_lock_metrics
            .lock()
            .unwrap()
            .iter()
            .map(|(store, metrics)| StoreLockHealth {
                store: store.clone(),
                waiter_count: metrics.waiter_count(),
                waits: summarize_bounded(&metrics.waits_ms),
                holds: summarize_bounded(&metrics.holds_ms),
            })
            .collect::<Vec<_>>();
        let warming_up = self.fleet_warming_up(executors.len(), captured_at_unix_ms);
        let mut reasons = Vec::new();
        if executors.is_empty() && !warming_up {
            reasons.push(SubstrateHealthReason::NoExecutors);
        }
        for executor in &executors {
            reasons.extend(executor_health_reasons(executor));
        }
        let status = substrate_health_status(&reasons);
        SubstrateHealthSnapshot {
            schema_version: SUBSTRATE_HEALTH_SCHEMA_VERSION,
            captured_at_unix_ms,
            status,
            reasons,
            warming_up,
            executors,
            // Read from the supervisor's cached state. Snapshot construction
            // stays non-blocking: it must never probe a daemon or run
            // `--show-stats`, or the panel's own responsiveness would depend on
            // the health of the thing it is reporting on.
            compile_cache: self.compile_cache_health(),
            occupancy,
            inventory,
            fleet,
            store_locks,
            // Read separately from `executors` and left out of every verdict
            // computed above. A machine that is merely unreachable says nothing
            // about whether the machines that ARE attached hold an authoritative
            // inventory, and folding it in would have marked the fleet degraded
            // every time this runner's laptop left the network.
            enrolled_remotes: self.fleet.unattached_enrolled_remotes(),
        }
    }

    pub fn cancel_cell_request(&self, request_id: &str) -> crate::fleet::CancelOutcome {
        let cancelled = self.fleet.cancel_request(request_id);
        if cancelled != crate::fleet::CancelOutcome::Unknown {
            let _ = self
                .services
                .emitter
                .emit("substrate-health-change", serde_json::json!({}));
        }
        cancelled
    }

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

    /// Register the OS process id of the desktop UI now attached over the
    /// runner's `/ws` channel and return a generation token identifying THIS
    /// registration. The window-owning Tauri process sends its own
    /// `std::process::id()` when it connects; `process_info` returns it so
    /// `cairn://dev/pid` hands an external tool (e.g. Axon accessibility) the
    /// *window* pid rather than this windowless runner daemon's. Pass the
    /// returned token to [`detach_ui_pid`](Self::detach_ui_pid) on disconnect.
    /// Called on WS connect.
    pub fn attach_ui_pid(&self, pid: u32) -> u64 {
        let mut guard = self.attached_ui.lock().unwrap_or_else(|e| e.into_inner());
        let generation = guard.next_generation;
        guard.next_generation = guard.next_generation.wrapping_add(1);
        guard.current = Some((generation, pid));
        generation
    }

    /// Detach the desktop UI registration identified by `generation` on WS
    /// disconnect, but only if it is still the current one. A newer connection
    /// (even the same desktop pid, e.g. a reconnect or in-process reload) that
    /// already re-registered bumped the generation, so this stale close is a
    /// no-op and cannot clobber the live registration.
    pub fn detach_ui_pid(&self, generation: u64) {
        let mut guard = self.attached_ui.lock().unwrap_or_else(|e| e.into_inner());
        if guard
            .current
            .is_some_and(|(current, _)| current == generation)
        {
            guard.current = None;
        }
    }

    /// The OS process id of the desktop UI attached over `/ws`, or `None` when no
    /// desktop is connected (a headless server, or the window fully quit).
    pub fn attached_ui_pid(&self) -> Option<u32> {
        self.attached_ui
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .current
            .map(|(_, pid)| pid)
    }

    /// Record whether a browser's page is currently loading. Called from the
    /// app-side navigation handlers on the canonical nav-start / load-finish
    /// transitions; the browser read path reads it back via
    /// [`is_browser_loading`](Self::is_browser_loading).
    pub fn set_browser_loading(&self, browser_id: &str, loading: bool) {
        if let Ok(mut map) = self.browser_loading.lock() {
            map.insert(browser_id.to_string(), loading);
        }
    }

    /// Whether a browser's page is currently loading (defaults to `false` for an
    /// id never seen — a webview that has never navigated is treated as idle).
    pub(crate) fn is_browser_loading(&self, browser_id: &str) -> bool {
        self.browser_loading
            .lock()
            .ok()
            .and_then(|map| map.get(browser_id).copied())
            .unwrap_or(false)
    }

    /// Set the external MCP gateway after construction (mirrors `set_executor`).
    ///
    /// The gateway is wrapped in
    /// [`crate::mcp::untrusted::UntrustedGateway`] on the way in, so everything
    /// an external MCP server returns is sanitized before any Cairn code sees
    /// it. Wrapping here rather than at each host's wiring is what makes that a
    /// property of the system instead of a convention: this is the only way a
    /// gateway is installed, so an unwrapped one is unreachable.
    pub fn set_mcp_gateway(&self, gateway: Arc<dyn McpGateway>) -> Result<(), Arc<dyn McpGateway>> {
        self.mcp_gateway
            .set(crate::mcp::untrusted::UntrustedGateway::wrap(gateway))
    }

    /// The configured MCP gateway, if a host has set one.
    pub fn mcp_gateway(&self) -> Option<&Arc<dyn McpGateway>> {
        self.mcp_gateway.get()
    }

    /// Get or create a per-execution lock for serializing snapshot mutations.
    pub fn execution_lock(&self, execution_id: &str) -> Arc<TokioMutex<()>> {
        let mut map = self.execution_locks.lock().unwrap();
        map.entry(execution_id.to_string())
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
    }

    /// Get or create a per-job lock serializing that job's launch critical
    /// section (idle decision -> run insert -> turn allocation -> spawn ->
    /// registry registration). See `job_launch_locks`.
    pub fn job_launch_lock(&self, job_id: &str) -> Arc<Mutex<()>> {
        let mut map = self.job_launch_locks.lock().unwrap();
        map.entry(job_id.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Claim this job's launch on behalf of `run_id`, for a launch whose process
    /// registers after the launch lock has been released. Call only while
    /// holding the job's launch lock. Releasing the returned claim re-opens the
    /// job to resumes, so hold it until the process is registered or the start
    /// has failed. See `job_launch_claims`.
    pub fn claim_job_launch(&self, job_id: &str, run_id: &str) -> JobLaunchClaim {
        self.job_launch_claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(job_id.to_string(), run_id.to_string());
        JobLaunchClaim {
            claims: self.job_launch_claims.clone(),
            job_id: job_id.to_string(),
        }
    }

    /// The run of an admitted-but-unregistered launch for this job, if one is
    /// outstanding. See `job_launch_claims`.
    pub fn claimed_launch_run(&self, job_id: &str) -> Option<String> {
        self.job_launch_claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(job_id)
            .cloned()
    }

    /// Jobs currently admitted for launch but not yet registered as processes.
    /// Startup reconciliation snapshots these identities so it cannot cancel the
    /// pending turn of a launch this host accepted while recovery was running.
    pub fn claimed_launch_job_ids(&self) -> std::collections::HashSet<String> {
        self.job_launch_claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// Emit a typed attention event to any in-flight `watch` long-poll.
    ///
    /// Consults the short-window dedupe cache: if an emit for the same
    /// `(issue_id, fact-key)` landed within the dedupe window, the new one is
    /// dropped (the prior emit's content is already in-flight to subscribers).
    /// Fire-and-forget: a send error (no subscribers) is ignored.
    ///
    /// Also opportunistically prunes the dedupe cache on each emit: when the
    /// cache exceeds `DEDUPE_CACHE_SWEEP_THRESHOLD` entries, drop everything
    /// older than the dedupe window (those entries can no longer suppress
    /// anything). Bounded growth, no background task required.
    pub fn emit_attention_event(&self, mut event: AttentionEvent) {
        if event.route_provenance.is_none() {
            event.route_provenance = crate::routes::current_provenance();
        }
        let key = event.fact.key();
        log::debug!(
            "attention_emit: issue={} kind={} status={} attention={}",
            event.issue_id,
            key.kind,
            event.status,
            event.attention
        );
        // CAIRN-1647: issue-attention facts drive the durable attention ledger
        // (open/bump/resolve + watcher evaluation), which replaces the frozen
        // `[Child update] … Read X.` system-direct and its 1s dedupe cache —
        // keyed-item idempotency makes the cache unnecessary. The legacy router
        // is retained only for `ExternalMessageReply`, which targets external
        // `cairn watch` drivers rather than a parent wake.
        match &event.fact {
            AttentionFact::ExternalMessageReply { .. } => {
                let _ = crate::orchestrator::wakes::route_child_attention(
                    self,
                    &event.issue_id,
                    &event.issue_uri,
                    &event.attention.to_string(),
                    key.kind,
                    key.detail_uri.as_deref(),
                    event.fact.urgency(),
                );
            }
            _ => {
                crate::orchestrator::attention_delivery::create_resolved_push(self, &event);
            }
        }
        team_attention_push::maybe_notify(self, &event);
        let _ = self.attention_changed.send(event);
    }

    /// Wake any in-flight `cairn watch` for an issue whose actionable state
    /// may have just changed. Reads the live projection and emits a typed
    /// `Resolved` (terminal status) or `AgentIdleWithWork` (attention != None,
    /// or attention None with an open PR work product) event. No-ops if none of
    /// those hold: the agent isn't idle in any way the watcher needs to learn
    /// about.
    ///
    /// Use this at *boundary* sites that don't correspond to a single typed
    /// fact (manual merge resolution, execution start, permission answered,
    /// manual issue close, prompt answered, PR just opened). Discrete
    /// actionable facts — question stored, permission requested, artifact
    /// written, webhook PR state — should construct the event inline so the
    /// watch handler can render the specific content. Fact construction is
    /// shared with the turn-end emit via [`attention::idle_fact_for_issue`].
    pub async fn wake_for_issue(&self, issue_id: &str) {
        let db = match crate::issues::crud::owning_db_for_issue(&self.db, issue_id).await {
            Ok(db) => db,
            Err(e) => {
                log::debug!("wake_for_issue skip ({}): {}", issue_id, e);
                return;
            }
        };
        let ctx = match attention::read_issue_for_attention(&db, issue_id).await {
            Ok(ctx) => ctx,
            Err(e) => {
                log::debug!("wake_for_issue skip ({}): {}", issue_id, e);
                return;
            }
        };
        let issue_uri = ctx.issue_uri();
        let Some(idle) = attention::idle_fact_for_issue(&db, issue_id, &ctx, None).await else {
            // No actionable state, not terminal, no open PR — nothing for `watch`.
            return;
        };
        self.emit_attention_event(AttentionEvent {
            issue_id: issue_id.to_string(),
            issue_uri,
            fact: idle.fact,
            attention: ctx.attention,
            status: ctx.status,
            updated_at: idle.updated_at,
            route_provenance: None,
        });
    }

    /// Start the async embed worker. Call once at startup, on a tokio runtime.
    /// Handles both event vibe coloring and corpus resource embedding through a
    /// single channel. Vibe coloring is skipped when axes are unavailable;
    /// resource embedding always runs (the gateway returns `Ok(None)` with no
    /// account, so starting unconditionally is safe). Jobs enqueued before this
    /// is called are dropped.
    /// Fence an executor's exact-object request, resolve its team from the
    /// synced project owner, and mint through the runner's device identity.
    pub async fn mint_authorized_cloud_object_grant(
        &self,
        request: &cairn_common::executor_protocol::CloudObjectGrantRequest,
        base_commit: &str,
    ) -> Result<cairn_common::executor_protocol::CloudObjectGrant, CloudObjectGrantMintError> {
        if !crate::orchestrator::object_plane::ObjectPlaneState::valid_cloud_grant_shape(request) {
            return Err(CloudObjectGrantMintError::InvalidRequest);
        }
        if !self
            .object_plane
            .authorizes_cloud_grant(request, base_commit)
        {
            return Err(CloudObjectGrantMintError::StaleExecution);
        }
        let team_id = self
            .db
            .team_id_for_project(&request.coordinate.repository.project_id)
            .await
            .map_err(CloudObjectGrantMintError::Unavailable)?
            .ok_or_else(|| CloudObjectGrantMintError::MissingProjectAssociation {
                project_id: request.coordinate.repository.project_id.clone(),
            })?;
        let device_jwt = crate::account::read_device_jwt(&self.db.local)
            .await
            .map_err(CloudObjectGrantMintError::Unavailable)?
            .ok_or_else(|| {
                CloudObjectGrantMintError::Unavailable(
                    "Cloud object grants require a connected runner account".into(),
                )
            })?;
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|error| CloudObjectGrantMintError::Unavailable(error.to_string()))?;
        crate::account::mint_cloud_object_grant(
            &client,
            &device_jwt,
            &team_id,
            &request.content_hash,
            request.operation,
            request.byte_count,
            &self.api_config,
        )
        .await
        .map_err(CloudObjectGrantMintError::Unavailable)
    }

    /// Search's semantic lane. Always available; it answers only when span
    /// vectors exist and the query embeds, and search degrades to the text
    /// index whenever it does not.
    pub fn semantic_search(&self) -> &Arc<SemanticSearch> {
        self.semantic_search
            .get_or_init(|| Arc::new(SemanticSearch::new()))
    }

    /// Start the process-lifetime workers that hold this orchestrator: thread-era
    /// summary upgrades, tenet evidence evaluation, the wake-condition
    /// evaluator, and the read-only bginf mirror. Hosts call this once;
    /// `build()` never does, so a dropped test orchestrator releases every
    /// channel it owns.
    pub fn start_background_workers(&self) {
        crate::threads::era_worker::spawn(self.clone(), &self.era_summary_tx);
        crate::tenets::evidence::spawn(self.clone());
        crate::orchestrator::wakes::conditions::install_evaluator(self.clone());
        crate::orchestrator::inference::spawn_bginf_poller(self.clone());
    }

    pub fn start_embed_worker(&self) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_embed_worker(
            rx,
            self.embedder(),
            self.db.clone(),
            self.vibe_state.clone(),
            self.services.emitter.clone(),
        );
        if let Ok(mut guard) = self.embed_tx.lock() {
            *guard = Some(tx);
        }

        // The semantic lane's corpus sweep. `has_account` deliberately asks the
        // ACCOUNT manager, not the token provider: every install registers an
        // anonymous device token, so a usable token says nothing about consent.
        // Live turns embed regardless; walking back through pre-existing
        // transcripts is what requires a connected account.
        let account_manager = self.account_manager.clone();
        crate::embeddings::turns::spawn_turn_embedding_sweep(
            self.embedder(),
            self.db.local.clone(),
            Arc::new(move || account_manager.get_jwt().ok().flatten().is_some()),
        );
    }

    /// Claim the turn-end-check single-flight slot for a job. Returns the job's
    /// [`TurnEndCancel`] handle when this call newly claimed the slot (no run was
    /// already in flight), or `None` when a run is already active for the job — in
    /// which case the caller must NOT spawn another suite. The claim is released
    /// with [`Self::end_turn_end_checks`].
    pub fn try_begin_turn_end_checks(&self, job_id: &str) -> Option<TurnEndCancel> {
        let mut map = self.turn_end_checks_in_flight.lock().ok()?;
        if map.contains_key(job_id) {
            return None;
        }
        let cancel = TurnEndCancel::default();
        map.insert(job_id.to_string(), cancel.clone());
        drop(map);
        if let Ok(mut jobs) = self.recent_turn_end_check_requests.lock() {
            jobs.remove(job_id);
        }
        self.invalidate_node_check_status(job_id, "check-submission");
        Some(cancel)
    }

    /// Release the turn-end-check single-flight slot for a job. Idempotent.
    pub fn end_turn_end_checks(&self, job_id: &str) {
        if let Ok(mut map) = self.turn_end_checks_in_flight.lock() {
            map.remove(job_id);
        }
        self.invalidate_node_check_status(job_id, "check-settlement");
    }

    /// Whether a turn-end check run is currently in flight for a job — the signal
    /// the PR-node / `/checks` render reads to show the "running" state.
    pub fn turn_end_checks_in_flight(&self, job_id: &str) -> bool {
        self.turn_end_checks_in_flight
            .lock()
            .map(|map| map.contains_key(job_id))
            .unwrap_or(false)
    }

    pub(crate) fn begin_write_checks(&self, job_id: &str) {
        if let Ok(mut jobs) = self.write_checks_in_flight.lock() {
            *jobs.entry(job_id.to_string()).or_default() += 1;
        }
        self.invalidate_node_check_status(job_id, "head-tree-or-write-check-submission");
    }

    pub(crate) fn end_write_checks(&self, job_id: &str) {
        if let Ok(mut jobs) = self.write_checks_in_flight.lock() {
            if let Some(count) = jobs.get_mut(job_id) {
                *count -= 1;
                if *count == 0 {
                    jobs.remove(job_id);
                }
            }
        }
        self.invalidate_node_check_status(job_id, "write-check-settlement-or-result");
    }

    pub(crate) fn write_checks_in_flight(&self, job_id: &str) -> bool {
        self.write_checks_in_flight
            .lock()
            .map(|jobs| jobs.contains_key(job_id))
            .unwrap_or(false)
    }

    pub(crate) fn turn_end_check_runtime_status(
        &self,
        job_id: &str,
    ) -> Option<TurnEndCheckRuntimeStatus> {
        self.turn_end_checks_in_flight
            .lock()
            .ok()?
            .get(job_id)?
            .runtime_status()
    }

    pub(crate) fn recent_turn_end_check_request_ids(
        &self,
        job_id: &str,
    ) -> HashMap<String, String> {
        self.recent_turn_end_check_requests
            .lock()
            .ok()
            .and_then(|jobs| jobs.get(job_id).cloned())
            .unwrap_or_default()
    }

    pub(crate) fn set_turn_end_check_request_id(
        &self,
        job_id: &str,
        check_name: String,
        request_id: String,
    ) {
        if let Ok(mut jobs) = self.recent_turn_end_check_requests.lock() {
            jobs.entry(job_id.to_string())
                .or_default()
                .insert(check_name.clone(), request_id.clone());
        }
        if let Ok(checks) = self.turn_end_checks_in_flight.lock() {
            if let Some(cancel) = checks.get(job_id) {
                cancel.set_request_id(check_name, request_id);
            }
        }
    }

    /// Signal any in-flight turn-end check suite for `job_id` to quit. Best-effort
    /// and idempotent: a no-op when no suite is in flight for the job. Fired when
    /// the job's issue reaches a terminal state, its branch advances beyond the
    /// checked tree, or a user explicitly cancels the suite.
    /// Signal the job's in-flight turn-end check wave, reporting whether there
    /// was one to signal.
    pub fn cancel_turn_end_checks(&self, job_id: &str, reason: TurnEndCancelReason) -> bool {
        let signalled = match self.turn_end_checks_in_flight.lock() {
            Ok(map) => match map.get(job_id) {
                Some(cancel) => {
                    cancel.cancel(reason);
                    true
                }
                None => false,
            },
            Err(_) => false,
        };
        self.invalidate_node_check_status(job_id, "cancellation");
        signalled
    }

    /// Advance the complete correctness generation for a job's settled check
    /// projection. Call only after the corresponding state transition commits.
    pub(crate) fn invalidate_node_check_status(&self, job_id: &str, reason: &'static str) {
        self.node_check_status_cache.invalidate(job_id, reason);
    }

    pub(crate) fn invalidate_project_check_results(&self, project_id: &str) {
        self.node_check_status_cache
            .invalidate_project_results(project_id);
    }

    /// Advance the correctness generation for a job's PR projection.
    ///
    /// Call after the durable transition commits: the PR is seeded, bound, or
    /// opened; its title/body or publication changes; its source head, base, or
    /// rebase state moves; an authoritative GitHub webhook lands; it merges or
    /// closes; or an operator asks for a refresh. Anything outside that set does
    /// not change the projection and must not invalidate it — the notification a
    /// refresh emits is not itself cache coherence.
    pub fn invalidate_pr_refresh(&self, job_id: &str, reason: &'static str) {
        self.pr_refresh_cache.invalidate(job_id, reason);
    }

    /// Advance the correctness generation for a project's thread activity rollup.
    ///
    /// The projection reads threads, their jobs, those jobs' head turns,
    /// unanswered prompts, and pending permissions — and nothing else. A merge
    /// request, a check result, or an issue transition cannot move it, so routing
    /// those here would rebuild a whole-project SQL rollup to arrive at the same
    /// answer.
    pub fn invalidate_thread_status(&self, project_id: &str, reason: &'static str) {
        self.thread_status_cache.invalidate(project_id, reason);
    }

    /// Spawn a background task that registers an anonymous device JWT for the
    /// `/embed` gateway and keeps it fresh. Registers immediately, then
    /// re-checks every ~12h (well inside the 30-day token TTL). Best-effort:
    /// failures leave embedding neutral until the next attempt. Idempotent at
    /// the API level (reuses the persisted device_id).
    ///
    /// Runs unconditionally, even when an account is connected: the account JWT
    /// takes precedence in `resolve_embed_token`, but keeping the anon token
    /// warm means embedding keeps working immediately on logout. The anon token
    /// is `type: "device_anon"` and is only ever accepted by `/embed`.
    pub fn start_anon_device(&self) {
        let anon = self.anon_device_manager.clone();
        tokio::spawn(async move {
            loop {
                anon.ensure_registered().await;
                tokio::time::sleep(tokio::time::Duration::from_secs(12 * 3600)).await;
            }
        });
    }

    /// Run one archival sweep pass: archive the leftover `full` events of every
    /// execution whose issue was resolved more than the grace period ago.
    ///
    /// This frees pages to the freelist but does NOT shrink the database file:
    /// file-size reclamation is a separate offline step run via the
    /// `vacuum_reclaim` cargo example (see the `archival::sweep` module docs and
    /// docs/database.md).
    pub async fn run_archival_maintenance(&self) {
        match crate::archival::run_archival_sweep(&self.db.local).await {
            Ok(summary) => log::info!(
                "archival sweep: {} executions archived, {} failed ({})",
                summary.executions,
                summary.failed,
                summary.archive,
            ),
            Err(e) => log::warn!("archival sweep failed: {e}"),
        }
    }

    /// Verify the whole database and repair recoverable index-entry drift.
    ///
    /// Whole-database `PRAGMA integrity_check` used to run inside every fk_off
    /// table rebuild, where its cost scaled with the database rather than the
    /// migration and where unrelated pre-existing drift could fail an unrelated
    /// migration and panic the runner's boot (CAIRN-3103). Migrations now verify
    /// their own rebuilt tables; whole-database verification belongs here, off
    /// the boot path, where every finding is a report rather than a gate. Gated
    /// by `integrity_sweep_state` to a weekly cadence, so most boots pay nothing.
    pub async fn run_integrity_sweep(&self) {
        if let Err(e) = crate::storage::run_integrity_sweep(&self.db.local).await {
            log::warn!("integrity sweep failed: {e}");
        }
    }

    pub fn spawn_integrity_sweep(&self) {
        let db = self.db.local.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::storage::run_integrity_sweep(&db).await {
                log::warn!("integrity sweep failed: {e}");
            }
        });
    }

    /// Retire the attention queue's historical backlog once, in the background.
    ///
    /// Gated by `attention_push_retirement_backfill`, so it runs to completion
    /// once per install and costs nothing on every later boot. Detached for the
    /// same reason archival maintenance is: it walks a table whose size is a
    /// property of how long this install has been running, and boot must not
    /// wait for it.
    pub fn spawn_attention_retirement_backfill(&self) {
        let db = self.db.local.clone();
        tokio::spawn(async move {
            if let Err(e) =
                crate::orchestrator::attention_retirement::run_retirement_backfill(&db).await
            {
                log::warn!("attention retirement backfill failed: {e}");
            }
        });
    }

    /// Spawn the archival sweep on its own task: one pass now, then one every
    /// [`crate::archival::sweep::SWEEP_INTERVAL`]. Detached because the first
    /// pass on an install with a backlog is a long catch-up that no other
    /// startup work should wait behind.
    pub fn spawn_archival_maintenance(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(crate::archival::sweep::SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                orch.run_archival_maintenance().await;
            }
        });
    }

    /// Spawn the one-time historical analytics-rollup backfill on a background
    /// task.
    ///
    /// Call once at startup, on a tokio runtime. Live event inserts now maintain
    /// the token/cost and tool-invocation rollups incrementally, so the analytics
    /// page never folds/backfills on open; this fills both rollups in for events
    /// that predate that per-event seam. Gated by `analytics_rollup_backfill_state`
    /// so it runs once, never on every startup, and detached so it never blocks
    /// startup or the UI (best-effort: a failure logs and is retried next start).
    pub async fn run_analytics_rollup_backfill(&self) {
        if let Err(e) = cairn_analytics::run_historical_backfill(&self.db.local).await {
            log::warn!("analytics rollup backfill failed: {e}");
        }
    }

    pub fn spawn_analytics_rollup_backfill(&self) {
        let db = self.db.local.clone();
        tokio::spawn(async move {
            if let Err(e) = cairn_analytics::run_historical_backfill(&db).await {
                log::warn!("analytics rollup backfill failed: {e}");
            }
        });
    }

    /// Spawn the live recipe scheduler: a detached loop that fires
    /// schedule-triggered recipes. Each fire creates a fresh issue and starts the
    /// recipe as that issue's execution, stamped `triggered_by = schedule`, so a
    /// scheduled run gets the normal issue/execution/attention surface.
    ///
    /// The loop uses a bounded sleep (`min(soonest_fire - now, 60s)`): a capped
    /// wake just recomputes next-fire times, so recipe create/edit/delete lands
    /// within a minute without a poke channel back into core. Firing is
    /// restart-safe and double-fire-safe via the executions-table dedupe
    /// (`MAX(started_at) WHERE triggered_by = 'schedule'`) — no marker table.
    ///
    /// Owned by the always-on hosts (runner and non-inert server); the thin
    /// desktop app must never call this. Must run within a tokio runtime.
    pub fn spawn_recipe_scheduler(&self) {
        crate::execution::scheduler::spawn_recipe_scheduler(self.clone());
    }

    /// Spawn the single host-global scheduler for durable MCP continuations.
    /// It scans every routed database in bounded batches and arms only due waits.
    pub fn spawn_mcp_scheduler(&self) {
        crate::mcp::handlers::mcp_continuation::spawn_scheduler(self.clone());
    }

    /// Spawn external human-gate delivery. Runtime owners choose and construct
    /// the provider so channel processes remain outside the orchestrator.
    pub fn spawn_channel_router(
        &self,
        provider: std::sync::Arc<dyn crate::channels::ChannelProvider>,
        config: crate::models::IMessageChannelConfig,
    ) {
        crate::channels::router::spawn(
            self.clone(),
            provider,
            "imessage",
            config.to,
            config.route,
            config.inbound_capabilities,
            crate::channels::router::SessionBoundary::Fresh,
        );
    }

    /// Re-dispatch workflow runs that were in flight when the process died
    /// (CAIRN-2498): re-spawn each crashed workflow's `bun <script>` for the
    /// SAME run_id so its journal replays already-completed `agent()` calls
    /// instead of re-running them. Called once at host startup, after the MCP
    /// callback server is bound so the re-spawned scripts can call back. Owned
    /// by the always-on hosts (runner, non-inert server).
    /// Re-arm durable inline waits after a host restart. Overdue deadlines and
    /// retained terminal conditions resolve immediately; pending conditions are
    /// watched again from their authoritative database rows.
    pub async fn reconcile_owned_waits(&self) {
        crate::mcp::handlers::durable_suspend::reconcile(self).await;
    }

    /// Resume, once, each job whose turn this boot's stale-run sweep settled
    /// because the host serving it is gone. Blocking; the transport runs it
    /// detached once the MCP callback gateway is ready, because every resumed
    /// agent calls straight back through that gateway. See
    /// [`crate::runs::queries::resume_jobs_interrupted_by_restart`].
    pub fn resume_jobs_interrupted_by_restart(&self) -> crate::runs::queries::RestartResumeSummary {
        crate::runs::queries::resume_jobs_interrupted_by_restart(self, self.boot_at)
    }

    pub async fn redispatch_crashed_workflows(&self) {
        crate::execution::jobs::redispatch_crashed_workflows(self).await;
    }

    /// Heal node-less delegated children (calls/workflows) left in the CAIRN-2559
    /// stuck shape by a host crash under the old code: run terminal, job status
    /// never terminalized, caller durably blocked. Terminalize each from its
    /// turn+artifact and fire the resume gate. `redispatch_crashed_workflows`
    /// only re-spawns workflows whose latest run is NON-terminal, so it never
    /// revisits this shape; run this once at startup right after it. Idempotent.
    /// Owned by the always-on hosts (runner, non-inert server).
    pub async fn heal_stuck_delegated_children(&self) {
        crate::execution::advancement::heal_stuck_delegated_children(self).await;
    }

    /// Deliver artifact revisions whose quiet-period timer was lost with the
    /// previous host process, so a job launched with an older version still
    /// hears about the newer one. Runs after crashed runs have settled, since a
    /// delivery may wake an idle consumer. Idempotent.
    pub async fn deliver_pending_artifact_revisions_on_startup(&self) {
        crate::execution::artifact_revisions::deliver_pending_revisions(self).await;
    }

    /// Fail-fast ephemeral calls left in-flight by a host crash (CAIRN-2548) so a
    /// queued Claude call can never strand `starting` forever once the admission
    /// ceiling makes queuing reachable. Must run at startup BEFORE
    /// [`Self::redispatch_crashed_workflows`], so it only touches stale pre-crash
    /// runs and never a call the workflow replay just re-issued. Owned by the
    /// always-on hosts (runner, non-inert server).
    pub async fn fail_orphaned_calls_on_startup(&self) {
        crate::execution::jobs::fail_orphaned_calls_on_startup(self).await;
    }

    /// Warn about open sessions whose provider launch never confirmed. Reads
    /// only; runs after the stale-run sweep has settled predecessor runs.
    pub async fn log_unconfirmed_launch_sessions_on_startup(&self) {
        crate::sessions::log_unconfirmed_launch_sessions_on_startup(self).await;
    }

    /// Terminalize turn-end check attempts whose ephemeral runtime ownership was
    /// lost with the previous host process.
    pub async fn reconcile_turn_end_check_attempts_on_startup(&self) {
        crate::execution::checks_turn_end::reconcile_turn_end_attempts_on_startup(self).await;
    }

    /// Spawn the memory-triage reconciliation sweep: once immediately at
    /// startup, then on a periodic timer. The sweep is driven entirely by DB
    /// state and guarantees every at-threshold pending pool has a triage issue
    /// even when no fresh same-scope confirmation occurs (accumulated pools,
    /// reverted/deferred batches, lowered thresholds, crashes between claim and
    /// issue creation, drafts stranded on failed/interrupted jobs). Idempotent;
    /// errors are logged, never fatal. Must be called from within a tokio
    /// runtime context.
    pub async fn run_memory_triage_reconcile(&self) {
        if let Err(error) = crate::memories::triage::reconcile_memory_triage(self.clone()).await {
            log::warn!("memory triage reconcile failed: {error}");
        }
    }

    pub fn spawn_memory_triage_reconcile(&self) {
        self.spawn_memory_triage_reconcile_loop(false);
    }

    pub fn spawn_memory_triage_reconcile_periodic(&self) {
        self.spawn_memory_triage_reconcile_loop(true);
    }

    fn spawn_memory_triage_reconcile_loop(&self, skip_initial: bool) {
        const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);
        let orch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            if skip_initial {
                interval.tick().await;
            }
            loop {
                interval.tick().await;
                if let Err(error) =
                    crate::memories::triage::reconcile_memory_triage(orch.clone()).await
                {
                    log::warn!("memory triage reconcile failed: {error}");
                }
            }
        });
    }

    /// Spawn the Memory Review delivery reconciliation sweep: once immediately at
    /// startup, then periodically. This backstops jobs that were already marked
    /// `memory_review_state = 'sent'` before the direct-delivery path moved to
    /// attention pushes. It is conservative and idempotent; errors are logged.
    pub async fn run_memory_review_reconcile(&self) {
        let orch = self.clone();
        match tokio::task::spawn_blocking(move || {
            crate::memories::commands::reconcile_stranded_memory_reviews(orch)
        })
        .await
        {
            Ok(Ok(count)) if count > 0 => {
                log::info!("memory review reconcile resumed {count} stranded review(s)")
            }
            Ok(Ok(_)) => {}
            Ok(Err(error)) => log::warn!("memory review reconcile failed: {error}"),
            Err(error) => log::warn!("memory review reconcile task failed: {error}"),
        }
    }

    pub fn spawn_memory_review_reconcile(&self) {
        self.spawn_memory_review_reconcile_loop(false);
    }

    pub fn spawn_memory_review_reconcile_periodic(&self) {
        self.spawn_memory_review_reconcile_loop(true);
    }

    fn spawn_memory_review_reconcile_loop(&self, skip_initial: bool) {
        const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);
        let orch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            if skip_initial {
                interval.tick().await;
            }
            loop {
                interval.tick().await;
                match crate::memories::commands::reconcile_stranded_memory_reviews(orch.clone()) {
                    Ok(count) if count > 0 => {
                        log::info!("memory review reconcile resumed {count} stranded review(s)");
                    }
                    Ok(_) => {}
                    Err(error) => log::warn!("memory review reconcile failed: {error}"),
                }
            }
        });
    }

    /// Reconcile in-flight default-branch workspaces once at startup for remote
    /// advances that landed while the app was closed. Live advances reach the
    /// store through `base_branch_sync`: the GitHub `push` webhook, Cairn's
    /// own merge path, and a polling floor that `spawn_default_advance_reconcile`
    /// starts after this catch-up, which bounds the lag when the relay is absent.
    ///
    /// Must be called from within a tokio runtime context.
    pub async fn run_default_advance_reconcile(&self) {
        crate::projects::crud::reconcile_project_repositories(&self.db.local).await;
        crate::projects::crud::reconcile_default_branches(&self.db.local).await;
        crate::orchestrator::base_advance::reconcile_startup_remote_default_advances(self).await;
        crate::execution::checks_main::spawn_all_current(
            self,
            crate::execution::checks_main::Trigger::Startup,
        )
        .await;
        crate::orchestrator::base_advance::sweep_reconcile_intents(self).await;
    }

    pub fn spawn_default_advance_reconcile(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            crate::projects::crud::reconcile_project_repositories(&orch.db.local).await;
            // One-time at startup, before the catch-up: re-detect and persist each
            // local project's real default branch, correcting any row left on the
            // unverified 'main' schema default. The catch-up and merges resolve
            // onto the stored default, so this repair must land first. Best-effort
            // and logged per project.
            crate::projects::crud::reconcile_default_branches(&orch.db.local).await;

            crate::orchestrator::base_advance::reconcile_startup_remote_default_advances(&orch)
                .await;
            crate::execution::checks_main::spawn_all_current(
                &orch,
                crate::execution::checks_main::Trigger::Startup,
            )
            .await;
            crate::execution::checks_main::spawn_heartbeat(&orch);
            orch.spawn_reconcile_intent_sweep();
            crate::orchestrator::base_branch_sync::spawn_base_branch_poll(&orch);
        });
    }

    /// Drain durable sibling intents periodically, including work re-queued when
    /// a branch is protected by an in-flight RunBatch bracket.
    ///
    /// This loop is the ONLY thing that ever revisits a deferred base advance, so
    /// its liveness is the liveness of the whole recovery path. Each pass
    /// therefore runs as its own task and is joined here: a panic inside the
    /// sweep surfaces as a `JoinError` on this side instead of unwinding the
    /// loop. Run inline, one panic — a database assertion, a subprocess parse —
    /// silently retires the sweep for the lifetime of the process, and every
    /// branch whose advance was deferred waits forever on a worker that no longer
    /// exists, with nothing in the logs and nothing on the branch to say so.
    pub fn spawn_reconcile_intent_sweep(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            loop {
                let pass = orch.clone();
                let joined = crate::orchestrator::base_advance::supervised_sweep_pass(async move {
                    crate::orchestrator::base_advance::sweep_reconcile_intents(&pass).await;
                })
                .await;
                if let Err(error) = joined {
                    log::error!(
                        "jj reconcile sweep pass ended abnormally and was not completed; \
                         retrying on the next cycle. Deferred base advances stay queued until a \
                         pass succeeds: {error}"
                    );
                }
                tokio::time::sleep(crate::orchestrator::base_advance::RECONCILE_SWEEP_INTERVAL)
                    .await;
            }
        });
    }

    /// Enable the per-team background sync loop: build a [`SyncRuntime`] from the
    /// services emitter and default cadence and hand it to `DbState`. Mirrors the
    /// other detached background-spawn methods (`spawn_memory_*`,
    /// `spawn_default_advance_reconcile`) — same fire-and-forget pattern, owned by
    /// runtime teardown. The actual per-team push/pull tasks then spawn lazily as
    /// teams open. When no device JWT exists, the runtime is retained but its
    /// network tasks remain paused until the account connect transition re-arms
    /// them. Inert with no team configured. Must run within a tokio runtime.
    pub fn start_team_sync(&self) {
        let db = self.db.clone();
        let runtime = crate::db::SyncRuntime {
            emitter: self.services.emitter.clone(),
            cadence: crate::storage::SyncCadence::default(),
        };
        tokio::spawn(async move {
            let authorized = match crate::account::team_sync::read_device_jwt(&db.local).await {
                Ok(jwt) => jwt.is_some(),
                Err(error) => {
                    log::warn!(
                        "failed to determine team sync authentication state; starting paused: {error}"
                    );
                    false
                }
            };
            db.set_team_sync_authorized(authorized).await;
            db.enable_team_sync(runtime).await;
        });
    }

    /// Resolve the replica path for a team's synced database: `<dir>/teams/<id>.db`
    /// beside the private database, so a dev instance (`CAIRN_DB_PATH`) keeps its
    /// team replicas under its own home rather than a shared location.
    fn team_replica_path(&self, team_id: &str) -> PathBuf {
        self.db
            .local
            .path()
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("teams")
            .join(format!("{team_id}.db"))
    }

    /// Runtime entry point for using a team's synced data: fetch the team's sync
    /// config (member-authed, reusing the stored device JWT), and on an active
    /// team register it in the private `teams` catalog and open its replica
    /// (which reconciles the team's project routes). 404/503 are treated as
    /// not-yet-available: nothing is opened or written and the caller can poll.
    ///
    /// The durable local `teams` registry is the source of truth for which teams
    /// to reopen at startup; this method is what populates it in production. The
    /// api remains the membership authority (the broker rechecks every request).
    pub async fn connect_team(
        &self,
        team_id: &str,
    ) -> Result<crate::account::team_sync::TeamConnectStatus, String> {
        use crate::account::team_sync::{
            fetch_team_sync_config, read_device_jwt, SyncConfigStatus, TeamConnectStatus,
        };

        // Read the device JWT directly (async, no AccountManager refresh loop).
        let device_jwt = match read_device_jwt(&self.db.local).await? {
            Some(jwt) => jwt,
            None => return Ok(TeamConnectStatus::NotAuthenticated),
        };

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;

        match fetch_team_sync_config(&client, &device_jwt, team_id, &self.api_config).await? {
            SyncConfigStatus::NotConfigured => Ok(TeamConnectStatus::NotConfigured),
            SyncConfigStatus::Provisioning => Ok(TeamConnectStatus::Provisioning),
            SyncConfigStatus::Active(cfg) => {
                let replica_path = self.team_replica_path(team_id);
                if let Some(parent) = replica_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("create team replica dir: {e}"))?;
                }
                // Resolve the team's human-readable name from the account's org
                // memberships — a team id IS its org id. This is the name seeded
                // into the synced `teams` root row and stored in the private
                // registry, so members converge on the same id+name. Falls back to
                // the api's db_name only if the membership is unexpectedly absent.
                let team_name = crate::account::queries::get(&self.db.local)
                    .await?
                    .and_then(|conn| {
                        conn.org_memberships
                            .into_iter()
                            .find(|m| m.org_id == team_id)
                            .map(|m| m.org_name)
                    })
                    .unwrap_or_else(|| cfg.db_name.clone());
                // Register the team durably BEFORE opening, so a project_routes
                // row written during reconcile satisfies its FK to `teams(id)`.
                self.db
                    .upsert_team_registry(
                        team_id,
                        &team_name,
                        &cfg.sync_url,
                        &replica_path.to_string_lossy(),
                    )
                    .await
                    .map_err(|e| format!("register team in catalog: {e}"))?;
                self.db
                    .open_team(crate::db::TeamConfig {
                        team_id: team_id.to_string(),
                        team_name,
                        sync_url: cfg.sync_url,
                        auth_token: None,
                        replica_path,
                    })
                    .await
                    .map_err(|e| format!("open team replica: {e}"))?;
                Ok(TeamConnectStatus::Connected)
            }
        }
    }

    /// Read-only companion to [`Self::connect_team`]: probe the sync readiness of
    /// every team the account belongs to WITHOUT opening any replica. This backs
    /// the desktop create-into-team selector, which must show which teams can
    /// receive a project before the user picks one (the actual replica open still
    /// happens via `connect_team` at submit). Teams are enumerated from the
    /// account's org memberships; with no account connected the list is empty, and
    /// with no device JWT every team is reported `NotAuthenticated` without a probe.
    pub async fn list_team_sync_status(
        &self,
    ) -> Result<Vec<crate::account::team_sync::TeamSyncStatus>, String> {
        use crate::account::team_sync::{
            probe_team_sync_status, read_device_jwt, TeamSyncReadiness, TeamSyncStatus,
        };

        let team_ids: Vec<String> = match crate::account::queries::get(&self.db.local).await? {
            Some(conn) => conn.org_memberships.into_iter().map(|m| m.org_id).collect(),
            None => return Ok(Vec::new()),
        };
        if team_ids.is_empty() {
            return Ok(Vec::new());
        }

        let device_jwt = match read_device_jwt(&self.db.local).await? {
            Some(jwt) => jwt,
            None => {
                return Ok(team_ids
                    .into_iter()
                    .map(|team_id| TeamSyncStatus {
                        team_id,
                        status: TeamSyncReadiness::NotAuthenticated,
                    })
                    .collect());
            }
        };

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;

        Ok(probe_team_sync_status(&client, &device_jwt, &team_ids, &self.api_config).await)
    }

    /// Auto-connect every team the account belongs to: a fail-soft fan-out over
    /// [`Self::connect_team`] for each org membership. This is the single
    /// reconcile primitive behind the startup, sign-in, and membership-refresh
    /// triggers that make a joined team's projects and issues visible without a
    /// create-into-team flow.
    ///
    /// A per-team failure is logged and swallowed so one unreachable or
    /// unprovisioned team never blocks the rest — mirroring
    /// [`crate::account::probe_team_sync_status`]'s fail-soft contract.
    /// `Provisioning`/`NotConfigured`/`NotAuthenticated` open nothing and are
    /// simply retried on the next reconcile. Because `connect_team` is
    /// idempotent (its `open_team` is single-flight and `upsert_team_registry`
    /// is an upsert), this is safe to call repeatedly. Strict no-op for
    /// local-only installs: no account means no memberships means nothing to do.
    pub async fn connect_account_teams(&self) -> crate::account::ConnectAccountTeamsSummary {
        use crate::account::{ConnectAccountTeamsSummary, TeamConnectStatus};

        let team_ids: Vec<String> = match crate::account::queries::get(&self.db.local).await {
            Ok(Some(conn)) => conn.org_memberships.into_iter().map(|m| m.org_id).collect(),
            // No account row: strict no-op. We only forget a team when we
            // positively know the current membership set excludes it; a missing
            // account (transient read gap, or a logout handled elsewhere) must
            // not tear down team data.
            Ok(None) => return ConnectAccountTeamsSummary::default(),
            Err(error) => {
                log::warn!("connect_account_teams: failed to read account memberships: {error}");
                return ConnectAccountTeamsSummary::default();
            }
        };

        let mut summary = ConnectAccountTeamsSummary::default();

        // Subtractive reconcile FIRST: forget every registered team the account
        // no longer belongs to, so a member removed from a team loses local
        // visibility (its replica closes and it won't reopen at startup). The
        // membership set is authoritative here — an empty set (account in zero
        // teams) correctly forgets all previously-registered teams. The api
        // remains the ultimate authority; this enforces it on the client too.
        let membership_set: std::collections::HashSet<&str> =
            team_ids.iter().map(String::as_str).collect();
        match self.db.registered_team_ids().await {
            Ok(registered) => {
                for team_id in registered {
                    if membership_set.contains(team_id.as_str()) {
                        continue;
                    }
                    match self.db.forget_team(&team_id).await {
                        Ok(_) => summary.forgotten += 1,
                        Err(error) => log::warn!(
                            "connect_account_teams: failed to forget team `{team_id}`: {error}"
                        ),
                    }
                }
            }
            Err(error) => {
                log::warn!("connect_account_teams: failed to list registered teams: {error}");
            }
        }

        for team_id in team_ids {
            match self.connect_team(&team_id).await {
                Ok(TeamConnectStatus::Connected) => summary.connected += 1,
                Ok(TeamConnectStatus::Provisioning) => summary.provisioning += 1,
                Ok(TeamConnectStatus::NotConfigured) => summary.not_configured += 1,
                Ok(TeamConnectStatus::NotAuthenticated) => summary.not_authenticated += 1,
                Err(error) => {
                    log::warn!(
                        "connect_account_teams: team `{team_id}` failed to connect: {error}"
                    );
                    summary.failed += 1;
                }
            }
        }
        if summary.connected > 0 || summary.failed > 0 || summary.forgotten > 0 {
            log::info!(
                "connect_account_teams: {} connected, {} provisioning, {} not-configured, {} not-authenticated, {} failed, {} forgotten",
                summary.connected,
                summary.provisioning,
                summary.not_configured,
                summary.not_authenticated,
                summary.failed,
                summary.forgotten,
            );
        }
        summary
    }

    /// Send a job to the embed worker. Non-blocking; a no-op if the worker
    /// hasn't been started.
    fn send_embed_job(&self, job: EmbedJob) {
        if let Ok(guard) = self.embed_tx.lock() {
            if let Some(tx) = guard.as_ref() {
                let _ = tx.send(job);
            }
        }
    }

    /// Enqueue an assistant event for async embedding + vibe coloring only
    /// (no session position). Used by the outbox replay/recovery path, where
    /// re-folding would double-count. Non-blocking; a no-op if the worker
    /// hasn't been started.
    pub(crate) fn enqueue_event_embed(&self, event_id: &str, text: String) {
        self.send_embed_job(EmbedJob::Event {
            event_id: event_id.to_string(),
            text,
            position: None,
        });
    }

    /// Enqueue an event for async embedding that both (for agent content) colors
    /// and folds into the session's semantic position. `kind` selects the feed
    /// (user / agent content / change signal); `tokens` is the event's token
    /// count when known, used to weight its contribution. Non-blocking; a no-op
    /// if the worker hasn't been started.
    pub(crate) fn enqueue_position_embed(
        &self,
        session_id: &str,
        event_id: &str,
        kind: PositionKind,
        text: String,
        tokens: Option<i32>,
    ) {
        let weight = PositionConfig::default().weight_for(kind, tokens, &text);
        self.send_embed_job(EmbedJob::Event {
            event_id: event_id.to_string(),
            text,
            position: Some(PositionMeta::new(session_id, kind, weight)),
        });
    }

    /// Enqueue a corpus resource for async embedding. Empty/whitespace text
    /// enqueues a delete instead (e.g. a description cleared to blank).
    /// Non-blocking; a no-op if the worker hasn't been started.
    pub fn enqueue_resource_embed(&self, uri: &str, text: String) {
        self.send_embed_job(EmbedJob::resource(uri, text));
    }

    /// Enqueue removal of a corpus resource's embedding.
    /// Non-blocking; a no-op if the worker hasn't been started.
    pub fn enqueue_resource_delete(&self, uri: &str) {
        self.send_embed_job(EmbedJob::ResourceDelete {
            uri: uri.to_string(),
        });
    }

    /// Evict warm processes to satisfy the memory budget before a new spawn.
    /// Returns the run ids evicted.
    pub(crate) fn collect_warm_if_needed(&self) -> Vec<String> {
        self.collect_warm(crate::agent_process::gc::SpawnRoom::NextProcess)
    }

    /// Run a warm-GC lookup to completion from synchronous code, off any async
    /// worker the caller may be on. `None` when no runtime could be built.
    fn run_gc_lookup<T, F, Fut>(lookup: F) -> Option<T>
    where
        T: Send + 'static,
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = T>,
    {
        let run = move || match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => Some(rt.block_on(lookup())),
            Err(error) => {
                log::error!("GC: failed to create database runtime: {}", error);
                None
            }
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::spawn(run).join().unwrap_or_else(|_| {
                log::error!("GC: database lookup thread panicked");
                None
            })
        } else {
            run()
        }
    }

    /// Shared warm-eviction pass. `spawn_room` additionally frees room for work
    /// about to start: one process of unknown size at a spawn, a queued
    /// admission's declared estimate when the private lane's head is blocked,
    /// nothing on the periodic sweep. Kills *every* run in the computed eviction
    /// set, so a parallel-spawn burst or an over-budget idle pool is trimmed in
    /// one pass rather than one eviction per call (CAIRN-2543).
    pub(crate) fn collect_warm(
        &self,
        spawn_room: crate::agent_process::gc::SpawnRoom,
    ) -> Vec<String> {
        let Some(gc) = self.warm_gc.as_ref().cloned() else {
            return Vec::new();
        };
        let process_state = self.process_state.clone();
        let dbs = self.db.clone();
        let eviction_set = Self::run_gc_lookup(move || async move {
            gc.find_eviction_set(&process_state, &dbs, spawn_room).await
        })
        .unwrap_or_default();
        self.evict_warm(&eviction_set);
        eviction_set
    }

    /// Evict the single least relevant unprotected warm process among
    /// `unit_holders`, for a queued admission an explicit unit ceiling blocks.
    pub(crate) fn evict_one_warm_unit(
        &self,
        unit_holders: &std::collections::HashSet<String>,
    ) -> Option<String> {
        let gc = self.warm_gc.as_ref().cloned()?;
        let process_state = self.process_state.clone();
        let dbs = self.db.clone();
        let holders = unit_holders.clone();
        let evicted = Self::run_gc_lookup(move || async move {
            gc.find_unit_eviction(&process_state, &dbs, &holders).await
        })
        .flatten()?;
        self.evict_warm(std::slice::from_ref(&evicted));
        Some(evicted)
    }

    fn evict_warm(&self, eviction_set: &[String]) {
        for run_id in eviction_set {
            log::info!(
                "GC: evicting warm process {}",
                &run_id[..run_id.len().min(8)]
            );
            if let Err(e) = lifecycle::kill_session_with_reason(self, run_id, "warm_evict") {
                log::error!("GC: failed to kill evicted process {}: {}", run_id, e);
            }
        }
    }

    /// The periodic process sweep: reconcile durable run rows against the
    /// processes this host actually holds, then trim the warm pool to its memory
    /// budget. Two jobs on one tick because both are decided from the same
    /// evidence — which runs have a live process handle right now.
    ///
    /// Reaping runs unconditionally (`crate::runs::reap`). Nothing else inside a
    /// live session settles a run whose process vanished without finalizing it,
    /// and such a row makes its node read as permanently starting to every reader
    /// until the app restarts (CAIRN-3291).
    ///
    /// Warm trimming is a no-op when warm retention is disabled. Once spawning
    /// stops, an over-budget idle pool would otherwise hold its memory forever
    /// (CAIRN-2543); this re-measures every ~30s and converges (eviction kills
    /// are async, so a tick can overshoot conservatively and the next tick
    /// re-measures). It also prunes stale GC view records.
    ///
    /// Must be called from within a tokio runtime.
    pub fn spawn_process_sweep(&self) {
        const SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
        let orch = self.clone();
        tokio::spawn(async move {
            {
                let orch = orch.clone();
                tokio::task::spawn_blocking(move || {
                    orch.migrate_claude_homes();
                    // Backfills a first period for every account that
                    // predates the table, and catches up on any change made
                    // while Cairn was not running.
                    orch.reconcile_subscription_periods();
                });
            }
            let mut interval = tokio::time::interval(SWEEP_INTERVAL);
            // Skip the immediate first tick: at startup the pool is empty, and a
            // spawn's own admission check covers pressure between sweeps.
            interval.tick().await;
            loop {
                interval.tick().await;
                {
                    // Not awaited: a renewal can wait on a provider round trip,
                    // and the reaping below must not wait with it.
                    let orch = orch.clone();
                    tokio::task::spawn_blocking(move || orch.sweep_claude_credentials());
                }
                for db in orch.db.all_dbs().await {
                    match crate::runs::watchdog_reconciler::reconcile_watchdog_leases(
                        &db,
                        crate::runs::watchdog_reconciler::ReconcileScope::Periodic,
                        chrono::Utc::now().timestamp(),
                        crate::runs::watchdog_reconciler::DEFAULT_HEARTBEAT_GRACE_SECS,
                        &orch,
                    )
                    .await
                    {
                        // The all-zero result is the positive control that distinguishes a
                        // healthy idle sweep from a sweep that was never spawned or stalled.
                        Ok(report) => log::info!(
                            "Codex watchdog periodic reconciliation: inspected={}, recovered={}, lost_claim={}, failed={}, ambiguous_clock={}",
                            report.inspected,
                            report.recovered,
                            report.lost_claim,
                            report.recovery_failed,
                            report.ambiguous_clock
                        ),
                        Err(error) => {
                            log::error!("Codex watchdog periodic reconciliation failed: {error}")
                        }
                    }
                }
                // A child that lost its session to provider silence and never
                // got another one is not visible from any edge — the edge that
                // would have fired is the successor launch that did not happen —
                // so the parent-facing notice is swept for here, beside the
                // stranded-job recovery that reads the same lifecycle evidence
                // (CAIRN-4609).
                crate::orchestrator::lifecycle::sweep_stalled_children(&orch).await;
                let orch = orch.clone();
                // Off the async worker: the eviction lookup and kills block on
                // DB round-trips, so keep the runtime responsive.
                if let Err(error) = tokio::task::spawn_blocking(move || {
                    let _span = tracing::info_span!(target: "profiler", "process_sweep").entered();
                    crate::runs::reap::reap_stale_runs(&orch);
                    // A call row whose run is over holds a lane unit and the
                    // only handle on its process; close both before the warm
                    // trim and promotion measure the lanes.
                    crate::execution::jobs::heal_settled_call_admissions(&orch, None);
                    crate::orchestrator::lifecycle::sweep_stranded_watchdog_jobs(&orch);
                    orch.collect_warm(crate::agent_process::gc::SpawnRoom::None);
                    // After the warm trim, so a queue head measures the memory
                    // that trim freed; and on every tick, so memory freed by
                    // anything other than an agent session still wakes it.
                    crate::execution::jobs::withdraw_orphaned_ordinary_admissions(&orch);
                    crate::execution::jobs::promote_private_lanes_blocking(&orch);
                    crate::execution::jobs::promote_call_lanes(&orch);
                    if let Some(gc) = orch.warm_gc.as_ref() {
                        gc.cleanup_stale_views();
                    }
                })
                .await
                {
                    log::warn!("Process sweep task failed: {error}");
                }
            }
        });
    }

    /// Seed one backend's discovered catalog, for a test outside this module.
    #[cfg(test)]
    pub(crate) fn seed_model_catalog_for_test(&self, catalog: ProviderModelCatalog) {
        self.model_catalog
            .write()
            .unwrap()
            .insert(catalog.backend.clone(), catalog);
    }

    pub fn get_model_catalog(&self) -> Vec<ProviderModelCatalog> {
        let Ok(catalog) = self.model_catalog.read() else {
            return Vec::new();
        };
        let mut providers: Vec<_> = catalog.values().cloned().collect();
        providers.sort_by(|a, b| a.backend.cmp(&b.backend));
        providers
    }

    /// Store a provider/account usage snapshot and notify the frontend.
    ///
    /// The single store path for both backends: it writes the in-memory cache
    /// (read by `get_provider_usage_snapshot`) **and** emits `provider-usage-updated`
    /// so the usage panel updates live instead of only on a manual refresh. Both
    /// Claude (`rate_limit_event`) and Codex (`account/rateLimits/updated`) route
    /// here, as does the manual refresh command.
    pub fn store_provider_usage_snapshot(&self, snapshot: ProviderUsageSnapshot) {
        self.store_provider_account_usage_snapshot(None, snapshot);
    }

    pub fn store_provider_account_usage_snapshot(
        &self,
        account_id: Option<String>,
        snapshot: ProviderUsageSnapshot,
    ) {
        // The book keeps the richer measurement and records a failure against
        // it rather than dropping either; see `UsageBook::record`.
        let Some(stored) = self
            .provider_usage_snapshots
            .record(account_id.as_deref(), snapshot)
        else {
            return;
        };
        let _ = self.services.emitter.emit(
            "provider-usage-updated",
            serde_json::json!({
                "backend": stored.backend,
                "accountId": account_id,
                "snapshot": stored,
            }),
        );
    }

    /// Forget a cached usage result after the credential generation changes.
    /// Account health is invalidated by the credential writer; this keeps the
    /// provider card from displaying an authentication error from the old token.
    pub(crate) fn clear_provider_account_usage_snapshot(&self, backend: &str, account_id: &str) {
        self.provider_usage_snapshots
            .remove(backend, Some(account_id));
        // Publish a neutral, non-canonical replacement through the canonical
        // update event so frontend query caches stop displaying the previous
        // credential's error and remain eligible to fetch the new generation.
        self.store_provider_account_usage_snapshot(
            Some(account_id.to_string()),
            ProviderUsageSnapshot {
                backend: backend.to_string(),
                source: "credential_generation".to_string(),
                captured_at: chrono::Utc::now().timestamp(),
                // Reads as a neutral placeholder in the provider card rather
                // than an empty panel, and ranks below any real snapshot, so
                // the first measurement of the new credential replaces it.
                unsupported_reason: Some(
                    "Usage is refreshing after this profile signed in.".to_string(),
                ),
                ..Default::default()
            },
        );
    }

    /// Store a normalized context-token snapshot and notify the frontend.
    ///
    /// Snapshots are keyed by durable session id and represent the latest turn's
    /// full prompt plus that turn's output, not cumulative usage across turns.
    pub(crate) fn store_context_token_snapshot(&self, state: ContextTokenState) {
        let Some(session_id) = state.session_id.clone() else {
            return;
        };
        {
            let Ok(mut guard) = self.context_token_snapshots.write() else {
                return;
            };
            guard.insert(session_id.clone(), state.clone());
        }

        let _ = self.services.emitter.emit(
            "context-tokens-updated",
            serde_json::json!({
                "sessionId": session_id,
                "state": state,
            }),
        );
    }

    pub async fn get_context_token_state(&self, session_id: &str) -> Option<ContextTokenState> {
        if let Some(state) = self
            .context_token_snapshots
            .read()
            .ok()
            .and_then(|guard| guard.get(session_id).cloned())
        {
            return Some(state);
        }

        // A team session's events live in its synced replica, so resolve the
        // owning database by the run that carries this session (CAIRN-2225)
        // rather than reading the private DB. Fail-closed: a team session whose
        // replica is not open yields no snapshot (the token meter stays empty)
        // rather than reading the private DB's empty result. A local session
        // short-circuits on the always-open private DB — behavior unchanged.
        let db = match crate::execution::routing::owning_db_for_session(&self.db, session_id).await
        {
            Ok(db) => db,
            Err(error) => {
                log::warn!("Failed to resolve owning database for context token state: {error}");
                return None;
            }
        };
        let snapshot = match get_latest_context_token_event(db, session_id).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                log::warn!("Failed to derive context token state from events: {error}");
                None
            }
        }?;
        let context_window = if snapshot
            .backend
            .eq_ignore_ascii_case(crate::backends::CLAUDE_FAMILY_BACKEND)
        {
            use cairn_models::types::catalog::{
                claude_reported_context_window, claude_session_window,
            };
            let resolved = snapshot.resolved_model.as_deref();
            let reported = snapshot
                .model_usage
                .as_ref()
                .and_then(|usage| claude_reported_context_window(usage, resolved));
            claude_session_window(reported, resolved, snapshot.model.as_deref())
        } else {
            self.context_window_for_context_tokens(&snapshot.backend, snapshot.model.as_deref())
        };
        let state = snapshot.into_state(context_window);

        if let Ok(mut guard) = self.context_token_snapshots.write() {
            guard.insert(session_id.to_string(), state.clone());
        }
        Some(state)
    }

    pub(crate) fn context_window_for_context_tokens(
        &self,
        backend: &str,
        model: Option<&str>,
    ) -> Option<i64> {
        let catalog = self.model_catalog.read().ok();
        let discovered = catalog
            .as_ref()
            .and_then(|catalog| catalog.get(&backend.to_lowercase()))
            .map(|entry| entry.models.as_slice())
            .unwrap_or_default();
        cairn_models::types::catalog::context_window(backend, model, discovered)
    }

    fn refresh_provider_model_catalog_blocking(&self, backend_name: &str) -> ProviderModelCatalog {
        let backend = backend_for_name(Some(backend_name));
        let (mut models, error) = if let Some(account_id) =
            cairn_models::types::catalog::custom_account_id(backend_name)
        {
            // Inventory is per provider, and a custom provider IS one account,
            // so discovery is scoped to that account rather than merged across
            // a vendor's hosts. Two custom providers serving the same model id
            // therefore stay two distinct models, each on its own provider.
            match crate::backends::custom::CustomProviderConfig::resolve(self, account_id, None) {
                Ok(config) => crate::backends::custom::models::discover_blocking(&config),
                Err(error) => (Vec::new(), Some(error)),
            }
        } else if backend_name == "claude" {
            crate::backends::claude::discover_catalog_blocking(self)
        } else if backend_name == "codex" {
            crate::backends::codex::discover_catalog_blocking(self)
        } else if backend_name == "ollama" {
            crate::backends::ollama::models::discover_catalog_blocking(self)
        } else if backend_name == crate::backends::actual::ACTUAL_BACKEND_KEY {
            // Inventory is per target, so discovery needs the configured
            // accounts rather than a vendor-wide list.
            crate::backends::actual::models::discover_catalog_blocking(self)
        } else if backend_name == crate::backends::openrouter::OPENROUTER_BACKEND_KEY {
            // Chat, image and embeddings listings, read side by side; one
            // failing must neither hide the others nor go unsaid, and a failed
            // chat or embeddings listing keeps what the last good one found.
            let previous = self
                .model_catalog
                .read()
                .ok()
                .and_then(|catalog| catalog.get(backend_name).map(|entry| entry.models.clone()))
                .unwrap_or_default();
            crate::backends::openrouter::discover_catalog_blocking(&previous)
        } else {
            match backend.discover_models() {
                Ok(models) => (models, None),
                Err(error) => (Vec::new(), Some(error)),
            }
        };
        let mut refreshed_at = Some(chrono::Utc::now().timestamp());
        // A failed refresh must not erase the last usable catalog. The error is
        // retained so clients can label these models as stale and offer Retry;
        // the prior successful timestamp remains the truthful age of the data.
        if error.is_some() {
            if let Some(previous) = self
                .model_catalog
                .read()
                .ok()
                .and_then(|catalog| catalog.get(backend_name).cloned())
            {
                if models.is_empty() {
                    models = previous.models;
                    refreshed_at = previous.refreshed_at;
                } else if cairn_models::types::catalog::custom_account_id(backend_name).is_some() {
                    // A custom provider's declared embedding models stay
                    // offered when its listing fails; the chat models that
                    // listing named are kept from the last good refresh
                    // rather than dropped beside them.
                    for model in previous.models {
                        if !models.iter().any(|known| known.id == model.id) {
                            models.push(model);
                        }
                    }
                }
            }
        }
        let entry = ProviderModelCatalog {
            backend: backend_name.to_string(),
            models,
            options: backend.option_descriptors(),
            refreshed_at,
            error,
        };
        if let Ok(mut catalog) = self.model_catalog.write() {
            catalog.insert(backend_name.to_string(), entry.clone());
        }
        // The meter registry's offer index caches this catalog; this is the
        // one place a provider's catalog is stored, so it is fed here.
        crate::token_meters::set_offer_tokenizers(backend_name, &entry.models);
        entry
    }

    /// Rediscover every provider's models, then tell clients the catalog moved.
    ///
    /// The announcement belongs here rather than at any one entry point. A full
    /// refresh runs on several schedules — the startup pass, the account paths
    /// that change credentials, and a managed-Codex version swap — and every
    /// one of them leaves clients holding a catalog they fetched before
    /// discovery finished. A scoped single-backend refresh deliberately stays
    /// silent: it hands the new entry straight back to the caller that asked.
    ///
    /// Every full refresh, detached or awaited, passes through here, so this is
    /// where [`Self::catalog_rediscovery`] holds it back.
    fn refresh_model_catalog(&self) {
        if !self.catalog_rediscovery {
            return;
        }
        // Only what this workspace installed. A provider it never enabled has
        // no models anyone can choose, so discovering them would be an
        // unprompted network call on a credential the user did not offer.
        for backend_name in self.enabled_providers() {
            self.refresh_provider_model_catalog_blocking(&backend_name);
        }
        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "model_catalog"}),
        );
    }

    pub async fn run_provider_model_catalog_refresh(
        &self,
        backend: &str,
    ) -> Result<ProviderModelCatalog, String> {
        let backend = backend.to_ascii_lowercase();
        if !cairn_models::types::catalog::is_supported(&backend) {
            return Err(format!("Unsupported backend: {backend}"));
        }
        if !self.provider_enabled(&backend) {
            return Err(format!(
                "{backend} is not enabled in this workspace. Add it from Settings → Providers first."
            ));
        }
        let orch = self.clone();
        let name = backend.clone();
        tokio::task::spawn_blocking(move || orch.refresh_provider_model_catalog_blocking(&name))
            .await
            .map_err(|e| format!("model catalog refresh task failed: {e}"))
    }

    /// Rediscover every provider's models on a dedicated thread.
    ///
    /// Discovery builds `reqwest::blocking` clients, and each of those owns a
    /// tokio runtime that panics when dropped inside an async context, so the
    /// refresh may never run on a runtime thread. That is also why callers
    /// cannot await a fresh catalog — they learn it landed from the completion
    /// event [`Self::refresh_model_catalog`] emits.
    pub fn spawn_model_catalog_refresh(&self) {
        let orch = self.clone();
        std::thread::spawn(move || {
            orch.refresh_model_catalog();
        });
    }
}

/// Run `task` to completion in the background on `runtime`, or, given none, on
/// a thread with a current-thread runtime of its own. Never blocks the caller.
///
/// Everything `task` spawns lands on the runtime `task` runs on, and is dropped
/// mid-await when that runtime is. A runtime built here is dropped the moment
/// `task` returns, so it suits only a task that spawns nothing meant to outlive
/// it. Work that may spawn for others (a coalesced leader, a ref-retirement
/// sweep) goes through [`Orchestrator::spawn_detached`], which supplies the
/// orchestrator's own runtime. If the fallback runtime cannot be built, `task`
/// never runs and `on_spawn_failure` is called in its place.
pub(crate) fn spawn_onto(
    runtime: Option<&tokio::runtime::Handle>,
    task: impl std::future::Future<Output = ()> + Send + 'static,
    on_spawn_failure: impl FnOnce() + Send + 'static,
) {
    if let Some(runtime) = runtime {
        runtime.spawn(task);
        return;
    }
    std::thread::spawn(move || {
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(task),
            Err(error) => {
                log::error!("detached task: failed to build a runtime to run it on: {error}");
                on_spawn_failure();
            }
        }
    });
}

#[cfg(test)]
mod detach_tests;

#[cfg(test)]
pub(crate) mod launch_coverage_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::jwt::encrypt_jwt_for_storage;
    use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
    use crate::services::EventEmitter;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_models::types::catalog::DiscoveredModel;

    #[test]
    fn startup_silence_warms_until_deadline_then_becomes_evidence() {
        let boot_at = 1_000;
        assert!(fleet_is_warming(0, boot_at, 1_089_999));
        assert!(!fleet_is_warming(0, boot_at, 1_090_000));
        assert!(!fleet_is_warming(1, boot_at, 1_001_000));
    }

    async fn test_db() -> Arc<DbState> {
        let local =
            LocalDb::open_migrated_for_test(tempfile::tempdir().unwrap().keep().join("orch.db"))
                .await
                .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        Arc::new(DbState::new(Arc::new(local), search))
    }

    /// Fixtures gate on the provisioned sidecar and then publish through the
    /// orchestrator. With a bare `jj` default those were two binaries: on a
    /// machine with no PATH install every such test failed `jj git init
    /// --git-repo: No such file or directory`, and on one with an install it
    /// passed, so the verdict described the machine's PATH rather than the tree.
    #[tokio::test]
    async fn a_default_test_orchestrator_drives_the_provisioned_jj() {
        let services = Arc::new(TestServicesBuilder::new().build());
        let orch = OrchestratorBuilder::new(
            test_db().await,
            services,
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        match cairn_vcs::jj::testing::provisioned_jj() {
            Some(sidecar) => assert_eq!(
                std::path::Path::new(&orch.jj_binary_path),
                sidecar.as_path(),
                "a test orchestrator must drive the build slot's provisioned jj, not PATH's"
            ),
            None => assert_eq!(
                orch.jj_binary_path, "jj",
                "with no provisioned sidecar, PATH is the documented fallback"
            ),
        }
    }

    /// A snapshot shaped like a healthy remote: every reading its platform can
    /// take, taken.
    fn healthy_non_macos_executor() -> cairn_common::executor_protocol::ExecutorHealthSnapshot {
        use cairn_common::executor_protocol::{
            AdmissionHealth, CpuPressure, DiskAccounting, DiskHealth, DiskHealthStatus,
            ExecutorAdvertisement, ExecutorCapabilities, ExecutorHealthSnapshot,
            ExecutorHealthStatus, ExecutorIdentity, ExecutorRuntimePolicy, HostHealth,
            MachineMemory, MachineTelemetry, MachineVolume, Measurement, MeasurementGap,
            ProcessTelemetry,
        };

        let identity = ExecutorIdentity {
            device_id: "device".into(),
            executor_id: "bglab-win".into(),
            display_name: "bglab-win".into(),
        };
        ExecutorHealthSnapshot {
            build_volume: None,
            identity: identity.clone(),
            public_name: "bglab-win".into(),
            colocated: false,
            status: ExecutorHealthStatus::Online,
            heartbeat_age_ms: 0,
            liveness_age_ms: Some(0),
            telemetry_stale: false,
            advertisement: ExecutorAdvertisement {
                identity,
                capabilities: ExecutorCapabilities {
                    os: "windows".into(),
                    arch: "x86_64".into(),
                    logical_cores: 16,
                    concurrency_capacity: None,
                    toolchains: Vec::new(),
                    projects_served: Vec::new(),
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
                liveness_observed_at_unix_ms: Some(0),
                facts_stale_refresh_blocked: None,
                last_refresh: None,
            },
            admission: AdmissionHealth::default(),
            queues: Vec::new(),
            host: HostHealth::default(),
            disk: DiskHealth {
                status: DiskHealthStatus::Ok,
                ..DiskHealth::default()
            },
            machine: MachineTelemetry {
                build_volume: None,
                build_disk_accounting: None,
                cpu: Measurement::measured(
                    10,
                    CpuPressure {
                        utilization: 0.4,
                        user: 0.3,
                        system: 0.1,
                        logical_cores: 16,
                    },
                ),
                memory: Measurement::measured(
                    10,
                    MachineMemory {
                        total_bytes: 64_000,
                        available_bytes: 40_000,
                    },
                ),
                volume: Measurement::measured(
                    10,
                    MachineVolume {
                        total_bytes: 900_000,
                        free_bytes: 400_000,
                    },
                ),
                disk_accounting: Measurement::measured(10, DiskAccounting::default()),
                process: ProcessTelemetry {
                    // Windows working set: taken. macOS physical footprint: no
                    // equivalent exists here, and never will.
                    resident_bytes: Measurement::measured(10, 17_000),
                    physical_footprint_bytes: Measurement::unavailable(
                        10,
                        MeasurementGap::UnsupportedPlatform,
                    ),
                    virtual_bytes: Measurement::unavailable(
                        10,
                        MeasurementGap::UnsupportedPlatform,
                    ),
                    // Allocator bookkeeping is read only from glibc's
                    // `mallinfo2`; every other platform names the gap.
                    allocator: Measurement::unavailable(10, MeasurementGap::UnsupportedPlatform),
                    // Every channel is spelled out rather than defaulted: a
                    // defaulted channel reads `NotSampled`, which a running
                    // executor never reports, so a new channel must be placed
                    // here deliberately.
                    retained: Default::default(),
                },
            },
            inventory: Default::default(),
            connection_generation: 1,
            applied_policy: ExecutorRuntimePolicy::default(),
            drain_mode: false,
            role: Default::default(),
            resident_processes: Default::default(),
            command_processes: Default::default(),
            evicted_cells: Vec::new(),
            busy_declined_cells: Vec::new(),
            repository_faults: Vec::new(),
            quarantines: None,
            build_skew: None,
            stopped_commands: Vec::new(),
        }
    }

    /// A machine whose only missing reading is one it was never going to have.
    ///
    /// macOS physical footprint has no equivalent anywhere else, Windows has no
    /// single quantity that means mapped address space, and allocator
    /// bookkeeping is read only from glibc, so a healthy Windows executor
    /// reports all three unavailable forever. Counting any of them as a reason made a completely healthy remote hold the entire
    /// fleet at `Unknown` permanently, and conflated the daemon's own size with
    /// the machine pressure placement actually reads.
    #[test]
    fn a_healthy_non_macos_executor_stays_healthy_despite_its_unsupported_footprint() {
        use cairn_common::executor_protocol::{
            MachineMeasurement, MeasurementGap, SubstrateHealthStatus,
        };

        let executor = healthy_non_macos_executor();
        // The gaps are real and still visible on the machine itself — they are
        // only barred from the fleet-wide verdict.
        assert_eq!(
            executor.machine.gaps(),
            vec![
                (
                    MachineMeasurement::ProcessPhysicalFootprint,
                    MeasurementGap::UnsupportedPlatform
                ),
                (
                    MachineMeasurement::ProcessVirtual,
                    MeasurementGap::UnsupportedPlatform
                ),
                (
                    MachineMeasurement::ProcessAllocator,
                    MeasurementGap::UnsupportedPlatform
                )
            ]
        );
        assert_eq!(executor.machine.placement_gaps(), vec![]);

        let reasons = executor_health_reasons(&executor);
        assert_eq!(
            reasons,
            vec![],
            "a healthy machine must say nothing; a reason a working platform \
             emits forever makes the whole vocabulary unreadable"
        );
        assert_eq!(
            substrate_health_status(&reasons),
            SubstrateHealthStatus::Healthy
        );
    }

    /// The diagnostic readings are excluded from the verdict, not from view, and
    /// a placement input that goes missing still counts.
    #[test]
    fn a_missing_placement_input_still_reaches_the_verdict() {
        use cairn_common::executor_protocol::{
            MachineMeasurement, Measurement, MeasurementGap, SubstrateHealthReason,
            SubstrateHealthStatus,
        };

        let mut executor = healthy_non_macos_executor();
        executor.machine.memory = Measurement::unavailable(10, MeasurementGap::SamplingFailed);
        // A failing categorized walk is diagnosis, not a placement input: the
        // executor admits work against free volume bytes, never against the
        // storage breakdown.
        executor.machine.disk_accounting =
            Measurement::unavailable(10, MeasurementGap::SamplingFailed);

        let reasons = executor_health_reasons(&executor);
        assert_eq!(
            reasons,
            vec![SubstrateHealthReason::MeasurementUnavailable {
                executor_id: "bglab-win".into(),
                measurement: MachineMeasurement::Memory,
                reason: MeasurementGap::SamplingFailed,
            }]
        );
        assert_eq!(
            substrate_health_status(&reasons),
            SubstrateHealthStatus::Unknown
        );
    }

    #[test]
    fn substrate_health_status_precedence_reaches_unknown() {
        use cairn_common::executor_protocol::{
            MachineMeasurement, MeasurementGap, SkippedEntry, SkippedEntryOperation,
            SkippedEntryReason, SubstrateHealthReason, SubstrateHealthStatus,
        };

        assert_eq!(substrate_health_status(&[]), SubstrateHealthStatus::Healthy);
        assert_eq!(
            substrate_health_status(&[SubstrateHealthReason::MeasurementUnavailable {
                executor_id: "one".into(),
                measurement: MachineMeasurement::Volume,
                reason: MeasurementGap::SamplingFailed,
            }]),
            SubstrateHealthStatus::Unknown
        );
        assert_eq!(
            substrate_health_status(&[
                SubstrateHealthReason::MeasurementUnavailable {
                    executor_id: "one".into(),
                    measurement: MachineMeasurement::Volume,
                    reason: MeasurementGap::SamplingFailed,
                },
                SubstrateHealthReason::StaleExecutor {
                    executor_id: "one".into(),
                },
            ]),
            SubstrateHealthStatus::Degraded
        );
        assert_eq!(
            substrate_health_status(&[SubstrateHealthReason::DiskAccountingPartial {
                executor_id: "one".into(),
                skipped_entries: 2,
                skipped: vec![SkippedEntry {
                    path: "build-slots/slot-3".into(),
                    operation: SkippedEntryOperation::ReadDirectory,
                    reason: SkippedEntryReason::PermissionDenied,
                }],
            }]),
            SubstrateHealthStatus::Degraded
        );
        assert_eq!(
            substrate_health_status(&[SubstrateHealthReason::StaleTelemetry {
                executor_id: "one".into(),
                age_ms: 600_000,
            }]),
            SubstrateHealthStatus::Degraded,
            "aged facts degrade the snapshot; they do not make it unknown, because \
             the facts are still there"
        );
        assert_eq!(
            substrate_health_status(&[SubstrateHealthReason::NoExecutors]),
            SubstrateHealthStatus::Blocked
        );
    }

    /// `build()` must leave nothing running that holds the orchestrator: once the
    /// caller drops it, every sender it owned is gone and the channel closes.
    #[tokio::test]
    async fn build_spawns_nothing_that_outlives_the_orchestrator() {
        let db = test_db().await;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let orch = Orchestrator::builder(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .browser_command_tx(Some(tx))
        .build();
        drop(orch);
        let closed = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv()).await;
        assert!(
            matches!(closed, Ok(None)),
            "browser command channel stayed open after drop(orch): build() spawned a task holding an Orchestrator clone"
        );
    }

    #[tokio::test]
    async fn store_phase_trace_is_correlated_bounded_and_does_not_retain_guard() {
        let db = test_db().await;
        let config_dir = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .build();
        let store = config_dir.join("jj-stores").join("phase-test");

        let guard = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "phase operation",
            )
            .await;
        let trace = guard.trace();
        let correlation = trace.correlation_id.clone();
        let phase = trace.phase("local mutation");
        drop(guard);

        // A phase may deliberately outlive the lock (push/publication). Keeping
        // its timer alive must not keep the mutex locked.
        orch.acquire_jj_store_lock_with_timeout(
            &store,
            crate::orchestrator::StoreLockClass::Foreground,
            "phase overlap probe",
            Some(std::time::Duration::from_millis(100)),
        )
        .await
        .expect("phase instrumentation does not retain the guard");
        drop(phase);

        for _ in 0..(JJ_STORE_LOCK_SAMPLE_CAPACITY + 20) {
            drop(trace.phase("local mutation"));
        }

        let all = orch.jj_store_lock_metrics.lock().unwrap();
        let metrics = all.get(store.to_string_lossy().as_ref()).unwrap();
        let samples = metrics
            .phase_ms
            .get("phase operation/local mutation")
            .unwrap();
        assert_eq!(samples.len(), JJ_STORE_LOCK_SAMPLE_CAPACITY);
        assert!(!correlation.is_empty());
    }

    #[tokio::test]
    async fn aborted_store_lock_waiter_releases_registration() {
        let db = test_db().await;
        let config_dir = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .build();
        let store = config_dir.join("jj-stores").join("abort-test");
        let held = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "abort test holder",
            )
            .await;
        let waiting_orch = orch.clone();
        let waiting_store = store.clone();
        let waiter = tokio::spawn(async move {
            waiting_orch
                .acquire_jj_store_lock_with_timeout(
                    &waiting_store,
                    crate::orchestrator::StoreLockClass::Foreground,
                    "aborted waiter",
                    None,
                )
                .await
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let waiter_count = orch
                    .substrate_health_snapshot()
                    .store_locks
                    .iter()
                    .find(|lock| lock.store == store.to_string_lossy())
                    .map_or(0, |lock| lock.waiter_count);
                if waiter_count == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the blocked acquisition registers as a waiter");

        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            orch.substrate_health_snapshot()
                .store_locks
                .iter()
                .find(|lock| lock.store == store.to_string_lossy())
                .unwrap()
                .waiter_count,
            0
        );
        drop(held);
    }

    #[tokio::test]
    async fn a_waiting_branch_preparation_reports_its_holder_and_queue_position() {
        let db = test_db().await;
        let config_dir = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .build();
        let store = config_dir.join("jj-stores").join("preparation-wait");
        let held = orch
            .acquire_jj_store_lock(
                &store,
                StoreLockClass::Background,
                "sibling reconcile (test): feature",
            )
            .await;
        assert!(orch.branch_preparation_wait("job-1").is_none());

        let waiting_orch = orch.clone();
        let waiting_store = store.clone();
        let preparation = tokio::spawn(async move {
            drop(
                waiting_orch
                    .acquire_jj_store_lock(
                        &waiting_store,
                        StoreLockClass::Foreground,
                        branch_preparation_operation("job-1"),
                    )
                    .await,
            );
        });
        let (status, position, _) =
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    if let Some(wait) = orch.branch_preparation_wait("job-1") {
                        break wait;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("the queued preparation is visible");
        assert_eq!(position, 0);
        assert_eq!(
            status
                .holder
                .as_ref()
                .map(|(operation, _)| operation.as_str()),
            Some("sibling reconcile (test): feature")
        );

        drop(held);
        preparation.await.unwrap();
        assert!(orch.branch_preparation_wait("job-1").is_none());
    }

    #[tokio::test]
    async fn store_lock_timeout_returns_without_acquiring_and_releases_waiter() {
        let db = test_db().await;
        let config_dir = tempfile::tempdir().unwrap().keep();
        let orch = Orchestrator::builder(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .build();
        let store = config_dir.join("jj-stores").join("timeout-test");
        let held = orch
            .acquire_jj_store_lock(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "timeout test holder",
            )
            .await;

        let result = orch
            .acquire_jj_store_lock_with_timeout(
                &store,
                crate::orchestrator::StoreLockClass::Foreground,
                "timeout test waiter",
                Some(std::time::Duration::from_millis(10)),
            )
            .await;
        assert!(result.is_err(), "a bounded waiter must time out");

        drop(held);
        orch.acquire_jj_store_lock_with_timeout(
            &store,
            crate::orchestrator::StoreLockClass::Foreground,
            "timeout test retry",
            Some(std::time::Duration::from_millis(100)),
        )
        .await
        .expect("the lock remains usable after a timed-out waiter");

        let snapshot = orch.substrate_health_snapshot();
        let lock = snapshot
            .store_locks
            .iter()
            .find(|lock| lock.store == store.to_string_lossy())
            .expect("the per-store lock metrics remain queryable");
        assert_eq!(lock.waiter_count, 0);
        assert!(lock.waits.sample_count >= 3);
        assert!(lock.waits.max_ms.is_some_and(|wait| wait >= 10));
        assert!(lock.holds.sample_count >= 1);
        assert!(lock.waits.sample_count <= JJ_STORE_LOCK_SAMPLE_CAPACITY as u64);
        assert!(lock.holds.sample_count <= JJ_STORE_LOCK_SAMPLE_CAPACITY as u64);
    }

    #[tokio::test]
    async fn review_request_ids_survive_settlement_until_the_next_wave() {
        let orch = Orchestrator::builder(
            test_db().await,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();

        assert!(orch.try_begin_turn_end_checks("job-1").is_some());
        orch.set_turn_end_check_request_id(
            "job-1",
            "rust-tests".to_string(),
            "request-1".to_string(),
        );
        orch.end_turn_end_checks("job-1");
        assert_eq!(
            orch.recent_turn_end_check_request_ids("job-1")
                .get("rust-tests")
                .map(String::as_str),
            Some("request-1")
        );

        assert!(orch.try_begin_turn_end_checks("job-1").is_some());
        assert!(orch.recent_turn_end_check_request_ids("job-1").is_empty());
    }

    async fn insert_account_jwt(db: &DbState, jwt: &str) {
        let enc = encrypt_jwt_for_storage(jwt).unwrap();
        db.local
            .write(|conn| {
                let enc = enc.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO account (user_id, email, name, device_id, plan,
                             jwt_encrypted, jwt_expires_at, org_memberships, connected_at, updated_at)
                         VALUES ('u1','a@b.com','A','dev','free', ?1, ?2, NULL, 0, 0)",
                        (enc.as_str(), chrono::Utc::now().timestamp() + 3600),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn insert_anon_jwt(db: &DbState, jwt: &str) {
        let enc = encrypt_jwt_for_storage(jwt).unwrap();
        db.local
            .write(|conn| {
                let enc = enc.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO anon_device (device_id, jwt_encrypted, jwt_expires_at,
                             created_at, updated_at)
                         VALUES ('anon-dev', ?1, ?2, 0, 0)",
                        (enc.as_str(), chrono::Utc::now().timestamp() + 3600),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    async fn insert_expired_account_jwt(db: &DbState, jwt: &str) {
        let enc = encrypt_jwt_for_storage(jwt).unwrap();
        db.local
            .write(|conn| {
                let enc = enc.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO account (user_id, email, name, device_id, plan,
                             jwt_encrypted, jwt_expires_at, org_memberships, connected_at, updated_at)
                         VALUES ('u1','a@b.com','A','dev','free', ?1, ?2, NULL, 0, 0)",
                        (enc.as_str(), chrono::Utc::now().timestamp() - 10),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    fn managers(db: Arc<DbState>) -> (Arc<AccountManager>, Arc<AnonDeviceManager>) {
        let account = Arc::new(AccountManager::new(
            db.clone(),
            Arc::new(CapturingEmitter::new()),
        ));
        let anon = Arc::new(AnonDeviceManager::new(db, ApiConfig::default()));
        (account, anon)
    }

    #[derive(Clone, Default)]
    struct RecordingEmitter {
        events: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    }

    impl EventEmitter for RecordingEmitter {
        fn emit(&self, event: &str, payload: serde_json::Value) -> Result<(), String> {
            self.events
                .lock()
                .unwrap()
                .push((event.to_string(), payload));
            Ok(())
        }

        fn emit_empty(&self, event: &str) -> Result<(), String> {
            self.emit(event, serde_json::Value::Null)
        }
    }

    #[tokio::test]
    async fn context_token_snapshot_round_trips_and_emits() {
        let db = test_db().await;
        let emitter = RecordingEmitter::default();
        let captured_events = emitter.events.clone();
        let services = Arc::new(TestServicesBuilder::new().with_emitter(emitter).build());
        let orch =
            OrchestratorBuilder::new(db, services, tempfile::tempdir().unwrap().keep()).build();
        let state = ContextTokenState {
            run_id: "run-1".to_string(),
            session_id: Some("session-1".to_string()),
            backend: "codex".to_string(),
            model: Some("gpt-5".to_string()),
            used_tokens: 18_676,
            context_window: Some(258_400),
            auto_compact_limit: None,
            reasoning_tokens: Some(0),
            last_output_tokens: Some(5),
            captured_at: 123,
        };

        orch.store_context_token_snapshot(state.clone());

        assert_eq!(
            orch.get_context_token_state("session-1").await,
            Some(state.clone())
        );
        let payloads: Vec<_> = captured_events
            .lock()
            .unwrap()
            .iter()
            .filter(|(event, _)| event == "context-tokens-updated")
            .map(|(_, payload)| payload.clone())
            .collect();
        assert_eq!(payloads.len(), 1);
        assert_eq!(payloads[0]["sessionId"], "session-1");
        assert_eq!(payloads[0]["state"]["usedTokens"], 18_676);
    }

    #[tokio::test]
    async fn codex_context_token_state_does_not_fall_back_to_turn_sum() {
        let db = test_db().await;
        db.local
            .execute_script(
                "
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('project-ctx', 'default', 'Context Project', 'CTX', '/tmp/ctx', 1, 1);
                INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
                 VALUES ('job-ctx', 'project-ctx', 'running', 'gpt-5', 1, 1);
                INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('session-ctx', 'job-ctx', 'codex', 'open', 1, 1, 1);
                INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES ('run-ctx', 'project-ctx', 'job-ctx', 'exited', 'session-ctx', 1, 1);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('event-old', 'run-ctx', 'session-ctx', 1, 10, 'result:success', '{}',
                    NULL, 10, 100, 999, NULL, 25, 3);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('event-latest', 'run-ctx', 'session-ctx', 2, 20, 'result:success', '{}',
                    NULL, 20, 200, 999, NULL, 50, 7);
                ",
            )
            .await
            .unwrap();

        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        orch.model_catalog.write().unwrap().insert(
            "codex".to_string(),
            ProviderModelCatalog {
                backend: "codex".to_string(),
                models: vec![DiscoveredModel {
                    id: "gpt-5".to_string(),
                    model: "gpt-5".to_string(),
                    display_name: "GPT-5".to_string(),
                    description: None,
                    hidden: false,
                    is_default: true,
                    default_reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    context_window: Some(258_400),
                    canonical_slug: None,
                    serving_account_ids: Vec::new(),
                    pricing: None,
                    supported_parameters: Vec::new(),
                    router: false,
                    modalities: None,
                    serves: None,
                    tokenizer: None,
                }],
                options: Vec::new(),
                refreshed_at: Some(20),
                error: None,
            },
        );

        // Codex result rows now contain the sum of every model response in the
        // turn, not context occupancy. After restart the gauge remains empty
        // until the next live token notification supplies a real snapshot.
        assert!(orch.get_context_token_state("session-ctx").await.is_none());
    }

    #[tokio::test]
    async fn claude_context_token_state_derives_full_model_id_from_event_data() {
        let db = test_db().await;
        db.local
            .execute_script(
                "
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('project-claude-ctx', 'default', 'Claude Context Project', 'CLCTX', '/tmp/clctx', 1, 1);
                INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
                 VALUES ('job-claude-ctx', 'project-claude-ctx', 'running', NULL, 1, 1);
                INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('session-claude-ctx', 'job-claude-ctx', 'claude', 'open', 1, 1, 1);
                INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES ('run-claude-ctx', 'project-claude-ctx', 'job-claude-ctx', 'exited', 'session-claude-ctx', 1, 1);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('event-assistant-final', 'run-claude-ctx', 'session-claude-ctx', 10, 10, 'assistant',
                    json_object('raw', json_object('message', json_object('model', 'claude-opus-5-5'))),
                    NULL, 10, 7, 31468, 6200, 38, 12);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('event-result-cumulative', 'run-claude-ctx', 'session-claude-ctx', 11, 11, 'result:success', '{}',
                    NULL, 11, 7, 118888, 37711, 439, NULL);
                ",
            )
            .await
            .unwrap();

        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();

        let state = orch
            .get_context_token_state("session-claude-ctx")
            .await
            .unwrap();
        assert_eq!(state.run_id, "run-claude-ctx");
        assert_eq!(state.backend, "claude");
        assert_eq!(state.model, Some("claude-opus-5-5".to_string()));
        assert_eq!(state.context_window, Some(1_000_000));
        assert_eq!(state.used_tokens, 37_713);
        assert_ne!(state.used_tokens, 157_045);
        assert_eq!(state.reasoning_tokens, Some(12));
        assert_eq!(state.last_output_tokens, Some(38));
        assert_eq!(state.captured_at, 10);
    }

    /// The `(used, window)` reading `session_occupancy` hands the continuation's
    /// capacity trigger: no window, no reading.
    fn occupancy_of(state: &ContextTokenState) -> Option<(i64, i64)> {
        Some((state.used_tokens, state.context_window?))
    }

    async fn insert_model_less_claude_session(db: &DbState, prefix: &str, init_data: Option<&str>) {
        let init = init_data
            .map(|data| {
                format!(
                    "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                        parent_tool_use_id, created_at)
                     VALUES ('{prefix}-init', '{prefix}-run', '{prefix}-session', 1, 5, 'system:init',
                        '{data}', NULL, 5);"
                )
            })
            .unwrap_or_default();
        db.local
            .execute_script(&format!(
                "
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('{prefix}-project', 'default', 'Model-less', '{prefix}', '/tmp/{prefix}', 1, 1);
                INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
                 VALUES ('{prefix}-job', '{prefix}-project', 'running', NULL, 1, 1);
                INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('{prefix}-session', '{prefix}-job', 'CLAUDE', 'open', 1, 1, 1);
                INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES ('{prefix}-run', '{prefix}-project', '{prefix}-job', 'exited', '{prefix}-session', 1, 1);
                {init}
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('{prefix}-assistant', '{prefix}-run', '{prefix}-session', 2, 10, 'assistant', '{{}}',
                    NULL, 10, 4, 150000, 19000, 996, NULL);
                "
            ))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn claude_context_token_state_falls_back_to_the_init_reported_model() {
        use cairn_models::continuation::window_pressure_reached;
        let db = test_db().await;
        insert_model_less_claude_session(
            &db,
            "CLINIT",
            Some(r#"{"raw":{"model":"claude-opus-5-5"}}"#),
        )
        .await;
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();

        let state = orch
            .get_context_token_state("CLINIT-session")
            .await
            .unwrap();
        assert_eq!(state.model.as_deref(), Some("claude-opus-5-5"));
        assert_eq!(state.context_window, Some(1_000_000));
        assert_eq!(state.used_tokens, 170_000);
        // 170k of a 1M window is nowhere near the compaction threshold.
        assert!(!window_pressure_reached(occupancy_of(&state), 0.8));
    }

    /// A Claude session whose job selected `job_model`, which its CLI resolved
    /// to `init_model` at init, with its latest inference `used` tokens full and,
    /// when given, a result whose `modelUsage` is `model_usage`.
    async fn insert_claude_session(
        db: &DbState,
        prefix: &str,
        job_model: &str,
        init_model: &str,
        model_usage: Option<&str>,
        used: i64,
    ) {
        let result = model_usage
            .map(|usage| {
                format!(
                    "INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                        parent_tool_use_id, created_at)
                     VALUES ('{prefix}-result', '{prefix}-run', '{prefix}-session', 3, 11,
                        'result:success', '{{\"raw\":{{\"modelUsage\":{usage}}}}}', NULL, 11);"
                )
            })
            .unwrap_or_default();
        db.local
            .execute_script(&format!(
                "
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('{prefix}-project', 'default', 'Claude', '{prefix}', '/tmp/{prefix}', 1, 1);
                INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
                 VALUES ('{prefix}-job', '{prefix}-project', 'running', '{job_model}', 1, 1);
                INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('{prefix}-session', '{prefix}-job', 'CLAUDE', 'open', 1, 1, 1);
                INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES ('{prefix}-run', '{prefix}-project', '{prefix}-job', 'exited', '{prefix}-session', 1, 1);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at)
                 VALUES ('{prefix}-init', '{prefix}-run', '{prefix}-session', 1, 5, 'system:init',
                    '{{\"raw\":{{\"model\":\"{init_model}\"}}}}', NULL, 5);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('{prefix}-assistant', '{prefix}-run', '{prefix}-session', 2, 10, 'assistant', '{{}}',
                    NULL, 10, 0, {used}, 0, 0, NULL);
                {result}
                "
            ))
            .await
            .unwrap();
    }

    async fn claude_state(db: Arc<DbState>, session_id: &str) -> ContextTokenState {
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        orch.get_context_token_state(session_id).await.unwrap()
    }

    /// The default `md` tier selects `sonnet`, which CLI 2.1.302 runs as Sonnet
    /// 5.5 with a 1M window. Read as the 200k alias, 160k already crossed the
    /// 0.8 compaction threshold; read through the model the CLI resolved, it is
    /// a sixth of the window.
    #[tokio::test]
    async fn a_default_tier_sonnet_5_5_session_is_not_compacted_at_160k() {
        use cairn_models::continuation::window_pressure_reached;
        let db = test_db().await;
        insert_claude_session(&db, "SON55", "sonnet", "claude-sonnet-5-5", None, 160_000).await;

        let state = claude_state(db, "SON55-session").await;
        assert_eq!(state.model.as_deref(), Some("sonnet"));
        assert_eq!(state.context_window, Some(1_000_000));
        assert_eq!(state.used_tokens, 160_000);
        assert!(!window_pressure_reached(occupancy_of(&state), 0.8));
        assert!(window_pressure_reached(Some((160_000, 200_000)), 0.8));
    }

    #[tokio::test]
    async fn a_haiku_5_5_session_reads_the_1m_window_its_cli_reports() {
        use cairn_models::continuation::window_pressure_reached;
        let db = test_db().await;
        insert_claude_session(
            &db,
            "HAI55",
            "haiku",
            "claude-haiku-5-5",
            Some(r#"{"claude-haiku-5-5":{"contextWindow":1000000}}"#),
            170_000,
        )
        .await;

        let state = claude_state(db, "HAI55-session").await;
        assert_eq!(state.context_window, Some(1_000_000));
        assert!(!window_pressure_reached(occupancy_of(&state), 0.8));
    }

    /// CLI 2.1.220 resolves `haiku` to Haiku 4.5, whose window really is 200k:
    /// that session must still compact before it overruns.
    #[tokio::test]
    async fn an_older_clis_haiku_4_5_session_keeps_its_200k_window() {
        use cairn_models::continuation::window_pressure_reached;
        let db = test_db().await;
        insert_claude_session(
            &db,
            "HAI45",
            "haiku",
            "claude-haiku-4-5-20251001",
            Some(r#"{"claude-haiku-4-5-20251001":{"contextWindow":200000}}"#),
            170_000,
        )
        .await;

        let state = claude_state(db, "HAI45-session").await;
        assert_eq!(state.context_window, Some(200_000));
        assert!(window_pressure_reached(occupancy_of(&state), 0.8));
    }

    /// The table reads `claude-sonnet-5` as 200k because one CLI sizes it so;
    /// where the session's own CLI reported 1M, that report decides.
    #[tokio::test]
    async fn the_window_the_cli_reported_outranks_the_table() {
        let db = test_db().await;
        insert_claude_session(
            &db,
            "SON5",
            "sonnet",
            "claude-sonnet-5",
            Some(r#"{"claude-sonnet-5":{"contextWindow":1000000}}"#),
            170_000,
        )
        .await;

        let state = claude_state(db, "SON5-session").await;
        assert_eq!(state.context_window, Some(1_000_000));
    }

    #[tokio::test]
    async fn claude_context_token_state_without_any_model_has_no_window() {
        use cairn_models::continuation::window_pressure_reached;
        let db = test_db().await;
        insert_model_less_claude_session(&db, "CLNONE", None).await;
        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();

        let state = orch
            .get_context_token_state("CLNONE-session")
            .await
            .unwrap();
        assert_eq!(state.model, None);
        assert_eq!(state.used_tokens, 170_000);
        // No model means no known window: the gauge shows a bare count and the
        // capacity trigger has no reading, so it never rebuilds the thread.
        assert_eq!(state.context_window, None);
        assert_eq!(occupancy_of(&state), None);
        assert!(!window_pressure_reached(occupancy_of(&state), 0.8));
    }

    #[tokio::test]
    async fn openrouter_context_window_sourced_from_catalog_not_hardcoded() {
        let db = test_db().await;
        db.local
            .execute_script(
                "
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('project-or', 'default', 'OR Project', 'ORP', '/tmp/orp', 1, 1);
                INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
                 VALUES ('job-or', 'project-or', 'running', 'anthropic/claude-sonnet-4.5', 1, 1);
                INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('session-or', 'job-or', 'openrouter', 'open', 1, 1, 1);
                INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES ('run-or', 'project-or', 'job-or', 'exited', 'session-or', 1, 1);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('event-or', 'run-or', 'session-or', 1, 10, 'assistant', '{}',
                    NULL, 10, 10, 0, 0, 40, 5);
                ",
            )
            .await
            .unwrap();

        let orch = OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build();
        orch.model_catalog.write().unwrap().insert(
            "openrouter".to_string(),
            ProviderModelCatalog {
                backend: "openrouter".to_string(),
                models: vec![DiscoveredModel {
                    id: "anthropic/claude-sonnet-4.5".to_string(),
                    model: "anthropic/claude-sonnet-4.5".to_string(),
                    display_name: "Claude Sonnet 4.5".to_string(),
                    description: None,
                    hidden: false,
                    is_default: false,
                    default_reasoning_effort: None,
                    supported_reasoning_efforts: Vec::new(),
                    context_window: Some(200_000),
                    canonical_slug: None,
                    serving_account_ids: Vec::new(),
                    pricing: None,
                    supported_parameters: Vec::new(),
                    router: false,
                    modalities: None,
                    serves: None,
                    tokenizer: None,
                }],
                options: Vec::new(),
                refreshed_at: Some(10),
                error: None,
            },
        );

        let state = orch.get_context_token_state("session-or").await.unwrap();
        assert_eq!(state.backend, "openrouter");
        assert_eq!(state.model, Some("anthropic/claude-sonnet-4.5".to_string()));
        // The selected model's real catalog window flows into ContextTokenState,
        // not a hardcoded 1M assumption.
        assert_eq!(state.context_window, Some(200_000));
        assert_ne!(state.context_window, Some(1_000_000));
    }

    #[tokio::test]
    async fn embed_token_prefers_account_jwt() {
        let db = test_db().await;
        insert_account_jwt(&db, "account-jwt").await;
        insert_anon_jwt(&db, "anon-jwt").await;
        let (account, anon) = managers(db);
        assert_eq!(
            resolve_embed_token(&account, &anon),
            Some("account-jwt".to_string())
        );
    }

    #[tokio::test]
    async fn embed_token_falls_back_to_anon_when_no_account() {
        let db = test_db().await;
        insert_anon_jwt(&db, "anon-jwt").await;
        let (account, anon) = managers(db);
        assert_eq!(
            resolve_embed_token(&account, &anon),
            Some("anon-jwt".to_string())
        );
    }

    #[tokio::test]
    async fn embed_token_none_when_neither_present() {
        let db = test_db().await;
        let (account, anon) = managers(db);
        assert_eq!(resolve_embed_token(&account, &anon), None);
    }

    #[tokio::test]
    async fn embed_token_skips_expired_account_jwt_for_anon() {
        let db = test_db().await;
        insert_expired_account_jwt(&db, "stale-account-jwt").await;
        insert_anon_jwt(&db, "anon-jwt").await;
        let (account, anon) = managers(db);
        // An expired account JWT must not shadow the valid anon token — otherwise
        // embedding 401s and vibe colors silently stop.
        assert_eq!(
            resolve_embed_token(&account, &anon),
            Some("anon-jwt".to_string())
        );
    }

    #[tokio::test]
    async fn embed_token_none_when_account_expired_and_no_anon() {
        let db = test_db().await;
        insert_expired_account_jwt(&db, "stale-account-jwt").await;
        let (account, anon) = managers(db);
        assert_eq!(resolve_embed_token(&account, &anon), None);
    }

    // ── connect_account_teams: fail-soft fan-out ────────────────────────────

    /// Insert an account with the given org memberships (`org_id`, `org_name`)
    /// and, when `jwt` is `Some`, a valid device JWT. Populates the
    /// `org_memberships` JSON so `connect_account_teams` has teams to enumerate.
    async fn insert_account_teams(db: &DbState, jwt: Option<&str>, teams: &[(&str, &str)]) {
        let enc = jwt.map(|j| encrypt_jwt_for_storage(j).unwrap());
        let memberships: Vec<serde_json::Value> = teams
            .iter()
            .map(|(id, name)| serde_json::json!({"orgId": id, "orgName": name, "role": "member"}))
            .collect();
        let memberships_json = serde_json::to_string(&memberships).unwrap();
        db.local
            .write(|conn| {
                let enc = enc.clone();
                let memberships_json = memberships_json.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO account (user_id, email, name, device_id, plan,
                             jwt_encrypted, jwt_expires_at, org_memberships, connected_at, updated_at)
                         VALUES ('u1','a@b.com','A','dev','free', ?1, ?2, ?3, 0, 0)",
                        (
                            enc.as_deref(),
                            chrono::Utc::now().timestamp() + 3600,
                            memberships_json.as_str(),
                        ),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    /// Build an orchestrator whose cloud API points at `base_url` (a mock).
    fn orch_with_api(db: Arc<DbState>, base_url: String) -> Orchestrator {
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db, services, tempfile::tempdir().unwrap().keep())
            .api_config(ApiConfig { base_url })
            .build()
    }

    /// Multi-connection mock: reply to each `/teams/<id>/sync-config` request
    /// with a status mapped by team id. Loops until the listener drops. Loopback
    /// only (fence-safe); std sockets on a background thread keep it independent
    /// of tokio's IO feature set.
    fn routing_sync_config_mock(
        routes: std::collections::HashMap<String, (&'static str, &'static str)>,
    ) -> String {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut sock) = stream else { break };
                let mut buf = [0u8; 4096];
                let n = sock.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                let (status_line, body) = routes
                    .iter()
                    .find(|(team_id, _)| req.contains(&format!("/teams/{team_id}/")))
                    .map(|(_, resp)| *resp)
                    .unwrap_or(("HTTP/1.1 404 Not Found", r#"{"error":"x"}"#));
                let response = format!(
                    "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(response.as_bytes());
                let _ = sock.flush();
            }
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn connect_account_teams_no_account_is_noop() {
        // Local-only install: no account row => no memberships => strict no-op.
        let db = test_db().await;
        let orch = orch_with_api(db.clone(), "http://127.0.0.1:1".to_string());
        let summary = orch.connect_account_teams().await;
        assert_eq!(
            summary,
            crate::account::ConnectAccountTeamsSummary::default()
        );
        assert_eq!(db.open_team_count().await, 0);
    }

    #[tokio::test]
    async fn connect_account_teams_without_jwt_reports_not_authenticated() {
        // Memberships present but no device JWT: every team maps to
        // NotAuthenticated with no network call, and nothing opens.
        let db = test_db().await;
        insert_account_teams(&db, None, &[("t1", "T1"), ("t2", "T2")]).await;
        let orch = orch_with_api(db.clone(), "http://127.0.0.1:1".to_string());
        let summary = orch.connect_account_teams().await;
        assert_eq!(summary.not_authenticated, 2);
        assert_eq!(summary.connected, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(db.open_team_count().await, 0);
    }

    #[tokio::test]
    async fn connect_account_teams_is_fail_soft_over_mixed_teams() {
        // A mix of provisioning (503) and not-configured (404) teams: the batch
        // never errors, maps each team to its status, and opens nothing.
        let db = test_db().await;
        insert_account_teams(&db, Some("jwt"), &[("t1", "T1"), ("t2", "T2")]).await;
        let mut routes = std::collections::HashMap::new();
        routes.insert(
            "t1".to_string(),
            ("HTTP/1.1 503 Service Unavailable", r#"{"error":"x"}"#),
        );
        routes.insert(
            "t2".to_string(),
            ("HTTP/1.1 404 Not Found", r#"{"error":"x"}"#),
        );
        let base = routing_sync_config_mock(routes);
        let orch = orch_with_api(db.clone(), base);
        let summary = orch.connect_account_teams().await;
        assert_eq!(summary.provisioning, 1);
        assert_eq!(summary.not_configured, 1);
        assert_eq!(summary.connected, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(db.open_team_count().await, 0);
    }

    #[tokio::test]
    async fn connect_account_teams_forgets_teams_the_account_left() {
        // A previously-connected team (registered + open replica + a route) that
        // is no longer in the account's memberships must be forgotten: its
        // replica closed and its registry row deleted so it neither serves reads
        // nor reopens at startup. This ties memberships_match's leave detection
        // to actually hiding the removed team.
        let db = test_db().await;
        // The account belongs to t1 only (t2 was left).
        insert_account_teams(&db, Some("jwt"), &[("t1", "T1")]).await;
        // t2 is registered and open from a prior membership.
        db.upsert_team_registry("t2", "T2", "http://broker/t2", "/tmp/t2.db")
            .await
            .unwrap();
        db.insert_team_db_for_test("t2", db.local.clone()).await;
        db.local
            .execute(
                "INSERT INTO project_routes(project_key, team_id, created_at) VALUES ('PROJ2', 't2', 0)",
                (),
            )
            .await
            .unwrap();
        db.set_route("PROJ2", Some("t2".to_string())).await;

        // t1's connect will fail (no reachable broker), which is irrelevant to
        // the subtractive leave path being validated here.
        let orch = orch_with_api(db.clone(), "http://127.0.0.1:1".to_string());
        let summary = orch.connect_account_teams().await;

        assert_eq!(summary.forgotten, 1, "the left team t2 must be forgotten");
        assert_eq!(db.open_team_count().await, 0, "t2's replica must be closed");
        assert!(
            db.registered_team_ids().await.unwrap().is_empty(),
            "t2 must be deregistered so it won't reopen at startup"
        );
    }

    #[tokio::test]
    async fn connect_account_teams_keeps_current_teams_registered() {
        // A registered team that IS still in the membership set is not forgotten
        // by the subtractive pass (guard against over-eager teardown).
        let db = test_db().await;
        insert_account_teams(&db, Some("jwt"), &[("t1", "T1")]).await;
        db.upsert_team_registry("t1", "T1", "http://broker/t1", "/tmp/t1.db")
            .await
            .unwrap();

        let orch = orch_with_api(db.clone(), "http://127.0.0.1:1".to_string());
        let summary = orch.connect_account_teams().await;

        assert_eq!(summary.forgotten, 0, "a current team must not be forgotten");
        assert_eq!(
            db.registered_team_ids().await.unwrap(),
            vec!["t1".to_string()],
            "t1 stays registered"
        );
    }

    #[tokio::test]
    async fn connect_account_teams_swallows_unreachable_team() {
        // An unreachable api (connection refused) makes connect_team error; the
        // batch counts it failed and still returns without erroring the whole run.
        let db = test_db().await;
        insert_account_teams(&db, Some("jwt"), &[("t1", "T1")]).await;
        // Reserve a port, then drop the listener so the connection is refused.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let orch = orch_with_api(db.clone(), format!("http://{addr}"));
        let summary = orch.connect_account_teams().await;
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.connected, 0);
        assert_eq!(db.open_team_count().await, 0);
    }
}
