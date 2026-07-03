//! Orchestrator — bundles all runtime state needed for agent execution.
//!
//! Both the Tauri desktop app and cairn-server create their own `Orchestrator`.
//! All orchestration functions take `&Orchestrator` instead of framework-specific
//! handles (e.g. `&AppHandle`).

pub mod account_manager;
pub mod agents;
pub mod attention;
pub mod attention_delivery;
pub mod attention_push;
pub mod base_advance;
pub mod build_services;
pub mod config_resource;
pub mod docs;
pub mod identity;
pub mod lifecycle;
pub mod parent_wake;
pub mod recipes;
pub mod session;
pub mod settings;
pub mod skills;
pub mod wakes;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use tokio::sync::broadcast;
use tokio::sync::Mutex as TokioMutex;

use crate::agent_process::process::AgentProcessState;
use crate::api::ApiConfig;
use crate::backends::context_window::{claude_context_window, ClaudeContextOptIn};
use crate::backends::{backend_for_name, DiscoveredModel, ProviderModelCatalog};
use crate::db::DbState;
use crate::effects::executor::EffectExecutor;
use crate::effects::types::WorkflowEffect;
use crate::embeddings::{
    spawn_embed_worker, EmbedJob, EmbeddingClient, PositionConfig, PositionKind, PositionMeta,
    VibeState,
};
use crate::identity::IdentityStore;
use crate::mcp::gateway::McpGateway;
use crate::mcp::McpAuthState;
use crate::models::{
    get_latest_context_token_event, ContextTokenState, ProviderUsageSnapshot, TriggerEvent,
};
use crate::notify::Notifier;
use crate::services::{ChildProcess, PtyState, Services};

pub use crate::account::AnonDeviceManager;
pub use account_manager::AccountManager;
pub use attention::{AttentionEvent, AttentionFact, AttentionFactKey};

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
    browser_bridge_responses: broadcast::Sender<(String, String)>,
    browser_nav_events: broadcast::Sender<crate::browsers::BrowserNavEvent>,
    trigger_events: broadcast::Sender<TriggerEvent>,
    attention_changed: broadcast::Sender<AttentionEvent>,
    session_allowed_tools: Arc<Mutex<HashSet<String>>>,
    session_allowed_crossings: Arc<Mutex<HashSet<String>>>,
    identity_store: Arc<Mutex<Option<IdentityStore>>>,
    mcp_binary_path: String,
    jj_binary_path: String,
    config_dir: PathBuf,
    schema_dir: Option<PathBuf>,
    mcp_callback_port: u16,
    vibe_state: Option<Arc<VibeState>>,
    account_manager: Arc<AccountManager>,
    notifier: Notifier,
    api_config: ApiConfig,
    effect_tx: Option<tokio::sync::mpsc::UnboundedSender<WorkflowEffect>>,
    browser_command_tx: Option<crate::browsers::BrowserCommandTx>,
    model_catalog: Arc<RwLock<HashMap<String, ProviderModelCatalog>>>,
    provider_usage_snapshots: Arc<RwLock<HashMap<String, ProviderUsageSnapshot>>>,
    context_token_snapshots: Arc<RwLock<HashMap<String, ContextTokenState>>>,
}

impl OrchestratorBuilder {
    pub fn new(db: Arc<DbState>, services: Arc<Services>, config_dir: PathBuf) -> Self {
        let process_state = Arc::new(AgentProcessState::default());
        let mcp_auth = Arc::new(McpAuthState::new(config_dir.clone()));
        let pty_state = Arc::new(PtyState::default());
        let permission_responses = broadcast::channel(16).0;
        let run_completions = broadcast::channel(64).0;
        let prompt_responses = broadcast::channel(16).0;
        let browser_bridge_responses = broadcast::channel(16).0;
        let browser_nav_events = broadcast::channel(64).0;
        let trigger_events = broadcast::channel(256).0;
        let attention_changed = broadcast::channel(64).0;
        let session_allowed_tools = Arc::new(Mutex::new(HashSet::new()));
        let session_allowed_crossings = Arc::new(Mutex::new(HashSet::new()));
        let identity_store = Arc::new(Mutex::new(None));
        let account_manager = Arc::new(AccountManager::new(db.clone(), services.emitter.clone()));
        let notifier = Notifier::new(services.emitter.clone());

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
            browser_bridge_responses,
            browser_nav_events,
            trigger_events,
            attention_changed,
            session_allowed_tools,
            session_allowed_crossings,
            identity_store,
            mcp_binary_path: "cairn-cmd".to_string(),
            jj_binary_path: "jj".to_string(),
            config_dir,
            schema_dir: None,
            mcp_callback_port: 0,
            vibe_state: None,
            account_manager,
            notifier,
            api_config: ApiConfig::default(),
            effect_tx: None,
            browser_command_tx: None,
            model_catalog: Arc::new(RwLock::new(HashMap::new())),
            provider_usage_snapshots: Arc::new(RwLock::new(HashMap::new())),
            context_token_snapshots: Arc::new(RwLock::new(HashMap::new())),
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

    pub fn attention_changed(
        mut self,
        attention_changed: broadcast::Sender<AttentionEvent>,
    ) -> Self {
        self.attention_changed = attention_changed;
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

    pub fn build(self) -> Orchestrator {
        // Install the process-wide rustls CryptoProvider at the single startup
        // chokepoint every sync-capable host passes through before it can open a
        // team replica. Turso's sync IO builds its hyper-rustls client the
        // instant the `turso-sync-io` thread spawns (inside
        // `turso::sync::Builder::build()`), so the provider must already be
        // installed by then — doing it lazily per-open races that thread spawn
        // (CAIRN-2196). Idempotent and `Once`-guarded; see
        // `storage::install_crypto_provider`.
        crate::storage::install_crypto_provider();

        let anon_device_manager = Arc::new(AnonDeviceManager::new(
            self.db.clone(),
            self.api_config.clone(),
        ));
        Orchestrator {
            db: self.db,
            services: self.services,
            process_state: self.process_state,
            mcp_auth: self.mcp_auth,
            warm_gc: self.warm_gc,
            pty_state: self.pty_state,
            worktree_search: Arc::new(crate::worktree_search::WorktreeSearchPool::default()),
            permission_responses: self.permission_responses,
            run_completions: self.run_completions,
            prompt_responses: self.prompt_responses,
            browser_bridge_responses: self.browser_bridge_responses,
            browser_nav_events: self.browser_nav_events,
            trigger_events: self.trigger_events,
            attention_changed: self.attention_changed,
            session_allowed_tools: self.session_allowed_tools,
            session_allowed_crossings: self.session_allowed_crossings,
            identity_store: self.identity_store,
            mcp_binary_path: self.mcp_binary_path,
            jj_binary_path: self.jj_binary_path,
            config_dir: self.config_dir,
            schema_dir: self.schema_dir,
            mcp_callback_port: self.mcp_callback_port,
            vibe_state: self.vibe_state,
            embed_tx: Arc::new(Mutex::new(None)),
            anon_device_manager,
            account_manager: self.account_manager,
            notifier: self.notifier,
            api_config: self.api_config,
            effect_tx: self.effect_tx,
            browser_command_tx: self.browser_command_tx,
            browser_loading: Arc::new(Mutex::new(HashMap::new())),
            executor: Arc::new(OnceLock::new()),
            mcp_gateway: Arc::new(OnceLock::new()),
            model_catalog: self.model_catalog,
            provider_usage_snapshots: self.provider_usage_snapshots,
            context_token_snapshots: self.context_token_snapshots,
            execution_locks: Arc::new(Mutex::new(HashMap::new())),
            jj_store_locks: Arc::new(Mutex::new(HashMap::new())),
            setup_registry: Arc::new(Mutex::new(HashMap::new())),
            build_service_children: Arc::new(Mutex::new(HashMap::new())),
            agent_completion_attention_dedupe: Arc::new(Mutex::new(HashSet::new())),
            turn_end_checks_in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

/// Cancel state for an in-flight worktree/setup preparation.
pub struct SetupHandle {
    pub cancel: Arc<AtomicBool>,
    pub child: Arc<Mutex<Option<Box<dyn ChildProcess>>>>,
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
    /// Warm process GC (optional — hosts may not enable warm processes)
    pub warm_gc: Option<Arc<crate::agent_process::gc::WarmProcessGC>>,
    /// Active PTY sessions (terminals)
    pub pty_state: Arc<PtyState>,
    /// Bounded pool of per-worktree warm search indexes (CAIRN-2303). Repeated
    /// `?grep=` reads in a worktree hit a resident fff index instead of
    /// re-walking the tree; cold/ineligible queries fall back to the ripgrep
    /// walk. Dropped per worktree at teardown and LRU-evicted at capacity.
    pub worktree_search: Arc<crate::worktree_search::WorktreeSearchPool>,

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
    pub session_allowed_tools: Arc<Mutex<HashSet<String>>>,

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
    pub identity_store: Arc<Mutex<Option<IdentityStore>>>,

    // === Host-specific paths (set by Tauri or cairn-server) ===
    /// Path to the cairn-cmd binary
    pub mcp_binary_path: String,
    /// Path to the jj binary (bundled sidecar; falls back to PATH `jj`).
    pub jj_binary_path: String,
    /// Directory for writing MCP config files
    pub config_dir: PathBuf,
    /// Job ids with an in-flight turn-end (`when:idle`/`when:review`) check run.
    /// Single-flight guard so a rapid re-idle does not stack suites, and the
    /// signal the PR-node / `/checks` render reads to show a "running" state.
    /// Runtime-only; never persisted.
    pub turn_end_checks_in_flight: Arc<Mutex<HashSet<String>>>,
    /// Directory containing bundled preset schemas (None if not available)
    pub schema_dir: Option<PathBuf>,
    /// Port for the MCP callback server
    pub mcp_callback_port: u16,
    /// Vibe state for embedding-based color assignment (None if centroids unavailable)
    pub vibe_state: Option<Arc<VibeState>>,
    /// Sender into the async event-embed worker. None until the worker is started.
    pub embed_tx: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::UnboundedSender<EmbedJob>>>>,

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
    pub browser_loading: Arc<Mutex<HashMap<String, bool>>>,

    /// Host-specific effect executor for `StartAgentJobs` and `ExecuteAction`.
    ///
    /// Wrapped in `Arc<OnceLock<...>>` so it's Clone-compatible (Orchestrator
    /// derives Clone) and settable after construction via `&self`. The executor
    /// may need a reference to the host's wrapper (e.g. `Arc<ServerState>`)
    /// which isn't available until after the Orchestrator is built.
    pub executor: Arc<OnceLock<Arc<dyn EffectExecutor>>>,

    /// Host-specific gateway to external MCP servers (the `cairn://mcp/...`
    /// family). `None` until a host sets it; `read`/`run` MCP dispatch returns a
    /// clear error when unset. Wrapped like `executor` so it's Clone-compatible
    /// and settable after construction via `&self`.
    pub mcp_gateway: Arc<OnceLock<Arc<dyn McpGateway>>>,

    /// Cached provider model catalog loaded at startup and refreshed on demand.
    pub model_catalog: Arc<RwLock<HashMap<String, ProviderModelCatalog>>>,
    /// Latest provider/account usage snapshots keyed by backend name.
    pub provider_usage_snapshots: Arc<RwLock<HashMap<String, ProviderUsageSnapshot>>>,
    /// Latest normalized context-token snapshots keyed by durable session id.
    pub context_token_snapshots: Arc<RwLock<HashMap<String, ContextTokenState>>>,

    /// Per-execution locks to serialize read-modify-write operations on snapshots.
    /// Prevents concurrent `persist_task_packet` calls from losing packets.
    pub execution_locks: Arc<Mutex<HashMap<String, Arc<TokioMutex<()>>>>>,

    /// Per-store locks serializing base-advance reconcile and merge-fold
    /// mutations on a shared jj project store, keyed by the store directory.
    /// Concurrent jj rebase/import ops on one store from forked operation logs
    /// mint divergent conflicted copies of the same change-id; this single-writer
    /// discipline closes that window. Keyed by store path (not execution/job) so
    /// reconciles from different executions on the same project store serialize.
    pub jj_store_locks: Arc<Mutex<HashMap<String, Arc<TokioMutex<()>>>>>,

    /// In-flight worktree/setup preparation handles, keyed by job id.
    pub setup_registry: Arc<Mutex<HashMap<String, SetupHandle>>>,

    /// Launcher handles for supervised Managed Build Service daemons, keyed by
    /// service name. Held so a foreground daemon can be stopped on shutdown; a
    /// daemon that detaches (e.g. an sccache server) outlives its launcher,
    /// which is acceptable for a shared cache. See `orchestrator::build_services`.
    pub build_service_children: Arc<Mutex<HashMap<String, Box<dyn ChildProcess>>>>,

    /// Per-run dedupe for legacy `agent-attention` terminal toasts.
    ///
    /// The completion toast now fires at the turn idle boundary while the same
    /// run may later EOF/finalize. Keying by run id suppresses cleanup-path
    /// duplicates and repeated crash finalization attempts.
    pub agent_completion_attention_dedupe: Arc<Mutex<HashSet<String>>>,
}

/// Resolve the `/embed` gateway token: account JWT preferred, anonymous device
/// JWT as fallback. An *expired* account JWT is treated as absent (see
/// `AccountManager::get_jwt`), so it falls through to the anonymous token rather
/// than shadowing it — keeping vibe coloring and the recommender alive when the
/// account token lapses. Single source of truth for `embed_token_provider`'s
/// precedence so it can be unit-tested without a full Orchestrator.
fn resolve_embed_token(account: &AccountManager, anon: &AnonDeviceManager) -> Option<String> {
    account
        .get_jwt()
        .ok()
        .flatten()
        .or_else(|| anon.get_anon_jwt().ok().flatten())
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
    pub fn is_browser_loading(&self, browser_id: &str) -> bool {
        self.browser_loading
            .lock()
            .ok()
            .and_then(|map| map.get(browser_id).copied())
            .unwrap_or(false)
    }

    /// Set the external MCP gateway after construction (mirrors `set_executor`).
    pub fn set_mcp_gateway(&self, gateway: Arc<dyn McpGateway>) -> Result<(), Arc<dyn McpGateway>> {
        self.mcp_gateway.set(gateway)
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

    /// Get or create a per-store lock serializing base-advance reconcile and
    /// merge-fold mutations on a shared jj project store. Acquire it once per
    /// logical operation at exactly one level — `TokioMutex` is not reentrant, so
    /// the inner reconcile helpers must never re-acquire it while it is held.
    pub fn jj_store_lock(&self, store_dir: &Path) -> Arc<TokioMutex<()>> {
        let key = store_dir.to_string_lossy().into_owned();
        let mut map = self.jj_store_locks.lock().unwrap();
        map.entry(key)
            .or_insert_with(|| Arc::new(TokioMutex::new(())))
            .clone()
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
    pub fn emit_attention_event(&self, event: AttentionEvent) {
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
        });
    }

    /// Start the async embed worker. Call once at startup, on a tokio runtime.
    /// Handles both event vibe coloring and corpus resource embedding through a
    /// single channel. Vibe coloring is skipped when axes are unavailable;
    /// resource embedding always runs (the gateway returns `Ok(None)` with no
    /// account, so starting unconditionally is safe). Jobs enqueued before this
    /// is called are dropped.
    pub fn start_embed_worker(&self) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let client = EmbeddingClient::new(self.api_config.clone(), self.embed_token_provider());
        spawn_embed_worker(
            rx,
            client,
            self.db.clone(),
            self.vibe_state.clone(),
            self.services.emitter.clone(),
        );
        if let Ok(mut guard) = self.embed_tx.lock() {
            *guard = Some(tx);
        }
    }

    /// Claim the turn-end-check single-flight slot for a job. Returns `true` when
    /// this call newly claimed it (no run was already in flight), `false` when a
    /// run is already active for the job — in which case the caller must NOT spawn
    /// another suite. The claim is released with [`Self::end_turn_end_checks`].
    pub fn try_begin_turn_end_checks(&self, job_id: &str) -> bool {
        self.turn_end_checks_in_flight
            .lock()
            .map(|mut set| set.insert(job_id.to_string()))
            .unwrap_or(false)
    }

    /// Release the turn-end-check single-flight slot for a job. Idempotent.
    pub fn end_turn_end_checks(&self, job_id: &str) {
        if let Ok(mut set) = self.turn_end_checks_in_flight.lock() {
            set.remove(job_id);
        }
    }

    /// Whether a turn-end check run is currently in flight for a job — the signal
    /// the PR-node / `/checks` render reads to show the "running" state.
    pub fn turn_end_checks_in_flight(&self, job_id: &str) -> bool {
        self.turn_end_checks_in_flight
            .lock()
            .map(|set| set.contains(job_id))
            .unwrap_or(false)
    }

    /// Token provider for the `/embed` gateway: prefers the connected account's
    /// JWT, falling back to the anonymous device JWT when logged out. Returns
    /// `None` only when neither is available (embedding then no-ops).
    pub fn embed_token_provider(&self) -> crate::embeddings::TokenProvider {
        let am = self.account_manager.clone();
        let anon = self.anon_device_manager.clone();
        Arc::new(move || resolve_embed_token(&am, &anon))
    }

    /// Spawn a background task that registers an anonymous device JWT for the
    /// `/embed` gateway and keeps it fresh. Registers immediately, then
    /// re-checks every ~12h (well inside the 30-day token TTL). Best-effort:
    /// failures leave embedding neutral until the next attempt. Idempotent at
    /// the API level (reuses the persisted device_id).
    ///
    /// Runs unconditionally, even when an account is connected: the account JWT
    /// takes precedence in `embed_token_provider`, but keeping the anon token
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

    /// Spawn the one-time archival backfill on a background task.
    ///
    /// Call once at startup, on a tokio runtime. Compresses the events of
    /// already-torn-down executions (history the teardown writer can never reach)
    /// into their durable archival form, gated by `archival_backfill_state` so it
    /// runs once, never on every startup; the work is detached so it never blocks
    /// startup or the UI. This frees pages to the freelist but does NOT shrink the
    /// database file: file-size reclamation is a separate one-time, offline step
    /// run via the `vacuum_reclaim` cargo example (see the `archival::backfill`
    /// module docs, docs/database.md, and CAIRN-1556).
    pub fn spawn_archival_maintenance(&self) {
        let db = self.db.local.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::archival::run_archival_maintenance(&db).await {
                log::warn!("archival maintenance failed: {e}");
            }
        });
    }

    /// Spawn the periodic worktree garbage collector: once shortly after startup
    /// (a short delay so it does not compete with launch), then every ~24h.
    ///
    /// Reclaims worktree disk that teardown never got to — worktrees of jobs whose
    /// issue reached a terminal state (the DB pass) and row-less filesystem debris
    /// plus leftover `*.trash-*` tombstones (the canonical-instance filesystem
    /// pass), both bounded by the `orphan_cleanup_days` age cutoff. Best-effort;
    /// errors log and never abort the loop. Must be called from within a tokio
    /// runtime. Both hosts (cairn-runner, cairn-server) spawn it at startup.
    pub fn spawn_worktree_gc(&self) {
        /// Delay before the first pass so it does not compete with launch.
        const STARTUP_DELAY: std::time::Duration = std::time::Duration::from_secs(120);
        /// Cadence after the startup pass.
        const INTERVAL: std::time::Duration = std::time::Duration::from_secs(24 * 3600);
        let orch = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(STARTUP_DELAY).await;
            loop {
                crate::execution::worktree_gc::run_worktree_gc(&orch).await;
                tokio::time::sleep(INTERVAL).await;
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
    pub fn spawn_analytics_rollup_backfill(&self) {
        let db = self.db.local.clone();
        tokio::spawn(async move {
            if let Err(e) = crate::analytics::run_historical_backfill(&db).await {
                log::warn!("analytics rollup backfill failed: {e}");
            }
        });
    }

    /// Spawn the memory-triage reconciliation sweep: once immediately at
    /// startup, then on a periodic timer. The sweep is driven entirely by DB
    /// state and guarantees every at-threshold pending pool has a triage issue
    /// even when no fresh same-scope confirmation occurs (accumulated pools,
    /// reverted/deferred batches, lowered thresholds, crashes between claim and
    /// issue creation, drafts stranded on failed/interrupted jobs). Idempotent;
    /// errors are logged, never fatal. Must be called from within a tokio
    /// runtime context.
    pub fn spawn_memory_triage_reconcile(&self) {
        /// Cadence of the reconciliation sweep after its immediate startup pass.
        /// Hourly: this is a safety net behind the event-driven fast path, not a
        /// latency-sensitive path.
        const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);
        let orch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
            loop {
                // The first tick fires immediately, so reconciliation runs once
                // at startup, then every RECONCILE_INTERVAL.
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
    pub fn spawn_memory_review_reconcile(&self) {
        const RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);
        let orch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
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
    /// advances that landed while the app was closed. Live default-branch pushes
    /// are handled by the GitHub webhook, and no-remote projects have no external
    /// source of default-branch movement to poll, so there is deliberately no
    /// recurring timer here.
    ///
    /// Must be called from within a tokio runtime context.
    pub fn spawn_default_advance_reconcile(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            // One-time at startup, before the catch-up: re-detect and persist each
            // local project's real default branch, correcting any row left on the
            // unverified 'main' schema default. The catch-up and merges resolve
            // onto the stored default, so this repair must land first. Best-effort
            // and logged per project.
            crate::projects::crud::reconcile_default_branches(&orch.db.local).await;

            crate::orchestrator::base_advance::reconcile_startup_remote_default_advances(&orch)
                .await;
        });
    }

    /// Enable the per-team background sync loop: build a [`SyncRuntime`] from the
    /// services emitter and default cadence and hand it to `DbState`. Mirrors the
    /// other detached background-spawn methods (`spawn_memory_*`,
    /// `spawn_default_advance_reconcile`) — same fire-and-forget pattern, owned by
    /// runtime teardown. The actual per-team push/pull tasks then spawn lazily as
    /// teams open. Inert with no team configured. Must run within a tokio runtime.
    pub fn start_team_sync(&self) {
        let db = self.db.clone();
        let runtime = crate::db::SyncRuntime {
            emitter: self.services.emitter.clone(),
            cadence: crate::storage::SyncCadence::default(),
        };
        tokio::spawn(async move {
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
    pub fn enqueue_event_embed(&self, event_id: &str, text: String) {
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
    pub fn enqueue_position_embed(
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

    /// Evict a warm process if needed to make room for a new one.
    /// Returns the run_id of the evicted process, if any.
    pub fn collect_warm_if_needed(&self) -> Option<String> {
        let gc = self.warm_gc.as_ref()?.clone();
        let process_state = self.process_state.clone();
        let dbs = self.db.clone();

        let eviction_candidate = {
            fn run_lookup(
                gc: Arc<crate::agent_process::gc::WarmProcessGC>,
                process_state: Arc<AgentProcessState>,
                dbs: Arc<crate::db::DbState>,
            ) -> Option<String> {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| {
                        log::error!("GC: failed to create database runtime: {}", error);
                        error
                    })
                    .ok()?
                    .block_on(async move { gc.find_eviction_candidate(&process_state, &dbs).await })
            }

            if tokio::runtime::Handle::try_current().is_ok() {
                std::thread::spawn(move || run_lookup(gc, process_state, dbs))
                    .join()
                    .map_err(|_| log::error!("GC: database lookup thread panicked"))
                    .ok()
                    .flatten()
            } else {
                run_lookup(gc, process_state, dbs)
            }
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

    /// Store a provider/account usage snapshot and notify the frontend.
    ///
    /// The single store path for both backends: it writes the in-memory cache
    /// (read by `get_provider_usage_snapshot`) **and** emits `provider-usage-updated`
    /// so the usage panel updates live instead of only on a manual refresh. Both
    /// Claude (`rate_limit_event`) and Codex (`account/rateLimits/updated`) route
    /// here, as does the manual refresh command.
    pub fn store_provider_usage_snapshot(&self, snapshot: ProviderUsageSnapshot) {
        {
            let Ok(mut guard) = self.provider_usage_snapshots.write() else {
                return;
            };
            // Prefer the richer source. A coarse live snapshot (Claude
            // `rate_limit_event`, a single status window) must not overwrite a
            // richer manual-probe snapshot (`claude_usage_tui` / `codex_rate_limits`,
            // the canonical 5-hour + weekly windows) already cached, or the panel
            // would flip shape mid-run. Equal-or-greater rank still updates, so
            // Codex's rich live events and every manual refresh flow through.
            if let Some(existing) = guard.get(&snapshot.backend) {
                if snapshot.panel_rank() < existing.panel_rank() {
                    return;
                }
            }
            guard.insert(snapshot.backend.clone(), snapshot.clone());
        }
        let _ = self.services.emitter.emit(
            "provider-usage-updated",
            serde_json::json!({
                "backend": snapshot.backend,
                "snapshot": snapshot,
            }),
        );
    }

    /// Store a normalized context-token snapshot and notify the frontend.
    ///
    /// Snapshots are keyed by durable session id and represent the latest turn's
    /// full prompt plus that turn's output, not cumulative usage across turns.
    pub fn store_context_token_snapshot(&self, state: ContextTokenState) {
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
        let context_window =
            self.context_window_for_context_tokens(&snapshot.backend, snapshot.model.as_deref());
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
        if backend.eq_ignore_ascii_case("claude") {
            return Some(claude_context_window(
                model.unwrap_or("unknown"),
                ClaudeContextOptIn::default(),
            ));
        }

        if backend.eq_ignore_ascii_case("codex") || backend.eq_ignore_ascii_case("openrouter") {
            return model.and_then(|model| {
                self.model_catalog
                    .read()
                    .ok()
                    .and_then(|catalog| catalog.get(&backend.to_lowercase()).cloned())
                    .and_then(|catalog| context_window_from_catalog(&catalog.models, model))
            });
        }

        None
    }

    pub fn refresh_model_catalog(&self) {
        for backend_name in ["claude", "codex", "openrouter"] {
            let backend = backend_for_name(Some(backend_name));
            let entry = match backend.discover_models() {
                Ok(models) => ProviderModelCatalog {
                    backend: backend_name.to_string(),
                    models,
                    options: backend.option_descriptors(),
                    refreshed_at: Some(chrono::Utc::now().timestamp()),
                    error: None,
                },
                Err(error) => ProviderModelCatalog {
                    backend: backend_name.to_string(),
                    models: Vec::new(),
                    options: backend.option_descriptors(),
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

fn context_window_from_catalog(models: &[DiscoveredModel], model: &str) -> Option<i64> {
    models
        .iter()
        .find(|entry| entry.model == model || entry.id == model)
        .and_then(|entry| entry.context_window)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::jwt::encrypt_jwt_for_storage;
    use crate::services::testing::{CapturingEmitter, TestServicesBuilder};
    use crate::services::EventEmitter;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};

    async fn test_db() -> Arc<DbState> {
        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("orch.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        Arc::new(DbState::new(Arc::new(local), search))
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
    async fn context_token_state_falls_back_to_latest_event_tokens() {
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
                    pricing: None,
                    supported_parameters: Vec::new(),
                    router: false,
                    architecture_modality: None,
                }],
                options: Vec::new(),
                refreshed_at: Some(20),
                error: None,
            },
        );

        let state = orch.get_context_token_state("session-ctx").await.unwrap();
        assert_eq!(state.run_id, "run-ctx");
        assert_eq!(state.backend, "codex");
        assert_eq!(state.model, Some("gpt-5".to_string()));
        assert_eq!(state.used_tokens, 250);
        assert_eq!(state.context_window, Some(258_400));
        assert_eq!(state.reasoning_tokens, Some(7));
        assert_eq!(state.last_output_tokens, Some(50));
        assert_eq!(state.captured_at, 20);
    }

    #[tokio::test]
    async fn claude_context_token_state_ignores_cumulative_result_usage() {
        let db = test_db().await;
        db.local
            .execute_script(
                "
                INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES ('project-claude-ctx', 'default', 'Claude Context Project', 'CLCTX', '/tmp/clctx', 1, 1);
                INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
                 VALUES ('job-claude-ctx', 'project-claude-ctx', 'running', 'claude-sonnet-4-20250514', 1, 1);
                INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
                 VALUES ('session-claude-ctx', 'job-claude-ctx', 'claude', 'open', 1, 1, 1);
                INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
                 VALUES ('run-claude-ctx', 'project-claude-ctx', 'job-claude-ctx', 'exited', 'session-claude-ctx', 1, 1);
                INSERT INTO events(id, run_id, session_id, sequence, timestamp, event_type, data,
                    parent_tool_use_id, created_at, input_tokens, cache_read_tokens,
                    cache_create_tokens, output_tokens, thinking_tokens)
                 VALUES ('event-assistant-final', 'run-claude-ctx', 'session-claude-ctx', 10, 10, 'assistant', '{}',
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
        assert_eq!(state.model, Some("claude-sonnet-4-20250514".to_string()));
        assert_eq!(state.used_tokens, 37_713);
        assert_ne!(state.used_tokens, 157_045);
        assert_eq!(state.reasoning_tokens, Some(12));
        assert_eq!(state.last_output_tokens, Some(38));
        assert_eq!(state.captured_at, 10);
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
                    pricing: None,
                    supported_parameters: Vec::new(),
                    router: false,
                    architecture_modality: None,
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
