//! The orchestrator's answer to the cairn-models facet's read side.
//!
//! Every method is composed from the same orchestrator functions the Settings →
//! Providers page calls: `provider_catalog` (the page's provider tabs and
//! Add-provider list), the discovered `model_catalog`, `list_accounts`,
//! and `provider_usage_pool` (the provider card). So `cairn://models/*` shows
//! what the page shows by construction,
//! and the parity tests below pin it.

use std::collections::HashMap;
use std::sync::Arc;

use cairn_models::complete::HttpComplete;
use cairn_models::image::http::HttpImages;
use cairn_models::interface::complete::Complete;
use cairn_models::interface::image::ImageTransport;
use cairn_models::interface::session::{
    Cli, CodexSessionProvider, SessionError, SessionHost, SessionProvider,
};
use cairn_models::interface::speak::SpeechTransport;
use cairn_models::managed::{InstallProgress, ManagedTools, UpdateOutcome, UPDATE_INTERVAL};
use cairn_models::session::claude::ClaudeCli;
use cairn_models::session::codex::CodexAppServer;
use cairn_models::speak::HttpSpeech;

use cairn_db::storage::run_db_blocking;
use cairn_db::turso::params;
use cairn_models::accounts::AccountInfo;
use cairn_models::interface::{
    Catalog, CatalogProvider, ListedProvider, ModelScope, Models, SessionState, SessionView,
    LOCAL_HOST,
};
use cairn_models::types::catalog::{self, ServedVerb, CLAUDE_FAMILY_BACKEND};
use cairn_models::types::pool::ProviderPoolSnapshot;

use super::Orchestrator;
use crate::db::DbState;
use crate::storage::RowExt;

/// The cairn-models facet's launch side as this orchestrator uses it.
///
/// Every HTTP token request goes through `complete`; every image drawn over
/// HTTP through `images`, and every utterance through `speech`; every Claude
/// process is opened by `claude`, and every Codex app-server and thread by
/// `codex`, on `host`. The builder installs the implementations that
/// reach providers, with sessions on this machine; a test replaces them to
/// observe or script every launch without touching a network or a CLI.
#[derive(Clone)]
pub struct ModelRuntime {
    pub complete: Arc<dyn Complete>,
    /// The HTTP image wires. Codex's image offer draws on a pooled
    /// app-server instead, through a transport built per call.
    pub images: Arc<dyn ImageTransport>,
    pub speech: Arc<dyn SpeechTransport>,
    pub claude: Arc<dyn SessionProvider>,
    pub codex: Arc<dyn CodexSessionProvider>,
    /// Where session processes run.
    pub host: Arc<dyn SessionHost>,
    /// The CLIs the local host launches (Codex and Claude Code), installed
    /// and kept on their published pins by Cairn.
    pub managed: ManagedTools,
    /// Every outside client's request the served API relays.
    pub relay: Arc<dyn cairn_models::interface::relay::Relay>,
}

impl ModelRuntime {
    /// The implementations that reach providers, with sessions run as
    /// children of this runner from the CLIs `managed` installs.
    pub(crate) fn local(
        process: Arc<dyn crate::services::ProcessSpawner>,
        managed: ManagedTools,
    ) -> Self {
        Self {
            complete: Arc::new(HttpComplete),
            images: Arc::new(HttpImages),
            speech: Arc::new(HttpSpeech),
            claude: Arc::new(ClaudeCli),
            codex: Arc::new(CodexAppServer),
            host: Arc::new(cairn_models::session::local::LocalSessionHost::new(
                process,
                crate::backends::codex::cairn_codex_home(),
                managed.clone(),
            )),
            managed,
            relay: Arc::new(HttpComplete),
        }
    }
}

/// Exit reason for an idle provider process retired because its managed CLI
/// was replaced.
pub(crate) const MANAGED_CLI_SWAP_EXIT_REASON: &str = "managed_cli_swap";

/// The managed `cli` on the session host for the run `run_id`.
///
/// While its first install is still landing, the launch waits on it, and the
/// run's transcript is told once where the install stands. A launch that is
/// stopped meanwhile (its run settled, or its admission claim lost) stops
/// waiting at once, and one that waited is checked again before anything is
/// spawned, so stopped work never reaches the provider. A refusal (the
/// install's own text, naming the manual route) is written to the transcript
/// once. Never a CLI found on the `PATH`.
pub(crate) fn resolve_managed_cli(
    orch: &Orchestrator,
    host: &dyn SessionHost,
    cli: Cli,
    run_id: &str,
    session_id: Option<&str>,
) -> Result<String, String> {
    let claim = orch
        .process_state
        .admission_claim(run_id)
        .map(|claim| (claim.request_id, claim.generation));
    let still_wanted =
        || crate::orchestrator::lifecycle::launch_still_wanted(orch, run_id, claim.as_ref());
    let waited = std::cell::Cell::new(false);
    let mut on_wait = |progress: InstallProgress| {
        waited.set(true);
        let raw = serde_json::json!({
            "type": "system",
            "subtype": "managed_cli_install_wait",
            "tool": progress.tool.name,
            "version": progress.version,
            "downloaded": progress.downloaded,
            "total": progress.total,
            "last_error": progress.last_error,
        });
        if let Err(error) = crate::messages::transcript::insert_system_message_sync(
            orch,
            run_id,
            session_id,
            None,
            &progress.waiting_message(),
            raw,
        ) {
            log::warn!(
                "managed {}: could not record the wait: {error}",
                progress.tool.name
            );
        }
    };
    let resolved = host
        .resolve_cli_waiting(cli, &mut on_wait, &still_wanted)
        .map_err(|error| {
            match &error {
                SessionError::CliNotFound(reason) => {
                    crate::orchestrator::session::insert_error_event(
                        orch,
                        run_id,
                        session_id,
                        &format!("{cli:?} CLI not found: {reason}"),
                    )
                }
                SessionError::CliInstallFailed(reason) => {
                    crate::orchestrator::session::insert_error_event(
                        orch, run_id, session_id, reason,
                    )
                }
                _ => {}
            }
            error.to_string()
        })?;
    if waited.get() {
        if !still_wanted() {
            return Err(format!(
                "run {run_id} was stopped while its {cli:?} CLI installed; nothing was launched"
            ));
        }
        // The wait may have outlived the team lease while the authority was
        // unreachable; the local claim cannot tell.
        crate::execution::jobs::confirm_team_lease_blocking(orch, run_id)?;
        // The confirmation is a network round trip; a stop during it still
        // wins.
        if !still_wanted() {
            return Err(format!(
                "run {run_id} was stopped while its admission lease was confirmed; nothing was launched"
            ));
        }
    }
    Ok(resolved)
}

impl Orchestrator {
    /// Keep every managed CLI on its published pin: each tool once now (an
    /// install for any tool with no complete release begins here), then
    /// every [`UPDATE_INTERVAL`]. Each tool follows on its own task, so one
    /// tool's install waiting out a network fault never holds the other's.
    /// Running sessions finish on the release they started with.
    ///
    /// When `shutdown` turns true (or its sender goes away), every tool is
    /// told to stop: an install retrying past a network fault ends at its
    /// next wait, and launches waiting on it return, so an offline first
    /// install never holds the process open.
    pub fn spawn_managed_tools_follow(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let managed = self.models_runtime().managed.clone();
        tokio::spawn(async move {
            while !*shutdown.borrow_and_update() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
            managed.shutdown();
        });
        for cli in [Cli::Codex, Cli::Claude] {
            let orch = self.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(UPDATE_INTERVAL);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    interval.tick().await;
                    orch.follow_managed_tool(cli, false).await;
                }
            });
        }
    }

    /// Retry `cli`'s install, forgetting a refusal that stands for the
    /// current pin, unless an install is already in progress. Onboarding's
    /// "Check again" lands here.
    pub fn kick_managed_install(&self, cli: Cli) {
        let orch = self.clone();
        tokio::spawn(async move { orch.follow_managed_tool(cli, true).await });
    }

    async fn follow_managed_tool(&self, cli: Cli, retry: bool) {
        let tool = self.models_runtime().managed.get(cli).clone();
        let name = tool.spec().name;
        let outcome = tokio::task::spawn_blocking(move || {
            if retry {
                tool.retry_if_idle()
            } else {
                Some(tool.update())
            }
        })
        .await;
        match outcome {
            Ok(Some(Ok(UpdateOutcome::Installed { resolved, previous }))) => {
                log::info!(
                    "managed {name}: now on {} (was {})",
                    resolved.version,
                    previous.as_deref().unwrap_or("not installed")
                );
                self.on_managed_swap(cli, &resolved.version);
            }
            Ok(Some(Ok(UpdateOutcome::Current(_)))) | Ok(None) => {}
            // The tool logs and records the refusal; its status reports it.
            Ok(Some(Err(_))) => {}
            Err(error) => log::warn!("managed {name}: the update task failed: {error}"),
        }
    }

    /// What a newly active release changes, per tool.
    fn on_managed_swap(&self, cli: Cli, version: &str) {
        match cli {
            // Pooled app-servers on the old release retire; the catalog is
            // the new release's.
            Cli::Codex => {
                self.codex_pool.retire_versions_except(version);
                self.spawn_model_catalog_refresh();
            }
            // Every warm Claude process predates the swap. Each one still
            // idle is retired (provider only: its job's terminals keep
            // running), so the next session starts on the new release; one a
            // launch or resume holds is left alone, and a session mid-turn
            // finishes on the release it started with, which pruning keeps.
            // The catalog is refreshed because a first install is what makes
            // a signed-in profile's models visible.
            Cli::Claude => {
                let warm: Vec<String> = self
                    .process_state
                    .processes
                    .lock()
                    .map(|processes| {
                        processes
                            .iter()
                            .filter(|(_, handle)| {
                                handle.is_warm()
                                    && handle
                                        .facts
                                        .as_ref()
                                        .map(|facts| facts.provider.as_str())
                                        .or(handle.backend.as_deref())
                                        .is_none_or(|provider| provider == "claude")
                            })
                            .map(|(run_id, _)| run_id.clone())
                            .collect()
                    })
                    .unwrap_or_default();
                let retired = warm
                    .iter()
                    .filter(|run_id| {
                        crate::orchestrator::lifecycle::retire_idle_provider(
                            self,
                            run_id,
                            MANAGED_CLI_SWAP_EXIT_REASON,
                        )
                    })
                    .count();
                if !warm.is_empty() {
                    log::info!(
                        "managed claude: retired {retired} of {} warm process(es) from the previous release",
                        warm.len()
                    );
                }
                self.spawn_model_catalog_refresh();
            }
        }
    }
}

/// What the database knows about a live run: which session it is, when it
/// started, and the node it belongs to. The provider, account, and backend
/// session id come from the live handle's launch facts instead.
#[derive(Default)]
struct RunFacts {
    session_id: Option<String>,
    started_at: Option<i64>,
    node_uri: Option<String>,
}

async fn run_facts(dbs: &DbState, run_id: &str) -> Result<RunFacts, String> {
    let db = crate::execution::routing::owning_db_for_run(dbs, run_id)
        .await
        .map_err(|error| error.to_string())?;
    let owned = run_id.to_string();
    let row = db
        .read(move |conn| {
            let run_id = owned.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT r.session_id, r.started_at, r.job_id FROM runs r WHERE r.id = ?1",
                        params![run_id.as_str()],
                    )
                    .await?;
                let Some(row) = rows.next().await? else {
                    return Ok(None);
                };
                Ok(Some((row.opt_text(0)?, row.opt_i64(1)?, row.opt_text(2)?)))
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some((session_id, started_at, job_id)) = row else {
        return Ok(RunFacts::default());
    };
    let node_uri = match job_id {
        Some(job_id) => crate::jobs::queries::home_uri_for_job(&db, &job_id)
            .await
            .ok()
            .flatten(),
        None => None,
    };
    Ok(RunFacts {
        session_id,
        started_at,
        node_uri,
    })
}

impl Orchestrator {
    /// What provider `key` serves for any model it routes in `project_id`'s
    /// scope. A key this build does not ship serves nothing, rather than
    /// whatever the backend fallback would claim.
    pub(crate) fn provider_serves(&self, key: &str, project_id: Option<&str>) -> Vec<ServedVerb> {
        if !catalog::is_supported(key) {
            return Vec::new();
        }
        crate::backends::backend_for_name(Some(key)).serves(self, project_id)
    }

    /// Provider `key` as the catalog sees it in `project_id`'s scope: the one
    /// record the launch doors, the pickers, and the frontend's resolved
    /// serves all ask, so none of them can disagree about what a model
    /// serves.
    ///
    /// It is built whether or not the workspace installed `key` (`installed`
    /// says which), because a session can launch on a provider the workspace
    /// has not installed; installation policy is enforced elsewhere.
    pub(crate) fn catalog_provider(&self, key: &str, project_id: Option<&str>) -> CatalogProvider {
        self.provider_record(
            key,
            project_id,
            &self.provider_catalog(),
            self.provider_installed(key),
        )
    }

    /// The providers that serve this workspace: the ones it installed, then
    /// Cairn inference whenever its deployments serve a model, which is what
    /// installs a built-in provider.
    pub(crate) fn installed_providers(&self) -> Vec<String> {
        let mut installed = self.enabled_providers();
        let inference = crate::backends::inference::INFERENCE_BACKEND_KEY;
        if !installed.iter().any(|key| key == inference) && self.serves_any(inference) {
            installed.push(inference.to_string());
        }
        installed
    }

    pub(crate) fn provider_installed(&self, key: &str) -> bool {
        self.installed_providers().iter().any(|entry| entry == key)
    }

    /// Whether `key`'s last discovery found any model.
    fn serves_any(&self, key: &str) -> bool {
        self.model_catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.get(key).map(|entry| !entry.models.is_empty()))
            .unwrap_or(false)
    }

    /// Rediscover `backend`'s models and answer with that provider as the
    /// workspace's [`Models::catalog_listing`] lists it, unnarrowed.
    pub async fn refresh_catalog_listing(&self, backend: &str) -> Result<ListedProvider, String> {
        let refreshed = self.run_provider_model_catalog_refresh(backend).await?;
        Ok(self.catalog_provider(&refreshed.backend, None).listed(None))
    }

    /// What discovery last found for `key`, with every model its backend
    /// declares itself ([`crate::backends::AgentBackend::declared_models`])
    /// added when discovery has not run, failed, or left it out. A declared
    /// model is never mistaken for an unlisted one.
    fn provider_models(
        &self,
        key: &str,
    ) -> Option<cairn_models::types::catalog::ProviderModelCatalog> {
        let mut models = self
            .model_catalog
            .read()
            .ok()
            .and_then(|catalog| catalog.get(key).cloned());
        if !catalog::is_supported(key) {
            return models;
        }
        for declared in crate::backends::backend_for_name(Some(key)).declared_models() {
            let entry =
                models.get_or_insert_with(|| cairn_models::types::catalog::ProviderModelCatalog {
                    backend: key.to_string(),
                    models: Vec::new(),
                    options: Vec::new(),
                    refreshed_at: None,
                    error: None,
                });
            if !entry
                .models
                .iter()
                .any(|model| model.id == declared.id || model.model == declared.model)
            {
                entry.models.push(declared);
            }
        }
        models
    }

    fn provider_record(
        &self,
        key: &str,
        project_id: Option<&str>,
        descriptors: &[cairn_models::types::catalog::ProviderDescriptor],
        installed: bool,
    ) -> CatalogProvider {
        CatalogProvider {
            descriptor: descriptors
                .iter()
                .find(|descriptor| descriptor.key == key)
                .cloned()
                .unwrap_or_else(|| catalog::unknown_descriptor(key)),
            installed,
            models: self.provider_models(key),
            alias_resolutions: if key == "claude" {
                self.claude_alias_resolutions()
            } else {
                Vec::new()
            },
            serves: self.provider_serves(key, project_id),
            unable: {
                let mut unable = self.decision_observations.unable(key);
                if key == crate::backends::inference::INFERENCE_BACKEND_KEY {
                    unable.extend(crate::backends::inference::unable_now());
                }
                unable
            },
        }
    }
}

impl Models for Orchestrator {
    fn catalog(&self, scope: &ModelScope) -> Result<Catalog, String> {
        let descriptors = self.provider_catalog();
        let enabled = self.installed_providers();

        let mut providers: Vec<CatalogProvider> = enabled
            .iter()
            .map(|key| self.provider_record(key, scope.project_id.as_deref(), &descriptors, true))
            .collect();
        providers.extend(self.embed_gateway_provider());
        providers.extend(
            descriptors
                .into_iter()
                .filter(|descriptor| !enabled.iter().any(|key| key == descriptor.key.as_ref()))
                .map(|descriptor| CatalogProvider {
                    descriptor,
                    installed: false,
                    models: None,
                    serves: Vec::new(),
                    unable: Vec::new(),
                    alias_resolutions: Vec::new(),
                }),
        );
        Ok(Catalog { providers })
    }

    fn accounts(&self, scope: &ModelScope) -> Result<Vec<AccountInfo>, String> {
        Ok(self.list_accounts(scope.project_id.as_deref()))
    }

    fn usage(
        &self,
        scope: &ModelScope,
        provider: Option<&str>,
    ) -> Result<Vec<ProviderPoolSnapshot>, String> {
        let installed = self.enabled_providers();
        let keys: Vec<&str> = match provider {
            Some(provider) if installed.iter().any(|key| key == provider) => vec![provider],
            Some(provider) => {
                return Err(format!(
                    "Provider '{provider}' is not installed in this workspace. Installed: {}.",
                    installed.join(", ")
                ))
            }
            None => installed.iter().map(String::as_str).collect(),
        };
        Ok(keys
            .into_iter()
            .map(|key| self.provider_usage_pool(key, scope.project_id.as_deref()))
            .collect())
    }

    fn requests(
        &self,
        query: &cairn_models::interface::RequestsQuery,
    ) -> Result<Vec<cairn_models::interface::RequestRecord>, String> {
        use cairn_db::storage::served_api as store;
        let db = self.db.local.clone();
        let query = query.clone();
        run_db_blocking(move || async move {
            let clients = store::list_served_api_clients(&db, crate::authorization::WORKSPACE_ID)
                .await
                .map_err(|error| error.to_string())?;
            let client_id = match &query.client {
                None => None,
                Some(wanted) => Some(
                    clients
                        .iter()
                        .filter(|client| &client.id == wanted || &client.name == wanted)
                        .max_by_key(|client| client.revoked_at.is_none())
                        .map(|client| client.id.clone())
                        .ok_or_else(|| format!("no served-API client is named {wanted}"))?,
                ),
            };
            let since =
                chrono::Utc::now().timestamp_millis() - i64::from(query.hours) * 60 * 60 * 1000;
            let rows = store::list_model_requests(
                &db,
                since,
                client_id.as_deref(),
                i64::try_from(query.limit).unwrap_or(i64::MAX),
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(rows
                .into_iter()
                .map(|row| cairn_models::interface::RequestRecord {
                    client: row.client_id.as_ref().and_then(|id| {
                        clients
                            .iter()
                            .find(|client| &client.id == id)
                            .map(|client| client.name.clone())
                    }),
                    id: row.id,
                    at: row.ts,
                    door: row.door,
                    client_id: row.client_id,
                    model_asked: row.model_asked,
                    model: row.model,
                    provider: row.provider,
                    account_id: row.account_id,
                    status: row.status,
                    stream: row.stream,
                    input_tokens: row.input_tokens,
                    output_tokens: row.output_tokens,
                    cached_tokens: row.cached_tokens,
                    cost: row.cost,
                    latency_ms: row.latency_ms,
                    wait_ms: row.wait_ms,
                    outcome: row.outcome,
                    grant_id: row.grant_id,
                    detail: row.detail,
                })
                .collect())
        })
    }

    fn sessions(&self) -> Vec<SessionView> {
        // What the process registry knows; the database facts are filled in
        // below, after the registry lock is released.
        let mut sessions: Vec<SessionView> = match self.process_state.processes.lock() {
            Ok(processes) => processes
                .iter()
                .map(|(run_id, handle)| SessionView {
                    session_id: None,
                    run_id: run_id.clone(),
                    node_uri: None,
                    // The launch's own facts. A handle without them (one no
                    // launch registered) and without a backend is Claude's,
                    // by the registry's own convention.
                    provider: handle
                        .facts
                        .as_ref()
                        .map(|facts| facts.provider.clone())
                        .or_else(|| handle.backend.clone())
                        .unwrap_or_else(|| CLAUDE_FAMILY_BACKEND.to_string()),
                    model: handle.model.clone(),
                    account_id: handle
                        .facts
                        .as_ref()
                        .and_then(|facts| facts.account_id.clone()),
                    backend_session_id: handle
                        .facts
                        .as_ref()
                        .and_then(|facts| facts.backend_session_id.clone()),
                    state: if handle.is_warm() {
                        SessionState::Warm
                    } else {
                        SessionState::Busy
                    },
                    host: handle
                        .facts
                        .as_ref()
                        .map(|facts| facts.host.clone())
                        .unwrap_or_else(|| LOCAL_HOST.to_string()),
                    pid: handle
                        .child
                        .lock()
                        .ok()
                        .and_then(|child| child.as_ref().map(|child| child.id())),
                    started_at: None,
                })
                .collect(),
            Err(_) => return Vec::new(),
        };
        if sessions.is_empty() {
            return sessions;
        }

        let dbs = self.db.clone();
        let run_ids: Vec<String> = sessions.iter().map(|view| view.run_id.clone()).collect();
        let mut facts: HashMap<String, RunFacts> = run_db_blocking(move || async move {
            let mut facts = HashMap::new();
            for run_id in run_ids {
                match run_facts(&dbs, &run_id).await {
                    Ok(found) => {
                        facts.insert(run_id, found);
                    }
                    Err(error) => log::debug!("model sessions: no run facts for {run_id}: {error}"),
                }
            }
            Ok(facts)
        })
        .unwrap_or_default();

        for view in &mut sessions {
            let found = facts.remove(&view.run_id).unwrap_or_default();
            view.session_id = found.session_id;
            view.node_uri = found.node_uri;
            view.started_at = found.started_at;
        }
        sessions.sort_by(|a, b| {
            a.started_at
                .cmp(&b.started_at)
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        sessions
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::credentials::{
        ApiProvider, CustomModelPolicy, CustomProtocol, CustomSampling, ProviderAuth,
    };
    use crate::db::DbState;
    use crate::models::UpdateSettings;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_models::types::catalog::{claude_models, DiscoveredModel, ProviderModelCatalog};
    use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageWindow};
    use std::sync::Arc;

    pub(crate) const API_KEY_SECRET: &str = "sk-ant-secret-api-key-value";
    pub(crate) const OAUTH_SECRET: &str = "oauth-secret-token-value";
    pub(crate) const HEADER_SECRET: &str = "tenant-secret-header-value";
    pub(crate) const CUSTOM_KEY_SECRET: &str = "sk-custom-secret-value";

    pub(crate) async fn seed_alias_session(
        orch: &Orchestrator,
        name: &str,
        model: &str,
        backend: &str,
    ) {
        orch.db.local.execute_script(format!(
            "INSERT OR IGNORE INTO workspaces(id, name, created_at, updated_at)
               VALUES ('alias-ws', 'Aliases', 1, 1);
             INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('alias-project', 'alias-ws', 'Aliases', 'ALIAS', '/tmp/alias', 1, 1);
             INSERT INTO jobs(id, project_id, status, model, created_at, updated_at)
               VALUES ('{name}-job', 'alias-project', 'running', '{model}', 1, 1);
             INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('{name}-session', '{name}-job', '{backend}', 'open', 1, 1, 1);
             INSERT INTO runs(id, project_id, job_id, status, session_id, created_at, updated_at)
               VALUES ('{name}-run', 'alias-project', '{name}-job', 'live', '{name}-session', 1, 1);"
        )).await.unwrap();
    }

    pub(crate) async fn bare_orchestrator() -> Orchestrator {
        bare_orchestrator_with(Arc::new(cairn_models::subscription::Unsubscribed)).await
    }

    /// [`bare_orchestrator`] with `subscriptions` installed.
    ///
    /// Pins the process's scratch root first: a response or a Codex server this
    /// orchestrator starts provisions its working directory there, which must
    /// not be the machine's real Cairn scratch on any entry point.
    pub(crate) async fn bare_orchestrator_with(
        subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    ) -> Orchestrator {
        cairn_common::scratch::testing::pin_private_root();
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open_migrated_for_test(root.join("orch.db"))
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir)
            .subscriptions(subscriptions)
            .build()
    }

    fn window(id: &str, scope: ProviderUsageScope, remaining: f64) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: id.to_string(),
            label: id.to_string(),
            scope,
            scope_target: None,
            used_percent: 100.0 - remaining,
            remaining_percent: remaining,
            resets_at: Some(i64::MAX),
            reset_at_text: None,
            window_duration_mins: None,
        }
    }

    /// Shipped and custom providers, a discovered catalog, and managed,
    /// API-key, OAuth and custom-endpoint accounts, one with measured windows.
    /// Every secret value it stores is one of the constants above.
    pub(crate) async fn seeded_orchestrator() -> Orchestrator {
        seeded_orchestrator_with(Arc::new(cairn_models::subscription::Unsubscribed)).await
    }

    /// [`seeded_orchestrator`] with `subscriptions` installed.
    pub(crate) async fn seeded_orchestrator_with(
        subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    ) -> Orchestrator {
        let orch = bare_orchestrator_with(subscriptions).await;
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(vec![
                "claude".to_string(),
                "codex".to_string(),
                "ollama".to_string(),
            ]),
            ..Default::default()
        })
        .unwrap();
        let profile = orch
            .add_account(
                ApiProvider::Anthropic,
                "Work Max".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(
            &profile.id,
            vec![
                window("session", ProviderUsageScope::Session, 64.0),
                window("weekly_all", ProviderUsageScope::Weekly, 30.0),
            ],
            None,
        )
        .unwrap();
        orch.add_account(
            ApiProvider::Anthropic,
            "Anthropic key".into(),
            ProviderAuth::ApiKey {
                value: API_KEY_SECRET.into(),
            },
            None,
        )
        .unwrap();
        orch.add_account(
            ApiProvider::OpenAI,
            "ChatGPT token".into(),
            ProviderAuth::OAuthToken {
                value: OAUTH_SECRET.into(),
            },
            None,
        )
        .unwrap();
        orch.add_custom_provider(
            "DGX Spark".into(),
            ProviderAuth::custom_endpoint(
                "http://10.10.10.83:8000",
                CustomProtocol::OpenAiChatCompletions,
                Some(CUSTOM_KEY_SECRET),
                &[("x-tenant".to_string(), HEADER_SECRET.to_string())],
                CustomModelPolicy::Explicit {
                    ids: vec!["qwen3.8-27b".into()],
                },
                CustomSampling::default(),
                32_768,
            )
            .unwrap(),
            None,
        )
        .unwrap();

        let mut ollama = claude_models().remove(0);
        ollama.id = "qwen3:32b".into();
        ollama.model = "qwen3:32b".into();
        ollama.display_name = "qwen3:32b".into();
        ollama.context_window = Some(40_960);
        let mut catalog = orch.model_catalog.write().unwrap();
        for (backend, models) in [("claude", claude_models()), ("ollama", vec![ollama])] {
            catalog.insert(
                backend.to_string(),
                ProviderModelCatalog {
                    backend: backend.to_string(),
                    models,
                    options: Vec::new(),
                    refreshed_at: Some(1_790_000_000),
                    error: None,
                },
            );
        }
        catalog.insert(
            "codex".to_string(),
            ProviderModelCatalog {
                backend: "codex".to_string(),
                models: Vec::<DiscoveredModel>::new(),
                options: Vec::new(),
                refreshed_at: None,
                error: Some("codex is not installed".to_string()),
            },
        );
        drop(catalog);
        orch
    }

    fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_facet_reads_exactly_what_the_providers_page_reads() {
        let orch = seeded_orchestrator().await;
        let scope = ModelScope::workspace();
        let catalog = orch.catalog(&scope).unwrap();

        // Installed providers: the page's tabs, in its order, described by the
        // same catalog the page fetches.
        let enabled = orch.enabled_providers();
        assert!(enabled.iter().any(|key| key.starts_with("custom:")));
        let installed: Vec<&str> = catalog
            .installed()
            .map(|provider| provider.descriptor.key.as_ref())
            .collect();
        assert_eq!(
            installed,
            enabled.iter().map(String::as_str).collect::<Vec<_>>()
        );
        let page_catalog = orch.provider_catalog();
        for provider in &catalog.providers {
            let page = page_catalog
                .iter()
                .find(|entry| entry.key == provider.descriptor.key)
                .expect("every provider comes from the page's catalog");
            assert_eq!(json(&provider.descriptor), json(page));
        }
        // Not installed: the Add-provider list is every other catalog entry.
        assert_eq!(catalog.providers.len(), page_catalog.len());

        // Models: what the app's pages read (`get_catalog_listing`) is the
        // catalog's own discovery, entry for entry, a backend's own declared
        // models included.
        let listing = orch.catalog_listing(&scope, None).unwrap();
        assert_eq!(listing.providers.len(), catalog.providers.len());
        let ids = |found: Option<&ProviderModelCatalog>| {
            found.map(|found| {
                found
                    .models
                    .iter()
                    .map(|model| model.id.clone())
                    .collect::<Vec<_>>()
            })
        };
        for (listed, provider) in listing.providers.iter().zip(&catalog.providers) {
            assert_eq!(listed.descriptor.key, provider.descriptor.key);
            assert_eq!(ids(listed.catalog.as_ref()), ids(provider.models.as_ref()));
        }
        // Codex's discovery failed here, and its image offer is still known.
        let codex = catalog
            .installed()
            .find(|provider| provider.descriptor.key == "codex")
            .unwrap();
        assert!(!codex.serves(
            cairn_models::image::codex::CODEX_IMAGE_MODEL,
            cairn_models::types::catalog::VerbKind::Session
        ));

        // Accounts and usage: the page's commands, unchanged.
        assert_eq!(
            json(&orch.accounts(&scope).unwrap()),
            json(&orch.list_accounts(None))
        );
        let usage = orch.usage(&scope, None).unwrap();
        let page_usage: Vec<ProviderPoolSnapshot> = enabled
            .iter()
            .map(|key| orch.provider_usage_pool(key, None))
            .collect();
        assert_eq!(json(&usage), json(&page_usage));
        let claude_pool = orch.usage(&scope, Some("claude")).unwrap();
        assert_eq!(claude_pool[0].families[0].remaining_percent, 64.0);
        assert!(orch.usage(&scope, Some("openrouter")).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_installed_key_this_build_cannot_place_keeps_its_place() {
        let orch = seeded_orchestrator().await;
        // A custom provider whose account is gone: still a supported key, so
        // the workspace may keep it installed, but no catalog entry names it.
        let mut enabled = orch.enabled_providers();
        enabled.push("custom:acc_gone".to_string());
        orch.update_settings(UpdateSettings {
            enabled_providers: Some(enabled),
            ..Default::default()
        })
        .unwrap();
        let catalog = orch.catalog(&ModelScope::workspace()).unwrap();
        let gone = catalog
            .installed()
            .find(|provider| provider.descriptor.key == "custom:acc_gone")
            .expect("kept");
        assert_eq!(gone.descriptor.display_name, "custom:acc_gone");
        assert!(gone.descriptor.setup_requirement.contains("remove it"));
        assert!(gone.models.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_live_process_means_no_sessions() {
        let orch = bare_orchestrator().await;
        assert!(orch.sessions().is_empty());
    }

    /// The frontend reads what each model serves from the catalog listing, and
    /// `packages/ui/src/models.test.ts` holds its pickers against the same
    /// golden, so the two languages cannot disagree about a model.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_catalog_listing_says_what_each_model_serves_as_its_golden() {
        use cairn_models::types::catalog::{ServedVerb, VerbKind, Wire};
        let orch = bare_orchestrator().await;
        orch.update_settings(UpdateSettings {
            // Claude stays: the default tiers are bound to it.
            enabled_providers: Some(vec![
                "claude".to_string(),
                "codex".to_string(),
                "opencode-go".to_string(),
            ]),
            ..Default::default()
        })
        .unwrap();
        let named = |id: &str, serves: Option<Vec<ServedVerb>>| {
            let mut model = claude_models().remove(0);
            model.id = id.to_string();
            model.model = id.to_string();
            model.display_name = id.to_string();
            model.serves = serves;
            model
        };
        for (backend, models) in [
            (
                "codex",
                vec![
                    cairn_models::image::codex::discovered_model(),
                    named("gpt-5.5", None),
                ],
            ),
            (
                "opencode-go",
                vec![
                    named("glm-unpublished", None),
                    named(
                        "kimi-k3",
                        Some(ServedVerb::conversation(Wire::OpenAiChatCompletions)),
                    ),
                ],
            ),
        ] {
            orch.seed_model_catalog_for_test(ProviderModelCatalog {
                backend: backend.to_string(),
                models,
                options: Vec::new(),
                refreshed_at: Some(1_790_000_000),
                error: None,
            });
        }

        let listing = orch
            .catalog_listing(&ModelScope::workspace(), None)
            .unwrap();
        let view: Vec<ProviderModelCatalog> = listing
            .providers
            .iter()
            .filter(|provider| matches!(provider.descriptor.key.as_ref(), "codex" | "opencode-go"))
            .filter_map(|provider| provider.catalog.clone())
            .collect();
        let serves = |backend: &str, model: &str| -> Vec<VerbKind> {
            let mut kinds: Vec<VerbKind> = Vec::new();
            let found = view
                .iter()
                .find(|entry| entry.backend == backend)
                .unwrap()
                .models
                .iter()
                .find(|entry| entry.model == model)
                .unwrap();
            for served in found.serves.as_deref().expect("always resolved") {
                if !kinds.contains(&served.verb.kind()) {
                    kinds.push(served.verb.kind());
                }
            }
            kinds
        };
        assert_eq!(
            serves("codex", cairn_models::image::codex::CODEX_IMAGE_MODEL),
            [VerbKind::Image]
        );
        assert_eq!(
            serves("codex", "gpt-5.5"),
            [VerbKind::Complete, VerbKind::Session]
        );
        assert!(serves("opencode-go", "glm-unpublished").is_empty());
        assert_eq!(
            serves("opencode-go", "kimi-k3"),
            [VerbKind::Complete, VerbKind::Session]
        );
        // Every listed model agrees with the facet's own answer for its
        // provider.
        for entry in &view {
            let provider = orch.catalog_provider(&entry.backend, None);
            for model in &entry.models {
                let verbs = serves(&entry.backend, &model.model);
                for verb in VerbKind::ALL {
                    assert_eq!(
                        verbs.contains(&verb),
                        provider.serves(&model.model, verb),
                        "{} {verb}",
                        model.model
                    );
                }
            }
        }

        let rendered = serde_json::to_string_pretty(&view).unwrap() + "\n";
        crate::backends::assert_golden_file(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/orchestrator/fixtures/catalog_listing.json"
            ),
            &rendered,
        );
    }
}
