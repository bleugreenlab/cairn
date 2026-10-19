//! The orchestrator's answer to the cairn-models facet's read side.
//!
//! Every method is composed from the same orchestrator functions the Settings →
//! Providers page calls: `provider_catalog` (the page's provider tabs and
//! Add-provider list), the discovered `model_catalog`, `list_accounts`,
//! `provider_usage_pool` (the provider card), and the effective presets (the
//! tier matrix). So `cairn://models/*` shows what the page shows by construction,
//! and the parity tests below pin it.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use cairn_models::complete::HttpComplete;
use cairn_models::interface::complete::Complete;
use cairn_models::interface::session::{CodexSessionProvider, SessionHost, SessionProvider};
use cairn_models::session::claude::ClaudeCli;
use cairn_models::session::codex::managed::{UpdateOutcome, UPDATE_INTERVAL};
use cairn_models::session::codex::{CodexAppServer, ManagedCodex};

use cairn_db::storage::run_db_blocking;
use cairn_db::turso::params;
use cairn_models::accounts::AccountInfo;
use cairn_models::interface::{
    Catalog, CatalogProvider, ListedProvider, ModelScope, Models, SessionState, SessionView,
    TierBinding, LOCAL_HOST,
};
use cairn_models::types::catalog::{self, ServedVerb, CLAUDE_FAMILY_BACKEND};
use cairn_models::types::pool::ProviderPoolSnapshot;
use serde_json::Value;

use super::Orchestrator;
use crate::config::presets::{load_effective_presets, resolve_runtime_selection, PresetsConfig};
use crate::db::DbState;
use crate::storage::RowExt;

/// The cairn-models facet's launch side as this orchestrator uses it.
///
/// Every HTTP token request goes through `complete`; every Claude process is
/// opened by `claude`, and every Codex app-server and thread by `codex`, on
/// `host`. The builder installs the implementations that
/// reach providers, with sessions on this machine; a test replaces them to
/// observe or script every launch without touching a network or a CLI.
#[derive(Clone)]
pub struct ModelRuntime {
    pub complete: Arc<dyn Complete>,
    pub claude: Arc<dyn SessionProvider>,
    pub codex: Arc<dyn CodexSessionProvider>,
    /// Where session processes run.
    pub host: Arc<dyn SessionHost>,
    /// The Codex CLI the local host launches, installed and kept on the
    /// published pin by Cairn.
    pub codex_cli: Arc<ManagedCodex>,
}

impl ModelRuntime {
    /// The implementations that reach providers, with sessions run as
    /// children of this runner.
    pub(crate) fn local(
        process: Arc<dyn crate::services::ProcessSpawner>,
        process_state: Arc<crate::agent_process::process::AgentProcessState>,
    ) -> Self {
        let codex_cli = Arc::new(ManagedCodex::for_host());
        Self {
            complete: Arc::new(HttpComplete),
            claude: Arc::new(ClaudeCli),
            codex: Arc::new(CodexAppServer),
            host: Arc::new(
                cairn_models::session::local::LocalSessionHost::new(
                    process,
                    crate::backends::codex::cairn_codex_home(),
                    codex_cli.clone(),
                )
                .with_claude_resolver(Arc::new(move || {
                    crate::orchestrator::session::get_claude_path(&process_state)
                })),
            ),
            codex_cli,
        }
    }
}

impl Orchestrator {
    /// Keep the managed Codex on the published pin: once now, then every
    /// [`UPDATE_INTERVAL`]. A swap refreshes the model catalog from the new
    /// release and retires pooled app-servers on the old one; running
    /// sessions finish on the release they started with.
    pub fn spawn_managed_codex_follow(&self) {
        let orch = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(UPDATE_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                orch.follow_managed_codex(false).await;
            }
        });
    }

    /// Retry an install that failed or never ran, unless one is already in
    /// progress. Onboarding's "Check again" lands here.
    pub fn kick_managed_codex_install(&self) {
        let orch = self.clone();
        tokio::spawn(async move { orch.follow_managed_codex(true).await });
    }

    async fn follow_managed_codex(&self, only_if_idle: bool) {
        let codex = self.models_runtime().codex_cli.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            if only_if_idle {
                codex.update_if_idle()
            } else {
                Some(codex.update())
            }
        })
        .await;
        match outcome {
            Ok(Some(Ok(UpdateOutcome::Installed { resolved, previous }))) => {
                log::info!(
                    "managed codex: now on {} (was {})",
                    resolved.version,
                    previous.as_deref().unwrap_or("not installed")
                );
                self.codex_pool.retire_versions_except(&resolved.version);
                self.spawn_model_catalog_refresh();
            }
            Ok(Some(Ok(UpdateOutcome::Current(_)))) | Ok(None) => {}
            // `ManagedCodex` logs and records the refusal; status reports it.
            Ok(Some(Err(_))) => {}
            Err(error) => log::warn!("managed codex: the update task failed: {error}"),
        }
    }
}

impl Orchestrator {
    /// The presets a scope resolves against: the workspace's, plus the
    /// project's overrides when the scope names one.
    fn models_presets(&self, scope: &ModelScope) -> Result<PresetsConfig, String> {
        let project_path = scope
            .project_id
            .as_deref()
            .map(|project_id| self.project_path(project_id))
            .transpose()?;
        Ok(load_effective_presets(
            &self.config_dir,
            project_path.as_deref(),
        ))
    }
}

/// The tiers bound on `provider`, in the workspace's tier order.
///
/// Whether a binding is the tier's default is asked of resolution itself, so
/// this can never claim a default that an unqualified tier reference would not
/// actually land on.
fn tier_bindings(provider: &str, presets: &PresetsConfig) -> Vec<TierBinding> {
    presets
        .tiers
        .iter()
        .filter_map(|tier| {
            let preset = presets.backends.get(provider)?.get(tier)?;
            let options: BTreeMap<String, Value> = preset
                .options
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        serde_json::to_value(value).unwrap_or(Value::Null),
                    )
                })
                .collect();
            let is_tier_default = resolve_runtime_selection(Some(tier), None, presets)
                .is_ok_and(|(selection, _)| selection.backend == provider);
            Some(TierBinding {
                tier: tier.clone(),
                model: preset.model.as_str().to_string(),
                options,
                is_tier_default,
            })
        })
        .collect()
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
        let scope = ModelScope {
            project_id: project_id.map(str::to_string),
        };
        // A project the config cannot place still has the workspace's tiers.
        let presets = self
            .models_presets(&scope)
            .unwrap_or_else(|_| load_effective_presets(&self.config_dir, None));
        self.provider_record(
            key,
            project_id,
            &presets,
            &self.provider_catalog(),
            self.provider_enabled(key),
        )
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
        presets: &PresetsConfig,
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
            tiers: tier_bindings(key, presets),
            serves: self.provider_serves(key, project_id),
            unable: self.decision_observations.unable(key),
        }
    }
}

impl Models for Orchestrator {
    fn catalog(&self, scope: &ModelScope) -> Result<Catalog, String> {
        let presets = self.models_presets(scope)?;
        let descriptors = self.provider_catalog();
        let enabled = self.enabled_providers();

        let mut providers: Vec<CatalogProvider> = enabled
            .iter()
            .map(|key| {
                self.provider_record(
                    key,
                    scope.project_id.as_deref(),
                    &presets,
                    &descriptors,
                    true,
                )
            })
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
                    tiers: Vec::new(),
                    serves: Vec::new(),
                    unable: Vec::new(),
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

    pub(crate) async fn bare_orchestrator() -> Orchestrator {
        bare_orchestrator_with(Arc::new(cairn_models::subscription::Unsubscribed)).await
    }

    /// [`bare_orchestrator`] with `subscriptions` installed.
    pub(crate) async fn bare_orchestrator_with(
        subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    ) -> Orchestrator {
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

    fn json<T: serde::Serialize>(value: &T) -> Value {
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

        // Tier bindings: the page's matrix, from the same effective presets.
        let presets = orch.models_presets(&scope).unwrap();
        let claude = catalog
            .installed()
            .find(|provider| provider.descriptor.key == "claude")
            .unwrap();
        let matrix: Vec<(String, String)> = presets
            .tiers
            .iter()
            .filter_map(|tier| {
                let preset = presets.backends.get("claude")?.get(tier)?;
                Some((tier.clone(), preset.model.as_str().to_string()))
            })
            .collect();
        let bound: Vec<(String, String)> = claude
            .tiers
            .iter()
            .map(|binding| (binding.tier.clone(), binding.model.clone()))
            .collect();
        assert!(!bound.is_empty());
        assert_eq!(bound, matrix);

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
    async fn every_catalog_binding_is_what_resolution_returns() {
        let orch = seeded_orchestrator().await;
        let scope = ModelScope::workspace();
        let catalog = orch.catalog(&scope).unwrap();
        let presets = orch.models_presets(&scope).unwrap();
        let mut checked = 0;
        for provider in catalog.installed() {
            let key = provider.descriptor.key.as_ref();
            for binding in &provider.tiers {
                let reference = format!("{key}/{}", binding.tier);
                let (selection, _) = resolve_runtime_selection(Some(&reference), None, &presets)
                    .unwrap_or_else(|error| panic!("{reference} does not resolve: {error}"));
                assert_eq!(selection.backend, key, "{reference}");
                assert_eq!(selection.model.as_str(), binding.model, "{reference}");
                if binding.is_tier_default {
                    let (unqualified, _) =
                        resolve_runtime_selection(Some(&binding.tier), None, &presets).unwrap();
                    assert_eq!(unqualified.backend, key, "{}", binding.tier);
                }
                checked += 1;
            }
        }
        assert!(checked > 0, "the seeded workspace binds tiers");
        // Each tier has exactly one default among installed providers.
        for tier in &presets.tiers {
            let defaults = catalog
                .installed()
                .flat_map(|provider| provider.tiers.iter())
                .filter(|binding| &binding.tier == tier && binding.is_tier_default)
                .count();
            assert!(defaults <= 1, "{tier} has {defaults} defaults");
        }
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
