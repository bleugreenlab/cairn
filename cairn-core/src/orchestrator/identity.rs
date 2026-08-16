//! Orchestrator identity operations.

use crate::identity::local;
use crate::identity::{
    AccountInfo, AccountOverrides, AccountSource, ApiProvider, GitIdentity, IdentityStore,
    ProviderAccount, ProviderAuth, RoutedProvider, UserIdentity,
};

use super::Orchestrator;

impl Orchestrator {
    // === Backward-compatible API ===

    /// Get the current user identity, if configured.
    /// Resolves the multi-account store to a single `UserIdentity`.
    pub fn get_identity(&self) -> Option<UserIdentity> {
        self.identity_store
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|store| store.resolve(None, None)))
    }

    pub fn resolve_provider_account(
        &self,
        backend: &str,
        account_id: &str,
    ) -> Option<UserIdentity> {
        self.get_identity_store()?
            .resolve_with_provider_account(backend, account_id)
    }

    // === New multi-account API ===

    /// Get the full identity store.
    pub(crate) fn get_identity_store(&self) -> Option<IdentityStore> {
        self.identity_store
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Save the full identity store to disk and update in-memory state.
    ///
    /// Persistence only, and deliberately so. Model discovery is a consequence
    /// of *credentials* changing, not of the store being written, so the few
    /// paths that change credentials ask for a catalog refresh themselves.
    /// Everything else that writes this file — a git identity rename, a
    /// reorder, a rate-limit block recorded from the Claude stream reader —
    /// leaves the catalog alone. Discovery must also never run inline here:
    /// this is reached from async invoke tasks, and the discovery clients own
    /// tokio runtimes that panic when dropped inside one.
    pub(crate) fn save_identity_store(&self, mut store: IdentityStore) -> Result<(), String> {
        local::save_identity_store(&self.config_dir, &store)?;
        // Stamp where it now lives, so the in-memory copy resolves profile
        // paths against the same root it was just written to.
        store.config_dir = self.config_dir.clone();
        if let Ok(mut guard) = self.identity_store.lock() {
            *guard = Some(store);
        }
        Ok(())
    }

    /// Resolve identity for a specific project (with overrides).
    pub(crate) fn resolve_identity_for_project(
        &self,
        project_id: Option<&str>,
        overrides: Option<&AccountOverrides>,
    ) -> Option<UserIdentity> {
        self.identity_store.lock().ok().and_then(|guard| {
            guard
                .as_ref()
                .map(|store| store.resolve(project_id, overrides))
        })
    }

    /// Select a subscription account for a provider by remaining usage and
    /// resolve the runtime identity to it.
    pub(crate) fn select_routed_identity(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        excluded_account_id: Option<&str>,
    ) -> Option<(String, UserIdentity)> {
        let assignments = crate::storage::run_db_blocking({
            let db = self.db.local.clone();
            move || async move { crate::sessions::queries::account_assignments(&db).await }
        })
        .unwrap_or_default();
        let store = self.get_identity_store()?;
        let account = store.select_routed_account(
            provider,
            project_id,
            override_id,
            excluded_account_id,
            &assignments,
            chrono::Utc::now().timestamp(),
        )?;
        let account_id = account.id.clone();
        let identity = store.resolve_with_routed_account(provider, project_id, &account_id);
        Some((account_id, identity))
    }

    pub(crate) fn resolve_available_routed_account(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        account_id: &str,
    ) -> Option<UserIdentity> {
        let store = self.get_identity_store()?;
        store
            .routed_account_is_available(provider, account_id, chrono::Utc::now().timestamp())
            .then(|| store.resolve_with_routed_account(provider, project_id, account_id))
    }

    /// Resolve only the git author/committer identity for a project.
    pub(crate) fn resolve_git_identity_for_project(
        &self,
        project_id: Option<&str>,
    ) -> Option<(String, String)> {
        let overrides = project_id.and_then(|pid| {
            self.get_identity_store()
                .and_then(|store| store.project_overrides.get(pid).cloned())
        });
        self.resolve_identity_for_project(project_id, overrides.as_ref())
            .and_then(|identity| {
                if identity.name.trim().is_empty() || identity.email.trim().is_empty() {
                    None
                } else {
                    Some((identity.name, identity.email))
                }
            })
    }

    // === Account CRUD ===

    /// List accounts visible in a scope. Global scope returns shared accounts only;
    /// project scope returns shared accounts plus accounts private to that project.
    ///
    /// Every account here is one Cairn holds a credential for. An installed CLI
    /// that happens to be signed in is not an account: it cannot be routed by
    /// usage, ordered against the others, or signed out of from here.
    pub fn list_accounts(&self, project_id: Option<&str>) -> Vec<AccountInfo> {
        let store = match self.get_identity_store() {
            Some(s) => s,
            None => return vec![],
        };

        store
            .accounts
            .iter()
            .filter(|account| {
                account.project_id.is_none() || account.project_id.as_deref() == project_id
            })
            .map(AccountInfo::from)
            .collect()
    }

    /// Add a new account to the store.
    pub fn add_account(
        &self,
        api_provider: ApiProvider,
        label: String,
        auth: ProviderAuth,
        project_id: Option<String>,
    ) -> Result<AccountInfo, String> {
        let mut store = self
            .get_identity_store()
            .unwrap_or_else(|| local::identity_store_from_git_config(&self.config_dir));

        let now = chrono::Utc::now().timestamp();
        let max_sort = store
            .accounts
            .iter()
            .filter(|a| a.api_provider == api_provider && a.project_id == project_id)
            .map(|a| a.sort_order)
            .max()
            .unwrap_or(-1);

        let account = ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label,
            api_provider,
            source: crate::identity::AccountSource::Configured,
            auth,
            project_id,
            sort_order: max_sort + 1,
            created_at: now,
            last_used_at: None,
            email: None,
            plan: None,
            health: None,
        };

        let info = AccountInfo::from(&account);
        store.accounts.push(account);
        self.save_identity_store(store)?;

        self.emit_config_changed();
        self.spawn_model_catalog_refresh();
        Ok(info)
    }

    /// Store a completed ChatGPT OAuth login as a Cairn-owned Codex account.
    ///
    /// Codex refresh tokens are single-use, so Cairn must hold exactly one
    /// credential per ChatGPT account and replace it in place when that account
    /// signs in again — a second profile for the same subscription would hold a
    /// refresh token the provider has already retired, and whichever session
    /// reached it first would knock the other out. That invariant is per
    /// account, not global: logins carrying different ChatGPT account ids are
    /// separate faucets, so they all persist and sessions route across them by
    /// remaining usage.
    ///
    /// The match is on the ChatGPT account id the credential carries rather than
    /// on its bytes, which every token refresh rewrites.
    pub fn upsert_codex_oauth_account(
        &self,
        label: String,
        auth_json: String,
        project_id: Option<String>,
    ) -> Result<AccountInfo, String> {
        let identity = crate::backends::codex::codex_account_identity(&auth_json)?;
        let mut store = self
            .get_identity_store()
            .unwrap_or_else(|| local::identity_store_from_git_config(&self.config_dir));

        let now = chrono::Utc::now().timestamp();
        let target_id = store
            .accounts
            .iter()
            .find(|account| {
                account.api_provider == ApiProvider::OpenAI
                    && account.source == AccountSource::Configured
                    && account.project_id == project_id
                    && matches!(&account.auth, ProviderAuth::OAuthToken { value }
                    if crate::backends::codex::codex_account_identity(value)
                        .is_ok_and(|existing| {
                            existing.chatgpt_account_id == identity.chatgpt_account_id
                        }))
            })
            .map(|account| account.id.clone());

        let label = identity.email.clone().unwrap_or(label);
        let info = if let Some(target_id) = target_id {
            let account = store
                .accounts
                .iter_mut()
                .find(|account| account.id == target_id)
                .ok_or_else(|| "Codex OAuth account disappeared during upsert".to_string())?;
            account.label = label;
            account.auth = ProviderAuth::OAuthToken { value: auth_json };
            account.email = identity.email;
            account.plan = identity.plan;
            account.last_used_at = Some(now);
            AccountInfo::from(&*account)
        } else {
            let max_sort = store
                .accounts
                .iter()
                .filter(|account| {
                    account.api_provider == ApiProvider::OpenAI && account.project_id == project_id
                })
                .map(|account| account.sort_order)
                .max()
                .unwrap_or(-1);
            let account = ProviderAccount {
                id: format!("acc_{}", uuid::Uuid::new_v4()),
                label,
                api_provider: ApiProvider::OpenAI,
                source: AccountSource::Configured,
                auth: ProviderAuth::OAuthToken { value: auth_json },
                project_id,
                sort_order: max_sort + 1,
                created_at: now,
                last_used_at: Some(now),
                email: identity.email,
                plan: identity.plan,
                health: None,
            };
            let info = AccountInfo::from(&account);
            store.accounts.push(account);
            info
        };

        self.save_identity_store(store)?;
        self.emit_config_changed();
        self.spawn_model_catalog_refresh();
        Ok(info)
    }

    /// Record provider-reported metadata after a managed login completes.
    pub fn update_account_metadata(
        &self,
        id: &str,
        email: Option<String>,
        plan: Option<String>,
    ) -> Result<AccountInfo, String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        account.email = email.clone();
        account.plan = plan;
        if let Some(email) = email {
            account.label = email;
        }
        let info = AccountInfo::from(&*account);
        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(info)
    }

    /// Record what Cairn last knew about an account's remaining subscription
    /// usage. This is the only writer of account health, from both of its
    /// sources: a usage probe (no block) and a blocking rate-limit event.
    ///
    /// Returns the account's display label, for the message a caller reporting
    /// a block writes into the session.
    pub(crate) fn record_account_health(
        &self,
        id: &str,
        windows: Vec<crate::models::ProviderUsageWindow>,
        blocked_until: Option<i64>,
    ) -> Result<String, String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        account.health = Some(crate::identity::ProviderAccountHealth {
            windows,
            blocked_until,
            captured_at: chrono::Utc::now().timestamp(),
        });
        let label = account
            .email
            .clone()
            .unwrap_or_else(|| account.label.clone());
        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(label)
    }

    /// Probe a subscription account's remaining usage and record it.
    ///
    /// Detached, because the probe drives a provider CLI and the caller — a
    /// sign-in that just completed — must not wait on it. Nothing depends on it
    /// finishing: an account with no snapshot is routable at full headroom, and
    /// this replaces that assumption with a measurement.
    pub fn capture_account_usage(&self, provider: RoutedProvider, account_id: String) {
        let orch = self.clone();
        let spawned = std::thread::Builder::new()
            .name("provider-usage-capture".to_string())
            .spawn(move || {
                let snapshot = match provider {
                    RoutedProvider::Claude => {
                        crate::backends::collect_claude_usage_snapshot(&orch, Some(&account_id))
                    }
                    RoutedProvider::Codex => crate::backends::codex::collect_codex_usage_snapshot(
                        &orch,
                        Some(&account_id),
                    ),
                };
                if snapshot.windows.is_empty() {
                    log::debug!(
                        "No {} usage windows reported for account {account_id}",
                        provider.backend()
                    );
                    return;
                }
                // The panel reads per (backend, account), so the measurement
                // that arms routing also fills that account's usage card.
                orch.store_provider_account_usage_snapshot(
                    Some(account_id.clone()),
                    snapshot.clone(),
                );
                if let Err(error) = orch.record_account_health(&account_id, snapshot.windows, None)
                {
                    log::warn!(
                        "Failed to record {} usage for {account_id}: {error}",
                        provider.backend()
                    );
                }
            });
        if let Err(error) = spawned {
            log::warn!("Failed to start provider usage capture: {error}");
        }
    }

    /// Update an existing account's label.
    pub fn update_account(&self, id: &str, label: Option<String>) -> Result<AccountInfo, String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        let account = store
            .accounts
            .iter_mut()
            .find(|a| a.id == id)
            .ok_or_else(|| format!("Account not found: {}", id))?;

        if let Some(l) = label {
            account.label = l;
        }

        let info = AccountInfo::from(&*account);
        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(info)
    }

    /// Remove an account from the store.
    pub fn remove_account(&self, id: &str) -> Result<(), String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        let account = store
            .accounts
            .iter()
            .find(|account| account.id == id)
            .cloned()
            .ok_or_else(|| format!("Account not found: {id}"))?;
        if matches!(account.auth, ProviderAuth::ClaudeProfile) {
            let claude = crate::env::find_binary("claude").map_err(|_| {
                "Claude CLI not found. Install it before removing this login.".to_string()
            })?;
            crate::identity::claude_profile::logout(
                std::path::Path::new(&claude),
                &crate::identity::claude_profile::profile_dir_in(&self.config_dir, id),
            )?;
        }

        let initial_len = store.accounts.len();
        store.accounts.retain(|a| a.id != id);

        if store.accounts.len() == initial_len {
            return Err(format!("Account not found: {}", id));
        }

        for overrides in store.project_overrides.values_mut() {
            if overrides.anthropic_account_id.as_deref() == Some(id) {
                overrides.anthropic_account_id = None;
            }
            if overrides.openai_account_id.as_deref() == Some(id) {
                overrides.openai_account_id = None;
            }
            if overrides.github_account_id.as_deref() == Some(id) {
                overrides.github_account_id = None;
            }
        }

        self.save_identity_store(store)?;
        self.emit_config_changed();
        self.spawn_model_catalog_refresh();
        Ok(())
    }

    /// Reorder accounts within a provider.
    pub fn reorder_accounts(
        &self,
        api_provider: ApiProvider,
        ordered_ids: &[String],
    ) -> Result<(), String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        for (idx, id) in ordered_ids.iter().enumerate() {
            if let Some(account) = store
                .accounts
                .iter_mut()
                .find(|a| a.id == *id && a.api_provider == api_provider)
            {
                account.sort_order = idx as i32;
            }
        }

        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Retired logins ===

    /// Anthropic logins the profiles-only migration dropped, still unread.
    pub fn list_retired_logins(&self) -> Vec<crate::identity::RetiredLogin> {
        self.get_identity_store()
            .map(|store| store.retired_logins)
            .unwrap_or_default()
    }

    /// Acknowledge the retired logins, so settings stops reporting them.
    pub fn dismiss_retired_logins(&self) -> Result<(), String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;
        if store.retired_logins.is_empty() {
            return Ok(());
        }
        store.retired_logins.clear();
        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Git Identity CRUD ===

    /// List all git identities.
    pub fn list_git_identities(&self) -> Vec<GitIdentity> {
        self.get_identity_store()
            .map(|s| s.git_identities)
            .unwrap_or_default()
    }

    /// Add a new git identity.
    pub fn add_git_identity(
        &self,
        label: String,
        name: String,
        email: String,
    ) -> Result<GitIdentity, String> {
        let mut store = self
            .get_identity_store()
            .unwrap_or_else(|| local::identity_store_from_git_config(&self.config_dir));

        let max_sort = store
            .git_identities
            .iter()
            .map(|g| g.sort_order)
            .max()
            .unwrap_or(-1);

        let identity = GitIdentity {
            id: format!("gi_{}", uuid::Uuid::new_v4()),
            label,
            name,
            email,
            sort_order: max_sort + 1,
        };

        let result = identity.clone();
        store.git_identities.push(identity);
        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(result)
    }

    /// Update an existing git identity.
    pub fn update_git_identity(
        &self,
        id: &str,
        label: Option<String>,
        name: Option<String>,
        email: Option<String>,
    ) -> Result<GitIdentity, String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        let gi = store
            .git_identities
            .iter_mut()
            .find(|g| g.id == id)
            .ok_or_else(|| format!("Git identity not found: {}", id))?;

        if let Some(l) = label {
            gi.label = l;
        }
        if let Some(n) = name {
            gi.name = n;
        }
        if let Some(e) = email {
            gi.email = e;
        }

        let result = gi.clone();
        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(result)
    }

    /// Remove a git identity.
    pub fn remove_git_identity(&self, id: &str) -> Result<(), String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        let initial_len = store.git_identities.len();
        store.git_identities.retain(|g| g.id != id);

        if store.git_identities.len() == initial_len {
            return Err(format!("Git identity not found: {}", id));
        }

        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    /// Reorder git identities.
    pub fn reorder_git_identities(&self, ordered_ids: &[String]) -> Result<(), String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        for (idx, id) in ordered_ids.iter().enumerate() {
            if let Some(gi) = store.git_identities.iter_mut().find(|g| g.id == *id) {
                gi.sort_order = idx as i32;
            }
        }

        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Project Overrides ===

    /// Get account overrides for a project.
    pub fn get_project_overrides(&self, project_id: &str) -> Option<AccountOverrides> {
        self.get_identity_store()
            .and_then(|store| store.project_overrides.get(project_id).cloned())
    }

    /// Set account overrides for a project.
    pub fn set_project_overrides(
        &self,
        project_id: &str,
        overrides: Option<AccountOverrides>,
    ) -> Result<(), String> {
        let mut store = self
            .get_identity_store()
            .unwrap_or_else(|| local::identity_store_from_git_config(&self.config_dir));

        match overrides {
            Some(o) => {
                store.project_overrides.insert(project_id.to_string(), o);
            }
            None => {
                store.project_overrides.remove(project_id);
            }
        }

        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Helper ===

    fn emit_config_changed(&self) {
        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "identity"}),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use std::sync::Arc;

    async fn test_orchestrator() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    /// Persisting the identity store must not discover models inline.
    ///
    /// This is served from an async invoke task, and provider discovery builds
    /// `reqwest::blocking` clients that each own a tokio runtime — dropping one
    /// inside a runtime panics the task, which is how saving an OpenCode Go key
    /// failed with a 500. The multi-thread flavor reproduces that context, and
    /// an untouched catalog is the evidence no discovery ran: a refresh records
    /// an entry per known backend even when every one of them fails.
    #[tokio::test(flavor = "multi_thread")]
    async fn saving_the_identity_store_does_not_discover_models() {
        let orch = test_orchestrator().await;

        orch.save_identity_store(local::identity_store_from_git_config(&orch.config_dir))
            .expect("saving the identity store should succeed");

        assert!(
            orch.get_model_catalog().is_empty(),
            "saving the identity store ran provider discovery inline; it must be spawned off the async task"
        );
    }

    /// A store persisted through the orchestrator resolves its managed profiles
    /// under the same root it was written to, rather than an inferred one.
    #[tokio::test(flavor = "multi_thread")]
    async fn saving_stamps_the_config_root_onto_the_in_memory_store() {
        let orch = test_orchestrator().await;

        orch.save_identity_store(local::identity_store_from_git_config(&orch.config_dir))
            .expect("saving the identity store should succeed");

        let stored = orch.get_identity_store().expect("store is held in memory");
        assert_eq!(stored.config_dir, orch.config_dir);
    }

    /// A Codex `auth.json` shaped like the one the app-server writes: the
    /// ChatGPT account id rides in the access token's claims, the label and
    /// plan in the id token's.
    fn codex_auth_json(chatgpt_account_id: &str, email: &str, refresh_token: &str) -> String {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;

        let jwt = |payload: serde_json::Value| {
            format!(
                "header.{}.signature",
                URL_SAFE_NO_PAD.encode(payload.to_string())
            )
        };
        let id_token = jwt(serde_json::json!({
            "email": email,
            "https://api.openai.com/auth": { "chatgpt_plan_type": "pro" },
        }));
        let access_token = jwt(serde_json::json!({
            "https://api.openai.com/auth": { "chatgpt_account_id": chatgpt_account_id },
        }));
        serde_json::json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": id_token,
                "access_token": access_token,
                "refresh_token": refresh_token,
            },
        })
        .to_string()
    }

    /// The OpenAI subscription credentials the store currently holds. A store
    /// that was never written holds none, which is what a refused login leaves.
    fn openai_oauth_accounts(orch: &Orchestrator) -> Vec<ProviderAccount> {
        orch.get_identity_store()
            .map(|store| store.accounts)
            .unwrap_or_default()
            .into_iter()
            .filter(|account| {
                account.api_provider == ApiProvider::OpenAI
                    && matches!(account.auth, ProviderAuth::OAuthToken { .. })
            })
            .collect()
    }

    /// Subscriptions are inventory: signing in to a second ChatGPT account adds
    /// a faucet rather than replacing the first one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_second_chatgpt_login_does_not_evict_the_first() {
        let orch = test_orchestrator().await;

        let first = orch
            .upsert_codex_oauth_account(
                "Codex OAuth".to_string(),
                codex_auth_json("acct-one", "one@example.com", "refresh-one"),
                None,
            )
            .expect("first account stored");
        let second = orch
            .upsert_codex_oauth_account(
                "Codex OAuth".to_string(),
                codex_auth_json("acct-two", "two@example.com", "refresh-two"),
                None,
            )
            .expect("second account stored");

        assert_ne!(first.id, second.id);
        let accounts = openai_oauth_accounts(&orch);
        assert_eq!(accounts.len(), 2, "both subscriptions persist");
        assert_eq!(
            accounts
                .iter()
                .filter_map(|account| account.email.clone())
                .collect::<Vec<_>>(),
            vec!["one@example.com".to_string(), "two@example.com".to_string()],
            "each account keeps its own identity"
        );
        assert_ne!(
            accounts[0].sort_order, accounts[1].sort_order,
            "the newcomer takes its own priority slot"
        );
    }

    /// Reconnecting a subscription replaces that account's credential in place.
    /// A ChatGPT refresh token is single-use, so a duplicate profile would hold
    /// one the provider has already retired.
    #[tokio::test(flavor = "multi_thread")]
    async fn reconnecting_a_chatgpt_account_replaces_its_credential_in_place() {
        let orch = test_orchestrator().await;

        orch.upsert_codex_oauth_account(
            "Codex OAuth".to_string(),
            codex_auth_json("acct-other", "other@example.com", "refresh-other"),
            None,
        )
        .expect("unrelated account stored");
        let first = orch
            .upsert_codex_oauth_account(
                "Codex OAuth".to_string(),
                codex_auth_json("acct-one", "one@example.com", "refresh-one"),
                None,
            )
            .expect("account stored");

        let reconnected_json = codex_auth_json("acct-one", "renamed@example.com", "refresh-two");
        let reconnected = orch
            .upsert_codex_oauth_account("Codex OAuth".to_string(), reconnected_json.clone(), None)
            .expect("account reconnected");

        assert_eq!(
            reconnected.id, first.id,
            "the same ChatGPT account keeps its Cairn account"
        );
        let accounts = openai_oauth_accounts(&orch);
        assert_eq!(accounts.len(), 2, "reconnecting adds nothing");
        let stored = accounts
            .iter()
            .find(|account| account.id == first.id)
            .expect("the reconnected account is still stored");
        match &stored.auth {
            ProviderAuth::OAuthToken { value } => assert_eq!(
                value, &reconnected_json,
                "the newly issued credential replaced the retired one"
            ),
            other => panic!("expected an OAuth credential, got {other:?}"),
        }
        assert_eq!(stored.email.as_deref(), Some("renamed@example.com"));
        assert_eq!(stored.plan.as_deref(), Some("pro"));
        assert!(
            accounts
                .iter()
                .any(|account| account.email.as_deref() == Some("other@example.com")),
            "the other subscription is untouched"
        );
    }

    /// An `auth.json` with no ChatGPT account id cannot be keyed, and storing it
    /// anyway would make the next reconnect duplicate rather than replace.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_credential_without_a_chatgpt_account_id_is_refused() {
        let orch = test_orchestrator().await;

        let stored = orch.upsert_codex_oauth_account(
            "Codex OAuth".to_string(),
            serde_json::json!({ "auth_mode": "chatgpt", "tokens": {} }).to_string(),
            None,
        );

        assert!(stored.is_err());
        assert!(openai_oauth_accounts(&orch).is_empty());
    }
}
