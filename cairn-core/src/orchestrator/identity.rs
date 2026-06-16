//! Orchestrator identity operations.

use crate::identity::local;
use crate::identity::{
    AccountInfo, AccountOverrides, AccountSource, ApiProvider, GitIdentity, IdentityStore,
    ProviderAccount, ProviderAuth, UserIdentity,
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

    /// Save identity to the local store and update the in-memory state.
    /// Backward-compatible: maps `UserIdentity` fields into the store.
    pub fn save_identity(&self, identity: UserIdentity) -> Result<(), String> {
        local::save_local_identity(&self.config_dir, &identity)?;
        // Reload the store from disk (save_local_identity writes v2 format)
        let store = local::load_identity_store(&self.config_dir)?;
        if let Ok(mut guard) = self.identity_store.lock() {
            *guard = store;
        }
        self.refresh_model_catalog();
        Ok(())
    }

    /// Clear the stored identity (remove from disk and memory).
    pub fn clear_identity(&self) -> Result<(), String> {
        let path = self.config_dir.join("identity.yaml");
        if path.exists() {
            std::fs::remove_file(&path)
                .map_err(|e| format!("Failed to remove identity file: {}", e))?;
        }
        if let Ok(mut guard) = self.identity_store.lock() {
            *guard = None;
        }
        self.refresh_model_catalog();
        Ok(())
    }

    // === New multi-account API ===

    /// Get the full identity store.
    pub fn get_identity_store(&self) -> Option<IdentityStore> {
        self.identity_store
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    /// Save the full identity store to disk and update in-memory state.
    pub fn save_identity_store(&self, store: IdentityStore) -> Result<(), String> {
        local::save_identity_store(&self.config_dir, &store)?;
        if let Ok(mut guard) = self.identity_store.lock() {
            *guard = Some(store);
        }
        self.refresh_model_catalog();
        Ok(())
    }

    /// Resolve identity for a specific project (with overrides).
    pub fn resolve_identity_for_project(
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

    /// Resolve only the git author/committer identity for a project.
    pub fn resolve_git_identity_for_project(
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
    pub fn list_accounts(&self, project_id: Option<&str>) -> Vec<AccountInfo> {
        let mut store = match self.get_identity_store() {
            Some(s) => s,
            None => return vec![],
        };

        // Merge ephemeral local CLI accounts as shared accounts.
        let local_accounts = local::detect_local_accounts();
        for local_acc in &local_accounts {
            if !store.has_local_cli(local_acc.api_provider) {
                store.accounts.push(local_acc.clone());
            }
        }

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
            .unwrap_or_else(local::identity_store_from_git_config);

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
        };

        let info = AccountInfo::from(&account);
        store.accounts.push(account);
        self.save_identity_store(store)?;

        self.emit_config_changed();
        Ok(info)
    }

    /// Replace or create the single Cairn-owned OpenAI OAuth account for Codex.
    ///
    /// Codex refresh tokens are single-use, so reconnecting must not leave stale
    /// OAuth profiles ahead of the newly issued token. The OAuth account is
    /// promoted to OpenAI priority 0 and legacy OpenAI Local CLI entries are
    /// removed from the persisted store.
    pub fn upsert_codex_oauth_account(
        &self,
        label: String,
        auth_json: String,
        project_id: Option<String>,
    ) -> Result<AccountInfo, String> {
        let mut store = self
            .get_identity_store()
            .unwrap_or_else(local::identity_store_from_git_config);

        let now = chrono::Utc::now().timestamp();
        let target_id = store
            .accounts
            .iter()
            .filter(|account| {
                account.api_provider == ApiProvider::OpenAI
                    && account.source == AccountSource::Configured
                    && account.project_id == project_id
                    && matches!(&account.auth, ProviderAuth::OAuthToken { .. })
            })
            .min_by_key(|account| account.sort_order)
            .map(|account| account.id.clone());

        store.accounts.retain(|account| {
            if account.api_provider != ApiProvider::OpenAI {
                return true;
            }
            if account.project_id != project_id {
                return true;
            }
            if account.source == AccountSource::LocalCli {
                return false;
            }
            if matches!(&account.auth, ProviderAuth::OAuthToken { .. }) {
                return target_id.as_deref() == Some(account.id.as_str());
            }
            true
        });

        for account in store.accounts.iter_mut().filter(|account| {
            account.api_provider == ApiProvider::OpenAI && account.project_id == project_id
        }) {
            account.sort_order += 1;
        }

        let info = if let Some(target_id) = target_id {
            let account = store
                .accounts
                .iter_mut()
                .find(|account| account.id == target_id)
                .ok_or_else(|| "Codex OAuth account disappeared during upsert".to_string())?;
            account.label = label;
            account.auth = ProviderAuth::OAuthToken { value: auth_json };
            account.project_id = project_id.clone();
            account.sort_order = 0;
            account.last_used_at = Some(now);
            AccountInfo::from(&*account)
        } else {
            let account = ProviderAccount {
                id: format!("acc_{}", uuid::Uuid::new_v4()),
                label,
                api_provider: ApiProvider::OpenAI,
                source: AccountSource::Configured,
                auth: ProviderAuth::OAuthToken { value: auth_json },
                project_id,
                sort_order: 0,
                created_at: now,
                last_used_at: Some(now),
            };
            let info = AccountInfo::from(&account);
            store.accounts.push(account);
            info
        };

        self.save_identity_store(store)?;
        self.emit_config_changed();
        Ok(info)
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
        Ok(())
    }

    /// Reorder accounts within a provider.
    ///
    /// If reorder includes a Local CLI account that isn't persisted yet,
    /// it gets promoted into the store so its sort_order is remembered.
    pub fn reorder_accounts(
        &self,
        api_provider: ApiProvider,
        ordered_ids: &[String],
    ) -> Result<(), String> {
        let mut store = self.get_identity_store().ok_or("No identity store")?;

        // Promote any ephemeral Local CLI accounts into the store if referenced
        let local_accounts = local::detect_local_accounts();
        for id in ordered_ids {
            let in_store = store.accounts.iter().any(|a| a.id == *id);
            if !in_store {
                if let Some(local_acc) = local_accounts.iter().find(|a| a.id == *id) {
                    store.accounts.push(local_acc.clone());
                }
            }
        }

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
            .unwrap_or_else(local::identity_store_from_git_config);

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
            .unwrap_or_else(local::identity_store_from_git_config);

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
