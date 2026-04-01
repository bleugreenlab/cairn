//! Orchestrator identity operations.

use crate::identity::local;
use crate::identity::{
    AccountInfo, AccountOverrides, ApiProvider, GitIdentity, IdentityStore, ProviderAccount,
    ProviderAuth, UserIdentity,
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
            .and_then(|guard| guard.as_ref().map(|store| store.resolve(None)))
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
        overrides: Option<&AccountOverrides>,
    ) -> Option<UserIdentity> {
        self.identity_store
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|store| store.resolve(overrides)))
    }

    // === Account CRUD ===

    /// List all accounts (configured + detected local CLI), masked for frontend.
    pub fn list_accounts(&self) -> Vec<AccountInfo> {
        let mut store = match self.get_identity_store() {
            Some(s) => s,
            None => return vec![],
        };

        // Merge ephemeral local CLI accounts
        let local_accounts = local::detect_local_accounts();
        for local_acc in &local_accounts {
            if !store.has_local_cli(local_acc.api_provider) {
                store.accounts.push(local_acc.clone());
            }
        }

        store.accounts.iter().map(AccountInfo::from).collect()
    }

    /// Add a new account to the store.
    pub fn add_account(
        &self,
        api_provider: ApiProvider,
        label: String,
        auth: ProviderAuth,
    ) -> Result<AccountInfo, String> {
        let mut store = self
            .get_identity_store()
            .unwrap_or_else(local::identity_store_from_git_config);

        let now = chrono::Utc::now().timestamp();
        let max_sort = store
            .accounts
            .iter()
            .filter(|a| a.api_provider == api_provider)
            .map(|a| a.sort_order)
            .max()
            .unwrap_or(-1);

        let account = ProviderAccount {
            id: format!("acc_{}", uuid::Uuid::new_v4()),
            label,
            api_provider,
            source: crate::identity::AccountSource::Configured,
            auth,
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
        let mut store = self.get_identity_store().ok_or("No identity store")?;

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
    use crate::agent_process::process::AgentProcessState;
    use crate::db::DbState;
    use crate::mcp::McpAuthState;
    use crate::orchestrator::AccountManager;
    use crate::services::testing::TestServicesBuilder;
    use crate::services::PtyState;
    use crate::test_utils::test_diesel_conn;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex, OnceLock, RwLock};
    use tempfile::TempDir;

    fn test_orchestrator() -> Orchestrator {
        let conn = test_diesel_conn();
        let db = Arc::new(DbState {
            conn: Mutex::new(conn),
        });
        let services = Arc::new(TestServicesBuilder::new().build());
        let config_dir = TempDir::new().unwrap();
        let account_manager = Arc::new(AccountManager::new(db.clone(), services.emitter.clone()));
        let sync_tx = Arc::new(Mutex::new(None));
        Orchestrator {
            db,
            services: services.clone(),
            process_state: Arc::new(AgentProcessState::default()),
            mcp_auth: Arc::new(McpAuthState::new(config_dir.path().to_path_buf())),
            warm_gc: None,
            pty_state: Arc::new(PtyState::default()),
            permission_responses: tokio::sync::broadcast::channel(16).0,
            run_completions: tokio::sync::broadcast::channel(64).0,
            prompt_responses: tokio::sync::broadcast::channel(16).0,
            trigger_events: tokio::sync::broadcast::channel(256).0,
            session_allowed_tools: Arc::new(Mutex::new(HashSet::new())),
            identity_store: Arc::new(Mutex::new(None)),
            mcp_binary_path: "cairn-mcp".to_string(),
            config_dir: config_dir.keep(),
            schema_dir: None,
            mcp_callback_port: 3847,
            embedding_engine: None,
            vibe_state: None,
            account_manager,
            sync_tx: sync_tx.clone(),
            notifier: crate::notify::Notifier::new(sync_tx, services.emitter.clone()),
            api_config: crate::api::ApiConfig::default(),
            effect_tx: None,
            model_catalog: Arc::new(RwLock::new(HashMap::new())),
            provider_usage_snapshots: Default::default(),
            executor: Arc::new(OnceLock::new()),
        }
    }

    #[test]
    fn save_identity_store_refreshes_model_catalog() {
        let orch = test_orchestrator();
        orch.model_catalog.write().unwrap().insert(
            "codex".to_string(),
            crate::backends::ProviderModelCatalog {
                backend: "codex".to_string(),
                models: vec![],
                refreshed_at: None,
                error: Some("stale".to_string()),
            },
        );

        orch.save_identity_store(IdentityStore {
            user_id: "user-1".to_string(),
            accounts: vec![],
            git_identities: vec![],
            project_overrides: HashMap::new(),
        })
        .unwrap();

        let catalog = orch.get_model_catalog();
        assert_eq!(catalog.len(), 2);
        assert!(catalog.iter().all(|entry| entry.refreshed_at.is_some()));
    }
}
