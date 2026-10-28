//! Orchestrator identity and credential operations.

use crate::credentials::{
    AccountInfo, AccountOverrides, ApiProvider, CredentialStore, CredentialStoreHandle,
    ProviderAuth, RoutedProvider,
};
use crate::identity::{CredentialStoreExt, GitIdentity, UserIdentity};

use super::Orchestrator;

/// The one measurement of an account in flight. Taken by every probe that
/// records what it measures, so an account is never probed twice at once.
pub(super) struct ProviderUsageCaptureReservation {
    captures: super::ProviderUsageCaptures,
    key: (String, String),
}

impl ProviderUsageCaptureReservation {
    pub(super) fn acquire(
        captures: super::ProviderUsageCaptures,
        provider: RoutedProvider,
        account_id: &str,
    ) -> Option<Self> {
        let key = (provider.backend().to_string(), account_id.to_string());
        let inserted = captures.lock().ok()?.insert(key.clone());
        inserted.then_some(Self { captures, key })
    }
}

impl Drop for ProviderUsageCaptureReservation {
    fn drop(&mut self) {
        if let Ok(mut captures) = self.captures.lock() {
            captures.remove(&self.key);
        }
    }
}

impl Orchestrator {
    pub(crate) fn routed_account_unavailable_reason(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        requested_model: Option<&str>,
    ) -> Option<String> {
        let store = self.get_credential_store()?;
        let diagnostics = store.routed_account_diagnostics(
            provider,
            project_id,
            override_id,
            None,
            requested_model,
            chrono::Utc::now().timestamp(),
        );
        if override_id.is_some_and(|id| !diagnostics.iter().any(|item| item.account_id == id)) {
            return Some(
                "the explicitly selected account no longer exists or is outside this project"
                    .into(),
            );
        }
        let rejected: Vec<_> = diagnostics
            .iter()
            .filter_map(|item| {
                item.rejection
                    .as_deref()
                    .map(|reason| format!("{}: {reason}", item.account_id))
            })
            .collect();
        match rejected.as_slice() {
            [] => None,
            [reason] => Some(reason.clone()),
            reasons => Some(format!(
                "configured accounts were rejected: {}",
                reasons.join("; ")
            )),
        }
    }

    // === Backward-compatible API ===

    /// Get the current user identity, if configured.
    /// Resolves the multi-account store to a single `UserIdentity`.
    pub fn get_identity(&self) -> Option<UserIdentity> {
        self.credential_store
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|store| store.resolve(None, None)))
    }

    /// Record a probe's measurement of an account, unless the credential it
    /// read was rewritten while it ran. Answers whether it was recorded.
    pub(super) fn apply_captured_account_usage_if_current(
        &self,
        provider: RoutedProvider,
        account_id: &str,
        captured_generation: Option<String>,
        snapshot: cairn_models::types::usage::ProviderUsageSnapshot,
    ) -> Result<bool, String> {
        if provider == RoutedProvider::Claude {
            let current_generation = self
                .subscriptions
                .credential_generation(self, provider, account_id);
            if current_generation != captured_generation {
                log::debug!(
                    "Discarding Claude usage captured across a credential rewrite for account {account_id}"
                );
                return Ok(false);
            }
        }
        self.apply_captured_account_usage(account_id, snapshot)
            .map(|()| true)
    }

    pub fn resolve_provider_account(
        &self,
        backend: &str,
        account_id: &str,
    ) -> Option<UserIdentity> {
        self.get_credential_store()?
            .resolve_with_provider_account(backend, account_id)
    }

    // === New multi-account API ===

    /// The credential store as this process holds it. See
    /// [`CredentialStoreHandle`] for the read, save, update, and durable-read
    /// contract the methods below delegate to.
    pub fn credential_store_handle(&self) -> &CredentialStoreHandle {
        &self.credential_store
    }

    /// The subscription mechanics this runtime was built with.
    pub fn subscriptions(&self) -> &dyn cairn_models::subscription::Subscriptions {
        self.subscriptions.as_ref()
    }

    /// The facet implementations every launch goes through.
    pub fn models_runtime(&self) -> &super::models::ModelRuntime {
        &self.models_runtime
    }

    /// Get the full identity store.
    pub(crate) fn get_credential_store(&self) -> Option<CredentialStore> {
        self.credential_store.get()
    }

    /// Save the full identity store to disk and update in-memory state.
    ///
    /// Discovery must never run inline here: this is reached from async invoke
    /// tasks, and the discovery clients own tokio runtimes that panic when
    /// dropped inside one.
    pub(crate) fn save_credential_store(&self, store: CredentialStore) -> Result<(), String> {
        self.credential_store.save(store)
    }

    /// Resolve identity for a specific project (with overrides).
    pub(crate) fn resolve_identity_for_project(
        &self,
        project_id: Option<&str>,
        overrides: Option<&AccountOverrides>,
    ) -> Option<UserIdentity> {
        self.credential_store.lock().ok().and_then(|guard| {
            guard
                .as_ref()
                .map(|store| store.resolve(project_id, overrides))
        })
    }

    /// When the accounts a routing policy may select can next serve, from the
    /// stored health of each. See [`crate::credentials::CredentialStore::routed_availability`].
    pub(crate) fn routed_availability(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        explicit_account: Option<&str>,
        requested_model: Option<&str>,
        now: i64,
    ) -> crate::credentials::RoutedAvailability {
        self.get_credential_store()
            .map(|store| {
                store.routed_availability(
                    provider,
                    project_id,
                    explicit_account,
                    requested_model,
                    now,
                )
            })
            .unwrap_or(crate::credentials::RoutedAvailability::NoCandidates)
    }

    /// Select a subscription account for a provider by remaining usage and
    /// resolve the runtime identity to it.
    pub(crate) fn select_routed_identity(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        excluded_account_id: Option<&str>,
        requested_model: Option<&str>,
    ) -> Option<(String, UserIdentity)> {
        let assignments = crate::storage::run_db_blocking({
            let db = self.db.local.clone();
            move || async move { crate::sessions::queries::account_assignments(&db).await }
        })
        .unwrap_or_default();
        self.select_routed_identity_among(
            provider,
            project_id,
            override_id,
            excluded_account_id,
            requested_model,
            &assignments,
            true,
        )
    }

    /// [`Self::select_routed_identity`] against the assignments given rather
    /// than the live ones. `diagnose` logs why nothing was selected; a probe
    /// asked once per listed model leaves it off.
    // The routing facts are the store's own selection inputs, passed through.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn select_routed_identity_among(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        override_id: Option<&str>,
        excluded_account_id: Option<&str>,
        requested_model: Option<&str>,
        assignments: &[(String, i64)],
        diagnose: bool,
    ) -> Option<(String, UserIdentity)> {
        let store = self.get_credential_store()?;
        let account = store.select_routed_account(
            provider,
            project_id,
            override_id,
            excluded_account_id,
            requested_model,
            assignments,
            chrono::Utc::now().timestamp(),
        );
        if account.is_none() && diagnose {
            let diagnostics = store.routed_account_diagnostics(
                provider,
                project_id,
                override_id,
                excluded_account_id,
                requested_model,
                chrono::Utc::now().timestamp(),
            );
            let usage_snapshots: Vec<_> = self
                .provider_usage_snapshots
                .read()
                .map(|snapshots| {
                    diagnostics
                        .iter()
                        .filter_map(|item| {
                            snapshots
                                .get(&(
                                    provider.backend().to_string(),
                                    Some(item.account_id.clone()),
                                ))
                                .map(|snapshot| (item.account_id.clone(), snapshot.clone()))
                        })
                        .collect()
                })
                .unwrap_or_default();
            log::warn!(
                "No routed {} account selected: {}",
                provider.backend(),
                serde_json::to_string(&serde_json::json!({
                    "identityStore": diagnostics,
                    "providerUsageSnapshots": usage_snapshots,
                }))
                .unwrap_or_else(|_| "diagnostics unavailable".into())
            );
        }
        let account = account?;
        let account_id = account.id.clone();
        let identity = store.resolve_with_routed_account(provider, project_id, &account_id);
        Some((account_id, identity))
    }

    pub(crate) fn resolve_available_routed_account(
        &self,
        provider: RoutedProvider,
        project_id: Option<&str>,
        account_id: &str,
        requested_model: Option<&str>,
    ) -> Option<UserIdentity> {
        let store = self.get_credential_store()?;
        store
            .routed_account_is_available(
                provider,
                project_id,
                account_id,
                requested_model,
                chrono::Utc::now().timestamp(),
            )
            .then(|| store.resolve_with_routed_account(provider, project_id, account_id))
    }

    /// Resolve only the git author/committer identity for a project.
    pub(crate) fn resolve_git_identity_for_project(
        &self,
        project_id: Option<&str>,
    ) -> Option<(String, String)> {
        let overrides = project_id.and_then(|pid| {
            self.get_credential_store()
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
        let store = match self.get_credential_store() {
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

    /// Whether `id` names a managed Claude profile visible in this scope.
    /// Used by re-authentication so a caller can refresh one durable profile
    /// without creating a duplicate account or crossing a project boundary.
    pub fn is_managed_claude_account(&self, id: &str, project_id: Option<&str>) -> bool {
        self.get_credential_store().is_some_and(|store| {
            store
                .accounts_for_provider(ApiProvider::Anthropic, project_id)
                .into_iter()
                .any(|account| {
                    account.id == id && matches!(account.auth, ProviderAuth::ClaudeProfile { .. })
                })
        })
    }

    /// Add a new account to the store.
    pub fn add_account(
        &self,
        api_provider: ApiProvider,
        label: String,
        auth: ProviderAuth,
        project_id: Option<String>,
    ) -> Result<AccountInfo, String> {
        let mut store = self.get_credential_store().unwrap_or_else(|| {
            crate::identity::local::credential_store_from_git_config(&self.config_dir)
        });
        let info = store.add_account(
            api_provider,
            label,
            auth,
            project_id,
            chrono::Utc::now().timestamp(),
        );
        self.save_credential_store(store)?;
        self.reconcile_subscription_periods();

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
        target: &cairn_models::subscription::SignInTarget,
    ) -> Result<AccountInfo, cairn_models::subscription::SignInRecordError> {
        let mut store = self.get_credential_store().unwrap_or_else(|| {
            crate::identity::local::credential_store_from_git_config(&self.config_dir)
        });
        let info = cairn_models::subscription::store_codex_sign_in(
            &mut store,
            label,
            auth_json,
            project_id,
            target,
            chrono::Utc::now().timestamp(),
        )?;
        self.save_credential_store(store)?;
        self.reconcile_subscription_periods();
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
        // Hold the store lock through persistence. Detached probes for different
        // accounts may finish together; cloning before mutation lets the later
        // save silently erase the earlier account's health update.
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        let info = store.set_account_metadata(id, email, plan)?;
        self.credential_store.persist_locked(store)?;
        drop(guard);
        self.clear_provider_account_usage_snapshot("claude", id);
        self.reconcile_subscription_periods();
        self.emit_config_changed();
        // A completed sign-in is a credential change: it can be what makes
        // Claude's models usable at all.
        self.spawn_model_catalog_refresh();
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
        windows: Vec<cairn_models::types::usage::ProviderUsageWindow>,
        block: Option<crate::credentials::AccountBlock>,
    ) -> Result<String, String> {
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        account.health = Some(crate::credentials::ProviderAccountHealth {
            windows,
            blocked_until: block.map(crate::credentials::AccountBlock::until),
            blocked_until_declared: block
                .is_some_and(crate::credentials::AccountBlock::is_declared),
            held: account
                .health
                .as_ref()
                .and_then(|health| health.held.clone()),
            captured_at: chrono::Utc::now().timestamp(),
        });
        let label = account
            .email
            .clone()
            .unwrap_or_else(|| account.label.clone());
        self.credential_store.persist_locked(store)?;
        self.emit_config_changed();
        Ok(label)
    }

    /// Hold an account out of rotation for a refusal no clock heals: credit
    /// exhaustion, or terms its owner has not accepted. Unlike a rolling
    /// provider block, it stays unavailable until an operator acts.
    pub(crate) fn record_account_hold(
        &self,
        id: &str,
        kind: crate::credentials::AccountHoldKind,
        detail: String,
    ) -> Result<String, String> {
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        let now = chrono::Utc::now().timestamp();
        let health = account
            .health
            .get_or_insert_with(crate::credentials::ProviderAccountHealth::default);
        health.held = Some(crate::credentials::AccountHold {
            since: now,
            kind,
            detail,
        });
        health.captured_at = now;
        let label = account
            .email
            .clone()
            .unwrap_or_else(|| account.label.clone());
        self.credential_store.persist_locked(store)?;
        drop(guard);
        self.emit_config_changed();
        Ok(label)
    }

    /// Authorize one deliberate retry by lifting an account's hold, whatever
    /// its kind. Automatic usage probes intentionally do not call this: they
    /// can see neither a credit top-up nor a terms acceptance, and therefore
    /// cannot safely put the account in rotation.
    pub fn clear_account_hold(&self, id: &str) -> Result<AccountInfo, String> {
        self.lift_account_hold(id, |_| true).map(|(info, _)| info)
    }

    /// Lift a terms hold because an operator explicitly asked to re-measure
    /// the account, which is how they say they have accepted the terms. The
    /// usage endpoint is no evidence either way, so the account's next launch
    /// is the re-probe: refused again, it is held again. A credits hold stays.
    /// Answers whether a hold was lifted.
    pub(crate) fn admit_terms_reprobe(&self, id: &str) -> Result<bool, String> {
        self.lift_account_hold(id, |hold| {
            hold.kind == crate::credentials::AccountHoldKind::TermsAcceptance
        })
        .map(|(_, lifted)| lifted)
    }

    fn lift_account_hold(
        &self,
        id: &str,
        lifts: impl Fn(&crate::credentials::AccountHold) -> bool,
    ) -> Result<(AccountInfo, bool), String> {
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        let lifted = account
            .health
            .as_mut()
            .and_then(|health| health.held.take_if(|hold| lifts(hold)))
            .is_some();
        let info = AccountInfo::from(&*account);
        if lifted {
            self.credential_store.persist_locked(store)?;
        }
        drop(guard);
        if lifted {
            self.emit_config_changed();
        }
        Ok((info, lifted))
    }

    /// Probe a subscription account's remaining usage and record it.
    ///
    /// Detached, because the probe drives a provider CLI and the caller — a
    /// sign-in that just completed — must not wait on it. Nothing depends on it
    /// finishing: an account with no snapshot is routable at full headroom, and
    /// this replaces that assumption with a measurement.
    pub fn capture_account_usage(&self, provider: RoutedProvider, account_id: String) {
        let Some(capture_reservation) = ProviderUsageCaptureReservation::acquire(
            self.provider_usage_captures.clone(),
            provider,
            &account_id,
        ) else {
            log::debug!(
                "Skipping duplicate {} usage capture for {account_id}",
                provider.backend()
            );
            return;
        };
        let orch = self.clone();
        let spawned = std::thread::Builder::new()
            .name("provider-usage-capture".to_string())
            .spawn(move || {
                let _capture_reservation = capture_reservation;
                let (snapshot, credential_generation) =
                    orch.subscriptions
                        .probe_usage(&orch, provider, Some(&account_id));
                if let Err(error) = orch.apply_captured_account_usage_if_current(
                    provider,
                    &account_id,
                    credential_generation,
                    snapshot,
                ) {
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

    /// Re-probe after a successful managed Claude launch only when that launch
    /// can advance stale recovery state. Fresh measured snapshots do not need a
    /// network request and concurrent starts collapse into one capture.
    pub(crate) fn capture_claude_usage_after_launch(&self, account_id: String) {
        const REPROBE_INTERVAL_SECS: i64 = 5 * 60;

        let now = chrono::Utc::now().timestamp();
        let snapshot = self
            .provider_usage_snapshots
            .read()
            .ok()
            .and_then(|snapshots| {
                snapshots
                    .get(&("claude".to_string(), Some(account_id.clone())))
                    .cloned()
            });
        let should_probe = match snapshot {
            None => true,
            Some(snapshot)
                if snapshot.error_kind
                    == Some(
                        cairn_models::types::usage::ProviderUsageErrorKind::UsageProbeAuthentication,
                    ) =>
            {
                true
            }
            Some(snapshot) if snapshot.source == "credential_generation" => true,
            Some(snapshot) if !snapshot.windows.is_empty() => {
                now >= snapshot.captured_at.saturating_add(REPROBE_INTERVAL_SECS)
            }
            Some(_) => false,
        };
        if should_probe {
            self.capture_account_usage(RoutedProvider::Claude, account_id);
        }
    }

    fn apply_captured_account_usage(
        &self,
        account_id: &str,
        snapshot: cairn_models::types::usage::ProviderUsageSnapshot,
    ) -> Result<(), String> {
        // Commit the provider-card snapshot and routing health while account
        // identity is held stable. Removal takes this same lock and then clears
        // the snapshot, so a probe that started before deletion cannot publish
        // an orphaned generation after deletion.
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == account_id)
            .ok_or_else(|| format!("Account not found: {account_id}"))?;
        if !snapshot.windows.is_empty() {
            account.health = Some(crate::credentials::ProviderAccountHealth {
                windows: snapshot.windows.clone(),
                blocked_until: None,
                blocked_until_declared: false,
                held: account
                    .health
                    .as_ref()
                    .and_then(|health| health.held.clone()),
                captured_at: chrono::Utc::now().timestamp(),
            });
            self.credential_store.persist_locked(store)?;
        }
        self.store_provider_account_usage_snapshot(Some(account_id.to_string()), snapshot);
        drop(guard);
        self.emit_config_changed();
        Ok(())
    }

    /// Update an existing account's label.
    pub fn update_account(&self, id: &str, label: Option<String>) -> Result<AccountInfo, String> {
        let mut store = self.get_credential_store().ok_or("No identity store")?;
        let account = store
            .accounts
            .iter_mut()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        if let Some(label) = label {
            account.label = label;
        }
        let info = AccountInfo::from(&*account);
        self.save_credential_store(store)?;
        self.reconcile_subscription_periods();
        self.emit_config_changed();
        Ok(info)
    }

    /// Record which of its candidate plans an ambiguous subscription account
    /// is on, or clear the choice with `None`. The one writer of a plan
    /// choice: Settings and `cairn://models/accounts` both reach it.
    pub fn set_account_plan(&self, id: &str, plan: Option<String>) -> Result<AccountInfo, String> {
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        let info = store.set_plan_choice(id, plan)?;
        self.credential_store.persist_locked(store)?;
        drop(guard);
        self.reconcile_subscription_periods();
        self.emit_config_changed();
        Ok(info)
    }

    /// Remove an account from the store.
    pub fn remove_account(&self, id: &str) -> Result<(), String> {
        let account = self
            .get_credential_store()
            .ok_or("No identity store")?
            .accounts
            .into_iter()
            .find(|account| account.id == id)
            .ok_or_else(|| format!("Account not found: {id}"))?;
        // Released before the store lock is taken: releasing reads the store
        // itself, and a managed home is signed out before its row goes.
        if matches!(account.auth, ProviderAuth::ClaudeProfile { .. }) {
            self.subscriptions.release_claude_home(self, id)?;
        }
        let mut guard = self
            .credential_store
            .lock()
            .map_err(|_| "Identity store lock poisoned".to_string())?;
        let store = guard.as_mut().ok_or("No identity store")?;
        store.remove_account(id)?;
        self.credential_store.persist_locked(store)?;
        self.provider_usage_snapshots.remove("claude", Some(id));
        self.provider_usage_snapshots.remove("codex", Some(id));
        drop(guard);
        self.reconcile_subscription_periods();
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
        let mut store = self.get_credential_store().ok_or("No identity store")?;

        for (idx, id) in ordered_ids.iter().enumerate() {
            if let Some(account) = store
                .accounts
                .iter_mut()
                .find(|a| a.id == *id && a.api_provider == api_provider)
            {
                account.sort_order = idx as i32;
            }
        }

        self.save_credential_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Retired logins ===

    /// Anthropic logins the profiles-only migration dropped, still unread.
    pub fn list_retired_logins(&self) -> Vec<crate::credentials::RetiredLogin> {
        self.get_credential_store()
            .map(|store| store.retired_logins)
            .unwrap_or_default()
    }

    /// Acknowledge the retired logins, so settings stops reporting them.
    pub fn dismiss_retired_logins(&self) -> Result<(), String> {
        let mut store = self.get_credential_store().ok_or("No identity store")?;
        if store.retired_logins.is_empty() {
            return Ok(());
        }
        store.retired_logins.clear();
        self.save_credential_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Git Identity CRUD ===

    /// List all git identities.
    pub fn list_git_identities(&self) -> Vec<GitIdentity> {
        self.get_credential_store()
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
        let mut store = self.get_credential_store().unwrap_or_else(|| {
            crate::identity::local::credential_store_from_git_config(&self.config_dir)
        });

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
        self.save_credential_store(store)?;
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
        let mut store = self.get_credential_store().ok_or("No identity store")?;

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
        self.save_credential_store(store)?;
        self.emit_config_changed();
        Ok(result)
    }

    /// Remove a git identity.
    pub fn remove_git_identity(&self, id: &str) -> Result<(), String> {
        let mut store = self.get_credential_store().ok_or("No identity store")?;

        let initial_len = store.git_identities.len();
        store.git_identities.retain(|g| g.id != id);

        if store.git_identities.len() == initial_len {
            return Err(format!("Git identity not found: {}", id));
        }

        self.save_credential_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    /// Reorder git identities.
    pub fn reorder_git_identities(&self, ordered_ids: &[String]) -> Result<(), String> {
        let mut store = self.get_credential_store().ok_or("No identity store")?;

        for (idx, id) in ordered_ids.iter().enumerate() {
            if let Some(gi) = store.git_identities.iter_mut().find(|g| g.id == *id) {
                gi.sort_order = idx as i32;
            }
        }

        self.save_credential_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Project Overrides ===

    /// Get account overrides for a project.
    pub fn get_project_overrides(&self, project_id: &str) -> Option<AccountOverrides> {
        self.get_credential_store()
            .and_then(|store| store.project_overrides.get(project_id).cloned())
    }

    /// Set account overrides for a project.
    pub fn set_project_overrides(
        &self,
        project_id: &str,
        overrides: Option<AccountOverrides>,
    ) -> Result<(), String> {
        let mut store = self.get_credential_store().unwrap_or_else(|| {
            crate::identity::local::credential_store_from_git_config(&self.config_dir)
        });

        match overrides {
            Some(o) => {
                store.project_overrides.insert(project_id.to_string(), o);
            }
            None => {
                store.project_overrides.remove(project_id);
            }
        }

        self.save_credential_store(store)?;
        self.emit_config_changed();
        Ok(())
    }

    // === Helper ===

    pub(super) fn emit_config_changed(&self) {
        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "identity"}),
        );
    }
}

/// The host services the cairn-models facet's subscription mechanics run against.
impl cairn_models::subscription::ModelHost for Orchestrator {
    fn store(&self) -> &CredentialStoreHandle {
        &self.credential_store
    }

    fn process_spawner(&self) -> &dyn cairn_platform::services::process::ProcessSpawner {
        self.services.process.as_ref()
    }

    fn claude_path(&self) -> Result<String, String> {
        crate::orchestrator::session::get_claude_path(&self.process_state)
    }

    fn codex_path(&self) -> Result<String, String> {
        self.models_runtime()
            .host
            .resolve_cli(cairn_models::interface::session::Cli::Codex)
            .map_err(String::from)
    }

    fn credentials_published(&self) {
        self.reconcile_subscription_periods();
        self.emit_config_changed();
        self.claude_credentials_published();
    }

    fn register_secret(&self, provider: &str, key: &str, value: &str) {
        crate::security::broker::backend::register_injected(provider, key, value);
    }

    fn is_managed_claude_account(&self, id: &str, project_id: Option<&str>) -> bool {
        Orchestrator::is_managed_claude_account(self, id, project_id)
    }

    fn add_claude_profile_account(
        &self,
        project_id: Option<String>,
    ) -> Result<AccountInfo, String> {
        self.add_account(
            ApiProvider::Anthropic,
            "Claude".to_string(),
            ProviderAuth::ClaudeProfile { oauth: None },
            project_id,
        )
    }

    fn remove_account(&self, id: &str) {
        if let Err(error) = Orchestrator::remove_account(self, id) {
            log::debug!("Discarding sign-in account {id}: {error}");
        }
    }

    fn record_claude_sign_in(
        &self,
        id: &str,
        email: Option<String>,
        plan: Option<String>,
    ) -> Result<AccountInfo, String> {
        let connected = self.update_account_metadata(id, email, plan)?;
        self.capture_account_usage(RoutedProvider::Claude, id.to_string());
        Ok(connected)
    }

    fn record_codex_sign_in(
        &self,
        auth_json: String,
        project_id: Option<String>,
        target: &cairn_models::subscription::SignInTarget,
    ) -> Result<AccountInfo, cairn_models::subscription::SignInRecordError> {
        let account = self.upsert_codex_oauth_account(
            "Codex OAuth".to_string(),
            auth_json,
            project_id,
            target,
        )?;
        self.capture_account_usage(RoutedProvider::Codex, account.id.clone());
        Ok(account)
    }

    fn sign_in_settled(
        &self,
        sign_in_ref: &str,
        provider: RoutedProvider,
        outcome: cairn_models::subscription::SignInOutcome,
    ) {
        self.record_sign_in_settled(sign_in_ref, provider, outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::store;
    use crate::credentials::ProviderAccount;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_models::subscription::testing::TempHomes;
    use cairn_models::subscription::{ModelHost, Unsubscribed};
    use std::sync::Arc;

    async fn test_orchestrator() -> Orchestrator {
        test_builder().await.build()
    }

    async fn test_builder() -> OrchestratorBuilder {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir.clone())
            .subscriptions(Arc::new(TempHomes::new(config_dir.join("claude-profiles"))))
    }

    /// Reports whatever credential generation a test sets; every other
    /// subscription question gets `Unsubscribed`'s answer.
    #[derive(Default)]
    struct Generations(std::sync::Mutex<Option<String>>);

    impl Generations {
        fn set(&self, generation: &str) {
            *self.0.lock().unwrap() = Some(generation.to_string());
        }
    }

    impl cairn_models::subscription::Subscriptions for Generations {
        fn claude_home(&self, host: &dyn ModelHost, id: &str) -> Option<std::path::PathBuf> {
            Unsubscribed.claude_home(host, id)
        }
        fn prepare_claude_home(
            &self,
            host: &dyn ModelHost,
            id: &str,
        ) -> Result<std::path::PathBuf, String> {
            Unsubscribed.prepare_claude_home(host, id)
        }
        fn claude_home_signed_in(&self, host: &dyn ModelHost, id: &str) -> bool {
            Unsubscribed.claude_home_signed_in(host, id)
        }
        fn release_claude_home(&self, host: &dyn ModelHost, id: &str) -> Result<(), String> {
            Unsubscribed.release_claude_home(host, id)
        }
        fn claude_session_credential(
            &self,
            host: &dyn ModelHost,
            id: &str,
        ) -> Result<cairn_models::subscription::ClaudeSessionCredential, String> {
            Unsubscribed.claude_session_credential(host, id)
        }
        fn renew_claude_credentials(&self, host: &dyn ModelHost) -> Vec<String> {
            Unsubscribed.renew_claude_credentials(host)
        }
        fn claude_credential_rejected(
            &self,
            host: &dyn ModelHost,
            id: &str,
            generation: u64,
        ) -> Result<cairn_models::subscription::ClaudeSessionCredential, String> {
            Unsubscribed.claude_credential_rejected(host, id, generation)
        }
        fn migrate_claude_homes(&self, host: &dyn ModelHost) -> Vec<String> {
            Unsubscribed.migrate_claude_homes(host)
        }
        fn codex_session_login(
            &self,
            host: &dyn ModelHost,
            auth: &cairn_models::codex::tokens::CodexAuthState,
        ) -> Result<cairn_models::interface::session::Secret<serde_json::Value>, String> {
            Unsubscribed.codex_session_login(host, auth)
        }
        fn probe_usage(
            &self,
            host: &dyn ModelHost,
            provider: RoutedProvider,
            id: Option<&str>,
        ) -> (
            cairn_models::types::usage::ProviderUsageSnapshot,
            Option<String>,
        ) {
            Unsubscribed.probe_usage(host, provider, id)
        }
        fn credential_generation(
            &self,
            _host: &dyn ModelHost,
            _provider: RoutedProvider,
            _id: &str,
        ) -> Option<String> {
            self.0.lock().unwrap().clone()
        }
        fn consume_usage_reset(
            &self,
            host: &dyn ModelHost,
            provider: RoutedProvider,
            id: &str,
        ) -> Result<cairn_models::types::usage::ProviderUsageResetResult, String> {
            Unsubscribed.consume_usage_reset(host, provider, id)
        }
        fn refresh_codex(
            &self,
            host: &dyn ModelHost,
            request: cairn_models::codex::tokens::CodexRefreshRequest,
        ) -> Result<
            (String, cairn_models::codex::tokens::CodexAccountRef),
            cairn_models::codex::tokens::CodexRefreshFailure,
        > {
            Unsubscribed.refresh_codex(host, request)
        }

        fn start_sign_in(
            &self,
            host: std::sync::Arc<dyn ModelHost>,
            request: cairn_models::subscription::SignInRequest,
        ) -> cairn_models::subscription::SignInFuture<
            Result<
                cairn_models::subscription::SignInStart,
                cairn_models::subscription::SignInError,
            >,
        > {
            Unsubscribed.start_sign_in(host, request)
        }
        fn sign_in_status(
            &self,
            sign_in_ref: &str,
        ) -> Result<cairn_models::subscription::SignInStatus, cairn_models::subscription::SignInError>
        {
            Unsubscribed.sign_in_status(sign_in_ref)
        }
        fn submit_sign_in_code(
            &self,
            host: std::sync::Arc<dyn ModelHost>,
            sign_in_ref: String,
            code: String,
        ) -> cairn_models::subscription::SignInFuture<
            Result<
                cairn_models::subscription::SubmitCodeResult,
                cairn_models::subscription::SignInError,
            >,
        > {
            Unsubscribed.submit_sign_in_code(host, sign_in_ref, code)
        }
        fn cancel_sign_in(&self, host: &dyn ModelHost, sign_in_ref: &str) -> bool {
            Unsubscribed.cancel_sign_in(host, sign_in_ref)
        }
        fn pending_sign_ins(&self) -> Vec<cairn_models::subscription::PendingSignIn> {
            Unsubscribed.pending_sign_ins()
        }
    }

    async fn test_orchestrator_with(
        subscriptions: Arc<dyn cairn_models::subscription::Subscriptions>,
    ) -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir)
            .subscriptions(subscriptions)
            .build()
    }

    /// A build without subscription support still resolves a managed Claude
    /// account — it is a real row — but refuses to launch it, and says why.
    #[tokio::test(flavor = "multi_thread")]
    async fn without_subscription_support_a_managed_claude_session_is_refused() {
        let orch = test_orchestrator_with(Arc::new(Unsubscribed)).await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Claude".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let identity = orch
            .resolve_provider_account("claude", &account.id)
            .expect("the managed account resolves");
        assert!(matches!(
            identity.claude_auth,
            Some(crate::credentials::ClaudeAuth::ManagedProfile { ref account_id }) if account_id == &account.id
        ));
        let refusal = crate::backends::claude::claude_launch_credential(&orch, &identity)
            .expect_err("no subscription support, no managed launch");
        assert!(
            refusal.contains(cairn_models::subscription::NO_SUBSCRIPTION_SUPPORT),
            "{refusal}"
        );
    }

    fn usage_window(
        id: &str,
        target: Option<&str>,
        remaining_percent: f64,
    ) -> cairn_models::types::usage::ProviderUsageWindow {
        cairn_models::types::usage::ProviderUsageWindow {
            id: id.to_string(),
            label: id.to_string(),
            scope: cairn_models::types::usage::ProviderUsageScope::Weekly,
            scope_target: target.map(str::to_string),
            used_percent: 100.0 - remaining_percent,
            remaining_percent,
            resets_at: Some(i64::MAX),
            reset_at_text: None,
            window_duration_mins: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn adding_accounts_updates_memory_and_preserves_each_persisted_profile() {
        let orch = test_orchestrator().await;
        let first = orch
            .add_account(
                ApiProvider::Anthropic,
                "First".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let second = orch
            .add_account(
                ApiProvider::Anthropic,
                "Second".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();

        orch.record_account_health(&first.id, vec![usage_window("weekly", None, 50.0)], None)
            .expect("the first account remains installed in memory");
        let in_memory = orch
            .get_credential_store()
            .expect("identity store installed");
        assert!(in_memory
            .accounts
            .iter()
            .any(|account| account.id == first.id));
        assert!(in_memory
            .accounts
            .iter()
            .any(|account| account.id == second.id));

        let persisted = store::load_credential_store(&orch.config_dir)
            .unwrap()
            .expect("identity store persisted");
        assert!(persisted
            .accounts
            .iter()
            .any(|account| account.id == first.id));
        assert!(persisted
            .accounts
            .iter()
            .any(|account| account.id == second.id));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removing_exhausted_claude_profile_keeps_healthy_profile_routable_after_restart() {
        let orch = test_orchestrator().await;
        let exhausted = orch
            .add_account(
                ApiProvider::Anthropic,
                "Exhausted".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let healthy = orch
            .add_account(
                ApiProvider::Anthropic,
                "Healthy".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(&exhausted.id, vec![usage_window("weekly", None, 0.0)], None)
            .unwrap();
        orch.record_account_health(
            &healthy.id,
            vec![
                usage_window("weekly", None, 99.0),
                usage_window("fable", Some("Fable"), 0.0),
            ],
            None,
        )
        .unwrap();
        orch.set_project_overrides(
            "project",
            Some(AccountOverrides {
                anthropic_account_id: Some(exhausted.id.clone()),
                ..Default::default()
            }),
        )
        .unwrap();

        orch.remove_account(&exhausted.id).unwrap();

        let loaded = store::load_credential_store(&orch.config_dir)
            .unwrap()
            .expect("identity store persists");
        let db = orch.db.clone();
        let services = orch.services.clone();
        let config_dir = orch.config_dir.clone();
        drop(orch);
        let restarted = OrchestratorBuilder::new(db, services, config_dir)
            .credential_store(Some(loaded))
            .build();

        assert_eq!(
            restarted
                .get_project_overrides("project")
                .and_then(|overrides| overrides.anthropic_account_id),
            None
        );
        let (selected_id, identity) = restarted
            .select_routed_identity(
                RoutedProvider::Claude,
                Some("project"),
                None,
                None,
                Some("opus"),
            )
            .expect("global headroom keeps Opus routable");
        assert_eq!(selected_id, healthy.id);
        assert!(matches!(
            identity.claude_auth,
            Some(crate::credentials::ClaudeAuth::ManagedProfile { .. })
        ));
        assert!(
            restarted
                .select_routed_identity(
                    RoutedProvider::Claude,
                    Some("project"),
                    None,
                    None,
                    Some("fable")
                )
                .is_none(),
            "a Fable-only empty window constrains Fable"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn claude_probe_paused_before_commit_cannot_publish_after_account_removal() {
        let orch = test_orchestrator().await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Removed while probing".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let captured_generation =
            orch.subscriptions()
                .credential_generation(&orch, RoutedProvider::Claude, &account.id);
        let snapshot = cairn_models::types::usage::ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "claude_usage_oauth".to_string(),
            captured_at: 1,
            windows: vec![usage_window("weekly", None, 90.0)],
            ..Default::default()
        };

        orch.remove_account(&account.id).unwrap();
        assert!(orch
            .apply_captured_account_usage_if_current(
                RoutedProvider::Claude,
                &account.id,
                captured_generation,
                snapshot,
            )
            .is_err());
        assert!(!orch
            .provider_usage_snapshots
            .read()
            .unwrap()
            .contains_key(&("claude".to_string(), Some(account.id))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn old_claude_probe_cannot_commit_after_credential_generation_changes() {
        // The subscription support reports which credential generation a probe
        // would read; the orchestrator's job is to discard a result captured
        // under an older one.
        let generations = Arc::new(Generations::default());
        let orch = test_orchestrator_with(generations.clone()).await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Claude".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .expect("profile added");
        generations.set("generation-a");
        let generation_a =
            orch.subscriptions()
                .credential_generation(&orch, RoutedProvider::Claude, &account.id);
        generations.set("generation-b");
        let stale = cairn_models::types::usage::ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "claude_usage_oauth".to_string(),
            captured_at: 1,
            windows: vec![cairn_models::types::usage::ProviderUsageWindow {
                id: "session".to_string(),
                label: "Current session".to_string(),
                scope: cairn_models::types::usage::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0,
                remaining_percent: 0.0,
                resets_at: Some(i64::MAX),
                reset_at_text: None,
                window_duration_mins: None,
            }],
            ..Default::default()
        };

        orch.apply_captured_account_usage_if_current(
            RoutedProvider::Claude,
            &account.id,
            generation_a,
            stale,
        )
        .expect("stale completion is ignored");

        let stored = orch.get_credential_store().unwrap();
        assert!(stored
            .accounts
            .iter()
            .find(|candidate| candidate.id == account.id)
            .unwrap()
            .health
            .is_none());
        assert!(!orch
            .provider_usage_snapshots
            .read()
            .unwrap()
            .contains_key(&("claude".to_string(), Some(account.id))));
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
    async fn saving_the_credential_store_does_not_discover_models() {
        let orch = test_orchestrator().await;

        orch.save_credential_store(crate::identity::local::credential_store_from_git_config(
            &orch.config_dir,
        ))
        .expect("saving the identity store should succeed");

        assert!(
            orch.get_model_catalog().is_empty(),
            "saving the identity store ran provider discovery inline; it must be spawned off the async task"
        );
    }

    /// Discovery's blocking HTTP clients may not run on a runtime thread.
    async fn await_full_catalog_refresh(orch: &Orchestrator) {
        let orch = orch.clone();
        tokio::task::spawn_blocking(move || orch.refresh_model_catalog())
            .await
            .unwrap();
    }

    /// A test orchestrator does not rediscover models on its own. Rediscovery
    /// visits every enabled provider over the network, so in a unit test it is
    /// a request leaving the machine and a race with any catalog the test
    /// seeded. An awaited full refresh and the one `add_account` spawns go
    /// through the same switch, so awaiting the former shows the latter was a
    /// no-op too; a refresh that ran would record an entry per enabled
    /// provider even if every one of them failed.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_test_orchestrator_does_not_rediscover_models_when_an_account_is_added() {
        let orch = test_orchestrator().await;
        assert!(!orch.enabled_providers().is_empty());

        orch.add_account(
            ApiProvider::OpenAI,
            "Key".to_string(),
            ProviderAuth::ApiKey {
                value: "sk-test".to_string(),
            },
            None,
        )
        .unwrap();
        await_full_catalog_refresh(&orch).await;

        assert!(
            orch.get_model_catalog().is_empty(),
            "a test orchestrator rediscovered models: {:?}",
            orch.get_model_catalog()
        );
    }

    /// A test about rediscovery opts in, and stays offline by enabling only
    /// providers that answer from what is stored: Claude behind an API key,
    /// and a custom provider whose models are declared rather than listed.
    /// Claude stays enabled because it serves the default tiers. The accounts
    /// are stored without `add_account` so no refresh starts before the
    /// enabled set is narrowed to them.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_opted_in_test_orchestrator_rediscovers_its_enabled_providers() {
        use crate::credentials::{CustomModelPolicy, CustomProtocol, CustomSampling};
        let orch = test_builder().await.catalog_rediscovery(true).build();
        let mut store = crate::identity::local::credential_store_from_git_config(&orch.config_dir);
        store.add_account(
            ApiProvider::Anthropic,
            "Key".to_string(),
            ProviderAuth::ApiKey {
                value: "sk-ant-test".to_string(),
            },
            None,
            0,
        );
        let account = store.add_account(
            ApiProvider::Custom,
            "Local".to_string(),
            ProviderAuth::custom_endpoint(
                "http://127.0.0.1:9/v1",
                CustomProtocol::OpenAiChatCompletions,
                None,
                &[],
                CustomModelPolicy::Explicit {
                    ids: vec!["local-model".to_string()],
                },
                CustomSampling::default(),
                32_768,
            )
            .unwrap(),
            None,
            0,
        );
        orch.save_credential_store(store).unwrap();
        let key = cairn_models::types::catalog::custom_key(&account.id);
        orch.update_settings(crate::models::UpdateSettings {
            enabled_providers: Some(vec!["claude".to_string(), key.clone()]),
            ..Default::default()
        })
        .unwrap();

        await_full_catalog_refresh(&orch).await;

        let catalog = orch.get_model_catalog();
        assert_eq!(
            catalog
                .iter()
                .map(|entry| (entry.backend.as_str(), entry.error.as_deref()))
                .collect::<Vec<_>>(),
            [("claude", None), (key.as_str(), None)]
        );
        assert!(catalog[1]
            .models
            .iter()
            .any(|model| model.id == "local-model"));
    }

    /// Claude's models are offered on the strength of a configured credential,
    /// never the CLI's own user-level login. The stand-in CLI reports itself
    /// signed in whenever no profile is named, which is what an ambient
    /// `~/.claude` login looks like.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn claude_models_follow_configured_accounts_not_the_ambient_login() {
        use std::os::unix::fs::PermissionsExt;

        let orch = test_orchestrator().await;
        let claude = orch.config_dir.join("fake-claude");
        std::fs::write(
            &claude,
            r#"#!/bin/sh
if [ -z "$CLAUDE_CONFIG_DIR" ] || [ -f "$CLAUDE_CONFIG_DIR/.signed-in" ]; then
  echo '{"loggedIn":true}'
else
  echo '{"loggedIn":false}'
fi
"#,
        )
        .unwrap();
        std::fs::set_permissions(&claude, std::fs::Permissions::from_mode(0o755)).unwrap();
        *orch.process_state.cli_binary_path.lock().unwrap() =
            Some(claude.to_string_lossy().into_owned());
        let discover = || crate::backends::claude::discover_catalog_blocking(&orch);

        let (models, error) = discover();
        assert!(models.is_empty(), "an ambient login is not a credential");
        assert!(error.unwrap().contains("No Claude account is configured"));

        let profile = orch
            .add_account(
                ApiProvider::Anthropic,
                "Claude".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let (models, error) = discover();
        assert!(models.is_empty(), "a profile that is not signed in");
        assert!(error.unwrap().contains("signed in"));

        TempHomes::new(orch.config_dir.join("claude-profiles")).sign_in(&profile.id);
        let (models, error) = discover();
        assert_eq!(error, None);
        assert!(models.iter().any(|model| model.id == "sonnet"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_claude_api_key_offers_models_without_probing_the_cli() {
        let orch = test_orchestrator().await;
        *orch.process_state.cli_binary_path.lock().unwrap() =
            Some("/nonexistent/claude".to_string());
        orch.add_account(
            ApiProvider::Anthropic,
            "Key".to_string(),
            ProviderAuth::ApiKey {
                value: "sk-ant-test".to_string(),
            },
            None,
        )
        .unwrap();

        let (models, error) = crate::backends::claude::discover_catalog_blocking(&orch);
        assert_eq!(error, None);
        assert!(!models.is_empty());
    }

    /// A store persisted through the orchestrator resolves its managed profiles
    /// under the same root it was written to, rather than an inferred one.
    #[tokio::test(flavor = "multi_thread")]
    async fn saving_stamps_the_config_root_onto_the_in_memory_store() {
        let orch = test_orchestrator().await;

        orch.save_credential_store(crate::identity::local::credential_store_from_git_config(
            &orch.config_dir,
        ))
        .expect("saving the identity store should succeed");

        let stored = orch
            .get_credential_store()
            .expect("store is held in memory");
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
        orch.get_credential_store()
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
                &cairn_models::subscription::SignInTarget::NewOrMatching,
            )
            .expect("first account stored");
        let second = orch
            .upsert_codex_oauth_account(
                "Codex OAuth".to_string(),
                codex_auth_json("acct-two", "two@example.com", "refresh-two"),
                None,
                &cairn_models::subscription::SignInTarget::NewOrMatching,
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
            &cairn_models::subscription::SignInTarget::NewOrMatching,
        )
        .expect("unrelated account stored");
        let first = orch
            .upsert_codex_oauth_account(
                "Codex OAuth".to_string(),
                codex_auth_json("acct-one", "one@example.com", "refresh-one"),
                None,
                &cairn_models::subscription::SignInTarget::NewOrMatching,
            )
            .expect("account stored");

        let reconnected_json = codex_auth_json("acct-one", "renamed@example.com", "refresh-two");
        let reconnected = orch
            .upsert_codex_oauth_account(
                "Codex OAuth".to_string(),
                reconnected_json.clone(),
                None,
                &cairn_models::subscription::SignInTarget::NewOrMatching,
            )
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
            &cairn_models::subscription::SignInTarget::NewOrMatching,
        );

        assert!(stored.is_err());
        assert!(openai_oauth_accounts(&orch).is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn managed_claude_reauth_preserves_identity_and_invalidates_old_generation_health() {
        let orch = test_orchestrator().await;
        let first = orch
            .add_account(
                ApiProvider::Anthropic,
                "First".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .expect("first profile added");
        let refreshed = orch
            .add_account(
                ApiProvider::Anthropic,
                "Second".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .expect("profile added");
        let project_override = AccountOverrides {
            anthropic_account_id: Some(refreshed.id.clone()),
            ..Default::default()
        };
        orch.set_project_overrides("project-1", Some(project_override))
            .expect("project override stored");
        orch.record_account_health(
            &refreshed.id,
            vec![cairn_models::types::usage::ProviderUsageWindow {
                id: "session".to_string(),
                label: "Current session".to_string(),
                scope: cairn_models::types::usage::ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0,
                remaining_percent: 0.0,
                resets_at: Some(i64::MAX),
                reset_at_text: None,
                window_duration_mins: None,
            }],
            Some(crate::credentials::AccountBlock::Declared(i64::MAX)),
        )
        .expect("old health recorded");
        orch.store_provider_account_usage_snapshot(
            Some(refreshed.id.clone()),
            cairn_models::types::usage::ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_oauth",
                "old credential error",
                None,
            ),
        );

        let before = orch.get_credential_store().unwrap();
        let before_order = before
            .accounts
            .iter()
            .map(|account| (account.id.clone(), account.sort_order))
            .collect::<Vec<_>>();
        let connected = orch
            .update_account_metadata(
                &refreshed.id,
                Some("same@example.com".to_string()),
                Some("max".to_string()),
            )
            .expect("profile re-authenticated");

        assert_eq!(connected.id, refreshed.id);
        let after = orch.get_credential_store().unwrap();
        assert_eq!(after.accounts.len(), 2, "re-auth adds no duplicate");
        assert_eq!(
            after
                .accounts
                .iter()
                .map(|account| (account.id.clone(), account.sort_order))
                .collect::<Vec<_>>(),
            before_order,
            "account identity and ordering survive"
        );
        assert!(after
            .accounts
            .iter()
            .find(|account| account.id == refreshed.id)
            .unwrap()
            .health
            .is_none());
        assert_eq!(
            orch.provider_usage_snapshots
                .read()
                .unwrap()
                .get(&("claude".to_string(), Some(refreshed.id.clone())))
                .and_then(|snapshot| snapshot.unsupported_reason.as_deref()),
            Some("Usage is refreshing after this profile signed in.")
        );
        assert_eq!(
            orch.get_project_overrides("project-1")
                .and_then(|overrides| overrides.anthropic_account_id),
            Some(refreshed.id.clone()),
            "project override keeps the durable account id"
        );
        assert!(orch.is_managed_claude_account(&refreshed.id, Some("project-1")));
        assert!(orch.is_managed_claude_account(&first.id, None));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_sticky_hold_survives_usage_refresh_until_explicitly_cleared() {
        let orch = test_orchestrator().await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Claude".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .expect("profile added");

        orch.record_account_hold(
            &account.id,
            crate::credentials::AccountHoldKind::Credits,
            "credits_required".to_string(),
        )
        .expect("hold stored");
        orch.record_account_health(&account.id, vec![usage_window("weekly", None, 75.0)], None)
            .expect("usage refresh stored");
        assert!(
            !orch.admit_terms_reprobe(&account.id).unwrap(),
            "a terms re-probe never lifts a credits hold"
        );

        let store = orch.get_credential_store().unwrap();
        assert!(store
            .accounts
            .iter()
            .find(|candidate| candidate.id == account.id)
            .unwrap()
            .health
            .as_ref()
            .unwrap()
            .held
            .is_some());
        assert!(store
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                None,
                None,
                None,
                &[],
                i64::MAX,
            )
            .is_none());

        orch.clear_account_hold(&account.id)
            .expect("operator retry clears the hold");
        let cleared = orch.get_credential_store().unwrap();
        let health = cleared
            .accounts
            .iter()
            .find(|candidate| candidate.id == account.id)
            .unwrap()
            .health
            .as_ref()
            .unwrap();
        assert!(health.held.is_none());
        assert_eq!(health.windows[0].remaining_percent, 75.0);
        assert!(cleared
            .select_routed_account(
                RoutedProvider::Claude,
                None,
                None,
                None,
                None,
                &[],
                i64::MAX,
            )
            .is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn usage_probe_authentication_failure_cannot_overwrite_good_health() {
        let orch = test_orchestrator().await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Claude".to_string(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .expect("profile added");
        let windows = vec![cairn_models::types::usage::ProviderUsageWindow {
            id: "weekly".to_string(),
            label: "Current week".to_string(),
            scope: cairn_models::types::usage::ProviderUsageScope::Weekly,
            scope_target: None,
            used_percent: 20.0,
            remaining_percent: 80.0,
            resets_at: Some(i64::MAX),
            reset_at_text: None,
            window_duration_mins: None,
        }];
        orch.record_account_health(&account.id, windows.clone(), None)
            .expect("good health stored");
        orch.apply_captured_account_usage(
            &account.id,
            cairn_models::types::usage::ProviderUsageSnapshot::error(
                "claude",
                "claude_usage_oauth",
                "usage probe could not authenticate",
                None,
            ),
        )
        .expect("windowless probe errors are non-blocking");

        let health = orch
            .get_credential_store()
            .unwrap()
            .accounts
            .into_iter()
            .find(|candidate| candidate.id == account.id)
            .unwrap()
            .health
            .unwrap();
        assert_eq!(health.windows, windows);
        assert_eq!(
            orch.provider_usage_snapshots
                .read()
                .unwrap()
                .get(&("claude".to_string(), Some(account.id.clone())))
                .and_then(|snapshot| snapshot.error.as_deref()),
            Some("usage probe could not authenticate")
        );
    }
}
