//! Orchestrator settings and keybinds operations.

use crate::config::keybinds::{self, KeySequence, KeybindsFile};
use crate::config::settings;
use crate::models::{ChannelsConfig, Settings, UpdateSettings};

use super::Orchestrator;

// Keep token resolution behind this closure: on macOS, merely reading a stored
// token can open a keychain authorization dialog and block headless runners.
fn validate_telegram_channel_update(
    channels: Option<&ChannelsConfig>,
    token: impl FnOnce() -> Result<Option<crate::security::broker::BrokeredSecret>, String>,
) -> Result<(), String> {
    let Some(channels) = channels else {
        return Ok(());
    };
    let Some(token) = token()? else {
        return Ok(());
    };

    if let Some(error) = crate::channels::telegram_identity_error_for_brokered_token(
        &channels.telegram.chat_id,
        &channels.telegram.allow_from,
        &token,
    ) {
        return Err(error);
    }
    Ok(())
}

impl Orchestrator {
    /// Get current settings from file.
    pub fn get_settings(&self) -> Settings {
        settings::load_settings(&self.config_dir)
    }

    /// The providers this workspace has installed, in catalog order.
    ///
    /// Every prospective surface — model discovery, the responses catalog,
    /// picker presence — asks here rather than asking which providers Cairn
    /// supports. Reading *persisted* data (a job's model, a session's backend)
    /// asks `backends::catalog::is_supported` instead, so disabling a provider
    /// changes what can be chosen next without making history unreadable.
    pub fn enabled_providers(&self) -> Vec<String> {
        self.get_settings().enabled_providers
    }

    /// Whether this workspace has installed `backend`.
    pub fn provider_enabled(&self, backend: &str) -> bool {
        self.enabled_providers().iter().any(|key| key == backend)
    }

    /// Record which providers this workspace has installed, if it has not
    /// answered that question yet.
    ///
    /// Runs at startup, where the identity store is loaded and can say which
    /// backends the workspace's accounts serve — evidence `settings.yaml`
    /// cannot see on its own. Idempotent: once the answer is on disk, this
    /// never revisits it.
    pub fn migrate_enabled_providers(&self) -> Result<Option<Vec<String>>, String> {
        let credentialed: Vec<String> = self
            .get_credential_store()
            .map(|store| {
                store
                    .accounts
                    .iter()
                    .flat_map(|account| account.compatible_backends())
                    .collect()
            })
            .unwrap_or_default();
        let migrated = settings::migrate_enabled_providers(&self.config_dir, &credentialed)?;
        if let Some(enabled) = &migrated {
            log::info!("Recorded installed providers for this workspace: {enabled:?}");
        }
        let retired = settings::retire_built_in_providers(&self.config_dir)?;
        if !retired.is_empty() {
            log::info!("Built-in providers left enabledProviders: {retired:?}");
        }
        if migrated.is_some() || !retired.is_empty() {
            let _ = self.services.emitter.emit(
                "config-changed",
                serde_json::json!({"entity_type": "settings"}),
            );
        }
        Ok(migrated)
    }

    /// Move a workspace configured against the retired compiled web-provider
    /// catalogs onto pack providers: stored keys move to the pack credential
    /// namespace and a selected bundled provider pack is installed, so search
    /// and fetch keep working with no click. A no-op once done, and a no-op
    /// before the desktop has recorded where the app's packs live.
    pub fn migrate_web_providers(&self) {
        let Some(resource_dir) = cairn_packs::source_dir(&self.config_dir) else {
            return;
        };
        let report = crate::web_providers::migrate_workspace(
            &crate::services::RealGitClient,
            &crate::services::RealFileSystem,
            &self.config_dir,
            &resource_dir,
        );
        for account in &report.keys_moved {
            log::info!("web providers: moved the key at {account} to its pack credential");
        }
        for pack in &report.installed {
            log::info!("web providers: installed `{pack}`, which this workspace had selected");
        }
        for skipped in &report.skipped {
            log::info!("web providers: left alone: {skipped}");
        }
        if report.changed() {
            self.emit_pack_registry_change();
        }
    }

    /// Write down the conversion of the retired desktop-automation toggles into
    /// machine-service selections, if it has not been written yet, and log what
    /// did not carry over exactly. Idempotent.
    pub fn migrate_desktop_automation(&self) -> Result<(), String> {
        let Some(notes) = crate::workspace::bundle::migrate_desktop_automation(
            &crate::services::RealGitClient,
            &crate::services::RealFileSystem,
            &self.config_dir,
        )?
        else {
            return Ok(());
        };
        log::info!("Converted buildSlots.desktopAutomation into buildSlots.machineServices");
        for note in notes {
            log::info!("desktop automation conversion: {note}");
        }
        Ok(())
    }

    /// Convert this workspace's tier grid into pools and a ladder, if it has
    /// not been converted yet, and log what did not carry over exactly.
    ///
    /// Runs at startup after the installed-provider set is recorded, since
    /// only installed providers' cells become ladder entries. Effort words are
    /// placed on each model's scale by the catalog, which before discovery
    /// answers from each provider's static vocabulary. Idempotent: once
    /// `models` is on disk this never revisits it.
    pub fn migrate_models(&self) -> Result<Option<Vec<String>>, String> {
        use cairn_models::interface::Models;
        let catalog = self.catalog(&cairn_models::interface::ModelScope::default())?;
        let position = |selection: &crate::models::ModelSelection, level: &str| {
            super::model_selection::position_in(&catalog, selection, level)
        };
        let enabled = self.enabled_providers();
        let migrated = settings::migrate_models(&self.config_dir, &enabled, &position)?;
        if let Some(notes) = &migrated {
            log::info!("Converted this workspace's model tiers to pools and a ladder");
            for note in notes {
                log::info!("model tier conversion: {note}");
            }
        }
        // A project's tier overrides convert in memory whenever they are read
        // (`effective_models_in`), against the workspace grid, which retiring
        // archives rather than loses. A project's repository is never written
        // to: these are the notes its conversion would give.
        let workspace_grid = settings::workspace_legacy_grid(&self.config_dir)?;
        for (project_id, project_path) in self.local_project_paths() {
            let file =
                cairn_config::project_settings::load_project_settings_read_only(&project_path);
            if file.models.is_some() {
                continue;
            }
            if let Some((_, notes)) = cairn_config::project_settings::convert_project_tiers(
                &file,
                &workspace_grid,
                &enabled,
                &position,
            ) {
                log::info!(
                    "Project {project_id}'s model tiers convert to a models overlay in memory; its .cairn/config.yaml is left as written"
                );
                for note in notes {
                    log::info!("model tier conversion ({project_id}): {note}");
                }
            }
        }
        let retired = settings::retire_tier_keys(&self.config_dir)?;
        if retired {
            log::info!("Removed the retired tier grid from this workspace's settings");
        }
        if migrated.is_some() || retired {
            let _ = self.services.emitter.emit(
                "config-changed",
                serde_json::json!({"entity_type": "settings"}),
            );
        }
        Ok(migrated)
    }

    /// Every project with a checkout on this machine, by id.
    fn local_project_paths(&self) -> Vec<(String, std::path::PathBuf)> {
        let db = self.db.local.clone();
        let projects = cairn_db::storage::run_db_blocking(move || async move {
            crate::projects::crud::list_db(&db)
                .await
                .map_err(|error| error.to_string())
        })
        .unwrap_or_default();
        projects
            .into_iter()
            .filter_map(|project| {
                let path = self.project_path(&project.id).ok()?;
                path.exists().then_some((project.id, path))
            })
            .collect()
    }

    /// The OS-sandbox read denylist for executor cells: the configured
    /// `sandboxDenyRead` (or the narrow built-in default of external secret
    /// stores), plus the desktop operator credential.
    ///
    /// The credential is appended unconditionally rather than added to the
    /// default list, because the default list is *replaced* wholesale when
    /// `sandboxDenyRead` is configured. A denylist entry an operator can
    /// remove by editing settings would be one an agent could remove by
    /// getting one settings write approved, which is the reverse of the
    /// dependency this credential is supposed to have.
    pub(crate) fn sandbox_deny_read(&self) -> Vec<std::path::PathBuf> {
        let mut paths = settings::load_sandbox_deny_read(&self.config_dir);
        let credential =
            crate::authorization::protected::operator_credential_path(&self.config_dir);
        if !paths.contains(&credential) {
            paths.push(credential);
        }
        paths
    }

    /// Update settings with partial input.
    pub fn update_settings(&self, input: UpdateSettings) -> Result<Settings, String> {
        validate_telegram_channel_update(input.channels.as_ref(), || {
            crate::security::broker::web_provider_key(
                "channel/telegram",
                "BOT_TOKEN",
                "validate Telegram channel settings",
            )
        })?;
        let before = self.get_settings();
        let embedding_before = before.embedding_model;
        let current = settings::update_settings(&self.config_dir, input, &self.provider_names())?;
        if current.private_dedicated_process_capacity != before.private_dedicated_process_capacity {
            self.db.local.provider_admission_changed();
        }

        // A provider just enabled has no models yet: discover them now, not at
        // the next periodic refresh, so it can be chosen at once.
        let added: Vec<String> = current
            .enabled_providers
            .iter()
            .filter(|key| !before.enabled_providers.contains(key))
            .cloned()
            .collect();
        if !added.is_empty() {
            self.spawn_provider_model_catalog_refresh(added);
        }

        // A new embedding model is a new vector space: bring the stores into
        // it now rather than at the sweep's next loop, so search stops
        // answering from the old model at once. Every settings write funnels
        // through here, whichever surface made it.
        if current.embedding_model != embedding_before {
            let keeper = self.space_keeper();
            super::spawn_onto(
                self.runtime.as_ref(),
                async move {
                    if let Err(error) = keeper
                        .reconcile(crate::embeddings::Trigger::SettingsChange)
                        .await
                    {
                        log::warn!("embeddings: reconciling after a model change failed: {error}");
                    }
                },
                || log::warn!("embeddings: could not schedule the reconcile after a model change"),
            );
        }

        // Raising either execution-queue cap may free held starts at once.
        if current.max_concurrent_executions != before.max_concurrent_executions
            || current.max_open_children_per_thread != before.max_open_children_per_thread
        {
            crate::execution::gates::sweep::notify_gate_evidence_changed();
        }

        // Emit config-changed event
        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "settings"}),
        );

        Ok(current)
    }

    /// Get current keybinds from file.
    pub fn get_keybinds(&self) -> KeybindsFile {
        keybinds::load_keybinds(&self.config_dir)
    }

    /// Set a single keybind.
    pub fn set_keybind(&self, action: &str, sequence: KeySequence) -> Result<KeybindsFile, String> {
        let mut file = keybinds::load_keybinds(&self.config_dir);
        file.set_keybind(action, sequence)?;
        keybinds::save_keybinds(&self.config_dir, &file)?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "keybinds"}),
        );

        Ok(file)
    }

    /// Reset a single keybind to default.
    pub fn reset_keybind(&self, action: &str) -> Result<KeybindsFile, String> {
        let mut file = keybinds::load_keybinds(&self.config_dir);
        file.remove_keybind(action)?;
        keybinds::save_keybinds(&self.config_dir, &file)?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "keybinds"}),
        );

        Ok(file)
    }

    /// Reset all keybinds to defaults.
    pub fn reset_all_keybinds(&self) -> Result<KeybindsFile, String> {
        let mut file = keybinds::load_keybinds(&self.config_dir);
        file.reset();
        keybinds::save_keybinds(&self.config_dir, &file)?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "keybinds"}),
        );

        Ok(file)
    }

    /// Save a complete keybinds file.
    pub fn save_keybinds(&self, file: &KeybindsFile) -> Result<(), String> {
        keybinds::save_keybinds(&self.config_dir, file)?;

        let _ = self.services.emitter.emit(
            "config-changed",
            serde_json::json!({"entity_type": "keybinds"}),
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settings_update_without_channels_does_not_request_telegram_token() {
        validate_telegram_channel_update(None, || {
            panic!("Telegram token lookup must be lazy when channels are absent")
        })
        .unwrap();
    }

    #[test]
    fn settings_update_with_channels_preserves_token_lookup_failure() {
        let error = "Keychain unavailable: unlock the login keychain".to_string();
        let result = validate_telegram_channel_update(Some(&ChannelsConfig::default()), || {
            Err(error.clone())
        });
        assert_eq!(result, Err(error));
    }

    #[test]
    fn settings_update_with_channels_requests_telegram_token() {
        let mut requested = false;

        validate_telegram_channel_update(Some(&ChannelsConfig::default()), || {
            requested = true;
            Ok(None)
        })
        .unwrap();

        assert!(requested);
    }
}
