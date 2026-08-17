//! Orchestrator settings and keybinds operations.

use crate::config::keybinds::{self, KeySequence, KeybindsFile};
use crate::config::settings;
use crate::models::{Settings, UpdateSettings};

use super::Orchestrator;

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
            .get_identity_store()
            .map(|store| {
                store
                    .accounts
                    .iter()
                    .flat_map(|account| account.compatible_backends())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        let migrated = settings::migrate_enabled_providers(&self.config_dir, &credentialed)?;
        if let Some(enabled) = &migrated {
            log::info!("Recorded installed providers for this workspace: {enabled:?}");
            let _ = self.services.emitter.emit(
                "config-changed",
                serde_json::json!({"entity_type": "settings"}),
            );
        }
        Ok(migrated)
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
        if let (Some(channels), Some(token)) = (
            input.channels.as_ref(),
            crate::security::broker::web_provider_key(
                "channel/telegram",
                "BOT_TOKEN",
                "validate Telegram channel settings",
            ),
        ) {
            if let Some(error) = crate::channels::telegram_identity_error_for_brokered_token(
                &channels.telegram.chat_id,
                &channels.telegram.allow_from,
                &token,
            ) {
                return Err(error);
            }
        }
        let current = settings::update_settings(&self.config_dir, input)?;

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
