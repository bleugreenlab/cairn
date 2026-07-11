//! Orchestrator settings and keybinds operations.

use crate::config::keybinds::{self, KeybindsFile, Modifier};
use crate::config::settings;
use crate::models::{Settings, UpdateSettings};

use super::Orchestrator;

impl Orchestrator {
    /// Get current settings from file.
    pub fn get_settings(&self) -> Settings {
        settings::load_settings(&self.config_dir)
    }

    /// The OS-sandbox read denylist for worktree agents (configured
    /// `sandboxDenyRead` or the narrow built-in default: external secret stores).
    pub fn sandbox_deny_read(&self) -> Vec<std::path::PathBuf> {
        settings::load_sandbox_deny_read(&self.config_dir)
    }

    /// Update settings with partial input.
    pub fn update_settings(&self, input: UpdateSettings) -> Result<Settings, String> {
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
    pub fn set_keybind(
        &self,
        action: &str,
        key: String,
        modifiers: Vec<Modifier>,
    ) -> Result<KeybindsFile, String> {
        let mut file = keybinds::load_keybinds(&self.config_dir);
        file.set_keybind(action, key, modifiers);
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
        file.remove_keybind(action);
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
