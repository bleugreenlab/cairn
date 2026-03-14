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

    /// Update settings with partial input.
    pub fn update_settings(&self, input: UpdateSettings) -> Result<Settings, String> {
        let mut current = settings::load_settings(&self.config_dir);

        if let Some(model) = input.default_model {
            current.default_model = model;
        }
        if let Some(prefix) = input.branch_prefix {
            current.branch_prefix = prefix;
        }
        if let Some(tokens) = input.max_thinking_tokens {
            current.max_thinking_tokens = tokens;
        }
        if let Some(merge_type) = input.merge_type {
            current.merge_type = merge_type;
        }
        if let Some(pull_on_merge) = input.pull_on_merge {
            current.pull_on_merge = pull_on_merge;
        }
        if let Some(auto_start) = input.auto_start_jobs {
            current.auto_start_jobs = auto_start;
        }
        if let Some(tz) = input.timezone {
            current.timezone = tz;
        }
        if let Some(days) = input.orphan_cleanup_days {
            current.orphan_cleanup_days = days.clamp(1, 30);
        }
        if let Some(device) = input.audio_device {
            current.audio_device = device;
        }
        if let Some(model) = input.whisper_model {
            current.whisper_model = model;
        }

        settings::save_settings(&self.config_dir, &current)?;

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
