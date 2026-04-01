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

        // Preset fields
        if let Some(ab) = input.active_backend {
            current.active_backend = ab;
        }
        if let Some(dt) = input.default_tier {
            current.default_tier = dt;
        }
        if let Some(t) = input.tiers {
            current.tiers = t;
        }
        if let Some(b) = input.backends {
            current.backends = b;
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
        // auto_start_jobs is always true — ignored
        if let Some(tz) = input.timezone {
            current.timezone = tz;
        }
        if let Some(days) = input.orphan_cleanup_days {
            current.orphan_cleanup_days = days.clamp(1, 30);
        }
        if let Some(default_terminal) = input.default_terminal {
            current.default_terminal = default_terminal;
        }
        if let Some(default_app) = input.default_app {
            current.default_app = default_app;
        }
        if let Some(device) = input.audio_device {
            current.audio_device = device;
        }
        if let Some(model) = input.whisper_model {
            current.whisper_model = model;
        }
        if let Some(key) = input.web_search_api_key {
            current.web_search_api_key = key;
        }
        if let Some(level) = input.lookup_detail_level {
            current.lookup_detail_level = level;
        }
        if let Some(level) = input.change_detail_level {
            current.change_detail_level = level;
        }
        if let Some(mode) = input.thinking_display_mode {
            current.thinking_display_mode = mode;
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
