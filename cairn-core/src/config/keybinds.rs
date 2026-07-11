//! File-based keybinds customization.
//!
//! Keybinds are stored in `~/.cairn/keybinds.json` and override default shortcuts.
//! Empty key values disable the shortcut entirely.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Modifier keys for keybinds
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Modifier {
    Meta,
    Ctrl,
    Shift,
    Alt,
}

/// A keybind customization entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeybindCustomization {
    /// Action ID to customize (e.g., "issue.create")
    pub action: String,
    /// New key (empty string to disable)
    pub key: String,
    /// New modifiers
    #[serde(default)]
    pub modifiers: Vec<Modifier>,
    /// Optional description of why this was changed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Full keybinds file format
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KeybindsFile {
    /// Schema version for future migrations
    #[serde(default = "default_version")]
    pub version: i32,
    /// User customizations
    #[serde(default)]
    pub keybinds: Vec<KeybindCustomization>,
}

fn default_version() -> i32 {
    1
}

impl KeybindsFile {
    /// Add or update a customization
    pub fn set_keybind(&mut self, action: &str, key: String, modifiers: Vec<Modifier>) {
        // Remove existing customization if present
        self.keybinds.retain(|k| k.action != action);

        // Add new customization
        self.keybinds.push(KeybindCustomization {
            action: action.to_string(),
            key,
            modifiers,
            description: None,
        });
    }

    /// Remove a customization (revert to default)
    pub fn remove_keybind(&mut self, action: &str) {
        self.keybinds.retain(|k| k.action != action);
    }

    /// Clear all customizations
    pub fn reset(&mut self) {
        self.keybinds.clear();
    }
}

/// Get the path to the keybinds file
pub fn get_keybinds_path(config_dir: &Path) -> PathBuf {
    config_dir.join("keybinds.json")
}

/// Load keybinds from file. Returns empty file if doesn't exist or is invalid.
pub fn load_keybinds(config_dir: &Path) -> KeybindsFile {
    match load_keybinds_file(config_dir) {
        Ok(file) => file,
        Err(e) => {
            log::info!("Using default keybinds: {}", e);
            KeybindsFile::default()
        }
    }
}

/// Load the raw keybinds file
fn load_keybinds_file(config_dir: &Path) -> Result<KeybindsFile, String> {
    let path = get_keybinds_path(config_dir);

    if !path.exists() {
        return Ok(KeybindsFile::default());
    }

    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read keybinds file: {}", e))?;

    serde_json::from_str(&content).map_err(|e| format!("Failed to parse keybinds file: {}", e))
}

/// Save keybinds to file
pub fn save_keybinds(config_dir: &Path, file: &KeybindsFile) -> Result<(), String> {
    let path = get_keybinds_path(config_dir);

    // Ensure directory exists
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {}", e))?;
    }

    let content = serde_json::to_string_pretty(file)
        .map_err(|e| format!("Failed to serialize keybinds: {}", e))?;

    std::fs::write(&path, content).map_err(|e| format!("Failed to write keybinds file: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keybinds_file() -> KeybindsFile {
        KeybindsFile {
            version: 1,
            ..Default::default()
        }
    }

    #[test]
    fn test_set_keybind() {
        let mut file = keybinds_file();
        file.set_keybind("issue.create", "n".to_string(), vec![Modifier::Meta]);

        assert_eq!(file.keybinds.len(), 1);
        assert_eq!(file.keybinds[0].action, "issue.create");
        assert_eq!(file.keybinds[0].key, "n");
        assert_eq!(file.keybinds[0].modifiers, vec![Modifier::Meta]);
    }

    #[test]
    fn test_set_keybind_updates_existing() {
        let mut file = keybinds_file();
        file.set_keybind("issue.create", "n".to_string(), vec![Modifier::Meta]);
        file.set_keybind("issue.create", "c".to_string(), vec![]);

        assert_eq!(file.keybinds.len(), 1);
        assert_eq!(file.keybinds[0].key, "c");
        assert!(file.keybinds[0].modifiers.is_empty());
    }

    #[test]
    fn test_remove_keybind() {
        let mut file = keybinds_file();
        file.set_keybind("issue.create", "n".to_string(), vec![Modifier::Meta]);
        file.remove_keybind("issue.create");

        assert!(file.keybinds.is_empty());
    }

    #[test]
    fn test_reset() {
        let mut file = keybinds_file();
        file.set_keybind("issue.create", "n".to_string(), vec![Modifier::Meta]);
        file.set_keybind("issue.open", "o".to_string(), vec![]);
        file.reset();

        assert!(file.keybinds.is_empty());
    }

    #[test]
    fn test_serialization() {
        let mut file = keybinds_file();
        file.set_keybind(
            "issue.create",
            "n".to_string(),
            vec![Modifier::Meta, Modifier::Shift],
        );

        let json = serde_json::to_string(&file).unwrap();
        let parsed: KeybindsFile = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.keybinds.len(), 1);
        assert_eq!(parsed.keybinds[0].action, "issue.create");
        assert_eq!(parsed.keybinds[0].key, "n");
        assert_eq!(
            parsed.keybinds[0].modifiers,
            vec![Modifier::Meta, Modifier::Shift]
        );
    }

    #[test]
    fn test_disabled_keybind() {
        let mut file = keybinds_file();
        file.set_keybind("issue.create", "".to_string(), vec![]);

        assert!(file.keybinds[0].key.is_empty()); // Empty key = disabled
    }
}
