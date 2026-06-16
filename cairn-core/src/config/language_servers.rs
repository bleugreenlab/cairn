//! Language Server registry: settings-configured LSP servers Cairn can spawn.
//!
//! A language server entry declares how to launch a Language Server Protocol
//! implementation (e.g. rust-analyzer, typescript-language-server), which file
//! extensions route to it, the files that mark its indexing root, and the
//! separator used for container-qualified symbol names. This is the **config
//! layer only**: entries are stored and template-expanded here; spawning the
//! server, walking root markers, and the read/UI surfaces land in later phases.
//!
//! The registry mirrors Managed Build Services (`super::build_services`) in
//! shape and lifecycle: a map keyed by language id, returned with built-in
//! defaults when unset, edited through surgical settings helpers, and expanded
//! through the shared `Templates`.
//!
//! `container_separator` is data on each entry, not a constant baked into
//! resolution code, so adding a language is purely a config change and never
//! touches the symbol resolver.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::build_services::Templates;

/// One configured language server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LanguageServerConfig {
    /// Whether Cairn launches this server. Disabled entries stay in settings
    /// but are skipped by the launching consumer.
    #[serde(default)]
    pub enabled: bool,
    /// Argv Cairn spawns to launch the server (later phases spawn it; here it
    /// is just stored and template-expanded).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// File extensions this server handles — the routing key (e.g. `["rs"]`,
    /// `["ts", "tsx"]`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<String>,
    /// Files that mark the indexing root (e.g. `Cargo.toml`, `package.json`).
    /// Stored as data here; the ancestor walk that consumes them lands later.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_markers: Vec<String>,
    /// Separator for container-qualified symbol names (`"::"` for Rust, `"."`
    /// for TypeScript/Python/Go). Data on the entry, not hardcoded in the
    /// resolver, so adding a language never touches resolution code.
    #[serde(default)]
    pub container_separator: String,
    /// LSP `initializationOptions` passed verbatim to the server on init.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initialization_options: Option<serde_json::Value>,
    /// Env injected into the server's spawn.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
}

impl LanguageServerConfig {
    /// The launch argv with templates expanded.
    pub fn expanded_command(&self, t: &Templates) -> Vec<String> {
        self.command.iter().map(|s| t.expand(s)).collect()
    }

    /// The spawn env with templates expanded.
    pub fn expanded_env(&self, t: &Templates) -> HashMap<String, String> {
        self.env
            .iter()
            .map(|(k, v)| (k.clone(), t.expand(v)))
            .collect()
    }
}

/// The built-in default language servers, used when none are configured.
///
/// Returned **unconditionally** as config, but every entry ships **disabled**:
/// language tooling is opt-in. A language's server is enabled when the user
/// selects it during project creation (or from settings), so Cairn never drives
/// a server for a language the user does not work in. On-PATH gating ("inert
/// when the binary is absent") is the launching consumer's job in a later phase.
pub fn default_language_servers() -> HashMap<String, LanguageServerConfig> {
    let mut map = HashMap::new();

    map.insert(
        "rust".to_string(),
        LanguageServerConfig {
            enabled: false,
            command: vec!["rust-analyzer".to_string()],
            extensions: vec!["rs".to_string()],
            root_markers: vec!["Cargo.toml".to_string()],
            container_separator: "::".to_string(),
            initialization_options: None,
            env: HashMap::new(),
        },
    );

    map.insert(
        "typescript".to_string(),
        LanguageServerConfig {
            enabled: false,
            command: vec![
                "typescript-language-server".to_string(),
                "--stdio".to_string(),
            ],
            extensions: vec![
                "ts".to_string(),
                "tsx".to_string(),
                "js".to_string(),
                "jsx".to_string(),
            ],
            root_markers: vec!["tsconfig.json".to_string(), "package.json".to_string()],
            container_separator: ".".to_string(),
            initialization_options: None,
            env: HashMap::new(),
        },
    );

    map.insert(
        "python".to_string(),
        LanguageServerConfig {
            enabled: false,
            command: vec!["pyright-langserver".to_string(), "--stdio".to_string()],
            extensions: vec!["py".to_string()],
            root_markers: vec!["pyproject.toml".to_string(), "setup.py".to_string()],
            container_separator: ".".to_string(),
            initialization_options: None,
            env: HashMap::new(),
        },
    );

    map.insert(
        "go".to_string(),
        LanguageServerConfig {
            enabled: false,
            command: vec!["gopls".to_string()],
            extensions: vec!["go".to_string()],
            root_markers: vec!["go.mod".to_string()],
            container_separator: ".".to_string(),
            initialization_options: None,
            env: HashMap::new(),
        },
    );

    map
}

/// Route a file extension to its language server: the first enabled entry whose
/// `extensions` contains `ext`. A pure helper over the registry map; its real
/// consumer arrives in a later phase.
pub fn server_for_extension<'a>(
    map: &'a HashMap<String, LanguageServerConfig>,
    ext: &str,
) -> Option<(&'a String, &'a LanguageServerConfig)> {
    map.iter()
        .find(|(_, cfg)| cfg.enabled && cfg.extensions.iter().any(|e| e == ext))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn templates() -> Templates {
        Templates {
            home: PathBuf::from("/home/u"),
            cairn_home: PathBuf::from("/home/u/.cairn"),
            worktrees: PathBuf::from("/home/u/.cairn/worktrees"),
            worktree: Some(PathBuf::from("/home/u/.cairn/worktrees/CAIRN-1")),
        }
    }

    #[test]
    fn language_server_config_yaml_roundtrip() {
        let yaml = r#"
enabled: true
command: ["rust-analyzer"]
extensions: ["rs"]
rootMarkers: ["Cargo.toml"]
containerSeparator: "::"
initializationOptions:
  cargo:
    allFeatures: true
env:
  RA_LOG: "info"
"#;
        let cfg: LanguageServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.command, vec!["rust-analyzer"]);
        assert_eq!(cfg.extensions, vec!["rs"]);
        assert_eq!(cfg.root_markers, vec!["Cargo.toml"]);
        assert_eq!(cfg.container_separator, "::");
        assert_eq!(
            cfg.initialization_options
                .as_ref()
                .and_then(|v| v.pointer("/cargo/allFeatures"))
                .and_then(|v| v.as_bool()),
            Some(true)
        );
        assert_eq!(cfg.env.get("RA_LOG").map(String::as_str), Some("info"));

        // Re-serialize and re-parse to confirm a stable round trip.
        let serialized = serde_yaml::to_string(&cfg).unwrap();
        let reparsed: LanguageServerConfig = serde_yaml::from_str(&serialized).unwrap();
        assert_eq!(cfg, reparsed);
    }

    #[test]
    fn template_expansion_reused_for_command_and_env() {
        let t = templates();
        let mut env = HashMap::new();
        env.insert("CACHE".to_string(), "{cairnHome}/ra-cache".to_string());
        let cfg = LanguageServerConfig {
            enabled: true,
            command: vec!["{home}/bin/rust-analyzer".to_string()],
            extensions: vec!["rs".to_string()],
            root_markers: vec!["Cargo.toml".to_string()],
            container_separator: "::".to_string(),
            initialization_options: None,
            env,
        };
        assert_eq!(cfg.expanded_command(&t), vec!["/home/u/bin/rust-analyzer"]);
        assert_eq!(
            cfg.expanded_env(&t).get("CACHE").map(String::as_str),
            Some("/home/u/.cairn/ra-cache")
        );
    }

    #[test]
    fn server_for_extension_routes_by_extension() {
        let mut map = default_language_servers();
        // Defaults ship disabled (opt-in); routing only considers enabled entries.
        for cfg in map.values_mut() {
            cfg.enabled = true;
        }
        assert_eq!(
            server_for_extension(&map, "rs").map(|(id, _)| id.as_str()),
            Some("rust")
        );
        assert_eq!(
            server_for_extension(&map, "tsx").map(|(id, _)| id.as_str()),
            Some("typescript")
        );
        assert_eq!(
            server_for_extension(&map, "py").map(|(id, _)| id.as_str()),
            Some("python")
        );
        assert_eq!(
            server_for_extension(&map, "go").map(|(id, _)| id.as_str()),
            Some("go")
        );
        assert!(server_for_extension(&map, "zig").is_none());
    }

    #[test]
    fn server_for_extension_skips_disabled_entries() {
        let mut map = default_language_servers();
        for cfg in map.values_mut() {
            cfg.enabled = true;
        }
        map.get_mut("rust").unwrap().enabled = false;
        assert!(server_for_extension(&map, "rs").is_none());
        // A still-enabled sibling keeps routing.
        assert_eq!(
            server_for_extension(&map, "tsx").map(|(id, _)| id.as_str()),
            Some("typescript")
        );
    }

    #[test]
    fn default_language_servers_ship_disabled() {
        let map = default_language_servers();
        for id in ["rust", "typescript", "python", "go"] {
            assert!(!map[id].enabled, "{id} must ship disabled (opt-in)");
        }
    }

    #[test]
    fn default_language_servers_is_well_formed() {
        let map = default_language_servers();
        assert_eq!(map.len(), 4);
        for id in ["rust", "typescript", "python", "go"] {
            let cfg = map.get(id).unwrap_or_else(|| panic!("missing {id}"));
            assert!(!cfg.command.is_empty(), "{id} command must be non-empty");
            assert!(
                !cfg.extensions.is_empty(),
                "{id} extensions must be non-empty"
            );
            assert!(
                !cfg.container_separator.is_empty(),
                "{id} separator must be non-empty"
            );
        }
        assert_eq!(map["rust"].container_separator, "::");
        assert_eq!(map["typescript"].container_separator, ".");
        assert_eq!(map["python"].container_separator, ".");
        assert_eq!(map["go"].container_separator, ".");
    }
}
