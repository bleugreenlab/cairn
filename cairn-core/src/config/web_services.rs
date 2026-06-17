//! Web fetch provider registry.
//!
//! `read http(s)://…` and local `.pdf` reads route through a configurable
//! web-fetch *provider* rather than a hardcoded `bmd` shell-out. Providers are
//! declared at the workspace level (`~/.cairn/settings.yaml`) under
//! `webServices`, and a single scalar `activeWebFetch` names the provider that
//! backs fetch. Absent (or `regular`) selects the built-in plain-HTTP fetch,
//! which depends on nothing.
//!
//! Two built-in providers are always offered to the UI without hand-written
//! YAML: `regular` (plain reqwest GET, content-type-aware HTML→markdown) and
//! `bmd` (the hosted `bmd {url} --md --stdout` command, a one-click switch for
//! existing users). The selection model mirrors `activeBackend → backends`: a
//! single scalar names the one provider that backs fetch, so the invariant
//! "exactly one provider per request" lives in one place rather than a per-entry
//! default flag.
//!
//! ## Templating and secrets
//!
//! `{url}` is replaced with the fetch target across `args` (command), `url`, and
//! `headers` (http). HTTP `headers` and `url` additionally support `${VAR}`
//! interpolation, resolved from the OS keychain first (see [`super::secrets`])
//! then the process environment at fetch time — the same model as the external
//! MCP registry. Secrets live in the keychain, never in `settings.yaml`.
//!
//! ## Forward-compatibility for search
//!
//! The same `webServices` map serves search providers later: a future
//! `activeWebSearch` selector plus a `{query}` template placeholder reuse this
//! schema unchanged.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

/// The built-in plain-HTTP fetch provider name (also the default when
/// `activeWebFetch` is absent or empty).
pub const REGULAR: &str = "regular";
/// The built-in `bmd` command provider name.
pub const BMD: &str = "bmd";

/// Configuration for a single web-fetch provider.
///
/// A struct (rather than a tagged enum) so the command fields and the http
/// fields share one schema; `transport` selects which apply. Mirrors
/// [`super::mcp_servers::McpServerConfig`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WebServiceConfig {
    /// Transport: `command` (spawn a CLI, read markdown from stdout) or `http`
    /// (issue a reqwest request). Selects which field group below applies.
    #[serde(default = "default_transport")]
    pub transport: String,
    /// Command transport: program to spawn.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Command transport: arguments. `{url}` is replaced with the fetch target.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// HTTP transport: request URL. `{url}` is replaced with the fetch target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// HTTP transport: request method (default `GET`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// HTTP transport: headers sent on every request. `{url}` and `${VAR}`
    /// secrets are expanded at fetch time.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Whether this provider is selectable. A disabled provider named by
    /// `activeWebFetch` falls back to the built-in regular fetch. Defaults true,
    /// and is omitted from serialized output while true so hand-written configs
    /// stay enabled.
    #[serde(default = "default_enabled", skip_serializing_if = "is_enabled")]
    pub enabled: bool,
}

fn default_transport() -> String {
    "command".to_string()
}

fn default_enabled() -> bool {
    true
}

fn is_enabled(enabled: &bool) -> bool {
    *enabled
}

impl WebServiceConfig {
    /// Command arguments with `{url}` replaced by the fetch target.
    pub fn expanded_args(&self, url: &str) -> Vec<String> {
        self.args.iter().map(|a| a.replace("{url}", url)).collect()
    }

    /// The request URL with `{url}` replaced by the fetch target, if configured.
    pub fn expanded_url(&self, url: &str) -> Option<String> {
        self.url.as_deref().map(|u| u.replace("{url}", url))
    }

    /// The HTTP method, defaulting to `GET`.
    pub fn method(&self) -> String {
        self.method
            .clone()
            .filter(|m| !m.trim().is_empty())
            .unwrap_or_else(|| "GET".to_string())
    }

    /// Headers with `{url}` substituted and `${VAR}` secrets resolved from the
    /// keychain (then process env, then empty) under `credential_key`.
    pub fn expanded_headers(&self, url: &str, credential_key: &str) -> HashMap<String, String> {
        self.headers
            .iter()
            .map(|(k, v)| {
                let with_url = v.replace("{url}", url);
                let resolved = super::mcp_servers::expand_with(&with_url, &|var| {
                    super::secrets::get_secret(credential_key, var)
                        .or_else(|| std::env::var(var).ok())
                        .unwrap_or_default()
                });
                (k.clone(), resolved)
            })
            .collect()
    }

    /// The set of `${VAR}` names referenced across templated string fields.
    /// Drives the settings UI (which secrets to prompt for) and secret cleanup
    /// on delete.
    pub fn referenced_vars(&self) -> BTreeSet<String> {
        let mut vars = BTreeSet::new();
        let mut fields: Vec<&str> = Vec::new();
        if let Some(command) = &self.command {
            fields.push(command);
        }
        for arg in &self.args {
            fields.push(arg);
        }
        if let Some(url) = &self.url {
            fields.push(url);
        }
        for value in self.headers.values() {
            fields.push(value);
        }
        for field in fields {
            collect_vars(field, &mut vars);
        }
        vars
    }
}

/// Append every `${VAR}` name found in `input` to `out`. A literal `${` with no
/// closing brace is ignored.
fn collect_vars(input: &str, out: &mut BTreeSet<String>) {
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(end) = input[i + 2..].find('}') {
                out.insert(input[i + 2..i + 2 + end].to_string());
                i = i + 2 + end + 1;
                continue;
            }
        }
        i += 1;
    }
}

/// The built-in plain-HTTP fetch provider: a content-type-aware reqwest GET,
/// depending on nothing. Routing keys off the provider name, so its `url`/`args`
/// are intentionally empty — it is a marker, not a templated request.
pub fn builtin_regular() -> WebServiceConfig {
    WebServiceConfig {
        transport: "http".to_string(),
        command: None,
        args: Vec::new(),
        url: None,
        method: None,
        headers: HashMap::new(),
        enabled: true,
    }
}

/// The built-in `bmd` command provider — the former hardcoded shell-out, now a
/// one-click selectable provider for existing users.
pub fn builtin_bmd() -> WebServiceConfig {
    WebServiceConfig {
        transport: "command".to_string(),
        command: Some("bmd".to_string()),
        args: vec![
            "{url}".to_string(),
            "--md".to_string(),
            "--stdout".to_string(),
        ],
        url: None,
        method: None,
        headers: HashMap::new(),
        enabled: true,
    }
}

/// The synthesized built-in providers, always offered so the UI can present
/// them without hand-written YAML.
pub fn builtin_web_services() -> HashMap<String, WebServiceConfig> {
    HashMap::from([
        (REGULAR.to_string(), builtin_regular()),
        (BMD.to_string(), builtin_bmd()),
    ])
}

/// Whether `name` is one of the synthesized built-in providers.
pub fn is_builtin(name: &str) -> bool {
    name == REGULAR || name == BMD
}

/// The effective provider map for the management UI: the built-ins overlaid by
/// any configured `webServices` entries (a configured entry of the same name
/// wins, e.g. a user customizing `bmd`).
pub fn load_web_services(config_dir: &Path) -> HashMap<String, WebServiceConfig> {
    let mut map = builtin_web_services();
    if let Some(configured) = super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.web_services)
    {
        for (name, cfg) in configured {
            map.insert(name, cfg);
        }
    }
    map
}

/// The user-configured `webServices` entries only (no built-ins). Returns an
/// empty map when the block is absent.
pub fn load_configured_web_services(config_dir: &Path) -> HashMap<String, WebServiceConfig> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.web_services)
        .unwrap_or_default()
}

/// The configured `activeWebFetch` provider name, if any.
pub fn active_web_fetch_name(config_dir: &Path) -> Option<String> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.active_web_fetch)
}

/// The resolved provider that backs a fetch request.
#[derive(Debug, Clone, PartialEq)]
pub enum ActiveFetch {
    /// The built-in plain-HTTP fetch (the default).
    Regular,
    /// A configured (or built-in `bmd`) provider.
    Provider {
        name: String,
        config: WebServiceConfig,
    },
}

/// Resolve which provider backs fetch. Absent / empty / `regular` ⇒ the built-in
/// regular fetch. A named provider that is unknown or disabled falls back to
/// regular fetch with a warning rather than failing the read.
pub fn resolve_active_fetch(config_dir: &Path) -> ActiveFetch {
    let name = match active_web_fetch_name(config_dir) {
        Some(n) if !n.trim().is_empty() && n != REGULAR => n,
        _ => return ActiveFetch::Regular,
    };
    match load_web_services(config_dir).get(&name) {
        Some(cfg) if cfg.enabled => ActiveFetch::Provider {
            name,
            config: cfg.clone(),
        },
        Some(_) => {
            log::warn!("activeWebFetch '{name}' is disabled; using built-in regular fetch");
            ActiveFetch::Regular
        }
        None => {
            log::warn!("activeWebFetch '{name}' is not a known web service; using built-in regular fetch");
            ActiveFetch::Regular
        }
    }
}

const WORKSPACE_HEADER: &str = "# Cairn Workspace Settings";

/// Insert or replace one workspace web-fetch provider, keyed by `name`.
///
/// Edits `settings.yaml` surgically through `serde_yaml::Value` (mirroring
/// [`super::mcp_servers`]) so every other setting is preserved verbatim — a
/// typed round-trip would re-emit defaulted fields and clobber unrelated keys.
pub fn upsert_web_service(
    config_dir: &Path,
    name: &str,
    config: &WebServiceConfig,
) -> Result<(), String> {
    let path = super::settings::get_settings_path(config_dir);
    let mut root = load_settings_mapping(&path)?;

    let services = root
        .entry(serde_yaml::Value::String("webServices".to_string()))
        .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    let services = services
        .as_mapping_mut()
        .ok_or_else(|| "`webServices` in config is not a mapping".to_string())?;

    let config_value = serde_yaml::to_value(config)
        .map_err(|e| format!("Failed to serialize web service: {e}"))?;
    services.insert(serde_yaml::Value::String(name.to_string()), config_value);

    write_settings_mapping(&path, &root)
}

/// Remove one workspace web-fetch provider by `name`. Succeeds even if the
/// provider (or the file) does not exist. Drops the `webServices` block entirely
/// when it becomes empty so the file stays clean. If the removed provider was
/// the active one, `activeWebFetch` is cleared back to the regular default.
pub fn delete_web_service(config_dir: &Path, name: &str) -> Result<(), String> {
    let path = super::settings::get_settings_path(config_dir);
    if !path.exists() {
        return Ok(());
    }
    let mut root = load_settings_mapping(&path)?;

    let key = serde_yaml::Value::String("webServices".to_string());
    if let Some(services) = root.get_mut(&key).and_then(|v| v.as_mapping_mut()) {
        services.remove(serde_yaml::Value::String(name.to_string()));
        if services.is_empty() {
            root.remove(&key);
        }
    }

    // Clearing the active provider it pointed at keeps fetch from falling into
    // the warn-and-fallback path on every request.
    let active_key = serde_yaml::Value::String("activeWebFetch".to_string());
    if root.get(&active_key).and_then(|v| v.as_str()) == Some(name) {
        root.remove(&active_key);
    }

    write_settings_mapping(&path, &root)
}

/// Set (or clear) the active fetch provider. `None`, an empty string, or
/// `regular` clears the scalar so the built-in regular fetch is the default.
pub fn set_active_web_fetch(config_dir: &Path, name: Option<&str>) -> Result<(), String> {
    let path = super::settings::get_settings_path(config_dir);
    let mut root = load_settings_mapping(&path)?;
    let key = serde_yaml::Value::String("activeWebFetch".to_string());
    match name {
        Some(n) if !n.trim().is_empty() && n != REGULAR => {
            root.insert(key, serde_yaml::Value::String(n.to_string()));
        }
        _ => {
            root.remove(&key);
        }
    }
    write_settings_mapping(&path, &root)
}

/// Parse a settings file into a YAML mapping, or an empty mapping if the file is
/// absent or holds only `null`.
fn load_settings_mapping(path: &Path) -> Result<serde_yaml::Mapping, String> {
    if !path.exists() {
        return Ok(serde_yaml::Mapping::new());
    }
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("Failed to read settings file: {e}"))?;
    match serde_yaml::from_str::<serde_yaml::Value>(&content)
        .map_err(|e| format!("Failed to parse settings file: {e}"))?
    {
        serde_yaml::Value::Mapping(m) => Ok(m),
        serde_yaml::Value::Null => Ok(serde_yaml::Mapping::new()),
        _ => Err("settings file root is not a mapping".to_string()),
    }
}

/// Serialize a YAML mapping back to `settings.yaml`, re-adding the leading
/// header comment (serde_yaml does not preserve comments).
fn write_settings_mapping(path: &Path, root: &serde_yaml::Mapping) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {e}"))?;
    }
    let yaml =
        serde_yaml::to_string(root).map_err(|e| format!("Failed to serialize settings: {e}"))?;
    let content = format!("{WORKSPACE_HEADER}\n{yaml}");
    std::fs::write(path, content).map_err(|e| format!("Failed to write settings file: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn jina_config() -> WebServiceConfig {
        WebServiceConfig {
            transport: "http".to_string(),
            command: None,
            args: Vec::new(),
            url: Some("https://r.jina.ai/{url}".to_string()),
            method: None,
            headers: HashMap::from([(
                "Authorization".to_string(),
                "Bearer ${JINA_API_KEY}".to_string(),
            )]),
            enabled: true,
        }
    }

    #[test]
    fn missing_transport_defaults_to_command() {
        let cfg: WebServiceConfig = serde_yaml::from_str("command: bmd\n").unwrap();
        assert_eq!(cfg.transport, "command");
        assert!(cfg.enabled);
    }

    #[test]
    fn enabled_true_is_omitted_when_serialized() {
        let yaml = serde_yaml::to_string(&builtin_bmd()).unwrap();
        assert!(!yaml.contains("enabled"));
        let mut disabled = builtin_bmd();
        disabled.enabled = false;
        let yaml = serde_yaml::to_string(&disabled).unwrap();
        assert!(yaml.contains("enabled: false"));
    }

    #[test]
    fn url_template_expands_across_fields() {
        let cfg = jina_config();
        assert_eq!(
            cfg.expanded_url("https://example.com").as_deref(),
            Some("https://r.jina.ai/https://example.com")
        );
        let bmd = builtin_bmd();
        assert_eq!(
            bmd.expanded_args("https://example.com"),
            vec!["https://example.com", "--md", "--stdout"]
        );
    }

    #[test]
    fn headers_expand_url_then_secret() {
        super::super::secrets::mock_keychain::install();
        super::super::secrets::set_secret("jina", "JINA_API_KEY", "tok123").unwrap();
        let cfg = jina_config();
        let headers = cfg.expanded_headers("https://example.com", "jina");
        assert_eq!(
            headers.get("Authorization").map(String::as_str),
            Some("Bearer tok123")
        );
    }

    #[test]
    fn referenced_vars_collects_secret_names() {
        let vars = jina_config().referenced_vars();
        assert!(vars.contains("JINA_API_KEY"));
        assert_eq!(vars.len(), 1);
        // A command provider with no `${VAR}` references contributes nothing.
        assert!(builtin_bmd().referenced_vars().is_empty());
    }

    #[test]
    fn method_defaults_to_get() {
        assert_eq!(builtin_regular().method(), "GET");
        let mut cfg = jina_config();
        cfg.method = Some("POST".to_string());
        assert_eq!(cfg.method(), "POST");
    }

    #[test]
    fn load_web_services_includes_builtins() {
        let ws = TempDir::new().unwrap();
        let map = load_web_services(ws.path());
        assert!(map.contains_key(REGULAR));
        assert!(map.contains_key(BMD));
    }

    #[test]
    fn resolve_defaults_to_regular_when_unset() {
        let ws = TempDir::new().unwrap();
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);
    }

    #[test]
    fn resolve_picks_named_provider() {
        let ws = TempDir::new().unwrap();
        upsert_web_service(ws.path(), "jina", &jina_config()).unwrap();
        set_active_web_fetch(ws.path(), Some("jina")).unwrap();
        match resolve_active_fetch(ws.path()) {
            ActiveFetch::Provider { name, config } => {
                assert_eq!(name, "jina");
                assert_eq!(config.url.as_deref(), Some("https://r.jina.ai/{url}"));
            }
            other => panic!("expected provider, got {other:?}"),
        }
    }

    #[test]
    fn resolve_falls_back_when_active_unknown_or_disabled() {
        let ws = TempDir::new().unwrap();
        // Unknown name -> regular.
        set_active_web_fetch(ws.path(), Some("ghost")).unwrap();
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);

        // Disabled provider -> regular.
        let mut disabled = jina_config();
        disabled.enabled = false;
        upsert_web_service(ws.path(), "jina", &disabled).unwrap();
        set_active_web_fetch(ws.path(), Some("jina")).unwrap();
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);
    }

    #[test]
    fn active_bmd_resolves_to_builtin() {
        let ws = TempDir::new().unwrap();
        // bmd is a built-in, so it resolves without any webServices block.
        set_active_web_fetch(ws.path(), Some("bmd")).unwrap();
        match resolve_active_fetch(ws.path()) {
            ActiveFetch::Provider { name, config } => {
                assert_eq!(name, "bmd");
                assert_eq!(config.command.as_deref(), Some("bmd"));
            }
            other => panic!("expected bmd provider, got {other:?}"),
        }
    }

    #[test]
    fn upsert_load_delete_roundtrip_preserves_other_keys() {
        let ws = TempDir::new().unwrap();
        std::fs::write(
            super::super::settings::get_settings_path(ws.path()),
            "branchPrefix: custom\n",
        )
        .unwrap();

        upsert_web_service(ws.path(), "jina", &jina_config()).unwrap();
        let configured = load_configured_web_services(ws.path());
        assert_eq!(configured.len(), 1);
        assert!(configured.contains_key("jina"));
        // Unrelated key survives the surgical write.
        assert_eq!(
            super::super::settings::load_settings(ws.path()).branch_prefix,
            "custom"
        );

        delete_web_service(ws.path(), "jina").unwrap();
        assert!(load_configured_web_services(ws.path()).is_empty());
        let raw =
            std::fs::read_to_string(super::super::settings::get_settings_path(ws.path())).unwrap();
        assert!(!raw.contains("webServices"));
        assert!(raw.contains("branchPrefix: custom"));
    }

    #[test]
    fn delete_active_provider_clears_active_scalar() {
        let ws = TempDir::new().unwrap();
        upsert_web_service(ws.path(), "jina", &jina_config()).unwrap();
        set_active_web_fetch(ws.path(), Some("jina")).unwrap();
        delete_web_service(ws.path(), "jina").unwrap();
        assert_eq!(active_web_fetch_name(ws.path()), None);
        assert_eq!(resolve_active_fetch(ws.path()), ActiveFetch::Regular);
    }

    #[test]
    fn set_active_to_regular_clears_scalar() {
        let ws = TempDir::new().unwrap();
        set_active_web_fetch(ws.path(), Some("bmd")).unwrap();
        assert_eq!(active_web_fetch_name(ws.path()).as_deref(), Some("bmd"));
        set_active_web_fetch(ws.path(), Some(REGULAR)).unwrap();
        assert_eq!(active_web_fetch_name(ws.path()), None);
    }
}
