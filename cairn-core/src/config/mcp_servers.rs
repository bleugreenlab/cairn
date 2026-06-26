//! External MCP server registry.
//!
//! Cairn acts as an MCP *client/gateway*: configured external servers are
//! reachable through the `cairn://mcp/...` URI family without being injected
//! into a spawned agent's own MCP config. Servers are declared at the workspace
//! level (`~/.cairn/settings.yaml`) and/or per-project
//! (`[project]/.cairn/config.yaml`); project entries overlay the workspace set,
//! with the project entry winning on a key collision.
//!
//! ## Secrets
//!
//! Plaintext secrets are not stored. Values in `command`, `args`, `env`, `url`,
//! and `headers` support `${VAR}` interpolation, expanded at connect time. Each
//! reference resolves from the OS keychain first (see [`super::secrets`]) and
//! then from the app process environment. The keychain path is what lets a
//! Finder/Dock-launched app — which inherits a minimal environment — reach
//! token-bearing servers: the user enters the secret in the settings UI, it is
//! stored in the keychain, and `settings.yaml` keeps only the `${VAR}`
//! reference.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

/// Configuration for a single external MCP server.
///
/// A struct (rather than a tagged enum) so the stdio fields and the remote
/// (`http`/`sse`) fields share one schema; the active transport selects which
/// fields apply. `type` defaults to `stdio`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct McpServerConfig {
    /// Transport type: `stdio` (spawned child process), `http` (streamable
    /// HTTP), or `sse` (legacy HTTP+SSE). `http`/`sse` use `url` + `headers`.
    #[serde(rename = "type", default = "default_transport")]
    pub transport: String,
    /// Stdio: command to spawn the server process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// Stdio: arguments passed to the command.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// Stdio: environment variables for the spawned process.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    /// Remote (`http`/`sse`): server URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Remote (`http`/`sse`): headers sent on every request (e.g. auth).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub headers: HashMap<String, String>,
    /// Whether this server is exposed to agents. A disabled server stays in
    /// settings but is filtered out of agent-facing resolution
    /// (`read cairn://mcp` / `run cairn://mcp/...`); the management UI still
    /// loads and can toggle it. Defaults to true, and is omitted from
    /// serialized output while true so existing and hand-written configs stay
    /// enabled.
    #[serde(default = "default_enabled", skip_serializing_if = "is_enabled")]
    pub enabled: bool,
    /// Remote (`http`/`sse`): OAuth 2.1 authorization for this server. Presence
    /// signals "this server authenticates via the browser OAuth flow" — the
    /// gateway resolves a bearer token from the keychain (see
    /// [`super::super::mcp::oauth`]) and attaches it at connect time. Only
    /// non-secret fields live here; tokens and any client secret live in the
    /// keychain, never in `settings.yaml`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth: Option<OAuthServerConfig>,
}

/// Non-secret OAuth configuration persisted in `settings.yaml`. The interactive
/// authorize flow discovers endpoints and obtains tokens; what is durable and
/// non-secret (a pre-registered `client_id`, the granted/requested `scopes`)
/// rides here so a re-authorize starts from the right place. Tokens, the client
/// secret, and a Dynamic-Client-Registration result live in the OS keychain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct OAuthServerConfig {
    /// Pre-registered OAuth client id, if the user supplied one. Absent when the
    /// client is obtained via Dynamic Client Registration at authorize time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// Scopes to request. Empty means "let discovery decide" (challenge scope →
    /// server `scopes_supported` → omit).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
}

fn default_transport() -> String {
    "stdio".to_string()
}

fn default_enabled() -> bool {
    true
}

fn is_enabled(enabled: &bool) -> bool {
    *enabled
}

impl McpServerConfig {
    /// Return a copy with every `${VAR}` reference replaced by `resolve(var)`.
    /// This is the resolver seam: connect-time expansion (`expanded`) and the
    /// settings connection-test both build on it with different sources.
    pub fn expand_vars(&self, resolve: &dyn Fn(&str) -> String) -> McpServerConfig {
        let map = |s: &str| expand_with(s, resolve);
        McpServerConfig {
            transport: self.transport.clone(),
            command: self.command.as_deref().map(&map),
            args: self.args.iter().map(|a| map(a)).collect(),
            env: self.env.iter().map(|(k, v)| (k.clone(), map(v))).collect(),
            url: self.url.as_deref().map(&map),
            headers: self
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), map(v)))
                .collect(),
            enabled: self.enabled,
            // OAuth config is non-secret and carries no `${VAR}` references; the
            // bearer token is resolved separately from the keychain at connect.
            oauth: self.oauth.clone(),
        }
    }

    /// Connect-time expansion for `server_name` (the scoped credential key):
    /// each `${VAR}` resolves from the OS keychain first, then the process environment, then an empty string. A
    /// missing secret surfaces as a connect/auth failure from the server, not a
    /// panic here.
    pub fn expanded(&self, server_name: &str) -> McpServerConfig {
        self.expand_vars(&|var| {
            super::secrets::get_secret(server_name, var)
                .or_else(|| std::env::var(var).ok())
                .unwrap_or_default()
        })
    }

    /// The set of `${VAR}` names referenced across all string fields. Drives the
    /// settings UI (which secrets to prompt for) and secret cleanup on delete.
    pub fn referenced_vars(&self) -> BTreeSet<String> {
        let mut vars = BTreeSet::new();
        let mut scan = |s: &str| collect_vars(s, &mut vars);
        if let Some(command) = &self.command {
            scan(command);
        }
        for arg in &self.args {
            scan(arg);
        }
        for value in self.env.values() {
            scan(value);
        }
        if let Some(url) = &self.url {
            scan(url);
        }
        for value in self.headers.values() {
            scan(value);
        }
        vars
    }
}

/// Append every `${VAR}` name found in `input` to `out`.
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

/// Expand `${VAR}` occurrences in `input`, resolving each name via `resolve`.
/// A literal `${` with no closing brace is left untouched.
pub fn expand_with(input: &str, resolve: &dyn Fn(&str) -> String) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            if let Some(end) = input[i + 2..].find('}') {
                let name = &input[i + 2..i + 2 + end];
                out.push_str(&resolve(name));
                i = i + 2 + end + 1;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Expand `${VAR}` occurrences in `input` from the process environment only.
/// Unknown variables expand to an empty string.
pub fn expand_env_vars(input: &str) -> String {
    expand_with(input, &|name| std::env::var(name).unwrap_or_default())
}

/// Resolve the effective MCP server registry for a run: workspace servers from
/// `~/.cairn/settings.yaml` overlaid by the project's `.cairn/config.yaml`
/// (project wins on key collision). `project_path` is `None` for project-less
/// (workspace-only) contexts.
pub fn resolve_mcp_servers(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> HashMap<String, McpServerConfig> {
    let mut servers: HashMap<String, McpServerConfig> =
        super::settings::load_settings_file(config_dir)
            .ok()
            .and_then(|f| f.mcp_servers)
            .unwrap_or_default();

    if let Some(project_path) = project_path {
        if let Some(project_servers) =
            super::project_settings::load_project_settings(project_path).mcp_servers
        {
            for (name, cfg) in project_servers {
                servers.insert(name, cfg);
            }
        }
    }

    // Disabled servers stay in settings (so the UI can show + toggle them via
    // `load_workspace_mcp_servers`) but are invisible and uninvokable to agents.
    servers.retain(|_, cfg| cfg.enabled);

    servers
}

/// Load the workspace-level external MCP server registry from
/// `~/.cairn/settings.yaml`. Returns an empty map if the file is missing,
/// unparsable, or has no `mcpServers` block.
///
/// This is the registry the management UI edits; project-level overlays live in
/// each project's `.cairn/config.yaml` and are out of scope for workspace edits.
pub fn load_workspace_mcp_servers(config_dir: &Path) -> HashMap<String, McpServerConfig> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.mcp_servers)
        .unwrap_or_default()
}

/// Load the project-level external MCP server registry from
/// `[project]/.cairn/config.yaml`. Returns an empty map if the file is missing
/// or has no `mcpServers` block.
///
/// This is the unfiltered registry the management UI edits, so disabled entries
/// are included and can be toggled back on.
pub fn load_project_mcp_servers(project_path: &Path) -> HashMap<String, McpServerConfig> {
    super::project_settings::load_project_settings(project_path)
        .mcp_servers
        .unwrap_or_default()
}

/// Insert or replace one workspace MCP server, keyed by `name`.
///
/// Edits `settings.yaml` surgically through `serde_yaml::Value` rather than
/// round-tripping the typed `SettingsFile`: a typed round-trip would re-emit
/// defaulted fields (e.g. an absent `maxThinkingTokens` would serialize as
/// `null`, flipping thinking from default-enabled to disabled). Touching only
/// the `mcpServers` mapping leaves every other setting exactly as written.
pub fn upsert_workspace_mcp_server(
    config_dir: &Path,
    name: &str,
    config: &McpServerConfig,
) -> Result<(), String> {
    let path = super::settings::get_settings_path(config_dir);
    upsert_mcp_server_in_file(&path, WORKSPACE_HEADER, name, config)?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update mcp servers",
        Some(config_dir),
    );
    Ok(())
}

/// Remove one workspace MCP server by `name`. Succeeds even if the server (or
/// the file) does not exist. Drops the `mcpServers` block entirely when it
/// becomes empty so the file stays clean.
pub fn delete_workspace_mcp_server(config_dir: &Path, name: &str) -> Result<(), String> {
    let path = super::settings::get_settings_path(config_dir);
    delete_mcp_server_from_file(&path, WORKSPACE_HEADER, name)?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update mcp servers",
        Some(config_dir),
    );
    Ok(())
}

/// Insert or replace one project MCP server in `[project]/.cairn/config.yaml`.
///
/// Same surgical guarantee as the workspace writer: only the `mcpServers`
/// mapping is touched, so every other project setting is preserved verbatim.
/// Project entries overlay the workspace set (project wins on key collision).
pub fn upsert_project_mcp_server(
    project_path: &Path,
    name: &str,
    config: &McpServerConfig,
) -> Result<(), String> {
    let path = super::project_settings::get_project_config_path(project_path);
    upsert_mcp_server_in_file(&path, PROJECT_HEADER, name, config)?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update mcp servers",
        None,
    );
    Ok(())
}

/// Remove one project MCP server by `name` from `[project]/.cairn/config.yaml`.
/// Succeeds even if the server (or the file) does not exist.
pub fn delete_project_mcp_server(project_path: &Path, name: &str) -> Result<(), String> {
    let path = super::project_settings::get_project_config_path(project_path);
    delete_mcp_server_from_file(&path, PROJECT_HEADER, name)?;
    super::commit_and_maybe_push(
        std::slice::from_ref(&path),
        "cairn: update mcp servers",
        None,
    );
    Ok(())
}

const WORKSPACE_HEADER: &str = "# Cairn Workspace Settings";
const PROJECT_HEADER: &str = "# Cairn Project Configuration";

/// Insert or replace one MCP server in the `mcpServers` mapping of a YAML
/// settings file, leaving every other key untouched. Shared by the workspace
/// and project writers; `header` is the file's leading comment line.
fn upsert_mcp_server_in_file(
    path: &Path,
    header: &str,
    name: &str,
    config: &McpServerConfig,
) -> Result<(), String> {
    let mut root = load_settings_mapping(path)?;

    let servers = root
        .entry(serde_yaml::Value::String("mcpServers".to_string()))
        .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    let servers = servers
        .as_mapping_mut()
        .ok_or_else(|| "`mcpServers` in config is not a mapping".to_string())?;

    let config_value =
        serde_yaml::to_value(config).map_err(|e| format!("Failed to serialize server: {e}"))?;
    servers.insert(serde_yaml::Value::String(name.to_string()), config_value);

    write_settings_mapping(path, header, &root)
}

/// Remove one MCP server from the `mcpServers` mapping of a YAML settings file.
/// Drops the now-empty `mcpServers` block so the file stays clean.
fn delete_mcp_server_from_file(path: &Path, header: &str, name: &str) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let mut root = load_settings_mapping(path)?;

    let key = serde_yaml::Value::String("mcpServers".to_string());
    if let Some(servers) = root.get_mut(&key).and_then(|v| v.as_mapping_mut()) {
        servers.remove(serde_yaml::Value::String(name.to_string()));
        if servers.is_empty() {
            root.remove(&key);
        }
    }

    write_settings_mapping(path, header, &root)
}

/// Parse a settings file into a YAML mapping, or an empty mapping if the file
/// is absent or holds only `null`.
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

/// Serialize a YAML mapping back to a settings file, re-adding the leading
/// `header` comment (serde_yaml does not preserve comments, matching
/// `save_settings` / `save_project_settings`).
fn write_settings_mapping(
    path: &Path,
    header: &str,
    root: &serde_yaml::Mapping,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create config directory: {e}"))?;
    }
    let yaml =
        serde_yaml::to_string(root).map_err(|e| format!("Failed to serialize settings: {e}"))?;
    let content = format!("{header}\n{yaml}");
    std::fs::write(path, content).map_err(|e| format!("Failed to write settings file: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn resolve_overlays_project_over_workspace() {
        let ws = TempDir::new().unwrap();
        let proj = TempDir::new().unwrap();
        std::fs::write(
            ws.path().join("settings.yaml"),
            "mcpServers:\n  shared:\n    command: workspace-cmd\n  wsonly:\n    command: ws-only-cmd\n",
        )
        .unwrap();
        std::fs::create_dir_all(proj.path().join(".cairn")).unwrap();
        std::fs::write(
            proj.path().join(".cairn").join("config.yaml"),
            "mcpServers:\n  shared:\n    command: project-cmd\n  projonly:\n    command: proj-only-cmd\n",
        )
        .unwrap();

        let servers = resolve_mcp_servers(ws.path(), Some(proj.path()));
        assert_eq!(servers.len(), 3);
        // Project entry wins on a key collision.
        assert_eq!(servers["shared"].command.as_deref(), Some("project-cmd"));
        assert_eq!(servers["wsonly"].command.as_deref(), Some("ws-only-cmd"));
        assert_eq!(
            servers["projonly"].command.as_deref(),
            Some("proj-only-cmd")
        );

        // Workspace-only when there is no project path.
        let ws_only = resolve_mcp_servers(ws.path(), None);
        assert_eq!(ws_only.len(), 2);
        assert!(ws_only.contains_key("shared"));
        assert!(ws_only.contains_key("wsonly"));
    }

    #[test]
    fn resolve_filters_disabled_servers() {
        let ws = TempDir::new().unwrap();
        std::fs::write(
            ws.path().join("settings.yaml"),
            "mcpServers:\n  on:\n    command: on-cmd\n  off:\n    command: off-cmd\n    enabled: false\n",
        )
        .unwrap();

        // Agent-facing resolution drops the disabled server.
        let resolved = resolve_mcp_servers(ws.path(), None);
        assert_eq!(resolved.len(), 1);
        assert!(resolved.contains_key("on"));
        assert!(!resolved.contains_key("off"));

        // The management load path still surfaces both, so the UI can toggle.
        let loaded = load_workspace_mcp_servers(ws.path());
        assert_eq!(loaded.len(), 2);
        assert!(loaded["on"].enabled);
        assert!(!loaded["off"].enabled);
    }

    #[test]
    fn missing_enabled_field_defaults_to_true() {
        let cfg: McpServerConfig = serde_yaml::from_str("command: npx\n").unwrap();
        assert!(cfg.enabled);
    }

    #[test]
    fn enabled_true_is_omitted_when_serialized() {
        let yaml = serde_yaml::to_string(&axon_config()).unwrap();
        assert!(!yaml.contains("enabled"));
        let mut disabled = axon_config();
        disabled.enabled = false;
        let yaml = serde_yaml::to_string(&disabled).unwrap();
        assert!(yaml.contains("enabled: false"));
    }

    #[test]
    fn expand_env_substitutes_and_defaults_empty() {
        std::env::set_var("CAIRN_TEST_TOKEN", "secret123");
        assert_eq!(
            expand_env_vars("Bearer ${CAIRN_TEST_TOKEN}"),
            "Bearer secret123"
        );
        assert_eq!(expand_env_vars("x${CAIRN_TEST_MISSING_VAR}y"), "xy");
        // Unterminated brace is left literal.
        assert_eq!(expand_env_vars("${oops"), "${oops");
        std::env::remove_var("CAIRN_TEST_TOKEN");
    }

    #[test]
    fn stdio_config_deserializes_with_default_transport() {
        let yaml = r#"
command: npx
args: ["@playwright/mcp@latest"]
"#;
        let cfg: McpServerConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.transport, "stdio");
        assert_eq!(cfg.command.as_deref(), Some("npx"));
        assert_eq!(cfg.args, vec!["@playwright/mcp@latest"]);
    }

    #[test]
    fn expand_vars_applies_to_all_fields() {
        let cfg = McpServerConfig {
            transport: "http".to_string(),
            command: Some("${BIN}".to_string()),
            args: vec!["--key=${KEY}".to_string()],
            env: HashMap::from([("TOKEN".to_string(), "${KEY}".to_string())]),
            url: Some("https://${HOST}/mcp".to_string()),
            headers: HashMap::from([("Authorization".to_string(), "Bearer ${KEY}".to_string())]),
            enabled: true,
            oauth: None,
        };
        let e = cfg.expand_vars(&|var| match var {
            "BIN" => "server".to_string(),
            "KEY" => "abc".to_string(),
            "HOST" => "example.com".to_string(),
            _ => String::new(),
        });
        assert_eq!(e.command.as_deref(), Some("server"));
        assert_eq!(e.args[0], "--key=abc");
        assert_eq!(e.env.get("TOKEN").map(|s| s.as_str()), Some("abc"));
        assert_eq!(e.url.as_deref(), Some("https://example.com/mcp"));
        assert_eq!(
            e.headers.get("Authorization").map(|s| s.as_str()),
            Some("Bearer abc")
        );
    }

    #[test]
    fn referenced_vars_collects_across_fields() {
        let cfg = McpServerConfig {
            transport: "http".to_string(),
            command: Some("${BIN}".to_string()),
            args: vec!["--flag".to_string(), "${ARG_TOKEN}".to_string()],
            env: HashMap::from([("E".to_string(), "${ENV_TOKEN}".to_string())]),
            url: Some("https://${HOST}/mcp".to_string()),
            headers: HashMap::from([(
                "Authorization".to_string(),
                "Bearer ${HDR_TOKEN}".to_string(),
            )]),
            enabled: true,
            oauth: None,
        };
        let vars = cfg.referenced_vars();
        assert!(vars.contains("BIN"));
        assert!(vars.contains("ARG_TOKEN"));
        assert!(vars.contains("ENV_TOKEN"));
        assert!(vars.contains("HOST"));
        assert!(vars.contains("HDR_TOKEN"));
        // A plain field with no reference contributes nothing.
        assert!(!vars.contains("flag"));
        assert_eq!(vars.len(), 5);
    }

    fn axon_config() -> McpServerConfig {
        McpServerConfig {
            transport: "stdio".to_string(),
            command: Some("/opt/homebrew/bin/axon".to_string()),
            args: vec!["mcp".to_string()],
            env: HashMap::new(),
            url: None,
            headers: HashMap::new(),
            enabled: true,
            oauth: None,
        }
    }

    fn git_init(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_bare(path: &Path) {
        assert!(crate::env::git()
            .args(["init", "--bare", "-q"])
            .current_dir(path)
            .status()
            .unwrap()
            .success());
    }

    fn git_set_origin(repo: &Path, origin: &Path) {
        assert!(crate::env::git()
            .args(["remote", "add", "origin"])
            .arg(origin)
            .current_dir(repo)
            .status()
            .unwrap()
            .success());
    }

    fn git_status(path: &Path) -> String {
        let out = crate::env::git()
            .args(["status", "--porcelain"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_head_subject(path: &Path) -> String {
        let out = crate::env::git()
            .args(["log", "-1", "--pretty=%s"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn git_commit_count(path: &Path) -> usize {
        let out = crate::env::git()
            .args(["rev-list", "--count", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout)
            .trim()
            .parse()
            .unwrap_or(0)
    }

    fn git_branch(path: &Path) -> String {
        let out = crate::env::git()
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn origin_has_branch(origin: &Path, branch: &str) -> bool {
        crate::env::git()
            .args(["rev-parse", "--verify", "--quiet"])
            .arg(format!("refs/heads/{branch}"))
            .current_dir(origin)
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    #[test]
    fn workspace_mcp_writes_commit_scoped() {
        let temp = TempDir::new().unwrap();
        let home = temp.path();
        git_init(home);
        std::fs::write(home.join("unrelated.txt"), "dirty").unwrap();

        upsert_workspace_mcp_server(home, "axon", &axon_config()).unwrap();
        assert_eq!(git_head_subject(home), "cairn: update mcp servers");

        delete_workspace_mcp_server(home, "axon").unwrap();
        assert_eq!(git_head_subject(home), "cairn: update mcp servers");

        // One commit per write (no double-commit); unrelated dirt untouched.
        assert_eq!(git_commit_count(home), 2);
        let status = git_status(home);
        assert!(status.contains("unrelated.txt"));
        assert!(!status.contains("settings.yaml"));
    }

    #[test]
    fn project_mcp_upsert_commits_once_and_does_not_push() {
        let temp = TempDir::new().unwrap();
        let origin = temp.path().join("origin.git");
        std::fs::create_dir_all(&origin).unwrap();
        git_bare(&origin);
        let proj = temp.path().join("proj");
        std::fs::create_dir_all(proj.join(".cairn")).unwrap();
        git_init(&proj);
        git_set_origin(&proj, &origin);

        upsert_project_mcp_server(&proj, "axon", &axon_config()).unwrap();

        assert!(git_status(&proj).is_empty(), "project save left repo dirty");
        assert_eq!(git_head_subject(&proj), "cairn: update mcp servers");
        // Exactly one scoped commit — the removed mcp.rs commit no longer doubles up.
        assert_eq!(git_commit_count(&proj), 1);
        // Project scope is commit-only: origin must not have received a push.
        assert!(!origin_has_branch(&origin, &git_branch(&proj)));
    }

    #[test]
    fn upsert_then_load_and_delete_roundtrip() {
        let ws = TempDir::new().unwrap();
        assert!(load_workspace_mcp_servers(ws.path()).is_empty());

        upsert_workspace_mcp_server(ws.path(), "axon", &axon_config()).unwrap();
        let loaded = load_workspace_mcp_servers(ws.path());
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded["axon"].command.as_deref(),
            Some("/opt/homebrew/bin/axon")
        );
        assert_eq!(loaded["axon"].args, vec!["mcp"]);

        // Upsert replaces in place (no duplicate keys).
        let mut updated = axon_config();
        updated.args = vec!["mcp".to_string(), "--verbose".to_string()];
        upsert_workspace_mcp_server(ws.path(), "axon", &updated).unwrap();
        let loaded = load_workspace_mcp_servers(ws.path());
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded["axon"].args, vec!["mcp", "--verbose"]);

        // Delete removes it and clears the now-empty mcpServers block.
        delete_workspace_mcp_server(ws.path(), "axon").unwrap();
        assert!(load_workspace_mcp_servers(ws.path()).is_empty());
        let raw =
            std::fs::read_to_string(super::super::settings::get_settings_path(ws.path())).unwrap();
        assert!(!raw.contains("mcpServers"));
    }

    #[test]
    fn delete_missing_server_is_ok() {
        let ws = TempDir::new().unwrap();
        // No file at all.
        delete_workspace_mcp_server(ws.path(), "nope").unwrap();
        // File exists but server absent.
        upsert_workspace_mcp_server(ws.path(), "axon", &axon_config()).unwrap();
        delete_workspace_mcp_server(ws.path(), "nope").unwrap();
        assert_eq!(load_workspace_mcp_servers(ws.path()).len(), 1);
    }

    #[test]
    fn project_upsert_and_delete_edit_dotcairn_config() {
        let proj = TempDir::new().unwrap();
        let config_path = super::super::project_settings::get_project_config_path(proj.path());

        upsert_project_mcp_server(proj.path(), "axon", &axon_config()).unwrap();
        let loaded = load_project_mcp_servers(proj.path());
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded["axon"].command.as_deref(),
            Some("/opt/homebrew/bin/axon")
        );
        // The project overlay surfaces the server through resolve_mcp_servers.
        let resolved = resolve_mcp_servers(proj.path(), Some(proj.path()));
        assert_eq!(
            resolved["axon"].command.as_deref(),
            Some("/opt/homebrew/bin/axon")
        );
        let raw = std::fs::read_to_string(&config_path).unwrap();
        assert!(raw.starts_with("# Cairn Project Configuration"));

        delete_project_mcp_server(proj.path(), "axon").unwrap();
        let raw = std::fs::read_to_string(&config_path).unwrap();
        assert!(!raw.contains("mcpServers"));
    }

    #[test]
    fn project_edit_preserves_other_project_keys_verbatim() {
        let proj = TempDir::new().unwrap();
        let config_path = super::super::project_settings::get_project_config_path(proj.path());
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::write(
            &config_path,
            "# Cairn Project Configuration\ndefaultBranch: develop\nsetupCommands:\n  - npm install\n",
        )
        .unwrap();

        upsert_project_mcp_server(proj.path(), "axon", &axon_config()).unwrap();

        let raw = std::fs::read_to_string(&config_path).unwrap();
        assert!(raw.contains("defaultBranch: develop"));
        assert!(raw.contains("npm install"));
        assert!(raw.contains("axon"));
        // The surviving config must still parse cleanly into ProjectSettingsFile.
        let loaded = super::super::project_settings::load_project_settings(proj.path());
        assert_eq!(loaded.default_branch.as_deref(), Some("develop"));
        assert!(loaded.mcp_servers.unwrap().contains_key("axon"));
    }

    #[test]
    fn edit_preserves_other_settings_keys_verbatim() {
        let ws = TempDir::new().unwrap();
        let path = super::super::settings::get_settings_path(ws.path());
        // A realistic file WITHOUT maxThinkingTokens: a typed round-trip would
        // re-emit it as `null` (= disabled). The surgical edit must not.
        std::fs::write(
            &path,
            "# Cairn Workspace Settings\nbranchPrefix: feature\nmergeType: rebase\n",
        )
        .unwrap();

        upsert_workspace_mcp_server(ws.path(), "axon", &axon_config()).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("branchPrefix: feature"));
        assert!(raw.contains("mergeType: rebase"));
        assert!(raw.contains("axon"));
        // The bug we are guarding against: no maxThinkingTokens was added.
        assert!(!raw.contains("maxThinkingTokens"));
    }
}
