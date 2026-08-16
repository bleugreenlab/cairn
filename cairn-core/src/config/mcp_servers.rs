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
//! and `headers` support `${VAR}` interpolation, expanded at connect time.
//! Expansion goes through the credential broker
//! ([`crate::security::broker`]), which is the only thing in the system that
//! turns one of these references into plaintext: it resolves from the OS
//! keychain first (see [`super::secrets`]) and then from the app process
//! environment, and registers whatever it resolved for scrubbing before the
//! value can be injected anywhere. The keychain path is what lets a
//! Finder/Dock-launched app — which inherits a minimal environment — reach
//! token-bearing servers: the user enters the secret in the settings UI, it is
//! stored in the keychain, and `settings.yaml` keeps only the `${VAR}`
//! reference.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

/// Filesystem context carried only by an installed Agent Plugin MCP server.
///
/// This is deliberately excluded from serde: plugin roots are installation
/// state, not authored MCP configuration, and must not become a way for native
/// or user entries to opt into plugin placeholder semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPluginRuntime {
    pub root: PathBuf,
    pub data: PathBuf,
}

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
    /// Stdio: working directory for the spawned process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
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
    /// Which of this server's `${VAR}` references carry credentials.
    ///
    /// The declared-secret signal the broker consumes. A value stored in the OS
    /// keychain is already declared by the choice of store, so this list exists
    /// for the other case: a `${VAR}` that resolves from the *process
    /// environment* and really is a token, which nothing about the bytes can
    /// distinguish from `${HOME}`. Naming it here makes the resolved value a
    /// scrub target.
    ///
    /// Deliberately not "every referenced var". `${VAR}` interpolation reaches
    /// `command`, `args`, and `url` as well as `env` and `headers`, so most
    /// references are ordinary configuration; registering those would refuse
    /// every model-authored write that mentions the path. See
    /// `security::broker` for the full reasoning.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<String>,
    /// Installation-only Agent Plugin launch context.
    #[serde(skip)]
    pub agent_plugin_runtime: Option<AgentPluginRuntime>,
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

// ============================================================================
// Configuration identity
// ============================================================================

/// Digest algorithm behind [`fingerprint_mcp_config`].
pub const MCP_CONFIG_FINGERPRINT_ALGORITHM: &str = "sha256";

/// Version of the canonical encoding fed to that digest. Bump on any change to
/// the field set or its order, so a digest can never mean two things.
///
/// v2 added the `secrets` declaration. It is identity-bearing because removing
/// a var from it downgrades that credential from "scrubbed everywhere" to "in
/// the clear if the server echoes it", and a standing grant must not carry a
/// server across that change silently. Bumping re-prompts on the next
/// reconfigure, which is the intended cost.
/// v3 added stdio `cwd`, which changes where a server executes and therefore
/// belongs to the authority identity just like its command and arguments.
pub const MCP_CONFIG_FINGERPRINT_ENCODING_VERSION: u32 = 3;

/// Domain separator. Keeps these digests from colliding with any other
/// sha256-over-config in the system, now or later.
const MCP_CONFIG_FINGERPRINT_DOMAIN: &str = "cairn.authority.mcp-config";

/// A canonical encoder: every field is written as a length-prefixed key and a
/// length-prefixed value, so no value can impersonate a field boundary and two
/// different configurations cannot produce the same byte stream by splicing.
struct CanonicalEncoder {
    hasher: sha2::Sha256,
}

impl CanonicalEncoder {
    fn new(domain: &str, version: u32) -> Self {
        let mut encoder = Self {
            hasher: <sha2::Sha256 as sha2::Digest>::new(),
        };
        encoder.field("domain", domain);
        encoder.field("encoding", &version.to_string());
        encoder
    }

    fn field(&mut self, key: &str, value: &str) {
        use sha2::Digest;
        self.hasher.update((key.len() as u64).to_be_bytes());
        self.hasher.update(key.as_bytes());
        self.hasher.update((value.len() as u64).to_be_bytes());
        self.hasher.update(value.as_bytes());
    }

    /// An ordered sequence: the count is hashed too, so a list cannot be
    /// lengthened or shortened without changing the digest.
    fn sequence(&mut self, key: &str, values: &[String]) {
        self.field(key, &values.len().to_string());
        for (index, value) in values.iter().enumerate() {
            self.field(&format!("{key}[{index}]"), value);
        }
    }

    /// An unordered map, canonicalized by sorting keys. `HashMap` iteration
    /// order is arbitrary and varies run to run, so without this the same
    /// configuration would fingerprint differently on each attempt and no
    /// standing grant could ever be reused.
    fn map(&mut self, key: &str, values: &HashMap<String, String>) {
        let sorted: BTreeMap<&String, &String> = values.iter().collect();
        self.field(key, &sorted.len().to_string());
        for (name, value) in sorted {
            self.field(&format!("{key}.key"), name);
            self.field(&format!("{key}.value"), value);
        }
    }

    fn finish(self) -> String {
        use sha2::Digest;
        format!("{:x}", self.hasher.finalize())
    }
}

/// Fingerprint the **resultant** configuration of an MCP registry mutation:
/// what would be registered under this name once the change has been applied,
/// or — for a delete — the entry that would be removed.
///
/// This is the identity an authority grant binds to, so it must cover
/// everything that decides what gets executed or connected to, and nothing that
/// does not. `config` is `None` only when there is genuinely no entry (an
/// unresolvable target), which fingerprints distinctly from any real one rather
/// than collapsing into some default.
///
/// # Secrets
///
/// Values are hashed **exactly as authored**. A `${TOKEN}` reference contributes
/// the literal seven characters `${TOKEN}`, so repointing a server at a
/// different secret changes its identity and re-prompts, while nothing here
/// reads the keychain, the process environment, or any resolved bearer value.
/// The digest is one-way regardless, but not resolving is what keeps a secret
/// value from ever entering this code path at all.
pub fn fingerprint_mcp_config(
    workspace_id: &str,
    project_id: Option<&str>,
    server: &str,
    mutation: &str,
    config: Option<&McpServerConfig>,
) -> cairn_common::authorization::McpConfigFingerprint {
    let mut encoder = CanonicalEncoder::new(
        MCP_CONFIG_FINGERPRINT_DOMAIN,
        MCP_CONFIG_FINGERPRINT_ENCODING_VERSION,
    );
    encoder.field("workspace", workspace_id);
    encoder.field("project", project_id.unwrap_or(""));
    encoder.field("server", server);
    encoder.field("mutation", mutation);

    match config {
        None => encoder.field("config", "absent"),
        Some(config) => {
            // Destructured exhaustively, not field-accessed, on purpose. The
            // whole value of this constraint is the claim that every
            // security-relevant field changes the digest; a field added to
            // `McpServerConfig` later must be a compile error HERE rather than a
            // digest that silently keeps its old value while the configuration
            // it names has changed. If you are reading this because the
            // destructure stopped compiling: decide whether the new field is
            // identity-bearing, and either hash it or bind it to `_` with a note
            // saying why it is not.
            let McpServerConfig {
                transport,
                command,
                args,
                env,
                cwd,
                url,
                headers,
                enabled,
                oauth,
                secrets,
                agent_plugin_runtime: _,
            } = config;
            encoder.field("config", "present");
            encoder.field("transport", transport);
            encoder.field("command", command.as_deref().unwrap_or(""));
            encoder.sequence("args", args);
            encoder.map("env", env);
            encoder.field("cwd", cwd.as_deref().unwrap_or(""));
            encoder.field("url", url.as_deref().unwrap_or(""));
            encoder.map("headers", headers);
            encoder.field("enabled", if *enabled { "true" } else { "false" });
            // Identity-bearing: which references are treated as credentials
            // decides whether their resolved values are scrubbed from observed
            // output, so widening or narrowing it is a change to what the
            // approval covered.
            encoder.sequence("secrets", secrets);
            match oauth.as_ref() {
                None => encoder.field("oauth", "absent"),
                Some(oauth) => {
                    encoder.field("oauth", "present");
                    encoder.field("oauth.clientId", oauth.client_id.as_deref().unwrap_or(""));
                    encoder.sequence("oauth.scopes", &oauth.scopes);
                }
            }
        }
    }

    cairn_common::authorization::McpConfigFingerprint {
        algorithm: MCP_CONFIG_FINGERPRINT_ALGORITHM.to_string(),
        encoding_version: MCP_CONFIG_FINGERPRINT_ENCODING_VERSION,
        digest: encoder.finish(),
    }
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
    ///
    /// The pure substitution mechanism, with no opinion about where a value
    /// comes from. Crate-visible on purpose: supplying the resolver is the
    /// credential broker's job, and a second caller with its own closure is
    /// exactly the parallel resolution chain that left the settings-save
    /// connection test registering nothing.
    pub(crate) fn expand_vars(&self, resolve: &dyn Fn(&str) -> String) -> McpServerConfig {
        // Agent Plugin portable fields use only PLUGIN_ROOT / PLUGIN_DATA and
        // are expanded by the gateway after the managed data directory exists.
        // Sending them through Cairn's credential broker would erase them as
        // unknown native `${VAR}` references.
        if self.agent_plugin_runtime.is_some() {
            return self.clone();
        }
        let map = |s: &str| expand_with(s, resolve);
        McpServerConfig {
            transport: self.transport.clone(),
            command: self.command.as_deref().map(&map),
            args: self.args.iter().map(|a| map(a)).collect(),
            env: self.env.iter().map(|(k, v)| (k.clone(), map(v))).collect(),
            cwd: self.cwd.as_deref().map(&map),
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
            // The declaration names variables, not values, so it survives
            // expansion unchanged.
            secrets: self.secrets.clone(),
            agent_plugin_runtime: None,
        }
    }

    /// Connect-time expansion for `credential_key` (the scoped credential key).
    ///
    /// Every `${VAR}` resolves through the credential broker, which is what
    /// makes a resolved credential a scrub target before it can be injected
    /// anywhere. A missing secret expands to an empty string and surfaces as a
    /// connect/auth failure from the server, not a panic here.
    ///
    /// The result deliberately is not an `McpServerConfig`: an expanded config
    /// carries credentials, and `McpServerConfig` is `Serialize` because the
    /// *authored* form lives in `settings.yaml`. See
    /// [`crate::security::BrokeredMcpConfig`].
    pub(crate) fn brokered(
        &self,
        credential_key: &str,
        purpose: &str,
    ) -> crate::security::BrokeredMcpConfig {
        crate::security::broker::mcp_server(credential_key, self, &HashMap::new(), purpose)
    }

    /// Whether `var` is declared to carry a credential. See [`Self::secrets`].
    pub fn declares_secret(&self, var: &str) -> bool {
        self.secrets.iter().any(|declared| declared == var)
    }

    /// Why this server cannot connect yet, or `None` when it can.
    ///
    /// Both checks are synchronous and reuse machinery the connect path already
    /// relies on, so a readiness verdict and a connect attempt agree by
    /// construction rather than by convention. A readiness probe must never
    /// perform network I/O, so the OAuth check reads the stored credential
    /// directly rather than going through the refreshing token path.
    ///
    /// This gates PACK-origin servers only (see [`resolve_mcp_servers`]).
    /// Content the user did not author must be inert until it can actually
    /// work; configuration the user DID author still fails loudly.
    pub fn readiness(&self, credential_key: &str) -> Option<NotReady> {
        let missing: Vec<String> = self
            .referenced_vars()
            .into_iter()
            .filter(|var| !crate::security::broker::mcp_var_is_set(credential_key, self, var))
            .collect();
        if !missing.is_empty() {
            return Some(NotReady::MissingVars { vars: missing });
        }

        if self.oauth.is_some()
            && crate::mcp::oauth::store::status(credential_key).state == "needs_auth"
        {
            return Some(NotReady::NeedsAuth);
        }

        None
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
        if let Some(cwd) = &self.cwd {
            scan(cwd);
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

/// Why a configured MCP server cannot connect yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "reason")]
pub enum NotReady {
    /// `${VAR}` references with no value in the keychain or the environment.
    MissingVars { vars: Vec<String> },
    /// OAuth is configured but no usable token is stored.
    NeedsAuth,
}

impl NotReady {
    /// Compact rendering for logs and the catalog resource.
    pub fn summary(&self) -> String {
        match self {
            NotReady::MissingVars { vars } => format!("missing_vars({})", vars.join(", ")),
            NotReady::NeedsAuth => "needs_auth".to_string(),
        }
    }
}

/// Where a workspace-visible MCP server came from, and whether it can connect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkspaceMcpEntry {
    #[serde(flatten)]
    pub config: McpServerConfig,
    /// `workspace` for a user-authored entry, `pack:<id>` for a pack default.
    pub origin: String,
    /// Present when the server is configured but cannot connect yet. A
    /// pack-origin entry in this state is withheld from agents; the settings
    /// surface still shows it so the user can supply what it needs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_ready: Option<NotReady>,
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
fn expand_with(input: &str, resolve: &dyn Fn(&str) -> String) -> String {
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

/// Resolve the effective MCP server registry for a run: installed packs' server
/// definitions, overlaid by workspace servers from `~/.cairn/settings.yaml`,
/// overlaid by the project's `.cairn/config.yaml` (the innermost scope wins on a
/// key collision). `project_path` is `None` for project-less (workspace-only)
/// contexts.
///
/// Packs sit at the bottom of the chain rather than being merged into the user's
/// settings, so a pack default and a user's fork of it stay distinguishable —
/// the same pack-owned / user-forked model the file-backed resources use.
pub(crate) fn resolve_mcp_servers(
    config_dir: &Path,
    project_path: Option<&Path>,
) -> HashMap<String, McpServerConfig> {
    let workspace = load_settings_mcp_servers(config_dir);
    let mut servers: HashMap<String, McpServerConfig> = HashMap::new();

    for (name, entry) in super::pack::mcp::load_pack_mcp_servers(config_dir) {
        if workspace.contains_key(&name) {
            // The user forked this server into their own settings; that entry is
            // theirs and resolves ungated below.
            continue;
        }
        // A pack server arrives without the user having configured anything, so
        // it must be inert until it can actually connect. Left ungated, every
        // holder of a connector pack would get a failing spawn or an
        // unauthenticated request in every session.
        if let Some(reason) = entry
            .config
            .readiness(&super::secrets::credential_key(&name, None))
        {
            log::debug!(
                "Withholding MCP server `{name}` from agents: provided by pack `{}` and not ready ({})",
                entry.pack_id,
                reason.summary()
            );
            continue;
        }
        servers.insert(name, entry.config);
    }

    servers.extend(workspace);

    if let Some(project_path) = project_path {
        if let Some(project_servers) =
            super::project_settings::load_project_settings_read_only(project_path).mcp_servers
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

/// The raw `mcpServers` block of `~/.cairn/settings.yaml` — the servers the user
/// authored, with no pack layer beneath. Returns an empty map if the file is
/// missing, unparsable, or has no `mcpServers` block.
pub fn load_settings_mcp_servers(config_dir: &Path) -> HashMap<String, McpServerConfig> {
    super::settings::load_settings_file(config_dir)
        .ok()
        .and_then(|f| f.mcp_servers)
        .unwrap_or_default()
}

/// Load the workspace-level external MCP server registry: every installed pack's
/// servers, overlaid by the user's own `settings.yaml` entries.
///
/// This is the registry the management UI edits and authorizes against, so it is
/// deliberately unfiltered — a disabled or not-yet-ready server is present and
/// can be toggled, configured, or authorized. Use [`workspace_mcp_entries`] when
/// the caller needs to render *why* a server is not live.
pub fn load_workspace_mcp_servers(config_dir: &Path) -> HashMap<String, McpServerConfig> {
    let mut servers: HashMap<String, McpServerConfig> =
        super::pack::mcp::load_pack_mcp_servers(config_dir)
            .into_iter()
            .map(|(name, entry)| (name, entry.config))
            .collect();
    servers.extend(load_settings_mcp_servers(config_dir));
    servers
}

/// The workspace registry annotated with each server's origin and, when it
/// cannot connect yet, the reason. This is what lets a surface render "needs
/// path" or "needs auth" and offer the authorize action, rather than silently
/// showing a server that does nothing.
pub fn workspace_mcp_entries(config_dir: &Path) -> BTreeMap<String, WorkspaceMcpEntry> {
    let mut entries = BTreeMap::new();
    let workspace = load_settings_mcp_servers(config_dir);

    for (name, entry) in super::pack::mcp::load_pack_mcp_servers(config_dir) {
        if workspace.contains_key(&name) {
            continue;
        }
        let not_ready = entry
            .config
            .readiness(&super::secrets::credential_key(&name, None));
        entries.insert(
            name,
            WorkspaceMcpEntry {
                config: entry.config,
                origin: format!("pack:{}", entry.pack_id),
                not_ready,
            },
        );
    }

    for (name, config) in workspace {
        entries.insert(
            name,
            WorkspaceMcpEntry {
                config,
                origin: "workspace".to_string(),
                not_ready: None,
            },
        );
    }

    entries
}

/// Load the project-level external MCP server registry from
/// `[project]/.cairn/config.yaml`. Returns an empty map if the file is missing
/// or has no `mcpServers` block.
///
/// This is the unfiltered registry the management UI edits, so disabled entries
/// are included and can be toggled back on.
pub fn load_project_mcp_servers(project_path: &Path) -> HashMap<String, McpServerConfig> {
    super::project_settings::load_project_settings_read_only(project_path)
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
    super::settings::mutate_workspace_settings(config_dir, "cairn: update mcp servers", |root| {
        upsert_mcp_server(root, name, config)
    })
}

/// Remove one workspace MCP server by `name`. Succeeds even if the server (or
/// the file) does not exist. Drops the `mcpServers` block entirely when it
/// becomes empty so the file stays clean.
///
/// A server supplied by an installed pack is removed by recording the removal
/// against that pack, not by editing a settings file that never mentioned it.
/// The pack stays installed and keeps updating; this one server stops being
/// offered. Removing an item must not require uninstalling the pack around it.
pub fn delete_workspace_mcp_server(config_dir: &Path, name: &str) -> Result<(), String> {
    super::pack::note_removed_item(config_dir, super::pack::PackItemKind::Mcp, name);
    super::settings::mutate_workspace_settings(config_dir, "cairn: update mcp servers", |root| {
        delete_mcp_server(root, name);
        Ok(())
    })
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
    upsert_mcp_server(&mut root, name, config)?;
    write_settings_mapping(path, header, &root)
}

fn upsert_mcp_server(
    root: &mut serde_yaml::Mapping,
    name: &str,
    config: &McpServerConfig,
) -> Result<(), String> {
    let servers = root
        .entry(serde_yaml::Value::String("mcpServers".to_string()))
        .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    let servers = servers
        .as_mapping_mut()
        .ok_or_else(|| "`mcpServers` in config is not a mapping".to_string())?;
    servers.insert(
        serde_yaml::Value::String(name.to_string()),
        serde_yaml::to_value(config)
            .map_err(|error| format!("Failed to serialize server: {error}"))?,
    );
    Ok(())
}

/// Remove one MCP server from the `mcpServers` mapping of a YAML settings file.
/// Drops the now-empty `mcpServers` block so the file stays clean.
fn delete_mcp_server_from_file(path: &Path, header: &str, name: &str) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let mut root = load_settings_mapping(path)?;
    delete_mcp_server(&mut root, name);
    write_settings_mapping(path, header, &root)
}

fn delete_mcp_server(root: &mut serde_yaml::Mapping, name: &str) {
    let key = serde_yaml::Value::String("mcpServers".to_string());
    if let Some(servers) = root.get_mut(&key).and_then(|value| value.as_mapping_mut()) {
        servers.remove(serde_yaml::Value::String(name.to_string()));
        if servers.is_empty() {
            root.remove(&key);
        }
    }
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
    use crate::config::secrets::mock_keychain;
    use tempfile::TempDir;

    /// Install a pack in `config_dir` whose `mcp.yaml` holds `servers`.
    fn install_pack_with_servers(config_dir: &Path, id: &str, servers: &str) {
        let dir = config_dir.join("packs").join(id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("pack.yaml"),
            format!(
                "cairnVersion: 1\nid: {id}\nname: {id}\nversion: 1.0.0\n\
                 installedAt: now\ncontentHash: h\nsource:\n  kind: bundled\n"
            ),
        )
        .unwrap();
        std::fs::write(dir.join("mcp.yaml"), servers).unwrap();
    }

    #[test]
    fn a_pack_server_resolves_and_a_settings_entry_shadows_it() {
        mock_keychain::install();
        let ws = TempDir::new().unwrap();
        install_pack_with_servers(
            ws.path(),
            "demo",
            "mcpServers:\n  demo:\n    command: pack-cmd\n  other:\n    command: other-cmd\n",
        );

        let servers = resolve_mcp_servers(ws.path(), None);
        assert_eq!(servers["demo"].command.as_deref(), Some("pack-cmd"));
        assert_eq!(servers["other"].command.as_deref(), Some("other-cmd"));

        // The user forks one of them into their own settings: their entry wins,
        // and the sibling pack default is untouched.
        std::fs::write(
            ws.path().join("settings.yaml"),
            "mcpServers:\n  demo:\n    command: /usr/local/bin/demo\n",
        )
        .unwrap();
        let servers = resolve_mcp_servers(ws.path(), None);
        assert_eq!(
            servers["demo"].command.as_deref(),
            Some("/usr/local/bin/demo")
        );
        assert_eq!(servers["other"].command.as_deref(), Some("other-cmd"));

        let entries = workspace_mcp_entries(ws.path());
        assert_eq!(entries["demo"].origin, "workspace");
        assert_eq!(entries["other"].origin, "pack:demo");

        // Disabling the fork removes it from agent-facing resolution entirely;
        // the pack layer does not resurrect it.
        std::fs::write(
            ws.path().join("settings.yaml"),
            "mcpServers:\n  demo:\n    command: /usr/local/bin/demo\n    enabled: false\n",
        )
        .unwrap();
        assert!(!resolve_mcp_servers(ws.path(), None).contains_key("demo"));
    }

    #[test]
    fn a_pack_server_with_an_unresolved_var_is_inert_until_it_can_connect() {
        mock_keychain::install();
        let ws = TempDir::new().unwrap();
        install_pack_with_servers(
            ws.path(),
            "matlab",
            "mcpServers:\n  readiness-matlab:\n    command: ${READINESS_MATLAB_BIN}\n",
        );

        // Unconfigured: withheld from agents, but visible and explained in the
        // surface the user acts on.
        assert!(!resolve_mcp_servers(ws.path(), None).contains_key("readiness-matlab"));
        let entries = workspace_mcp_entries(ws.path());
        assert_eq!(
            entries["readiness-matlab"].not_ready,
            Some(NotReady::MissingVars {
                vars: vec!["READINESS_MATLAB_BIN".to_string()]
            })
        );
        assert_eq!(entries["readiness-matlab"].origin, "pack:matlab");

        // Supplying the value is the only step: nothing else has to be flipped.
        crate::config::secrets::set_secret(
            "readiness-matlab",
            "READINESS_MATLAB_BIN",
            "/opt/matlab-mcp",
        )
        .unwrap();
        assert!(resolve_mcp_servers(ws.path(), None).contains_key("readiness-matlab"));
        assert!(workspace_mcp_entries(ws.path())["readiness-matlab"]
            .not_ready
            .is_none());
    }

    #[test]
    fn a_user_authored_server_with_an_unresolved_var_is_never_gated() {
        mock_keychain::install();
        let ws = TempDir::new().unwrap();
        std::fs::write(
            ws.path().join("settings.yaml"),
            "mcpServers:\n  mine:\n    command: ${DEFINITELY_UNSET_FOR_THIS_TEST}\n",
        )
        .unwrap();

        // Their configuration, their failure to see. Swallowing it would be
        // worse than the connect error.
        assert!(resolve_mcp_servers(ws.path(), None).contains_key("mine"));
        assert!(workspace_mcp_entries(ws.path())["mine"].not_ready.is_none());
    }

    #[test]
    fn an_oauth_pack_server_is_ready_only_with_a_usable_credential() {
        mock_keychain::install();
        let ws = TempDir::new().unwrap();
        install_pack_with_servers(
            ws.path(),
            "linear",
            "mcpServers:\n  readiness-linear:\n    type: http\n    url: https://mcp.example.test/mcp\n    oauth:\n      scopes: []\n",
        );

        // Ships enabled, but no stored credential means it does nothing yet.
        assert!(!resolve_mcp_servers(ws.path(), None).contains_key("readiness-linear"));
        assert_eq!(
            workspace_mcp_entries(ws.path())["readiness-linear"].not_ready,
            Some(NotReady::NeedsAuth)
        );

        // Readiness follows token USABILITY, not mere presence: an expired token
        // with no refresh is still needs-auth.
        let mut auth = crate::mcp::oauth::store::StoredAuth {
            issuer: Some("https://auth.example.test".into()),
            resource: "https://mcp.example.test/mcp".into(),
            authorization_endpoint: "https://auth.example.test/authorize".into(),
            token_endpoint: "https://auth.example.test/token".into(),
            registration_endpoint: None,
            client_id: "client".into(),
            client_secret: None,
            access_token: "token".into(),
            refresh_token: None,
            expires_at: Some(chrono::Utc::now().timestamp() - 10),
            scopes: vec![],
            needs_scopes: vec![],
        };
        crate::mcp::oauth::store::save("readiness-linear", &auth).unwrap();
        assert!(!resolve_mcp_servers(ws.path(), None).contains_key("readiness-linear"));

        // A live token promotes it, with no other state to change.
        auth.expires_at = Some(chrono::Utc::now().timestamp() + 3600);
        crate::mcp::oauth::store::save("readiness-linear", &auth).unwrap();
        assert!(resolve_mcp_servers(ws.path(), None).contains_key("readiness-linear"));
        assert!(workspace_mcp_entries(ws.path())["readiness-linear"]
            .not_ready
            .is_none());
    }

    /// Deleting a pack-provided server removes THAT server, not the pack around
    /// it. Wanting a pack's skill but not its connector is an ordinary thing to
    /// want, and the removal has to survive the next sync.
    #[test]
    fn deleting_a_pack_provided_server_removes_just_that_item() {
        mock_keychain::install();
        let ws = TempDir::new().unwrap();
        install_pack_with_servers(
            ws.path(),
            "connectors",
            "mcpServers:\n  alpha:\n    type: http\n    url: https://alpha.test/mcp\n  beta:\n    type: http\n    url: https://beta.test/mcp\n",
        );
        assert!(resolve_mcp_servers(ws.path(), None).contains_key("alpha"));

        delete_workspace_mcp_server(ws.path(), "alpha").unwrap();

        let servers = resolve_mcp_servers(ws.path(), None);
        assert!(!servers.contains_key("alpha"), "the removed server is gone");
        assert!(
            servers.contains_key("beta"),
            "its pack stays installed and keeps supplying everything else"
        );

        // The removal is recorded against the pack, so a sync cannot undo it.
        let lock = crate::config::pack::lock::read_lock(ws.path(), "connectors").unwrap();
        assert!(lock.is_removed(crate::config::pack::PackItemKind::Mcp, "alpha"));

        // And it is reversible without reinstalling anything.
        crate::config::pack::lock::restore_removed_items(
            &crate::services::RealFileSystem,
            ws.path(),
            "connectors",
        )
        .unwrap();
        assert!(resolve_mcp_servers(ws.path(), None).contains_key("alpha"));
    }

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
            secrets: Vec::new(),
            cwd: None,
            agent_plugin_runtime: None,
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
            cwd: Some("${WORK_DIR}".to_string()),
            url: Some("https://${HOST}/mcp".to_string()),
            headers: HashMap::from([(
                "Authorization".to_string(),
                "Bearer ${HDR_TOKEN}".to_string(),
            )]),
            enabled: true,
            oauth: None,
            secrets: Vec::new(),
            agent_plugin_runtime: None,
        };
        let vars = cfg.referenced_vars();
        assert!(vars.contains("BIN"));
        assert!(vars.contains("ARG_TOKEN"));
        assert!(vars.contains("ENV_TOKEN"));
        assert!(vars.contains("HOST"));
        assert!(vars.contains("HDR_TOKEN"));
        // A plain field with no reference contributes nothing.
        assert!(!vars.contains("flag"));
        assert!(vars.contains("WORK_DIR"));
        assert_eq!(vars.len(), 6);
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
            secrets: Vec::new(),
            cwd: None,
            agent_plugin_runtime: None,
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
        let loaded = super::super::project_settings::load_project_settings_read_only(proj.path());
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
            "# Cairn Workspace Settings\nlogLevel: verbose\nmergeType: rebase\n",
        )
        .unwrap();

        upsert_workspace_mcp_server(ws.path(), "axon", &axon_config()).unwrap();

        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(raw.contains("logLevel: verbose"));
        assert!(raw.contains("mergeType: rebase"));
        assert!(raw.contains("axon"));
        // The bug we are guarding against: no maxThinkingTokens was added.
        assert!(!raw.contains("maxThinkingTokens"));
    }
}

#[cfg(test)]
mod fingerprint_tests {
    use super::*;

    fn base() -> McpServerConfig {
        McpServerConfig {
            transport: "stdio".to_string(),
            command: Some("npx".to_string()),
            args: vec!["linear-mcp".to_string()],
            env: HashMap::from([("TOKEN".to_string(), "${LINEAR_TOKEN}".to_string())]),
            url: None,
            headers: HashMap::new(),
            enabled: true,
            oauth: None,
            secrets: Vec::new(),
            cwd: None,
            agent_plugin_runtime: None,
        }
    }

    fn digest_of(config: &McpServerConfig) -> String {
        fingerprint_mcp_config("default", None, "linear", "create", Some(config)).digest
    }

    #[test]
    fn identical_configurations_share_an_identity() {
        assert_eq!(digest_of(&base()), digest_of(&base()));
    }

    #[test]
    fn map_iteration_order_does_not_change_the_identity() {
        // HashMap order varies run to run. If it leaked into the digest, the
        // same configuration would fingerprint differently on each attempt and
        // no standing grant could ever be reused.
        let mut wide = base();
        wide.env = HashMap::from([
            ("A".to_string(), "1".to_string()),
            ("B".to_string(), "2".to_string()),
            ("C".to_string(), "3".to_string()),
            ("D".to_string(), "4".to_string()),
        ]);
        wide.headers = HashMap::from([
            ("X-One".to_string(), "1".to_string()),
            ("X-Two".to_string(), "2".to_string()),
        ]);
        let first = digest_of(&wide);
        for _ in 0..16 {
            let mut shuffled = wide.clone();
            shuffled.env = wide.env.clone().into_iter().collect();
            shuffled.headers = wide.headers.clone().into_iter().collect();
            assert_eq!(first, digest_of(&shuffled));
        }
    }

    #[test]
    fn argument_order_is_identity_bearing() {
        let mut one = base();
        one.args = vec!["a".to_string(), "b".to_string()];
        let mut other = base();
        other.args = vec!["b".to_string(), "a".to_string()];
        assert_ne!(
            digest_of(&one),
            digest_of(&other),
            "argument order decides what a command does"
        );
    }

    #[test]
    fn every_security_relevant_field_changes_the_identity() {
        let original = digest_of(&base());
        let mut variants: Vec<(&str, McpServerConfig)> = Vec::new();

        let mut transport = base();
        transport.transport = "http".to_string();
        transport.url = Some("https://example.test".to_string());
        variants.push(("transport+url", transport));

        let mut command = base();
        command.command = Some("curl".to_string());
        variants.push(("command", command));

        let mut args = base();
        args.args = vec!["linear-mcp".to_string(), "--unsafe".to_string()];
        variants.push(("args", args));

        let mut env_value = base();
        env_value.env = HashMap::from([("TOKEN".to_string(), "${OTHER_TOKEN}".to_string())]);
        variants.push(("env secret reference", env_value));

        let mut env_key = base();
        env_key.env = HashMap::from([("OTHER".to_string(), "${LINEAR_TOKEN}".to_string())]);
        variants.push(("env key", env_key));

        let mut headers = base();
        headers.headers = HashMap::from([("Authorization".to_string(), "${K}".to_string())]);
        variants.push(("headers", headers));

        let mut enabled = base();
        enabled.enabled = false;
        variants.push(("enabled", enabled));

        let mut oauth = base();
        oauth.oauth = Some(OAuthServerConfig {
            client_id: Some("client".to_string()),
            scopes: vec!["read".to_string()],
        });
        variants.push(("oauth", oauth.clone()));

        let mut scopes = oauth.clone();
        scopes.oauth = Some(OAuthServerConfig {
            client_id: Some("client".to_string()),
            scopes: vec!["read".to_string(), "write".to_string()],
        });
        assert_ne!(digest_of(&oauth), digest_of(&scopes), "oauth scopes");

        for (field, variant) in variants {
            assert_ne!(
                original,
                digest_of(&variant),
                "changing {field} must change the configuration identity, or a standing grant \
                 would authorize the changed server without asking"
            );
        }
    }
}

#[cfg(test)]
mod fingerprint_secret_tests {
    use super::*;

    fn authored() -> McpServerConfig {
        McpServerConfig {
            transport: "stdio".to_string(),
            command: Some("npx".to_string()),
            args: vec!["linear-mcp".to_string()],
            env: HashMap::from([("TOKEN".to_string(), "${LINEAR_TOKEN}".to_string())]),
            url: None,
            headers: HashMap::new(),
            enabled: true,
            oauth: None,
            secrets: Vec::new(),
            cwd: None,
            agent_plugin_runtime: None,
        }
    }

    #[test]
    fn identity_covers_the_server_the_scope_and_the_mutation() {
        let config = authored();
        let create = fingerprint_mcp_config("default", None, "linear", "create", Some(&config));
        assert_ne!(
            create.digest,
            fingerprint_mcp_config("default", None, "linear", "delete", Some(&config)).digest,
            "an approval to install is not an approval to remove"
        );
        assert_ne!(
            create.digest,
            fingerprint_mcp_config("default", None, "github", "create", Some(&config)).digest,
            "the same config under another name is another server"
        );
        assert_ne!(
            create.digest,
            fingerprint_mcp_config("default", Some("proj"), "linear", "create", Some(&config))
                .digest,
            "scope is part of what was approved"
        );
        assert_ne!(
            create.digest,
            fingerprint_mcp_config("default", None, "linear", "create", None).digest,
            "an absent entry is not some default entry"
        );
    }

    #[test]
    fn the_authored_form_is_what_gets_hashed_never_a_resolved_secret() {
        // If the digest were taken after expansion, two installs of the same
        // server would disagree whenever the environment did -- and a secret's
        // plaintext would have had to be read into the authorization path to
        // get there.
        let expanded = authored().expand_vars(&|_var| "super-secret-value".to_string());
        assert_eq!(
            expanded.env.get("TOKEN").map(String::as_str),
            Some("super-secret-value"),
            "fixture sanity: expansion really does substitute"
        );
        assert_ne!(
            fingerprint_mcp_config("default", None, "linear", "create", Some(&authored())).digest,
            fingerprint_mcp_config("default", None, "linear", "create", Some(&expanded)).digest,
            "authored and expanded are different inputs; only the authored one may reach here"
        );
    }

    #[test]
    fn a_fingerprint_carries_no_configuration_text() {
        let mut secretive = authored();
        secretive.env =
            HashMap::from([("TOKEN".to_string(), "literal-plaintext-token".to_string())]);
        let printed = format!(
            "{:?}",
            fingerprint_mcp_config("default", None, "linear", "create", Some(&secretive))
        );
        assert!(
            !printed.contains("literal-plaintext-token"),
            "a fingerprint is persisted and rendered; it must never carry authored values"
        );
        assert!(!printed.contains("npx"));
        assert!(printed.contains("sha256"));
    }
}
