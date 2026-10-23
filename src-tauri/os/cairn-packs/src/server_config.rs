//! Authored MCP server configuration and pure substitution/fingerprint operations.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;

use crate::mcp::McpVarRequirement;

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
    /// the host OAuth client) and attaches it at connect time. Only
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

/// Version of the catalog binding's canonical encoding. Bump when its field
/// set, order, or interpretation changes; validators must reject other versions.
pub const CATALOG_BINDING_VERSION: u32 = 1;

const CATALOG_BINDING_DOMAIN: &str = "cairn.mcp.catalog-binding";

/// Token-free identity of the context under which a catalog was fetched.
///
/// Ownership paths must already be canonicalized by the caller. The config
/// identity must come from `fingerprint_mcp_config` over the authored config,
/// with the requesting project's identity, server name, and "catalog" operation.
/// Server-provided cacheScope is deliberately not authority-bearing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogBinding {
    pub binding_version: u32,
    /// "workspace" or "project:{canonical path}".
    pub ownership_scope: String,
    /// Authorization identity, separate from filesystem ownership.
    pub project_id: Option<String>,
    /// "user" or "pack:{id}".
    pub owner: String,
    pub config_identity: cairn_common::authorization::McpConfigFingerprint,
    pub credential_key: String,
    pub credential_context: CatalogCredentialContext,
    pub protocol_version: String,
    /// The operation being cached: "tools/list".
    pub method: String,
    pub authorization_revision: u32,
}

/// Stable credential witnesses, never credential values.
///
/// Static credentials and OAuth may coexist on one server. An absent witness
/// means that credential kind is not used, NOT that its storage is unavailable.
/// Callers must reject missing, unreadable, or updating required witnesses before
/// constructing a binding; this pure encoder cannot establish credential state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogCredentialContext {
    pub static_generation: Option<CredentialGeneration>,
    pub oauth: CatalogOAuthState,
}

/// The OAuth state a catalog was fetched under.
///
/// Signed-out carries its tombstone generation rather than collapsing into
/// "no OAuth": a sign-in and sign-out that both land during one fetch must leave
/// the witness different, or the signed-in account's catalog would be recorded
/// as anonymous.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CatalogOAuthState {
    /// No OAuth credential has ever been stored under this key.
    Never,
    SignedOut(CredentialGeneration),
    SignedIn(CatalogOAuthContext),
}

impl CatalogOAuthState {
    pub fn signed_in(&self) -> Option<&CatalogOAuthContext> {
        match self {
            Self::SignedIn(context) => Some(context),
            _ => None,
        }
    }

    pub fn signed_in_mut(&mut self) -> Option<&mut CatalogOAuthContext> {
        match self {
            Self::SignedIn(context) => Some(context),
            _ => None,
        }
    }
}

/// A credential witness that is never reused for a different credential.
///
/// Writers that share Cairn's credential mutation lock advance `counter`
/// strictly, and keep it across sign-out, so no two committed credentials share
/// one. `incarnation` is random and chosen whenever a witness has to be created
/// from nothing, which is what keeps a witness deleted outside Cairn (a cleared
/// keychain entry) from restarting at a counter an old catalog still carries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialGeneration {
    pub incarnation: String,
    pub counter: u64,
}

/// Non-secret OAuth context projected by the credential store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogOAuthContext {
    pub issuer: Option<String>,
    pub resource: String,
    pub client_id: String,
    /// Granted scopes are encoded as a set, independent of order or duplicates.
    pub scopes: Vec<String>,
    pub generation: CredentialGeneration,
}

/// Compute a lowercase SHA-256 digest of the structured catalog binding.
///
/// This is encoding, not validation: callers must check the supported binding
/// version, stable credential witnesses, protocol and authorization context.
/// Persisted digests are only inspection copies and must be recomputed from the
/// binding at verification time. No keychain, environment or filesystem is read.
pub fn fingerprint_catalog_binding(binding: &CatalogBinding) -> String {
    // Exhaustive destructures keep newly added identity fields from silently
    // escaping the digest, including fields added to nested witness types.
    let CatalogBinding {
        binding_version,
        ownership_scope,
        project_id,
        owner,
        config_identity,
        credential_key,
        credential_context,
        protocol_version,
        method,
        authorization_revision,
    } = binding;
    let cairn_common::authorization::McpConfigFingerprint {
        algorithm,
        encoding_version,
        digest,
    } = config_identity;
    let CatalogCredentialContext {
        static_generation,
        oauth,
    } = credential_context;

    let mut encoder = CanonicalEncoder::new(CATALOG_BINDING_DOMAIN, CATALOG_BINDING_VERSION);
    encoder.field("binding_version", &binding_version.to_string());
    encoder.field("ownership_scope", ownership_scope);
    match project_id {
        None => encoder.field("project_id", "absent"),
        Some(id) => {
            encoder.field("project_id", "present");
            encoder.field("project_id.value", id);
        }
    }
    encoder.field("owner", owner);
    encoder.field("config.algorithm", algorithm);
    encoder.field("config.encoding_version", &encoding_version.to_string());
    encoder.field("config.digest", digest);
    encoder.field("credential_key", credential_key);
    match static_generation {
        None => encoder.field("credential.static", "absent"),
        Some(generation) => {
            encoder.field("credential.static", "present");
            encode_generation(&mut encoder, "credential.static", generation);
        }
    }
    match oauth {
        CatalogOAuthState::Never => encoder.field("credential.oauth", "never"),
        CatalogOAuthState::SignedOut(generation) => {
            encoder.field("credential.oauth", "signed-out");
            encode_generation(&mut encoder, "credential.oauth", generation);
        }
        CatalogOAuthState::SignedIn(oauth) => {
            let CatalogOAuthContext {
                issuer,
                resource,
                client_id,
                scopes,
                generation,
            } = oauth;
            encoder.field("credential.oauth", "present");
            match issuer {
                None => encoder.field("credential.oauth.issuer", "absent"),
                Some(issuer) => {
                    encoder.field("credential.oauth.issuer", "present");
                    encoder.field("credential.oauth.issuer.value", issuer);
                }
            }
            encoder.field("credential.oauth.resource", resource);
            encoder.field("credential.oauth.client_id", client_id);
            let scopes: Vec<String> = scopes
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            encoder.sequence("credential.oauth.scopes", &scopes);
            encode_generation(&mut encoder, "credential.oauth", generation);
        }
    }
    encoder.field("protocol_version", protocol_version);
    encoder.field("method", method);
    encoder.field(
        "authorization_revision",
        &authorization_revision.to_string(),
    );
    encoder.finish()
}

fn encode_generation(
    encoder: &mut CanonicalEncoder,
    prefix: &str,
    generation: &CredentialGeneration,
) {
    let CredentialGeneration {
        incarnation,
        counter,
    } = generation;
    encoder.field(&format!("{prefix}.incarnation"), incarnation);
    encoder.field(&format!("{prefix}.counter"), &counter.to_string());
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
    /// comes from. Supplying the resolver is the
    /// credential broker's job, and a second caller with its own closure is
    /// exactly the parallel resolution chain that left the settings-save
    /// connection test registering nothing.
    pub fn expand_vars(&self, resolve: &dyn Fn(&str) -> String) -> McpServerConfig {
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

    /// Whether `var` is declared to carry a credential. See [`Self::secrets`].
    pub fn declares_secret(&self, var: &str) -> bool {
        self.secrets.iter().any(|declared| declared == var)
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
    /// What to ask the user for, one entry per variable [`Self::not_ready`]
    /// names, described the way the providing pack described it.
    ///
    /// This is what makes the gate an action rather than a dead end: the
    /// verdict says a variable is unset, and this says what the value is, what
    /// kind of field takes it, and where to get it. Empty for a user-authored
    /// entry, which is never gated.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub setup: Vec<McpVarRequirement>,
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

#[cfg(test)]
mod catalog_binding_tests {
    use super::*;

    fn generation(incarnation: &str, counter: u64) -> CredentialGeneration {
        CredentialGeneration {
            incarnation: incarnation.into(),
            counter,
        }
    }

    fn binding() -> CatalogBinding {
        CatalogBinding {
            binding_version: CATALOG_BINDING_VERSION,
            ownership_scope: "project:/canonical/project".into(),
            project_id: Some("project-id".into()),
            owner: "pack:example".into(),
            config_identity: cairn_common::authorization::McpConfigFingerprint {
                algorithm: "sha256".into(),
                encoding_version: MCP_CONFIG_FINGERPRINT_ENCODING_VERSION,
                digest: "authored-config-digest".into(),
            },
            credential_key: "project:server".into(),
            credential_context: CatalogCredentialContext {
                static_generation: Some(generation("static-incarnation", 4)),
                oauth: CatalogOAuthState::SignedIn(CatalogOAuthContext {
                    issuer: Some("https://issuer.test".into()),
                    resource: "https://server.test/mcp".into(),
                    client_id: "client-id".into(),
                    scopes: vec!["read".into(), "write".into()],
                    generation: generation("oauth-incarnation", 8),
                }),
            },
            protocol_version: "2025-06-18".into(),
            method: "tools/list".into(),
            authorization_revision: 2,
        }
    }

    #[test]
    fn every_bound_dimension_changes_the_digest() {
        let original = fingerprint_catalog_binding(&binding());
        macro_rules! changes {
            ($field:ident $(.$nested:ident)*, $value:expr) => {{
                let mut changed = binding();
                changed.$field$(.$nested)* = $value;
                assert_ne!(
                    original,
                    fingerprint_catalog_binding(&changed),
                    stringify!($field$(.$nested)*)
                );
            }};
        }
        changes!(binding_version, CATALOG_BINDING_VERSION + 1);
        changes!(ownership_scope, "workspace".into());
        changes!(ownership_scope, "project:/other/project".into());
        changes!(project_id, Some("other-project".into()));
        changes!(project_id, None);
        changes!(owner, "user".into());
        changes!(owner, "pack:other".into());
        changes!(config_identity.algorithm, "other-algorithm".into());
        changes!(config_identity.encoding_version, 99);
        changes!(config_identity.digest, "other-config-digest".into());
        changes!(credential_key, "workspace:server".into());
        changes!(credential_context.static_generation, None);
        changes!(
            credential_context.static_generation,
            Some(generation("static-incarnation", 5))
        );
        changes!(
            credential_context.static_generation,
            Some(generation("static-incarnation", u64::MAX))
        );
        changes!(
            credential_context.static_generation,
            Some(generation("recreated-incarnation", 4))
        );
        changes!(credential_context.oauth, CatalogOAuthState::Never);
        changes!(
            credential_context.oauth,
            CatalogOAuthState::SignedOut(generation("oauth-incarnation", 8))
        );
        changes!(protocol_version, "2024-11-05".into());
        changes!(method, "resources/list".into());
        changes!(authorization_revision, 3);

        macro_rules! oauth_changes {
            ($field:ident, $value:expr) => {{
                let mut changed = binding();
                changed
                    .credential_context
                    .oauth
                    .signed_in_mut()
                    .unwrap()
                    .$field = $value;
                assert_ne!(
                    original,
                    fingerprint_catalog_binding(&changed),
                    stringify!($field)
                );
            }};
        }
        oauth_changes!(issuer, None);
        oauth_changes!(issuer, Some("https://other-issuer.test".into()));
        oauth_changes!(resource, "https://other-resource.test".into());
        oauth_changes!(client_id, "other-client".into());
        oauth_changes!(scopes, vec!["read".into()]);
        oauth_changes!(scopes, vec!["read".into(), "write".into(), "admin".into()]);
        oauth_changes!(generation, generation("oauth-incarnation", 9));
        oauth_changes!(generation, generation("oauth-incarnation", u64::MAX));
        oauth_changes!(generation, generation("recreated-incarnation", 8));
    }

    fn assert_distinct(first: &CatalogBinding, second: &CatalogBinding) {
        assert_ne!(
            fingerprint_catalog_binding(first),
            fingerprint_catalog_binding(second)
        );
    }

    #[test]
    fn optional_values_do_not_collapse_absence_into_empty_or_zero() {
        let mut absent = binding();
        absent.project_id = None;
        let mut empty = absent.clone();
        empty.project_id = Some(String::new());
        assert_distinct(&absent, &empty);

        absent = binding();
        absent.credential_context.static_generation = None;
        empty = absent.clone();
        empty.credential_context.static_generation = Some(generation("", 0));
        assert_distinct(&absent, &empty);

        absent = binding();
        absent
            .credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .issuer = None;
        empty = absent.clone();
        empty
            .credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .issuer = Some(String::new());
        assert_distinct(&absent, &empty);
    }

    #[test]
    fn credential_kinds_cannot_alias_even_with_equal_generations() {
        let mut none = binding();
        none.credential_context = CatalogCredentialContext {
            static_generation: None,
            oauth: CatalogOAuthState::Never,
        };
        // Same incarnation and counter on both kinds: only the kind may differ.
        let shared = generation("oauth-incarnation", 8);
        let mut static_only = none.clone();
        static_only.credential_context.static_generation = Some(shared.clone());
        let mut oauth_only = none.clone();
        oauth_only.credential_context.oauth = binding().credential_context.oauth;
        let mut both = oauth_only.clone();
        both.credential_context.static_generation = Some(shared);
        // Signed out at the same generation is neither signed in nor never.
        let mut signed_out = none.clone();
        signed_out.credential_context.oauth =
            CatalogOAuthState::SignedOut(generation("oauth-incarnation", 8));
        let digests: BTreeSet<_> = [none, static_only, oauth_only, both, signed_out]
            .iter()
            .map(fingerprint_catalog_binding)
            .collect();
        assert_eq!(digests.len(), 5);
    }

    #[test]
    fn encoding_is_deterministic_and_scope_sets_are_canonical() {
        let original = binding();
        let digest = fingerprint_catalog_binding(&original);
        assert_eq!(digest.len(), 64);
        assert!(digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
        for _ in 0..16 {
            let decoded: CatalogBinding =
                serde_json::from_str(&serde_json::to_string(&original).unwrap()).unwrap();
            assert_eq!(original, decoded);
            assert_eq!(digest, fingerprint_catalog_binding(&decoded));
        }
        let mut reordered = original;
        reordered
            .credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .scopes = vec!["write".into(), "read".into(), "read".into()];
        assert_eq!(digest, fingerprint_catalog_binding(&reordered));
    }

    #[test]
    fn field_and_sequence_boundaries_are_unambiguous() {
        let mut first = binding();
        first.ownership_scope = "a".into();
        first.owner = "bc".into();
        let mut second = first.clone();
        second.ownership_scope = "ab".into();
        second.owner = "c".into();
        assert_distinct(&first, &second);
        first
            .credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .scopes = vec!["readwrite".into()];
        second = first.clone();
        second
            .credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .scopes = vec!["read".into(), "write".into()];
        assert_distinct(&first, &second);
    }

    #[test]
    fn serialized_binding_rejects_unbound_or_secret_fields() {
        for path in [
            vec![],
            vec!["credential_context"],
            vec!["credential_context", "oauth", "signedIn"],
            vec!["credential_context", "oauth", "signedIn", "generation"],
            vec!["credential_context", "static_generation"],
            vec!["config_identity"],
        ] {
            let mut value = serde_json::to_value(binding()).unwrap();
            let mut object = &mut value;
            for key in path {
                object = &mut object[key];
            }
            object
                .as_object_mut()
                .unwrap()
                .insert("access_token".into(), "not-allowed".into());
            assert!(serde_json::from_value::<CatalogBinding>(value).is_err());
        }
        let mut value = serde_json::to_value(binding()).unwrap();
        value["cacheScope"] = "public".into();
        assert!(serde_json::from_value::<CatalogBinding>(value).is_err());
    }
}
