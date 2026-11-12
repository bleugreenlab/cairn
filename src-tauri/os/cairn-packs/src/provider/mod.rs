//! Web-service providers: a credential the operator fills, and a provider bound
//! to one slot of the app's closed slot set.
//!
//! A pack ships `credentials/<name>.yaml` and `providers/<slot>.yaml`. The app
//! owns the slot set ([`Slot`]) and each slot's result contract, plus one
//! generic declarative HTTP engine ([`http`]); the provider itself — its
//! endpoint, auth header, body, result paths and options — is data in the pack.
//! Adding a provider is therefore a change to a pack, never to the app.
//!
//! Both kinds are installed into the pack's own layer (`packs/<id>/providers/`,
//! `packs/<id>/credentials/`), not the flat workspace, because every provider
//! pack ships the same file names: the pack id is their namespace, the way it
//! already is for `packs/<id>/mcp.yaml`. The installed layer has the same
//! relative layout as the pack source, so [`load_pack_provider`] reads either.
//!
//! A provider has one of two adapter forms. An `http` adapter is a declared
//! request and result mapping run by [`http`], sending the pack credential it
//! names. An `mcp` adapter ([`binding`]) calls a tool on an MCP server the same
//! pack declares; that server's own connection authenticates it, so it names no
//! credential.
//!
//! A credential is a `secret` the operator pastes, or an `oauth` access token
//! the operator obtains by authorizing a client the pack declares. Either way
//! only the hosts it lists may receive it.
//!
//! Every declaration is validated when it is loaded, at install and at use:
//! the slot must be known and match the file stem, the credential must exist
//! and name the provider as an accessor, the request may only send the
//! credential to a host the credential lists, an MCP binding may only name a
//! server in its own pack, and every interpolation must be one the slot
//! defines. Forms that are reserved for later (`run`/`tool:` accessors, the
//! `tool` adapter, an `http` adapter for `pdf`) parse and are refused as not supported yet,
//! so adopting them is not a schema change.

pub mod binding;
pub mod http;

use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::mcp::{McpVarKind, McpVarSetup};
use crate::provider_options::{Choice, OptionControl, ProviderOption};

/// The app's OAuth relay. An authorization server redirects to
/// `{OAUTH_RELAY_PREFIX}<relay>{OAUTH_RELAY_SUFFIX}`, and that page forwards the
/// response to the desktop as `cairn://auth-callback?provider=<relay>&…`.
pub const OAUTH_RELAY_PREFIX: &str = "https://cairn.computer/oauth/";
pub const OAUTH_RELAY_SUFFIX: &str = "/callback";

/// Pack-relative directory of provider declarations.
pub const PROVIDERS_DIR: &str = "providers";
/// Pack-relative directory of credential declarations.
pub const CREDENTIALS_DIR: &str = "credentials";
/// The words every reserved form is refused with.
pub const NOT_SUPPORTED_YET: &str = "not supported yet";
/// The keychain owner segment reserved for operator-owned credentials that
/// belong to no pack, so no pack id may take it.
pub const RESERVED_CREDENTIAL_OWNER: &str = "workspace";

/// The closed set of slots a provider can fill. Each is a built-in verb with a
/// fixed result contract, owned by the app.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Slot {
    /// `cairn://websearch?q=`: a ranked list of title, url, snippet and score.
    WebSearch,
    /// `read http(s)://`: markdown.
    WebFetch,
    /// `read *.pdf`: markdown.
    Pdf,
}

impl Slot {
    pub const ALL: [Slot; 3] = [Slot::WebSearch, Slot::WebFetch, Slot::Pdf];

    /// The `provides:` value and the provider's file stem.
    pub fn as_str(self) -> &'static str {
        match self {
            Slot::WebSearch => "web-search",
            Slot::WebFetch => "web-fetch",
            Slot::Pdf => "pdf",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Slot::ALL.into_iter().find(|slot| slot.as_str() == value)
    }

    /// The slot's name mid-sentence: "the <label> web-search provider".
    pub fn noun(self) -> &'static str {
        match self {
            Slot::WebSearch => "web-search",
            Slot::WebFetch => "web-fetch",
            Slot::Pdf => "PDF",
        }
    }
}

/// A credential a pack declares and the operator fills.
///
/// Carries only where the value may go and who may use it; the value lives in
/// the keychain and never in a declaration, a catalog, or a read path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialDecl {
    /// The file stem: `api-key`.
    pub name: String,
    pub label: String,
    #[serde(flatten)]
    pub kind: CredentialKind,
    /// The only hosts this value may be sent to.
    pub hosts: Vec<String>,
    /// The pack items that may use the value. Only providers in the same pack
    /// are implemented.
    pub accessors: Vec<Slot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<McpVarSetup>,
}

/// How a credential's value comes to exist.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CredentialKind {
    /// A value the operator pastes, stored under `var`: the env-style name it
    /// is stored and, later, injected under.
    Secret { var: String },
    /// An access token the operator obtains by authorizing the declared client.
    /// `var` is the name it would be injected under by a later `run` accessor.
    #[serde(rename = "oauth")]
    OAuth {
        #[serde(skip_serializing_if = "Option::is_none")]
        var: Option<String>,
        oauth: OAuthDecl,
    },
}

/// A pre-registered OAuth client, its endpoints and the scopes it asks for.
/// Run by the generic PKCE and token-store code; nothing here is discovered.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OAuthDecl {
    pub authorize: String,
    pub token: String,
    pub client_id: String,
    pub scopes: Vec<String>,
    pub redirect: OAuthRedirect,
}

/// Where the authorization server sends the operator back to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum OAuthRedirect {
    /// The app relay page for this segment, which deep-links into the desktop.
    Relay(String),
    /// A one-shot listener on `127.0.0.1`.
    Loopback,
}

impl OAuthRedirect {
    /// The relay segment, for a relay redirect.
    pub fn relay(&self) -> Option<&str> {
        match self {
            OAuthRedirect::Relay(relay) => Some(relay),
            OAuthRedirect::Loopback => None,
        }
    }

    /// The redirect URI a relay redirect sends to the authorization server.
    pub fn relay_uri(&self) -> Option<String> {
        self.relay()
            .map(|relay| format!("{OAUTH_RELAY_PREFIX}{relay}{OAUTH_RELAY_SUFFIX}"))
    }
}

impl CredentialDecl {
    /// The env-style name the value is stored or injected under, if any.
    pub fn var(&self) -> Option<&str> {
        match &self.kind {
            CredentialKind::Secret { var } => Some(var),
            CredentialKind::OAuth { var, .. } => var.as_deref(),
        }
    }

    /// The declared OAuth client, for an `oauth` credential.
    pub fn oauth(&self) -> Option<&OAuthDecl> {
        match &self.kind {
            CredentialKind::Secret { .. } => None,
            CredentialKind::OAuth { oauth, .. } => Some(oauth),
        }
    }
}

/// A provider bound to one slot.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderDecl {
    pub slot: Slot,
    pub label: String,
    /// The credential's name in the same pack. An `mcp` adapter has none.
    pub credential: Option<String>,
    pub options: Vec<ProviderOption>,
    pub adapter: Adapter,
}

/// How a provider reaches its service.
#[derive(Debug, Clone, PartialEq)]
pub enum Adapter {
    /// A declared HTTP request and result mapping.
    Http(http::HttpAdapter),
    /// A tool on an MCP server the same pack declares.
    Mcp(binding::McpBinding),
}

impl ProviderDecl {
    /// The HTTP adapter, for an `http` provider.
    pub fn http(&self) -> Option<&http::HttpAdapter> {
        match &self.adapter {
            Adapter::Http(adapter) => Some(adapter),
            Adapter::Mcp(_) => None,
        }
    }
}

/// One provider with the credential it reads, both from one pack.
#[derive(Debug, Clone, PartialEq)]
pub struct PackProvider {
    pub pack: String,
    pub provider: ProviderDecl,
    /// `None` for an `mcp` adapter, which authenticates through its server.
    pub credential: Option<CredentialDecl>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    label: String,
    kind: String,
    #[serde(default)]
    var: Option<String>,
    hosts: Vec<String>,
    accessors: Vec<String>,
    #[serde(default)]
    setup: Option<McpVarSetup>,
    #[serde(default)]
    oauth: Option<OAuthFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct OAuthFile {
    authorize: String,
    token: String,
    client_id: String,
    scopes: Vec<String>,
    redirect: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderFile {
    label: String,
    provides: String,
    #[serde(default)]
    credential: Option<String>,
    #[serde(default)]
    options: Vec<OptionFile>,
    adapter: serde_yaml::Mapping,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionFile {
    key: String,
    label: String,
    #[serde(default)]
    select: Option<Vec<(String, String)>>,
    #[serde(default)]
    number: Option<NumberFile>,
    #[serde(default)]
    text: Option<TextFile>,
    #[serde(default, rename = "bool")]
    boolean: Option<serde_yaml::Value>,
    #[serde(default)]
    default: Option<serde_yaml::Value>,
    #[serde(default)]
    required: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NumberFile {
    min: f64,
    max: f64,
    #[serde(default)]
    integer: bool,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct TextFile {
    #[serde(default)]
    placeholder: String,
}

fn file_stem(path: &Path) -> Option<String> {
    (path.extension().and_then(|e| e.to_str()) == Some("yaml"))
        .then(|| {
            path.file_stem()
                .and_then(|s| s.to_str())
                .map(str::to_string)
        })
        .flatten()
}

fn is_env_name(var: &str) -> bool {
    let mut chars = var.chars();
    chars.next().is_some_and(|c| c.is_ascii_uppercase())
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

fn is_host(host: &str) -> bool {
    !host.is_empty()
        && host == host.to_ascii_lowercase()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
        && !host.starts_with('.')
        && !host.ends_with('.')
}

/// An OAuth endpoint: `https://` with a literal lowercase host.
fn oauth_endpoint(field: &str, url: &str) -> Result<String, String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| format!("`oauth.{field}` must start with `https://`, not `{url}`"))?;
    let host = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    if url.contains("${") || !is_host(host) {
        return Err(format!(
            "`oauth.{field}` must name a literal lowercase host, not `{url}`"
        ));
    }
    Ok(url.to_string())
}

fn parse_oauth(file: OAuthFile) -> Result<OAuthDecl, String> {
    let authorize = oauth_endpoint("authorize", &file.authorize)?;
    let token = oauth_endpoint("token", &file.token)?;
    let client_id = file.client_id.trim();
    if client_id.is_empty() || client_id.contains("${") {
        return Err("`oauth.clientId` must be a literal, pre-registered client id".to_string());
    }
    if file.scopes.is_empty() {
        return Err("`oauth.scopes` must name at least one scope".to_string());
    }
    if let Some(bad) = file
        .scopes
        .iter()
        .find(|scope| scope.is_empty() || scope.contains(char::is_whitespace))
    {
        return Err(format!("`oauth.scopes` entry `{bad}` is not one scope"));
    }
    let redirect = if file.redirect == "loopback" {
        OAuthRedirect::Loopback
    } else {
        let relay = file
            .redirect
            .strip_prefix(OAUTH_RELAY_PREFIX)
            .and_then(|rest| rest.strip_suffix(OAUTH_RELAY_SUFFIX))
            .filter(|relay| {
                !relay.is_empty()
                    && relay
                        .chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            })
            .ok_or_else(|| {
                format!(
                    "`oauth.redirect` must be `loopback` or \
                     `{OAUTH_RELAY_PREFIX}<relay>{OAUTH_RELAY_SUFFIX}`, not `{}`",
                    file.redirect
                )
            })?;
        OAuthRedirect::Relay(relay.to_string())
    };
    Ok(OAuthDecl {
        authorize,
        token,
        client_id: client_id.to_string(),
        scopes: file.scopes,
        redirect,
    })
}

/// Parse and validate one credential declaration. `name` is its file stem.
pub fn parse_credential(name: &str, text: &str) -> Result<CredentialDecl, String> {
    let file: CredentialFile = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    if let Some(var) = file.var.as_deref().filter(|var| !is_env_name(var)) {
        return Err(format!(
            "`var` must be an env-style name such as `API_KEY`, not `{var}`"
        ));
    }
    let kind = match file.kind.as_str() {
        "secret" => {
            if file.oauth.is_some() {
                return Err("a `secret` credential takes no `oauth` block".to_string());
            }
            CredentialKind::Secret {
                var: file
                    .var
                    .ok_or_else(|| "a `secret` credential needs `var`".to_string())?,
            }
        }
        "oauth" => {
            let oauth = file.oauth.ok_or_else(|| {
                "an `oauth` credential needs an `oauth` block \
                 (authorize, token, clientId, scopes, redirect)"
                    .to_string()
            })?;
            CredentialKind::OAuth {
                var: file.var,
                oauth: parse_oauth(oauth)?,
            }
        }
        other => return Err(format!("unknown credential kind `{other}` (secret, oauth)")),
    };
    if file.hosts.is_empty() {
        return Err("`hosts` must name at least one host".to_string());
    }
    if let Some(bad) = file.hosts.iter().find(|host| !is_host(host)) {
        return Err(format!(
            "`hosts` entry `{bad}` is not a lowercase host name (no scheme, port, path or wildcard)"
        ));
    }
    let mut accessors = Vec::new();
    for accessor in &file.accessors {
        match accessor.split_once(':') {
            Some(("provider", slot)) => match Slot::parse(slot) {
                Some(slot) => accessors.push(slot),
                None => return Err(format!("accessor `{accessor}` names an unknown slot")),
            },
            Some(("tool", _)) => {
                return Err(format!("accessor `{accessor}` is {NOT_SUPPORTED_YET}"))
            }
            None if accessor == "run" => {
                return Err(format!("accessor `run` is {NOT_SUPPORTED_YET}"))
            }
            _ => return Err(format!("unknown accessor `{accessor}` (provider:<slot>)")),
        }
    }
    if accessors.is_empty() {
        return Err("`accessors` must name at least one provider".to_string());
    }
    if let Some(setup) = &file.setup {
        if setup.kind != McpVarKind::Secret {
            return Err("a credential's `setup.kind` can only be `secret`".to_string());
        }
    }
    Ok(CredentialDecl {
        name: name.to_string(),
        label: file.label,
        kind,
        hosts: file.hosts,
        accessors,
        setup: file.setup,
    })
}

fn parse_option(option: OptionFile) -> Result<ProviderOption, String> {
    let key = option.key;
    if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!("option key `{key}` must be letters, digits or `_`"));
    }
    let forms = [
        option.select.is_some(),
        option.number.is_some(),
        option.text.is_some(),
        option.boolean.is_some(),
    ];
    if forms.iter().filter(|form| **form).count() != 1 {
        return Err(format!(
            "option `{key}` must declare exactly one of `select`, `number`, `text`, `bool`"
        ));
    }
    if option.required && option.text.is_none() {
        return Err(format!(
            "option `{key}`: only a `text` option can be `required`"
        ));
    }
    let default = option.default;
    let control = if let Some(choices) = option.select {
        if choices.is_empty() {
            return Err(format!("option `{key}` has no choices"));
        }
        let default = match default.as_ref() {
            Some(value) => value
                .as_str()
                .ok_or_else(|| format!("option `{key}`'s default must be one of its choices"))?
                .to_string(),
            None => choices[0].0.clone(),
        };
        if !choices.iter().any(|(value, _)| *value == default) {
            return Err(format!(
                "option `{key}`'s default `{default}` is not one of its choices"
            ));
        }
        OptionControl::Select {
            choices: choices
                .into_iter()
                .map(|(value, label)| Choice { value, label })
                .collect(),
            default,
        }
    } else if let Some(number) = option.number {
        let default = default
            .as_ref()
            .and_then(serde_yaml::Value::as_f64)
            .ok_or_else(|| format!("option `{key}` needs a numeric `default`"))?;
        if number.min > number.max || default < number.min || default > number.max {
            return Err(format!(
                "option `{key}`'s default {default} is outside {}..={}",
                number.min, number.max
            ));
        }
        if number.integer
            && [number.min, number.max, default]
                .iter()
                .any(|n| n.fract() != 0.0)
        {
            return Err(format!(
                "option `{key}` is an integer but its range or default is not"
            ));
        }
        OptionControl::Number {
            min: number.min,
            max: number.max,
            default,
            integer: number.integer,
        }
    } else if let Some(text) = option.text {
        let default = match default.as_ref() {
            Some(value) => value
                .as_str()
                .ok_or_else(|| format!("option `{key}`'s default must be text"))?
                .to_string(),
            None => String::new(),
        };
        OptionControl::Text {
            default,
            placeholder: text.placeholder,
        }
    } else {
        let default = default
            .as_ref()
            .and_then(serde_yaml::Value::as_bool)
            .ok_or_else(|| format!("option `{key}` needs a boolean `default`"))?;
        OptionControl::Bool { default }
    };
    Ok(ProviderOption {
        key,
        label: option.label,
        control,
        required: option.required,
    })
}

/// The credential an `http` adapter names, which must exist in the pack and
/// list the provider's slot as an accessor.
fn named_credential<'a>(
    slot: Slot,
    name: Option<&str>,
    credentials: &'a [CredentialDecl],
) -> Result<&'a CredentialDecl, String> {
    let name = name.ok_or_else(|| "an `http` adapter needs `credential:`".to_string())?;
    let credential = credentials
        .iter()
        .find(|credential| credential.name == name)
        .ok_or_else(|| {
            format!("credential `{name}` is not declared in this pack's `credentials/`")
        })?;
    if !credential.accessors.contains(&slot) {
        return Err(format!(
            "credential `{}` does not list `provider:{}` in its `accessors`",
            credential.name,
            slot.as_str()
        ));
    }
    Ok(credential)
}

fn parse_adapter(
    slot: Slot,
    options: &[ProviderOption],
    credential: Option<&str>,
    credentials: &[CredentialDecl],
    servers: &BTreeSet<String>,
    adapter: serde_yaml::Mapping,
) -> Result<Adapter, String> {
    if adapter.len() != 1 {
        return Err("`adapter` must name exactly one form (http, mcp)".to_string());
    }
    let (form, body) = adapter.into_iter().next().expect("one entry");
    match form.as_str() {
        // A PDF is extracted by a tool that takes the URL; the app has no
        // path that would run a declared HTTP request for the slot, so a
        // declaration of one would install, show ready, and never run.
        Some("http") if slot == Slot::Pdf => Err(format!(
            "adapter `http` for `pdf` is {NOT_SUPPORTED_YET}: it serves the `mcp` form"
        )),
        Some("http") => {
            let credential = named_credential(slot, credential, credentials)?;
            http::parse_adapter(slot, options, credential, body).map(Adapter::Http)
        }
        Some("mcp") => {
            if credential.is_some() {
                return Err(
                    "an `mcp` adapter takes no `credential:`; its MCP server's own connection \
                     authenticates it"
                        .to_string(),
                );
            }
            binding::parse(slot, options, servers, body).map(Adapter::Mcp)
        }
        Some(form @ "tool") => Err(format!("adapter `{form}` is {NOT_SUPPORTED_YET}")),
        _ => Err(format!("unknown adapter form {form:?} (http, mcp)")),
    }
}

/// The names of the MCP servers a pack root declares in its `mcp.yaml`.
pub fn pack_mcp_server_names(root: &Path) -> Result<BTreeSet<String>, String> {
    let path = root.join(crate::manifest::PACK_MCP_FILE);
    if !path.is_file() {
        return Ok(BTreeSet::new());
    }
    Ok(crate::mcp::parse_pack_mcp_file(&path)?
        .servers
        .into_keys()
        .collect())
}

/// Parse and validate one provider declaration against its pack's credentials
/// and MCP servers. `stem` is the file stem, which must equal the slot.
pub fn parse_provider(
    stem: &str,
    text: &str,
    credentials: &[CredentialDecl],
    servers: &BTreeSet<String>,
) -> Result<ProviderDecl, String> {
    let file: ProviderFile = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    let slot = Slot::parse(&file.provides).ok_or_else(|| {
        format!(
            "`provides: {}` is not a slot (web-search, web-fetch, pdf)",
            file.provides
        )
    })?;
    if slot.as_str() != stem {
        return Err(format!(
            "`provides: {}` must match the file name `{stem}.yaml`",
            slot.as_str()
        ));
    }
    let mut keys = BTreeSet::new();
    let mut options = Vec::new();
    for option in file.options {
        let option = parse_option(option)?;
        if !keys.insert(option.key.clone()) {
            return Err(format!("option `{}` is declared twice", option.key));
        }
        options.push(option);
    }
    let adapter = parse_adapter(
        slot,
        &options,
        file.credential.as_deref(),
        credentials,
        servers,
        file.adapter,
    )?;
    Ok(ProviderDecl {
        slot,
        label: file.label,
        credential: file.credential,
        options,
        adapter,
    })
}

/// Every credential a pack root declares, validated.
pub fn load_pack_credentials(root: &Path, pack_id: &str) -> Result<Vec<CredentialDecl>, String> {
    let mut paths: Vec<_> = match std::fs::read_dir(root.join(CREDENTIALS_DIR)) {
        Ok(entries) => entries.filter_map(|e| e.ok()).map(|e| e.path()).collect(),
        Err(_) => return Ok(Vec::new()),
    };
    paths.sort();
    let mut credentials = Vec::new();
    for path in paths {
        let Some(name) = file_stem(&path) else {
            continue;
        };
        let text = std::fs::read_to_string(&path)
            .map_err(|e| format!("credential `{pack_id}/{name}`: cannot read {path:?}: {e}"))?;
        credentials.push(
            parse_credential(&name, &text)
                .map_err(|e| format!("credential `{pack_id}/{name}`: {e}"))?,
        );
    }
    let mut relays = BTreeSet::new();
    for credential in &credentials {
        if let Some(relay) = credential.oauth().and_then(|oauth| oauth.redirect.relay()) {
            if !relays.insert(relay) {
                return Err(format!(
                    "credential `{pack_id}/{}`: the OAuth relay `{relay}` is claimed by another \
                     credential in this pack",
                    credential.name
                ));
            }
        }
    }
    if !credentials.is_empty() && pack_id == RESERVED_CREDENTIAL_OWNER {
        return Err(format!(
            "pack id `{RESERVED_CREDENTIAL_OWNER}` is reserved for credentials that belong to no pack"
        ));
    }
    Ok(credentials)
}

/// Every installed pack credential that claims the OAuth relay `relay`, with
/// its pack id. Read from each pack's installed layer.
pub fn relay_holders(config_dir: &Path, relay: &str) -> Vec<(String, CredentialDecl)> {
    crate::lock::installed_packs(config_dir)
        .into_iter()
        .flat_map(|lock| {
            let root = crate::lock::pack_dir(config_dir, &lock.id);
            load_pack_credentials(&root, &lock.id)
                .unwrap_or_default()
                .into_iter()
                .filter(|credential| {
                    credential.oauth().and_then(|oauth| oauth.redirect.relay()) == Some(relay)
                })
                .map(move |credential| (lock.id.clone(), credential))
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Every provider a pack root declares, each with its credential, validated.
/// `root` is a pack source or an installed `packs/<id>/` layer.
pub fn load_pack_providers(root: &Path, pack_id: &str) -> Result<Vec<PackProvider>, String> {
    let credentials = load_pack_credentials(root, pack_id)?;
    let servers = pack_mcp_server_names(root)?;
    let mut out = Vec::new();
    for slot in Slot::ALL {
        if let Some(provider) = load_one(root, pack_id, slot, &credentials, &servers)? {
            out.push(provider);
        }
    }
    let mut stray: Vec<String> = match std::fs::read_dir(root.join(PROVIDERS_DIR)) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter_map(|e| file_stem(&e.path()))
            .filter(|stem| Slot::parse(stem).is_none())
            .collect(),
        Err(_) => Vec::new(),
    };
    stray.sort();
    if let Some(stem) = stray.first() {
        return Err(format!(
            "provider `{pack_id}/{stem}`: `{stem}.yaml` is not a slot (web-search, web-fetch, pdf)"
        ));
    }
    Ok(out)
}

/// The provider one pack declares for `slot`, with its credential, if any.
pub fn load_pack_provider(
    root: &Path,
    pack_id: &str,
    slot: Slot,
) -> Result<Option<PackProvider>, String> {
    let credentials = load_pack_credentials(root, pack_id)?;
    load_one(
        root,
        pack_id,
        slot,
        &credentials,
        &pack_mcp_server_names(root)?,
    )
}

fn load_one(
    root: &Path,
    pack_id: &str,
    slot: Slot,
    credentials: &[CredentialDecl],
    servers: &BTreeSet<String>,
) -> Result<Option<PackProvider>, String> {
    let path = root
        .join(PROVIDERS_DIR)
        .join(format!("{}.yaml", slot.as_str()));
    if !path.is_file() {
        return Ok(None);
    }
    let label = format!("provider `{pack_id}/{}`", slot.as_str());
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("{label}: cannot read {path:?}: {e}"))?;
    let provider = parse_provider(slot.as_str(), &text, credentials, servers)
        .map_err(|e| format!("{label}: {e}"))?;
    let credential = provider.credential.as_ref().map(|name| {
        credentials
            .iter()
            .find(|credential| &credential.name == name)
            .cloned()
            .expect("parse_provider resolved the credential")
    });
    Ok(Some(PackProvider {
        pack: pack_id.to_string(),
        provider,
        credential,
    }))
}

#[cfg(test)]
mod tests;
