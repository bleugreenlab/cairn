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
//! Every declaration is validated when it is loaded, at install and at use:
//! the slot must be known and match the file stem, the credential must exist
//! and name the provider as an accessor, the request may only send the
//! credential to a host the credential lists, and every interpolation must be
//! one the slot defines. Forms that are reserved for later (`oauth`
//! credentials, `run`/`tool:` accessors, `mcp`/`tool` adapters, the `pdf`
//! slot) parse and are refused as not supported yet, so adopting them is not
//! a schema change.

pub mod http;

use std::collections::BTreeSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::mcp::{McpVarKind, McpVarSetup};
use crate::provider_options::{Choice, OptionControl, ProviderOption};

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
    /// The env-style name the value is stored and, later, injected under.
    pub var: String,
    /// The only hosts this value may be sent to.
    pub hosts: Vec<String>,
    /// The pack items that may use the value. Only providers in the same pack
    /// are implemented.
    pub accessors: Vec<Slot>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub setup: Option<McpVarSetup>,
}

/// A provider bound to one slot.
#[derive(Debug, Clone, PartialEq)]
pub struct ProviderDecl {
    pub slot: Slot,
    pub label: String,
    /// The credential's name in the same pack.
    pub credential: String,
    pub options: Vec<ProviderOption>,
    pub adapter: http::HttpAdapter,
}

/// One provider with the credential it reads, both from one pack.
#[derive(Debug, Clone, PartialEq)]
pub struct PackProvider {
    pub pack: String,
    pub provider: ProviderDecl,
    pub credential: CredentialDecl,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    label: String,
    kind: String,
    var: String,
    hosts: Vec<String>,
    accessors: Vec<String>,
    #[serde(default)]
    setup: Option<McpVarSetup>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderFile {
    label: String,
    provides: String,
    credential: String,
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

/// Parse and validate one credential declaration. `name` is its file stem.
pub fn parse_credential(name: &str, text: &str) -> Result<CredentialDecl, String> {
    let file: CredentialFile = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    match file.kind.as_str() {
        "secret" => {}
        "oauth" => return Err(format!("credential kind `oauth` is {NOT_SUPPORTED_YET}")),
        other => return Err(format!("unknown credential kind `{other}` (secret)")),
    }
    if !is_env_name(&file.var) {
        return Err(format!(
            "`var` must be an env-style name such as `API_KEY`, not `{}`",
            file.var
        ));
    }
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
        var: file.var,
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
    })
}

fn parse_adapter(
    slot: Slot,
    options: &[ProviderOption],
    credential: &CredentialDecl,
    adapter: serde_yaml::Mapping,
) -> Result<http::HttpAdapter, String> {
    if adapter.len() != 1 {
        return Err("`adapter` must name exactly one form (http)".to_string());
    }
    let (form, body) = adapter.into_iter().next().expect("one entry");
    match form.as_str() {
        Some("http") => http::parse_adapter(slot, options, credential, body),
        Some(form @ ("mcp" | "tool")) => Err(format!("adapter `{form}` is {NOT_SUPPORTED_YET}")),
        _ => Err(format!("unknown adapter form {form:?} (http)")),
    }
}

/// Parse and validate one provider declaration against its credential.
/// `stem` is the file stem, which must equal the slot.
pub fn parse_provider(
    stem: &str,
    text: &str,
    credentials: &[CredentialDecl],
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
    if slot == Slot::Pdf {
        return Err(format!("a `pdf` provider is {NOT_SUPPORTED_YET}"));
    }
    let credential = credentials
        .iter()
        .find(|credential| credential.name == file.credential)
        .ok_or_else(|| {
            format!(
                "credential `{}` is not declared in this pack's `credentials/`",
                file.credential
            )
        })?;
    if !credential.accessors.contains(&slot) {
        return Err(format!(
            "credential `{}` does not list `provider:{}` in its `accessors`",
            credential.name,
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
    let adapter = parse_adapter(slot, &options, credential, file.adapter)?;
    Ok(ProviderDecl {
        slot,
        label: file.label,
        credential: credential.name.clone(),
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
    if !credentials.is_empty() && pack_id == RESERVED_CREDENTIAL_OWNER {
        return Err(format!(
            "pack id `{RESERVED_CREDENTIAL_OWNER}` is reserved for credentials that belong to no pack"
        ));
    }
    Ok(credentials)
}

/// Every provider a pack root declares, each with its credential, validated.
/// `root` is a pack source or an installed `packs/<id>/` layer.
pub fn load_pack_providers(root: &Path, pack_id: &str) -> Result<Vec<PackProvider>, String> {
    let credentials = load_pack_credentials(root, pack_id)?;
    let mut out = Vec::new();
    for slot in Slot::ALL {
        if let Some(provider) = load_one(root, pack_id, slot, &credentials)? {
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
    load_one(root, pack_id, slot, &credentials)
}

fn load_one(
    root: &Path,
    pack_id: &str,
    slot: Slot,
    credentials: &[CredentialDecl],
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
    let provider =
        parse_provider(slot.as_str(), &text, credentials).map_err(|e| format!("{label}: {e}"))?;
    let credential = credentials
        .iter()
        .find(|credential| credential.name == provider.credential)
        .cloned()
        .expect("parse_provider resolved the credential");
    Ok(Some(PackProvider {
        pack: pack_id.to_string(),
        provider,
        credential,
    }))
}

#[cfg(test)]
mod tests;
