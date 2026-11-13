//! The web-service slots, backed by providers that packs declare.
//!
//! The app compiles in the slot set and each slot's result contract
//! ([`cairn_packs::provider::Slot`]), the generic adapter engines (the
//! declarative HTTP engine in [`cairn_packs::provider::http`] and the MCP-tool
//! binding in [`cairn_packs::provider::binding`]), the generic OAuth flow for a
//! declared client ([`oauth`]), and the zero-config defaults (`regular` fetch,
//! `local` PDF). Everything about a particular provider — endpoint, headers,
//! body, result paths, options, the hosts its credential may reach, the MCP
//! tool it calls, the OAuth client it authorizes — is data in an installed
//! pack. Adding a provider is a change to a pack, never here.
//!
//! Which provider fills a slot is workspace state: the slot's scalar in
//! `settings.yaml` (`activeWebSearch`, `activeWebFetch`) names a pack id, and a
//! pack provides at most one provider per slot (its `providers/<slot>.yaml`).
//! Options live under the slot's map keyed by pack id (`webSearch.<pack>`).
//! Installing a provider pack never activates it.
//!
//! A provider failure (a non-2xx response, a refused tool call) is an `Err` in
//! every slot, so an agent reads it as a failed read. A configuration gap
//! (nothing selected, not installed, no key, not authorized, needs setup) is
//! `Ok` guidance naming where to fix it.
//!
//! An option whose choices come from the provider's credential is filled from
//! that credential where it can be, at connect and at use ([`choices`]).
//! [`readiness`] never makes that request: it stays a synchronous read of what
//! is stored.

pub mod choices;
mod migrate;
pub mod oauth;

use std::collections::HashMap;
use std::path::Path;

use cairn_packs::provider::binding::McpBinding;
use cairn_packs::provider::http::{Inputs, PreparedRequest};
use cairn_packs::provider::{self, Adapter, CredentialDecl, CredentialKind, PackProvider, Slot};
use cairn_packs::server_config::NotReady;

use crate::security::BrokeredSecret;

pub use migrate::{migrate_workspace, MigrationReport};

/// Where the operator installs, selects and keys a web-service provider. Every
/// agent-facing message that says where to configure one uses this, so the
/// install surface's final name is one edit.
pub const LIBRARY_LOCATION: &str = "the Library";

/// How long a slot waits on an MCP-bound provider's tool call.
const MCP_SLOT_TIMEOUT_MS: u32 = 120_000;

type Options = HashMap<String, serde_yaml::Value>;

/// The `settings.yaml` scalar naming the slot's active provider.
pub fn scalar_key(slot: Slot) -> &'static str {
    match slot {
        Slot::WebSearch => "activeWebSearch",
        Slot::WebFetch => "activeWebFetch",
        Slot::Pdf => "activePdf",
    }
}

/// The `settings.yaml` map holding each provider's options, keyed by pack id.
pub fn options_key(slot: Slot) -> &'static str {
    match slot {
        Slot::WebSearch => "webSearch",
        Slot::WebFetch => "webFetch",
        Slot::Pdf => "pdf",
    }
}

/// The compiled zero-config provider a slot falls back to, which a scalar may
/// name explicitly. Web search has none.
pub fn builtin_default(slot: Slot) -> Option<&'static str> {
    match slot {
        Slot::WebSearch => None,
        Slot::WebFetch => Some("regular"),
        Slot::Pdf => Some("local"),
    }
}

/// The built-in default's display label.
fn builtin_label(slot: Slot) -> Option<&'static str> {
    match slot {
        Slot::WebSearch => None,
        Slot::WebFetch => Some("Regular"),
        Slot::Pdf => Some("Local"),
    }
}

/// The slot's selected provider name, trimmed, if one is set.
pub fn active_name(config_dir: &Path, slot: Slot) -> Option<String> {
    let file = crate::config::settings::load_settings_file(config_dir).ok()?;
    let name = match slot {
        Slot::WebSearch => file.active_web_search,
        Slot::WebFetch => file.active_web_fetch,
        Slot::Pdf => file.active_pdf,
    }?;
    let name = name.trim().to_string();
    (!name.is_empty()).then_some(name)
}

/// The options stored for one pack's provider in a slot (empty when none).
pub fn load_options(config_dir: &Path, slot: Slot, pack: &str) -> Options {
    let Ok(file) = crate::config::settings::load_settings_file(config_dir) else {
        return Options::new();
    };
    match slot {
        Slot::WebSearch => file.web_search,
        Slot::WebFetch => file.web_fetch,
        Slot::Pdf => file.pdf,
    }
    .and_then(|mut map| map.remove(pack))
    .unwrap_or_default()
}

/// The provider an installed pack declares for `slot`, read from the pack's
/// installed layer (what the operator approved), not its source.
pub fn installed_provider(
    config_dir: &Path,
    pack: &str,
    slot: Slot,
) -> Result<Option<PackProvider>, String> {
    if cairn_packs::lock::read_lock(config_dir, pack).is_none() {
        return Ok(None);
    }
    provider::load_pack_provider(&cairn_packs::lock::pack_dir(config_dir, pack), pack, slot)
}

/// Every installed pack's provider for `slot`, in pack-id order. A pack whose
/// declaration does not load is reported beside the ones that do.
pub fn installed_providers(
    config_dir: &Path,
    slot: Slot,
) -> Vec<Result<PackProvider, (String, String)>> {
    cairn_packs::installed_packs(config_dir)
        .into_iter()
        .filter(|lock| {
            lock.items.iter().any(|item| {
                item.kind == cairn_packs::PackItemKind::Provider && item.id == slot.as_str()
            })
        })
        .filter_map(
            |lock| match installed_provider(config_dir, &lock.id, slot) {
                Ok(Some(provider)) => Some(Ok(provider)),
                Ok(None) => None,
                Err(error) => Some(Err((lock.id, error))),
            },
        )
        .collect()
}

/// What backs a slot right now.
#[derive(Debug)]
pub enum Resolved {
    /// Nothing is selected, or the slot's built-in default is.
    Unconfigured,
    /// The scalar names a pack that is not installed or provides no provider
    /// for this slot.
    NotInstalled { name: String },
    /// The named pack's installed declaration does not load.
    Invalid { name: String, error: String },
    /// A pack provider with its stored options.
    Provider(Box<Active>),
}

/// The selected provider and the options it was configured with.
#[derive(Debug)]
pub struct Active {
    pub provider: PackProvider,
    pub options: Options,
}

/// Resolve which pack provider backs `slot`.
pub fn resolve(config_dir: &Path, slot: Slot) -> Resolved {
    let Some(name) = active_name(config_dir, slot) else {
        return Resolved::Unconfigured;
    };
    if builtin_default(slot) == Some(name.as_str()) {
        return Resolved::Unconfigured;
    }
    match installed_provider(config_dir, &name, slot) {
        Ok(Some(provider)) => Resolved::Provider(Box::new(Active {
            options: load_options(config_dir, slot, &name),
            provider,
        })),
        Ok(None) => Resolved::NotInstalled { name },
        Err(error) => Resolved::Invalid { name, error },
    }
}

/// Whether a pack credential holds a usable value: a stored secret, or an
/// OAuth token that is not known to be dead. A store read failure is kept,
/// because absence has not been established.
pub fn credential_present(pack: &str, credential: &CredentialDecl) -> Result<bool, String> {
    match &credential.kind {
        CredentialKind::Secret { var } => crate::config::secrets::has_secret(
            &crate::config::secrets::pack_credential_key(pack, &credential.name),
            var,
        ),
        CredentialKind::OAuth { .. } => {
            let status = oauth::status(pack, &credential.name);
            match status.state.as_str() {
                "authorized" => Ok(true),
                "error" => Err(status
                    .detail
                    .unwrap_or_else(|| "OAuth credential store unavailable".to_string())),
                _ => Ok(false),
            }
        }
    }
}

/// Why a credential cannot be used yet: an unfilled secret is `MissingVars`
/// ("needs key"), an unauthorized OAuth credential is `NeedsAuth`.
pub fn credential_readiness(pack: &str, credential: &CredentialDecl) -> Option<NotReady> {
    match credential_present(pack, credential) {
        Ok(true) => None,
        Ok(false) => Some(match &credential.kind {
            CredentialKind::Secret { var } => NotReady::MissingVars {
                vars: vec![var.clone()],
            },
            CredentialKind::OAuth { .. } => NotReady::NeedsAuth,
        }),
        Err(error) => Some(NotReady::CredentialUnavailable { error }),
    }
}

/// Why the MCP server a binding names cannot be called yet, in the readiness
/// vocabulary the MCP layer gates connections with. A server the user
/// authored is not gated, as for any agent; it fails loudly at call time.
fn binding_readiness(config_dir: &Path, binding: &McpBinding) -> Option<NotReady> {
    let entries = crate::config::mcp_servers::workspace_mcp_entries(
        config_dir,
        &crate::config::machine_services::MachineServiceSnapshot::empty(),
    );
    match entries.get(&binding.server) {
        Some(entry) => entry.not_ready.clone(),
        None => Some(NotReady::NeedsSetup {
            missing: vec![format!("MCP server `{}`", binding.server)],
        }),
    }
}

/// Why a provider cannot run yet: its credential, or the MCP server it binds,
/// then any required option left blank.
pub fn readiness(config_dir: &Path, provider: &PackProvider) -> Option<NotReady> {
    let reach = match (&provider.provider.adapter, &provider.credential) {
        (Adapter::Mcp(binding), _) => binding_readiness(config_dir, binding),
        (Adapter::Http(_), Some(credential)) => credential_readiness(&provider.pack, credential),
        (Adapter::Http(_), None) => None,
    };
    reach.or_else(|| {
        let options = load_options(config_dir, provider.provider.slot, &provider.pack);
        cairn_packs::provider_options::missing_required(&provider.provider.options, &options).map(
            |option| NotReady::NeedsSetup {
                missing: vec![option.label.clone()],
            },
        )
    })
}

/// The credential `pack` declares under `credential`, read from its installed
/// layer.
fn installed_credential(
    config_dir: &Path,
    pack: &str,
    credential: &str,
) -> Result<CredentialDecl, String> {
    if cairn_packs::lock::read_lock(config_dir, pack).is_none() {
        return Err(format!("Pack `{pack}` is not installed"));
    }
    provider::load_pack_credentials(&cairn_packs::lock::pack_dir(config_dir, pack), pack)?
        .into_iter()
        .find(|declared| declared.name == credential)
        .ok_or_else(|| format!("Pack `{pack}` declares no credential `{credential}`"))
}

/// Store the operator's value for a pack `secret` credential in the keychain.
pub fn store_credential(
    config_dir: &Path,
    pack: &str,
    credential: &str,
    value: &str,
) -> Result<(), String> {
    let declared = installed_credential(config_dir, pack, credential)?;
    let CredentialKind::Secret { var } = &declared.kind else {
        return Err(format!(
            "`{pack}/{credential}` is an OAuth credential: connect it instead of pasting a value"
        ));
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(format!(
            "An empty value cannot be stored for {var}; clear it instead"
        ));
    }
    crate::config::secrets::set_secret(
        &crate::config::secrets::pack_credential_key(pack, credential),
        var,
        value,
    )
}

/// Remove a pack credential's stored value: a secret, or an OAuth token and
/// any pending authorization. Succeeds when none is stored.
pub fn delete_credential(config_dir: &Path, pack: &str, credential: &str) -> Result<(), String> {
    let declared = installed_credential(config_dir, pack, credential)?;
    match &declared.kind {
        CredentialKind::Secret { var } => crate::config::secrets::delete_secret(
            &crate::config::secrets::pack_credential_key(pack, credential),
            var,
        ),
        CredentialKind::OAuth { .. } => oauth::sign_out(pack, credential),
    }
}

/// The value a provider's credential holds right now, brokered so it is a
/// scrub target before it reaches a header: page content and error bodies flow
/// straight to the agent. `None` when nothing usable is stored.
pub(crate) async fn credential_value(
    pack: &str,
    credential: &CredentialDecl,
    purpose: &str,
) -> Result<Option<BrokeredSecret>, String> {
    match &credential.kind {
        CredentialKind::Secret { var } => {
            crate::security::broker::pack_credential(pack, &credential.name, var, purpose)
        }
        CredentialKind::OAuth { .. } => {
            crate::security::broker::pack_oauth_token(pack, &credential.name, purpose).await
        }
    }
}

/// Select (or clear) the provider for a slot. A name must be the built-in
/// default or an installed pack that provides the slot.
pub fn set_active(config_dir: &Path, slot: Slot, name: Option<&str>) -> Result<(), String> {
    let name = name.map(str::trim).filter(|name| !name.is_empty());
    if let Some(name) = name {
        if builtin_default(slot) != Some(name)
            && installed_provider(config_dir, name, slot)?.is_none()
        {
            return Err(format!(
                "`{name}` is not an installed {} provider",
                slot.noun()
            ));
        }
    }
    crate::config::settings::mutate_workspace_settings(
        config_dir,
        "cairn: update settings",
        |root| {
            let key = serde_yaml::Value::String(scalar_key(slot).to_string());
            match name {
                Some(name) if builtin_default(slot) != Some(name) => {
                    root.insert(key, serde_yaml::Value::String(name.to_string()));
                }
                _ => {
                    root.remove(&key);
                }
            }
            Ok(())
        },
    )
}

/// Validate and store one pack provider's options under `<mapkey>.<pack>`.
pub fn upsert_options(
    config_dir: &Path,
    slot: Slot,
    pack: &str,
    options: &Options,
) -> Result<(), String> {
    let provider = installed_provider(config_dir, pack, slot)?
        .ok_or_else(|| format!("`{pack}` is not an installed {} provider", slot.noun()))?;
    cairn_packs::provider_options::validate_options(
        &provider.provider.options,
        options,
        &format!("{} {}", provider.provider.label, slot.noun()),
    )?;
    crate::config::settings::mutate_workspace_settings(
        config_dir,
        "cairn: update settings",
        |root| {
            let map = root
                .entry(serde_yaml::Value::String(options_key(slot).to_string()))
                .or_insert_with(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
            let map = map
                .as_mapping_mut()
                .ok_or_else(|| format!("`{}` in config is not a mapping", options_key(slot)))?;
            map.insert(
                serde_yaml::Value::String(pack.to_string()),
                serde_yaml::to_value(options)
                    .map_err(|error| format!("Failed to serialize options: {error}"))?,
            );
            Ok(())
        },
    )
}

/// A slot as the Library shows it: which provider is selected and every
/// installed provider for it. Carries credential presence, never a value.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlotView {
    pub slot: Slot,
    pub active: Option<String>,
    /// The compiled zero-config provider, selected when `active` is null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub builtin: Option<BuiltinView>,
    pub providers: Vec<ProviderView>,
    /// Installed packs whose declaration for this slot does not load.
    pub invalid: Vec<InvalidProviderView>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltinView {
    pub id: &'static str,
    pub label: &'static str,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderView {
    pub pack: String,
    pub label: String,
    pub active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_ready: Option<NotReady>,
    pub options: Vec<cairn_packs::provider_options::ProviderOption>,
    pub saved_options: Options,
    /// The credential an `http` provider sends; absent for an MCP binding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<CredentialView>,
    /// The MCP server an `mcp` provider calls, which its own connection
    /// authorizes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mcp_server: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialView {
    pub name: String,
    pub label: String,
    /// `secret` or `oauth`.
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub var: Option<String>,
    /// A secret is stored, or an OAuth token is authorized. Never the value.
    pub present: bool,
    pub hosts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub setup: Option<cairn_packs::mcp::McpVarSetup>,
}

impl CredentialView {
    pub fn of(pack: &str, credential: &CredentialDecl, installed: bool) -> Self {
        CredentialView {
            name: credential.name.clone(),
            label: credential.label.clone(),
            kind: match credential.kind {
                CredentialKind::Secret { .. } => "secret",
                CredentialKind::OAuth { .. } => "oauth",
            },
            var: credential.var().map(str::to_string),
            present: installed && credential_present(pack, credential).unwrap_or(false),
            hosts: credential.hosts.clone(),
            setup: credential.setup.clone(),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InvalidProviderView {
    pub pack: String,
    pub error: String,
}

impl ProviderView {
    pub fn of(config_dir: &Path, provider: &PackProvider, active: bool) -> Self {
        ProviderView {
            pack: provider.pack.clone(),
            label: provider.provider.label.clone(),
            active,
            options: provider.provider.options.clone(),
            saved_options: cairn_packs::provider_options::usable_options(
                &provider.provider.options,
                &load_options(config_dir, provider.provider.slot, &provider.pack),
            ),
            credential: provider
                .credential
                .as_ref()
                .map(|credential| CredentialView::of(&provider.pack, credential, true)),
            mcp_server: match &provider.provider.adapter {
                Adapter::Mcp(binding) => Some(binding.server.clone()),
                Adapter::Http(_) => None,
            },
            not_ready: readiness(config_dir, provider),
        }
    }
}

/// The view of one slot.
pub fn slot_view(config_dir: &Path, slot: Slot) -> SlotView {
    let active = active_name(config_dir, slot).filter(|name| builtin_default(slot) != Some(name));
    let mut providers = Vec::new();
    let mut invalid = Vec::new();
    for entry in installed_providers(config_dir, slot) {
        match entry {
            Ok(provider) => {
                let is_active = active.as_deref() == Some(provider.pack.as_str());
                providers.push(ProviderView::of(config_dir, &provider, is_active));
            }
            Err((pack, error)) => invalid.push(InvalidProviderView { pack, error }),
        }
    }
    SlotView {
        slot,
        active,
        builtin: builtin_default(slot)
            .zip(builtin_label(slot))
            .map(|(id, label)| BuiltinView { id, label }),
        providers,
        invalid,
    }
}

/// The view of every slot a pack provider can fill today.
pub fn slot_views(config_dir: &Path) -> Vec<SlotView> {
    Slot::ALL
        .into_iter()
        .map(|slot| slot_view(config_dir, slot))
        .collect()
}

/// The labels of the providers the app's library offers for `slot`, for
/// guidance that tells an agent what it could ask the operator to install.
pub fn offered_labels(config_dir: &Path, slot: Slot) -> Vec<String> {
    let Some(resource_dir) = cairn_packs::source_dir(config_dir) else {
        return Vec::new();
    };
    let mut labels: Vec<String> = cairn_packs::discover_available_packs(&resource_dir)
        .into_iter()
        .filter(|manifest| {
            matches!(
                provider::load_pack_provider(&manifest.root, &manifest.id, slot),
                Ok(Some(_))
            )
        })
        .map(|manifest| manifest.name)
        .collect();
    labels.sort();
    labels
}

/// Whether a credentialed request may go to `url`: HTTPS, to a host the
/// credential lists. Checked before the first byte is sent and again on every
/// redirect hop.
fn may_reach(hosts: &[String], url: &url::Url) -> bool {
    url.scheme() == "https"
        && url
            .host_str()
            .is_some_and(|host| hosts.contains(&host.to_ascii_lowercase()))
}

/// Send a prepared provider request, refusing before anything leaves the
/// machine unless the URL's host is one the credential lists, and refusing any
/// redirect that would carry the request to a host it does not list.
///
/// reqwest drops `Authorization` on a cross-host redirect but keeps custom
/// headers such as `x-api-key`, so following redirects freely would hand a
/// key to whatever host a provider pointed at.
pub(crate) async fn send(
    pack: &str,
    credential: &CredentialDecl,
    prepared: &PreparedRequest,
) -> Result<reqwest::Response, String> {
    let hosts = credential.hosts.clone();
    let url = url::Url::parse(&prepared.url).map_err(|e| format!("invalid request URL: {e}"))?;
    if !may_reach(&hosts, &url) {
        return Err(format!(
            "refusing to send credential `{pack}/{}` to `{}`, which is not in its hosts",
            credential.name,
            url.host_str().unwrap_or_default()
        ));
    }
    let policy = reqwest::redirect::Policy::custom(move |attempt| {
        if !may_reach(&hosts, attempt.url()) {
            let next = attempt.url().host_str().unwrap_or_default().to_string();
            attempt.error(format!(
                "a redirect to `{next}` was refused: the credential may only reach its own hosts"
            ))
        } else if attempt.previous().len() >= 10 {
            attempt.error("too many redirects")
        } else {
            attempt.follow()
        }
    });
    let client = cairn_tls::client_builder()
        .redirect(policy)
        .build()
        .map_err(|e| e.to_string())?;
    let method =
        reqwest::Method::from_bytes(prepared.method.as_bytes()).unwrap_or(reqwest::Method::GET);
    let mut request = client.request(method, url);
    for (name, value) in &prepared.headers {
        request = request.header(name, value);
    }
    if let Some(body) = &prepared.body {
        request = request.body(body.clone());
    }
    request.send().await.map_err(|e| e.to_string())
}

/// What an MCP-bound provider's call produced.
pub(crate) enum McpOutcome {
    /// The tool's text result.
    Text(String),
    /// The bound server cannot be connected: it is not ready, or is gone.
    NotConnected,
}

/// Call an MCP-bound provider's tool. The server is resolved through the
/// readiness-gated MCP layer, so a pack server that is not ready is never
/// connected and a settings server of the same name shadows the pack's.
pub(crate) async fn mcp_call(
    orch: &crate::orchestrator::Orchestrator,
    provider: &PackProvider,
    binding: &McpBinding,
    inputs: &Inputs<'_>,
) -> Result<McpOutcome, String> {
    use crate::config::mcp_servers::McpServerConfigExt;
    let label = &provider.provider.label;
    let noun = provider.provider.slot.noun();
    let Some(gateway) = orch.mcp_gateway() else {
        return Err(format!(
            "The {label} {noun} provider needs the MCP gateway, which is not available in this host."
        ));
    };
    let machines = crate::config::machine_services::MachineServiceSnapshot::capture(orch);
    let entries =
        crate::config::mcp_servers::resolve_mcp_server_entries(&orch.config_dir, None, &machines);
    let Some(entry) = entries.get(&binding.server) else {
        return Ok(McpOutcome::NotConnected);
    };
    let key = crate::config::secrets::credential_key(&binding.server, None);
    let brokered = entry.config.brokered(&key, &format!("{label} {noun}"))?;
    call_binding(gateway.as_ref(), &brokered, &key, provider, binding, inputs)
        .await
        .map(McpOutcome::Text)
}

/// The gateway call for a binding, split out so it is testable against a mock
/// gateway.
async fn call_binding(
    gateway: &dyn crate::mcp::gateway::McpGateway,
    config: &crate::security::BrokeredMcpConfig,
    key: &str,
    provider: &PackProvider,
    binding: &McpBinding,
    inputs: &Inputs<'_>,
) -> Result<String, String> {
    let arguments = cairn_packs::provider::binding::arguments(&provider.provider, binding, inputs);
    gateway
        .call_tool(
            &format!("cairn-{}", provider.provider.slot.as_str()),
            key,
            config,
            &binding.tool,
            arguments,
            Some(MCP_SLOT_TIMEOUT_MS),
        )
        .await
        // `result: text`, the only form: a markdown slot has no place for an
        // image block, so the composed text is the result.
        .map(|result| result.text)
}

#[cfg(test)]
mod tests;
