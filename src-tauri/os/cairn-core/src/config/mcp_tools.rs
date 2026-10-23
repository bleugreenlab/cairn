//! Persisted, untrusted MCP catalogs. Only `verify_catalog` exposes stored tools.
//!
//! A record is evidence of an authorized fetch, not authority to fetch or reuse.
//! Callers read the credential witnesses before expanding any credential, fetch
//! over a connection opened for that fetch alone, then re-read the witnesses and
//! publish while holding the credential mutation lock ([`record_catalog`] takes
//! the guard as proof). The lock never spans the network fetch, which may itself
//! refresh credentials. A pooled connection can carry credentials expanded long
//! before the witness was read, so no cacheable fetch reuses one.
//! Local withdrawal is immediate. Server-side withdrawal is invisible until the
//! next authorized refresh; a verified record is only an observation as of its
//! fetched_at_ms timestamp, not a claim about the server's current catalog.

use cairn_packs::server_config::{
    fingerprint_catalog_binding, CatalogBinding, CATALOG_BINDING_VERSION,
    MCP_CONFIG_FINGERPRINT_ALGORITHM, MCP_CONFIG_FINGERPRINT_ENCODING_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::credential_witness::CredentialMutationGuard;
use crate::mcp::gateway::{McpToolCatalog, McpToolDef};

const STORE_FILENAME: &str = "mcp_tools.json";
pub const DEFAULT_TTL_MS: u64 = 5 * 60 * 1_000;
pub const MIN_TTL_MS: u64 = 5 * 60 * 1_000;
pub const MAX_TTL_MS: u64 = 24 * 60 * 60 * 1_000;
pub const MAX_CATALOG_TOOLS: usize = 256;
/// Maximum compact JSON encoding of one complete record, not just its tools.
pub const MAX_CATALOG_BYTES: usize = 256 * 1_024;

/// Fetch provenance supplied by the authorized caller, never inferred on read.
/// Authentication, audience, protocol, and policy provenance live in the binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogOrigin {
    pub server: String,
    pub ownership_scope: String,
    pub owner: String,
    pub transport: String,
    /// Non-secret endpoint or executable identity supplied by the caller.
    pub endpoint: String,
    pub protocol_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CatalogRecord {
    pub binding_version: u32,
    pub binding: CatalogBinding,
    pub binding_digest: String,
    pub origin: CatalogOrigin,
    pub tools: Vec<McpToolDef>,
    pub content_hash: String,
    pub ttl_ms: u64,
    pub server_ttl_ms: Option<u64>,
    /// An inert server hint: never consulted for identity, sharing, or authority.
    pub cache_scope: Option<String>,
    pub fetched_at_ms: u64,
    pub expires_at_ms: u64,
    pub withdrawn: Option<String>,
}

/// No rejection variant carries tool metadata, including legacy and corrupt data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogRejection {
    Missing,
    Unreadable,
    BindingMismatch {
        field: &'static str,
    },
    ContentHashMismatch,
    Expired,
    Withdrawn,
    BoundsExceeded,
    /// Authorized handlers may return this before opening the sidecar at all.
    CredentialUpdating,
}

#[derive(Debug, Clone)]
pub enum CatalogVerdict {
    Verified(Box<CatalogRecord>),
    Rejected(CatalogRejection),
}

/// Everything a request binds a catalog to that it can know before connecting:
/// the binding without the protocol, which only a connection negotiates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogContext {
    pub ownership_scope: String,
    pub project_id: Option<String>,
    pub owner: String,
    pub config_identity: cairn_common::authorization::McpConfigFingerprint,
    pub credential_key: String,
    pub credential_context: cairn_packs::server_config::CatalogCredentialContext,
}

impl CatalogContext {
    /// The full binding for a catalog fetched under `protocol_version`.
    pub fn binding(&self, protocol_version: &str) -> CatalogBinding {
        CatalogBinding {
            binding_version: CATALOG_BINDING_VERSION,
            ownership_scope: self.ownership_scope.clone(),
            project_id: self.project_id.clone(),
            owner: self.owner.clone(),
            config_identity: self.config_identity.clone(),
            credential_key: self.credential_key.clone(),
            credential_context: self.credential_context.clone(),
            protocol_version: protocol_version.to_string(),
            method: "tools/list".to_string(),
            authorization_revision: cairn_common::authorization::AUTHORITY_MODEL_VERSION,
        }
    }
}

/// The scope a captured tool list belongs to, mirroring server resolution.
#[derive(Debug, Clone, Copy)]
pub enum ToolScope<'a> {
    Workspace,
    /// The caller supplies the same canonical path used in the binding.
    Project(&'a Path),
}

impl ToolScope<'_> {
    pub fn identity(self) -> String {
        match self {
            Self::Workspace => "workspace".into(),
            Self::Project(path) => format!("project:{}", path.to_string_lossy()),
        }
    }
}

/// Raw record values preserve legacy and malformed entries during unrelated
/// writes. There is deliberately no public raw-record or unverified-tool reader.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct McpToolStore {
    #[serde(default)]
    workspace: HashMap<String, Value>,
    #[serde(default)]
    projects: HashMap<String, HashMap<String, Value>>,
    /// How many times each scoped server has been withdrawn. A capture that
    /// began before a withdrawal must not publish after it, even if an
    /// identical definition was re-added in between.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    withdrawals: HashMap<String, u64>,
}

fn withdrawal_key(scope: ToolScope<'_>, server: &str) -> String {
    format!("{}\u{0}{server}", scope.identity())
}

/// The withdrawal count a capture reads before it fetches.
pub fn withdrawal_epoch(
    config_dir: &Path,
    scope: ToolScope<'_>,
    server: &str,
) -> Result<u64, String> {
    Ok(load(config_dir)?
        .withdrawals
        .get(&withdrawal_key(scope, server))
        .copied()
        .unwrap_or(0))
}

impl McpToolStore {
    fn entry(&self, scope: ToolScope<'_>, server: &str) -> Option<&Value> {
        match scope {
            ToolScope::Workspace => self.workspace.get(server),
            ToolScope::Project(path) => self.projects.get(&project_key(path))?.get(server),
        }
    }

    fn insert(&mut self, scope: ToolScope<'_>, server: &str, value: Value) {
        match scope {
            ToolScope::Workspace => {
                self.workspace.insert(server.into(), value);
            }
            ToolScope::Project(path) => {
                self.projects
                    .entry(project_key(path))
                    .or_default()
                    .insert(server.into(), value);
            }
        }
    }
}

fn store_path(config_dir: &Path) -> PathBuf {
    config_dir.join(STORE_FILENAME)
}

fn project_key(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn load(config_dir: &Path) -> Result<McpToolStore, String> {
    let content = match std::fs::read(store_path(config_dir)) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(McpToolStore::default()),
        Err(e) => return Err(format!("Failed to read MCP tool store: {e}")),
    };
    serde_json::from_slice(&content).map_err(|e| format!("Unreadable MCP tool store: {e}"))
}

fn save(config_dir: &Path, store: &McpToolStore) -> Result<(), String> {
    std::fs::create_dir_all(config_dir)
        .map_err(|e| format!("Failed to create config directory: {e}"))?;
    let json = serde_json::to_vec(store)
        .map_err(|e| format!("Failed to serialize MCP tool store: {e}"))?;
    std::fs::write(store_path(config_dir), json)
        .map_err(|e| format!("Failed to write MCP tool store: {e}"))
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn valid_binding(binding: &CatalogBinding, scope: ToolScope<'_>) -> bool {
    binding.binding_version == CATALOG_BINDING_VERSION
        && binding.ownership_scope == scope.identity()
        && binding.project_id.as_ref().is_none_or(|id| !id.is_empty())
        && (!matches!(scope, ToolScope::Project(_)) || binding.project_id.is_some())
        && (binding.owner == "user"
            || binding.owner.strip_prefix("pack:").is_some_and(|id| !id.is_empty()))
        && binding.config_identity.algorithm == MCP_CONFIG_FINGERPRINT_ALGORITHM
        && binding.config_identity.encoding_version == MCP_CONFIG_FINGERPRINT_ENCODING_VERSION
        && is_digest(&binding.config_identity.digest)
        && !binding.credential_key.is_empty()
        // The caller's trusted negotiated witness determines supported versions.
        && !binding.protocol_version.is_empty()
        && binding.authorization_revision == cairn_common::authorization::AUTHORITY_MODEL_VERSION
        && binding.method == "tools/list"
        && binding.credential_context.oauth.signed_in().is_none_or(|oauth| {
            oauth.issuer.as_ref().is_some_and(|issuer| !issuer.is_empty())
                && !oauth.resource.is_empty()
                && !oauth.client_id.is_empty()
        })
}

fn valid_origin(
    origin: &CatalogOrigin,
    binding: &CatalogBinding,
    scope: ToolScope<'_>,
    server: &str,
) -> bool {
    !server.is_empty()
        && origin.server == server
        && origin.ownership_scope == binding.ownership_scope
        && origin.owner == binding.owner
        && matches!(origin.transport.as_str(), "stdio" | "http" | "sse")
        && !origin.endpoint.is_empty()
        && origin.protocol_version == binding.protocol_version
        && binding.credential_key
            == super::secrets::credential_key(
                server,
                match scope {
                    ToolScope::Workspace => None,
                    ToolScope::Project(path) => Some(path),
                },
            )
}

/// JSON object key order is not content identity. Canonicalize recursively even
/// if serde_json is built with preserve_order by another workspace dependency.
fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted: std::collections::BTreeMap<_, _> = map.into_iter().collect();
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(k, v)| (k, canonical_json(v)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        value => value,
    }
}

fn content_hash(tools: &[McpToolDef]) -> Result<String, String> {
    let value = serde_json::to_value(tools).map_err(|e| e.to_string())?;
    let bytes = serde_json::to_vec(&canonical_json(value)).map_err(|e| e.to_string())?;
    let mut digest = Sha256::new();
    digest.update(b"cairn.mcp.catalog-content.v1\0");
    digest.update(bytes);
    Ok(format!("{:x}", digest.finalize()))
}

fn bounded_value(record: &CatalogRecord) -> Result<Value, CatalogRejection> {
    if record.tools.len() > MAX_CATALOG_TOOLS {
        return Err(CatalogRejection::BoundsExceeded);
    }
    let value = serde_json::to_value(record).map_err(|_| CatalogRejection::Unreadable)?;
    if serde_json::to_vec(&value)
        .map_err(|_| CatalogRejection::Unreadable)?
        .len()
        > MAX_CATALOG_BYTES
    {
        return Err(CatalogRejection::BoundsExceeded);
    }
    Ok(value)
}

/// The witnesses read before expanding credentials and again after fetching,
/// both completed with the protocol the fetch negotiated. They must agree.
pub struct CatalogCapture<'a> {
    pub before: &'a CatalogBinding,
    pub after: &'a CatalogBinding,
    pub origin: CatalogOrigin,
    pub fetched_at_ms: u64,
    /// [`withdrawal_epoch`] as read before the fetch began.
    pub withdrawal_epoch: u64,
}

/// Record an authorized, complete tools/list response. Refuses to overwrite an
/// unreadable store; no synthetic provenance or legacy upgrade is performed.
///
/// The guard is the proof that `capture.after` was read under the same lock
/// every credential mutation takes, so no mutation lands between that check
/// and this write.
pub fn record_catalog(
    _guard: &CredentialMutationGuard,
    config_dir: &Path,
    scope: ToolScope<'_>,
    server: &str,
    catalog: &McpToolCatalog,
    capture: CatalogCapture<'_>,
) -> Result<CatalogRecord, String> {
    if !valid_binding(capture.before, scope)
        || !valid_binding(capture.after, scope)
        || fingerprint_catalog_binding(capture.before) != fingerprint_catalog_binding(capture.after)
        || !valid_origin(&capture.origin, capture.before, scope, server)
        || catalog.protocol_version.as_deref() != Some(capture.before.protocol_version.as_str())
    {
        return Err("Catalog capture has unknown or disagreeing binding/origin witnesses".into());
    }
    if catalog.tools.len() > MAX_CATALOG_TOOLS {
        return Err("Catalog exceeds local tool bound".into());
    }
    let ttl_ms = effective_ttl(catalog.ttl_ms)
        .ok_or("Catalog TTL is outside the representable cacheable range")?;
    let expires_at_ms = capture.fetched_at_ms.saturating_add(ttl_ms);
    let record = CatalogRecord {
        binding_version: CATALOG_BINDING_VERSION,
        binding: capture.before.clone(),
        binding_digest: fingerprint_catalog_binding(capture.before),
        origin: capture.origin,
        content_hash: content_hash(&catalog.tools)?,
        tools: catalog.tools.clone(),
        ttl_ms,
        server_ttl_ms: catalog.ttl_ms,
        cache_scope: catalog.cache_scope.clone(),
        fetched_at_ms: capture.fetched_at_ms,
        expires_at_ms,
        withdrawn: None,
    };
    let value = bounded_value(&record).map_err(|e| format!("Invalid catalog: {e:?}"))?;
    let mut store = load(config_dir)?;
    let withdrawals = store
        .withdrawals
        .get(&withdrawal_key(scope, server))
        .copied()
        .unwrap_or(0);
    if withdrawals != capture.withdrawal_epoch {
        return Err("the server was removed while its catalog was being fetched".into());
    }
    store.insert(scope, server, value);
    save(config_dir, &store)?;
    Ok(record)
}

// Reject unrepresentable millisecond durations before clamping. Otherwise a
// corrupt u64::MAX hint becomes an apparently valid, day-long cache entry.
fn effective_ttl(server_ttl_ms: Option<u64>) -> Option<u64> {
    let ttl = server_ttl_ms.unwrap_or(DEFAULT_TTL_MS);
    i64::try_from(ttl).ok()?;
    (ttl >= MIN_TTL_MS).then_some(ttl.min(MAX_TTL_MS))
}

fn mismatched_field(actual: &CatalogBinding, expected: &CatalogBinding) -> &'static str {
    if actual.ownership_scope != expected.ownership_scope {
        "scope"
    } else if actual.project_id != expected.project_id {
        "project_id"
    } else if actual.owner != expected.owner {
        "owner"
    } else if actual.config_identity != expected.config_identity {
        "config"
    } else if actual.credential_key != expected.credential_key {
        "credential_key"
    } else if actual.credential_context != expected.credential_context {
        "credential"
    } else if actual.protocol_version != expected.protocol_version {
        "protocol"
    } else if actual.method != expected.method {
        "method"
    } else {
        "authorization_revision"
    }
}

/// `expected_for` maps the record's own protocol to the binding the current
/// request expects under it, or `None` when that protocol is not acceptable.
fn verify_value_with(
    value: &Value,
    scope: ToolScope<'_>,
    server: &str,
    expected_for: impl Fn(&str) -> Option<CatalogBinding>,
    now_ms: u64,
) -> Result<CatalogRecord, CatalogRejection> {
    // Enforce bounds before deserializing or hashing attacker-controlled tools.
    if serde_json::to_vec(value)
        .map_err(|_| CatalogRejection::Unreadable)?
        .len()
        > MAX_CATALOG_BYTES
        || value
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| tools.len() > MAX_CATALOG_TOOLS)
    {
        return Err(CatalogRejection::BoundsExceeded);
    }
    let record: CatalogRecord =
        serde_json::from_value(value.clone()).map_err(|_| CatalogRejection::Unreadable)?;
    if record.binding_version != CATALOG_BINDING_VERSION
        || !valid_binding(&record.binding, scope)
        || !valid_origin(&record.origin, &record.binding, scope, server)
    {
        return Err(CatalogRejection::Unreadable);
    }
    let expected = expected_for(&record.binding.protocol_version)
        .ok_or(CatalogRejection::BindingMismatch { field: "protocol" })?;
    let expected = &expected;
    if !valid_binding(expected, scope) {
        return Err(CatalogRejection::Unreadable);
    }
    let actual_digest = fingerprint_catalog_binding(&record.binding);
    if actual_digest != record.binding_digest {
        return Err(CatalogRejection::Unreadable);
    }
    if actual_digest != fingerprint_catalog_binding(expected) {
        return Err(CatalogRejection::BindingMismatch {
            field: mismatched_field(&record.binding, expected),
        });
    }
    if content_hash(&record.tools).map_err(|_| CatalogRejection::Unreadable)? != record.content_hash
    {
        return Err(CatalogRejection::ContentHashMismatch);
    }
    if !(MIN_TTL_MS..=MAX_TTL_MS).contains(&record.ttl_ms)
        || Some(record.ttl_ms) != effective_ttl(record.server_ttl_ms)
        || record.fetched_at_ms.saturating_add(record.ttl_ms) != record.expires_at_ms
    {
        return Err(CatalogRejection::Unreadable);
    }
    if record.withdrawn.is_some() {
        return Err(CatalogRejection::Withdrawn);
    }
    if now_ms < record.fetched_at_ms || now_ms >= record.expires_at_ms {
        return Err(CatalogRejection::Expired);
    }
    Ok(record)
}

/// The only cached-content read API. The context must come from the current
/// authorized request, never from the sidecar, and an unknown or updating
/// credential witness must be rejected before one can be built. The record's
/// protocol is accepted only if it is one this host can still negotiate.
/// There is no workspace fallback when a project record is missing or rejected.
pub fn verify_catalog(
    config_dir: &Path,
    scope: ToolScope<'_>,
    server: &str,
    context: &CatalogContext,
    supported_protocols: &[String],
    now_ms: u64,
) -> CatalogVerdict {
    let result = (|| {
        if server.is_empty() {
            return Err(CatalogRejection::Unreadable);
        }
        let store = load(config_dir).map_err(|_| CatalogRejection::Unreadable)?;
        let value = store
            .entry(scope, server)
            .ok_or(CatalogRejection::Missing)?;
        verify_value_with(
            value,
            scope,
            server,
            |protocol| {
                supported_protocols
                    .iter()
                    .any(|supported| supported == protocol)
                    .then(|| context.binding(protocol))
            },
            now_ms,
        )
    })();
    match result {
        Ok(record) => CatalogVerdict::Verified(Box::new(record)),
        Err(reason) => CatalogVerdict::Rejected(reason),
    }
}

/// What Settings may show about a stored record: its provenance, never its tools.
// Built once per Settings request and serialized straight out; boxing the
// recorded variant would buy nothing.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "state")]
pub enum CatalogInspection {
    Missing,
    /// Legacy, corrupt, or from another binding version. Never yields tools;
    /// the next authorized read replaces it.
    Unreadable,
    /// Structurally sound. Whether it verifies depends on the requesting run,
    /// so this says nothing about whether an agent would be served it.
    #[serde(rename_all = "camelCase")]
    Recorded {
        origin: CatalogOrigin,
        fetched_at_ms: u64,
        expires_at_ms: u64,
        content_hash: String,
        tool_count: usize,
        cache_scope: Option<String>,
    },
}

pub fn inspect_catalog(config_dir: &Path, scope: ToolScope<'_>, server: &str) -> CatalogInspection {
    let Ok(store) = load(config_dir) else {
        return CatalogInspection::Unreadable;
    };
    let Some(value) = store.entry(scope, server) else {
        return CatalogInspection::Missing;
    };
    match serde_json::from_value::<CatalogRecord>(value.clone()) {
        Ok(record)
            if record.binding_version == CATALOG_BINDING_VERSION
                && content_hash(&record.tools).as_deref() == Ok(record.content_hash.as_str()) =>
        {
            CatalogInspection::Recorded {
                origin: record.origin,
                fetched_at_ms: record.fetched_at_ms,
                expires_at_ms: record.expires_at_ms,
                content_hash: record.content_hash,
                tool_count: record.tools.len(),
                cache_scope: record.cache_scope,
            }
        }
        _ => CatalogInspection::Unreadable,
    }
}

/// Withdraw a scoped catalog. Local withdrawal is immediate.
pub fn remove_server(config_dir: &Path, scope: ToolScope<'_>, server: &str) -> Result<(), String> {
    let guard = super::credential_witness::acquire()?;
    withdraw(&guard, config_dir, scope, server)
}

/// Remove a server: withdraw its catalog, then run `unregister` (the registry
/// change), as one critical section under the credential mutation lock.
///
/// Withdrawal comes first. If it fails, nothing has changed and the removal
/// can simply be retried; were the registry changed first, a retry would find
/// no server to remove and the withdrawal obligation would be lost. If the
/// registry change fails afterwards, the catalog is merely gone and the next
/// read refetches. Holding the lock across both means no capture can publish
/// between them, since publication takes the same lock.
pub fn withdraw_and_unregister(
    config_dir: &Path,
    scope: ToolScope<'_>,
    server: &str,
    unregister: impl FnOnce() -> Result<(), String>,
) -> Result<(), String> {
    let guard = super::credential_witness::acquire()?;
    withdraw(&guard, config_dir, scope, server)?;
    unregister()
}

fn withdraw(
    _guard: &CredentialMutationGuard,
    config_dir: &Path,
    scope: ToolScope<'_>,
    server: &str,
) -> Result<(), String> {
    // Advances the withdrawal count even when there is no row: a capture in
    // flight has no row yet and must still be refused.
    let mut store = load(config_dir)?;
    match scope {
        ToolScope::Workspace => {
            store.workspace.remove(server);
        }
        ToolScope::Project(path) => {
            if let Some(servers) = store.projects.get_mut(&project_key(path)) {
                servers.remove(server);
            }
        }
    }
    let count = store
        .withdrawals
        .entry(withdrawal_key(scope, server))
        .or_insert(0);
    *count = count
        .checked_add(1)
        .ok_or("MCP catalog withdrawal count is exhausted")?;
    save(config_dir, &store)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_packs::server_config::{
        CatalogCredentialContext, CatalogOAuthContext, CatalogOAuthState, CredentialGeneration,
    };
    use serde_json::json;

    const SERVER: &str = "catalog-test";
    const NOW: u64 = 1_000_000;

    fn generation(incarnation: &str, counter: u64) -> CredentialGeneration {
        CredentialGeneration {
            incarnation: incarnation.into(),
            counter,
        }
    }

    fn lock() -> CredentialMutationGuard {
        super::super::credential_witness::acquire().unwrap()
    }

    fn context_of(binding: &CatalogBinding) -> CatalogContext {
        CatalogContext {
            ownership_scope: binding.ownership_scope.clone(),
            project_id: binding.project_id.clone(),
            owner: binding.owner.clone(),
            config_identity: binding.config_identity.clone(),
            credential_key: binding.credential_key.clone(),
            credential_context: binding.credential_context.clone(),
        }
    }

    /// Verify against exactly `expected`, protocol included.
    fn verify_value(
        value: &Value,
        scope: ToolScope<'_>,
        server: &str,
        expected: &CatalogBinding,
        now_ms: u64,
    ) -> Result<CatalogRecord, CatalogRejection> {
        verify_value_with(value, scope, server, |_| Some(expected.clone()), now_ms)
    }

    /// The public path, with `expected`'s protocol as the only supported one.
    fn verify(
        config_dir: &Path,
        scope: ToolScope<'_>,
        server: &str,
        expected: &CatalogBinding,
        now_ms: u64,
    ) -> CatalogVerdict {
        verify_catalog(
            config_dir,
            scope,
            server,
            &context_of(expected),
            std::slice::from_ref(&expected.protocol_version),
            now_ms,
        )
    }

    fn binding(scope: ToolScope<'_>) -> CatalogBinding {
        CatalogBinding {
            binding_version: CATALOG_BINDING_VERSION,
            ownership_scope: scope.identity(),
            project_id: Some("project-a".into()),
            owner: "user".into(),
            config_identity: cairn_common::authorization::McpConfigFingerprint {
                algorithm: MCP_CONFIG_FINGERPRINT_ALGORITHM.into(),
                encoding_version: MCP_CONFIG_FINGERPRINT_ENCODING_VERSION,
                digest: "a".repeat(64),
            },
            credential_key: super::super::secrets::credential_key(
                SERVER,
                match scope {
                    ToolScope::Workspace => None,
                    ToolScope::Project(path) => Some(path),
                },
            ),
            credential_context: CatalogCredentialContext {
                static_generation: Some(generation("static", 7)),
                oauth: CatalogOAuthState::SignedIn(CatalogOAuthContext {
                    issuer: Some("https://issuer.example".into()),
                    resource: "https://resource.example".into(),
                    client_id: "client".into(),
                    scopes: vec!["read".into(), "write".into()],
                    generation: generation("oauth", 9),
                }),
            },
            protocol_version: "2025-11-25".into(),
            method: "tools/list".into(),
            authorization_revision: cairn_common::authorization::AUTHORITY_MODEL_VERSION,
        }
    }

    fn origin(binding: &CatalogBinding) -> CatalogOrigin {
        CatalogOrigin {
            server: SERVER.into(),
            ownership_scope: binding.ownership_scope.clone(),
            owner: binding.owner.clone(),
            transport: "http".into(),
            endpoint: "https://resource.example/mcp".into(),
            protocol_version: binding.protocol_version.clone(),
        }
    }

    fn catalog() -> McpToolCatalog {
        McpToolCatalog {
            protocol_version: Some("2025-11-25".into()),
            tools: vec![McpToolDef {
                name: "lookup".into(),
                description: Some("Untrusted description".into()),
                input_schema: json!({"type": "object", "properties": {"q": {"type": "string"}}}),
            }],
            ttl_ms: None,
            cache_scope: Some("public".into()),
        }
    }

    fn capture(binding: &CatalogBinding) -> CatalogCapture<'_> {
        CatalogCapture {
            before: binding,
            after: binding,
            origin: origin(binding),
            fetched_at_ms: NOW,
            withdrawal_epoch: 0,
        }
    }

    fn fixture(scope: ToolScope<'_>) -> (tempfile::TempDir, CatalogBinding, Value) {
        let dir = tempfile::tempdir().unwrap();
        let binding = binding(scope);
        record_catalog(
            &lock(),
            dir.path(),
            scope,
            SERVER,
            &catalog(),
            capture(&binding),
        )
        .unwrap();
        let value = load(dir.path())
            .unwrap()
            .entry(scope, SERVER)
            .unwrap()
            .clone();
        assert!(verify_value(&value, scope, SERVER, &binding, NOW).is_ok());
        (dir, binding, value)
    }

    fn reject(value: &Value, binding: &CatalogBinding, reason: CatalogRejection) {
        assert_eq!(
            verify_value(value, ToolScope::Workspace, SERVER, binding, NOW).unwrap_err(),
            reason
        );
    }

    #[test]
    fn ttl_write_and_verify_boundaries() {
        for (hint, expected) in [
            (None, Some(DEFAULT_TTL_MS)),
            (Some(0), None),
            (Some(1), None),
            (Some(MIN_TTL_MS - 1), None),
            (Some(MIN_TTL_MS), Some(MIN_TTL_MS)),
            (Some(MAX_TTL_MS), Some(MAX_TTL_MS)),
            (Some(MAX_TTL_MS + 1), Some(MAX_TTL_MS)),
            (Some(u64::MAX), None),
            (Some(i64::MAX as u64 + 1), None),
        ] {
            let (dir, binding, mut value) = fixture(ToolScope::Workspace);
            let mut response = catalog();
            response.ttl_ms = hint;
            let before = std::fs::read(store_path(dir.path())).unwrap();
            let result = record_catalog(
                &lock(),
                dir.path(),
                ToolScope::Workspace,
                SERVER,
                &response,
                capture(&binding),
            );
            assert_eq!(result.is_ok(), expected.is_some(), "{hint:?}");
            // Forge a row independently of the write path, including the old clamp bug.
            let ttl = hint.unwrap_or(DEFAULT_TTL_MS).min(MAX_TTL_MS);
            value["serverTtlMs"] = json!(hint);
            value["ttlMs"] = json!(ttl);
            value["expiresAtMs"] = json!(NOW + ttl);
            assert_eq!(
                verify_value(&value, ToolScope::Workspace, SERVER, &binding, NOW).is_ok(),
                expected.is_some(),
                "{hint:?}"
            );
            if let Some(ttl) = expected {
                match verify(dir.path(), ToolScope::Workspace, SERVER, &binding, NOW) {
                    CatalogVerdict::Verified(record) => assert_eq!(record.ttl_ms, ttl),
                    other => panic!("{other:?}"),
                }
            } else {
                assert_eq!(std::fs::read(store_path(dir.path())).unwrap(), before);
            }
        }
    }

    #[test]
    fn time_boundaries_and_saturation_never_wrap() {
        let (_dir, binding, mut value) = fixture(ToolScope::Workspace);
        for (now, fresh) in [
            (NOW - 1, false),
            (NOW, true),
            (NOW + DEFAULT_TTL_MS - 1, true),
            (NOW + DEFAULT_TTL_MS, false),
        ] {
            assert_eq!(
                verify_value(&value, ToolScope::Workspace, SERVER, &binding, now).is_ok(),
                fresh
            );
        }
        value["expiresAtMs"] = json!(NOW + DEFAULT_TTL_MS + 1);
        reject(&value, &binding, CatalogRejection::Unreadable);
        value["fetchedAtMs"] = json!(u64::MAX - 1);
        value["expiresAtMs"] = json!(u64::MAX);
        assert!(verify_value(&value, ToolScope::Workspace, SERVER, &binding, u64::MAX - 1).is_ok());
        assert_eq!(
            verify_value(&value, ToolScope::Workspace, SERVER, &binding, u64::MAX).unwrap_err(),
            CatalogRejection::Expired
        );
    }

    #[test]
    fn every_binding_dimension_rejects_changed_context() {
        let (_dir, binding, value) = fixture(ToolScope::Workspace);
        let mutations: &[fn(&mut CatalogBinding)] = &[
            |b| b.binding_version += 1,
            |b| b.ownership_scope = "project:/other".into(),
            |b| b.project_id = Some("project-b".into()),
            |b| b.owner = "pack:other".into(),
            |b| b.config_identity.algorithm = "unknown".into(),
            |b| b.config_identity.encoding_version += 1,
            |b| b.config_identity.digest = "b".repeat(64),
            |b| b.credential_key = "another-server".into(),
            |b| b.credential_context.static_generation = Some(generation("static", 8)),
            |b| b.credential_context.static_generation = Some(generation("recreated", 7)),
            |b| b.credential_context.static_generation = None,
            |b| b.credential_context.oauth = CatalogOAuthState::Never,
            |b| b.credential_context.oauth = CatalogOAuthState::SignedOut(generation("oauth", 9)),
            |b| {
                b.credential_context
                    .oauth
                    .signed_in_mut()
                    .unwrap()
                    .generation
                    .counter += 1
            },
            |b| {
                b.credential_context
                    .oauth
                    .signed_in_mut()
                    .unwrap()
                    .generation
                    .incarnation = "recreated".into()
            },
            |b| {
                b.credential_context.oauth.signed_in_mut().unwrap().issuer =
                    Some("https://other.example".into())
            },
            |b| {
                b.credential_context.oauth.signed_in_mut().unwrap().resource =
                    "https://other.example".into()
            },
            |b| {
                b.credential_context
                    .oauth
                    .signed_in_mut()
                    .unwrap()
                    .client_id = "other".into()
            },
            |b| {
                b.credential_context
                    .oauth
                    .signed_in_mut()
                    .unwrap()
                    .scopes
                    .push("admin".into())
            },
            |b| b.protocol_version = "2024-11-05".into(),
            |b| b.method = "tools/call".into(),
            |b| b.authorization_revision += 1,
        ];
        for (index, mutate) in mutations.iter().enumerate() {
            let mut changed = binding.clone();
            mutate(&mut changed);
            assert!(
                verify_value(&value, ToolScope::Workspace, SERVER, &changed, NOW).is_err(),
                "expected dimension {index}"
            );
            let mut forged = value.clone();
            forged["binding"] = serde_json::to_value(&changed).unwrap();
            // A correct digest for the changed structure cannot match the current request.
            forged["bindingDigest"] = json!(fingerprint_catalog_binding(&changed));
            assert!(
                verify_value(&forged, ToolScope::Workspace, SERVER, &binding, NOW).is_err(),
                "stored dimension {index}"
            );
        }
    }

    #[test]
    fn digest_is_recomputed_and_content_hash_covers_all_tool_fields() {
        let (_dir, binding, value) = fixture(ToolScope::Workspace);
        let mut changed = value.clone();
        changed["bindingDigest"] = json!("0".repeat(64));
        reject(&changed, &binding, CatalogRejection::Unreadable);
        changed = value.clone();
        changed["binding"]["credential_context"]["static_generation"] =
            json!({"incarnation": "static", "counter": 8});
        reject(&changed, &binding, CatalogRejection::Unreadable);
        for field in ["name", "description", "input_schema"] {
            let mut changed = value.clone();
            changed["tools"][0][field] = if field == "input_schema" {
                json!({"type": "null"})
            } else {
                json!("changed")
            };
            reject(&changed, &binding, CatalogRejection::ContentHashMismatch);
        }
        changed = value;
        changed["contentHash"] = json!("0".repeat(64));
        reject(&changed, &binding, CatalogRejection::ContentHashMismatch);
    }

    #[test]
    fn unknown_legacy_malformed_and_missing_provenance_are_unreadable() {
        let (_dir, binding, value) = fixture(ToolScope::Workspace);
        for legacy in [
            Value::Null,
            json!([]),
            json!([{"name": "legacy"}]),
            json!({"tools": []}),
            json!("corrupt"),
        ] {
            reject(&legacy, &binding, CatalogRejection::Unreadable);
        }
        for field in [
            "binding",
            "bindingDigest",
            "origin",
            "contentHash",
            "ttlMs",
            "fetchedAtMs",
            "expiresAtMs",
        ] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            reject(&missing, &binding, CatalogRejection::Unreadable);
        }
        for (field, replacement) in [
            ("bindingVersion", json!(999)),
            ("unexpected", json!(true)),
            ("ttlMs", json!(-1)),
        ] {
            let mut changed = value.clone();
            changed[field] = replacement;
            reject(&changed, &binding, CatalogRejection::Unreadable);
        }
        for (field, replacement) in [
            ("server", "another"),
            ("ownershipScope", "project:/other"),
            ("owner", "pack:other"),
            ("transport", "unknown"),
            ("endpoint", ""),
            ("protocolVersion", "unknown"),
        ] {
            let mut changed = value.clone();
            changed["origin"][field] = json!(replacement);
            reject(&changed, &binding, CatalogRejection::Unreadable);
        }
    }

    #[test]
    fn bounds_apply_to_complete_record_on_write_and_verify() {
        let (dir, binding, mut value) = fixture(ToolScope::Workspace);
        let mut response = catalog();
        response.tools = vec![response.tools[0].clone(); MAX_CATALOG_TOOLS];
        record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &response,
            capture(&binding),
        )
        .unwrap();
        assert!(matches!(
            verify(dir.path(), ToolScope::Workspace, SERVER, &binding, NOW),
            CatalogVerdict::Verified(_)
        ));
        response.tools.push(response.tools[0].clone());
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &response,
            capture(&binding)
        )
        .is_err());
        value["tools"] = serde_json::to_value(&response.tools).unwrap();
        reject(&value, &binding, CatalogRejection::BoundsExceeded);

        let (_, _, mut value) = fixture(ToolScope::Workspace);
        value["cacheScope"] = json!("");
        let size = serde_json::to_vec(&value).unwrap().len();
        value["cacheScope"] = json!("x".repeat(MAX_CATALOG_BYTES - size));
        assert_eq!(serde_json::to_vec(&value).unwrap().len(), MAX_CATALOG_BYTES);
        assert!(verify_value(&value, ToolScope::Workspace, SERVER, &binding, NOW).is_ok());
        response = catalog();
        response.cache_scope = value["cacheScope"].as_str().map(str::to_owned);
        record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &response,
            capture(&binding),
        )
        .unwrap();
        response.cache_scope.as_mut().unwrap().push('x');
        value["cacheScope"] = json!(response.cache_scope);
        reject(&value, &binding, CatalogRejection::BoundsExceeded);
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &response,
            capture(&binding)
        )
        .is_err());
        response = catalog();
        response.tools[0].description = Some("x".repeat(MAX_CATALOG_BYTES));
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &response,
            capture(&binding)
        )
        .is_err());
    }

    #[test]
    fn cache_scope_is_inert_and_oauth_scopes_are_a_set() {
        let (_dir, binding, value) = fixture(ToolScope::Workspace);
        for hint in [
            Value::Null,
            json!("public"),
            json!("private"),
            json!("project-b"),
        ] {
            let mut changed = value.clone();
            changed["cacheScope"] = hint;
            assert!(verify_value(&changed, ToolScope::Workspace, SERVER, &binding, NOW).is_ok());
            type Mutation = (&'static str, fn(&mut CatalogBinding));
            let mutations: &[Mutation] = &[
                ("project_id", |b| b.project_id = Some("project-b".into())),
                ("credential", |b| {
                    b.credential_context.static_generation = Some(generation("static", 8))
                }),
                ("credential", |b| {
                    b.credential_context
                        .oauth
                        .signed_in_mut()
                        .unwrap()
                        .generation
                        .counter += 1
                }),
                ("config", |b| b.config_identity.digest = "b".repeat(64)),
                ("owner", |b| b.owner = "pack:other".into()),
                ("protocol", |b| b.protocol_version = "2024-11-05".into()),
            ];
            for (field, mutate) in mutations {
                let mut other = binding.clone();
                mutate(&mut other);
                assert_eq!(
                    verify_value(&changed, ToolScope::Workspace, SERVER, &other, NOW).unwrap_err(),
                    CatalogRejection::BindingMismatch { field },
                );
            }
        }
        let mut equivalent = binding.clone();
        equivalent
            .credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .scopes = vec!["write".into(), "read".into(), "read".into()];
        assert!(verify_value(&value, ToolScope::Workspace, SERVER, &equivalent, NOW).is_ok());
    }

    #[test]
    fn replacing_account_a_with_b_rejects_a_in_both_directions() {
        let (dir, a, _) = fixture(ToolScope::Workspace);
        let mut b = a.clone();
        b.credential_context.static_generation = Some(generation("static", 8));
        b.credential_context
            .oauth
            .signed_in_mut()
            .unwrap()
            .generation
            .counter += 1;
        let check = |expected: &CatalogBinding| match verify(
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            expected,
            NOW,
        ) {
            CatalogVerdict::Rejected(reason) => assert_eq!(
                reason,
                CatalogRejection::BindingMismatch {
                    field: "credential"
                }
            ),
            other => panic!("cross-account record accepted: {other:?}"),
        };
        check(&b);
        record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            capture(&b),
        )
        .unwrap();
        assert!(matches!(
            verify(dir.path(), ToolScope::Workspace, SERVER, &b, NOW),
            CatalogVerdict::Verified(_)
        ));
        check(&a);
    }

    #[test]
    fn project_identity_server_identity_and_no_workspace_fallback() {
        let path = Path::new("/canonical/project");
        let scope = ToolScope::Project(path);
        let (dir, project, value) = fixture(scope);
        assert!(verify_value(&value, ToolScope::Workspace, SERVER, &project, NOW).is_err());
        assert!(verify_value(&value, scope, "another-server", &project, NOW).is_err());
        for id in [None, Some(String::new()), Some("project-b".into())] {
            let mut other = project.clone();
            other.project_id = id;
            assert!(verify_value(&value, scope, SERVER, &other, NOW).is_err());
        }
        let workspace = binding(ToolScope::Workspace);
        record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            capture(&workspace),
        )
        .unwrap();
        remove_server(dir.path(), scope, SERVER).unwrap();
        assert!(matches!(
            verify(dir.path(), scope, SERVER, &project, NOW),
            CatalogVerdict::Rejected(CatalogRejection::Missing)
        ));
        assert!(matches!(
            verify(dir.path(), ToolScope::Workspace, SERVER, &workspace, NOW),
            CatalogVerdict::Verified(_)
        ));
    }

    #[test]
    fn disagreeing_capture_or_missing_negotiation_never_overwrites() {
        let (dir, binding, _) = fixture(ToolScope::Workspace);
        let original = std::fs::read(store_path(dir.path())).unwrap();
        let mut after = binding.clone();
        after.credential_context.static_generation = Some(generation("static", 8));
        let mut witness = capture(&binding);
        witness.after = &after;
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            witness
        )
        .is_err());
        let mut response = catalog();
        response.protocol_version = None;
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &response,
            capture(&binding)
        )
        .is_err());
        let mut witness = capture(&binding);
        witness.origin.endpoint.clear();
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            witness
        )
        .is_err());
        assert_eq!(std::fs::read(store_path(dir.path())).unwrap(), original);
    }

    #[test]
    fn malformed_entries_survive_unrelated_writes_and_corrupt_files_are_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let binding = binding(ToolScope::Workspace);
        let raw = json!({"workspace": {"legacy": [{"name": "old"}], "broken": {"tools": "invalid"}}, "projects": {}});
        std::fs::write(store_path(dir.path()), serde_json::to_vec(&raw).unwrap()).unwrap();
        record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            capture(&binding),
        )
        .unwrap();
        let stored: Value =
            serde_json::from_slice(&std::fs::read(store_path(dir.path())).unwrap()).unwrap();
        assert_eq!(stored["workspace"]["legacy"], raw["workspace"]["legacy"]);
        assert_eq!(stored["workspace"]["broken"], raw["workspace"]["broken"]);
        for name in ["legacy", "broken"] {
            assert!(matches!(
                verify(dir.path(), ToolScope::Workspace, name, &binding, NOW),
                CatalogVerdict::Rejected(CatalogRejection::Unreadable)
            ));
        }
        std::fs::write(store_path(dir.path()), b"{corrupt").unwrap();
        assert!(matches!(
            verify(dir.path(), ToolScope::Workspace, SERVER, &binding, NOW),
            CatalogVerdict::Rejected(CatalogRejection::Unreadable)
        ));
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            capture(&binding)
        )
        .is_err());
        assert!(remove_server(dir.path(), ToolScope::Workspace, SERVER).is_err());
        assert_eq!(std::fs::read(store_path(dir.path())).unwrap(), b"{corrupt");
    }

    #[test]
    fn a_cache_hit_needs_a_protocol_the_host_can_still_negotiate() {
        let (dir, binding, _) = fixture(ToolScope::Workspace);
        let context = context_of(&binding);
        let verdict = |supported: &[&str]| {
            let supported: Vec<String> = supported.iter().map(|p| p.to_string()).collect();
            verify_catalog(
                dir.path(),
                ToolScope::Workspace,
                SERVER,
                &context,
                &supported,
                NOW,
            )
        };
        assert!(matches!(
            verdict(&["2024-11-05", "2025-11-25"]),
            CatalogVerdict::Verified(_)
        ));
        // Dropped support, or a host that cannot capture at all, never reuses it.
        for supported in [&["2024-11-05"][..], &[][..]] {
            assert!(matches!(
                verdict(supported),
                CatalogVerdict::Rejected(CatalogRejection::BindingMismatch { field: "protocol" })
            ));
        }
        // The context fixes every other dimension: a supported protocol does not
        // rescue a credential change.
        let mut changed = context.clone();
        changed.credential_context.static_generation = Some(generation("static", 8));
        assert!(matches!(
            verify_catalog(
                dir.path(),
                ToolScope::Workspace,
                SERVER,
                &changed,
                &["2025-11-25".to_string()],
                NOW,
            ),
            CatalogVerdict::Rejected(CatalogRejection::BindingMismatch {
                field: "credential"
            })
        ));
    }

    #[test]
    fn inspection_reports_provenance_without_tools() {
        let (dir, _, _) = fixture(ToolScope::Workspace);
        let inspection = inspect_catalog(dir.path(), ToolScope::Workspace, SERVER);
        let CatalogInspection::Recorded {
            tool_count, origin, ..
        } = &inspection
        else {
            panic!("expected a recorded catalog, got {inspection:?}");
        };
        assert_eq!((*tool_count, origin.transport.as_str()), (1, "http"));
        let rendered = serde_json::to_string(&inspection).unwrap();
        assert!(!rendered.contains("lookup") && !rendered.contains("Untrusted description"));
        assert_eq!(
            inspect_catalog(dir.path(), ToolScope::Workspace, "absent"),
            CatalogInspection::Missing
        );
        let raw = json!({"workspace": {"legacy": [{"name": "old"}]}, "projects": {}});
        std::fs::write(store_path(dir.path()), serde_json::to_vec(&raw).unwrap()).unwrap();
        assert_eq!(
            inspect_catalog(dir.path(), ToolScope::Workspace, "legacy"),
            CatalogInspection::Unreadable
        );
    }

    #[test]
    fn a_capture_that_began_before_a_withdrawal_cannot_publish_after_it() {
        let (dir, binding, _) = fixture(ToolScope::Workspace);
        let before = withdrawal_epoch(dir.path(), ToolScope::Workspace, SERVER).unwrap();
        assert_eq!(before, 0);
        remove_server(dir.path(), ToolScope::Workspace, SERVER).unwrap();
        // Removing an absent row still counts: it is what refuses an in-flight
        // capture that has no row yet.
        remove_server(dir.path(), ToolScope::Workspace, SERVER).unwrap();
        assert_eq!(
            withdrawal_epoch(dir.path(), ToolScope::Workspace, SERVER).unwrap(),
            2
        );
        let mut stale = capture(&binding);
        stale.withdrawal_epoch = before;
        assert!(record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            stale
        )
        .is_err());
        assert_eq!(
            inspect_catalog(dir.path(), ToolScope::Workspace, SERVER),
            CatalogInspection::Missing
        );
        // Withdrawal counts are per scoped server.
        assert_eq!(
            withdrawal_epoch(dir.path(), ToolScope::Workspace, "other").unwrap(),
            0
        );
        let mut current = capture(&binding);
        current.withdrawal_epoch = 2;
        record_catalog(
            &lock(),
            dir.path(),
            ToolScope::Workspace,
            SERVER,
            &catalog(),
            current,
        )
        .unwrap();
    }

    #[test]
    fn the_context_binding_is_the_recorded_binding() {
        let binding = binding(ToolScope::Workspace);
        assert_eq!(context_of(&binding).binding("2025-11-25"), binding);
    }

    #[test]
    fn withdrawal_is_local_not_a_claim_of_remote_liveness() {
        let (dir, binding, mut value) = fixture(ToolScope::Workspace);
        // No network observation occurs on verify: this remains an as-of record.
        assert!(matches!(
            verify(dir.path(), ToolScope::Workspace, SERVER, &binding, NOW),
            CatalogVerdict::Verified(_)
        ));
        value["withdrawn"] = json!("locally removed");
        reject(&value, &binding, CatalogRejection::Withdrawn);
        remove_server(dir.path(), ToolScope::Workspace, SERVER).unwrap();
        assert!(matches!(
            verify(dir.path(), ToolScope::Workspace, SERVER, &binding, NOW),
            CatalogVerdict::Rejected(CatalogRejection::Missing)
        ));
        remove_server(dir.path(), ToolScope::Workspace, SERVER).unwrap();
    }
}
