//! Served-API client keys, and the principal and remote grants a key stands
//! for.
//!
//! A key authenticates a client record; it is not the client (tenet 1334). The
//! record's immutable id names the client's `External` principal, its name is
//! an alias for people, and the key is kept only as `hex(sha256(trimmed key))`,
//! bgllm's scheme, so a key imported by hash keeps working unchanged. Local
//! models need only a live key. A remote provider needs a standing grant on
//! that provider's account bound to the client's principal.

use std::sync::Arc;

use cairn_common::authorization::{
    AuthorityAction, AuthorityAudience, AuthorityConstraintSet, AuthorityGrant, AuthorityLifetime,
    AuthorityLifetimeKind, AuthorityMutation, AuthorityPlace, AuthorityPrincipal,
    AuthorityProvenance, AuthorityRequest, AuthorityScope,
};
use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, CredentialRef,
    PrincipalRef, VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
};
use cairn_db::storage::served_api::{self as store, ServedApiClient};
use sha2::{Digest, Sha256};

use crate::authorization::{self, WORKSPACE_ID};
use crate::storage::LocalDb;

/// The principal provider and verification issuer every served-API client
/// carries.
pub const API_ISSUER: &str = "cairn-api";
/// The address service the models API appears through.
pub const MODELS_SERVICE: &str = "models";
/// The prefix of a key Cairn mints.
pub const KEY_PREFIX: &str = "cak-";

fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The stored form of a key: lowercase hex SHA-256 of the trimmed key.
pub fn key_hash(key: &str) -> String {
    hex(&Sha256::digest(key.trim().as_bytes()))
}

/// A hash as an import supplies it, lowercased, or why it is not one.
pub fn imported_hash(hash: &str) -> Result<String, String> {
    let hash = hash.trim().to_ascii_lowercase();
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(hash)
    } else {
        Err("keyHash must be a hex SHA-256 digest (64 hex characters)".into())
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (l, r)| difference | (l ^ r))
            == 0
}

fn new_client_id() -> String {
    format!("cli_{}", hex(&rand::random::<[u8; 8]>()))
}

fn validate_name(name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() || name.len() > 64 {
        return Err("a client name is 1 to 64 characters".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(format!(
            "client name {name:?} may use only letters, digits, '-', '_' and '.'"
        ));
    }
    Ok(name.to_string())
}

/// The client's principal: stable ids only, never its name.
pub fn principal(client: &ServedApiClient) -> PrincipalRef {
    PrincipalRef::External {
        provider: API_ISSUER.to_string(),
        namespace: client.workspace_id.clone(),
        id: client.id.clone(),
    }
}

/// How the client appeared on one request: its principal, through the models
/// API, verified by its own credential. The credential ref names the record,
/// never the key or its hash.
pub fn appearance(client: &ServedApiClient, at: i64) -> Result<AppearanceSnapshot, String> {
    let verification = VerificationRecord::new(
        VerificationMethod::ApiClientCredential,
        VerificationStatus::Verified,
        Some(API_ISSUER.to_string()),
        Some(client.id.clone()),
        None,
        Some(
            CredentialRef::new(format!("served-api-client:{}", client.id))
                .map_err(|error| error.to_string())?,
        ),
        VerificationStrength::new("client-key").map_err(|error| error.to_string())?,
        at,
    )
    .map_err(|error| error.to_string())?;
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::ServedApi,
        Address::ServedApi {
            service: MODELS_SERVICE.to_string(),
        },
        verification,
        at,
        None,
    )
    .map_err(|error| error.to_string())?;
    AppearanceSnapshot::new(principal(client), evidence, Vec::new(), None)
        .map_err(|error| error.to_string())
}

/// The live client a presented key belongs to. Every live hash is compared in
/// constant time, so the lookup takes the same time whichever key matched.
pub async fn authenticate(
    db: &LocalDb,
    presented: &str,
) -> Result<Option<ServedApiClient>, String> {
    let presented = presented.trim();
    if presented.is_empty() {
        return Ok(None);
    }
    let hash = key_hash(presented);
    let clients = store::list_served_api_clients(db, WORKSPACE_ID)
        .await
        .map_err(|error| format!("could not read served-API clients: {error}"))?;
    let mut found = None;
    for client in clients {
        let live = client.revoked_at.is_none();
        if constant_time_eq(client.key_hash.as_bytes(), hash.as_bytes()) && live {
            found = Some(client);
        }
    }
    Ok(found)
}

pub async fn list(db: &LocalDb) -> Result<Vec<ServedApiClient>, String> {
    store::list_served_api_clients(db, WORKSPACE_ID)
        .await
        .map_err(|error| format!("could not read served-API clients: {error}"))
}

pub async fn find(db: &LocalDb, id: &str) -> Result<Option<ServedApiClient>, String> {
    Ok(list(db).await?.into_iter().find(|client| client.id == id))
}

fn insert_error(error: impl std::fmt::Display) -> String {
    let text = error.to_string();
    if text.contains("UNIQUE") || text.contains("unique") {
        "that key is already registered, or a live key already has that client name".into()
    } else {
        format!("could not record the client: {text}")
    }
}

/// Mint a key for a new client named `name`. The key is returned once and
/// never stored.
pub async fn mint(db: &LocalDb, name: &str) -> Result<(ServedApiClient, String), String> {
    let name = validate_name(name)?;
    let key = format!("{KEY_PREFIX}{}", hex(&rand::random::<[u8; 24]>()));
    let client = ServedApiClient {
        id: new_client_id(),
        workspace_id: WORKSPACE_ID.to_string(),
        name,
        key_hash: key_hash(&key),
        key_hint: Some(key[key.len() - 4..].to_string()),
        source: "minted".to_string(),
        created_at: now_secs(),
        revoked_at: None,
        revoked_by: None,
    };
    store::insert_served_api_clients(db, vec![client.clone()])
        .await
        .map_err(insert_error)?;
    Ok((client, key))
}

/// Where a minted key is handed to the operator: an owner-only file under the
/// config directory, `served-api/keys/<client id>.key`.
///
/// Not the write's result: the key is registered with the scrubber, which
/// redacts it in every transcript and streamed reply, so a key returned there
/// would reach only the model. The file is the operator's to read, hand to
/// the client, and delete.
pub fn write_key_file(
    config_dir: &std::path::Path,
    client: &ServedApiClient,
    key: &str,
) -> Result<std::path::PathBuf, String> {
    use std::io::Write as _;
    let dir = config_dir.join("served-api").join("keys");
    std::fs::create_dir_all(&dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    let path = dir.join(format!("{}.key", client.id));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    writeln!(file, "{key}").map_err(|error| format!("write {}: {error}", path.display()))?;
    Ok(path)
}

/// One client to import by its key's hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportedKey {
    pub name: String,
    pub key_hash: String,
}

/// Import clients by key hash, all or none. `origin` names where the keys
/// were issued (`bgllm`).
pub async fn import(
    db: &LocalDb,
    origin: &str,
    keys: Vec<ImportedKey>,
) -> Result<Vec<ServedApiClient>, String> {
    if keys.is_empty() {
        return Err("import names no key".into());
    }
    let mut clients = Vec::new();
    for key in keys {
        let name = validate_name(&key.name)?;
        let hash = imported_hash(&key.key_hash).map_err(|error| format!("{name}: {error}"))?;
        if clients
            .iter()
            .any(|client: &ServedApiClient| client.key_hash == hash || client.name == name)
        {
            return Err(format!("{name} appears twice in the import"));
        }
        clients.push(ServedApiClient {
            id: new_client_id(),
            workspace_id: WORKSPACE_ID.to_string(),
            name,
            key_hash: hash,
            key_hint: None,
            source: format!("imported:{origin}"),
            created_at: now_secs(),
            revoked_at: None,
            revoked_by: None,
        });
    }
    store::insert_served_api_clients(db, clients.clone())
        .await
        .map_err(insert_error)?;
    Ok(clients)
}

/// Revoke `client`'s key and every remote grant bound to it.
pub async fn revoke(db: &LocalDb, client: &ServedApiClient, by: &str) -> Result<bool, String> {
    let revoked = store::revoke_served_api_client(db, &client.id, now_secs(), by)
        .await
        .map_err(|error| format!("could not revoke the client: {error}"))?;
    for grant in remote_grants(db, client).await? {
        authorization::revoke_grant(db, &grant.id, Some(by)).await?;
    }
    Ok(revoked)
}

/// The authority remote use spends: running on `provider`'s `account_id`.
pub fn remote_scope(provider: &str, account_id: &str) -> AuthorityScope {
    AuthorityScope::new(
        AuthorityPlace::ExternalAccount {
            provider: provider.to_string(),
            account_id: account_id.to_string(),
        },
        AuthorityAction::Run,
    )
}

/// The live standing grants bound to `client`.
pub async fn remote_grants(
    db: &LocalDb,
    client: &ServedApiClient,
) -> Result<Vec<AuthorityGrant>, String> {
    let principal = principal(client);
    let now = now_secs();
    Ok(authorization::list_grants(db, 10_000)
        .await?
        .into_iter()
        .filter(|grant| {
            matches!(grant.lifetime, AuthorityLifetime::Standing)
                && grant.principal.actor.as_ref() == Some(&principal)
                && !grant.is_spent(now)
        })
        .collect())
}

/// Grant `client` remote use of `provider`'s `account_id`: a standing grant
/// on that account bound to the client's principal.
pub async fn grant_remote(
    db: &LocalDb,
    client: &ServedApiClient,
    provider: &str,
    account_id: &str,
    provenance: AuthorityProvenance,
) -> Result<AuthorityGrant, String> {
    let scope = remote_scope(provider, account_id);
    if let Some(existing) = remote_grants(db, client)
        .await?
        .into_iter()
        .find(|grant| grant.scope == scope)
    {
        return Ok(existing);
    }
    authorization::issue_grant(
        db,
        authorization::GrantIssue {
            request: AuthorityRequest::new(
                scope,
                AuthorityMutation::Update,
                format!("served-API client {} uses {provider}", client.name),
            ),
            principal: AuthorityPrincipal {
                actor: Some(principal(client)),
                ..Default::default()
            },
            audience: AuthorityAudience::workspace(WORKSPACE_ID),
            lifetime: AuthorityLifetimeKind::Standing,
            request_id: None,
            turn_id: None,
            session_id: None,
            expires_at: None,
            provenance,
            constraints: AuthorityConstraintSet::default(),
        },
    )
    .await
}

/// The actor a client's requests are authorized as.
pub fn actor(db: Arc<LocalDb>, client: &ServedApiClient) -> authorization::AuthorityActor {
    authorization::served_api_actor(db, principal(client))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bgllm's `hash_key`: lowercase hex SHA-256 of the UTF-8 key, here
    /// checked against the published SHA-256 test vector for "abc". The
    /// presented key is trimmed first, as bgllm's proxy trims it.
    #[test]
    fn the_hash_is_bgllms_and_ignores_surrounding_space() {
        assert_eq!(
            key_hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(key_hash(" abc \n"), key_hash("abc"));
    }

    #[test]
    fn an_imported_hash_is_64_hex_and_lowercased() {
        assert_eq!(imported_hash(&"AB".repeat(32)).unwrap(), "ab".repeat(32));
        assert!(imported_hash("abc").is_err());
        assert!(imported_hash(&"zz".repeat(32)).is_err());
    }

    #[tokio::test]
    async fn a_minted_or_imported_key_authenticates_until_revoked() {
        let db = crate::storage::migrated_test_db("served-api-auth.db").await;
        let (dig, key) = mint(&db, "dig").await.unwrap();
        assert!(key.starts_with(KEY_PREFIX) && key.len() == KEY_PREFIX.len() + 48);
        assert_eq!(dig.key_hint.as_deref(), Some(&key[key.len() - 4..]));
        let imported = import(
            &db,
            "bgllm",
            vec![ImportedKey {
                name: "sitrep".into(),
                key_hash: key_hash("sk-sitrep-old").to_uppercase(),
            }],
        )
        .await
        .unwrap();
        assert_eq!(imported[0].source, "imported:bgllm");
        assert_eq!(imported[0].key_hint, None);

        let found = authenticate(&db, &format!("  {key}\n"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id, dig.id);
        let found = authenticate(&db, "sk-sitrep-old").await.unwrap().unwrap();
        assert_eq!(found.name, "sitrep");
        assert!(authenticate(&db, "nope").await.unwrap().is_none());
        assert!(authenticate(&db, "").await.unwrap().is_none());

        // A duplicate import is refused whole.
        assert!(import(
            &db,
            "bgllm",
            vec![ImportedKey {
                name: "other".into(),
                key_hash: key_hash("sk-sitrep-old"),
            }],
        )
        .await
        .is_err());

        assert!(revoke(&db, &dig, "operator").await.unwrap());
        assert!(authenticate(&db, &key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn remote_use_is_a_standing_grant_bound_to_the_client() {
        let db = Arc::new(crate::storage::migrated_test_db("served-api-grant.db").await);
        let (dig, _) = mint(&db, "dig").await.unwrap();
        let (sitrep, _) = mint(&db, "sitrep").await.unwrap();
        let scope = remote_scope("openrouter", "acc_or");
        let decision = authorization::served_api_use(&actor(db.clone(), &dig), &scope)
            .await
            .unwrap();
        assert!(!decision.is_allowed());

        let grant = grant_remote(
            &db,
            &dig,
            "openrouter",
            "acc_or",
            AuthorityProvenance {
                issuer: "served_api_client".into(),
                decision_actor: None,
                appearance_snapshot: None,
                approver: None,
                request_uri: None,
                node_uri: None,
                rationale: None,
            },
        )
        .await
        .unwrap();
        let decision = authorization::served_api_use(&actor(db.clone(), &dig), &scope)
            .await
            .unwrap();
        assert_eq!(
            decision,
            cairn_common::authorization::AuthorityDecision::AllowedByGrant {
                grant_id: grant.id.clone(),
                reason: cairn_common::authorization::AuthorityReason::ServedApiRemoteUse,
            }
        );
        // Another client is not covered by dig's grant.
        assert!(
            !authorization::served_api_use(&actor(db.clone(), &sitrep), &scope)
                .await
                .unwrap()
                .is_allowed()
        );
        // Revoking the client revokes its grant.
        revoke(&db, &dig, "operator").await.unwrap();
        assert!(remote_grants(&db, &dig).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_minted_key_reaches_the_operator_through_an_owner_only_file() {
        let db = crate::storage::migrated_test_db("served-api-keyfile.db").await;
        let config = tempfile::tempdir().unwrap();
        let (client, key) = mint(&db, "open-webui").await.unwrap();
        let path = write_key_file(config.path(), &client, &key).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap().trim(), key);
        assert!(path.ends_with(format!("served-api/keys/{}.key", client.id)));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        // A second write never overwrites a key file.
        assert!(write_key_file(config.path(), &client, "other").is_err());
    }

    #[test]
    fn a_clients_appearance_names_its_record_not_its_key() {
        let client = ServedApiClient {
            id: "cli_0011223344556677".into(),
            workspace_id: "default".into(),
            name: "dig".into(),
            key_hash: key_hash("k"),
            key_hint: None,
            source: "minted".into(),
            created_at: 1,
            revoked_at: None,
            revoked_by: None,
        };
        let snapshot = appearance(&client, 5).unwrap();
        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("served-api-client:cli_0011223344556677"));
        assert!(!json.contains(&client.key_hash));
        assert!(!json.contains("dig"));
    }
}
