//! Writes to `cairn://models/clients`: minting and importing served-API client
//! keys, granting or withdrawing remote use, and revoking a key.
//!
//! Minting, importing and granting remote use expand who may spend this
//! workspace's models, so they cross the `servedApi` settings boundary and ask
//! the operator when an agent writes them.
//!
//! Withdrawing remote use and revoking a key only narrow authority, but they
//! take it from another principal, the client. Withdrawing remote use revokes
//! grants the client holds, so it follows the holder rule every grant revoke
//! does (`authorization::revoke`). Revoking the client is the operator's
//! alone and is refused here for every caller: nothing that arrives over the
//! agent callback can prove it comes from the operator, so the operator's door
//! is `revoke_served_api_client`, behind Settings → Served API.

use cairn_common::authorization::{
    ActDigest, AuthorityDecision, AuthorityGrant, AuthorityProvenance, AuthorityRequest,
};
use cairn_uri::{parse_uri, CairnResource};
use serde_json::Value;

use super::{build_failure, ResourceMutationResult};
use crate::authorization::revoke::{check_holder, Revoker};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::served_api::clients::{self, ImportedKey};
use crate::orchestrator::Orchestrator;

/// One client change, parsed from a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientChange {
    Mint {
        name: String,
        remote: bool,
    },
    Import {
        origin: String,
        keys: Vec<(ImportedKey, bool)>,
    },
    SetRemote {
        id: String,
        remote: bool,
    },
    Revoke {
        id: String,
    },
}

impl ClientChange {
    /// The boundary the change crosses, when it widens authority.
    pub(crate) fn authority(&self) -> Option<AuthorityRequest> {
        let widens = match self {
            Self::Mint { .. } | Self::Import { .. } => true,
            Self::SetRemote { remote, .. } => *remote,
            Self::Revoke { .. } => false,
        };
        if !widens {
            return None;
        }
        let mut request = crate::authorization::normalize::workspace_settings_write(
            crate::authorization::WORKSPACE_ID,
            "servedApi",
        )
        .ok()?;
        request.summary = match self {
            Self::Mint { name, remote } => format!(
                "mint a served-API key for client {name}{}",
                if *remote { " with remote use" } else { "" }
            ),
            Self::Import { keys, origin } => format!(
                "import {} served-API client key(s) from {origin}",
                keys.len()
            ),
            Self::SetRemote { id, .. } => {
                format!("grant served-API client {id} use of remote providers")
            }
            Self::Revoke { .. } => unreachable!("a revoke crosses no boundary"),
        };
        Some(request.with_act(self.act()))
    }

    /// The act a widening change performs: the client and remote use it mints
    /// or grants, or every key it imports, so a Once approval of one is not
    /// spent by another.
    fn act(&self) -> ActDigest {
        ActDigest::of("served_api.client", |encoder| match self {
            Self::Mint { name, remote } => {
                encoder.field("change", "mint");
                encoder.field("name", name);
                encoder.field("remote", &remote.to_string());
            }
            Self::Import { origin, keys } => {
                encoder.field("change", "import");
                encoder.field("origin", origin);
                encoder.field("keys", &keys.len().to_string());
                for (index, (key, remote)) in keys.iter().enumerate() {
                    encoder.field(&format!("keys[{index}].name"), &key.name);
                    encoder.field(&format!("keys[{index}].keyHash"), &key.key_hash);
                    encoder.field(&format!("keys[{index}].remote"), &remote.to_string());
                }
            }
            Self::SetRemote { id, remote } => {
                encoder.field("change", "set_remote");
                encoder.field("id", id);
                encoder.field("remote", &remote.to_string());
            }
            Self::Revoke { id } => {
                encoder.field("change", "revoke");
                encoder.field("id", id);
            }
        })
    }
}

fn flag(payload: &Value, key: &str) -> Result<Option<bool>, String> {
    match payload.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(format!("payload.{key} must be true or false")),
    }
}

fn text<'a>(payload: &'a Value, key: &str) -> Option<&'a str> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

/// The change `item` asks for, when it targets the clients resource.
pub(crate) fn parse_client_change(item: &ChangeItem) -> Option<Result<ClientChange, String>> {
    let resource = parse_uri(&item.target)?;
    let empty = Value::Null;
    let payload = item.payload.as_ref().unwrap_or(&empty);
    Some(match (resource, item.mode) {
        (CairnResource::ModelsClients, ChangeMode::Append) => parse_append(payload),
        (CairnResource::ModelsClient { id }, ChangeMode::Patch) => {
            match (flag(payload, "revoke"), flag(payload, "remote")) {
                (Err(error), _) | (_, Err(error)) => Err(error),
                (Ok(Some(true)), Ok(None)) => Ok(ClientChange::Revoke { id }),
                (Ok(None | Some(false)), Ok(Some(remote))) => {
                    Ok(ClientChange::SetRemote { id, remote })
                }
                (Ok(Some(true)), Ok(Some(_))) => {
                    Err("patch revoke or remote, not both".to_string())
                }
                _ => Err("payload needs remote:true|false or revoke:true".to_string()),
            }
        }
        (CairnResource::ModelsClients | CairnResource::ModelsClient { .. }, _) => Err(
            "cairn://models/clients takes append {name} or {import}; a client takes patch {remote} or {revoke}"
                .to_string(),
        ),
        _ => return None,
    })
}

fn parse_append(payload: &Value) -> Result<ClientChange, String> {
    let remote = flag(payload, "remote")?.unwrap_or(false);
    match (text(payload, "name"), payload.get("import")) {
        (Some(name), None) => Ok(ClientChange::Mint {
            name: name.to_string(),
            remote,
        }),
        (None, Some(Value::Array(entries))) => {
            let mut keys = Vec::new();
            for entry in entries {
                let name = text(entry, "name")
                    .ok_or("each import entry needs a name")?
                    .to_string();
                let key_hash = text(entry, "keyHash")
                    .ok_or_else(|| format!("import entry {name} needs a keyHash"))?
                    .to_string();
                let remote = flag(entry, "remote")?.unwrap_or(false);
                keys.push((ImportedKey { name, key_hash }, remote));
            }
            Ok(ClientChange::Import {
                origin: text(payload, "origin").unwrap_or("bgllm").to_string(),
                keys,
            })
        }
        (Some(_), Some(_)) => Err("append either name (mint) or import, not both".into()),
        (None, Some(_)) => {
            Err("payload.import must be an array of {name, keyHash, remote?}".into())
        }
        (None, None) => Err("payload needs name (mint a key) or import (keep keys by hash)".into()),
    }
}

/// The boundary a clients write crosses, for the batch's authority gate. A
/// widening change dispatch would refuse is refused here instead, so no card
/// is raised for it.
pub(crate) async fn models_client_authority(
    orch: &Orchestrator,
    item: &ChangeItem,
) -> Option<Result<AuthorityRequest, String>> {
    let change = match parse_client_change(item)? {
        Ok(change) => change,
        Err(error) => return Some(Err(error)),
    };
    let authority = change.authority()?;
    Some(widening_refusal(orch, &change).await.map(|()| authority))
}

/// What refuses a widening change without the operator: a malformed name or
/// key, remote use with no OpenRouter account to spend, or remote use for a
/// client that is not live. Dispatch meets each again on the way to the write.
async fn widening_refusal(orch: &Orchestrator, change: &ClientChange) -> Result<(), String> {
    let wants_remote = match change {
        ClientChange::Mint { name, remote } => {
            clients::validate_name(name)?;
            *remote
        }
        ClientChange::Import { keys, .. } => {
            clients::validate_import(keys.iter().map(|(key, _)| key))?;
            keys.iter().any(|(_, remote)| *remote)
        }
        ClientChange::SetRemote { id, remote: true } => {
            live_client(orch, id).await?;
            true
        }
        ClientChange::SetRemote { remote: false, .. } | ClientChange::Revoke { .. } => false,
    };
    if wants_remote {
        remote_account(orch)?;
    }
    Ok(())
}

/// What minted a remote grant: a write to the clients resource, already past
/// the `servedApi` boundary's own authorization.
fn provenance(_request: &McpCallbackRequest) -> AuthorityProvenance {
    AuthorityProvenance {
        issuer: "served_api_client".to_string(),
        decision_actor: None,
        appearance_snapshot: None,
        approver: None,
        request_uri: None,
        node_uri: None,
        rationale: None,
    }
}

/// The workspace's OpenRouter account, which remote use spends.
fn remote_account(orch: &Orchestrator) -> Result<String, String> {
    crate::backends::openrouter::openrouter_api_account(orch)
        .map(|(account, _)| account)
        .ok_or_else(|| "remote use spends the workspace's OpenRouter account, and none is configured; add one at cairn://models/accounts first".to_string())
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    _resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let Some(change) = parse_client_change(item) else {
        return Ok(None);
    };
    let fail = |error: String| build_failure(index, item, error);
    let change = change.map_err(fail)?;
    if let ClientChange::Revoke { id } = &change {
        return Err(fail(revoke_refusal(id)));
    }
    if dry_run {
        if let ClientChange::SetRemote { id, remote: false } = &change {
            withdrawal(orch, request, id).await.map_err(fail)?;
        }
        return Ok(Some(match change.authority() {
            Some(authority) => format!(
                "Would {} (scope {})",
                authority.summary,
                authority.scope.shorthand()
            ),
            None => format!("Would apply {change:?}"),
        }));
    }
    if let Some(authority) = change.authority() {
        let Some(actor) = crate::authorization::resolve_actor(orch, request).await else {
            return Err(fail(format!(
                "Denied: only an authenticated run can be authorized to {}.",
                authority.summary
            )));
        };
        let decision = crate::authorization::authorize(&actor, &authority)
            .await
            .map_err(fail)?;
        if let AuthorityDecision::ApprovalRequired(reason) | AuthorityDecision::Forbidden(reason) =
            decision
        {
            return Err(fail(crate::authorization::refusal_message(
                &authority, reason,
            )));
        }
    }
    let db = &orch.db.local;
    let summary = match change {
        ClientChange::Mint { name, remote } => {
            let account = if remote {
                Some(remote_account(orch).map_err(fail)?)
            } else {
                None
            };
            let (client, key) = clients::mint(db, &name).await.map_err(fail)?;
            crate::security::broker::backend::register_injected(
                clients::API_ISSUER,
                &client.id,
                &key,
            );
            if let Some(account) = account {
                clients::grant_remote(db, &client, "openrouter", &account, provenance(request))
                    .await
                    .map_err(fail)?;
            }
            // The key is scrubbed from every transcript and reply, so it
            // reaches the operator through a file only they can read.
            let path = clients::write_key_file(&orch.config_dir, &client, &key).map_err(fail)?;
            format!(
                "Minted client {} ({}){}. Cairn keeps only its hash; the key itself is in {} (readable only by this account), to hand to the client and then delete. Clients send it as `Authorization: Bearer <key>`. Manage it at cairn://models/clients/{}.",
                client.name,
                client.id,
                if remote { ", with remote use" } else { ", local-only" },
                path.display(),
                client.id
            )
        }
        ClientChange::Import { origin, keys } => {
            let wants_remote = keys.iter().any(|(_, remote)| *remote);
            let account = if wants_remote {
                Some(remote_account(orch).map_err(fail)?)
            } else {
                None
            };
            let remote: Vec<String> = keys
                .iter()
                .filter(|(_, remote)| *remote)
                .map(|(key, _)| key.name.trim().to_string())
                .collect();
            let imported =
                clients::import(db, &origin, keys.into_iter().map(|(key, _)| key).collect())
                    .await
                    .map_err(fail)?;
            if let Some(account) = account {
                for client in imported
                    .iter()
                    .filter(|client| remote.contains(&client.name))
                {
                    clients::grant_remote(db, client, "openrouter", &account, provenance(request))
                        .await
                        .map_err(fail)?;
                }
            }
            format!(
                "Imported {} client(s) from {origin}: {}",
                imported.len(),
                imported
                    .iter()
                    .map(|client| format!(
                        "{} ({}{})",
                        client.name,
                        client.id,
                        if remote.contains(&client.name) {
                            ", remote"
                        } else {
                            ""
                        }
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        ClientChange::SetRemote { id, remote: true } => {
            let client = live_client(orch, &id).await.map_err(fail)?;
            let account = remote_account(orch).map_err(fail)?;
            let grant =
                clients::grant_remote(db, &client, "openrouter", &account, provenance(request))
                    .await
                    .map_err(fail)?;
            format!(
                "Client {} may use OpenRouter account {account} (grant {}).",
                client.name, grant.id
            )
        }
        ClientChange::SetRemote { id, remote: false } => {
            let (client, grants, revoker) = withdrawal(orch, request, &id).await.map_err(fail)?;
            for grant in &grants {
                crate::authorization::revoke_grant(db, &grant.id, Some(&revoker.label()))
                    .await
                    .map_err(fail)?;
            }
            format!(
                "Client {} is local-only ({} grant(s) revoked).",
                client.name,
                grants.len()
            )
        }
        ClientChange::Revoke { id } => return Err(fail(revoke_refusal(&id))),
    };
    Ok(Some(summary))
}

fn revoke_refusal(id: &str) -> String {
    format!(
        "Refused: revoking served-API client '{id}' cuts off its key and revokes the grants it \
         holds, so only the operator may do it, at {}. A write to cairn://models/clients cannot \
         prove it comes from the operator.",
        clients::OPERATOR_CLIENT_SURFACE
    )
}

/// The live client `id`, the remote grants withdrawing its remote use would
/// revoke, and who is revoking them, once the caller is shown to hold every
/// one of those grants. They are bound to the client, so in practice only the
/// client itself holds them; the operator revokes them at Settings → Grants.
async fn withdrawal(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    id: &str,
) -> Result<
    (
        cairn_db::storage::served_api::ServedApiClient,
        Vec<AuthorityGrant>,
        Revoker,
    ),
    String,
> {
    let client = live_client(orch, id).await?;
    let grants = clients::remote_grants(&orch.db.local, &client).await?;
    let revoker = Revoker::resolve(orch, request).await;
    for grant in &grants {
        check_holder(&grant.id, grant, &revoker).map_err(|refusal| {
            format!(
                "Withdrawing client {}'s remote use revokes the grants it holds. {refusal}",
                client.name
            )
        })?;
    }
    Ok((client, grants, revoker))
}

async fn live_client(
    orch: &Orchestrator,
    id: &str,
) -> Result<cairn_db::storage::served_api::ServedApiClient, String> {
    match clients::find(&orch.db.local, id).await? {
        Some(client) if client.revoked_at.is_none() => Ok(client),
        Some(_) => Err(format!("client {id} is revoked")),
        None => Err(format!(
            "no served-API client {id}; clients are listed at cairn://models/clients"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(target: &str, mode: ChangeMode, payload: Value) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode,
            payload: Some(payload),
        }
    }

    const HASH: &str = "ab";

    fn key_hash() -> String {
        HASH.repeat(32)
    }

    #[tokio::test]
    async fn widening_changes_cross_the_served_api_boundary_and_narrowing_ones_do_not() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let mint = item(
            "cairn://models/clients",
            ChangeMode::Append,
            serde_json::json!({"name": "dig"}),
        );
        let authority = models_client_authority(&orch, &mint)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            authority.scope.shorthand(),
            "workspace/default/settings/servedApi:write"
        );
        let import = item(
            "cairn://models/clients",
            ChangeMode::Append,
            serde_json::json!({"import": [{"name": "sitrep", "keyHash": key_hash()}]}),
        );
        assert!(models_client_authority(&orch, &import)
            .await
            .unwrap()
            .is_ok());
        for narrowing in [
            serde_json::json!({"remote": false}),
            serde_json::json!({"revoke": true}),
        ] {
            let change = item("cairn://models/clients/cli_1", ChangeMode::Patch, narrowing);
            assert!(models_client_authority(&orch, &change).await.is_none());
        }
        let bad = item(
            "cairn://models/clients",
            ChangeMode::Append,
            serde_json::json!({}),
        );
        assert!(models_client_authority(&orch, &bad).await.unwrap().is_err());
        let unrelated = item(
            "cairn://models/sessions/s1",
            ChangeMode::Patch,
            serde_json::json!({"action": "stop"}),
        );
        assert!(models_client_authority(&orch, &unrelated).await.is_none());
    }

    /// A widening change dispatch would refuse is refused while the batch is
    /// prepared, so no card is raised for it.
    #[tokio::test]
    async fn a_widening_change_that_cannot_land_is_refused_before_any_card() {
        let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
        let cases = [
            (
                "cairn://models/clients",
                ChangeMode::Append,
                serde_json::json!({"name": "bad name!"}),
                "may use only letters",
            ),
            (
                "cairn://models/clients",
                ChangeMode::Append,
                serde_json::json!({"import": [{"name": "sitrep", "keyHash": HASH}]}),
                "keyHash must be a hex SHA-256 digest",
            ),
            (
                "cairn://models/clients",
                ChangeMode::Append,
                serde_json::json!({"import": [
                    {"name": "sitrep", "keyHash": key_hash()},
                    {"name": "sitrep", "keyHash": "cd".repeat(32)}
                ]}),
                "appears twice in the import",
            ),
            (
                "cairn://models/clients",
                ChangeMode::Append,
                serde_json::json!({"name": "dig", "remote": true}),
                "none is configured",
            ),
            (
                "cairn://models/clients/cli_1",
                ChangeMode::Patch,
                serde_json::json!({"remote": true}),
                "no served-API client cli_1",
            ),
        ];
        for (target, mode, payload, says) in cases {
            let error = models_client_authority(&orch, &item(target, mode, payload.clone()))
                .await
                .expect("a widening change")
                .expect_err("refused before any card");
            assert!(error.contains(says), "{payload}: {error}");
        }
    }
}
