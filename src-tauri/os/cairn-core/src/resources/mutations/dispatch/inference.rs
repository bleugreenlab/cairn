//! The inference facet's writes. The records': the whole inventory replaced
//! (`replace cairn://inference`), records added, replaced and removed
//! (`patch {upsert, remove}`), or the records mirrored from bginf adopted
//! (`patch {adopt: "bginf"}`), each decided by `orchestrator::inference`. The
//! pool controller's: a pool patch (take or hand back control, the windows, a
//! pin, a release or revocation) and the request queue (ask for a model,
//! cancel a request). The act side's: one deployment's
//! `patch {action: start|stop}`, which is parsed here and decided by the
//! deployment controller (`orchestrator::inference_control`).
//!
//! Who acts is the authenticated agent run, named by its home, so a pin's
//! holder and a request's requester cannot be claimed by another run. A write
//! that no agent run makes (the operator's) is the operator's, or the
//! `requester` its payload names.

use super::super::{build_failure, payload_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::inference::{adopt, write_inventory, Authoring, Removals};
use crate::orchestrator::pool_control::{cancel_request, patch_pool, request_model, PoolPatch};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityRequest;
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_inference::control::Request;
use cairn_inference::RecordSource;
use serde_json::Value;

/// A `cairn://inference` patch.
#[derive(Debug, Clone, PartialEq, Eq)]
enum InventoryPatch {
    Adopt(RecordSource),
    Author(Authoring),
}

fn removal_ids<T: From<String>>(remove: &Value, kind: &str) -> Result<Vec<T>, String> {
    match remove.get(kind) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(ids)) => ids
            .iter()
            .map(|id| {
                id.as_str()
                    .map(|id| T::from(id.to_string()))
                    .ok_or_else(|| format!("payload.remove.{kind} lists ids, as strings"))
            })
            .collect(),
        Some(_) => Err(format!("payload.remove.{kind} must be a list of ids")),
    }
}

fn parse_removals(remove: Option<&Value>) -> Result<Removals, String> {
    let Some(remove) = remove.filter(|remove| !remove.is_null()) else {
        return Ok(Removals::default());
    };
    let object = remove
        .as_object()
        .ok_or("payload.remove must be {recipes?, deployments?, pools?}, each a list of ids")?;
    if let Some(key) = object
        .keys()
        .find(|key| !["recipes", "deployments", "pools"].contains(&key.as_str()))
    {
        return Err(format!(
            "payload.remove.{key} is not a record kind; it takes recipes, deployments and pools"
        ));
    }
    Ok(Removals {
        recipes: removal_ids(remove, "recipes")?,
        deployments: removal_ids(remove, "deployments")?,
        pools: removal_ids(remove, "pools")?,
    })
}

fn parse_inventory_patch(payload: &Value) -> Result<InventoryPatch, String> {
    let object = payload.as_object().ok_or("payload must be an object")?;
    if let Some(source) = object.get("adopt") {
        if object.len() > 1 {
            return Err(
                "adopt stands alone: adopt first, then add or remove records in a second patch"
                    .into(),
            );
        }
        return source
            .as_str()
            .and_then(RecordSource::parse)
            .filter(|source| *source != RecordSource::Cairn)
            .map(InventoryPatch::Adopt)
            .ok_or_else(|| {
                "payload.adopt names the source whose records become Cairn's own: \"bginf\""
                    .to_string()
            });
    }
    let upsert = match object.get("upsert") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(_) => {
            return Err(
                "payload.upsert is a document in Cairn's record format, as one string".into(),
            )
        }
    };
    let remove = parse_removals(object.get("remove"))?;
    if upsert.is_none() && remove == Removals::default() {
        return Err("the patch changes nothing: it takes adopt, or upsert and remove".into());
    }
    Ok(InventoryPatch::Author(Authoring::Patch { upsert, remove }))
}

fn parse_inventory_replace(payload: &Value) -> Result<Authoring, String> {
    match payload_str(payload, "format", &[]) {
        None | Some("yaml") => {}
        Some(other) => {
            return Err(format!(
                "format '{other}' is not one the inventory is written in; it is yaml"
            ))
        }
    }
    payload_str(payload, "content", &[])
        .map(|content| Authoring::Replace(content.to_string()))
        .ok_or_else(|| {
            "payload.content must be the whole inventory in Cairn's record format, as cairn://inference?format=yaml reads it".to_string()
        })
}

/// The authority a write of Cairn's own inference records crosses, for
/// `prepare_batch`: `None` for any other write, adopting included, which
/// changes only who owns a record.
pub(crate) fn inference_records_authority(
    item: &ChangeItem,
) -> Option<Result<AuthorityRequest, String>> {
    if !matches!(parse_uri(&item.target), Some(CairnResource::Inference)) {
        return None;
    }
    let authoring = match item.mode {
        ChangeMode::Replace => true,
        ChangeMode::Patch => item
            .payload
            .as_ref()
            .is_some_and(|payload| payload.get("adopt").is_none()),
        _ => false,
    };
    authoring.then(|| {
        Ok(crate::authorization::normalize::inference_records_write(
            crate::authorization::WORKSPACE_ID,
        ))
    })
}

/// Write Cairn's own records, once this write is authorized: the final check,
/// immediately before anything persists, re-matching live grants.
async fn author(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    authoring: &Authoring,
    dry_run: bool,
) -> Result<String, String> {
    if dry_run {
        return Ok("Would write the inference records".into());
    }
    let authority = crate::authorization::normalize::inference_records_write(
        crate::authorization::WORKSPACE_ID,
    );
    crate::resources::mutations::authorize_account_change(orch, request, &authority).await?;
    let written = write_inventory(&orch.db.local, authoring).await?;
    refresh_offers(orch);
    Ok(written)
}

/// Rediscover what Cairn inference offers, now that its records changed.
fn refresh_offers(orch: &Orchestrator) {
    orch.spawn_provider_model_catalog_refresh(vec![
        crate::backends::inference::INFERENCE_BACKEND_KEY.to_string(),
    ]);
}

async fn actor(orch: &Orchestrator, request: &McpCallbackRequest, named: Option<&str>) -> String {
    match crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await {
        Ok(agent) => agent.home,
        Err(_) => named
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .unwrap_or("operator")
            .to_string(),
    }
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let fail = |error: String| build_failure(index, item, error);
    match (resource, item.mode) {
        (CairnResource::Inference, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            match parse_inventory_patch(payload).map_err(fail)? {
                InventoryPatch::Adopt(source) => {
                    if dry_run {
                        return Ok(Some(format!(
                            "Would adopt every {} record as Cairn's own",
                            source.as_str()
                        )));
                    }
                    let adopted = adopt(&orch.db.local, source).await.map_err(fail)?;
                    refresh_offers(orch);
                    Ok(Some(adopted))
                }
                InventoryPatch::Author(authoring) => author(orch, request, &authoring, dry_run)
                    .await
                    .map(Some)
                    .map_err(fail),
            }
        }
        (CairnResource::Inference, ChangeMode::Replace) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=replace requires payload".into()))?;
            let authoring = parse_inventory_replace(payload).map_err(fail)?;
            author(orch, request, &authoring, dry_run)
                .await
                .map(Some)
                .map_err(fail)
        }
        (CairnResource::InferencePools, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            let patch = PoolPatch::parse(payload).map_err(fail)?;
            if dry_run {
                return Ok(Some(format!("Would patch pool {}", patch.pool)));
            }
            let actor = actor(orch, request, None).await;
            patch_pool(orch, &patch, &actor)
                .await
                .map(Some)
                .map_err(fail)
        }
        (CairnResource::InferenceRequests, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=append requires payload".into()))?;
            let model = payload_str(payload, "model", &[]).ok_or_else(|| {
                fail("payload.model must name the model to load by its real name".into())
            })?;
            let pool = payload_str(payload, "pool", &[]).map(cairn_inference::PoolId::from);
            if dry_run {
                return Ok(Some(format!("Would request {model}")));
            }
            let requester = actor(orch, request, payload_str(payload, "requester", &[])).await;
            let handle = request_model(orch, model, pool.as_ref(), &requester)
                .await
                .map_err(fail)?;
            let status = handle.status(orch).await.map_err(fail)?;
            Ok(Some(format!(
                "Request #{} for {} on pool {}: {}. Read cairn://inference/requests for the queue.",
                handle.id,
                handle.deployment,
                handle.pool,
                status.describe()
            )))
        }
        (CairnResource::InferenceRequests, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            let id = payload
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| fail("payload.id must be the request's number".into()))?;
            match payload_str(payload, "action", &[]) {
                Some("cancel") => {}
                _ => return Err(fail("payload.action must be \"cancel\"".into())),
            }
            if dry_run {
                return Ok(Some(format!("Would cancel request #{id}")));
            }
            let by = actor(orch, request, None).await;
            cancel_request(orch, id, &by).await.map(Some).map_err(fail)
        }
        (CairnResource::InferenceDeployment { id }, ChangeMode::Patch) => {
            let action = match item
                .payload
                .as_ref()
                .and_then(|payload| payload_str(payload, "action", &[]))
            {
                Some("start") => Request::Start,
                Some("stop") => Request::Stop,
                Some(other) => {
                    return Err(fail(format!(
                        "payload.action `{other}` must be start or stop"
                    )))
                }
                None => return Err(fail("payload.action must be start or stop".into())),
            };
            if dry_run {
                return Ok(Some(format!("Would {} deployment {id}", action.as_str())));
            }
            crate::orchestrator::inference_control::request_deployment(orch, id, action)
                .await
                .map(Some)
                .map_err(fail)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(mode: ChangeMode, payload: Value) -> ChangeItem {
        ChangeItem {
            target: "cairn://inference".into(),
            mode,
            payload: Some(payload),
        }
    }

    #[test]
    fn a_patch_adopts_alone_or_authors_records() {
        assert_eq!(
            parse_inventory_patch(&serde_json::json!({"adopt": "bginf"})),
            Ok(InventoryPatch::Adopt(RecordSource::Bginf))
        );
        assert!(parse_inventory_patch(&serde_json::json!({"adopt": "cairn"})).is_err());
        assert!(parse_inventory_patch(
            &serde_json::json!({"adopt": "bginf", "upsert": "pools: []"})
        )
        .unwrap_err()
        .starts_with("adopt stands alone"));
        assert_eq!(
            parse_inventory_patch(&serde_json::json!({
                "upsert": "pools: []",
                "remove": {"deployments": ["qwen36-fast"]}
            })),
            Ok(InventoryPatch::Author(Authoring::Patch {
                upsert: Some("pools: []".into()),
                remove: Removals {
                    deployments: vec!["qwen36-fast".into()],
                    ..Removals::default()
                },
            }))
        );
        assert!(parse_inventory_patch(&serde_json::json!({})).is_err());
        assert!(parse_inventory_patch(&serde_json::json!({"remove": {"models": []}})).is_err());
        assert!(parse_inventory_patch(&serde_json::json!({"remove": {"pools": [1]}})).is_err());
    }

    #[test]
    fn a_replace_takes_the_whole_document_as_yaml() {
        assert_eq!(
            parse_inventory_replace(&serde_json::json!({"format": "yaml", "content": "pools: []"})),
            Ok(Authoring::Replace("pools: []".into()))
        );
        assert!(
            parse_inventory_replace(&serde_json::json!({"format": "json", "content": "{}"}))
                .is_err()
        );
        assert!(parse_inventory_replace(&serde_json::json!({"format": "yaml"})).is_err());
    }

    #[test]
    fn writing_records_crosses_the_inference_scope_and_adopting_does_not() {
        let scope = |item: &ChangeItem| {
            inference_records_authority(item).map(|request| request.unwrap().scope.shorthand())
        };
        let inference = Some("workspace/default/settings/inference:write".to_string());
        assert_eq!(
            scope(&item(
                ChangeMode::Replace,
                serde_json::json!({"content": ""})
            )),
            inference
        );
        assert_eq!(
            scope(&item(ChangeMode::Patch, serde_json::json!({"upsert": ""}))),
            inference
        );
        assert_eq!(
            scope(&item(
                ChangeMode::Patch,
                serde_json::json!({"adopt": "bginf"})
            )),
            None
        );
        let pools = ChangeItem {
            target: "cairn://inference/pools".into(),
            ..item(ChangeMode::Patch, serde_json::json!({"pool": "halo"}))
        };
        assert_eq!(scope(&pools), None);
        let request = crate::authorization::normalize::inference_records_write(
            crate::authorization::WORKSPACE_ID,
        );
        assert!(!matches!(
            crate::authorization::policy::classify(&request.scope, false),
            cairn_common::authorization::AuthorityPolicy::Direct
        ));
    }
}
