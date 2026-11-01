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
use crate::orchestrator::inference_generate::{generate, pool_hardware, StatedHardware};
use crate::orchestrator::inference_import::import;
use crate::orchestrator::inference_models::{register, Web};
use crate::orchestrator::inference_verify;
use crate::orchestrator::pool_control::{cancel_request, patch_pool, request_model, PoolPatch};
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityRequest;
use cairn_common::uri::{parse_uri, CairnResource};
use cairn_inference::control::Request;
use cairn_inference::import::ImportSpec;
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

const REMOVABLE: &[&str] = &["models", "profiles", "deployments", "pools"];

fn parse_removals(remove: Option<&Value>) -> Result<Removals, String> {
    let Some(remove) = remove.filter(|remove| !remove.is_null()) else {
        return Ok(Removals::default());
    };
    let object = remove.as_object().ok_or(
        "payload.remove must be {models?, profiles?, deployments?, pools?}, each a list of ids",
    )?;
    if let Some(key) = object.keys().find(|key| !REMOVABLE.contains(&key.as_str())) {
        return Err(format!(
            "payload.remove.{key} is not a record kind; it takes models, profiles, deployments and pools"
        ));
    }
    Ok(Removals {
        models: removal_ids(remove, "models")?,
        profiles: removal_ids(remove, "profiles")?,
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
    let payload_has = |key: &str| {
        item.payload
            .as_ref()
            .is_some_and(|payload| payload.get(key).is_some())
    };
    let authoring = match (parse_uri(&item.target)?, item.mode) {
        (CairnResource::Inference, ChangeMode::Replace) => true,
        (CairnResource::Inference, ChangeMode::Patch) => !payload_has("adopt"),
        // Registering, refreshing and removing a model, and importing or
        // generating a profile, write records. Verifying does not cross the
        // records authority, as starting a deployment from its page does not,
        // though it writes two bounded records of its own: its
        // `verify-<profile>-<pool>` deployment, which runs a profile already
        // written under the authority, and, when the engine's KV cache is
        // smaller than the profile's context, that profile's context lowered to
        // fit. Neither adds shell or raises what a profile asks of a machine.
        (CairnResource::InferenceModels, ChangeMode::Append) => true,
        (CairnResource::InferenceModel { .. }, ChangeMode::Delete) => true,
        (CairnResource::InferenceModel { .. }, ChangeMode::Patch) => payload_has("refresh"),
        (CairnResource::InferenceProfiles, ChangeMode::Append) => true,
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

/// Authorize a write of Cairn's own records, then run it.
async fn authorized<F>(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    write: F,
) -> Result<String, String>
where
    F: std::future::Future<Output = Result<String, String>>,
{
    let authority = crate::authorization::normalize::inference_records_write(
        crate::authorization::WORKSPACE_ID,
    );
    crate::resources::mutations::authorize_account_change(orch, request, &authority).await?;
    write.await
}

/// The latest health of every attached executor, by public name.
fn snapshots(
    orch: &Orchestrator,
) -> std::collections::BTreeMap<String, cairn_common::executor_protocol::ExecutorHealthSnapshot> {
    orch.fleet
        .executor_health(crate::fleet::unix_time_ms())
        .into_iter()
        .map(|snapshot| (snapshot.public_name.clone(), snapshot))
        .collect()
}

fn pool_of(payload: &Value, key: &str) -> Result<(String, bool), String> {
    let verify = payload
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| format!("payload.{key} must be {{pool}}"))?;
    let pool = verify
        .get("pool")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("payload.{key}.pool must name a pool"))?;
    let all = verify.get("all").and_then(Value::as_bool).unwrap_or(false);
    Ok((pool.to_string(), all))
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
        (CairnResource::InferenceModels, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=append requires payload".into()))?;
            let repo = payload_str(payload, "repo", &[])
                .ok_or_else(|| fail("payload.repo must be a Hugging Face repository id".into()))?;
            let revision = payload_str(payload, "revision", &[]);
            if dry_run {
                return Ok(Some(format!("Would register the model {repo}")));
            }
            let hub = Web::new().map_err(fail)?;
            authorized(
                orch,
                request,
                register(&orch.db.local, &hub, repo, revision),
            )
            .await
            .map(Some)
            .map_err(fail)
        }
        (CairnResource::InferenceModel { name }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            if payload.get("refresh").and_then(Value::as_bool) == Some(true) {
                if dry_run {
                    return Ok(Some(format!("Would read {name}'s config again")));
                }
                let hub = Web::new().map_err(fail)?;
                return authorized(orch, request, register(&orch.db.local, &hub, name, None))
                    .await
                    .map(Some)
                    .map_err(fail);
            }
            let (pool, all) = pool_of(payload, "verify").map_err(|why| {
                fail(format!(
                    "{why}; the patch takes refresh:true or verify:{{pool, all?}}"
                ))
            })?;
            if dry_run {
                return Ok(Some(format!(
                    "Would verify {name}'s profiles on pool {pool}"
                )));
            }
            let inventory = crate::orchestrator::inference::load_inventory(&orch.db.local)
                .await
                .map_err(fail)?;
            let pool_id = cairn_inference::PoolId::from(pool.as_str());
            let family = pool_hardware(&inventory, &pool_id, &snapshots(orch), &Default::default())
                .ok()
                .and_then(|hw| hw.machines.first().map(|m| m.family));
            let profiles = inference_verify::candidates(&inventory, name, &pool_id, family);
            if profiles.is_empty() {
                return Err(fail(format!(
                    "model {name} has no container profile that suits pool {pool}; import one or generate candidates at cairn://inference/profiles"
                )));
            }
            let requester = actor(orch, request, None).await;
            let count = profiles.len();
            let id = inference_verify::spawn(orch, profiles, &pool, &requester, all)
                .await
                .map_err(fail)?;
            Ok(Some(format!(
                "Verifying {name}'s profiles on pool {pool} ({count} to try, {}), starting with verification {id}. Read cairn://inference/models/{name} for each one's outcome.",
                if all { "every one" } else { "until one passes" }
            )))
        }
        (CairnResource::InferenceModel { name }, ChangeMode::Delete) => {
            if dry_run {
                return Ok(Some(format!("Would remove the model {name}")));
            }
            let authoring = Authoring::Patch {
                upsert: None,
                remove: Removals {
                    models: vec![name.clone()],
                    ..Removals::default()
                },
            };
            author(orch, request, &authoring, false)
                .await
                .map(Some)
                .map_err(fail)
        }
        (CairnResource::InferenceProfiles, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=append requires payload".into()))?;
            if let Some(spec) = payload.get("import") {
                let spec: ImportSpec = serde_json::from_value(spec.clone()).map_err(|error| {
                    fail(format!("payload.import is not an import spec: {error}"))
                })?;
                if dry_run {
                    return Ok(Some(format!(
                        "Would import service {} of {}",
                        spec.service, spec.url
                    )));
                }
                let hub = Web::new().map_err(fail)?;
                return authorized(orch, request, import(&orch.db.local, &hub, &spec))
                    .await
                    .map(Some)
                    .map_err(fail);
            }
            let generate_payload = payload
                .get("generate")
                .and_then(Value::as_object)
                .ok_or_else(|| {
                    fail("payload takes import:{...} or generate:{model, pool, hardware?}".into())
                })?;
            let model = generate_payload
                .get("model")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    fail("payload.generate.model must name a registered model".into())
                })?;
            let pool = generate_payload
                .get("pool")
                .and_then(Value::as_str)
                .ok_or_else(|| fail("payload.generate.pool must name a pool".into()))?;
            let stated: std::collections::BTreeMap<String, StatedHardware> =
                match generate_payload.get("hardware") {
                    None | Some(Value::Null) => Default::default(),
                    Some(hardware) => serde_json::from_value(hardware.clone()).map_err(|error| {
                        fail(format!("payload.generate.hardware is {{<machine>: {{family, memoryGib, unified, arch, gpu?}}}}: {error}"))
                    })?,
                };
            if dry_run {
                return Ok(Some(format!(
                    "Would generate profiles for {model} on pool {pool}"
                )));
            }
            let hub = Web::new().map_err(fail)?;
            let snapshots = snapshots(orch);
            authorized(
                orch,
                request,
                generate(&orch.db.local, &hub, model, pool, &snapshots, &stated),
            )
            .await
            .map(Some)
            .map_err(fail)
        }
        (CairnResource::InferenceProfile { id }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            let (pool, _) = pool_of(payload, "verify").map_err(fail)?;
            if dry_run {
                return Ok(Some(format!("Would verify profile {id} on pool {pool}")));
            }
            let requester = actor(orch, request, None).await;
            let verification = inference_verify::spawn(
                orch,
                vec![cairn_inference::InferenceProfileId::from(id.as_str())],
                &pool,
                &requester,
                false,
            )
            .await
            .map_err(fail)?;
            Ok(Some(format!(
                "Verification {verification} started: profile {id} loads on pool {pool} as deployment verify-{id}-{pool}, then its health is checked and it is benchmarked; the pool's own model is stopped first and restored after. Read cairn://inference/verifications/{verification} for its stage."
            )))
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
        assert!(parse_inventory_patch(&serde_json::json!({"remove": {"modelz": []}})).is_err());
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
        let register = ChangeItem {
            target: "cairn://inference/models".into(),
            ..item(
                ChangeMode::Append,
                serde_json::json!({"repo": "Qwen/Qwen3-Embedding-4B"}),
            )
        };
        assert_eq!(scope(&register), inference);
        let verify = ChangeItem {
            target: "cairn://inference/profiles/embed".into(),
            ..item(
                ChangeMode::Patch,
                serde_json::json!({"verify": {"pool": "halo"}}),
            )
        };
        assert_eq!(
            scope(&verify),
            None,
            "verifying starts a deployment, as its page does"
        );
        let refresh = ChangeItem {
            target: "cairn://inference/models/Qwen/Qwen3-Embedding-4B".into(),
            ..item(ChangeMode::Patch, serde_json::json!({"refresh": true}))
        };
        assert_eq!(scope(&refresh), inference);
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
