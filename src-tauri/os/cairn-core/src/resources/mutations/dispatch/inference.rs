//! The inference facet's writes. The records': the whole inventory replaced
//! (`replace cairn://inference`), records added, replaced and removed
//! (`patch {upsert, remove}`), or the records mirrored from bginf adopted
//! (`patch {adopt: "bginf"}`), each decided by `orchestrator::inference`.
//! Placement's (`fleet::models`): a pool patch (its warm model, its idle
//! window, a pin, a release or revocation), the request queue (ask for a
//! model, cancel a request), and one deployment's
//! `patch {action: start|stop}` by hand.
//!
//! Who acts is the authenticated agent run, named by its home, so a pin's
//! holder and a request's requester cannot be claimed by another run. A write
//! that no agent run makes (the operator's) is the operator's, or the
//! `requester` its payload names.

use super::super::{build_failure, payload_str, ResourceMutationResult};
use crate::fleet::models::{self, PinTarget};
use crate::fleet::pools::PoolKey;
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::inference::{adopt, write_inventory, Authoring, Removals};
use crate::orchestrator::inference_generate::{
    self as generation, detected_pool, executor_snapshots as snapshots, pool_hardware,
};
use crate::orchestrator::inference_import::import;
use crate::orchestrator::inference_models::{register, Web};
use crate::orchestrator::inference_verify;
use crate::orchestrator::Orchestrator;
use cairn_common::authorization::AuthorityRequest;
use cairn_inference::import::ImportSpec;
use cairn_inference::RecordSource;
use cairn_uri::{parse_uri, CairnResource};
use serde_json::Value;

/// A `cairn://inference/pools` patch: applied as preferences, then a pin,
/// then a release or revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PoolPatch {
    pub pool: PoolKey,
    /// `Some(None)` clears the warm model.
    pub warm: Option<Option<String>>,
    /// `Some(None)` derives the idle window again.
    pub idle_secs: Option<Option<u64>>,
    /// The model, by real name, to pin on the pool.
    pub pin: Option<String>,
    /// `Some(None)`: every pin on the pool the actor holds (or, revoking,
    /// anyone holds); `Some(Some(model))`: that model's.
    pub release: Option<Option<String>>,
    pub revoke: Option<Option<String>>,
}

const POOL_KEYS: &str = "pool, warm, idleSecs, pin, release and revoke";

fn model_or_all(key: &str, value: &Value) -> Result<Option<Option<String>>, String> {
    match value {
        Value::Bool(false) | Value::Null => Ok(None),
        Value::Bool(true) => Ok(Some(None)),
        Value::String(model) if !model.trim().is_empty() => Ok(Some(Some(model.trim().into()))),
        _ => Err(format!(
            "{key} is true (every pin on the pool) or a model's real name"
        )),
    }
}

impl PoolPatch {
    pub fn parse(payload: &Value) -> Result<Self, String> {
        let object = payload.as_object().ok_or("the payload must be an object")?;
        let mut patch = PoolPatch {
            pool: PoolKey(String::new()),
            warm: None,
            idle_secs: None,
            pin: None,
            release: None,
            revoke: None,
        };
        for (key, value) in object {
            match key.as_str() {
                "pool" => {
                    patch.pool = PoolKey(
                        value
                            .as_str()
                            .filter(|s| !s.trim().is_empty())
                            .ok_or("pool must name a pool by its key, as cairn://inference/pools lists it")?
                            .trim()
                            .to_string(),
                    );
                }
                "warm" => {
                    patch.warm = Some(match value {
                        Value::Null => None,
                        Value::String(model) if !model.trim().is_empty() => {
                            Some(model.trim().to_string())
                        }
                        _ => return Err("warm must be a model's real name, or null to clear it".into()),
                    })
                }
                "idleSecs" => {
                    patch.idle_secs = Some(match value {
                        Value::Null => None,
                        value => Some(value.as_u64().filter(|s| *s > 0).ok_or(
                            "idleSecs must be a positive number of seconds, or null to derive it",
                        )?),
                    })
                }
                "pin" => {
                    patch.pin = Some(
                        value
                            .as_str()
                            .filter(|s| !s.trim().is_empty())
                            .ok_or("pin must be a model's real name")?
                            .trim()
                            .to_string(),
                    )
                }
                "release" => patch.release = model_or_all("release", value)?,
                "revoke" => patch.revoke = model_or_all("revoke", value)?,
                "controlled" => {
                    return Err(format!(
                        "controlled retired: placement runs every pool's models now; a pool patch takes {POOL_KEYS}"
                    ))
                }
                "settings" => {
                    return Err(format!(
                        "settings retired: a pool has one idle window (idleSecs), and its minimum residency and load bound derive from its measured load; a pool patch takes {POOL_KEYS}"
                    ))
                }
                other => {
                    return Err(format!(
                        "unknown key '{other}'; a pool patch takes {POOL_KEYS}"
                    ))
                }
            }
        }
        if patch.pool.as_str().is_empty() {
            return Err(
                "pool must name a pool by its key, as cairn://inference/pools lists it".into(),
            );
        }
        if patch.release.is_some() && patch.revoke.is_some() {
            return Err("release your own pin or revoke one, not both".to_string());
        }
        if patch
            == (PoolPatch {
                pool: patch.pool.clone(),
                warm: None,
                idle_secs: None,
                pin: None,
                release: None,
                revoke: None,
            })
        {
            return Err(format!("the patch changes nothing; it takes {POOL_KEYS}"));
        }
        Ok(patch)
    }
}

async fn patch_pool(orch: &Orchestrator, patch: PoolPatch, actor: &str) -> Result<String, String> {
    let mut lines = Vec::new();
    if patch.warm.is_some() || patch.idle_secs.is_some() {
        lines.push(
            models::set_preference(orch, &patch.pool, patch.warm.clone(), patch.idle_secs).await?,
        );
    }
    if let Some(model) = &patch.pin {
        lines.push(
            models::pin(
                orch,
                PinTarget::Model {
                    name: model.clone(),
                    pool: Some(patch.pool.clone()),
                },
                actor,
            )
            .await?,
        );
    }
    for (which, revoke) in [(&patch.release, false), (&patch.revoke, true)] {
        let Some(which) = which else {
            continue;
        };
        let target = match which {
            None => PinTarget::Pool(patch.pool.clone()),
            Some(model) => PinTarget::Model {
                name: model.clone(),
                pool: Some(patch.pool.clone()),
            },
        };
        lines.push(models::release(orch, target, actor, revoke).await?);
    }
    Ok(lines.join("\n"))
}

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

const REMOVABLE: &[&str] = &["models", "profiles", "deployments", "machines"];

fn parse_removals(remove: Option<&Value>) -> Result<Removals, String> {
    let Some(remove) = remove.filter(|remove| !remove.is_null()) else {
        return Ok(Removals::default());
    };
    let object = remove.as_object().ok_or(
        "payload.remove must be {models?, profiles?, deployments?, machines?}, each a list of ids",
    )?;
    if let Some(key) = object.keys().find(|key| !REMOVABLE.contains(&key.as_str())) {
        return Err(format!(
            "payload.remove.{key} is not a record kind; it takes models, profiles, deployments and machines (pools are detected, not written)"
        ));
    }
    Ok(Removals {
        models: removal_ids(remove, "models")?,
        profiles: removal_ids(remove, "profiles")?,
        deployments: removal_ids(remove, "deployments")?,
        machines: removal_ids(remove, "machines")?,
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
        // Registering, refreshing and removing a model, and importing a
        // profile, write records. Verifying does not cross the records
        // authority, as starting a deployment from its page does not, though
        // it writes two bounded records of its own: its
        // `verify-<profile>-<pool>` deployment, which runs a profile already
        // written, and, when the engine's KV cache is smaller than the
        // profile's context, that profile's context lowered to fit. Neither
        // adds shell or raises what a profile asks of a machine.
        //
        // A generation's draft (`generated`) does not cross it either: the
        // operator's own start of the generation is the authority, and the
        // door is narrow. Only that generation's workflow node may write
        // through it, only a digest-pinned container profile of its model, and
        // never over a profile that is not its own draft.
        (CairnResource::InferenceModels, ChangeMode::Append) => true,
        (CairnResource::InferenceModel { .. }, ChangeMode::Delete) => true,
        (CairnResource::InferenceModel { .. }, ChangeMode::Patch) => payload_has("refresh"),
        (CairnResource::InferenceProfiles, ChangeMode::Append) => !payload_has("generated"),
        _ => false,
    };
    authoring.then(|| Ok(records_authority(item)))
}

/// The request a records write normalizes to, naming the item as its act. The
/// gate and the pre-persist re-check both derive it here.
fn records_authority(item: &ChangeItem) -> AuthorityRequest {
    crate::authorization::normalize::inference_records_write(
        crate::authorization::WORKSPACE_ID,
        item,
    )
}

/// Write Cairn's own records, once this write is authorized: the final check,
/// immediately before anything persists, re-matching live grants.
async fn author(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    item: &ChangeItem,
    authoring: &Authoring,
    dry_run: bool,
) -> Result<String, String> {
    if dry_run {
        return Ok("Would write the inference records".into());
    }
    let authority = records_authority(item);
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
    item: &ChangeItem,
    write: F,
) -> Result<String, String>
where
    F: std::future::Future<Output = Result<String, String>>,
{
    let authority = records_authority(item);
    crate::resources::mutations::authorize_account_change(orch, request, &authority).await?;
    write.await
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
    applied_data: &mut Option<Value>,
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
                InventoryPatch::Author(authoring) => {
                    author(orch, request, item, &authoring, dry_run)
                        .await
                        .map(Some)
                        .map_err(fail)
                }
            }
        }
        (CairnResource::Inference, ChangeMode::Replace) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=replace requires payload".into()))?;
            let authoring = parse_inventory_replace(payload).map_err(fail)?;
            author(orch, request, item, &authoring, dry_run)
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
            patch_pool(orch, patch, &actor)
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
            let pool = payload_str(payload, "pool", &[]).map(|pool| PoolKey(pool.to_string()));
            if dry_run {
                return Ok(Some(format!("Would request {model}")));
            }
            let requester = actor(orch, request, payload_str(payload, "requester", &[])).await;
            let inventory = crate::orchestrator::inference_route::served_inventory(orch)
                .await
                .map_err(fail)?;
            let real = crate::orchestrator::inference_route::resolve_model(&inventory, model)
                .map_err(fail)?;
            let handle = models::request(orch, &real, None, &requester, pool.as_ref())
                .await
                .map_err(fail)?;
            let status = handle.status(orch).await.map_err(fail)?;
            Ok(Some(format!(
                "Request #{} for {} (deployment {}) on pool {}: {}. Read cairn://inference/requests for the queue.",
                handle.id,
                handle.model,
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
            models::cancel(orch, id, &by).await.map(Some).map_err(fail)
        }
        (CairnResource::InferenceDeployment { id }, ChangeMode::Patch) => {
            let start = match item
                .payload
                .as_ref()
                .and_then(|payload| payload_str(payload, "action", &[]))
            {
                Some("start") => true,
                Some("stop") => false,
                Some(other) => {
                    return Err(fail(format!(
                        "payload.action `{other}` must be start or stop"
                    )))
                }
                None => return Err(fail("payload.action must be start or stop".into())),
            };
            if dry_run {
                return Ok(Some(format!(
                    "Would {} deployment {id}",
                    if start { "start" } else { "stop" }
                )));
            }
            let by = actor(orch, request, None).await;
            if start {
                models::start_by_hand(orch, id, &by).await
            } else {
                models::stop_by_hand(orch, id, &by).await
            }
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
                item,
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
                return authorized(
                    orch,
                    request,
                    item,
                    register(&orch.db.local, &hub, name, None),
                )
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
            let detected = detected_pool(orch, &pool).map_err(fail)?;
            let family = pool_hardware(&detected, &snapshots(orch), &Default::default())
                .ok()
                .and_then(|hw| hw.machines.first().map(|m| m.family));
            let mut profiles = Vec::new();
            for profile in
                inference_verify::candidates(&inventory, name, detected.members.len(), family)
            {
                // An open generation's draft is verified only once the
                // operator approved what it runs.
                if generation::unapproved_draft(&orch.db.local, profile.as_str())
                    .await
                    .map_err(fail)?
                    .is_none()
                {
                    profiles.push(profile);
                }
            }
            if profiles.is_empty() {
                return Err(fail(format!(
                    "model {name} has no container profile that suits pool {pool}; import one at cairn://inference/profiles, or generate one from Settings > Inference"
                )));
            }
            let requester = actor(orch, request, None).await;
            let count = profiles.len();
            let plan = inference_verify::spawn(orch, profiles, detected, &requester, all, None)
                .await
                .map_err(fail)?;
            Ok(Some(format!(
                "Verifying {name}'s profiles on pool {pool} ({count} to try, {}), starting with verification {}, which reaches its head at {}. Read cairn://inference/models/{name} for each one's outcome.",
                if all { "every one" } else { "until one passes" },
                plan.id,
                plan.head
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
            author(orch, request, item, &authoring, false)
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
                return authorized(orch, request, item, async {
                    import(&orch.db.local, &hub, &spec)
                        .await
                        .map_err(|refusal| refusal.to_string())
                })
                .await
                .map(Some)
                .map_err(fail);
            }
            if payload.get("generate").is_some() {
                return Err(fail(
                    "generate retired: a profile is generated by an agent session started from Settings > Inference (tenet 1364); its workflow appends its draft as generated".into(),
                ));
            }
            let generated = payload.get("generated").ok_or_else(|| {
                fail("payload takes import:{...}, or generated:{profile, sources?, answers?, startingTemplate?} from a profile generation's own workflow".into())
            })?;
            if dry_run {
                return Ok(Some("Would write a generation's draft profile".into()));
            }
            let agent = crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
                .await
                .map_err(|_| {
                    fail(
                        "only a profile generation's own workflow writes a generated profile"
                            .into(),
                    )
                })?;
            let (said, data) =
                generation::write_generated(&orch.db.local, &agent.home, &agent.job_id, generated)
                    .await
                    .map_err(fail)?;
            refresh_offers(orch);
            orch.notifier.emit_change("inference_generations");
            *applied_data = Some(data);
            Ok(Some(said))
        }
        (CairnResource::InferenceGeneration { id }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            let agent = if dry_run {
                None
            } else {
                Some(
                    crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request)
                        .await
                        .map_err(|_| {
                            fail("only the generation's own workflow patches it".into())
                        })?,
                )
            };
            if let Some(approve) = payload.get("approve") {
                let question = approve
                    .get("question")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        fail("payload.approve is {question}: the URI of the question the operator answered Approve".into())
                    })?;
                let Some(agent) = agent else {
                    return Ok(Some(format!(
                        "Would record the approval of generation {id}'s draft"
                    )));
                };
                let asked = generation::asked_question(orch, &agent.job_id, question)
                    .await
                    .map_err(fail)?;
                let digest = generation::approve(&orch.db.local, &agent.job_id, id, asked)
                    .await
                    .map_err(fail)?;
                *applied_data = Some(serde_json::json!({ "generation": id, "approved": digest }));
                return Ok(Some(format!(
                    "The operator approved generation {id}'s draft as it stands (surface {digest}); it may be verified now."
                )));
            }
            let outcome = payload
                .get("outcome")
                .filter(|outcome| outcome.is_object())
                .ok_or_else(|| {
                    fail("payload takes approve:{question}, or outcome:{outcome: verified|cannotRun|gaveUp, verification?, why?, measured?, sources?, attempts?}".into())
                })?;
            let Some(agent) = agent else {
                return Ok(Some(format!("Would end generation {id}")));
            };
            let verifications = inference_verify::load_verifications(&orch.db.local)
                .await
                .map_err(fail)?;
            let (said, ended) = generation::record_outcome(
                &orch.db.local,
                &agent.job_id,
                id,
                outcome,
                &verifications,
            )
            .await
            .map_err(fail)?;
            refresh_offers(orch);
            orch.notifier.emit_change("inference_generations");
            generation::close_anchor_when_done(orch, &ended);
            *applied_data =
                Some(serde_json::json!({ "generation": ended.id, "state": ended.state }));
            Ok(Some(said))
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
            if let Some(refusal) = generation::unapproved_draft(&orch.db.local, id)
                .await
                .map_err(fail)?
            {
                return Err(fail(refusal));
            }
            let detected = detected_pool(orch, &pool).map_err(fail)?;
            let requester = actor(orch, request, None).await;
            let plan = inference_verify::spawn(
                orch,
                vec![cairn_inference::InferenceProfileId::from(id.as_str())],
                detected,
                &requester,
                false,
                None,
            )
            .await
            .map_err(fail)?;
            let (verification, head) = (plan.id, plan.head);
            *applied_data = Some(serde_json::json!({ "verification": verification }));
            Ok(Some(format!(
                "Verification {verification} started: profile {id} loads on pool {pool} as deployment verify-{id}-{pool}, its head reached at {head}, pinned for the verification so placement displaces any unpinned model on its machines, then its health is checked and it is benchmarked; afterwards it is unloaded and placement's rules bring the pool's models back. Read cairn://inference/verifications/{verification} for its stage."
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
            &serde_json::json!({"adopt": "bginf", "upsert": "machines: []"})
        )
        .unwrap_err()
        .starts_with("adopt stands alone"));
        assert_eq!(
            parse_inventory_patch(&serde_json::json!({
                "upsert": "machines: []",
                "remove": {"deployments": ["qwen36-fast"], "machines": ["bbs"]}
            })),
            Ok(InventoryPatch::Author(Authoring::Patch {
                upsert: Some("machines: []".into()),
                remove: Removals {
                    deployments: vec!["qwen36-fast".into()],
                    machines: vec!["bbs".into()],
                    ..Removals::default()
                },
            }))
        );
        assert!(parse_inventory_patch(&serde_json::json!({})).is_err());
        assert!(parse_inventory_patch(&serde_json::json!({"remove": {"modelz": []}})).is_err());
        assert!(
            parse_inventory_patch(&serde_json::json!({"remove": {"pools": ["sparks"]}}))
                .unwrap_err()
                .contains("pools are detected, not written")
        );
        assert!(parse_inventory_patch(&serde_json::json!({"remove": {"machines": [1]}})).is_err());
    }

    #[test]
    fn a_pool_patch_takes_the_new_keys_and_names_them_for_the_retired_ones() {
        let parse = |value: Value| PoolPatch::parse(&value);
        let patch = parse(serde_json::json!({
            "pool": "bginf-sp1+bginf-sp2",
            "warm": "deepseek-ai/DeepSeek-V4-Flash",
            "idleSecs": 1200,
            "pin": "deepseek-ai/DeepSeek-V4-Flash"
        }))
        .unwrap();
        assert_eq!(patch.pool, PoolKey("bginf-sp1+bginf-sp2".into()));
        assert_eq!(
            patch.warm,
            Some(Some("deepseek-ai/DeepSeek-V4-Flash".into()))
        );
        assert_eq!(patch.idle_secs, Some(Some(1200)));
        assert_eq!(patch.pin.as_deref(), Some("deepseek-ai/DeepSeek-V4-Flash"));
        let cleared =
            parse(serde_json::json!({"pool": "fedora", "warm": null, "idleSecs": null})).unwrap();
        assert_eq!((cleared.warm, cleared.idle_secs), (Some(None), Some(None)));
        assert_eq!(
            parse(serde_json::json!({"pool": "fedora", "release": true}))
                .unwrap()
                .release,
            Some(None)
        );
        assert_eq!(
            parse(serde_json::json!({"pool": "fedora", "revoke": "m"}))
                .unwrap()
                .revoke,
            Some(Some("m".into()))
        );

        for (payload, says) in [
            (
                serde_json::json!({"pool": "sparks", "controlled": true}),
                "controlled retired",
            ),
            (
                serde_json::json!({"pool": "sparks", "settings": {"modelIdleSecs": 60}}),
                "settings retired: a pool has one idle window (idleSecs)",
            ),
            (
                serde_json::json!({"pool": "sparks", "default": ["x"]}),
                "unknown key 'default'",
            ),
            (serde_json::json!({"warm": "x"}), "pool must name a pool"),
            (
                serde_json::json!({"pool": "sparks", "idleSecs": 0}),
                "idleSecs must be a positive number",
            ),
            (
                serde_json::json!({"pool": "sparks", "release": true, "revoke": true}),
                "not both",
            ),
            (serde_json::json!({"pool": "sparks"}), "changes nothing"),
        ] {
            let refusal = parse(payload.clone()).unwrap_err();
            assert!(refusal.contains(says), "{payload}: {refusal}");
            if says.ends_with("retired") || says.starts_with("unknown") {
                assert!(refusal.contains(POOL_KEYS), "{refusal}");
            }
        }
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
        let import = ChangeItem {
            target: "cairn://inference/profiles".into(),
            ..item(ChangeMode::Append, serde_json::json!({"import": {}}))
        };
        assert_eq!(scope(&import), inference);
        let generated = ChangeItem {
            target: "cairn://inference/profiles".into(),
            ..item(
                ChangeMode::Append,
                serde_json::json!({"generated": {"profile": ""}}),
            )
        };
        assert_eq!(
            scope(&generated),
            None,
            "a generation's draft is authorized by the operator's start, through its own narrow door"
        );
        let pools = ChangeItem {
            target: "cairn://inference/pools".into(),
            ..item(ChangeMode::Patch, serde_json::json!({"pool": "halo"}))
        };
        assert_eq!(scope(&pools), None);
        let request = records_authority(&refresh);
        assert!(!matches!(
            crate::authorization::policy::classify(&request.scope, false),
            cairn_common::authorization::AuthorityPolicy::Direct
        ));
    }
}
