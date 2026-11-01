//! Routing a model request to the cairn-inference deployment that serves it,
//! loading the model on demand (tenet 1321).
//!
//! A name a deployment serves ([`cairn_inference::served`]) means that one
//! deployment and is never served by another. On a pool under Cairn's control
//! every call is a request to the pool controller
//! ([`super::pool_control::request_model`]): a loaded model answers on the same
//! pass, and an unloaded one loads while the caller waits on what its request
//! waits on. Every call counts as demand, which is what keeps a model in use
//! from looking idle to the controller. On a pool Cairn does not control, a
//! deployment the act side holds healthy (one started from its own page) is
//! reached directly and any other is refused, since nothing here would load
//! it.
//!
//! The wait belongs to the caller. A caller with a cancel signal (an agent
//! turn's interrupt) cancels its request when it stops waiting. A caller with
//! only a time budget (a completion, a decision) is released when the budget
//! runs out and leaves its request queued, so the model still loads and a later
//! call answers (tenet 1168: the caller is released at once, and nothing blocks
//! on the request). While one of a caller's requests still waits, its next ask
//! reuses that request instead of queueing another.
//!
//! Each engine is measured once per load, when a route to it is first taken or
//! the catalog is next refreshed ([`EngineMeasure`]): its own context limit from
//! its model listing and its KV cache from its metrics, which together clamp
//! the context Cairn advertises for it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use cairn_config::inference::InferenceProfileDeclaration;
use cairn_inference::control::Phase;
use cairn_inference::pool_control::{Ms, PoolControl, RequestState};
use cairn_inference::served::{self, EngineMeasure, ServedModel};
use cairn_inference::{Deployment, DeploymentId, InferenceProfileId, Inventory, PoolId};
use cairn_models::types::catalog::Modalities;

use super::inference::load_inventory;
use super::pool_control::{
    cancel_request, deployment_for, deployment_phases, load_controls, load_requests, request_model,
    uncontrolled_refusal, RequestHandle,
};
use super::Orchestrator;
use crate::storage::run_db_blocking;

/// How long one engine probe may take.
const MEASURE_TIMEOUT: Duration = Duration::from_secs(3);

/// Where one request for a served model goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Route {
    pub deployment: DeploymentId,
    pub pool: PoolId,
    /// The OpenAI-compatible API base of the deployment's head.
    pub api_base: String,
    /// The name the engine answers to, which every request is sent under.
    pub served_name: String,
    /// The context the engine can hold, when it was measured.
    pub context_length: Option<u64>,
}

impl Route {
    pub fn chat_url(&self) -> String {
        format!("{}/chat/completions", self.api_base)
    }
}

/// What one ask found.
pub(crate) enum Asked {
    Ready(Route),
    /// The request waits; `said` is what on, in words.
    Waiting {
        handle: RequestHandle,
        said: String,
    },
}

/// The inventory as served: the stored records, with the operator's
/// declarations (`inference.profiles` in settings) laid over each profile.
pub(crate) async fn served_inventory(orch: &Orchestrator) -> Result<Inventory, String> {
    let mut inventory = load_inventory(&orch.db.local).await?;
    declare(
        &mut inventory,
        &crate::config::settings::load_inference(&orch.config_dir).profiles,
    );
    Ok(inventory)
}

/// Lay each declaration over the profile it names: a declared part replaces
/// the record's own, and a declaration naming no profile here changes nothing.
pub(crate) fn declare(
    inventory: &mut Inventory,
    declarations: &BTreeMap<String, InferenceProfileDeclaration>,
) {
    for (id, declared) in declarations {
        let Some(profile) = inventory
            .profiles
            .get_mut(&InferenceProfileId::from(id.as_str()))
        else {
            continue;
        };
        if let Some(verbs) = &declared.verbs {
            profile.verbs = Some(verbs.clone());
        }
        if let (Some(input), Some(output)) =
            (&declared.input_modalities, &declared.output_modalities)
        {
            profile.modalities = Some(Modalities {
                input: input.clone(),
                output: output.clone(),
            });
        }
    }
}

/// The deployments answering now: those the act side holds healthy. One it
/// has never acted on is not known to answer, since nothing but Cairn starts a
/// model now; taking its pool under control stops it once and brings it under
/// probing.
async fn healthy_now(orch: &Orchestrator) -> Result<BTreeSet<DeploymentId>, String> {
    Ok(deployment_phases(&orch.db.local)
        .await?
        .into_iter()
        .filter(|(_, phase)| matches!(phase, Phase::Healthy { .. }))
        .map(|(id, _)| id)
        .collect())
}

/// Every name served now, each healthy engine measured for its current load.
pub(crate) async fn served_now(orch: &Orchestrator) -> Result<Vec<ServedModel>, String> {
    let inventory = served_inventory(orch).await?;
    let controls = load_controls(&orch.db.local).await?;
    let healthy = healthy_now(orch).await?;
    for deployment in inventory.deployments.values() {
        if !healthy.contains(&deployment.id) {
            forget(&deployment.id);
            continue;
        }
        let (Some(profile), Some(address)) = (
            inventory.profiles.get(&deployment.profile),
            inventory.address_of(&deployment.head),
        ) else {
            continue;
        };
        let load = inventory
            .pool_of_deployment(deployment)
            .and_then(|pool| current_load(&controls, &pool.id, &deployment.id));
        measure(deployment, address, &profile.served_name, load).await;
    }
    Ok(served::served_models(&inventory, &measures()))
}

/// The name the engine serving `model` answers to, when a deployment serves
/// it.
pub(crate) async fn served_name_of(orch: &Orchestrator, model: &str) -> Option<String> {
    let inventory = served_inventory(orch).await.ok()?;
    let (deployment, _) = deployment_for(&inventory, model, None).ok()?;
    let profile = &inventory.deployments.get(&deployment)?.profile;
    Some(inventory.profiles.get(profile)?.served_name.clone())
}

/// Ask once for `model` on behalf of `requester`.
pub(crate) async fn ask(
    orch: &Orchestrator,
    model: &str,
    requester: &str,
) -> Result<Asked, String> {
    let inventory = served_inventory(orch).await?;
    let (deployment, pool) = deployment_for(&inventory, model, None)?;
    let db = &orch.db.local;
    let controls = load_controls(db).await?;
    let controlled = controls
        .get(&pool)
        .is_some_and(|control| control.controlled);
    if !controlled {
        if !healthy_now(orch).await?.contains(&deployment) {
            forget(&deployment);
            return Err(uncontrolled_refusal(&pool, &deployment));
        }
        return Ok(Asked::Ready(
            route(&inventory, &deployment, &pool, None).await?,
        ));
    }
    let queued = load_requests(db).await?.into_iter().find(|stored| {
        let request = &stored.request;
        request.deployment == deployment
            && request.requester == requester
            && request.state == RequestState::Waiting
    });
    let handle = match queued {
        Some(stored) => RequestHandle {
            id: stored.request.id,
            pool: stored.pool,
            deployment,
        },
        None => request_model(orch, model, Some(&pool), requester).await?,
    };
    answer(orch, &inventory, handle).await
}

/// Where `handle`'s request stands now: routed once it is ready.
async fn answer(
    orch: &Orchestrator,
    inventory: &Inventory,
    handle: RequestHandle,
) -> Result<Asked, String> {
    let status = handle.status(orch).await?;
    match &status.request.request.state {
        RequestState::Ready { .. } => {
            let controls = load_controls(&orch.db.local).await?;
            let load = current_load(&controls, &handle.pool, &handle.deployment);
            Ok(Asked::Ready(
                route(inventory, &handle.deployment, &handle.pool, load).await?,
            ))
        }
        RequestState::Waiting => {
            forget(&handle.deployment);
            let said = status.describe();
            Ok(Asked::Waiting { handle, said })
        }
        RequestState::Failed { reason, .. } => {
            Err(format!("{} did not load: {reason}", handle.deployment))
        }
        RequestState::Cancelled { .. } => Err(format!(
            "request #{} for {} was cancelled",
            handle.id, handle.deployment
        )),
    }
}

async fn route(
    inventory: &Inventory,
    deployment: &DeploymentId,
    pool: &PoolId,
    load: Option<Ms>,
) -> Result<Route, String> {
    let record = inventory
        .deployments
        .get(deployment)
        .ok_or_else(|| format!("{deployment} left the inventory"))?;
    let profile = inventory.profiles.get(&record.profile).ok_or_else(|| {
        format!(
            "{deployment}'s profile {} is not in the inventory",
            record.profile
        )
    })?;
    let address = inventory.address_of(&record.head).ok_or_else(|| {
        format!(
            "{deployment}'s head {} has no address in pool {pool}'s record, so its API cannot be reached",
            record.head
        )
    })?;
    let measure = measure(record, address, &profile.served_name, load).await;
    Ok(Route {
        deployment: deployment.clone(),
        pool: pool.clone(),
        api_base: record.api_base(address),
        served_name: profile.served_name.clone(),
        context_length: measure.context_length(),
    })
}

/// Wait until `model` is ready for `requester`, then say where to send it.
///
/// `budget` bounds the wait (`None` waits until the request is answered or
/// cancelled), `cancelled` is polled throughout, and `waiting` hears each new
/// description of what the request waits on. Blocking: call it from a thread
/// that may block, never from async code.
pub(crate) fn ready_blocking(
    orch: &Orchestrator,
    model: &str,
    requester: &str,
    budget: Option<Duration>,
    cancelled: impl Fn() -> bool,
    waiting: impl FnMut(&str),
) -> Result<Route, String> {
    let first = run_db_blocking(|| ask(orch, model, requester))?;
    wait(
        first,
        &PACE,
        Patience {
            budget,
            model,
            cancelled,
            waiting,
        },
        |handle| {
            run_db_blocking(|| async {
                let inventory = served_inventory(orch).await?;
                answer(orch, &inventory, handle.clone()).await
            })
        },
        |handle| {
            if let Err(error) = run_db_blocking(|| cancel_request(orch, handle.id, requester)) {
                log::debug!(
                    "inference: request #{} for {model} was not cancelled: {error}",
                    handle.id
                );
            }
        },
    )
}

/// How often a waiting caller reads its request, and checks its cancel.
struct Pace {
    poll_every: Duration,
    slice: Duration,
}

const PACE: Pace = Pace {
    poll_every: Duration::from_secs(2),
    slice: Duration::from_millis(250),
};

/// What one caller will put up with.
struct Patience<'a, C, W> {
    budget: Option<Duration>,
    model: &'a str,
    cancelled: C,
    waiting: W,
}

fn wait<C: Fn() -> bool, W: FnMut(&str)>(
    first: Asked,
    pace: &Pace,
    mut patience: Patience<'_, C, W>,
    mut poll: impl FnMut(&RequestHandle) -> Result<Asked, String>,
    cancel: impl FnOnce(&RequestHandle),
) -> Result<Route, String> {
    let model = patience.model;
    let (handle, mut said) = match first {
        Asked::Ready(route) => return Ok(route),
        Asked::Waiting { handle, said } => (handle, said),
    };
    (patience.waiting)(&said);
    let started = Instant::now();
    let mut polled = Instant::now();
    loop {
        if (patience.cancelled)() {
            cancel(&handle);
            return Err(format!(
                "stopped waiting for {model}, and cancelled its request #{}; it was waiting on: {said}",
                handle.id
            ));
        }
        if patience
            .budget
            .is_some_and(|budget| started.elapsed() >= budget)
        {
            return Err(format!(
                "{model} is not ready yet: {said}. Its request #{} stays queued, so the model still loads and a later call answers once it is healthy; cairn://inference/requests shows the queue",
                handle.id
            ));
        }
        std::thread::sleep(pace.slice);
        if polled.elapsed() < pace.poll_every {
            continue;
        }
        polled = Instant::now();
        match poll(&handle)? {
            Asked::Ready(route) => return Ok(route),
            Asked::Waiting { said: now, .. } => {
                if now != said {
                    said = now;
                    (patience.waiting)(&said);
                }
            }
        }
    }
}

/// One engine's measure, and the load it was taken during.
struct Measured {
    load: Option<Ms>,
    api_base: String,
    measure: EngineMeasure,
}

static MEASURED: Mutex<BTreeMap<DeploymentId, Measured>> = Mutex::new(BTreeMap::new());

fn measured_table() -> MutexGuard<'static, BTreeMap<DeploymentId, Measured>> {
    MEASURED.lock().unwrap_or_else(|error| error.into_inner())
}

/// Every engine measure taken for a load that is still running.
pub(crate) fn measures() -> BTreeMap<DeploymentId, EngineMeasure> {
    measured_table()
        .iter()
        .map(|(id, measured)| (id.clone(), measured.measure))
        .collect()
}

/// Forget `deployment`'s measure: it is not loaded, so its next load is
/// measured afresh.
fn forget(deployment: &DeploymentId) {
    measured_table().remove(deployment);
}

/// The load running on `deployment` as the controller recorded it (when it
/// came healthy); `None` on a pool the controller does not drive.
fn current_load(
    controls: &BTreeMap<PoolId, PoolControl>,
    pool: &PoolId,
    deployment: &DeploymentId,
) -> Option<Ms> {
    controls
        .get(pool)?
        .active
        .members
        .get(deployment)?
        .healthy_since
}

/// `deployment`'s measure for `load`, probing the engine the first time.
async fn measure(
    deployment: &Deployment,
    address: &str,
    served_name: &str,
    load: Option<Ms>,
) -> EngineMeasure {
    let api_base = deployment.api_base(address);
    let known = measured_table()
        .get(&deployment.id)
        .filter(|measured| measured.load == load && measured.api_base == api_base)
        .map(|measured| measured.measure);
    if let Some(known) = known {
        return known;
    }
    let (measure, answered) = probe(deployment, address, served_name).await;
    // An engine that did not answer at all is measured again next time
    // rather than recorded as having no limits.
    if answered {
        measured_table().insert(
            deployment.id.clone(),
            Measured {
                load,
                api_base,
                measure,
            },
        );
    }
    measure
}

/// Read the engine's own model listing and metrics, and whether it answered
/// either.
async fn probe(deployment: &Deployment, address: &str, served_name: &str) -> (EngineMeasure, bool) {
    let client = cairn_tls::client();
    async fn body(client: &reqwest::Client, url: String) -> Result<Option<String>, ()> {
        let response = client
            .get(url)
            .timeout(MEASURE_TIMEOUT)
            .send()
            .await
            .map_err(|_| ())?;
        if !response.status().is_success() {
            return Ok(None);
        }
        Ok(response.text().await.ok())
    }
    let models = body(client, format!("{}/models", deployment.api_base(address))).await;
    let metrics = body(client, deployment.metrics_url(address)).await;
    let answered = models.is_ok() || metrics.is_ok();
    let measure = EngineMeasure {
        max_model_len: models
            .ok()
            .flatten()
            .and_then(|body| served::max_model_len(&body, served_name)),
        kv_cache_tokens: metrics
            .ok()
            .flatten()
            .and_then(|body| cairn_inference::metrics::kv_cache_tokens(&body)),
    };
    if answered && measure.kv_cache_tokens.is_none() {
        log::info!(
            "inference: {} reports no KV cache size, so its context is advertised unclamped",
            deployment.id
        );
    }
    (measure, answered)
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeSet;

    use cairn_inference::InferenceProfile;
    use cairn_models::types::catalog::{Modality, VerbKind};

    use super::*;

    const FAST: Pace = Pace {
        poll_every: Duration::from_millis(2),
        slice: Duration::from_millis(1),
    };

    fn handle() -> RequestHandle {
        RequestHandle {
            id: 7,
            pool: "sparks".into(),
            deployment: "dsv4f-tp2".into(),
        }
    }

    fn ready() -> Route {
        Route {
            deployment: "dsv4f-tp2".into(),
            pool: "sparks".into(),
            api_base: "http://10.20.20.40:8888/v1".into(),
            served_name: "deepseek-v4-flash-vision-exp".into(),
            context_length: Some(1_048_576),
        }
    }

    fn waiting(said: &str) -> Asked {
        Asked::Waiting {
            handle: handle(),
            said: said.into(),
        }
    }

    fn patience<'a>(
        budget: Option<Duration>,
        cancelled: impl Fn() -> bool + 'a,
        heard: &'a RefCell<Vec<String>>,
    ) -> Patience<'a, impl Fn() -> bool + 'a, impl FnMut(&str) + 'a> {
        Patience {
            budget,
            model: "dsv4-flash",
            cancelled,
            waiting: move |said: &str| heard.borrow_mut().push(said.to_string()),
        }
    }

    #[test]
    fn a_loaded_model_answers_without_waiting() {
        let heard = RefCell::new(Vec::new());
        let route = wait(
            Asked::Ready(ready()),
            &FAST,
            patience(None, || false, &heard),
            |_| panic!("a ready request is not polled"),
            |_| panic!("a ready request is not cancelled"),
        )
        .unwrap();
        assert_eq!(route, ready());
        assert!(heard.borrow().is_empty());
    }

    #[test]
    fn an_unloaded_model_is_waited_on_visibly_until_it_is_ready() {
        let heard = RefCell::new(Vec::new());
        let polls = Cell::new(0);
        let route = wait(
            waiting("builds draining"),
            &FAST,
            patience(None, || false, &heard),
            |_| {
                polls.set(polls.get() + 1);
                Ok(match polls.get() {
                    1 => waiting("builds draining"),
                    2 | 3 => waiting("loading"),
                    _ => Asked::Ready(ready()),
                })
            },
            |_| panic!("an answered wait cancels nothing"),
        )
        .unwrap();
        assert_eq!(route.served_name, "deepseek-v4-flash-vision-exp");
        // Each new description is heard once.
        assert_eq!(*heard.borrow(), ["builds draining", "loading"]);
    }

    #[test]
    fn a_cancelled_caller_is_released_and_cancels_its_request() {
        let heard = RefCell::new(Vec::new());
        let cancelled = Cell::new(None);
        let error = wait(
            waiting("loading"),
            &FAST,
            patience(None, || true, &heard),
            |_| Ok(waiting("loading")),
            |handle| cancelled.set(Some(handle.id)),
        )
        .unwrap_err();
        assert_eq!(cancelled.get(), Some(7));
        assert!(error.contains("stopped waiting for dsv4-flash"), "{error}");
        assert!(error.contains("loading"), "{error}");
    }

    #[test]
    fn a_spent_budget_releases_the_caller_and_leaves_the_request_to_load() {
        let heard = RefCell::new(Vec::new());
        let error = wait(
            waiting("loading, given up at 12:40"),
            &FAST,
            patience(Some(Duration::ZERO), || false, &heard),
            |_| panic!("a spent budget does not poll"),
            |_| panic!("a spent budget leaves the request queued"),
        )
        .unwrap_err();
        assert!(error.contains("stays queued"), "{error}");
        assert!(error.contains("request #7"), "{error}");
        assert!(error.contains("loading, given up at 12:40"), "{error}");
    }

    #[test]
    fn a_failed_load_ends_the_wait_with_its_reason() {
        let heard = RefCell::new(Vec::new());
        let error = wait(
            waiting("loading"),
            &FAST,
            patience(None, || false, &heard),
            |_| Err("dsv4f-tp2 did not load: health timed out".to_string()),
            |_| panic!("a failed request is not cancelled"),
        )
        .unwrap_err();
        assert_eq!(error, "dsv4f-tp2 did not load: health timed out");
    }

    #[test]
    fn a_declaration_lies_over_the_record_and_one_for_no_profile_changes_nothing() {
        let mut inventory = Inventory::from_yaml(
            r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
profiles:
  - id: dsv4f-tp2
    model: {name: deepseek-ai/DeepSeek-V4-Flash-Vision-Exp}
    servedName: deepseek-v4-flash-vision-exp
    engine: {kind: command, start: ./start.sh}
    requirements: {machines: 1}
deployments: []
pools: []
"#,
        )
        .unwrap();
        let declarations = BTreeMap::from([
            (
                "dsv4f-tp2".to_string(),
                InferenceProfileDeclaration {
                    verbs: Some(BTreeSet::from([VerbKind::Session, VerbKind::Decide])),
                    input_modalities: Some(vec![Modality::Text, Modality::Image]),
                    output_modalities: Some(vec![Modality::Text]),
                },
            ),
            (
                "gone".to_string(),
                InferenceProfileDeclaration {
                    verbs: Some(BTreeSet::from([VerbKind::Embed])),
                    ..Default::default()
                },
            ),
        ]);
        let before = inventory.clone();
        declare(&mut inventory, &declarations);
        let profile: &InferenceProfile =
            &inventory.profiles[&InferenceProfileId::from("dsv4f-tp2")];
        assert_eq!(
            profile.verbs,
            Some(BTreeSet::from([VerbKind::Session, VerbKind::Decide]))
        );
        assert_eq!(
            profile
                .modalities
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("text+image → text")
        );
        assert_eq!(inventory.profiles.len(), before.profiles.len());
    }
}
