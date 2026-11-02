//! Routing a model request to the cairn-inference deployment that serves it,
//! loading the model on demand (tenets 1321, 1336).
//!
//! A caller's name is turned into the model's real name here
//! ([`resolve_model`]: a deployment id, profile id or served name names no
//! model), and every call is then a request to placement
//! ([`crate::fleet::models::request`]), which sees only the real name and the
//! verb the call serves: it chooses among the model's deployments whose
//! profile serves that verb, a loaded one answers on the same pass, and an
//! unloaded one loads while the caller waits on what its request waits on. A
//! model is never served by another, nor a verb by a deployment that does not
//! serve it. Every call counts as demand, which is what keeps a model
//! in use from looking idle to placement.
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
use cairn_inference::served::{
    self, deployments_serving, profile_verbs, EngineMeasure, ServedModel,
};
use cairn_inference::{
    unknown_address, Addresses, Deployment, DeploymentId, InferenceProfileId, Inventory, Network,
};
use cairn_models::types::catalog::{Modalities, VerbKind};

use super::inference::load_inventory;
use super::inference_control::deployment_phases;
use super::Orchestrator;
use crate::fleet::models::requests::RequestState;
use crate::fleet::models::settings::Ms;
use crate::fleet::models::{self, RequestHandle};
use crate::fleet::pools::PoolKey;
use crate::storage::run_db_blocking;

/// How long one engine probe may take.
const MEASURE_TIMEOUT: Duration = Duration::from_secs(3);

/// Where one request for a served model goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Route {
    pub deployment: DeploymentId,
    pub pool: PoolKey,
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
/// has never acted on is probed once, and adopted if it answers.
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
    let healthy = healthy_now(orch).await?;
    let network = orch.fleet.network();
    let addresses = Addresses::new(&inventory, &network);
    for deployment in inventory.deployments.values() {
        if !healthy.contains(&deployment.id) {
            forget(&deployment.id);
            continue;
        }
        let (Some(profile), Some(head)) = (
            inventory.profiles.get(&deployment.profile),
            addresses.from_orch(&deployment.head),
        ) else {
            continue;
        };
        let load = models::healthy_since(orch, &deployment.id).await;
        measure(deployment, &head.address, &profile.served_name, load).await;
    }
    Ok(served::served_models(&inventory, &measures()))
}

/// The real name of the model a caller's name means: the model's own name,
/// or a served name, alias, profile or deployment id that resolves to exactly
/// one model. Placement takes only the real name.
pub(crate) fn resolve_model(inventory: &Inventory, name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("name the model to load".to_string());
    }
    let models: std::collections::BTreeSet<&str> = deployments_serving(inventory, name)
        .iter()
        .filter_map(|id| inventory.deployments.get(id))
        .filter_map(|d| inventory.profiles.get(&d.profile))
        .map(|profile| profile.model.name.as_str())
        .collect();
    match models.into_iter().collect::<Vec<_>>().as_slice() {
        [] => Err(format!(
            "no deployment serves '{name}'; a model is named by its real name, and cairn://inference/profiles lists them"
        )),
        [one] => Ok((*one).to_string()),
        many => Err(format!(
            "'{name}' could mean {}; name the model",
            many.join(", ")
        )),
    }
}

/// Whether `deployment`'s profile serves `verb`.
fn deployment_serves(inventory: &Inventory, deployment: &DeploymentId, verb: VerbKind) -> bool {
    inventory
        .deployments
        .get(deployment)
        .and_then(|record| inventory.profiles.get(&record.profile))
        .is_some_and(|profile| profile_verbs(profile).contains(&verb))
}

/// Ask once for `model` to serve `verb`, on behalf of `requester`: placement
/// routes it only to a deployment whose profile serves `verb`. While one of
/// the requester's requests for the model still waits on such a deployment,
/// it is reused.
pub(crate) async fn ask(
    orch: &Orchestrator,
    model: &str,
    verb: VerbKind,
    requester: &str,
) -> Result<Asked, String> {
    let inventory = served_inventory(orch).await?;
    let real = resolve_model(&inventory, model)?;
    let queued = models::store::load_requests(&orch.db.local)
        .await?
        .into_iter()
        .find(|request| {
            request.model.eq_ignore_ascii_case(&real)
                && request.requester == requester
                && request.state == RequestState::Waiting
                && deployment_serves(&inventory, &request.deployment, verb)
        });
    let handle = match queued {
        Some(request) => RequestHandle {
            id: request.id,
            pool: request.pool,
            deployment: request.deployment,
            model: request.model,
        },
        None => models::request(orch, &real, Some(verb), requester, None).await?,
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
    match &status.request.state {
        RequestState::Ready { .. } => {
            let load = models::healthy_since(orch, &handle.deployment).await;
            Ok(Asked::Ready(
                route(
                    inventory,
                    &orch.fleet.network(),
                    &handle.deployment,
                    &handle.pool,
                    load,
                )
                .await?,
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
    network: &Network,
    deployment: &DeploymentId,
    pool: &PoolKey,
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
    let head = Addresses::new(inventory, network)
        .from_orch(&record.head)
        .ok_or_else(|| {
            format!(
                "{deployment}'s API cannot be reached because its head {}",
                unknown_address(&record.head)
            )
        })?;
    let measure = measure(record, &head.address, &profile.served_name, load).await;
    Ok(Route {
        deployment: deployment.clone(),
        pool: pool.clone(),
        api_base: record.api_base(&head.address),
        served_name: profile.served_name.clone(),
        context_length: measure.context_length(),
    })
}

/// Wait until `model` is ready to serve `verb` for `requester`, then say
/// where to send it.
///
/// `budget` bounds the wait (`None` waits until the request is answered or
/// cancelled), `cancelled` is polled throughout, and `waiting` hears each new
/// description of what the request waits on. Blocking: call it from a thread
/// that may block, never from async code.
pub(crate) fn ready_blocking(
    orch: &Orchestrator,
    model: &str,
    verb: VerbKind,
    requester: &str,
    budget: Option<Duration>,
    cancelled: impl Fn() -> bool,
    waiting: impl FnMut(&str),
) -> Result<Route, String> {
    let first = run_db_blocking(|| ask(orch, model, verb, requester))?;
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
            if let Err(error) = run_db_blocking(|| models::cancel(orch, handle.id, requester)) {
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
pub(crate) mod tests {
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
            pool: "bginf-sp1+bginf-sp2".into(),
            deployment: "dsv4f-tp2".into(),
            model: "deepseek-ai/DeepSeek-V4-Flash-Vision-Exp".into(),
        }
    }

    fn ready() -> Route {
        Route {
            deployment: "dsv4f-tp2".into(),
            pool: "bginf-sp1+bginf-sp2".into(),
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

    /// Wait on `first` as an uncancelled caller does, each later read of its
    /// request answered by `polls`, at a pace a test can wait for.
    pub(crate) fn waited_for(
        first: Asked,
        heard: &RefCell<Vec<String>>,
        polls: impl FnMut(&RequestHandle) -> Result<Asked, String>,
    ) -> Result<Route, String> {
        wait(first, &FAST, patience(None, || false, heard), polls, |_| {
            panic!("an uncancelled caller cancels nothing")
        })
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
machines: []
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
