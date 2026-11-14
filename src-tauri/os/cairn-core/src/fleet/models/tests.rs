//! The rules placement runs local models by, stepped pass by pass the way the
//! host runs them: each plan's state and settled requests feed the next.

use std::collections::{BTreeMap, BTreeSet};

use cairn_common::executor_protocol::MachineRole;
use cairn_inference::control::Phase;
use cairn_inference::DeploymentId;

use super::requests::{choose_deployment, HoldCause, Request, RequestId, RequestState, Wait};
use super::rules::{
    plan, Acquirable, Activity, Input, Left, Pin, Plan, Preference, State, Why, RESTOP_AFTER_MS,
};
use super::settings::{EffectiveSettings, MeasuredLoad, Ms, Setting, SettingOrigin};
use crate::fleet::pools::{Pool, PoolKey, PoolMember};
use crate::orchestrator::inference_control::LoadRow;

pub(crate) async fn seed_catalog_placement(orch: &crate::orchestrator::Orchestrator, model: &str) {
    use cairn_models::types::catalog::VerbKind;
    let mut book = orch.fleet.models.book.lock().await;
    let mut deployment = acquirable("catalog-edit", model, &["catalog-box"], "catalog-box");
    deployment.verbs = BTreeSet::from([VerbKind::Complete, VerbKind::Session, VerbKind::Decide]);
    book.catalog
        .insert(deployment.deployment.clone(), deployment);
    book.pools.push(pool(&["catalog-box"]));
}

const MIN: Ms = 60_000;
const SPARKS: &str = "sp1+sp2";

fn d(id: &str) -> DeploymentId {
    id.into()
}

fn pool(machines: &[&str]) -> Pool {
    Pool {
        key: PoolKey::of(machines.iter().copied()),
        members: machines
            .iter()
            .map(|machine| PoolMember {
                machine: machine.to_string(),
                link_address: None,
            })
            .collect(),
        rate_gbps: (machines.len() > 1).then_some(200),
    }
}

fn acquirable(id: &str, model: &str, machines: &[&str], pool: &str) -> Acquirable {
    Acquirable {
        deployment: d(id),
        model: model.to_string(),
        machines: machines.iter().map(|m| m.to_string()).collect(),
        head: machines[0].to_string(),
        pool: Some(pool.into()),
        refused: None,
        verbs: BTreeSet::new(),
    }
}

/// sp1 and sp2 joined by a fast link: `dsv4f-tp2` spans both, `qwen36-fast`
/// is on sp1 and `other` on sp2.
fn catalog() -> BTreeMap<DeploymentId, Acquirable> {
    [
        acquirable("dsv4f-tp2", "deepseek", &["sp1", "sp2"], SPARKS),
        acquirable("qwen36-fast", "qwen", &["sp1"], SPARKS),
        acquirable("other", "other", &["sp2"], SPARKS),
    ]
    .into_iter()
    .map(|a| (a.deployment.clone(), a))
    .collect()
}

/// Every window ten minutes, so the arithmetic is legible.
fn ten_minutes() -> EffectiveSettings {
    let setting = Setting {
        secs: 600,
        origin: SettingOrigin::Operator,
    };
    EffectiveSettings {
        idle: setting,
        min_residency: setting,
        load_bound: setting,
    }
}

fn request(id: RequestId, deployment: &str, at: Ms) -> Request {
    Request {
        id,
        pool: SPARKS.into(),
        deployment: d(deployment),
        model: deployment.to_string(),
        requester: format!("agent-{id}"),
        at,
        state: RequestState::Waiting,
    }
}

fn ready(mut request: Request, at: Ms) -> Request {
    request.state = RequestState::Ready { at };
    request
}

fn healthy(since_ms: Ms) -> Phase {
    Phase::Healthy {
        since_ms,
        load_ms: None,
    }
}

fn starting(since_ms: Ms) -> Phase {
    Phase::Starting {
        since_ms,
        requested_ms: since_ms,
    }
}

fn reserving(since_ms: Ms) -> Phase {
    Phase::Reserving {
        since_ms,
        waiting_on: None,
    }
}

fn failed(at_ms: Ms, reason: &str) -> Phase {
    Phase::Failed {
        at_ms,
        reason: reason.into(),
    }
}

fn machines(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|m| m.to_string()).collect()
}

/// Placement over the Sparks, stepped pass by pass.
struct Harness {
    pools: Vec<Pool>,
    catalog: BTreeMap<DeploymentId, Acquirable>,
    state: State,
    phases: BTreeMap<DeploymentId, Phase>,
    requests: Vec<Request>,
    pins: BTreeMap<DeploymentId, Pin>,
    preferences: BTreeMap<PoolKey, Preference>,
    settings: BTreeMap<PoolKey, EffectiveSettings>,
    activity: Activity,
    last: Option<Plan>,
}

impl Harness {
    /// Placement starting at 0 with nothing running.
    fn new() -> Self {
        Self {
            pools: vec![pool(&["sp1", "sp2"])],
            catalog: catalog(),
            state: State::default(),
            phases: BTreeMap::new(),
            requests: Vec::new(),
            pins: BTreeMap::new(),
            preferences: BTreeMap::new(),
            settings: BTreeMap::from([(SPARKS.into(), ten_minutes())]),
            activity: Activity::default(),
            last: None,
        }
    }

    /// Placement starting at 0 with `id` healthy since then, adopted.
    fn with_loaded(id: &str) -> Self {
        let mut harness = Self::new();
        harness.phases.insert(d(id), healthy(0));
        harness.tick(0);
        harness
    }

    fn tick(&mut self, now: Ms) -> Plan {
        let plan = plan(
            &self.state,
            Input {
                now,
                pools: &self.pools,
                catalog: &self.catalog,
                phases: &self.phases,
                requests: &self.requests,
                pins: &self.pins,
                preferences: &self.preferences,
                settings: &self.settings,
                activity: &self.activity,
            },
        );
        self.state = plan.state.clone();
        for (id, state) in &plan.settled {
            let request = self.requests.iter_mut().find(|r| r.id == *id).unwrap();
            request.state = state.clone();
        }
        // A room refusal is reported once, and read once.
        self.activity.refused.clear();
        self.last = Some(plan.clone());
        plan
    }

    fn refused(&mut self, names: &[&str]) {
        self.activity.refused = machines(names);
    }

    fn residents(&self) -> Vec<&str> {
        self.state.residents.keys().map(|id| id.as_str()).collect()
    }

    fn wait(&self, id: RequestId) -> &Wait {
        &self.last.as_ref().unwrap().waits[&id]
    }

    fn state_of(&self, id: RequestId) -> &RequestState {
        &self.requests.iter().find(|r| r.id == id).unwrap().state
    }

    fn left(&self) -> Vec<(&str, &Left)> {
        self.last
            .as_ref()
            .unwrap()
            .departures
            .iter()
            .map(|d| (d.deployment.as_str(), &d.why))
            .collect()
    }
}

// Requests acquire their model.

#[test]
fn a_request_for_an_unloaded_model_acquires_it_and_asks_for_the_start() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 1_000));
    let plan = h.tick(2_000);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    assert!(matches!(
        h.state.residents[&d("dsv4f-tp2")].why,
        Why::Requested { request: 1, .. }
    ));
    assert_eq!(h.wait(1), &Wait::Starting);
    assert_eq!(plan.starts, [d("dsv4f-tp2")]);
    assert!(plan.stops.is_empty());
}

#[test]
fn a_request_waits_visibly_through_fit_load_and_ready() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(1_000);
    h.phases.insert(
        d("dsv4f-tp2"),
        Phase::Reserving {
            since_ms: 1_000,
            waiting_on: Some("memory: sp2 has 64.0 GiB free and needs 101.0 GiB".into()),
        },
    );
    let plan = h.tick(2_000);
    assert!(
        plan.starts.is_empty(),
        "reserving: the start is placement's to launch"
    );
    let Wait::Fitting { since, waiting_on } = h.wait(1) else {
        panic!("fitting: {:?}", h.wait(1));
    };
    assert_eq!(*since, 1_000);
    assert!(waiting_on
        .as_deref()
        .unwrap()
        .contains("sp2 has 64.0 GiB free"));
    assert!(h
        .wait(1)
        .describe(2_000)
        .starts_with("waiting to fit since 1s ago, its machines' room reserved for it; memory: sp2 has 64.0 GiB free and needs 101.0 GiB"));

    h.phases.insert(d("dsv4f-tp2"), starting(3_000));
    h.tick(4_000);
    assert_eq!(
        h.wait(1),
        &Wait::Loading {
            since: 3_000,
            bound_at: 3_000 + 10 * MIN
        }
    );

    h.phases.insert(d("dsv4f-tp2"), healthy(5 * MIN));
    h.tick(6 * MIN);
    assert_eq!(h.state_of(1), &RequestState::Ready { at: 6 * MIN });
    assert_eq!(
        h.state.residents[&d("dsv4f-tp2")].healthy_since,
        Some(5 * MIN),
        "residency counts from the first healthy observation"
    );
}

#[test]
fn a_request_for_a_loaded_model_is_ready_at_once() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    assert!(matches!(
        h.state.residents[&d("dsv4f-tp2")].why,
        Why::Adopted
    ));
    h.requests.push(request(1, "dsv4f-tp2", MIN));
    h.tick(MIN);
    assert_eq!(h.state_of(1), &RequestState::Ready { at: MIN });
    assert!(matches!(
        h.state.residents[&d("dsv4f-tp2")].why,
        Why::Adopted
    ));
}

#[test]
fn what_runs_unasked_is_adopted_and_an_unknown_deployment_is_left_alone() {
    let mut h = Harness::new();
    h.phases.insert(d("qwen36-fast"), healthy(0));
    let plan = h.tick(MIN);
    assert_eq!(h.residents(), ["qwen36-fast"], "found answering: adopted");
    assert_eq!(
        h.state.residents[&d("qwen36-fast")].healthy_since,
        Some(MIN),
        "its residency counts from the adoption"
    );
    assert!(plan.stops.is_empty() && plan.starts.is_empty());
    assert!(
        !h.state.residents.contains_key(&d("other")),
        "a deployment with no phase is unknown: never started or stopped on that account"
    );

    // Unknown again (its row is gone): it keeps its residency, and nothing
    // stops it.
    h.phases.remove(&d("qwen36-fast"));
    let plan = h.tick(2 * MIN);
    assert_eq!(
        h.state.residents[&d("qwen36-fast")].healthy_since,
        Some(MIN)
    );
    assert!(plan.stops.is_empty());
}

#[test]
fn a_resident_waits_for_what_placement_released_to_stop() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.requests.push(request(1, "dsv4f-tp2", 30 * MIN));
    let plan = h.tick(30 * MIN);
    assert_eq!(
        h.residents(),
        ["dsv4f-tp2"],
        "qwen36-fast was idle and gave way"
    );
    assert_eq!(
        h.left(),
        [("qwen36-fast", &Left::Displaced { by: d("dsv4f-tp2") })]
    );
    assert_eq!(plan.stops, [d("qwen36-fast")]);
    assert!(
        plan.starts.is_empty(),
        "not while sp1 still holds the other model"
    );
    assert_eq!(
        h.wait(1),
        &Wait::Unloading {
            deployments: vec![d("qwen36-fast")]
        }
    );
    assert_eq!(
        h.wait(1).describe(30 * MIN),
        "unloading qwen36-fast from its machines"
    );

    h.phases
        .insert(d("qwen36-fast"), Phase::Stopped { since_ms: 31 * MIN });
    let plan = h.tick(31 * MIN);
    assert_eq!(plan.starts, [d("dsv4f-tp2")]);
    assert!(h.state.released.is_empty(), "stopped: done with");
}

// Eviction for room.

#[test]
fn an_idle_resident_is_evicted_when_work_is_refused_its_room() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.refused(&["sp1"]);
    let plan = h.tick(30 * MIN);
    assert!(h.residents().is_empty());
    assert_eq!(
        h.left(),
        [(
            "dsv4f-tp2",
            &Left::Evicted {
                machines: vec!["sp1".into()]
            }
        )]
    );
    assert_eq!(plan.stops, [d("dsv4f-tp2")]);
    assert_eq!(
        plan.departures[0].describe(30 * MIN),
        "deepseek was evicted 0s ago: work was refused for the room it held on sp1 while it was idle"
    );
}

#[test]
fn only_the_residents_on_the_refusing_machines_are_evicted() {
    let mut h = Harness::new();
    h.phases.insert(d("qwen36-fast"), healthy(0));
    h.phases.insert(d("other"), healthy(0));
    h.tick(0);
    h.refused(&["sp2"]);
    h.tick(30 * MIN);
    assert_eq!(h.residents(), ["qwen36-fast"], "sp1 refused nothing");
}

#[test]
fn a_model_requested_within_its_idle_window_stays_while_work_is_refused() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests
        .push(ready(request(1, "dsv4f-tp2", 25 * MIN), 25 * MIN));
    h.refused(&["sp1"]);
    h.tick(30 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    h.refused(&["sp1"]);
    h.tick(35 * MIN);
    assert!(h.residents().is_empty(), "idle since 25m plus 10m");
}

#[test]
fn an_idle_model_stays_when_nothing_is_refused_its_room() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.activity.running = machines(&["sp1", "sp2"]);
    h.tick(60 * MIN);
    assert_eq!(
        h.residents(),
        ["dsv4f-tp2"],
        "batches beside it are no reason"
    );
}

#[test]
fn an_idle_resident_reads_the_same_evictable_instant_on_every_pass() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    let first = h.tick(30 * MIN).evictable_at[&d("dsv4f-tp2")];
    let second = h.tick(31 * MIN).evictable_at[&d("dsv4f-tp2")];
    assert_eq!(first, second, "nothing changed, so nothing is republished");
    assert!(
        first.is_some_and(|at| at <= 30 * MIN),
        "the hold that lapsed, not the pass's clock: {first:?}"
    );
}

#[test]
fn the_minimum_residency_holds_a_fresh_load_against_refusals() {
    let mut h = Harness::new();
    h.settings.insert(
        SPARKS.into(),
        EffectiveSettings {
            min_residency: Setting {
                secs: 1_200,
                origin: SettingOrigin::Operator,
            },
            ..ten_minutes()
        },
    );
    h.phases.insert(d("dsv4f-tp2"), healthy(0));
    h.tick(0);
    h.refused(&["sp1"]);
    h.tick(19 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    let hold = h.last.as_ref().unwrap().holds[&d("dsv4f-tp2")]
        .clone()
        .unwrap();
    assert_eq!(hold.because, HoldCause::Residency);
    assert_eq!(hold.until, Some(20 * MIN));
    h.refused(&["sp1"]);
    h.tick(20 * MIN);
    assert!(h.residents().is_empty());
}

#[test]
fn a_load_in_progress_is_not_evicted() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.requests[0].state = RequestState::Cancelled { at: MIN };
    h.phases.insert(d("dsv4f-tp2"), starting(MIN));
    h.refused(&["sp1"]);
    h.tick(2 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"], "mid-load");
    h.phases.insert(d("dsv4f-tp2"), healthy(3 * MIN));
    h.refused(&["sp1"]);
    h.tick(12 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"], "inside its residency");
    h.refused(&["sp1"]);
    h.tick(13 * MIN);
    assert!(h.residents().is_empty());
}

#[test]
fn a_chosen_model_that_never_started_yields_at_once() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.requests[0].state = RequestState::Cancelled { at: 1 };
    h.refused(&["sp2"]);
    h.tick(2);
    assert!(h.residents().is_empty());
}

#[test]
fn a_model_still_waiting_to_fit_yields_at_once_with_no_request() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.phases.insert(d("dsv4f-tp2"), reserving(0));
    h.requests[0].state = RequestState::Cancelled { at: 1 };
    h.refused(&["sp1"]);
    let plan = h.tick(2);
    assert!(h.residents().is_empty(), "never loaded: nothing to protect");
    assert_eq!(
        plan.stops,
        [d("dsv4f-tp2")],
        "the stop from reserving sends nothing"
    );
}

#[test]
fn a_model_with_requests_waiting_on_it_is_never_evicted() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    // Never loaded, so nothing but the waiting request protects it.
    h.phases.insert(d("dsv4f-tp2"), reserving(0));
    h.refused(&["sp1"]);
    h.tick(5 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    assert!(matches!(
        h.last.as_ref().unwrap().holds[&d("dsv4f-tp2")],
        Some(super::requests::Hold {
            because: HoldCause::Wanted { requests: 1 },
            ..
        })
    ));
}

// The warm preference.

fn with_warm(h: &mut Harness, model: &str) {
    h.preferences.insert(
        SPARKS.into(),
        Preference {
            warm_model: Some(model.into()),
            idle_override_secs: None,
        },
    );
}

#[test]
fn an_idle_pool_places_its_warm_model_after_its_idle_window() {
    let mut h = Harness::new();
    with_warm(&mut h, "deepseek");
    h.tick(0);
    h.tick(9 * MIN);
    assert!(h.residents().is_empty());
    let plan = h.tick(10 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    assert_eq!(
        h.state.residents[&d("dsv4f-tp2")].why,
        Why::Warm { idle_since: 0 }
    );
    assert_eq!(plan.starts, [d("dsv4f-tp2")]);
}

#[test]
fn running_batches_and_room_refusals_restart_the_pool_idle_window() {
    let mut h = Harness::new();
    with_warm(&mut h, "deepseek");
    h.tick(0);
    h.activity.running = machines(&["sp2"]);
    h.tick(MIN);
    h.activity.running.clear();
    h.tick(10 * MIN);
    assert!(h.residents().is_empty(), "a batch ran a minute in");
    h.refused(&["sp1"]);
    h.tick(10 * MIN + 1);
    h.tick(20 * MIN);
    assert!(
        h.residents().is_empty(),
        "a refusal restarts the window too"
    );
    h.tick(20 * MIN + 1);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
}

#[test]
fn after_an_eviction_the_warm_model_returns_only_once_the_pool_is_idle() {
    let mut h = Harness::new();
    with_warm(&mut h, "deepseek");
    h.phases.insert(d("dsv4f-tp2"), healthy(0));
    h.tick(0);
    h.refused(&["sp1"]);
    h.tick(30 * MIN);
    assert!(h.residents().is_empty());
    h.activity.running = machines(&["sp1"]);
    h.tick(35 * MIN);
    h.activity.running.clear();
    h.tick(44 * MIN);
    assert!(h.residents().is_empty());
    h.tick(45 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
}

#[test]
fn an_idle_pool_replaces_an_idle_other_model_with_its_warm_model() {
    let mut h = Harness::with_loaded("qwen36-fast");
    with_warm(&mut h, "deepseek");
    h.tick(9 * MIN);
    assert_eq!(h.residents(), ["qwen36-fast"]);
    h.tick(10 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    assert_eq!(
        h.left(),
        [("qwen36-fast", &Left::Displaced { by: d("dsv4f-tp2") })]
    );
}

#[test]
fn a_warm_model_that_fails_is_retried_once_a_window() {
    let mut h = Harness::new();
    with_warm(&mut h, "deepseek");
    h.tick(0);
    h.tick(10 * MIN);
    h.phases
        .insert(d("dsv4f-tp2"), failed(11 * MIN, "out of memory"));
    h.tick(11 * MIN);
    assert!(h.residents().is_empty());
    h.tick(20 * MIN);
    assert!(h.residents().is_empty(), "counted from the failure");
    h.tick(21 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
}

// Pins: standing leases.

#[test]
fn a_pin_acquires_its_model_displaces_others_and_holds_against_refusals() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.requests.push(ready(request(1, "qwen36-fast", MIN), MIN));
    h.pins.insert(
        d("dsv4f-tp2"),
        Pin {
            holder: "operator".into(),
            at: 2 * MIN,
        },
    );
    h.tick(2 * MIN);
    assert_eq!(
        h.residents(),
        ["dsv4f-tp2"],
        "a pin wins over a recent request"
    );
    assert_eq!(
        h.state.residents[&d("dsv4f-tp2")].why,
        Why::Pinned {
            holder: "operator".into()
        }
    );
    h.phases.insert(d("dsv4f-tp2"), healthy(3 * MIN));
    h.refused(&["sp1", "sp2"]);
    h.tick(90 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"], "never idle");
    h.requests.push(request(2, "other", 91 * MIN));
    h.tick(91 * MIN);
    assert_eq!(
        h.wait(2).describe(91 * MIN),
        "deepseek is pinned by operator"
    );
}

#[test]
fn a_pinned_deployment_is_started_but_a_failure_since_the_pin_stands() {
    let mut h = Harness::new();
    h.phases
        .insert(d("dsv4f-tp2"), failed(MIN, "before the pin"));
    h.pins.insert(
        d("dsv4f-tp2"),
        Pin {
            holder: "operator".into(),
            at: 2 * MIN,
        },
    );
    let plan = h.tick(2 * MIN);
    assert_eq!(plan.starts, [d("dsv4f-tp2")]);
    h.phases
        .insert(d("dsv4f-tp2"), failed(3 * MIN, "since the pin"));
    h.requests.push(request(1, "dsv4f-tp2", 3 * MIN));
    let plan = h.tick(4 * MIN);
    assert!(plan.starts.is_empty(), "not retried while pinned");
    assert!(plan.stops.is_empty(), "and not released");
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    assert!(
        matches!(h.state_of(1), RequestState::Failed { reason, .. } if reason.contains("since the pin"))
    );
}

// Between two models.

#[test]
fn the_loaded_model_keeps_its_machines_until_its_idle_window_ends() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests
        .push(ready(request(1, "dsv4f-tp2", 20 * MIN), 20 * MIN));
    h.requests.push(request(2, "qwen36-fast", 21 * MIN));
    h.tick(21 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
    let Wait::Held { by } = h.wait(2) else {
        panic!("{:?}", h.wait(2));
    };
    assert_eq!(by[0].because, HoldCause::RecentRequest);
    assert_eq!(by[0].until, Some(30 * MIN));
    assert_eq!(
        h.wait(2).describe(21 * MIN),
        "deepseek was requested within its idle window and keeps its machines, until in 9m"
    );
    h.tick(30 * MIN);
    assert_eq!(h.residents(), ["qwen36-fast"]);
}

#[test]
fn queue_depth_does_not_move_a_model_off_its_machines() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests
        .push(ready(request(1, "dsv4f-tp2", 20 * MIN), 20 * MIN));
    for id in 2..12 {
        h.requests.push(request(id, "qwen36-fast", 21 * MIN));
    }
    h.tick(25 * MIN);
    assert_eq!(h.residents(), ["dsv4f-tp2"]);
}

#[test]
fn a_request_on_free_machines_joins_without_displacing_anything() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.requests.push(request(1, "other", MIN));
    let plan = h.tick(MIN);
    assert_eq!(h.residents(), ["other", "qwen36-fast"]);
    assert!(plan.departures.is_empty());
    assert_eq!(plan.starts, [d("other")]);
}

#[test]
fn an_earlier_request_keeps_its_machines_against_a_later_one() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.requests.push(ready(request(1, "qwen36-fast", MIN), MIN));
    // dsv4f-tp2 asks first and waits for qwen36-fast on sp1; other asks later
    // for sp2, which dsv4f-tp2 is owed.
    h.requests.push(request(2, "dsv4f-tp2", 2 * MIN));
    h.requests.push(request(3, "other", 3 * MIN));
    h.tick(3 * MIN);
    assert_eq!(h.residents(), ["qwen36-fast"]);
    assert_eq!(
        h.wait(3),
        &Wait::Queued {
            behind: 2,
            deployment: d("dsv4f-tp2")
        }
    );
    h.tick(11 * MIN);
    assert_eq!(
        h.residents(),
        ["dsv4f-tp2"],
        "the earlier request goes first"
    );
}

// Failures.

#[test]
fn a_load_past_its_bound_fails_its_requests_and_is_released() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.phases.insert(d("dsv4f-tp2"), starting(MIN));
    h.tick(2 * MIN);
    let plan = h.tick(11 * MIN);
    assert!(h.residents().is_empty());
    assert!(matches!(
        h.state_of(1),
        RequestState::Failed { reason, .. }
            if reason == "dsv4f-tp2 failed to load: it did not come healthy within the load bound of 10m (operator)"
    ));
    assert_eq!(plan.stops, [d("dsv4f-tp2")]);
}

#[test]
fn a_reported_failure_fails_the_attempt_but_an_old_one_does_not() {
    let mut h = Harness::new();
    h.phases.insert(d("dsv4f-tp2"), failed(0, "an old failure"));
    h.requests.push(request(1, "dsv4f-tp2", MIN));
    let plan = h.tick(MIN);
    assert_eq!(plan.starts, [d("dsv4f-tp2")], "failed before it was chosen");
    assert_eq!(h.state_of(1), &RequestState::Waiting);
    h.phases
        .insert(d("dsv4f-tp2"), failed(2 * MIN, "out of memory"));
    h.tick(2 * MIN);
    assert!(matches!(
        h.state_of(1),
        RequestState::Failed { reason, .. } if reason == "dsv4f-tp2 failed to load: out of memory"
    ));
}

#[test]
fn a_released_failure_is_stopped_again_only_after_a_minute() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.phases.insert(d("dsv4f-tp2"), failed(MIN, "crashed"));
    let plan = h.tick(MIN);
    assert!(plan.stops.is_empty());
    assert!(h.state.released.contains(&d("dsv4f-tp2")));
    let plan = h.tick(MIN + RESTOP_AFTER_MS);
    assert_eq!(plan.stops, [d("dsv4f-tp2")]);
}

#[test]
fn a_request_for_a_deployment_that_cannot_run_fails_naming_why() {
    let mut h = Harness::new();
    let unjoined = h.catalog.get_mut(&d("dsv4f-tp2")).unwrap();
    unjoined.pool = None;
    unjoined.refused = Some("no fast link joins sp1 and sp2, so they are not one pool".into());
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    assert!(matches!(
        h.state_of(1),
        RequestState::Failed { reason, .. }
            if reason == "dsv4f-tp2 cannot run: no fast link joins sp1 and sp2, so they are not one pool"
    ));
    assert!(h.residents().is_empty());
}

// What placement may acquire.

fn inventory() -> cairn_inference::Inventory {
    cairn_inference::Inventory::from_yaml(
        r#"
models:
  - {name: deepseek-ai/DeepSeek-V4-Flash}
  - {name: nvidia/Qwen3.6-35B}
  - {name: Qwen/Qwen3-Embedding-4B}
profiles:
  - {id: dsv4f-tp2, model: {name: deepseek-ai/DeepSeek-V4-Flash}, servedName: deepseek-v4-flash, engine: {kind: command, start: ./start.sh}, requirements: {machines: 2}, aliases: [dsv4-flash]}
  - {id: qwen36-fast, model: {name: nvidia/Qwen3.6-35B}, servedName: qwen36-fast, engine: {kind: command, start: ./start.sh}, requirements: {machines: 1}}
  - {id: embed, model: {name: Qwen/Qwen3-Embedding-4B}, servedName: embed, engine: {kind: command, start: ./start.sh}, requirements: {machines: 1}}
deployments:
  - {id: dsv4f-tp2, profile: dsv4f-tp2, machines: [sp1, sp2], head: sp1, apiPort: 8888, apiPath: /v1, healthPath: /v1/models}
  - {id: qwen36-fast, profile: qwen36-fast, machines: [sp1], head: sp1, apiPort: 8001, apiPath: /v1, healthPath: /v1/models}
  - {id: embed-fedora, profile: embed, machines: [fedora], head: fedora, apiPort: 8090, apiPath: /v1, healthPath: /health}
  - {id: embed-bbs, profile: embed, machines: [bbs], head: bbs, apiPort: 8090, apiPath: /v1, healthPath: /health}
"#,
    )
    .unwrap()
}

#[test]
fn a_gang_must_lie_in_one_detected_pool_off_builds_only_machines() {
    let joined = [pool(&["sp1", "sp2"]), pool(&["fedora"]), pool(&["bbs"])];
    let catalog = super::rules::catalog(&inventory(), &joined, &BTreeMap::new());
    assert_eq!(catalog[&d("dsv4f-tp2")].pool, Some(SPARKS.into()));
    assert_eq!(
        catalog[&d("dsv4f-tp2")].model,
        "deepseek-ai/DeepSeek-V4-Flash"
    );
    assert_eq!(catalog[&d("embed-fedora")].pool, Some("fedora".into()));

    let unjoined = [pool(&["sp1"]), pool(&["sp2"]), pool(&["fedora"])];
    let catalog = super::rules::catalog(&inventory(), &unjoined, &BTreeMap::new());
    assert_eq!(
        catalog[&d("dsv4f-tp2")].pool,
        None,
        "an x2 profile on pools of one"
    );
    assert_eq!(
        catalog[&d("dsv4f-tp2")].refused.as_deref(),
        Some("no fast link joins sp1 and sp2, so they are not one pool")
    );
    assert_eq!(
        catalog[&d("embed-bbs")].refused.as_deref(),
        Some("bbs is in no detected pool: it is not attached or reports no GPU")
    );

    let roles = BTreeMap::from([("fedora".to_string(), MachineRole::BuildsOnly)]);
    let catalog = super::rules::catalog(&inventory(), &joined, &roles);
    assert_eq!(
        catalog[&d("embed-fedora")].refused.as_deref(),
        Some("fedora is builds only, so it runs no inference deployment")
    );

    let roles = BTreeMap::from([("fedora".to_string(), MachineRole::ServicesOnly)]);
    let catalog = super::rules::catalog(&inventory(), &joined, &roles);
    assert_eq!(
        catalog[&d("embed-fedora")].refused.as_deref(),
        Some("fedora is services only, so it runs no inference deployment")
    );

    let roles = BTreeMap::from([("fedora".to_string(), MachineRole::InferenceOnly)]);
    let catalog = super::rules::catalog(&inventory(), &joined, &roles);
    assert_eq!(catalog[&d("embed-fedora")].refused, None);
}

#[test]
fn a_request_names_a_real_model_and_is_never_served_by_another() {
    let pools = [pool(&["sp1", "sp2"]), pool(&["fedora"]), pool(&["bbs"])];
    let catalog = super::rules::catalog(&inventory(), &pools, &BTreeMap::new());
    let none = BTreeMap::new();
    assert_eq!(
        choose_deployment(&catalog, &none, "DEEPSEEK-AI/deepseek-v4-flash", None, None),
        Ok(d("dsv4f-tp2")),
        "exact, case-insensitive"
    );
    for not_a_name in ["dsv4-flash", "deepseek-v4-flash", "dsv4f-tp2", "deepseek"] {
        assert!(
            choose_deployment(&catalog, &none, not_a_name, None, None).is_err(),
            "{not_a_name}: an alias, a served name, an id or a partial name is not a model"
        );
    }
    // Two pools serve the embedder: an idle pool is preferred over one
    // already holding a model, then the lowest id; a named pool narrows it.
    assert_eq!(
        choose_deployment(&catalog, &none, "Qwen/Qwen3-Embedding-4B", None, None),
        Ok(d("embed-bbs"))
    );
    assert_eq!(
        choose_deployment(
            &catalog,
            &none,
            "Qwen/Qwen3-Embedding-4B",
            None,
            Some(&"fedora".into())
        ),
        Ok(d("embed-fedora"))
    );
    let busy_bbs = BTreeMap::from([(
        d("other-on-bbs"),
        super::rules::Resident {
            deployment: d("other-on-bbs"),
            model: "x".into(),
            pool: "bbs".into(),
            machines: machines(&["bbs"]),
            chosen_at: 0,
            loading_since: None,
            healthy_since: Some(0),
            why: Why::Adopted,
        },
    )]);
    assert_eq!(
        choose_deployment(&catalog, &busy_bbs, "Qwen/Qwen3-Embedding-4B", None, None),
        Ok(d("embed-fedora"))
    );
}

/// A request made to serve a verb goes only to a deployment whose profile
/// serves it, whatever placement would otherwise prefer; when none does it
/// is refused naming what each serves.
#[test]
fn a_request_for_a_verb_goes_only_to_a_deployment_that_serves_it() {
    use cairn_models::types::catalog::VerbKind;
    let pools = [pool(&["sp1", "sp2"]), pool(&["fedora"]), pool(&["bbs"])];
    // fedora's profile says nothing, so it is offered for conversation; the
    // bbs copy declares itself an embedder.
    let mut inventory = inventory();
    let mut embedder =
        inventory.profiles[&cairn_inference::InferenceProfileId::from("embed")].clone();
    embedder.id = "embed-declared".into();
    embedder.verbs = Some(BTreeSet::from([VerbKind::Embed]));
    inventory.insert_profile(embedder);
    let mut bbs = inventory.deployments[&d("embed-bbs")].clone();
    bbs.profile = "embed-declared".into();
    inventory.insert_deployment(bbs);
    let catalog = super::rules::catalog(&inventory, &pools, &BTreeMap::new());
    assert_eq!(
        catalog[&d("embed-fedora")].verbs,
        BTreeSet::from([VerbKind::Complete, VerbKind::Session, VerbKind::Decide])
    );
    assert_eq!(
        catalog[&d("embed-bbs")].verbs,
        BTreeSet::from([VerbKind::Embed])
    );

    let embed = "Qwen/Qwen3-Embedding-4B";
    // fedora resident would be preferred, but it does not embed.
    let fedora_resident = BTreeMap::from([(
        d("embed-fedora"),
        super::rules::Resident {
            deployment: d("embed-fedora"),
            model: embed.into(),
            pool: "fedora".into(),
            machines: machines(&["fedora"]),
            chosen_at: 0,
            loading_since: None,
            healthy_since: Some(0),
            why: Why::Adopted,
        },
    )]);
    assert_eq!(
        choose_deployment(
            &catalog,
            &fedora_resident,
            embed,
            Some(VerbKind::Embed),
            None
        ),
        Ok(d("embed-bbs"))
    );
    assert_eq!(
        choose_deployment(
            &catalog,
            &fedora_resident,
            embed,
            Some(VerbKind::Session),
            None
        ),
        Ok(d("embed-fedora"))
    );
    // Narrowed to a pool whose deployment does not embed.
    let refusal = choose_deployment(
        &catalog,
        &fedora_resident,
        embed,
        Some(VerbKind::Embed),
        Some(&"fedora".into()),
    )
    .unwrap_err();
    assert!(refusal.contains("no deployment of"), "{refusal}");
    // No deployment of the model serves the verb at all.
    let refusal = choose_deployment(
        &catalog,
        &fedora_resident,
        embed,
        Some(VerbKind::Image),
        None,
    )
    .unwrap_err();
    assert_eq!(
        refusal,
        "no deployment of 'Qwen/Qwen3-Embedding-4B' serves image: embed-bbs serves embed; embed-fedora serves complete, session, decide; a profile serves what it declares (inference.profiles.<id>.verbs), else what its outputs say"
    );
}

/// A finished load of `secs` to health; `cold` when its start downloaded first.
fn load(secs: u64, cold: bool) -> LoadRow {
    LoadRow {
        profile: String::new(),
        requested_at: 0,
        sent_at: None,
        finished_at: 0,
        healthy: true,
        load_ms: Some(secs * 1_000),
        reason: None,
        footprint: None,
        cold,
    }
}

fn failed_load() -> LoadRow {
    LoadRow {
        healthy: false,
        load_ms: None,
        reason: Some("the start command exited 1".into()),
        ..load(0, false)
    }
}

/// The Sparks pool's windows with `history`, each deployment's loads newest
/// first as `load_history` reads them.
fn sparks_windows(history: &[(&str, Vec<LoadRow>)]) -> EffectiveSettings {
    let history = history
        .iter()
        .map(|(id, loads)| (d(id), loads.clone()))
        .collect();
    let key: PoolKey = SPARKS.into();
    super::pool_settings(
        &[pool(&["sp1", "sp2"])],
        &catalog(),
        &history,
        &BTreeMap::new(),
    )[&key]
}

fn measured(secs: u64, cold: bool) -> SettingOrigin {
    SettingOrigin::Measured(MeasuredLoad { secs, cold })
}

/// The 11-02 bbs case: a first load of 3m 31s that pulled its image, then
/// two of 12 s. The windows follow the 12 s reload, down to their floors.
#[test]
fn a_pools_windows_follow_its_latest_load_not_its_slowest() {
    let windows = sparks_windows(&[(
        "dsv4f-tp2",
        vec![load(12, false), load(12, false), load(211, false)],
    )]);
    assert_eq!(windows.idle.origin, measured(12, false));
    assert_eq!(
        (
            windows.idle.secs,
            windows.min_residency.secs,
            windows.load_bound.secs
        ),
        (15 * 60, 10 * 60, 5 * 60)
    );
    assert_eq!(
        windows.idle.to_string(),
        "15m (from the latest measured load of 12s)"
    );

    let failed_since = sparks_windows(&[("dsv4f-tp2", vec![failed_load(), load(12, false)])]);
    assert_eq!(
        failed_since.idle.origin,
        measured(12, false),
        "a failed start measures nothing"
    );
}

#[test]
fn a_pool_with_one_slow_load_still_derives_from_it() {
    let windows = sparks_windows(&[("dsv4f-tp2", vec![load(211, false)])]);
    assert_eq!(windows.idle.origin, measured(211, false));
    assert_eq!(
        (
            windows.idle.secs,
            windows.min_residency.secs,
            windows.load_bound.secs
        ),
        (1_266, 844, 633)
    );
}

#[test]
fn a_pool_with_no_healthy_load_keeps_the_built_in_windows() {
    for history in [vec![], vec![("dsv4f-tp2", vec![failed_load()])]] {
        let windows = sparks_windows(&history);
        assert_eq!(windows, EffectiveSettings::default());
        assert_eq!(windows.idle.secs, 30 * 60);
        assert_eq!(windows.load_bound.origin, SettingOrigin::BuiltIn);
    }
}

/// A load whose start pulled its image is passed over while a load from cache
/// is on record, and counts, marked so, while none is.
#[test]
fn a_load_that_downloaded_first_counts_only_until_one_from_cache_is_measured() {
    let pulled_again = sparks_windows(&[("dsv4f-tp2", vec![load(211, true), load(12, false)])]);
    assert_eq!(pulled_again.idle.origin, measured(12, false));

    let only_pulled = sparks_windows(&[("dsv4f-tp2", vec![load(211, true)])]);
    assert_eq!(only_pulled.idle.origin, measured(211, true));
    assert_eq!(only_pulled.idle.secs, 1_266);
}

/// One pool's windows cover reloading whichever model it holds: the slowest
/// of its deployments' latest loads.
#[test]
fn a_pool_takes_the_slowest_of_its_deployments_latest_loads() {
    let windows = sparks_windows(&[
        ("dsv4f-tp2", vec![load(300, false), load(600, false)]),
        ("qwen36-fast", vec![load(40, false)]),
    ]);
    assert_eq!(windows.idle.origin, measured(300, false));
    assert_eq!(windows.load_bound.secs, 15 * 60);
}

/// The host: one pass against a real store and a fake fleet.
mod host {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    use cairn_common::executor_protocol::{DeploymentOperation, MachineModelResident, MachineRole};
    use cairn_db::turso::params;
    use cairn_inference::control::Phase;

    use super::super::{
        pass, pin_inner, pin_with, preference_with, release_with, request_with, status_with, store,
        unload_with, Host, ModelPlacement, PinTarget,
    };
    use super::pool;
    use crate::fleet::models::requests::RequestState;
    use crate::fleet::pools::Pool;
    use crate::orchestrator::inference_control::tests::{
        phase, profile, sparks_db, FakeHeads, DSV4F, QWEN, SP1, SP2,
    };
    use crate::orchestrator::inference_control::Heads;
    use crate::storage::LocalDb;
    use cairn_inference::DeploymentId;

    const GIB: u64 = 1 << 30;
    const SPARKS: &str = "bginf-sp1+bginf-sp2";

    #[derive(Default)]
    struct FakeHost {
        heads: FakeHeads,
        models: ModelPlacement,
        pools: Vec<Pool>,
        published: Mutex<HashMap<String, Vec<MachineModelResident>>>,
    }

    impl Host for FakeHost {
        fn heads(&self) -> &dyn Heads {
            &self.heads
        }
        fn models(&self) -> &ModelPlacement {
            &self.models
        }
        fn pools(&self) -> Vec<Pool> {
            self.pools.clone()
        }
        fn roles(&self) -> BTreeMap<String, MachineRole> {
            BTreeMap::new()
        }
        fn publish(&self, by_machine: HashMap<String, Vec<MachineModelResident>>) {
            *self.published.lock().unwrap() = by_machine;
        }
    }

    fn sparks() -> FakeHost {
        FakeHost {
            pools: vec![pool(&[SP1, SP2])],
            ..FakeHost::default()
        }
    }

    async fn footprint(db: &LocalDb, gib: u64) {
        db.execute(
            "INSERT INTO inference_deployment_loads
               (deployment_id, profile_id, requested_at, finished_at, outcome, load_ms, footprint_json)
             VALUES (?1, ?1, 0, 1, 'healthy', 240000, ?2)",
            params![DSV4F, format!("{{\"{SP1}\":{},\"{SP2}\":{}}}", gib * GIB, gib * GIB)],
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn catalog_availability_comes_from_stored_deployments_and_a_real_placement_pass() {
        use std::sync::Arc;

        use cairn_common::query::parse_query_params;
        use cairn_models::interface::{ModelScope, Models};
        use cairn_models::types::catalog::{ModelAvailability, ProviderModelCatalog, VerbKind};

        use crate::db::DbState;
        use crate::orchestrator::inference::load_inventory;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;

        cairn_common::scratch::testing::pin_private_root();
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = Arc::new(sparks_db("models-host-catalog").await);
        db.execute(
            "INSERT INTO inference_deployment_loads
               (deployment_id, profile_id, requested_at, finished_at, outcome, load_ms)
             VALUES (?1, ?1, 0, 1211000, 'healthy', 1211000)",
            params![DSV4F],
        )
        .await
        .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.path().join("search")).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(db.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build();
        let inventory = load_inventory(&db).await.unwrap();
        let cached = ProviderModelCatalog {
            backend: "inference".into(),
            models: cairn_inference::served::served_models(&inventory, &BTreeMap::new())
                .iter()
                .map(crate::backends::inference::discovered)
                .collect(),
            options: vec![],
            refreshed_at: Some(42),
            error: None,
        };
        assert_eq!(cached.models.len(), 2);
        orch.seed_model_catalog_for_test(cached.clone());

        let mut host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.attach(SP2, 110, 0);
        let render = |query: &str| {
            crate::resources::models::render_catalog(&orch, &parse_query_params(query).unwrap())
        };
        let read = || {
            Models::catalog(&orch, &ModelScope::workspace())
                .unwrap()
                .providers
                .into_iter()
                .find(|provider| provider.descriptor.key == "inference")
                .unwrap()
                .models
                .unwrap()
                .models
        };
        let now = crate::fleet::unix_time_ms() as i64;
        let requested = now - 75_000;
        for (phase, label) in [
            (Phase::Stopped { since_ms: now }, "loadable"),
            (
                Phase::Healthy {
                    since_ms: now,
                    load_ms: Some(1_211_000),
                },
                "loaded",
            ),
            (
                Phase::Starting {
                    since_ms: now - 5_000,
                    requested_ms: requested,
                },
                "loading",
            ),
        ] {
            db.execute(
                "INSERT OR REPLACE INTO inference_deployment_state
                   (deployment_id, phase_json, command_id, updated_at)
                 VALUES (?1, ?2, NULL, ?3)",
                params![DSV4F, serde_json::to_string(&phase).unwrap(), now],
            )
            .await
            .unwrap();
            pass(&db, &host, now).await.unwrap();
            let view = super::super::view_with(&db, &host).await.unwrap();
            assert_eq!(
                view.routes[DSV4F][&VerbKind::Complete],
                DeploymentId::from(DSV4F)
            );
            // Only the fleet boundary is fake: the pass built this book from
            // stored inventory, phase, load history and lease records.
            *orch.fleet.models.book.lock().await = host.models.book.lock().await.clone();

            let before = crate::fleet::unix_time_ms() as i64;
            let models = read();
            let model = models.iter().find(|model| model.model == DSV4F).unwrap();
            let availability = model.availability.as_ref().unwrap();
            let after = crate::fleet::unix_time_ms() as i64;
            match (label, availability) {
                (
                    "loadable",
                    ModelAvailability::Loadable {
                        pool,
                        expected_load_ms,
                    },
                ) => {
                    assert_eq!(pool, SPARKS);
                    assert_eq!(*expected_load_ms, Some(1_211_000));
                }
                ("loaded", ModelAvailability::Loaded { pool }) => assert_eq!(pool, SPARKS),
                (
                    "loading",
                    ModelAvailability::Loading {
                        pool,
                        expected_load_ms,
                        elapsed_ms,
                    },
                ) => {
                    assert_eq!(pool, SPARKS);
                    assert_eq!(*expected_load_ms, Some(1_211_000));
                    assert!((before - requested) as u64 <= *elapsed_ms);
                    assert!(*elapsed_ms <= (after - requested) as u64);
                }
                _ => panic!("expected {label}, got {availability:?}"),
            }
            for verb in [VerbKind::Complete, VerbKind::Session, VerbKind::Decide] {
                assert_eq!(model.availability_by_verb.get(&verb), Some(availability));
            }

            let markdown = render("provider=inference&serves=complete");
            let line = markdown.lines().find(|line| line.contains(DSV4F)).unwrap();
            assert!(
                line.contains(&format!("{label} in pool `{SPARKS}`")),
                "{line}"
            );
            if label != "loaded" {
                assert!(line.contains("expected load 20m 11s"), "{line}");
            }
            if label == "loading" {
                assert!(line.contains("elapsed (not remaining time)"), "{line}");
            }
            let json: serde_json::Value =
                serde_json::from_str(&render("provider=inference&serves=complete&format=json"))
                    .unwrap();
            let encoded = json["providers"][0]["catalog"]["models"]
                .as_array()
                .unwrap()
                .iter()
                .find(|model| model["model"] == DSV4F)
                .unwrap();
            assert_eq!(encoded["availability"]["pool"], SPARKS);
            assert_eq!(encoded["availability"]["state"], label);
            assert_eq!(
                encoded["availabilityByVerb"]["complete"],
                encoded["availability"]
            );
            if label != "loaded" {
                assert_eq!(encoded["availability"]["expectedLoadMs"], 1_211_000);
            }
            if label == "loading" {
                let elapsed = encoded["availability"]["elapsedMs"].as_u64().unwrap();
                assert!((before - requested) as u64 <= elapsed);
                assert!(elapsed <= (crate::fleet::unix_time_ms() as i64 - requested) as u64);
            }
            assert_eq!(orch.cached_model_catalog_for_test("inference"), cached);
        }

        host.pools.clear();
        pass(&db, &host, now).await.unwrap();
        *orch.fleet.models.book.lock().await = host.models.book.lock().await.clone();
        assert!(
            read().is_empty(),
            "stored deployments without a pool are not offered"
        );
        let markdown = render("provider=inference");
        assert!(!markdown.contains(&format!("`{DSV4F}`")));
        assert!(!markdown.contains(&format!("`{QWEN}`")));
        let json = render("provider=inference&format=json");
        assert!(!json.contains(DSV4F) && !json.contains(QWEN), "{json}");
        assert_eq!(orch.cached_model_catalog_for_test("inference"), cached);
        assert!(
            host.heads.starts().is_empty(),
            "catalog reads never launch an engine"
        );
    }

    /// A profile a generation names as its draft is governed by the promotion
    /// gate: until a pass of exactly what it is now promotes it, placement's
    /// catalog refuses it, so no warm model, model pin or deployment pin
    /// outside the operator-approved verification claim acquires it.
    #[tokio::test]
    async fn an_unpromoted_generated_profile_is_not_acquired_until_a_pass_promotes_it() {
        use crate::orchestrator::inference::load_inventory;
        use crate::orchestrator::inference_verify::digest;
        let db = sparks_db("models-unpromoted-generated").await;
        db.execute("INSERT INTO inference_generations
            (id,project_id,job_id,node_uri,model,pool,state,draft_profile,draft_written_at,started_at)
            VALUES ('g','p','j','cairn://p/p/1/1/g',?1,?2,'failed',?1,1,1)",
            (DSV4F, SPARKS)).await.unwrap();
        let host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.attach(SP2, 110, 0);
        // A preference written before the generation is re-evaluated on every pass.
        store::save_preference(
            &db,
            &SPARKS.into(),
            &super::Preference {
                warm_model: Some(DSV4F.into()),
                idle_override_secs: Some(0),
            },
            1,
        )
        .await
        .unwrap();
        pass(&db, &host, 1_000).await.unwrap();
        pass(&db, &host, 2_000).await.unwrap();
        assert!(
            host.heads.starts().is_empty(),
            "warming must not acquire it"
        );
        let model_pin = PinTarget::Model {
            name: DSV4F.into(),
            pool: None,
        };
        assert!(pin_with(&db, &host, model_pin.clone(), "operator", 2_001)
            .await
            .is_err());
        assert!(
            pin_with(
                &db,
                &host,
                PinTarget::Deployment(DSV4F.into()),
                "verify:v",
                2_002
            )
            .await
            .is_err(),
            "only the claim the operator approved may start an unpromoted draft"
        );
        assert!(host.models.book.lock().await.state.residents.is_empty());

        let inventory = load_inventory(&db).await.unwrap();
        let current = digest(&inventory.profiles[&DSV4F.into()]);
        db.execute("INSERT INTO inference_verifications
            (id,profile_id,profile_digest,pool_id,deployment_id,machines_json,requested_by,started_at,outcome,stage)
            VALUES ('v',?1,?2,?3,?1,'[]','operator',3,'passed','done')",
            (DSV4F, current.as_str(), SPARKS)).await.unwrap();
        pin_with(&db, &host, model_pin, "operator", 3_000)
            .await
            .unwrap();
        assert!(
            !host.models.book.lock().await.state.residents.is_empty(),
            "a pass of what it is now promotes it"
        );
    }

    #[tokio::test]
    async fn exact_profile_request_records_only_the_selected_deployment() {
        use crate::orchestrator::inference::{
            load_inventory, write_inventory, Authoring, Removals,
        };
        let db = sparks_db("models-exact-profile").await;
        let mut inventory = load_inventory(&db).await.unwrap();
        let mut selected = inventory.profiles[&DSV4F.into()].clone();
        selected.id = "selected".into();
        inventory.insert_profile(selected);
        let mut deployed = inventory.deployments[&DSV4F.into()].clone();
        deployed.id = "z-selected".into();
        deployed.profile = "selected".into();
        deployed.api_port = 9998;
        inventory.insert_deployment(deployed);
        write_inventory(
            &db,
            &Authoring::Patch {
                upsert: Some(inventory.to_yaml()),
                remove: Removals::default(),
            },
        )
        .await
        .unwrap();
        let host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.attach(SP2, 110, 0);
        let handle = super::super::request_selection_with(
            &db,
            &host,
            (DSV4F, Some("selected")),
            None,
            "operator",
            Some(&SPARKS.into()),
            1_000,
        )
        .await
        .unwrap();
        assert_eq!(handle.deployment.as_str(), "z-selected");
        let requests = store::load_requests(&db).await.unwrap();
        assert!(requests
            .iter()
            .all(|request| request.deployment.as_str() == "z-selected"));
        assert!(super::super::request_selection_with(
            &db,
            &host,
            (DSV4F, Some("missing")),
            None,
            "operator",
            Some(&SPARKS.into()),
            1_001
        )
        .await
        .is_err());
        assert_eq!(
            store::load_requests(&db).await.unwrap().len(),
            requests.len()
        );
    }

    /// A request acquires the gang in one decision: both Sparks carry the
    /// reservation from the same pass. The start waits while the work on sp1
    /// may finish, then only the check in the way is preempted (never the
    /// agent turn), and the start goes once the room is back.
    #[tokio::test]
    async fn a_request_reserves_the_gang_drains_it_and_launches_once_it_fits() {
        let db = sparks_db("models-host-drain").await;
        footprint(&db, 100).await;
        let host = sparks();
        host.heads.attach(SP1, 60, 0);
        host.heads.attach(SP2, 110, 0);
        host.heads.run(SP1, "check-big", true, 45);
        host.heads.run(SP1, "check-small", true, 5);
        host.heads.run(SP1, "turn", false, 30);

        let handle = request_with(&db, &host, DSV4F, None, "agent-1", None, 1_000)
            .await
            .unwrap();
        assert_eq!(handle.pool.as_str(), SPARKS);
        assert_eq!(phase(&db, DSV4F).await.label(), "reserving");
        let published = host.published.lock().unwrap().clone();
        for machine in [SP1, SP2] {
            let resident = &published[machine][0];
            assert_eq!(resident.deployment, DSV4F);
            assert_eq!(resident.phase, "reserving");
            assert_eq!(resident.held_bytes, Some(100 * GIB));
            assert!(resident.unified && resident.leased && !resident.pinned);
        }
        pass(&db, &host, 2_000).await.unwrap();
        let status = status_with(&db, &host, handle.id).await.unwrap();
        assert!(
            status.describe().contains("may finish for 5 min more"),
            "{}",
            status.describe()
        );
        assert!(host.heads.starts().is_empty());

        let past = 1_000 + (super::super::room::DRAIN_BOUND_SECS as i64) * 1_000;
        pass(&db, &host, past).await.unwrap();
        let preempted = host.heads.preempted.lock().unwrap().clone();
        assert_eq!(
            preempted
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            ["check-big"]
        );
        assert_eq!(
            (preempted[0].1.need_bytes, preempted[0].1.free_bytes),
            (100 * GIB, 60 * GIB)
        );
        assert!(
            host.heads.starts().is_empty(),
            "the memory is on its way back"
        );

        host.heads
            .running
            .lock()
            .unwrap()
            .get_mut(SP1)
            .unwrap()
            .retain(|b| !b.preempted);
        host.heads.attach(SP1, 105, 0);
        pass(&db, &host, past + 5_000).await.unwrap();
        assert_eq!(host.heads.starts(), [DSV4F]);
        let (executor, ..) = host.heads.last_sent();
        assert_eq!(executor, SP1, "only the head acts");
        assert_eq!(phase(&db, DSV4F).await.label(), "starting");
    }

    /// The operator's pin revokes another holder's pin in its way rather than
    /// being refused, and unloading takes a model down whoever pinned it and
    /// whatever waits for it.
    #[tokio::test]
    async fn the_operator_outranks_another_pin_and_unload_takes_a_model_down() {
        let db = sparks_db("models-host-operator").await;
        let host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.attach(SP2, 110, 0);
        pin_with(
            &db,
            &host,
            PinTarget::Deployment(DSV4F.into()),
            "verify:dsv4f-tp2",
            1_000,
        )
        .await
        .unwrap();
        let said = pin_inner(
            &db,
            &host,
            PinTarget::Deployment(DSV4F.into()),
            "operator",
            true,
            2_000,
        )
        .await
        .unwrap();
        assert!(
            said.starts_with("revoked verify:dsv4f-tp2's pin on"),
            "{said}"
        );
        let pins = store::load_pins(&db).await.unwrap();
        assert_eq!(pins[&DeploymentId::from(DSV4F)].pin.holder, "operator");

        let request =
            store::insert_request(&db, &SPARKS.into(), &DSV4F.into(), DSV4F, "agent", 3_000)
                .await
                .unwrap();
        let said = unload_with(&db, &host, &DSV4F.into(), "operator", 4_000)
            .await
            .unwrap();
        assert!(said.contains("revoked operator's pin"), "{said}");
        assert!(said.contains("stopped by operator"), "{said}");
        assert!(store::load_pins(&db).await.unwrap().is_empty());
        let requests = store::load_requests(&db).await.unwrap();
        let request = requests.iter().find(|r| r.id == request).unwrap();
        assert!(
            matches!(request.state, RequestState::Cancelled { .. }),
            "{request:?}"
        );
        assert!(host.models.book.lock().await.state.residents.is_empty());
    }

    /// A pin acquires its model at once and displaces what is idle there;
    /// another holder's pin on the same machines is refused by name, the same
    /// holder may move its pin, only the holder releases it (anyone may
    /// revoke), and released residency counts from the pin.
    #[tokio::test]
    async fn pins_are_standing_leases_held_by_one_holder() {
        let db = sparks_db("models-host-pins").await;
        let host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.attach(SP2, 110, 0);

        let said = pin_with(
            &db,
            &host,
            PinTarget::Deployment(DSV4F.into()),
            "operator",
            1_000,
        )
        .await
        .unwrap();
        assert!(said.contains("pinned dsv4f-tp2"), "{said}");
        assert_eq!(
            phase(&db, DSV4F).await.label(),
            "starting",
            "acquired, and launched at once: nothing on the gang is in the way"
        );
        assert!(host.published.lock().unwrap()[SP1][0].pinned);

        let refusal = pin_with(
            &db,
            &host,
            PinTarget::Model {
                name: QWEN.into(),
                pool: None,
            },
            "verify:qwen36-fast",
            2_000,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refusal,
            "dsv4f-tp2 is pinned by operator; only they can release it, or revoke it explicitly"
        );
        let refusal = release_with(
            &db,
            &host,
            PinTarget::Deployment(DSV4F.into()),
            "someone",
            false,
            3_000,
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("pinned by operator"), "{refusal}");

        // The same holder moves its pin to qwen36-fast, which displaces the
        // unpinned dsv4f-tp2 on sp1.
        pin_with(
            &db,
            &host,
            PinTarget::Model {
                name: QWEN.into(),
                pool: None,
            },
            "operator",
            4_000,
        )
        .await
        .unwrap();
        let book = host.models.book.lock().await;
        assert_eq!(
            book.state
                .residents
                .keys()
                .map(|id| id.as_str())
                .collect::<Vec<_>>(),
            [QWEN]
        );
        drop(book);

        // Healthy, then revoked by someone else: it stays, residency from
        // the pin.
        db.execute(
            "INSERT INTO inference_deployment_state (deployment_id, phase_json, command_id, updated_at)
             VALUES (?1, ?2, NULL, 90000)
             ON CONFLICT(deployment_id) DO UPDATE SET phase_json = excluded.phase_json",
            params![QWEN, r#"{"phase":"healthy","since_ms":90000}"#],
        )
        .await
        .unwrap();
        pass(&db, &host, 100_000).await.unwrap();
        let said = release_with(
            &db,
            &host,
            PinTarget::Pool(SPARKS.into()),
            "someone",
            true,
            200_000,
        )
        .await
        .unwrap();
        assert!(
            said.starts_with("revoked operator's pin on qwen36-fast"),
            "{said}"
        );
        let book = host.models.book.lock().await;
        let resident = &book.state.residents[&cairn_inference::DeploymentId::from(QWEN)];
        assert_eq!(resident.healthy_since, Some(4_000));
        assert!(!host.published.lock().unwrap()[SP1][0].pinned);
    }

    #[tokio::test]
    async fn a_pool_s_warm_model_must_run_there_and_the_idle_window_reports_its_origin() {
        let db = sparks_db("models-host-preferences").await;
        footprint(&db, 100).await;
        let host = sparks();
        let refusal = preference_with(
            &db,
            &host,
            &SPARKS.into(),
            Some(Some("Qwen/Qwen3-Embedding-4B".into())),
            None,
            1_000,
        )
        .await
        .unwrap_err();
        assert!(refusal.contains("runs dsv4f-tp2, qwen36-fast"), "{refusal}");
        let said = preference_with(
            &db,
            &host,
            &SPARKS.into(),
            Some(Some("DSV4F-TP2".into())),
            Some(None),
            1_000,
        )
        .await
        .unwrap();
        assert!(said.contains("warm model is dsv4f-tp2"), "{said}");
        assert!(
            said.contains("idle window 24m (from the latest measured load of 4m)"),
            "6 x the 4m load: {said}"
        );
        assert!(
            preference_with(&db, &host, &"nope".into(), None, Some(Some(60)), 1_000)
                .await
                .unwrap_err()
                .starts_with("there is no pool nope")
        );
        let stored = super::super::store::load_preferences(&db).await.unwrap();
        assert_eq!(
            stored[&crate::fleet::pools::PoolKey::from(SPARKS)]
                .warm_model
                .as_deref(),
            Some("dsv4f-tp2")
        );
    }

    /// A model left running is adopted on the first pass, a request for it is
    /// ready at once, and nothing is started or stopped for it.
    #[tokio::test]
    async fn a_running_model_is_adopted_and_answers_at_once() {
        let db = sparks_db("models-host-adopt").await;
        let host = sparks();
        host.heads.attach(SP1, 20, 100);
        db.execute(
            "INSERT INTO inference_deployment_state (deployment_id, phase_json, command_id, updated_at)
             VALUES (?1, ?2, NULL, 1)",
            params![DSV4F, r#"{"phase":"healthy","since_ms":1}"#],
        )
        .await
        .unwrap();
        let handle = request_with(&db, &host, DSV4F, None, "agent-1", None, 1_000)
            .await
            .unwrap();
        let status = status_with(&db, &host, handle.id).await.unwrap();
        assert_eq!(status.request.state, RequestState::Ready { at: 1_000 });
        assert!(host
            .heads
            .sent
            .lock()
            .unwrap()
            .iter()
            .all(|(_, _, _, op)| matches!(op, DeploymentOperation::Probe { .. })));
        assert!(matches!(phase(&db, DSV4F).await, Phase::Healthy { .. }));
    }

    /// The fixture's profiles state no memory and only their description
    /// names vLLM, as the bginf-era ones do: the need is vLLM's default share,
    /// 0.90 of each Spark's 122 GiB, and a request drains only the checks
    /// covering it, never waiting for the Sparks to run nothing.
    #[tokio::test]
    async fn a_predicted_need_drains_only_the_checks_covering_it() {
        let db = sparks_db("models-host-predicted").await;
        let host = sparks();
        host.heads.attach(SP1, 60, 0);
        host.heads.attach(SP2, 115, 0);
        host.heads.run(SP1, "check-big", true, 45);
        host.heads.run(SP1, "check-small", true, 5);
        host.heads.run(SP1, "check-tiny", true, 1);
        host.heads.run(SP1, "turn", false, 2);
        host.heads.run(SP2, "check-beside", true, 4);

        let handle = request_with(&db, &host, DSV4F, None, "agent-1", None, 1_000)
            .await
            .unwrap();
        let predicted = (122.0 * 0.9 * GIB as f64) as u64;
        let published = host.published.lock().unwrap().clone();
        for machine in [SP1, SP2] {
            assert_eq!(published[machine][0].held_bytes, Some(predicted));
        }
        pass(&db, &host, 2_000).await.unwrap();
        let status = status_with(&db, &host, handle.id).await.unwrap();
        let said = status.describe();
        assert!(
            said.contains("a predicted need: vLLM pre-allocates 0.90"),
            "{said}"
        );
        assert!(said.contains("may finish for 5 min more"), "{said}");

        let past = 1_000 + (super::super::room::DRAIN_BOUND_SECS as i64) * 1_000;
        pass(&db, &host, past).await.unwrap();
        let mut preempted: Vec<String> = host
            .heads
            .preempted
            .lock()
            .unwrap()
            .iter()
            .map(|(id, _)| id.clone())
            .collect();
        preempted.sort();
        assert_eq!(
            preempted,
            ["check-big", "check-small"],
            "49.8 GiB short on sp1: the two largest checks cover it; the tiny one, the turn and sp2's check run on"
        );
        host.heads
            .running
            .lock()
            .unwrap()
            .get_mut(SP1)
            .unwrap()
            .retain(|b| !b.preempted);
        host.heads.attach(SP1, 110, 0);
        pass(&db, &host, past + 5_000).await.unwrap();
        assert_eq!(
            host.heads.starts(),
            [DSV4F],
            "started with check-tiny, the turn and sp2's check still running"
        );
    }

    /// A measured load replaces the prediction.
    #[tokio::test]
    async fn a_measured_load_overrides_the_prediction() {
        let db = sparks_db("models-host-measured").await;
        footprint(&db, 101).await;
        let host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.attach(SP2, 110, 0);
        request_with(&db, &host, DSV4F, None, "agent-1", None, 1_000)
            .await
            .unwrap();
        let published = host.published.lock().unwrap().clone();
        assert_eq!(published[SP1][0].held_bytes, Some(101 * GIB));
    }

    /// A profile giving nothing to predict from is refused with what is
    /// missing: no wait for an empty machine, and nothing preempted.
    #[tokio::test]
    async fn a_profile_with_nothing_to_predict_from_is_refused_and_preempts_nothing() {
        let db = sparks_db("models-host-refused").await;
        let mut bare = profile(QWEN, 1);
        bare.engine = cairn_inference::Engine::Command {
            start: "./serve.sh".into(),
            stop: None,
            description: None,
        };
        db.execute(
            "UPDATE inference_profiles SET body_json = ?1 WHERE id = ?2",
            params![serde_json::to_string(&bare).unwrap(), QWEN],
        )
        .await
        .unwrap();
        let host = sparks();
        host.heads.attach(SP1, 110, 0);
        host.heads.run(SP1, "check", true, 40);

        request_with(&db, &host, QWEN, None, "agent-1", None, 1_000)
            .await
            .unwrap();
        let past = 1_000 + (super::super::room::DRAIN_BOUND_SECS as i64) * 1_000;
        pass(&db, &host, past).await.unwrap();
        let Phase::Failed { reason, .. } = phase(&db, QWEN).await else {
            panic!("the start is refused");
        };
        assert!(
            reason.starts_with("its memory need is unknown: its profile states no memory"),
            "{reason}"
        );
        assert!(reason.contains("add `requirements.memory`"), "{reason}");
        assert!(host.heads.preempted.lock().unwrap().is_empty());
        assert!(host.heads.starts().is_empty());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_catalog_reads_live_placement_and_phase_changes() {
    use cairn_db::turso::params;
    use cairn_models::interface::{ModelScope, Models};
    use cairn_models::types::catalog::{claude_models, ModelAvailability, ProviderModelCatalog};

    let orch = crate::orchestrator::models::tests::bare_orchestrator().await;
    let mut discovered = claude_models().remove(0);
    discovered.id = "deepseek".into();
    discovered.model = "deepseek".into();
    discovered.canonical_slug = None;
    discovered.serves = Some(vec![cairn_models::types::catalog::ServedVerb {
        verb: cairn_models::types::catalog::Verb::Complete,
        wire: cairn_models::types::catalog::Wire::OpenAiChatCompletions,
    }]);
    let cached = ProviderModelCatalog {
        backend: "inference".into(),
        models: vec![discovered],
        options: vec![],
        refreshed_at: Some(42),
        error: None,
    };
    orch.seed_model_catalog_for_test(cached.clone());
    {
        let mut book = orch.fleet.models.book.lock().await;
        book.catalog = catalog();
        book.catalog.get_mut(&d("dsv4f-tp2")).unwrap().verbs = BTreeSet::from([
            cairn_models::types::catalog::VerbKind::Complete,
            cairn_models::types::catalog::VerbKind::Session,
        ]);
        book.pools = vec![pool(&["sp1", "sp2"])];
        book.settings.insert(
            SPARKS.into(),
            EffectiveSettings::derive(
                None,
                Some(MeasuredLoad {
                    secs: 1211,
                    cold: false,
                }),
            ),
        );
        let resident = super::rules::Resident::chosen(
            &book.catalog[&d("dsv4f-tp2")],
            SPARKS.into(),
            Why::Adopted,
            1000,
        );
        book.state.residents.insert(d("dsv4f-tp2"), resident);
    }
    let read = || {
        let catalog = Models::catalog(&orch, &ModelScope::workspace()).unwrap();
        let provider = catalog
            .providers
            .iter()
            .find(|p| p.descriptor.key == "inference")
            .unwrap();
        assert_eq!(provider.models.as_ref().unwrap().refreshed_at, Some(42));
        provider.models.as_ref().unwrap().models[0]
            .availability
            .clone()
            .unwrap()
    };
    for (phase, expected) in [
        (
            Phase::Healthy {
                since_ms: 1000,
                load_ms: None,
            },
            ModelAvailability::Loaded {
                pool: SPARKS.into(),
            },
        ),
        (
            Phase::Stopped { since_ms: 2000 },
            ModelAvailability::Loadable {
                pool: SPARKS.into(),
                expected_load_ms: Some(1211000),
            },
        ),
    ] {
        orch.db
            .local
            .execute(
                "INSERT OR REPLACE INTO inference_deployment_state
                (deployment_id, phase_json, command_id, updated_at) VALUES (?1, ?2, NULL, 1)",
                params!["dsv4f-tp2", serde_json::to_string(&phase).unwrap()],
            )
            .await
            .unwrap();
        assert_eq!(read(), expected);
        let raw = orch.get_model_catalog();
        assert_eq!(raw[0].models[0].availability, Some(expected));
    }
    {
        let mut book = orch.fleet.models.book.lock().await;
        book.state.residents.clear();
        let mut fast = acquirable("z-fast", "deepseek", &["fast"], "fast");
        fast.verbs = book.catalog[&d("dsv4f-tp2")].verbs.clone();
        book.catalog.insert(d("z-fast"), fast);
        book.pools.push(pool(&["fast"]));
        book.settings.insert(
            "fast".into(),
            EffectiveSettings::derive(
                None,
                Some(MeasuredLoad {
                    secs: 10,
                    cold: false,
                }),
            ),
        );
    }
    // Placement chooses the earlier deployment id, not the shortest load estimate.
    assert_eq!(
        read(),
        ModelAvailability::Loadable {
            pool: SPARKS.into(),
            expected_load_ms: Some(1211000),
        }
    );
    // A read must not write its availability back to the discovery cache.
    assert_eq!(orch.cached_model_catalog_for_test("inference"), cached);
    orch.fleet.models.book.lock().await.pools.clear();
    let catalog = Models::catalog(&orch, &ModelScope::workspace()).unwrap();
    assert!(catalog
        .providers
        .iter()
        .find(|p| p.descriptor.key == "inference")
        .unwrap()
        .models
        .as_ref()
        .unwrap()
        .models
        .is_empty());
}
