use std::collections::BTreeMap;

use super::*;
use crate::fixture::{command_recipe, deployment, set, test_inventory};

const MIN: Ms = 60_000;

/// sp1 and sp2 in pool `sparks`: `dsv4f-tp2` spans both, `qwen36-fast` is on
/// sp1, `other` on sp2, and the default is `dsv4f-tp2`.
fn inventory() -> Inventory {
    let mut inventory = test_inventory();
    inventory.insert_recipe(command_recipe("other", 1));
    inventory.insert_deployment(deployment("other", &["sp2"]));
    assert_eq!(inventory.validate(), Ok(()));
    inventory
}

/// Every window ten minutes, so the arithmetic in the tests is legible.
fn settings() -> EffectiveSettings {
    PoolSettings {
        model_idle_secs: Some(600),
        pool_idle_secs: Some(600),
        min_residency_secs: Some(600),
        load_bound_secs: Some(600),
    }
    .effective(None)
}

fn d(id: &str) -> DeploymentId {
    id.into()
}

fn request(id: RequestId, deployment: &str, at: Ms) -> Request {
    Request {
        id,
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

fn phases(entries: &[(&str, Phase)]) -> BTreeMap<DeploymentId, Phase> {
    entries.iter().map(|(id, p)| (d(id), p.clone())).collect()
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

fn failed(at_ms: Ms, reason: &str) -> Phase {
    Phase::Failed {
        at_ms,
        reason: reason.into(),
    }
}

fn builds(running_turns: u32, running_other: u32, waiting: u32) -> BuildPressure {
    BuildPressure {
        running_turns,
        running_other,
        waiting,
    }
}

/// The controller for one pool, stepped tick by tick the way the host does:
/// each plan's control record and settled requests feed the next.
struct Harness {
    inventory: Inventory,
    control: PoolControl,
    requests: Vec<Request>,
    phases: BTreeMap<DeploymentId, Phase>,
    builds: BuildPressure,
    last: Option<Plan>,
}

impl Harness {
    /// Control of `sparks` taken at 0 with nothing running.
    fn new() -> Self {
        let inventory = inventory();
        let control = PoolControl::begin(
            &inventory.pools[&PoolId::from("sparks")],
            &inventory.deployments,
            &BTreeMap::new(),
            0,
        );
        Self {
            inventory,
            control,
            requests: Vec::new(),
            phases: BTreeMap::new(),
            builds: BuildPressure::default(),
            last: None,
        }
    }

    /// Control taken at 0 with `id` loaded and healthy since then.
    fn with_loaded(id: &str) -> Self {
        let mut harness = Self::new();
        harness.phases = phases(&[(id, healthy(0))]);
        harness.control = PoolControl::begin(
            harness.pool(),
            &harness.inventory.deployments,
            &harness.phases,
            0,
        );
        harness
    }

    fn pool(&self) -> &Pool {
        &self.inventory.pools[&PoolId::from("sparks")]
    }

    fn tick(&mut self, now: Ms) -> Plan {
        let plan = plan(
            self.pool(),
            &self.inventory.deployments,
            &self.control,
            &settings(),
            &self.requests,
            Observed {
                now,
                phases: &self.phases,
                builds: self.builds,
            },
        );
        self.control = plan.control.clone();
        for (id, state) in &plan.settled {
            let request = self.requests.iter_mut().find(|r| r.id == *id).unwrap();
            request.state = state.clone();
        }
        self.last = Some(plan.clone());
        plan
    }

    fn desired(&self) -> DeploymentSet {
        desired(&self.control).expect("the harness's pool is controlled")
    }

    fn wait(&self, id: RequestId) -> &Wait {
        &self.last.as_ref().unwrap().waits[&id]
    }

    fn state(&self, id: RequestId) -> &RequestState {
        &self.requests.iter().find(|r| r.id == id).unwrap().state
    }
}

// Rule 1: a request loads its model.

#[test]
fn a_request_for_an_unloaded_model_makes_it_the_desired_set() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 1_000));
    let plan = h.tick(1_000);
    assert_eq!(plan.resolution.desired, set(&["dsv4f-tp2"]));
    assert_eq!(plan.resolution.slot, ControlSlot::Requested);
    assert_eq!(
        plan.control.active.why,
        Why::Requested {
            deployment: d("dsv4f-tp2"),
            request: 1,
            requester: "agent-1".into()
        }
    );
    assert_eq!(h.wait(1), &Wait::Starting);
}

#[test]
fn a_request_waits_visibly_through_fit_load_and_ready() {
    let mut h = Harness::new();
    h.builds = builds(1, 2, 0);
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
    assert_eq!(
        h.wait(1),
        &Wait::Starting,
        "a load never waits for an empty machine, only for the memory it needs"
    );

    let on = "memory: sp1 has 20.0 GiB free and needs 100.0 GiB";
    h.phases = phases(&[(
        "dsv4f-tp2",
        Phase::Reserving {
            since_ms: 0,
            waiting_on: Some(on.into()),
        },
    )]);
    h.tick(MIN);
    assert_eq!(
        h.wait(1),
        &Wait::Fitting {
            since: 0,
            waiting_on: Some(on.into()),
            turns: 1,
            other: 2
        }
    );
    assert_eq!(
        h.wait(1).describe(MIN),
        "waiting to fit since 1m ago, its machines reserved and taking no new batches; waiting on memory: sp1 has 20.0 GiB free and needs 100.0 GiB; 3 batches still running on the pool's machines, 1 of them an agent turn that is never preempted; the start fails in 14m if it never fits"
    );
    assert_eq!(
        h.control.active.members[&d("dsv4f-tp2")].loading_since,
        None,
        "waiting to fit is not loading"
    );

    h.builds = BuildPressure::default();
    h.phases = phases(&[("dsv4f-tp2", starting(2 * MIN))]);
    h.tick(2 * MIN + 5_000);
    assert_eq!(
        h.wait(1),
        &Wait::Loading {
            since: 2 * MIN,
            bound_at: 12 * MIN
        }
    );
    assert_eq!(
        h.wait(1).describe(5 * MIN),
        "loading since 3m ago, given up in 7m if it is not healthy by then"
    );

    h.phases = phases(&[("dsv4f-tp2", healthy(6 * MIN))]);
    let plan = h.tick(6 * MIN + 5_000);
    assert!(plan.waits.is_empty());
    assert_eq!(
        h.state(1),
        &RequestState::Ready {
            at: 6 * MIN + 5_000
        }
    );
    assert_eq!(
        h.control.active.members[&d("dsv4f-tp2")].healthy_since,
        Some(6 * MIN),
        "residency counts from when the act side saw it healthy"
    );
}

#[test]
fn a_deployment_with_no_phase_is_unknown_not_stopped() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.phases.clear();
    h.tick(MIN);
    assert_eq!(
        h.control.active.members[&d("dsv4f-tp2")].healthy_since,
        Some(0),
        "nothing says it went away"
    );
    h.phases = phases(&[("dsv4f-tp2", Phase::Stopped { since_ms: MIN })]);
    h.tick(2 * MIN);
    assert_eq!(
        h.control.active.members[&d("dsv4f-tp2")].healthy_since,
        None,
        "stopped is gone: whatever comes back is a new load"
    );

    let inventory = inventory();
    let pool = &inventory.pools[&PoolId::from("sparks")];
    let control = PoolControl::begin(pool, &inventory.deployments, &BTreeMap::new(), 0);
    assert_eq!(
        control.active.set(),
        set(&[]),
        "taking control adopts only what the act side knows is healthy"
    );
}

#[test]
fn a_request_for_a_loaded_model_is_ready_at_once() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests.push(request(1, "dsv4f-tp2", MIN));
    let plan = h.tick(MIN);
    assert_eq!(plan.settled, vec![(1, RequestState::Ready { at: MIN })]);
    assert_eq!(plan.control.active.why, Why::Adopted);
}

#[test]
fn a_desired_model_waits_for_another_to_unload_from_its_machines() {
    let mut h = Harness::new();
    // `qwen36-fast` still runs on sp1 though nothing wants it.
    h.phases = phases(&[("qwen36-fast", healthy(0))]);
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    assert_eq!(
        h.wait(1),
        &Wait::Unloading {
            deployments: vec![d("qwen36-fast")]
        }
    );
    assert_eq!(
        h.wait(1).describe(0),
        "unloading qwen36-fast from its machines"
    );
}

// Rule 2: waiting builds unload an idle model.

#[test]
fn an_idle_model_unloads_when_builds_wait() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.builds = builds(0, 0, 3);
    let plan = h.tick(11 * MIN);
    assert_eq!(plan.resolution.desired, set(&[]));
    assert_eq!(plan.resolution.slot, ControlSlot::Nothing);
    assert_eq!(
        plan.control.active.why,
        Why::Unloaded {
            deployments: set(&["dsv4f-tp2"]),
            waiting: 3
        }
    );
    assert_eq!(
        plan.resolution.reason.describe(11 * MIN),
        "unloaded dsv4f-tp2 0s ago: no request for its idle window while 3 batches waited for the pool"
    );
}

#[test]
fn a_model_requested_within_its_idle_window_stays_while_builds_wait() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests
        .push(ready(request(1, "dsv4f-tp2", 15 * MIN), 15 * MIN));
    h.builds = builds(0, 0, 3);
    h.tick(20 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
    h.tick(25 * MIN);
    assert_eq!(h.desired(), set(&[]), "ten minutes after the last request");
}

#[test]
fn an_idle_model_stays_when_no_builds_wait() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.builds = builds(0, 0, 0);
    h.tick(60 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
}

#[test]
fn the_minimum_residency_holds_a_fresh_load_against_waiting_builds() {
    let settings = PoolSettings {
        model_idle_secs: Some(60),
        min_residency_secs: Some(1_200),
        ..PoolSettings::default()
    }
    .effective(None);
    let inventory = inventory();
    let pool = &inventory.pools[&PoolId::from("sparks")];
    let loaded = phases(&[("dsv4f-tp2", healthy(0))]);
    let control = PoolControl::begin(pool, &inventory.deployments, &loaded, 0);
    let at = |now| {
        plan(
            pool,
            &inventory.deployments,
            &control,
            &settings,
            &[],
            Observed {
                now,
                phases: &loaded,
                builds: builds(0, 0, 1),
            },
        )
        .resolution
        .desired
    };
    assert_eq!(
        at(19 * MIN),
        set(&["dsv4f-tp2"]),
        "idle, but resident 19m of 20m"
    );
    assert_eq!(at(20 * MIN), set(&[]));
}

#[test]
fn a_load_in_progress_is_not_unloaded_for_builds() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.requests[0].state = RequestState::Cancelled { at: MIN };
    h.phases = phases(&[("dsv4f-tp2", starting(2 * MIN))]);
    h.builds = builds(0, 0, 2);
    h.tick(2 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]), "mid-load");

    h.phases = phases(&[("dsv4f-tp2", healthy(3 * MIN))]);
    h.tick(3 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]), "resident from 3m");
    h.tick(13 * MIN);
    assert_eq!(h.desired(), set(&[]));
}

#[test]
fn a_chosen_model_that_never_started_yields_to_builds_at_once() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.requests[0].state = RequestState::Cancelled { at: MIN };
    h.builds = builds(0, 0, 1);
    h.tick(MIN);
    assert_eq!(h.desired(), set(&[]));
}

#[test]
fn a_model_still_waiting_to_fit_yields_to_builds_at_once() {
    let mut h = Harness::new();
    h.tick(10 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]), "the idle default");
    h.phases = phases(&[(
        "dsv4f-tp2",
        Phase::Reserving {
            since_ms: 10 * MIN,
            waiting_on: None,
        },
    )]);
    h.builds = builds(0, 0, 2);
    h.tick(11 * MIN);
    assert_eq!(
        h.desired(),
        set(&[]),
        "nothing has been loaded, so there is no load to protect"
    );
}

// Rule 3: an idle pool loads its default.

#[test]
fn an_idle_pool_loads_its_default_after_the_pool_idle_window() {
    let mut h = Harness::new();
    h.tick(9 * MIN);
    assert_eq!(h.desired(), set(&[]));
    let plan = h.tick(10 * MIN);
    assert_eq!(plan.resolution.desired, set(&["dsv4f-tp2"]));
    assert_eq!(plan.resolution.slot, ControlSlot::Default);
    assert_eq!(plan.control.active.why, Why::IdleDefault { idle_since: 0 });
}

#[test]
fn batch_work_restarts_the_pool_idle_window() {
    let mut h = Harness::new();
    h.builds = builds(0, 1, 0);
    h.tick(8 * MIN);
    h.builds = BuildPressure::default();
    h.tick(17 * MIN);
    assert_eq!(h.desired(), set(&[]), "nine minutes since the last build");
    h.tick(18 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
}

#[test]
fn after_builds_unload_a_model_the_default_returns_only_once_they_stop() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.builds = builds(0, 0, 4);
    h.tick(10 * MIN);
    assert_eq!(h.desired(), set(&[]));
    h.phases.clear();
    h.builds = builds(0, 4, 0);
    h.tick(30 * MIN);
    assert_eq!(h.desired(), set(&[]));
    h.builds = BuildPressure::default();
    h.tick(39 * MIN);
    assert_eq!(h.desired(), set(&[]));
    h.tick(40 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
}

#[test]
fn an_idle_pool_replaces_an_idle_non_default_model_with_the_default() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.tick(9 * MIN);
    assert_eq!(h.desired(), set(&["qwen36-fast"]), "still resident");
    h.tick(10 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
}

// Pins.

#[test]
fn a_pin_holds_its_set_against_waiting_builds_and_wins_over_requests() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.control.pin(set(&["dsv4f-tp2"]), "operator", 0).unwrap();
    h.builds = builds(0, 0, 5);
    h.requests.push(request(1, "qwen36-fast", MIN));
    let plan = h.tick(60 * MIN);
    assert_eq!(plan.resolution.desired, set(&["dsv4f-tp2"]));
    assert_eq!(plan.resolution.slot, ControlSlot::Pin);
    assert_eq!(
        h.wait(1),
        &Wait::Pinned {
            holder: "operator".into(),
            set: set(&["dsv4f-tp2"])
        }
    );
    assert_eq!(
        h.wait(1).describe(60 * MIN),
        "the pool is pinned to dsv4f-tp2 by operator"
    );
}

#[test]
fn a_second_pin_is_refused_naming_the_holder() {
    let mut h = Harness::new();
    h.control.pin(set(&["dsv4f-tp2"]), "operator", 0).unwrap();
    let refused = h.control.pin(set(&[]), "agent-7", MIN).unwrap_err();
    assert_eq!(
        refused.to_string(),
        "the pool is pinned to dsv4f-tp2 by operator; only they can release it, or revoke it explicitly"
    );
    h.control.pin(set(&[]), "operator", MIN).unwrap();
    assert_eq!(h.desired(), set(&[]), "the holder may re-pin");
}

#[test]
fn releasing_a_pin_hands_its_set_to_the_rules() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.control.active.members.clear();
    h.control.pin(set(&["dsv4f-tp2"]), "operator", 0).unwrap();
    assert_eq!(
        h.control.release("agent-7", false, &h.phases, 30 * MIN),
        Err(PinRefused::HeldBy(h.control.pin.clone().unwrap()))
    );
    h.control
        .release("operator", false, &h.phases, 30 * MIN)
        .unwrap();
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
    assert_eq!(
        h.control.active.members[&d("dsv4f-tp2")].healthy_since,
        Some(0),
        "its residency counts from the pin"
    );
    h.builds = builds(0, 0, 1);
    let plan = h.tick(31 * MIN);
    assert_eq!(
        plan.resolution.desired,
        set(&[]),
        "idle since the pin, builds waiting"
    );
}

#[test]
fn anyone_may_revoke_a_pin() {
    let mut h = Harness::new();
    h.control.pin(set(&[]), "operator", 0).unwrap();
    let pin = h.control.release("agent-7", true, &h.phases, MIN).unwrap();
    assert_eq!(pin.holder, "operator");
    assert_eq!(h.control.pin, None);
    assert_eq!(
        h.control.release("agent-7", true, &h.phases, MIN),
        Err(PinRefused::NotPinned)
    );
}

// Two models on one pool.

#[test]
fn the_loaded_model_keeps_the_pool_until_its_idle_window_ends() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests
        .push(ready(request(1, "dsv4f-tp2", 12 * MIN), 12 * MIN));
    h.requests.push(request(2, "qwen36-fast", 15 * MIN));
    h.tick(15 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
    assert_eq!(
        h.wait(2),
        &Wait::Held {
            by: vec![Hold {
                deployment: d("dsv4f-tp2"),
                because: HoldCause::RecentRequest,
                until: Some(22 * MIN)
            }]
        }
    );
    assert_eq!(
        describe_waiting(&h.requests[1], &"sparks".into(), h.wait(2), 15 * MIN),
        "qwen36-fast on pool sparks for agent-2, waiting 0s: dsv4f-tp2 was requested within its idle window and keeps the pool, until in 7m"
    );

    h.tick(22 * MIN);
    assert_eq!(h.desired(), set(&["qwen36-fast"]));
    assert_eq!(
        h.wait(2),
        &Wait::Unloading {
            deployments: vec![d("dsv4f-tp2")]
        }
    );
}

#[test]
fn queue_depth_does_not_move_a_model_off_the_pool() {
    let mut h = Harness::with_loaded("dsv4f-tp2");
    h.requests
        .push(ready(request(1, "dsv4f-tp2", 14 * MIN), 14 * MIN));
    for id in 2..12 {
        h.requests.push(request(id, "qwen36-fast", 15 * MIN));
    }
    h.tick(23 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
}

#[test]
fn a_model_with_requests_waiting_on_it_is_never_displaced() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.requests.push(request(2, "qwen36-fast", MIN));
    h.tick(MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));
    assert_eq!(
        h.wait(2),
        &Wait::Held {
            by: vec![Hold {
                deployment: d("dsv4f-tp2"),
                because: HoldCause::Wanted { requests: 1 },
                until: None
            }]
        }
    );
    h.builds = builds(0, 0, 9);
    h.tick(90 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]), "wanted, so not idle");
}

#[test]
fn a_request_on_free_machines_joins_the_set_without_unloading_anything() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.requests.push(ready(request(1, "qwen36-fast", MIN), MIN));
    h.requests.push(request(2, "other", 2 * MIN));
    h.tick(2 * MIN);
    assert_eq!(h.desired(), set(&["qwen36-fast", "other"]));
}

#[test]
fn an_earlier_request_keeps_its_machines_against_a_later_one() {
    let mut h = Harness::with_loaded("qwen36-fast");
    h.requests.push(ready(request(1, "qwen36-fast", MIN), MIN));
    // The gang needs sp1 (held) and sp2 (free); `other` asks later for sp2.
    h.requests.push(request(2, "dsv4f-tp2", 2 * MIN));
    h.requests.push(request(3, "other", 3 * MIN));
    h.tick(3 * MIN);
    assert_eq!(h.desired(), set(&["qwen36-fast"]));
    assert!(matches!(h.wait(2), Wait::Held { .. }));
    assert_eq!(
        h.wait(3),
        &Wait::Queued {
            behind: 2,
            deployment: d("dsv4f-tp2")
        }
    );
    assert_eq!(
        h.wait(3).describe(3 * MIN),
        "queued behind request #2 for dsv4f-tp2, which asked first for the same machines"
    );

    h.tick(11 * MIN);
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]), "first come, first served");
    assert!(matches!(h.wait(3), Wait::Held { .. }));
}

// Loads that fail.

#[test]
fn a_load_past_its_bound_fails_its_requests_and_leaves_the_set() {
    let mut h = Harness::new();
    h.requests.push(request(1, "dsv4f-tp2", 0));
    h.tick(0);
    h.phases = phases(&[("dsv4f-tp2", starting(MIN))]);
    h.tick(MIN);
    h.tick(10 * MIN);
    assert_eq!(h.state(1), &RequestState::Waiting);
    let plan = h.tick(11 * MIN);
    assert_eq!(plan.resolution.desired, set(&[]));
    assert_eq!(
        h.state(1),
        &RequestState::Failed {
            at: 11 * MIN,
            reason: "dsv4f-tp2 failed to load: it did not come healthy within the load bound of 10m (operator)".into()
        }
    );
    assert!(matches!(plan.control.active.why, Why::LoadFailed { .. }));

    // The pool is idle, but the failure restarted the window: the default is
    // retried once a window, not on every pass.
    h.phases = phases(&[("dsv4f-tp2", failed(11 * MIN, "gave up"))]);
    h.tick(20 * MIN);
    assert_eq!(h.desired(), set(&[]));
    let plan = h.tick(21 * MIN);
    assert_eq!(plan.resolution.desired, set(&["dsv4f-tp2"]));
    assert_eq!(
        plan.control.active.why,
        Why::IdleDefault {
            idle_since: 11 * MIN
        }
    );
}

#[test]
fn a_reported_failure_fails_the_attempt_but_an_old_one_does_not() {
    let mut h = Harness::new();
    h.phases = phases(&[("dsv4f-tp2", failed(0, "exit 1"))]);
    h.requests.push(request(1, "dsv4f-tp2", MIN));
    h.tick(MIN);
    assert_eq!(
        h.state(1),
        &RequestState::Waiting,
        "that failure was before this attempt"
    );
    assert_eq!(h.desired(), set(&["dsv4f-tp2"]));

    h.phases = phases(&[("dsv4f-tp2", failed(2 * MIN, "health timed out"))]);
    h.tick(3 * MIN);
    assert_eq!(
        h.state(1),
        &RequestState::Failed {
            at: 3 * MIN,
            reason: "dsv4f-tp2 failed to load: health timed out".into()
        }
    );
    assert_eq!(h.desired(), set(&[]));
}

#[test]
fn a_request_for_a_deployment_that_left_the_pool_fails() {
    let mut h = Harness::new();
    h.requests.push(request(1, "gone", 0));
    let plan = h.tick(0);
    assert_eq!(
        plan.settled,
        vec![(
            1,
            RequestState::Failed {
                at: 0,
                reason: "gone is no longer a deployment of pool sparks".into()
            }
        )]
    );
}

// What consumers read: the resolved state, `desired`, and its generation.

#[test]
fn the_resolution_reads_which_slot_won_and_why() {
    let mut h = Harness::new();
    let plan = h.tick(0);
    assert_eq!(plan.resolution.slot, ControlSlot::Nothing);
    assert_eq!(
        plan.resolution.reason.describe(MIN),
        "adopted what was running when Cairn took control, 1m ago"
    );

    h.requests.push(request(1, "qwen36-fast", MIN));
    let plan = h.tick(MIN);
    assert_eq!(plan.resolution.slot, ControlSlot::Requested);
    assert_eq!(
        plan.resolution.reason.describe(2 * MIN),
        "agent-1 requested qwen36-fast (request #1) 1m ago"
    );

    h.control.pin(set(&["other"]), "operator", 3 * MIN).unwrap();
    let plan = h.tick(3 * MIN);
    assert_eq!(plan.resolution.slot, ControlSlot::Pin);
    assert_eq!(
        plan.resolution.reason.describe(4 * MIN),
        "pinned to other by operator 1m ago; it holds against idle unload until released or revoked"
    );

    let adopted = Harness::with_loaded("dsv4f-tp2");
    assert_eq!(
        resolution(adopted.pool(), &adopted.control).slot,
        ControlSlot::Default,
        "an adopted set equal to the default reads as the default"
    );
}

#[test]
fn an_uncontrolled_pool_wants_nothing_decided_not_nothing_loaded() {
    let mut h = Harness::new();
    assert_eq!(desired(&h.control), Some(set(&[])));
    h.control.controlled = false;
    assert_eq!(desired(&h.control), None);
}

#[test]
fn the_generation_advances_only_when_desired_changes() {
    let mut h = Harness::new();
    h.builds = builds(0, 1, 0);
    h.tick(MIN);
    h.tick(2 * MIN);
    assert_eq!(h.control.generation, 0, "bookkeeping alone is no change");

    h.requests.push(request(1, "dsv4f-tp2", 3 * MIN));
    h.tick(3 * MIN);
    assert_eq!(h.control.generation, 1);
    h.tick(4 * MIN);
    assert_eq!(h.control.generation, 1);

    let before = desired(&h.control);
    h.control.controlled = false;
    h.control.advance_generation(&before);
    assert_eq!(h.control.generation, 2, "handing the pool back is a change");
}

#[test]
fn taking_control_adopts_what_is_healthy() {
    let inventory = inventory();
    let pool = &inventory.pools[&PoolId::from("sparks")];
    let control = PoolControl::begin(
        pool,
        &inventory.deployments,
        &phases(&[
            ("qwen36-fast", healthy(0)),
            ("other", starting(0)),
            ("qwen3-embed-4b", healthy(0)),
        ]),
        5,
    );
    assert_eq!(control.active.set(), set(&["qwen36-fast"]));
    assert_eq!(control.last_batch_work_at, 5);
}

// Convergence: what the act side is asked for.

fn wanting(h: &mut Harness, ids: &[&str], at: Ms) {
    h.control.active.members = ids.iter().map(|id| (d(id), Member::chosen(at))).collect();
}

fn converge_at(h: &Harness, now: Ms) -> Vec<(DeploymentId, act::Request)> {
    converge(
        h.pool(),
        &h.inventory.deployments,
        &h.control,
        &h.phases,
        now,
    )
}

fn stop(id: &str) -> (DeploymentId, act::Request) {
    (d(id), act::Request::Stop)
}

fn start(id: &str) -> (DeploymentId, act::Request) {
    (d(id), act::Request::Start)
}

#[test]
fn convergence_stops_what_is_not_wanted_before_starting_what_is() {
    let mut h = Harness::new();
    wanting(&mut h, &["dsv4f-tp2"], MIN);
    h.phases = phases(&[
        ("qwen36-fast", healthy(0)),
        ("other", Phase::Stopped { since_ms: 0 }),
        ("qwen3-embed-4b", healthy(0)),
    ]);
    assert_eq!(
        converge_at(&h, MIN),
        vec![stop("qwen36-fast"), start("dsv4f-tp2")],
        "a stopped deployment is left alone, and another pool's never touched"
    );

    // Every phase that holds a model, or is about to, is stopped.
    for phase in [
        starting(0),
        Phase::Reserving {
            since_ms: 0,
            waiting_on: None,
        },
    ] {
        h.phases = phases(&[
            ("qwen36-fast", phase),
            ("other", Phase::Stopped { since_ms: 0 }),
        ]);
        assert_eq!(
            converge_at(&h, MIN),
            vec![stop("qwen36-fast"), start("dsv4f-tp2")]
        );
    }
    // A stop in flight is waited for, and the start is asked for regardless:
    // the act side refuses it by name while a machine is held, and the next
    // pass asks again.
    h.phases = phases(&[
        ("qwen36-fast", Phase::Stopping { since_ms: MIN }),
        ("other", Phase::Stopped { since_ms: 0 }),
    ]);
    assert_eq!(converge_at(&h, MIN), vec![start("dsv4f-tp2")]);
    assert_eq!(converge_at(&h, 2 * MIN), vec![start("dsv4f-tp2")]);
}

#[test]
fn a_desired_deployment_is_asked_for_until_the_act_side_has_it() {
    let mut h = Harness::new();
    wanting(&mut h, &["other"], 2 * MIN);
    h.phases = phases(&[
        ("qwen36-fast", Phase::Stopped { since_ms: 0 }),
        ("dsv4f-tp2", Phase::Stopped { since_ms: 0 }),
    ]);
    let asked = |h: &mut Harness, phase: Option<Phase>| {
        match phase {
            Some(phase) => h.phases.insert(d("other"), phase),
            None => h.phases.remove(&d("other")),
        };
        converge_at(h, 3 * MIN)
    };
    assert_eq!(asked(&mut h, None), vec![start("other")], "unknown");
    assert_eq!(
        asked(&mut h, Some(Phase::Stopped { since_ms: 0 })),
        vec![start("other")]
    );
    assert_eq!(
        asked(&mut h, Some(failed(MIN, "exit 1"))),
        vec![start("other")],
        "that failure was before it was chosen"
    );
    assert_eq!(
        asked(&mut h, Some(failed(2 * MIN, "exit 1"))),
        vec![],
        "this attempt failed: the plan gives it up"
    );
    for phase in [
        Phase::Reserving {
            since_ms: 2 * MIN,
            waiting_on: None,
        },
        starting(2 * MIN),
        healthy(2 * MIN),
        Phase::Stopping { since_ms: 0 },
    ] {
        assert_eq!(asked(&mut h, Some(phase)), vec![]);
    }
}

#[test]
fn a_stop_that_ended_failed_is_asked_for_again_after_a_minute() {
    let mut h = Harness::new();
    h.phases = phases(&[
        (
            "dsv4f-tp2",
            failed(MIN, "the stop could not be sent: link down"),
        ),
        ("qwen36-fast", Phase::Stopped { since_ms: 0 }),
        ("other", Phase::Stopped { since_ms: 0 }),
    ]);
    assert_eq!(converge_at(&h, MIN + 30_000), vec![]);
    assert_eq!(converge_at(&h, 2 * MIN), vec![stop("dsv4f-tp2")]);
}

#[test]
fn an_unknown_deployment_is_stopped_unless_another_holds_its_machines() {
    let mut h = Harness::new();
    wanting(&mut h, &["other"], 0);
    h.phases = phases(&[("other", healthy(0))]);
    assert_eq!(
        converge_at(&h, MIN),
        vec![stop("qwen36-fast")],
        "dsv4f-tp2 cannot be running: `other` holds sp2"
    );
}

#[test]
fn a_pinned_deployment_is_started_but_a_failure_since_the_pin_stands() {
    let mut h = Harness::new();
    h.phases = phases(&[
        ("qwen36-fast", Phase::Stopped { since_ms: 0 }),
        ("other", Phase::Stopped { since_ms: 0 }),
        ("dsv4f-tp2", failed(MIN, "exit 1")),
    ]);
    h.control
        .pin(set(&["dsv4f-tp2"]), "operator", 2 * MIN)
        .unwrap();
    assert_eq!(converge_at(&h, 3 * MIN), vec![start("dsv4f-tp2")]);
    h.phases
        .insert(d("dsv4f-tp2"), failed(4 * MIN, "health timed out"));
    assert_eq!(converge_at(&h, 5 * MIN), vec![]);
}

#[test]
fn an_uncontrolled_pool_is_never_converged() {
    let mut h = Harness::new();
    wanting(&mut h, &["dsv4f-tp2"], 0);
    h.phases = phases(&[("qwen36-fast", healthy(0))]);
    h.control.controlled = false;
    assert_eq!(converge_at(&h, MIN), vec![]);
}

// Settings from measured loads.

#[test]
fn the_settings_derive_from_the_pool_s_slowest_healthy_load() {
    let inventory = inventory();
    let pool = &inventory.pools[&PoolId::from("sparks")];
    let (dsv4f, qwen, embed) = (d("dsv4f-tp2"), d("qwen36-fast"), d("qwen3-embed-4b"));
    let loads = [(&dsv4f, 250_400), (&qwen, 30_000), (&embed, 999_000)];
    let slowest = slowest_load_secs(pool, &inventory.deployments, loads);
    assert_eq!(
        slowest,
        Some(251),
        "rounded up; another pool's load is not counted"
    );
    assert_eq!(
        PoolSettings::default()
            .effective(slowest)
            .load_bound
            .to_string(),
        "12m 33s (from a measured load of 4m 11s)"
    );
    assert_eq!(
        slowest_load_secs(pool, &inventory.deployments, [(&embed, 999_000)]),
        None
    );
}

#[test]
fn check_set_refuses_overlaps_and_strangers() {
    let inventory = inventory();
    let pool = &inventory.pools[&PoolId::from("sparks")];
    let check = |ids: &[&str]| check_set(pool, &inventory.deployments, &set(ids));
    assert_eq!(check(&["qwen36-fast", "other"]), Ok(()));
    assert_eq!(check(&[]), Ok(()));
    assert_eq!(
        check(&["dsv4f-tp2", "other"]),
        Err("dsv4f-tp2 and other both need sp2".into())
    );
    assert_eq!(
        check(&["qwen3-embed-4b"]),
        Err("qwen3-embed-4b is not a deployment of pool sparks".into())
    );
}

#[test]
fn a_dismissed_deployment_leaves_the_pin_and_the_active_set() {
    let inventory = inventory();
    let pool = &inventory.pools[&PoolId::from("sparks")];
    let mut control = PoolControl::begin(pool, &inventory.deployments, &BTreeMap::new(), 0);
    control
        .active
        .members
        .insert(d("dsv4f-tp2"), Member::chosen(0));
    control
        .pin(BTreeSet::from([d("dsv4f-tp2")]), "agent", 0)
        .unwrap();
    assert!(control.dismiss(&d("dsv4f-tp2"), "operator", 5));
    assert_eq!(control.pin, None);
    assert!(control.active.members.is_empty());
    assert_eq!(
        control.active.why,
        Why::Dismissed {
            deployment: d("dsv4f-tp2"),
            by: "operator".into()
        }
    );
    assert_eq!(desired(&control), Some(BTreeSet::new()));
    // Nothing wanted it a second time.
    assert!(!control.dismiss(&d("dsv4f-tp2"), "operator", 6));
}

#[test]
fn only_a_models_real_name_finds_its_deployments() {
    let mut inventory = inventory();
    let recipe = inventory.recipes.get_mut(&"dsv4f-tp2".into()).unwrap();
    recipe.served_name = "deepseek-v4-flash".into();
    recipe.model.name = "deepseek-ai/DeepSeek-V4-Flash".into();
    assert_eq!(
        deployments_serving(&inventory, "deepseek-ai/DeepSeek-V4-Flash"),
        vec![d("dsv4f-tp2")]
    );
    assert_eq!(
        deployments_serving(&inventory, " DEEPSEEK-AI/deepseek-v4-flash "),
        vec![d("dsv4f-tp2")]
    );
    // The deployment id, recipe id and served name are internal keys.
    for internal in ["dsv4f-tp2", "deepseek-v4-flash", "deepseek", ""] {
        assert!(
            deployments_serving(&inventory, internal).is_empty(),
            "{internal}"
        );
    }
}
