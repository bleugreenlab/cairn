//! The execution queue's admission lock under contention (cairn/5844): a
//! filing, a sweep and a release must each make progress while the others
//! hold or wait for the lock, on as few runtime workers as a busy runner has
//! free.
//!
//! Every scenario runs on a runtime of its own, on a thread of its own, and is
//! bounded by wall-clock time from OUTSIDE that runtime. A runtime whose
//! workers are all parked cannot fire its own timers, so a deadlock bounded by
//! `tokio::time::timeout` inside it would hang the suite instead of failing
//! it. A scenario that misses its bound fails, and its runtime is abandoned.

use std::time::Duration;

use serde_json::{json, Value};

use super::sweep::tests::{count, env, Env};
use crate::execution::recipe::LaunchIntent;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// The sweep test world with the fleet capped at one, so the parent issue's
/// live coordinator fills it and every capped filing holds, plus backlog
/// issues `numbers`.
async fn saturated(name: &str, numbers: impl IntoIterator<Item = i32>) -> Env {
    let env = env(name).await;
    env.orch
        .update_settings(crate::models::UpdateSettings {
            max_concurrent_executions: Some(1),
            ..Default::default()
        })
        .unwrap();
    let mut seed = String::new();
    for number in numbers {
        seed.push_str(&format!(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                created_at, updated_at)
               VALUES('i{number}', 'p', {number}, 'Issue {number}', 'backlog', 'backlog',
                      'none', 1, 1);"
        ));
    }
    if !seed.is_empty() {
        env.db.execute_script(&seed).await.unwrap();
    }
    assert_eq!(
        super::capacity::fleet_load(&env.orch)
            .await
            .unwrap()
            .in_progress,
        1,
        "the coordinator fills the fleet"
    );
    env
}

/// A capped filing on issue `number`, as an executions append makes it.
async fn file_on(orch: &Orchestrator, number: i32) -> Result<String, String> {
    let filing = crate::mcp::handlers::executions::resolve_filing(
        orch,
        "prj",
        number,
        Some("memory-triage"),
        Default::default(),
        None,
    )
    .await?;
    crate::mcp::handlers::executions::start_execution_from_collection(
        orch,
        "prj",
        number,
        filing,
        None,
        None,
        None,
        LaunchIntent::NewAttempt,
        Vec::new(),
        crate::execution::recipe::QueuePlace::Tail,
    )
    .await
}

fn append(number: i32) -> Value {
    json!({
        "target": format!("cairn://p/prj/{number}/executions"),
        "mode": "append",
        "payload": {"recipe": "memory-triage"},
    })
}

/// One `write` through the write carrier, from the coordinator's run.
async fn write(orch: &Orchestrator, changes: Value) -> Value {
    let request = McpCallbackRequest {
        answer_bound_ms: None,
        thread_id: None,
        cwd: "/".to_string(),
        run_id: Some("run-coord".to_string()),
        tool: "write".to_string(),
        payload: json!({ "changes": changes }),
        tool_use_id: None,
        origin: None,
    };
    let raw = crate::mcp::handlers::write::handle_write(orch, &request).await;
    serde_json::from_str(&raw).unwrap_or_else(|_| panic!("{raw}"))
}

fn applied(report: &Value) -> usize {
    report["applied"].as_array().map_or(0, Vec::len)
}

async fn held_on(env: &Env, numbers: &[i32]) -> i64 {
    let ids: Vec<String> = numbers.iter().map(|n| format!("'i{n}'")).collect();
    count(
        env,
        &format!(
            "SELECT COUNT(*) FROM executions WHERE status = 'held' AND issue_id IN ({})",
            ids.join(", ")
        ),
    )
    .await
}

use super::capacity::AdmissionSite;

/// Harness's requirement, directly: each filing lets go of the admission lock
/// when its own decision and insert are done, so with no sweep running the
/// lock has no holder between the items of one write and is free at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_filing_lets_go_of_the_lock_when_it_returns() {
    let env = saturated("admission-guard-drops.db", 11..=14).await;
    let lock = &env.orch.admission_lock;
    for number in 11..=13 {
        let before = lock.acquisitions(AdmissionSite::Filing);
        file_on(&env.orch, number).await.unwrap();
        assert_eq!(
            lock.acquisitions(AdmissionSite::Filing),
            before + 1,
            "one acquisition per filing"
        );
        assert!(lock.holder().is_none(), "{:?}", lock.holder());
        let free = tokio::time::timeout(
            Duration::from_millis(100),
            lock.acquire(AdmissionSite::Release, "the test"),
        )
        .await
        .expect("the lock is free the moment a filing returns")
        .unwrap();
        drop(free);
    }

    let before = lock.acquisitions(AdmissionSite::Filing);
    let report = write(&env.orch, json!([append(14), append(1)])).await;
    assert_eq!(applied(&report), 2, "{report}");
    assert_eq!(lock.acquisitions(AdmissionSite::Filing), before + 2);
    assert!(lock.holder().is_none(), "{:?}", lock.holder());
}

/// Hold `depth` executions on a saturated fleet, numbered by one pass.
async fn queue_of(name: &str, depth: i32) -> Env {
    let env = saturated(name, 100..100 + depth).await;
    for number in 100..100 + depth {
        file_on(&env.orch, number).await.unwrap();
    }
    super::sweep::sweep(&env.orch).await.unwrap();
    env
}

/// [`queue_of`], with each held execution in a lane of its own (a parent
/// issue apiece): the most lanes a queue of `depth` can have, so a read per
/// lane would show as one per held execution.
async fn queue_in_lanes(name: &str, depth: i32) -> Env {
    let env = queue_of(name, depth).await;
    let mut seed = String::new();
    for number in 100..100 + depth {
        seed.push_str(&format!(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                created_at, updated_at)
               VALUES('lane{number}', 'p', {parent}, 'Lane {number}', 'active', 'active',
                      'none', 1, 1);
             UPDATE issues SET parent_issue_id = 'lane{number}' WHERE id = 'i{number}';",
            parent = number + 1000
        ));
    }
    env.db.execute_script(&seed).await.unwrap();
    super::sweep::sweep(&env.orch).await.unwrap();
    env
}

/// What one pass over a held queue cost, on the test's one database.
#[derive(Debug, Clone, Copy)]
struct PassCost {
    /// Read transactions over the whole pass.
    reads: usize,
    /// Statements over the whole pass.
    statements: usize,
    /// Statements while the pass held the admission lock.
    under_lock: usize,
}

/// One pass over a queue nothing changed: every held execution is a
/// candidate its cap still holds as stored. It takes the lock once, releases
/// nothing and writes nothing under it.
async fn quiet_pass(env: &Env, depth: i32) -> PassCost {
    let lock = &env.orch.admission_lock;
    let (passes, reads, statements, wrote) = (
        lock.acquisitions(AdmissionSite::SweepPass),
        env.db.read_transaction_count(),
        env.db.statement_count(),
        env.db.write_transaction_count(),
    );
    let report = super::sweep::sweep(&env.orch).await.unwrap();
    let cost = PassCost {
        reads: env.db.read_transaction_count() - reads,
        statements: env.db.statement_count() - statements,
        under_lock: report.lock_statements.expect("the pass took the lock"),
    };
    eprintln!("quiet pass at depth {depth}: {cost:?}");
    assert!(report.released.is_empty(), "{report:?}");
    assert_eq!(report.candidates, depth as usize, "{report:?}");
    assert_eq!(report.held_by_cap, depth as usize, "{report:?}");
    assert_eq!(
        lock.acquisitions(AdmissionSite::SweepPass) - passes,
        1,
        "one acquisition for the whole pass"
    );
    assert_eq!(
        report.lock_writes,
        Some(0),
        "{depth} holds that did not change cost no write under the lock"
    );
    // Outside the lock, the hold pass's supersession of orphaned supply holds
    // is one write a database, whatever the queue holds.
    assert_eq!(env.db.write_transaction_count() - wrote, 1);
    cost
}

/// A quiet pass takes the lock once and writes nothing, and what it reads
/// does not grow with the queue: the held executions' gates are listed
/// together, and under the lock every start's facts are read together, so
/// fifty more held executions, each in a lane of its own, cost no more read
/// transactions and no more statements under the lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_pass_takes_the_lock_once_and_writes_nothing() {
    let mut costs = Vec::new();
    for depth in [10, 60] {
        let env = queue_in_lanes(&format!("admission-pass-{depth}.db"), depth).await;
        costs.push(quiet_pass(&env, depth).await);
    }
    assert_eq!(
        costs[1].reads, costs[0].reads,
        "fifty more held executions cost no more read transactions: {costs:?}"
    );
    assert_eq!(
        costs[1].under_lock, costs[0].under_lock,
        "fifty more held executions cost no more statements under the lock: {costs:?}"
    );
    assert_eq!(
        costs[1].statements, costs[0].statements,
        "nor any more statements over the whole pass: {costs:?}"
    );
}

/// At the depth production reached on 11-06, one pass decides every held
/// execution under the lock in seven statements: the fleet count, the
/// capacity rows with their lanes' turns and the newest turn, and the starts'
/// devices, issues and lanes' turns. Before the starts' facts were read as
/// bound lists, this pass took 754.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pass_over_150_held_executions_reads_a_bounded_amount_under_the_lock() {
    let env = queue_in_lanes("admission-pass-150.db", 150).await;
    let cost = quiet_pass(&env, 150).await;
    assert!(
        cost.under_lock <= 7,
        "150 held executions are decided in at most 7 statements under the lock: {cost:?}"
    );
}

/// A held filing takes the lock once and decides once: the reply comes from
/// that decision, not from evaluating the execution again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_filing_takes_the_lock_once() {
    let env = queue_of("admission-filing-once.db", 5).await;
    let lock = &env.orch.admission_lock;
    let before: Vec<u64> = AdmissionSite::ALL_FOR_TESTS
        .iter()
        .map(|site| lock.acquisitions(*site))
        .collect();
    let filed = file_on(&env.orch, 1).await.unwrap();
    assert!(filed.contains("Queued"), "{filed}");
    let after: Vec<u64> = AdmissionSite::ALL_FOR_TESTS
        .iter()
        .map(|site| lock.acquisitions(*site))
        .collect();
    let taken: Vec<(AdmissionSite, u64)> = AdmissionSite::ALL_FOR_TESTS
        .iter()
        .zip(before.iter().zip(&after))
        .filter(|(_, (before, after))| after > before)
        .map(|(site, (before, after))| (*site, after - before))
        .collect();
    assert_eq!(taken, vec![(AdmissionSite::Filing, 1)]);
}

/// A filing that cannot get the lock within its bound refuses, naming who
/// holds it, and writes nothing; the same filing succeeds once the lock is
/// let go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_filing_that_cannot_get_the_lock_refuses_naming_the_holder() {
    let env = saturated("admission-busy.db", [11]).await;
    let lock = &env.orch.admission_lock;
    lock.set_wait_bound(Duration::from_millis(200));
    let pass = lock
        .acquire(AdmissionSite::SweepPass, "7 candidates")
        .await
        .unwrap();

    let refused = file_on(&env.orch, 11).await.unwrap_err();
    assert!(
        refused.contains("this filing waited")
            && refused.contains("held by a sweep_pass (7 candidates) for "),
        "the refusal names the holder and what it was doing: {refused}"
    );
    assert!(refused.contains("Nothing was written; retry."), "{refused}");
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM executions WHERE issue_id = 'i11'"
        )
        .await,
        0,
        "no execution row"
    );

    drop(pass);
    file_on(&env.orch, 11).await.unwrap();
    assert_eq!(held_on(&env, &[11]).await, 1, "the retry files it");
}

/// Within one pass, releasing an execution that runs on another device takes
/// no slot of this device's fleet, as `fleet_load` would count it: a later
/// on-device candidate still starts in the same pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn releasing_another_devices_execution_takes_no_local_slot() {
    let env = saturated("admission-off-device.db", 11..=12).await;
    file_on(&env.orch, 11).await.unwrap();
    file_on(&env.orch, 12).await.unwrap();
    assert_eq!(held_on(&env, &[11, 12]).await, 2);
    env.db
        .execute(
            "UPDATE executions SET runner_device_id = 'a-peer-device' WHERE issue_id = 'i11'",
            (),
        )
        .await
        .unwrap();
    env.orch
        .update_settings(crate::models::UpdateSettings {
            max_concurrent_executions: Some(2),
            ..Default::default()
        })
        .unwrap();

    let report = super::sweep::sweep(&env.orch).await.unwrap();
    assert_eq!(
        report.released.len(),
        2,
        "the peer's execution and this device's both start: {report:?}"
    );
    assert_eq!(held_on(&env, &[11, 12]).await, 0);
}

/// Records every agent start and starts nothing, so a released launch hold's
/// job is observed at the host seam without a process.
#[derive(Default)]
struct RecordedStarts(std::sync::Mutex<Vec<String>>);

#[async_trait::async_trait]
impl crate::effects::executor::EffectExecutor for RecordedStarts {
    async fn execute(
        &self,
        _orch: &Orchestrator,
        effect: crate::effects::types::WorkflowEffect,
    ) -> Result<Option<crate::effects::types::EffectResult>, String> {
        if let crate::effects::types::WorkflowEffect::StartAgentJobs { jobs, .. } = effect {
            self.0
                .lock()
                .unwrap()
                .extend(jobs.into_iter().map(|job| job.id));
        }
        Ok(None)
    }
}

impl RecordedStarts {
    fn started(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

/// The sweep world with thread `th` ("harness") and a recording executor.
async fn thread_world(name: &str) -> (Env, std::sync::Arc<RecordedStarts>) {
    let env = env(name).await;
    super::capacity::tests::add_thread(&env, "th", "harness").await;
    let starts = std::sync::Arc::new(RecordedStarts::default());
    assert!(env.orch.set_executor(starts.clone()).is_ok());
    (env, starts)
}

/// Child `number` whose builder node `sj{number}` is pending and placeable,
/// as a confirmed plan's recompute leaves it, in a recipe whose builder ships
/// a PR.
async fn pending_builder(env: &Env, number: i32) {
    super::capacity::tests::add_started(env, number, Some("th"), "pending").await;
    env.db
        .execute(
            "UPDATE executions SET snapshot = ?1 WHERE id = ?2",
            (
                builder_snapshot(&format!("si{number}")).to_string(),
                format!("se{number}"),
            ),
        )
        .await
        .unwrap();
    env.db
        .execute(
            "UPDATE jobs SET recipe_node_id = 'builder' WHERE id = ?1",
            (format!("sj{number}"),),
        )
        .await
        .unwrap();
}

/// An execution snapshot whose `builder` node feeds a PR node.
fn builder_snapshot(issue_id: &str) -> Value {
    json!({
        "recipe": {"id": "recipe", "name": "Recipe", "trigger": "manual",
            "nodes": [
                {"id":"trigger","name":"Trigger","nodeType":"trigger","position":{"x":0,"y":0}},
                {"id":"builder","name":"Builder","nodeType":"agent","position":{"x":0,"y":1}},
                {"id":"pr","name":"PR","nodeType":"pr","position":{"x":0,"y":2}}
            ],
            "edges": [
                {"id":"start","edgeType":"control","sourceNodeId":"trigger","sourceHandle":"control-out",
                 "targetNodeId":"builder","targetHandle":"control-in"},
                {"id":"out","edgeType":"context","sourceNodeId":"builder","sourceHandle":"context-out",
                 "targetNodeId":"pr","targetHandle":"context-in"}
            ]},
        "agents": {}, "skills": {}, "tools": {},
        "triggerContext": {"issueId": issue_id, "projectId": "p", "triggerType": "manual"},
        "delegatedPackets": [], "createdAt": 1
    })
}

/// The host's DAG-launch seam for a system re-entry, as the event loop calls
/// it; returns the hold it armed.
async fn launch_is_held(env: &Env, job: &str) -> crate::orchestrator::wakes::ProviderLimitHold {
    let admitted = crate::orchestrator::wakes::admit_thread_launch(
        &env.orch,
        job,
        crate::execution::jobs::WakeCause::System,
        false,
    )
    .await
    .unwrap();
    assert!(admitted.is_none(), "the launch is refused");
    let holds = crate::orchestrator::wakes::load_active_provider_limit_holds(&env.db)
        .await
        .unwrap();
    assert_eq!(holds.len(), 1, "{holds:?}");
    let hold = holds.into_iter().next().unwrap();
    assert_eq!(hold.job_id, job);
    assert_eq!(hold.kind, crate::orchestrator::wakes::HoldKind::Launch);
    assert_eq!(hold.next_due_at(), None, "thread holds have no timer");
    hold
}

async fn hold_state(env: &Env, id: &str) -> String {
    env.db
        .query_opt_text(
            "SELECT state FROM provider_limit_holds WHERE id = ?1",
            (id.to_string(),),
        )
        .await
        .unwrap()
        .unwrap()
}

/// Plan verification 3 (Acceptance B). At a full focus cap of 2, a confirmed
/// plan's builder launch becomes a `thread_focus` launch hold that is ahead of
/// an OLDER held fresh filing. A running builder's head then yields on its
/// lanes while its job stays `running`: the transition hints, and the hold
/// releases exactly once under a racing gate pass, scheduler timer and supply
/// release. The fresh filing stays held, because the re-entry took the slot.
#[tokio::test]
async fn a_confirmed_builder_reentry_precedes_an_older_fresh_filing_and_releases_once() {
    use super::capacity::tests::{add_issue, add_started, caps, is_held};
    use super::sweep::tests::{file, now};
    let (env, starts) = thread_world("focus-reentry-launch.db").await;
    caps(&env, 50, 2);
    add_started(&env, 100, Some("th"), "running").await;
    add_started(&env, 101, Some("th"), "running").await;
    env.db
        .execute_script(
            "INSERT INTO sessions(id, job_id, status, backend_id, created_at, updated_at)
               VALUES('ss100', 'sj100', 'open', 'backend', 1, 1);
             INSERT INTO runs(id, project_id, job_id, issue_id, status, session_id, created_at, updated_at)
               VALUES('sr100', 'p', 'sj100', 'si100', 'live', 'ss100', 1, 1);
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason,
                               created_at, updated_at)
               VALUES('st100', 'ss100', 'sr100', 'sj100', 1, 'running', 'initial', 1, 1);
             UPDATE jobs SET current_turn_id = 'st100', current_session_id = 'ss100'
              WHERE id = 'sj100';",
        )
        .await
        .unwrap();

    add_issue(&env, 103, Some("th")).await;
    let filed = file(&env, 103, json!([])).await.unwrap();
    assert!(filed.contains("harness 2/2 running"), "{filed}");
    assert!(is_held(&env, 103).await, "the older fresh filing is held");

    pending_builder(&env, 102).await;
    let hold = launch_is_held(&env, "sj102").await;
    assert_eq!(hold.boundary_source, "thread_focus");
    assert!(starts.started().is_empty());

    // The builder parks on its lanes; its job stays running.
    let hints = cairn_db::admission_hint::sent_on_this_thread();
    let parked = env
        .db
        .write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO agent_waits(id, job_id, run_id, session_id, predecessor_turn_id,
                                             tool_use_id, condition_json, state, created_at)
                       VALUES('sw100', 'sj100', 'sr100', 'ss100', 'st100', 'tool',
                              '{\"kind\":\"checks\",\"job_id\":\"sj100\"}', 'pending', 1)",
                    (),
                )
                .await?;
                crate::transitions::turn::yield_running_turn_conn(
                    conn,
                    "st100",
                    crate::models::TurnYieldReason::Wait,
                    2,
                )
                .await
            })
        })
        .await
        .unwrap();
    assert!(parked);
    assert!(
        cairn_db::admission_hint::sent_on_this_thread() > hints,
        "the yield nudges the queue"
    );
    assert_eq!(
        count(
            &env,
            "SELECT COUNT(*) FROM jobs WHERE id = 'sj100' AND status = 'running'"
        )
        .await,
        1
    );

    let (pass, timer, supply) = tokio::join!(
        super::sweep::sweep_at(&env.orch, now()),
        crate::orchestrator::wakes::resolve_provider_limit_hold(&env.orch, &hold, i64::MAX),
        crate::orchestrator::wakes::release_on_supply_change(&env.orch, &hold, now()),
    );
    let pass = pass.unwrap();
    assert!(
        !timer.unwrap(),
        "the scheduler timer never releases a thread hold"
    );
    supply.unwrap();
    assert_eq!(hold_state(&env, &hold.id).await, "resolved");
    assert_eq!(starts.started(), vec!["sj102".to_string()], "released once");
    assert!(!pass.released.iter().any(|id| id == "se103"), "{pass:?}");

    let later = super::sweep::sweep_at(&env.orch, now()).await.unwrap();
    assert!(later.released.is_empty(), "{later:?}");
    assert!(is_held(&env, 103).await, "the fresh filing waits behind it");
    assert_eq!(starts.started().len(), 1);
}

/// Plan verification 8 (Acceptance D), through the real seam and resolver. At
/// a bound of 2 a third child's PR-producing launch is held naming the open
/// PRs. The runner restarts and its real gate loop starts with a nudge channel
/// that is never sent on: the Boot pass keeps the hold, a merge is written as
/// silent SQL, and a Tick pass releases the persisted hold exactly once.
#[tokio::test]
async fn an_open_pr_bound_hold_releases_on_the_pass_after_a_merge_without_a_hint() {
    use super::capacity::tests::add_started;
    use super::sweep::tests::now;
    let (env, starts) = thread_world("open-pr-bound-release.db").await;
    env.orch
        .update_settings(crate::models::UpdateSettings {
            max_concurrent_executions: Some(50),
            max_open_children_per_thread: Some(10),
            max_open_prs_per_thread: Some(2),
            ..Default::default()
        })
        .unwrap();
    for number in [100, 101] {
        add_started(&env, number, Some("th"), "complete").await;
        env.db
            .execute_script(format!(
                "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                            target_branch, status, opened_at, updated_at)
                   VALUES('smr{number}', 'sj{number}', 'p', 'si{number}', 'PR', 'b{number}',
                          'main', 'open', 1, 1);"
            ))
            .await
            .unwrap();
    }
    pending_builder(&env, 102).await;
    // Each fact the bound reads, so a refusal that does not happen names why.
    assert_eq!(env.orch.get_settings().max_open_prs_per_thread, 2);
    let (children, ships, refusal) = env
        .db
        .read(|conn| {
            Box::pin(async move {
                Ok((
                    super::open_prs::children_conn(conn, "th").await?,
                    cairn_workflow::inputs::node_ships_a_pr_conn(conn, "builder", "se102").await?,
                    super::open_prs::launch_refusal_conn(conn, "sj102", 2, None).await?,
                ))
            })
        })
        .await
        .unwrap();
    assert_eq!(
        children,
        vec![(100, "si100".to_string()), (101, "si101".to_string())]
    );
    assert!(ships, "the builder feeds a PR node");
    assert_eq!(
        refusal.as_deref(),
        Some("harness has 2/2 open PRs: 100, 101")
    );
    let hold = launch_is_held(&env, "sj102").await;
    assert_eq!(hold.boundary_source, "thread_open_prs");
    assert_eq!(
        hold.observed_text.as_deref(),
        Some("harness has 2/2 open PRs: 100, 101")
    );

    super::sweep::sweep_at(&env.orch, now()).await.unwrap();
    assert_eq!(hold_state(&env, &hold.id).await, "active", "still bound");
    assert!(starts.started().is_empty());

    // Nothing this runner held in memory survives; only the hold row does.
    let env = super::sweep::tests::restarted(env);
    let starts = std::sync::Arc::new(RecordedStarts::default());
    assert!(env.orch.set_executor(starts.clone()).is_ok());
    let (_never_nudged, nudges) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(super::pulse::run_gate_sweep_loop(
        env.orch.clone(),
        nudges,
        super::pulse::SweepLoopConfig {
            debounce: Duration::from_millis(10),
            max_sleep: Duration::from_millis(50),
        },
    ));
    let pulse = env.orch.sweep_pulse.subscribe();
    let through = |passes: u64| {
        let mut pulse = pulse.clone();
        async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                pulse.wait_for(|summary| summary.passes_completed >= passes),
            )
            .await
            .unwrap_or_else(|_| panic!("no pass {passes} within 5s"))
            .unwrap()
            .clone()
        }
    };
    let boot = through(1).await;
    assert_eq!(boot.trigger, Some(super::pulse::Trigger::Boot));
    assert_eq!(hold_state(&env, &hold.id).await, "active", "boot keeps it");
    assert!(starts.started().is_empty());

    env.db
        .execute(
            "UPDATE merge_requests SET status = 'merged' WHERE id = 'smr100'",
            (),
        )
        .await
        .unwrap();
    // Pass 2 may have begun before the merge; pass 3 began after it.
    let tick = through(3).await;
    assert_eq!(tick.trigger, Some(super::pulse::Trigger::Tick));
    assert_eq!(hold_state(&env, &hold.id).await, "resolved");
    assert_eq!(starts.started(), vec!["sj102".to_string()]);
    through(5).await;
    assert_eq!(starts.started().len(), 1, "released once");
}

// -- one pass over held work --------------------------------------------------

/// Hold job `job` on its thread's open-PR bound, as the launch seam arms it.
async fn held_on_open_prs(
    db: &crate::storage::LocalDb,
    job: &str,
) -> crate::orchestrator::wakes::ProviderLimitHold {
    crate::orchestrator::wakes::arm_thread_hold(
        db,
        job,
        crate::orchestrator::wakes::HoldKind::Launch,
        "thread_open_prs",
        "Waiting for the thread's open PRs.".into(),
        String::new(),
        super::sweep::tests::now(),
    )
    .await
    .unwrap()
}

fn caps_and_bound(env: &Env, fleet: usize, children: i32, open_prs: i32) {
    env.orch
        .update_settings(crate::models::UpdateSettings {
            max_concurrent_executions: Some(fleet as i32),
            max_open_children_per_thread: Some(children),
            max_open_prs_per_thread: Some(open_prs),
            ..Default::default()
        })
        .unwrap();
}

async fn in_progress(env: &Env) -> usize {
    super::capacity::fleet_load(&env.orch)
        .await
        .unwrap()
        .in_progress
}

/// A team job id, routed to the `teamgate` replica `attach_team` opens.
fn team_job(n: i32) -> String {
    format!("teamgate~00000000-0000-4000-8000-{n:012}")
}

/// Thread `tz` in the team replica: child 0 has an open PR, and children
/// 1..=`count` each have a pending PR-producing builder.
async fn team_builders(team: &crate::storage::LocalDb, count: i32) {
    let snapshot = builder_snapshot("tc0").to_string();
    let mut script = "INSERT INTO threads(id, project_id, name, created_at, updated_at)
           VALUES('tz', 'tp', 'team-thread', 1, 1);"
        .to_string();
    for n in 0..=count {
        let status = if n == 0 { "complete" } else { "pending" };
        script.push_str(&format!(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention,
                                parent_thread_id, created_at, updated_at)
               VALUES('tc{n}', 'tp', {number}, 'Team {n}', 'active', 'active', 'none', 'tz', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
               VALUES('te{n}', 'recipe', 'tc{n}', 'tp', 'running', 1, 1, '{snapshot}');
             INSERT INTO jobs(id, execution_id, recipe_node_id, project_id, issue_id, node_name,
                              status, created_at, updated_at)
               VALUES('{job}', 'te{n}', 'builder', 'tp', 'tc{n}', 'builder', '{status}', 1, 1);",
            number = 100 + n,
            job = team_job(n),
        ));
    }
    script.push_str(&format!(
        "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                    target_branch, status, opened_at, updated_at)
           VALUES('tmr0', '{job}', 'tp', 'tc0', 'PR', 'b0', 'main', 'open', 1, 1);",
        job = team_job(0)
    ));
    team.execute_script(script).await.unwrap();
}

/// The acceptance bound for the orchestrator's hold-resolution cost: fifty
/// thread holds across two threads and two databases cost one fleet scan per
/// database in a pass, not one per hold. The team thread's holds are refused
/// by its open-PR bound and never reach the fleet; the private thread's pass
/// their bound and all meet a full fleet, decided against one count. A second
/// pass with nothing changed does not decide the bound holds again.
#[tokio::test]
async fn a_pass_over_fifty_thread_holds_counts_the_fleet_once_per_database() {
    use super::sweep::tests::{attach_team, now};
    let (env, starts) = thread_world("hold-pass-one-count.db").await;
    let team = attach_team(&env, "hold-pass-one-count-team.db").await;
    team_builders(&team, 25).await;
    for n in 1..=25 {
        held_on_open_prs(&team, &team_job(n)).await;
    }
    for number in 200..225 {
        pending_builder(&env, number).await;
        held_on_open_prs(&env.db, &format!("sj{number}")).await;
    }
    let full = in_progress(&env).await;
    caps_and_bound(&env, full, 50, 1);

    let scans = super::capacity::fleet_scans_on_this_thread();
    let report = crate::orchestrator::wakes::resolve_thread_holds(&env.orch, now())
        .await
        .unwrap();
    assert_eq!(report.considered, 50, "{report:?}");
    assert_eq!(report.released, 0, "{report:?}");
    assert_eq!(report.fleet_snapshots, 1, "{report:?}");
    assert_eq!(
        super::capacity::fleet_scans_on_this_thread() - scans,
        2,
        "one fleet scan for each of the two databases"
    );
    assert!(starts.started().is_empty());
    let bound = count(
        &env,
        "SELECT COUNT(*) FROM provider_limit_holds WHERE state = 'active'",
    )
    .await;
    assert_eq!(bound, 25, "the private thread's holds still wait");

    let scans = super::capacity::fleet_scans_on_this_thread();
    let again = crate::orchestrator::wakes::resolve_thread_holds(&env.orch, now())
        .await
        .unwrap();
    assert_eq!(again.considered, 50, "{again:?}");
    assert_eq!(
        again.skipped_unchanged, 25,
        "the bound holds' thread has not changed: {again:?}"
    );
    assert_eq!(again.fleet_snapshots, 1, "{again:?}");
    assert_eq!(super::capacity::fleet_scans_on_this_thread() - scans, 2);
}

/// The cost change keeps the release rule: with one free fleet slot and three
/// releasable holds, one pass releases exactly the first in queue order, and
/// its release counts before the other two are decided.
#[tokio::test]
async fn one_free_slot_releases_only_the_first_of_three_holds_in_one_pass() {
    use super::sweep::tests::now;
    let (env, starts) = thread_world("hold-pass-one-slot.db").await;
    let mut holds = Vec::new();
    for number in [300, 301, 302] {
        pending_builder(&env, number).await;
        holds.push(held_on_open_prs(&env.db, &format!("sj{number}")).await);
    }
    let free_one = in_progress(&env).await + 1;
    caps_and_bound(&env, free_one, 50, 10);

    let report = crate::orchestrator::wakes::resolve_thread_holds(&env.orch, now())
        .await
        .unwrap();
    assert_eq!(starts.started(), vec!["sj300".to_string()]);
    assert_eq!(report.released, 1, "{report:?}");
    assert_eq!(report.fleet_snapshots, 1, "{report:?}");
    assert_eq!(hold_state(&env, &holds[0].id).await, "resolved");
    assert_eq!(hold_state(&env, &holds[1].id).await, "active");
    assert_eq!(hold_state(&env, &holds[2].id).await, "active");
}

/// A hold its thread's open-PR bound refuses is answered by that bound alone:
/// neither a pass nor a supply change counts the fleet for it.
#[tokio::test]
async fn a_hold_refused_by_its_open_pr_bound_takes_no_fleet_count() {
    use super::capacity::tests::add_started;
    use super::sweep::tests::now;
    let (env, starts) = thread_world("hold-pass-bound-no-count.db").await;
    caps_and_bound(&env, 50, 10, 1);
    add_started(&env, 100, Some("th"), "complete").await;
    env.db
        .execute_script(
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                        target_branch, status, opened_at, updated_at)
               VALUES('smr100', 'sj100', 'p', 'si100', 'PR', 'b100', 'main', 'open', 1, 1);",
        )
        .await
        .unwrap();
    pending_builder(&env, 102).await;
    let hold = held_on_open_prs(&env.db, "sj102").await;

    let scans = super::capacity::fleet_scans_on_this_thread();
    let report = crate::orchestrator::wakes::resolve_thread_holds(&env.orch, now())
        .await
        .unwrap();
    assert!(
        !crate::orchestrator::wakes::release_on_supply_change(&env.orch, &hold, now())
            .await
            .unwrap()
    );
    assert_eq!(report.fleet_snapshots, 0, "{report:?}");
    assert_eq!(super::capacity::fleet_scans_on_this_thread(), scans);
    assert_eq!(hold_state(&env, &hold.id).await, "active");
    assert!(starts.started().is_empty());
    let reason = env
        .db
        .query_opt_text(
            "SELECT observed_text FROM provider_limit_holds WHERE id = ?1",
            (hold.id.clone(),),
        )
        .await
        .unwrap();
    assert_eq!(reason.as_deref(), Some("harness has 1/1 open PRs: 100"));
}

// -- Start means start (cairn/7364) ------------------------------------------

/// A thread at its open-PR bound of 1, with child 102's PR-producing builder
/// pending: the shape cairn/7339, 7360 and 7362 were held in.
async fn at_the_open_pr_bound(name: &str) -> (Env, std::sync::Arc<RecordedStarts>) {
    use super::capacity::tests::add_started;
    let (env, starts) = thread_world(name).await;
    caps_and_bound(&env, 50, 10, 1);
    add_started(&env, 100, Some("th"), "complete").await;
    env.db
        .execute_script(
            "INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch,
                                        target_branch, status, opened_at, updated_at)
               VALUES('smr100', 'sj100', 'p', 'si100', 'PR', 'b100', 'main', 'open', 1, 1);",
        )
        .await
        .unwrap();
    pending_builder(&env, 102).await;
    (env, starts)
}

async fn release_record(env: &Env, execution: &str) -> Value {
    let json = env
        .db
        .query_opt_text(
            "SELECT release_override_json FROM executions WHERE id = ?1",
            (execution.to_string(),),
        )
        .await
        .unwrap()
        .expect("the start is recorded");
    serde_json::from_str(&json).unwrap()
}

/// cairn/7360 and 7362: the operator's Start released the execution past the
/// fleet cap, and its root builder was then dispatched as a system launch,
/// which armed a `thread_open_prs` hold. Admission reads the operator's start
/// on the execution: the never-run builder launches, nothing is held, and the
/// start records the bound it skipped beside the fleet cap. A job that has run
/// before is not covered by that start and is held as ever.
#[tokio::test]
async fn an_operator_started_builder_launches_past_its_threads_open_pr_bound() {
    let (env, _starts) = at_the_open_pr_bound("operator-start-past-bound.db").await;
    env.db
        .execute(
            "UPDATE executions SET release_override_json = ?1 WHERE id = 'se102'",
            (
                json!({"principal": "operator", "at": 1, "operator": true, "skipped": ["fleet"]})
                    .to_string(),
            ),
        )
        .await
        .unwrap();

    let admitted = crate::orchestrator::wakes::admit_thread_launch(
        &env.orch,
        "sj102",
        crate::execution::jobs::WakeCause::System,
        true,
    )
    .await
    .unwrap();
    assert!(admitted.is_some(), "the operator's start is not held");
    drop(admitted);
    assert!(
        crate::orchestrator::wakes::load_active_provider_limit_holds(&env.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        release_record(&env, "se102").await["skipped"],
        json!(["fleet", "open_prs"])
    );

    env.db
        .execute_script(
            "INSERT INTO runs(id, project_id, job_id, issue_id, status, created_at, updated_at)
               VALUES('sr102', 'p', 'sj102', 'si102', 'exited', 1, 1);",
        )
        .await
        .unwrap();
    let hold = launch_is_held(&env, "sj102").await;
    assert_eq!(hold.boundary_source, "thread_open_prs");
}

/// The operator's Start on a started execution whose builder its thread's
/// open-PR bound holds releases it in one call: the hold resolves, the builder
/// is handed to a runner, the start records the bound it skipped, and the
/// system re-dispatch a refused launch gets is not held again.
#[tokio::test]
async fn start_now_on_a_job_held_execution_launches_its_builder_in_one_call() {
    let (env, starts) = at_the_open_pr_bound("start-now-job-held.db").await;
    let hold = launch_is_held(&env, "sj102").await;

    let census = env
        .db
        .read(|conn| {
            Box::pin(async move {
                super::census::census_for_executions_conn(conn, &["se102".to_string()]).await
            })
        })
        .await
        .unwrap();
    assert_eq!(
        census["se102"].job_held.as_deref(),
        Some("harness has 1/1 open PRs: 100"),
        "the issue page sees the held job, and offers Start now for it"
    );

    let outcome = super::start_now::start_now_within(
        &env.orch,
        "se102",
        json!("operator"),
        super::sweep::CapSkipAuthority::Operator,
        Duration::ZERO,
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        super::start_now::StartOutcome::Starting,
        "the recording runner starts no process, so no run reports"
    );
    assert_eq!(hold_state(&env, &hold.id).await, "resolved");
    assert_eq!(starts.started(), vec!["sj102".to_string()]);
    let record = release_record(&env, "se102").await;
    assert_eq!(record["operator"], json!(true));
    assert_eq!(record["skipped"], json!(["open_prs"]));

    let redispatched = crate::orchestrator::wakes::admit_thread_launch(
        &env.orch,
        "sj102",
        crate::execution::jobs::WakeCause::System,
        false,
    )
    .await
    .unwrap();
    assert!(redispatched.is_some(), "not held again by the bound");
    drop(redispatched);

    let again = super::start_now::start_now_within(
        &env.orch,
        "se102",
        json!("operator"),
        super::sweep::CapSkipAuthority::Operator,
        Duration::ZERO,
    )
    .await;
    assert_eq!(
        again.unwrap(),
        super::start_now::StartOutcome::AlreadyStarted
    );
}
