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
