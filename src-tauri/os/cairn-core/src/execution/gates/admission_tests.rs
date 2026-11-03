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

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{json, Value};

use super::sweep::tests::{count, env, Env};
use crate::execution::recipe::LaunchIntent;
use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// Bounds for a scenario: `setup` covers building its world, `bound` the part
/// under test, which starts when the scenario calls [`Timed::start`].
struct Bounds {
    setup: Duration,
    bound: Duration,
}

const SETUP: Duration = Duration::from_secs(180);

/// The scenario's signal that its setup is done and the bounded part begins.
struct Timed(mpsc::Sender<()>);

impl Timed {
    fn start(&self) {
        let _ = self.0.send(());
    }
}

type Outcome<T> = Result<T, Box<dyn std::any::Any + Send>>;

/// Run `make`'s future on a fresh `workers`-thread runtime and fail the test
/// if its bounded part does not finish within `bounds.bound`.
fn within<T, F, Fut>(name: &str, workers: usize, bounds: Bounds, make: F) -> T
where
    T: Send + 'static,
    F: FnOnce(Timed) -> Fut + Send + 'static,
    Fut: Future<Output = T> + 'static,
{
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel::<Outcome<T>>();
    std::thread::Builder::new()
        .name(format!("admission-{name}"))
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .enable_all()
                .build()
                .expect("scenario runtime");
            let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
                runtime.block_on(make(Timed(started_tx)))
            }));
            let _ = done_tx.send(outcome);
        })
        .expect("scenario thread");
    let finish = |outcome: Outcome<T>| match outcome {
        Ok(out) => out,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    match started_rx.recv_timeout(bounds.setup) {
        Ok(()) => {}
        // The scenario finished (or panicked) without starting its clock.
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            return finish(done_rx.recv().expect("scenario outcome"))
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{name}: setup did not finish within {:?}", bounds.setup)
        }
    }
    match done_rx.recv_timeout(bounds.bound) {
        Ok(outcome) => finish(outcome),
        Err(_) => panic!(
            "{name}: no progress within {:?} on {workers} runtime worker(s): the admission \
             path is deadlocked",
            bounds.bound
        ),
    }
}

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

/// The specimen (harness, 11-01 22:55-23:08 PST): with the real sweep
/// notifier installed, a `write` carrying two executions appends, then a
/// second `write` carrying one, on a saturated fleet. The first held filing
/// nudges a sweep whose pass takes the admission lock; a filing that arrives
/// while it holds the lock must wait for it without taking the worker the
/// sweep needs to finish.
fn a_two_start_write_then_a_one_start_write_complete(workers: usize) {
    within(
        "two-start-write",
        workers,
        Bounds {
            setup: SETUP,
            bound: Duration::from_secs(40),
        },
        move |timed| async move {
            let env = saturated(&format!("admission-specimen-{workers}.db"), 10..=49).await;
            // A queue forty deep keeps every sweep pass busy with the lock.
            for number in 10..=49 {
                file_on(&env.orch, number).await.unwrap();
            }
            super::sweep::install_sweep_notifier(env.orch.clone());
            timed.start();

            let orch = env.orch.clone();
            let first =
                tokio::spawn(async move { write(&orch, json!([append(1), append(2)])).await })
                    .await
                    .unwrap();
            // Past the notifier's debounce: the nudged sweep is mid-pass.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let orch = env.orch.clone();
            let second = tokio::spawn(async move { write(&orch, json!([append(3)])).await })
                .await
                .unwrap();

            assert_eq!(applied(&first), 2, "both items of the first write: {first}");
            assert_eq!(applied(&second), 1, "the second write: {second}");
            assert_eq!(held_on(&env, &[1, 2, 3]).await, 3, "three held executions");
        },
    )
}

#[test]
fn the_specimen_completes_on_one_worker() {
    a_two_start_write_then_a_one_start_write_complete(1);
}

#[test]
fn the_specimen_completes_on_two_workers() {
    a_two_start_write_then_a_one_start_write_complete(2);
}

/// A task holds the admission lock across an awaited database write and a
/// sleep, as a sweep pass does across its reads. Six capped filings and a
/// sweep that arrive meanwhile all finish once it lets go, on two workers.
#[test]
fn contended_filings_and_a_sweep_make_progress() {
    within(
        "contended",
        2,
        Bounds {
            setup: SETUP,
            bound: Duration::from_secs(10),
        },
        |timed| async move {
            let env = saturated("admission-contended.db", 11..=16).await;
            timed.start();

            let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
            let holder = {
                let (orch, db) = (env.orch.clone(), env.db.clone());
                tokio::spawn(async move {
                    let guard = orch
                        .admission_lock
                        .acquire(
                            super::capacity::AdmissionSite::SweepPass,
                            "held by the test",
                        )
                        .await
                        .unwrap();
                    let _ = locked_tx.send(());
                    db.execute(
                        "UPDATE projects SET updated_at = updated_at + 1 WHERE id = 'p'",
                        (),
                    )
                    .await
                    .unwrap();
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    drop(guard);
                })
            };
            locked_rx.await.unwrap();

            let mut filings = Vec::new();
            for number in 11..=16 {
                let orch = env.orch.clone();
                filings.push(tokio::spawn(async move { file_on(&orch, number).await }));
            }
            let sweep = {
                let orch = env.orch.clone();
                tokio::spawn(async move { super::sweep::sweep(&orch).await })
            };
            holder.await.unwrap();
            for filing in filings {
                filing.await.unwrap().unwrap();
            }
            sweep.await.unwrap().unwrap();
            assert_eq!(
                held_on(&env, &[11, 12, 13, 14, 15, 16]).await,
                6,
                "every filing held"
            );
        },
    )
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

/// A pass over a queue nothing has moved in takes the admission lock once,
/// writes nothing, and reads the queue in a number of transactions that does
/// not grow with its depth: the only per-execution read is listing each one's
/// authored gates, which happens before the lock is taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quiet_pass_takes_the_lock_once_and_writes_nothing() {
    let mut reads = Vec::new();
    for depth in [10, 60] {
        let env = queue_of(&format!("admission-pass-{depth}.db"), depth).await;
        let lock = &env.orch.admission_lock;
        let (passes, read, wrote) = (
            lock.acquisitions(AdmissionSite::SweepPass),
            env.db.read_transaction_count(),
            env.db.write_transaction_count(),
        );
        let report = super::sweep::sweep(&env.orch).await.unwrap();
        assert!(report.released.is_empty(), "{report:?}");
        assert_eq!(
            lock.acquisitions(AdmissionSite::SweepPass) - passes,
            1,
            "one acquisition for the whole pass"
        );
        assert_eq!(
            env.db.write_transaction_count() - wrote,
            0,
            "{depth} holds that did not change cost no write"
        );
        reads.push(env.db.read_transaction_count() - read);

        // The release phase's read on its own: one queue read for every
        // candidate at once.
        let read = env.db.read_transaction_count();
        let ids: Vec<String> = env
            .db
            .query_all(
                "SELECT e.id, e.issue_id FROM executions e WHERE e.status = 'held'",
                (),
                |row| Ok(format!("{}|{}", row.text(0)?, row.text(1)?)),
            )
            .await
            .unwrap();
        let pairs: Vec<(&str, &str)> = ids
            .iter()
            .map(|pair| pair.split_once('|').unwrap())
            .collect();
        let starts: Vec<super::capacity::Start<'_>> = pairs
            .iter()
            .map(|(execution_id, issue_id)| super::capacity::Start {
                db: &env.db,
                issue_id: Some(issue_id),
                execution_id: Some(execution_id),
            })
            .collect();
        assert_eq!(starts.len(), depth as usize);
        super::capacity::QueueSnapshot::load(&env.orch, &starts)
            .await
            .unwrap();
        assert_eq!(
            env.db.read_transaction_count() - read,
            3,
            "the fleet, the capacity rows, and the starts' threads: one read each"
        );
    }
    assert_eq!(
        reads[1] - reads[0],
        50,
        "fifty more held executions cost fifty more gate listings and nothing else: {reads:?}"
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

/// The acceptance measurement: a capped append that holds returns in well
/// under a second with sixty executions already queued, and ten appends in
/// one write all land and are all reported.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_filing_on_a_deep_queue_returns_in_well_under_a_second() {
    let env = saturated("admission-cost.db", 100..=179).await;
    let started = std::time::Instant::now();
    let report = write(&env.orch, json!([append(100)])).await;
    let idle = started.elapsed();
    assert_eq!(applied(&report), 1, "{report}");

    for number in 101..=160 {
        file_on(&env.orch, number).await.unwrap();
    }
    super::sweep::sweep(&env.orch).await.unwrap();
    let started = std::time::Instant::now();
    let report = write(&env.orch, json!([append(161)])).await;
    let deep = started.elapsed();
    assert_eq!(applied(&report), 1, "{report}");
    assert!(
        report["applied"][0]["summary"]
            .as_str()
            .is_some_and(|summary| summary.contains("Queued")),
        "{report}"
    );
    eprintln!(
        "held filing through the write carrier: {idle:?} with nothing queued, {deep:?} with 61 \
         queued"
    );
    assert!(
        deep < Duration::from_secs(1),
        "a held filing took {deep:?} on a 61-deep queue"
    );

    let started = std::time::Instant::now();
    let report = write(&env.orch, Value::Array((162..=171).map(append).collect())).await;
    eprintln!("ten held filings in one write: {:?}", started.elapsed());
    assert_eq!(applied(&report), 10, "{report}");
    assert_eq!(
        held_on(&env, &(162..=171).collect::<Vec<_>>()).await,
        10,
        "every item held"
    );
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
