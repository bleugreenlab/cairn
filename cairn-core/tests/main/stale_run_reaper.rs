//! CAIRN-3291: a run row nothing stands behind must not keep reading as a live
//! one.
//!
//! The incident's run sat in `status='starting'` for two hours with no process
//! registered anywhere. `latest_run_for_job` kept handing it out as the job's
//! current run and `is_active` kept saying false, so every delivery-ladder
//! decision that asks "is this recipient busy?" answered *idle* — permanently,
//! because the only thing that settled such a row was the startup sweep.
//!
//! These pin both halves of the in-session reaper: that it settles a row with
//! nothing behind it, and — the half that matters more — that it leaves alone
//! every run that is legitimately alive or legitimately mid-spawn.

use crate::common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::{BackendStdin, RunHandle};
use cairn_core::internal::execution::jobs::continue_job_impl;
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::runs::reap::{
    reap_stale_runs, reap_stale_runs_for_job, STALE_RUN_EXIT_REASON, STALE_RUN_GRACE_SECS,
};
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

/// A job with an open session and no runs yet. Runs and turns are added per test
/// so each one states the exact shape it is about.
async fn seed_job(db: &LocalDb) {
    let project_id = common::create_project(db, "REAP").await;
    db.write(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', ?1, 7, 'test issue', 'active', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES ('exec-1', 'recipe-default', 'issue-1', ?1, 'running', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment,
                                   status, created_at, updated_at)
                 VALUES ('job-1', 'exec-1', 'issue-1', ?1, 'Builder', 'builder', 'running', 1, 1)",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO sessions (id, job_id, status, backend_id, created_at, updated_at)
                 VALUES ('session-1', 'job-1', 'open', 'handle-1', 1, 1)",
                (),
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_session_id = 'session-1' WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// Seconds before now, as a run's `created_at` — how old this run looks.
fn seconds_ago(age: i64) -> i64 {
    chrono::Utc::now().timestamp() - age
}

async fn insert_run(db: &LocalDb, run_id: &str, status: &str, created_at: i64) {
    let run_id = run_id.to_string();
    let status = status.to_string();
    db.write(|conn| {
        let run_id = run_id.clone();
        let status = status.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id,
                                   created_at, updated_at, start_mode)
                 SELECT ?1, 'issue-1', project_id, 'job-1', ?2, 'session-1', ?3, ?3, 'resume'
                 FROM jobs WHERE id = 'job-1'",
                params![run_id.as_str(), status.as_str(), created_at],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn insert_turn(db: &LocalDb, turn_id: &str, run_id: &str, state: &str, head: bool) {
    let turn_id = turn_id.to_string();
    let run_id = run_id.to_string();
    let state = state.to_string();
    db.write(|conn| {
        let turn_id = turn_id.clone();
        let run_id = run_id.clone();
        let state = state.clone();
        Box::pin(async move {
            // `sequence` is unique per session, so derive it rather than
            // pinning it: a stale run can strand more than one turn.
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                                    created_at, updated_at)
                 SELECT ?1, 'session-1', ?2, 'job-1',
                        COALESCE(MAX(sequence), 0) + 1, ?3, 'initial', 1, 1
                 FROM turns WHERE session_id = 'session-1'",
                params![turn_id.as_str(), run_id.as_str(), state.as_str()],
            )
            .await?;
            if head {
                conn.execute(
                    "UPDATE jobs SET current_turn_id = ?1 WHERE id = 'job-1'",
                    params![turn_id.as_str()],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The agent's stdin, standing in for a live process so a resume takes the warm
/// path instead of spawning a CLI.
struct DiscardingStdin;

impl Write for DiscardingStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl BackendStdin for DiscardingStdin {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn register_process(orch: &Orchestrator, run_id: &str) {
    let mut processes = orch.process_state.processes.lock().unwrap();
    let handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(
            Box::new(DiscardingStdin) as Box<dyn BackendStdin>
        ))),
        Some("session-1".to_string()),
        Some("job-1".to_string()),
    );
    processes.register(run_id.to_string(), handle);
}

async fn run_status(db: &LocalDb, run_id: &str) -> Option<String> {
    common::scalar_text_by_id(db, "SELECT status FROM runs WHERE id = ?1", run_id).await
}

async fn run_exit_reason(db: &LocalDb, run_id: &str) -> Option<String> {
    common::scalar_text_by_id(db, "SELECT exit_reason FROM runs WHERE id = ?1", run_id).await
}

async fn turn_state(db: &LocalDb, turn_id: &str) -> Option<String> {
    common::scalar_text_by_id(db, "SELECT state FROM turns WHERE id = ?1", turn_id).await
}

/// The incident shape: a `starting` run, no process, a settled head turn. The
/// reaper must give the row a terminal status that says why.
#[tokio::test(flavor = "current_thread")]
async fn settles_a_run_no_process_stands_behind() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-stale", "starting", seconds_ago(7200)).await;
    insert_turn(&db, "turn-1", "run-stale", "complete", true).await;

    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 1);
    assert_eq!(
        run_status(&db, "run-stale").await.as_deref(),
        Some("crashed")
    );
    assert_eq!(
        run_exit_reason(&db, "run-stale").await.as_deref(),
        Some(STALE_RUN_EXIT_REASON)
    );
    assert_eq!(
        common::scalar_i64_by_id(
            &db,
            "SELECT COUNT(*) FROM runs WHERE id = ?1 AND exited_at IS NOT NULL",
            "run-stale"
        )
        .await,
        1
    );
}

/// A run inserted moments ago is mid-spawn, not stale. Reaping it would convert
/// every launch slower than this read into an invented crash.
#[tokio::test(flavor = "current_thread")]
async fn leaves_a_just_inserted_run_alone() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-new", "starting", seconds_ago(0)).await;
    insert_turn(&db, "turn-1", "run-new", "pending", true).await;

    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 0);
    assert_eq!(
        run_status(&db, "run-new").await.as_deref(),
        Some("starting")
    );
    assert_eq!(turn_state(&db, "turn-1").await.as_deref(), Some("pending"));
}

/// Handle presence, not occupancy. A warm process is idle by every occupancy
/// measure and completely alive; its run must survive an arbitrarily long park.
#[tokio::test(flavor = "current_thread")]
async fn leaves_a_warm_process_alone_however_old() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-warm", "live", seconds_ago(86_400)).await;
    insert_turn(&db, "turn-1", "run-warm", "complete", true).await;
    register_process(&orch, "run-warm");

    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 0);
    assert_eq!(run_status(&db, "run-warm").await.as_deref(), Some("live"));
}

/// The cold-start window: `prepare_job` has inserted the run and handed its
/// launch claim to the transport, which has not registered a process yet. The
/// claim is the evidence that this run is somebody's live launch.
#[tokio::test(flavor = "current_thread")]
async fn leaves_a_claimed_launch_alone() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-claimed", "starting", seconds_ago(7200)).await;
    insert_turn(&db, "turn-1", "run-claimed", "pending", true).await;

    let claim = orch.claim_job_launch("job-1", "run-claimed");
    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 0);
    assert_eq!(
        run_status(&db, "run-claimed").await.as_deref(),
        Some("starting")
    );

    // Dropping the claim is what the transport does when the start failed, and
    // it is the moment the row becomes nobody's.
    drop(claim);
    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 1);
    assert_eq!(
        run_status(&db, "run-claimed").await.as_deref(),
        Some("crashed")
    );
}

/// A crashed run must not leave live turns behind it: `running` was interrupted
/// by the loss, `pending` never got to start.
#[tokio::test(flavor = "current_thread")]
async fn settles_the_turns_a_stale_run_stranded() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-stale", "live", seconds_ago(7200)).await;
    insert_turn(&db, "turn-running", "run-stale", "running", false).await;
    insert_turn(&db, "turn-pending", "run-stale", "pending", true).await;

    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 1);
    assert_eq!(
        turn_state(&db, "turn-running").await.as_deref(),
        Some("interrupted")
    );
    assert_eq!(
        turn_state(&db, "turn-pending").await.as_deref(),
        Some("failed")
    );
}

/// The unattended sweep must not race a launch. A resume holds the job's launch
/// lock from run insert through process registration, and a slow one can outlast
/// the grace — so a locked job is skipped this tick, not reaped.
#[tokio::test(flavor = "current_thread")]
async fn sweep_skips_a_job_whose_launch_is_in_flight() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-stale", "starting", seconds_ago(7200)).await;
    insert_turn(&db, "turn-1", "run-stale", "complete", true).await;

    let launch_lock = orch.job_launch_lock("job-1");
    let settled_while_locked = {
        let _held = launch_lock.lock().unwrap();
        reap_stale_runs(&orch)
    };
    assert_eq!(settled_while_locked, 0);
    assert_eq!(
        run_status(&db, "run-stale").await.as_deref(),
        Some("starting")
    );

    assert_eq!(reap_stale_runs(&orch), 1);
    assert_eq!(
        run_status(&db, "run-stale").await.as_deref(),
        Some("crashed")
    );
}

/// The grace is a real boundary, not a rounding hint.
#[tokio::test(flavor = "current_thread")]
async fn the_grace_is_the_boundary() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(
        &db,
        "run-inside",
        "starting",
        seconds_ago(STALE_RUN_GRACE_SECS - 5),
    )
    .await;
    insert_run(
        &db,
        "run-outside",
        "starting",
        seconds_ago(STALE_RUN_GRACE_SECS + 5),
    )
    .await;
    insert_turn(&db, "turn-1", "run-outside", "complete", true).await;

    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 1);
    assert_eq!(
        run_status(&db, "run-inside").await.as_deref(),
        Some("starting")
    );
    assert_eq!(
        run_status(&db, "run-outside").await.as_deref(),
        Some("crashed")
    );
}

/// A registry that cannot be read is not a registry that reports every run
/// absent. A panic anywhere inside a registry operation poisons the lock for the
/// life of the host; read as "no process behind any of these rows", that one
/// poisoning would settle every live and warm run on the machine at once, sixty
/// seconds later. The reaper must decline instead.
#[tokio::test(flavor = "current_thread")]
async fn declines_to_settle_when_the_registry_cannot_be_inspected() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-stale", "live", seconds_ago(7200)).await;
    insert_turn(&db, "turn-1", "run-stale", "complete", true).await;

    // Poison the registry the way a real panic would: unwinding out of a held
    // guard. The default hook would print the backtrace as test noise.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = orch.process_state.processes.lock().unwrap();
        panic!("a registry operation panicked");
    }));
    std::panic::set_hook(hook);
    assert!(panicked.is_err());
    assert!(orch.process_state.processes.is_poisoned());

    assert_eq!(reap_stale_runs_for_job(&orch, &db, "job-1"), 0);
    assert_eq!(run_status(&db, "run-stale").await.as_deref(), Some("live"));
    assert_eq!(turn_state(&db, "turn-1").await.as_deref(), Some("complete"));
    assert_eq!(reap_stale_runs(&orch), 0);
    assert_eq!(run_status(&db, "run-stale").await.as_deref(), Some("live"));
}

/// The continue path, end to end: a resume settles the predecessor row it is
/// superseding, while the warm run it is actually reusing is untouched. Without
/// this the job keeps a second, permanently-`starting` run for every reader that
/// asks `latest_run_for_job` which run is current.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resume_settles_the_predecessor_it_supersedes() {
    let (_temp, orch) = common::test_orchestrator().await;
    let db = orch.db.local.clone();
    seed_job(&db).await;
    insert_run(&db, "run-stale", "starting", seconds_ago(7200)).await;
    insert_run(&db, "run-warm", "live", seconds_ago(600)).await;
    insert_turn(&db, "turn-1", "run-warm", "complete", true).await;
    register_process(&orch, "run-warm");

    let resumed = continue_job_impl(&orch, "job-1", Some("ping"), None, None).expect("resume");

    assert_eq!(resumed.id, "run-warm", "the warm process must be reused");
    assert_eq!(run_status(&db, "run-warm").await.as_deref(), Some("live"));
    assert_eq!(
        run_status(&db, "run-stale").await.as_deref(),
        Some("crashed")
    );
    assert_eq!(
        run_exit_reason(&db, "run-stale").await.as_deref(),
        Some(STALE_RUN_EXIT_REASON)
    );
}
