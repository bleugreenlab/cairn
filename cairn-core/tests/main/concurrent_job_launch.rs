//! CAIRN-3283: one wake must mint one agent context.
//!
//! A wake carrying two child facts routed each fact independently, and each
//! nudge evaluated "is this job idle?" before either had created a turn. Both
//! saw idle, both inserted a run, both drove the same turn row, and both spawned
//! a process against the same session — two independent agent contexts that each
//! ran the wake as a full turn, racing a GitHub merge and an artifact write
//! between them.
//!
//! These drive the real resume funnel (`continue_job_impl`) concurrently against
//! a job seeded in the incident's starting state, and pin the recheck predicate
//! that makes the per-job launch lock correct rather than merely serializing.
//! The fixture registers an in-memory process for the job's session so the
//! resume takes the warm path instead of spawning a real CLI; the seam under
//! test — decide, insert run, allocate turn, deliver — is the same either way.

use crate::common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use cairn_core::internal::agent_process::process::{BackendStdin, RunHandle};
use cairn_core::internal::execution::jobs::{continue_job_impl, in_flight_launch_for_test};
use cairn_core::internal::orchestrator::Orchestrator;
use cairn_core::internal::storage::LocalDb;
use cairn_db::turso::params;

/// A job one step before the wake: an open session, a live run, and a head turn
/// in `head_turn_state`. `complete` is the idle shape both nudges observed;
/// `running` is what the first launch leaves behind for the second to find.
async fn seed_job(db: &LocalDb, head_turn_state: &str) {
    let project_id = common::create_project(db, "RACE").await;
    let head_turn_state = head_turn_state.to_string();
    db.write(|conn| {
        let project_id = project_id.clone();
        let head_turn_state = head_turn_state.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                 VALUES ('issue-1', ?1, 42, 'test issue', 'active', 1, 1)",
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
                "INSERT INTO runs (id, issue_id, project_id, job_id, status, session_id,
                                   created_at, updated_at, start_mode)
                 VALUES ('run-1', 'issue-1', ?1, 'job-1', 'live', 'session-1', 1, 1, 'resume')",
                params![project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO turns (id, session_id, run_id, job_id, sequence, state, start_reason,
                                    created_at, updated_at)
                 VALUES ('turn-1', 'session-1', 'run-1', 'job-1', 1, ?1, 'initial', 1, 1)",
                params![head_turn_state.as_str()],
            )
            .await?;
            // current_session_id / current_turn_id carry FKs, so wire them only
            // once the session and turn rows exist.
            conn.execute(
                "UPDATE jobs SET current_session_id = 'session-1', current_turn_id = 'turn-1'
                 WHERE id = 'job-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

/// The agent's stdin, standing in for the live process. The backend writes one
/// newline-terminated JSON message per wake (in as many syscalls as it likes),
/// so the delivered *messages* are the lines of what accumulates here — and how
/// many times this one agent was woken is the invariant this file exists for.
struct RecordingStdin(Arc<Mutex<Vec<u8>>>);

impl Write for RecordingStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn delivered_wakes(stdin: &Arc<Mutex<Vec<u8>>>) -> usize {
    let bytes = stdin.lock().unwrap().clone();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count()
}

impl BackendStdin for RecordingStdin {
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// An in-memory handle for the job's session, so a resume takes the warm path
/// instead of spawning a CLI. Returns the log of prompts delivered to it.
fn register_process(orch: &Orchestrator, run_id: &str, session_id: &str) -> Arc<Mutex<Vec<u8>>> {
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let mut processes = orch.process_state.processes.lock().unwrap();
    let handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(
            Box::new(RecordingStdin(delivered.clone())) as Box<dyn BackendStdin>,
        ))),
        Some(session_id.to_string()),
        Some("job-1".to_string()),
    );
    processes.register(run_id.to_string(), handle);
    delivered
}

/// The state `prepare_job` leaves behind for the interval between admission and
/// process registration: a run in `starting`, a `pending` initial turn, and no
/// serving process. Read naively this is indistinguishable from an idle job.
async fn seed_cold_start(db: &LocalDb) {
    seed_job(db, "pending").await;
    db.write(|conn| {
        Box::pin(async move {
            conn.execute(
                "UPDATE runs SET status = 'starting', start_mode = 'fresh' WHERE id = 'run-1'",
                (),
            )
            .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

async fn run_count(db: &LocalDb) -> i64 {
    common::scalar_i64_by_id(db, "SELECT COUNT(*) FROM runs WHERE job_id = ?1", "job-1").await
}

async fn turn_count(db: &LocalDb) -> i64 {
    common::scalar_i64_by_id(db, "SELECT COUNT(*) FROM turns WHERE job_id = ?1", "job-1").await
}

async fn resume_event_count(db: &LocalDb) -> i64 {
    common::scalar_i64_by_id(
        db,
        "SELECT COUNT(*) FROM events WHERE run_id = ?1 AND event_type = 'user:continuation'",
        "run-1",
    )
    .await
}

/// The regression. Two resumes of one job, launched concurrently exactly as two
/// wake facts nudge it, must produce ONE agent context: one delivered resume,
/// one turn. The second must attach to the launch already in flight and return
/// its run rather than minting a second context.
///
/// Without the per-job launch lock both callers pass the idle check, both mint a
/// resume, and the assertions below see two of everything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_concurrent_resumes_mint_one_agent_context() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "complete").await;
    let delivered = register_process(&orch, "run-1", "session-1");

    let first = orch.clone();
    let second = orch.clone();
    let first = std::thread::spawn(move || continue_job_impl(&first, "job-1", None, None, None));
    let second = std::thread::spawn(move || continue_job_impl(&second, "job-1", None, None, None));
    let outcomes: Vec<Result<String, String>> = [first.join().unwrap(), second.join().unwrap()]
        .into_iter()
        .map(|result| result.map(|run| run.id))
        .collect();

    assert_eq!(
        delivered_wakes(&delivered),
        1,
        "one wake must wake the agent exactly once: {outcomes:?}"
    );
    assert_eq!(
        outcomes,
        vec![Ok("run-1".to_string()), Ok("run-1".to_string())],
        "both callers must end up on the one in-flight run — the loser attaches to it rather than \
         launching its own or being refused"
    );
    assert_eq!(
        resume_event_count(&orch.db.local).await,
        1,
        "one resume prompt is stored, not one per routed wake fact"
    );
    assert_eq!(
        turn_count(&orch.db.local).await,
        2,
        "the settled head turn plus exactly one successor for this wake"
    );
}

/// The predicate's positive case: a head turn `running` on a run whose process
/// is serving exactly that turn IS a launch in flight.
#[tokio::test]
async fn a_running_turn_on_a_serving_process_is_in_flight() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "running").await;
    let _delivered = register_process(&orch, "run-1", "session-1");
    orch.process_state.begin_turn("run-1", "turn-1");

    assert_eq!(
        in_flight_launch_for_test(&orch, "job-1").unwrap(),
        Some("run-1".to_string())
    );
}

/// The anti-wedge case, and the reason this is not a database constraint. The
/// incident's job read as idle to both nudges precisely because a run had sat in
/// `starting` for two hours with no process behind it. An active turn with no
/// live process is a STALE turn — the recovery path's business, not an in-flight
/// launch. Treating it as in-flight would refuse every future resume of the
/// node, turning a rare transient race into a permanent outage.
#[tokio::test]
async fn a_running_turn_with_no_live_process_is_not_in_flight() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "running").await;

    assert_eq!(in_flight_launch_for_test(&orch, "job-1").unwrap(), None);
}

/// A `pending` head turn is not somebody else's launch: it is a pre-created
/// retry or owned-wait successor whose owner is the caller about to start it.
#[tokio::test]
async fn a_pending_head_turn_is_not_in_flight() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "pending").await;
    let _delivered = register_process(&orch, "run-1", "session-1");
    orch.process_state.begin_turn("run-1", "turn-1");

    assert_eq!(in_flight_launch_for_test(&orch, "job-1").unwrap(), None);
}

/// The cold-start window. `prepare_job` releases the launch lock when it
/// returns, but the transport spawns and registers the process afterwards, so
/// for that whole interval the job has a run and a turn and no serving process.
/// Turn state alone cannot tell that apart from an idle job — which is exactly
/// why admission is carried in a claim rather than inferred from the database.
#[tokio::test]
async fn a_cold_start_is_in_flight_until_its_process_registers() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_cold_start(&orch.db.local).await;

    assert_eq!(
        in_flight_launch_for_test(&orch, "job-1").unwrap(),
        None,
        "nothing durable distinguishes this state from an idle job"
    );

    let claim = orch.claim_job_launch("job-1", "run-1");
    assert_eq!(
        in_flight_launch_for_test(&orch, "job-1").unwrap(),
        Some("run-1".to_string()),
        "an admitted cold start owns this job's launch until its process registers"
    );

    // The anti-wedge half: releasing admission re-opens the job immediately. The
    // claim is in memory and RAII-released, so a panic or a crash cannot leave a
    // job permanently unresumable the way a durable marker could.
    drop(claim);
    assert_eq!(in_flight_launch_for_test(&orch, "job-1").unwrap(), None);
}

/// The regression the cold-start claim exists for: a resume arriving between
/// `prepare_job` and process registration must attach to the cold start, not
/// start a second process against the same session. Deterministic — it drives
/// the real funnel against the exact state that interval leaves behind, rather
/// than racing a spawn and inspecting registry cardinality afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resume_racing_a_cold_start_attaches_instead_of_launching() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_cold_start(&orch.db.local).await;
    let _claim = orch.claim_job_launch("job-1", "run-1");

    let resuming = orch.clone();
    let outcome =
        std::thread::spawn(move || continue_job_impl(&resuming, "job-1", None, None, None))
            .join()
            .unwrap()
            .map(|run| run.id);

    assert_eq!(
        outcome,
        Ok("run-1".to_string()),
        "the resume must attach to the cold start's run"
    );
    assert_eq!(
        run_count(&orch.db.local).await,
        1,
        "no second run may be minted for the cold start's session"
    );
    assert_eq!(
        turn_count(&orch.db.local).await,
        1,
        "the cold start's initial turn is the only turn"
    );
}

/// A settled job is not in flight, so an ordinary idle resume is never refused.
#[tokio::test]
async fn a_settled_job_is_not_in_flight() {
    let (_temp, orch) = common::test_orchestrator().await;
    seed_job(&orch.db.local, "complete").await;
    let _delivered = register_process(&orch, "run-1", "session-1");

    assert_eq!(in_flight_launch_for_test(&orch, "job-1").unwrap(), None);
}
