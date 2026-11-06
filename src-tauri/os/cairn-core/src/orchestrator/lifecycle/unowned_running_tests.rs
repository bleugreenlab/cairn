//! A running job whose turn ended without finished work and that nothing owns
//! must be continued once on its warm process, or reported once when it cannot
//! be, and every shape with an owner must be left alone (cairn/6189).
//!
//! The rows are seeded directly, which is what lets each owner be added or
//! removed one at a time. The Codex reader path that produces the shape is
//! pinned in `backends::codex::runtime`'s tests.

use std::collections::HashSet;

use super::unowned_running::*;
use crate::agent_process::process::{AgentProcessState, RunHandle};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

const NOW: i64 = 10_000;
const ENDED: i64 = 1_000;
const CHILD_NODE: &str = "cairn://p/proj/2/1/builder";
const CHILD_KEY: &str = "child-unowned:cairn://p/proj/2/1/builder";

fn episode(end_reason: &str) -> Episode {
    Episode {
        turn_id: "t1".into(),
        state: "interrupted".into(),
        end_reason: end_reason.into(),
        ended_at: ENDED,
        retries: 0,
        dispatch_failed: false,
    }
}

fn alive() -> Liveness {
    Liveness::Alive {
        run_id: "run-1".into(),
    }
}

#[test]
fn the_decision_continues_only_an_unowned_failure_on_a_warm_process() {
    let resume = Decision::Resume {
        run_id: "run-1".into(),
    };
    for reason in ["provider_failure", "crash"] {
        assert_eq!(decide(&episode(reason), &alive(), false, NOW), resume);
    }
    // Every other ending has its own owner, or none is owed.
    for reason in [
        "",
        "user_stop",
        "artifact_handoff",
        "host_restart",
        "superseded",
        "provider_depleted",
    ] {
        assert_eq!(
            decide(&episode(reason), &alive(), false, NOW),
            Decision::Leave,
            "{reason}"
        );
    }
    let mut completed = episode("provider_failure");
    completed.state = "complete".into();
    assert_eq!(decide(&completed, &alive(), false, NOW), Decision::Leave);

    let failure = episode("provider_failure");
    assert_eq!(
        decide(&failure, &alive(), true, NOW),
        Decision::Leave,
        "an in-flight launch owns the job"
    );
    assert_eq!(
        decide(&failure, &alive(), false, ENDED + UNOWNED_GRACE_SECS - 1),
        Decision::Leave,
        "a fresh turn end belongs to whatever acts on it"
    );
    assert_eq!(
        decide(&failure, &Liveness::Busy, false, NOW),
        Decision::Leave
    );
    assert_eq!(
        decide(&failure, &Liveness::Dead, false, NOW),
        Decision::Leave,
        "confirmed death keeps its own policy"
    );
    assert_eq!(
        decide(&failure, &Liveness::Unknown { since: 77 }, false, NOW),
        Decision::Report(Cause::LivenessUnknown { since: 77 }),
        "unknown liveness is reported, never launched beside"
    );

    let mut spent = failure.clone();
    spent.retries = CONTINUATION_BUDGET;
    assert_eq!(
        decide(&spent, &alive(), false, NOW),
        Decision::Report(Cause::BudgetSpent)
    );
    let mut refused = failure;
    refused.dispatch_failed = true;
    assert_eq!(
        decide(&refused, &alive(), false, NOW),
        Decision::Report(Cause::DispatchFailed)
    );
}

/// A parent issue with a live coordinator, and a child issue whose builder is
/// `running` on an open session with one live run and no turns yet.
async fn seed(db: &LocalDb) {
    db.execute_script(
        "INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at)
           VALUES('p','w','P','proj','/tmp/p',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at)
           VALUES('parent','p',1,'Parent','active','active','none',1,1);
         INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at,parent_issue_id)
           VALUES('child','p',2,'Child','active','active','none',2,2,'parent');
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('parent-exec','r','parent','p','running',1,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('parent-job','p','parent','parent-exec','running','coordinator',
                  'parent-session',3,3,3);
         INSERT INTO sessions(id,job_id,status,sequence,created_at,updated_at)
           VALUES('parent-session','parent-job','open',1,3,3);
         INSERT INTO executions(id,recipe_id,issue_id,project_id,status,started_at,seq)
           VALUES('child-exec','r','child','p','running',4,1);
         INSERT INTO jobs(id,project_id,issue_id,execution_id,status,uri_segment,recipe_node_id,
                          current_session_id,created_at,updated_at,started_at)
           VALUES('child-job','p','child','child-exec','running','builder','builder',
                  'child-session',5,5,5);
         INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
           VALUES('child-session','child-job','codex','native-thread','open',1,5,5);
         INSERT INTO runs(id,project_id,job_id,session_id,status,created_at,updated_at)
           VALUES('run-1','p','child-job','child-session','live',5,500);",
    )
    .await
    .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn turn(
    db: &LocalDb,
    id: &str,
    seq: i64,
    state: &str,
    start_reason: &str,
    end_reason: Option<&str>,
    started: bool,
    predecessor: Option<&str>,
) {
    let ended = matches!(state, "complete" | "failed" | "interrupted" | "cancelled");
    db.execute(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,
                           end_reason,created_at,started_at,ended_at,updated_at)
         VALUES(?1,'child-session','run-1','child-job',?2,?3,?4,?5,?6,?7,?8,?9,?7)",
        (
            id.to_string(),
            seq,
            predecessor.map(str::to_string),
            state.to_string(),
            start_reason.to_string(),
            end_reason.map(str::to_string),
            seq,
            started.then_some(seq),
            ended.then_some(ENDED + seq),
        ),
    )
    .await
    .unwrap();
    db.execute(
        "UPDATE jobs SET current_turn_id = ?1 WHERE id = 'child-job'",
        (id.to_string(),),
    )
    .await
    .unwrap();
}

/// The specimen: one started turn the provider failed.
async fn failed_turn(db: &LocalDb) {
    turn(
        db,
        "t1",
        1,
        "interrupted",
        "initial",
        Some("provider_failure"),
        true,
        None,
    )
    .await;
}

fn warm(processes: &AgentProcessState) {
    processes
        .register_process(
            "run-1".into(),
            RunHandle::test_handle(Some("child-session"), Some("child-job")),
        )
        .unwrap();
    assert!(processes.transition_to_warm("run-1"));
}

async fn decisions(db: &LocalDb, processes: &AgentProcessState) -> Vec<Decision> {
    unowned_jobs(processes, db, &HashSet::new(), NOW)
        .await
        .unwrap()
        .into_iter()
        .map(|job| job.decision)
        .collect()
}

async fn migrated() -> LocalDb {
    crate::storage::migrated_test_db("unowned-running.db").await
}

#[tokio::test]
async fn a_warm_idle_job_whose_provider_failed_its_turn_is_continued() {
    for reason in ["provider_failure", "crash"] {
        let db = migrated().await;
        seed(&db).await;
        turn(
            &db,
            "t1",
            1,
            "interrupted",
            "initial",
            Some(reason),
            true,
            None,
        )
        .await;
        let processes = AgentProcessState::default();
        warm(&processes);
        let found = unowned_jobs(&processes, &db, &HashSet::new(), NOW)
            .await
            .unwrap();
        assert_eq!(found.len(), 1, "{reason}");
        assert_eq!(found[0].job_id, "child-job");
        assert_eq!(found[0].episode.turn_id, "t1");
        assert_eq!(found[0].child_node_uri.as_deref(), Some(CHILD_NODE));
        assert_eq!(
            found[0].decision,
            Decision::Resume {
                run_id: "run-1".into()
            }
        );
    }
}

/// Each owner, added alone to the specimen, takes the job out of scope.
#[tokio::test]
async fn every_owner_of_the_next_transition_suppresses_recovery() {
    let owners: &[(&str, &str)] = &[
        (
            "pending retry",
            "INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at)
               VALUES('t2','child-session','child-job',2,'t1','pending','retry',2,2);
             UPDATE jobs SET current_turn_id='t2' WHERE id='child-job';",
        ),
        (
            "running successor",
            "INSERT INTO turns(id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,started_at,updated_at)
               VALUES('t2','child-session','run-1','child-job',2,'t1','running','follow_up',2,2,2);
             UPDATE jobs SET current_turn_id='t2' WHERE id='child-job';",
        ),
        (
            "open question",
            "INSERT INTO turns(id,session_id,run_id,job_id,sequence,predecessor_id,state,yield_reason,start_reason,created_at,started_at,updated_at)
               VALUES('t2','child-session','run-1','child-job',2,'t1','yielded','user_input','follow_up',2,2,2);
             UPDATE jobs SET current_turn_id='t2' WHERE id='child-job';",
        ),
        (
            "durable wait",
            "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at)
               VALUES('w1','child-job','run-1','child-session','t1','tool-1','{}','pending',2);",
        ),
        (
            "closed session",
            "UPDATE sessions SET status='closed' WHERE id='child-session';",
        ),
        (
            "superseded execution",
            "UPDATE executions SET lifecycle='superseded' WHERE id='child-exec';",
        ),
        (
            "resolved issue",
            "UPDATE issues SET status='merged' WHERE id='child';",
        ),
        (
            "finished job",
            "UPDATE jobs SET status='complete' WHERE id='child-job';",
        ),
        (
            "workflow",
            "UPDATE jobs SET agent_config_id='workflow' WHERE id='child-job';",
        ),
        (
            "operator stop",
            "UPDATE turns SET end_reason='user_stop' WHERE id='t1';",
        ),
        (
            "artifact handoff",
            "UPDATE turns SET state='complete', end_reason='artifact_handoff' WHERE id='t1';",
        ),
    ];
    for (owner, sql) in owners {
        let db = migrated().await;
        seed(&db).await;
        failed_turn(&db).await;
        db.execute_script(*sql).await.unwrap();
        let processes = AgentProcessState::default();
        warm(&processes);
        assert_eq!(decisions(&db, &processes).await, vec![], "{owner}");
    }

    // In-memory owners: a launch reservation, and a process that is serving.
    let db = migrated().await;
    seed(&db).await;
    failed_turn(&db).await;
    let processes = AgentProcessState::default();
    warm(&processes);
    let claimed = HashSet::from(["child-job".to_string()]);
    assert!(unowned_jobs(&processes, &db, &claimed, NOW)
        .await
        .unwrap()
        .is_empty());
    assert!(processes.begin_turn("run-1", "other"));
    assert_eq!(decisions(&db, &processes).await, vec![]);
}

#[tokio::test]
async fn liveness_is_read_from_the_process_not_from_the_run_row() {
    // No process, yet the run reads live: unknown since its last update.
    let db = migrated().await;
    seed(&db).await;
    failed_turn(&db).await;
    assert_eq!(
        decisions(&db, &AgentProcessState::default()).await,
        vec![Decision::Report(Cause::LivenessUnknown { since: 500 })]
    );

    // No process and the run is over: death has its own policy.
    db.execute_script("UPDATE runs SET status='crashed' WHERE id='run-1';")
        .await
        .unwrap();
    assert_eq!(decisions(&db, &AgentProcessState::default()).await, vec![]);

    // A warm process outranks a run row that reads crashed: the specimen as
    // current main records it.
    let processes = AgentProcessState::default();
    warm(&processes);
    assert_eq!(
        decisions(&db, &processes).await,
        vec![Decision::Resume {
            run_id: "run-1".into()
        }]
    );
}

#[tokio::test]
async fn the_retry_chain_bounds_continuations_and_a_failed_dispatch_is_not_retried() {
    // Three automatic retries, each failed by the provider: the budget is spent.
    let db = migrated().await;
    seed(&db).await;
    failed_turn(&db).await;
    for (seq, id, predecessor) in [(2, "r1", "t1"), (3, "r2", "r1"), (4, "r3", "r2")] {
        turn(
            &db,
            id,
            seq,
            "interrupted",
            "retry",
            Some("provider_failure"),
            true,
            Some(predecessor),
        )
        .await;
    }
    let processes = AgentProcessState::default();
    warm(&processes);
    let found = unowned_jobs(&processes, &db, &HashSet::new(), NOW)
        .await
        .unwrap();
    assert_eq!(found[0].episode.turn_id, "r3");
    assert_eq!(found[0].episode.retries, 3);
    assert_eq!(found[0].decision, Decision::Report(Cause::BudgetSpent));

    // A continuation that was claimed and abandoned before it started: the
    // failed turn stays the episode, and it is reported rather than reclaimed.
    let db = migrated().await;
    seed(&db).await;
    failed_turn(&db).await;
    turn(
        &db,
        "r1",
        2,
        "interrupted",
        "retry",
        None,
        false,
        Some("t1"),
    )
    .await;
    let found = unowned_jobs(&processes, &db, &HashSet::new(), NOW)
        .await
        .unwrap();
    assert_eq!(found[0].episode.turn_id, "t1");
    assert!(found[0].episode.dispatch_failed);
    assert_eq!(found[0].decision, Decision::Report(Cause::DispatchFailed));
}

fn orchestrator(db: LocalDb) -> Orchestrator {
    super::warm_completion_tests::test_orchestrator(db)
}

async fn run_sweep(orch: &Orchestrator) -> UnownedSweep {
    let orch = orch.clone();
    tokio::task::spawn_blocking(move || {
        let db = orch.db.local.clone();
        sweep_unowned_running_in(&orch, &db, NOW).unwrap()
    })
    .await
    .unwrap()
}

async fn child_unowned_pushes(db: &LocalDb) -> Vec<(String, String, String)> {
    db.query_all(
        "SELECT recipient, key, COALESCE(fingerprint, '') FROM attention_pushes
          WHERE key LIKE 'child-unowned:%' ORDER BY created_at",
        (),
        |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknowable_child_is_reported_to_its_parent_once_and_withdrawn_when_it_moves() {
    let db = migrated().await;
    seed(&db).await;
    failed_turn(&db).await;
    let orch = orchestrator(db);
    let db = orch.db.local.clone();

    let first = run_sweep(&orch).await;
    assert!(
        first.resumed.is_empty(),
        "nothing launches beside an unknown provider"
    );
    assert_eq!(
        first.reported,
        vec![("child-job".to_string(), vec!["parent-job".to_string()])]
    );
    assert_eq!(
        child_unowned_pushes(&db).await,
        vec![(
            "parent-job".to_string(),
            CHILD_KEY.to_string(),
            "t1:liveness_unknown".to_string()
        )]
    );

    let body = super::unowned_running::render_unowned_push_body(&orch, "parent-job", CHILD_NODE)
        .await
        .unwrap();
    assert!(
        body.contains("marked running, but nothing is moving it"),
        "{body}"
    );
    assert!(body.contains("`t1`"), "{body}");
    assert!(body.contains("`provider_failure`"), "{body}");
    assert!(body.contains("unknown"), "{body}");

    // Every later tick sees the same episode and wakes nobody.
    for _ in 0..2 {
        assert!(run_sweep(&orch).await.reported.is_empty());
    }
    assert_eq!(child_unowned_pushes(&db).await.len(), 1);

    // The child moves on: no undelivered fact for it survives the next sweep
    // (the parent's wake may already have delivered the first), and a late
    // delivery says it moved on instead of sending the parent to look.
    db.execute_script(
        "INSERT INTO turns(id,session_id,run_id,job_id,sequence,predecessor_id,state,start_reason,created_at,started_at,updated_at)
           VALUES('t2','child-session','run-1','child-job',2,'t1','running','follow_up',2,2,2);
         UPDATE jobs SET current_turn_id='t2' WHERE id='child-job';",
    )
    .await
    .unwrap();
    let moved = run_sweep(&orch).await;
    assert!(moved.reported.is_empty() && moved.resumed.is_empty());
    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM attention_pushes
              WHERE key LIKE 'child-unowned:%' AND delivered_event_id IS NULL
                AND retired_at IS NULL",
            (),
        )
        .await
        .unwrap(),
        Some(0)
    );
    let body = super::unowned_running::render_unowned_push_body(&orch, "parent-job", CHILD_NODE)
        .await
        .unwrap();
    assert!(body.contains("no longer in that state"), "{body}");
}

/// Repeated sweeps over a warm, unowned job claim at most one continuation for
/// the episode, whether or not its launch takes, and a launch that does not
/// take is reported once rather than retried.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_sweeps_claim_one_continuation_per_episode() {
    let db = migrated().await;
    seed(&db).await;
    failed_turn(&db).await;
    let orch = orchestrator(db);
    warm(&orch.process_state);
    let db = orch.db.local.clone();

    for _ in 0..3 {
        run_sweep(&orch).await;
    }
    let successors = db
        .query_all(
            "SELECT start_reason FROM turns WHERE predecessor_id = 't1'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(successors, vec!["retry".to_string()]);
    let reports = child_unowned_pushes(&db).await;
    assert!(
        reports.len() <= 1,
        "a failed dispatch is reported at most once: {reports:?}"
    );
    if let Some((_, _, fingerprint)) = reports.first() {
        assert_eq!(fingerprint, "t1:dispatch_failed");
    }
}
