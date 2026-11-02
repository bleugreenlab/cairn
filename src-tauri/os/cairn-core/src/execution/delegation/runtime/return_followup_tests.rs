//! A delegated task that ends its turn without `return` (cairn/5783).
//!
//! Each test spawns a real blocking task through `spawn_task_packets`, parks
//! its parent on the durable wait, then plays the child's turns against the
//! real turn-end paths. The re-prompt is delivered by the real idle flush,
//! which resumes the child's warm process into its next turn; what the parent
//! receives is read back from the synthetic `tool_result` the resume stores
//! against the spawning call.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::agent_process::completion::{ClaudeTurnOutcome, OwnedTurnOutcome};
use crate::agent_process::process::RunHandle;
use crate::execution::delegation::{settle_missing_return, MissingReturn};
use crate::models::RunStatus;
use crate::orchestrator::attention_push::{revert_turn_deliveries, TurnDelivery};
use crate::orchestrator::lifecycle::{
    apply_claude_artifact_handoff, apply_claude_turn_completion, apply_owned_turn_outcome,
    finalize_run, transition_to_warm_state, ClaudeTurnContext,
};
use crate::orchestrator::Orchestrator;
use cairn_db::turso::params;

use super::spawn::unstarted_child_tests::{
    await_successor, count, delivered_tool_result, explore_payload, parent_fixture,
    register_parent_process, spawn, spawn_and_park, successor_count, text,
};
use super::DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX;

const REPROMPT_PREFIX: &str = "Your task ended without calling `return`";
const LAST_WORDS: &str = "Let me check how placement treats a drained executor";
const RETURNED: &str = "Placement skips drained executors before scoring the rest.";

/// Place the parked task on a live run with an open Claude session and open its
/// first work turn, the way a runner does. The child's model and session match
/// its process, so the re-prompt's continuation reuses that warm process rather
/// than spawning a provider the test environment has no account for.
async fn open_child_turn(orch: &Orchestrator, child: &str, run_id: &str, turn_id: &str) {
    let (child_owned, run, turn) = (child.to_string(), run_id.to_string(), turn_id.to_string());
    orch.db
        .local
        .write(move |conn| {
            let (child, run, turn) = (child_owned.clone(), run.clone(), turn.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT OR IGNORE INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
                     VALUES('session-child',?1,'claude','backend-child','open',1,2,2)",
                    params![child.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id,job_id,issue_id,project_id,session_id,status,start_mode,created_at,updated_at)
                     VALUES(?1,?2,'i','p','session-child','live','fresh',10,10)",
                    params![run.as_str(), child.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
                     VALUES(?1,'session-child',?2,?3,1,'running','initial',10,10,10)",
                    params![turn.as_str(), run.as_str(), child.as_str()],
                )
                .await?;
                conn.execute(
                    "UPDATE jobs SET status='running', started_at=10, current_turn_id=?1,
                         current_session_id='session-child', model='opus' WHERE id=?2",
                    params![turn.as_str(), child.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    let mut handle = RunHandle::new(
        Arc::new(Mutex::new(None)),
        Arc::new(Mutex::new(Some(
            cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
        ))),
        Some("session-child".into()),
        Some(child.to_string()),
    );
    handle.model = Some("opus".to_string());
    handle.backend = Some("claude".to_string());
    handle.begin_turn(turn_id);
    orch.process_state
        .processes
        .lock()
        .unwrap()
        .register(run_id.to_string(), handle);
    orch.process_state
        .set_current_turn_id(run_id, Some(turn_id));
}

/// The turn the idle flush resumed the child into, once the provider has
/// started it: `(run_id, turn_id)`.
async fn continued_turn(orch: &Orchestrator, child: &str, after: &str) -> (String, String) {
    let started = std::time::Instant::now();
    let (run_id, turn_id) = loop {
        let head = text(
            orch,
            &format!(
                "SELECT t.run_id || '|' || t.id FROM jobs j JOIN turns t ON t.id = j.current_turn_id
                 WHERE j.id = '{child}' AND t.id != '{after}'"
            ),
        )
        .await;
        if let Some((run_id, turn_id)) = head.as_deref().and_then(|head| head.split_once('|')) {
            break (run_id.to_string(), turn_id.to_string());
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the child was never resumed past {after}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    orch.db
        .local
        .execute(
            "UPDATE turns SET state = 'running', started_at = 20 WHERE id = ?1 AND state = 'pending'",
            params![turn_id.clone()],
        )
        .await
        .unwrap();
    orch.process_state.begin_turn(&run_id, &turn_id);
    orch.process_state
        .set_current_turn_id(&run_id, Some(&turn_id));
    (run_id, turn_id)
}

/// The child says something mid-thought on its turn, which is exactly what the
/// parent used to receive as the task's result.
async fn say(orch: &Orchestrator, run_id: &str, turn_id: &str, words: &str) {
    let data = serde_json::json!({
        "eventType": "assistant",
        "content": words,
        "isError": false
    })
    .to_string();
    orch.db
        .local
        .execute(
            "INSERT INTO events(id,run_id,session_id,sequence,timestamp,event_type,data,turn_id,created_at)
             VALUES(?1,?2,'session-child',
                    (SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE run_id = ?2),
                    100,'assistant',?3,?4,100)",
            params![
                format!("said-{turn_id}"),
                run_id.to_string(),
                data,
                turn_id.to_string()
            ],
        )
        .await
        .unwrap();
}

/// End the child's turn cleanly, with no terminal tool, through the warm
/// turn-end transition. The process stays warm, so the re-prompt the turn end
/// queues is delivered into it.
async fn end_turn_without_return(orch: &Orchestrator, run_id: &str) {
    let orch = orch.clone();
    let run_id = run_id.to_string();
    tokio::task::spawn_blocking(move || {
        assert!(transition_to_warm_state(&orch, &run_id, None));
    })
    .await
    .unwrap();
}

/// End the child's turn the way Claude reports a clean Result for a
/// task-spawned run: the run is finalized as exited.
async fn end_claude_task_turn(orch: &Orchestrator, run_id: &str) {
    let orch = orch.clone();
    let run_id = run_id.to_string();
    tokio::task::spawn_blocking(move || {
        apply_claude_turn_completion(
            &orch,
            &orch.db.local,
            &orch.services.emitter,
            &run_id,
            Some("session-child"),
            ClaudeTurnOutcome::Completed,
            ClaudeTurnContext {
                terminal_tool_completed: false,
                terminal_interrupt_ack_pending: false,
            },
        );
    })
    .await
    .unwrap();
}

async fn write_return(orch: &Orchestrator, child: &str, content: &str) {
    orch.db
        .local
        .execute(
            "INSERT INTO artifacts(id,job_id,artifact_type,output_name,data,version,created_at,updated_at)
             VALUES('child-result',?1,'return','return',?2,1,30,30)",
            params![
                child.to_string(),
                serde_json::json!({ "content": content }).to_string()
            ],
        )
        .await
        .unwrap();
}

/// Write the task's `return` artifact and end its turn through the warm
/// artifact-handoff path the `return` tool takes.
async fn end_turn_with_return(orch: &Orchestrator, child: &str, run_id: &str, content: &str) {
    write_return(orch, child, content).await;
    let orch = orch.clone();
    let run_id = run_id.to_string();
    tokio::task::spawn_blocking(move || {
        apply_claude_artifact_handoff(&orch, &orch.services.emitter, &run_id);
    })
    .await
    .unwrap();
}

/// Every system re-prompt addressed to the child's runs.
async fn reprompts(orch: &Orchestrator, child: &str) -> i64 {
    count(
        orch,
        &format!(
            "SELECT COUNT(*) FROM messages m JOIN runs r ON r.id = m.recipient_run_id
             WHERE r.job_id = '{child}' AND m.channel_type = 'direct'
               AND m.sender_name = 'system' AND m.sender_run_id IS NULL
               AND m.content LIKE '{REPROMPT_PREFIX}%'"
        ),
    )
    .await
}

/// The turn the re-prompt's push was delivered into, if it has been.
async fn reprompt_delivered_into(orch: &Orchestrator, child: &str) -> Option<String> {
    text(
        orch,
        &format!(
            "SELECT e.turn_id FROM attention_pushes ap
             JOIN messages m ON ap.key = 'direct:' || m.id
             JOIN events e ON e.id = ap.delivered_event_id
             WHERE ap.recipient = '{child}' AND m.content LIKE '{REPROMPT_PREFIX}%'"
        ),
    )
    .await
}

async fn job_status(orch: &Orchestrator, child: &str) -> String {
    text(
        orch,
        &format!("SELECT status FROM jobs WHERE id = '{child}'"),
    )
    .await
    .expect("the child job exists")
}

async fn abandoned_at(orch: &Orchestrator, child: &str) -> Option<String> {
    text(
        orch,
        &format!("SELECT CAST(return_abandoned_at AS TEXT) FROM jobs WHERE id = '{child}'"),
    )
    .await
}

async fn settle_directly(orch: &Orchestrator, child: &str, run_id: &str) -> MissingReturn {
    let orch = orch.clone();
    let (child, run_id) = (child.to_string(), run_id.to_string());
    tokio::task::spawn_blocking(move || settle_missing_return(&orch, &child, &run_id).unwrap())
        .await
        .unwrap()
}

async fn on_blocking_thread(
    orch: &Orchestrator,
    work: impl FnOnce(&Orchestrator) + Send + 'static,
) {
    let orch = orch.clone();
    tokio::task::spawn_blocking(move || work(&orch))
        .await
        .unwrap();
}

/// The first work turn ends with assistant text and no `return`: one system
/// re-prompt goes out and resumes the child into its next turn.
async fn first_miss(orch: &Orchestrator, child: &str) -> (String, String) {
    open_child_turn(orch, child, "run-child", "child-turn-1").await;
    say(orch, "run-child", "child-turn-1", LAST_WORDS).await;
    end_turn_without_return(orch, "run-child").await;
    assert_eq!(
        reprompts(orch, child).await,
        1,
        "the child is sent exactly one system re-prompt"
    );
    assert_eq!(
        text(
            orch,
            &format!("SELECT return_followup_turn_id FROM jobs WHERE id = '{child}'")
        )
        .await
        .as_deref(),
        Some("child-turn-1"),
        "the re-prompt is recorded against the turn whose end triggered it"
    );
    assert_eq!(
        successor_count(orch).await,
        0,
        "the parent is not woken with the child's mid-thought text"
    );
    let next = continued_turn(orch, child, "child-turn-1").await;
    assert_eq!(
        reprompt_delivered_into(orch, child).await.as_deref(),
        Some(next.1.as_str()),
        "the re-prompt is what the child's next turn carried"
    );
    next
}

/// The child's turn ends with assistant text and no `return`. Cairn sends it
/// one system follow-up instead of handing that text to the parent, the child
/// returns on the follow-up, and the parent receives what it returned.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_that_ends_without_return_is_asked_once_and_its_return_reaches_the_parent() {
    let (orch, child, _requests) = spawn_and_park("return-followup-returns.db").await;
    let (run_id, _turn) = first_miss(&orch, &child).await;

    end_turn_with_return(&orch, &child, &run_id, RETURNED).await;

    await_successor(&orch).await;
    assert_eq!(successor_count(&orch).await, 1);
    let delivered = delivered_tool_result(&orch)
        .await
        .expect("the parent receives the task's result");
    assert!(delivered.contains(RETURNED), "{delivered}");
    assert!(!delivered.contains(LAST_WORDS), "{delivered}");
    assert_eq!(job_status(&orch, &child).await, "complete");
    assert_eq!(reprompts(&orch, &child).await, 1);
}

/// The child ignores its re-prompt: the next work turn, which carried it, also
/// ends without `return`. The task fails, and the parent resumes without any
/// manual wake, told plainly that no result came, with the task's last words
/// and its address, as an error. No second re-prompt goes out, through a
/// replayed finalize, a boot reconcile, the repair resume, or the hook itself.
#[tokio::test(flavor = "multi_thread")]
async fn a_task_that_ignores_its_reprompt_fails_and_its_parent_is_told_so() {
    let (orch, child, _requests) = spawn_and_park("return-followup-ignored.db").await;
    let (run_id, turn_id) = first_miss(&orch, &child).await;
    say(&orch, &run_id, &turn_id, "Still reading the scorer").await;
    end_turn_without_return(&orch, &run_id).await;

    assert_eq!(job_status(&orch, &child).await, "failed");
    assert!(abandoned_at(&orch, &child).await.is_some());
    await_successor(&orch).await;
    assert_eq!(successor_count(&orch).await, 1);
    let delivered = delivered_tool_result(&orch)
        .await
        .expect("the parent is told what happened");
    assert!(
        delivered.contains(
            "Task 'map the fleet' ended without returning a result (asked once). Its last \
             message was: Still reading the scorer."
        ),
        "{delivered}"
    );
    assert!(
        delivered.contains("Read it at cairn://p/prj/7/1/builder/task/"),
        "{delivered}"
    );
    assert_eq!(
        count(
            &orch,
            "SELECT COUNT(*) FROM events WHERE run_id='run-parent' AND event_type='tool_result' \
             AND json_extract(data,'$.toolUseId')='tool-write' AND json_extract(data,'$.isError')",
        )
        .await,
        1,
        "the spawning call's result is an error"
    );
    assert_eq!(reprompts(&orch, &child).await, 1);

    let replay_run = run_id.clone();
    on_blocking_thread(&orch, move |orch| {
        let _ = finalize_run(orch, &replay_run, RunStatus::Exited);
        let _ = finalize_run(orch, &replay_run, RunStatus::Exited);
    })
    .await;
    on_blocking_thread(&orch, |orch| {
        crate::runs::queries::reconcile_stale_runs(orch, orch.services.emitter.as_ref(), 1_000_000);
    })
    .await;
    let repair_child = child.clone();
    on_blocking_thread(&orch, move |orch| {
        let _ =
            crate::execution::delegation::resume_parked_parent_after_repair(orch, &repair_child);
    })
    .await;
    assert_eq!(
        settle_directly(&orch, &child, &run_id).await,
        MissingReturn::NotOwed
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        reprompts(&orch, &child).await,
        1,
        "never a second re-prompt"
    );
    assert_eq!(successor_count(&orch).await, 1, "the parent resumes once");
    assert_eq!(job_status(&orch, &child).await, "failed");
}

/// "Asked once" means the child was asked: a work turn that ended without the
/// re-prompt reaching it (its delivery was returned, as a launch that never
/// reached a process returns it) does not abandon the task. The turn that
/// does carry it decides.
#[tokio::test(flavor = "multi_thread")]
async fn a_turn_that_never_saw_the_reprompt_does_not_abandon_the_task() {
    let (orch, child, _requests) = spawn_and_park("return-followup-unseen.db").await;
    let (run_id, unseen) = first_miss(&orch, &child).await;
    revert_turn_deliveries(&orch.db.local, &unseen, TurnDelivery::NeverSent)
        .await
        .unwrap();
    assert_eq!(reprompt_delivered_into(&orch, &child).await, None);

    end_turn_without_return(&orch, &run_id).await;
    assert_eq!(abandoned_at(&orch, &child).await, None);
    assert_ne!(job_status(&orch, &child).await, "failed");
    assert_eq!(reprompts(&orch, &child).await, 1);
    assert_eq!(successor_count(&orch).await, 0);

    let (run_id, seen) = continued_turn(&orch, &child, &unseen).await;
    assert_eq!(
        reprompt_delivered_into(&orch, &child).await.as_deref(),
        Some(seen.as_str())
    );
    end_turn_without_return(&orch, &run_id).await;
    assert_eq!(job_status(&orch, &child).await, "failed");
    assert_eq!(reprompts(&orch, &child).await, 1);
}

/// Replaying the turn end that sent the re-prompt, before the child has taken
/// another turn, neither sends a second one nor abandons the task.
#[tokio::test(flavor = "multi_thread")]
async fn replaying_the_first_turn_end_sends_nothing_more() {
    let (orch, child, _requests) = spawn_and_park("return-followup-replay.db").await;
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    // The turn ended and its job was recomputed Blocked; the hook itself is
    // what the replays repeat.
    orch.db
        .local
        .execute(
            "UPDATE turns SET state = 'complete', ended_at = 11 WHERE id = 'child-turn-1'",
            (),
        )
        .await
        .unwrap();
    let recompute_child = child.clone();
    on_blocking_thread(&orch, move |orch| {
        crate::execution::advancement::recompute_job(orch, &recompute_child).unwrap();
    })
    .await;
    assert_eq!(job_status(&orch, &child).await, "blocked");

    assert_eq!(
        settle_directly(&orch, &child, "run-child").await,
        MissingReturn::Asked
    );
    for _ in 0..2 {
        assert_eq!(
            settle_directly(&orch, &child, "run-child").await,
            MissingReturn::AwaitingReply
        );
    }
    assert_eq!(reprompts(&orch, &child).await, 1);
    assert_eq!(abandoned_at(&orch, &child).await, None);
    assert_eq!(job_status(&orch, &child).await, "blocked");
}

/// A turn yielded on an armed wait has not ended its work.
#[tokio::test(flavor = "multi_thread")]
async fn no_reprompt_for_a_turn_yielded_on_a_wait() {
    let (orch, child, _requests) = spawn_and_park("return-followup-yielded.db").await;
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    orch.db
        .local
        .execute(
            "UPDATE turns SET state = 'yielded', yield_reason = 'dependency_wait', ended_at = 11
             WHERE id = 'child-turn-1'",
            (),
        )
        .await
        .unwrap();
    end_claude_task_turn(&orch, "run-child").await;
    assert_eq!(reprompts(&orch, &child).await, 0);
    assert_ne!(job_status(&orch, &child).await, "failed");
}

/// The operator stopped the task: that is their call, not a missing return.
#[tokio::test(flavor = "multi_thread")]
async fn no_reprompt_after_an_operator_stop() {
    let (orch, child, _requests) = spawn_and_park("return-followup-stopped.db").await;
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    on_blocking_thread(&orch, |orch| {
        apply_owned_turn_outcome(
            orch,
            "run-child",
            "session-child",
            "Seam",
            OwnedTurnOutcome::Cancelled,
        );
    })
    .await;
    assert_eq!(reprompts(&orch, &child).await, 0);
    assert_eq!(successor_count(&orch).await, 0);
}

/// A failed turn fails the task as it always did, and releases the parent with
/// that failure rather than a re-prompt.
#[tokio::test(flavor = "multi_thread")]
async fn no_reprompt_for_a_failed_turn() {
    let (orch, child, _requests) = spawn_and_park("return-followup-failed.db").await;
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    on_blocking_thread(&orch, |orch| {
        apply_owned_turn_outcome(
            orch,
            "run-child",
            "session-child",
            "Seam",
            OwnedTurnOutcome::Failed {
                message: "fixture failure".to_string(),
            },
        );
    })
    .await;
    assert_eq!(reprompts(&orch, &child).await, 0);
    assert_eq!(job_status(&orch, &child).await, "failed");
    await_successor(&orch).await;
    let delivered = delivered_tool_result(&orch).await.expect("the failure");
    assert!(delivered.contains("failed"), "{delivered}");
    assert!(
        !delivered.contains("ended without returning"),
        "{delivered}"
    );
}

/// A task waiting on live children of its own is waiting, not stalled: their
/// completion wakes it. Once they settle, its next turn that ends without
/// `return` is re-prompted as usual.
#[tokio::test(flavor = "multi_thread")]
async fn no_reprompt_while_the_task_has_live_children() {
    let (orch, child, _requests) = spawn_and_park("return-followup-children.db").await;
    orch.db
        .local
        .execute(
            "INSERT INTO jobs(id,parent_job_id,project_id,issue_id,status,created_at,updated_at)
             VALUES('j-grandchild',?1,'p','i','running',5,5)",
            params![child.clone()],
        )
        .await
        .unwrap();
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    end_turn_without_return(&orch, "run-child").await;
    assert_eq!(reprompts(&orch, &child).await, 0);
    assert_eq!(job_status(&orch, &child).await, "blocked");

    orch.db
        .local
        .execute(
            "UPDATE jobs SET status = 'complete' WHERE id = 'j-grandchild'",
            (),
        )
        .await
        .unwrap();
    // The settled child's completion wakes the task into another turn.
    orch.db
        .local
        .execute(
            "INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,started_at,updated_at)
             VALUES('child-turn-2','session-child','run-child',?1,2,'running','follow_up',20,20,20)",
            params![child.clone()],
        )
        .await
        .unwrap();
    orch.process_state.begin_turn("run-child", "child-turn-2");
    orch.process_state
        .set_current_turn_id("run-child", Some("child-turn-2"));
    end_turn_without_return(&orch, "run-child").await;
    assert_eq!(reprompts(&orch, &child).await, 1);
}

/// An issue a task files belongs to the node the task acted for, not to the
/// task (CAIRN-1302: `owning_node_job_for_run` records the spawning node), and
/// its resolution wakes that node, never the task. So an open child issue is no
/// reason to leave the task parked: it is re-prompted, and the natural answer
/// is a `return` that names the issue.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_child_issue_the_task_filed_does_not_hold_off_the_reprompt() {
    let (orch, child, _requests) = spawn_and_park("return-followup-child-issue.db").await;
    orch.db
        .local
        .execute(
            "INSERT INTO issues(id,project_id,number,title,status,progress,attention,created_at,updated_at,parent_issue_id,parent_job_id)
             VALUES('i-filed','p',8,'Filed by the task','backlog','backlog','none',5,5,'i','j-parent')",
            (),
        )
        .await
        .unwrap();
    first_miss(&orch, &child).await;
}

/// A call is one-shot and reaped after its turn; it is never re-prompted.
#[tokio::test(flavor = "multi_thread")]
async fn no_reprompt_for_a_call() {
    let (orch, child, _requests) = spawn_and_park("return-followup-call.db").await;
    orch.db
        .local
        .execute(
            "UPDATE delegated_packets SET spec = json_set(spec, '$.origin', 'call_tool')
             WHERE result_artifact_job_id = ?1",
            params![child.clone()],
        )
        .await
        .unwrap();
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    end_turn_without_return(&orch, "run-child").await;
    assert_eq!(reprompts(&orch, &child).await, 0);
}

/// A top-level node owes its own output, not a `return`, and is never
/// re-prompted here, even resting Blocked on it.
#[tokio::test(flavor = "multi_thread")]
async fn no_reprompt_for_a_top_level_job() {
    let (orch, _child, _requests) = spawn_and_park("return-followup-top-level.db").await;
    orch.db
        .local
        .execute(
            "UPDATE jobs SET status = 'blocked' WHERE id = 'j-parent'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        settle_directly(&orch, "j-parent", "run-parent").await,
        MissingReturn::NotOwed
    );
    assert_eq!(
        count(
            &orch,
            &format!("SELECT COUNT(*) FROM messages WHERE content LIKE '{REPROMPT_PREFIX}%'"),
        )
        .await,
        0
    );
}

/// A clean run exit no longer counts as success, so the inline wait must still
/// release a task that did return. Every clean turn end recomputes the job
/// before it signals the wait; this is the regression that change could cause.
#[tokio::test(flavor = "multi_thread")]
async fn inline_wait_releases_a_task_that_returns_and_exits_cleanly() {
    let (orch, _executor) = parent_fixture("return-followup-inline-returned.db").await;
    let budget = Duration::from_secs(20);
    let started = std::time::Instant::now();
    let spawn_orch = orch.clone();
    let spawning = tokio::spawn(async move {
        spawn(&spawn_orch, &[explore_payload("map the fleet")], budget).await
    });
    let child = created_child(&orch).await;
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    say(&orch, "run-child", "child-turn-1", LAST_WORDS).await;
    write_return(&orch, &child, RETURNED).await;
    // Claude reports the clean Result after the return; the task-spawned path
    // finalizes the run as exited.
    end_claude_task_turn(&orch, "run-child").await;

    let response = spawning
        .await
        .unwrap()
        .response
        .expect("blocking text")
        .result;
    assert!(
        started.elapsed() < budget,
        "the inline wait ran out its budget"
    );
    assert_eq!(response, RETURNED);
    assert_eq!(reprompts(&orch, &child).await, 0);
}

/// The inline wait no longer hands a parent the child's last message when the
/// child ended without `return`: the child is re-prompted and the parent parks
/// on the real result instead.
#[tokio::test(flavor = "multi_thread")]
async fn inline_wait_parks_instead_of_answering_with_a_missing_return() {
    let (orch, _executor) = parent_fixture("return-followup-inline-missing.db").await;
    let _parent = register_parent_process(&orch);
    let spawn_orch = orch.clone();
    let spawning = tokio::spawn(async move {
        spawn(
            &spawn_orch,
            &[explore_payload("map the fleet")],
            Duration::from_secs(4),
        )
        .await
    });
    let child = created_child(&orch).await;
    open_child_turn(&orch, &child, "run-child", "child-turn-1").await;
    say(&orch, "run-child", "child-turn-1", LAST_WORDS).await;
    end_turn_without_return(&orch, "run-child").await;

    let response = spawning
        .await
        .unwrap()
        .response
        .expect("blocking text")
        .result;
    assert!(
        response.ends_with(DELEGATED_TASKS_SUSPENDED_PARENT_SUFFIX),
        "{response}"
    );
    assert!(!response.contains(LAST_WORDS), "{response}");
    assert_eq!(reprompts(&orch, &child).await, 1);
}

/// The child job a spawn under way created, once it exists.
async fn created_child(orch: &Orchestrator) -> String {
    let started = std::time::Instant::now();
    let child = loop {
        if let Some(child) =
            text(orch, "SELECT id FROM jobs WHERE parent_job_id = 'j-parent'").await
        {
            break child;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "no child created"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Let the spawn reach its wait before the child's turn ends.
    tokio::time::sleep(Duration::from_millis(300)).await;
    child
}
