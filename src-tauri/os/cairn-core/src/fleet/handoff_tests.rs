use super::*;
use crate::agent_process::process::{RunHandle, RunOccupancy};
use crate::mcp::handlers::permission::tests::{
    desktop_approval, seed_allow_all_permission, test_orchestrator,
};
use crate::mcp::handlers::permission::{
    resolve_permission_request, test_hooks, AnswerSurface, PermissionAnswer, PermissionDecision,
    PermissionScope,
};
use crate::storage::RowExt;
use cairn_common::callback_contract::{
    INLINE_DURABLE_WAIT_BUDGET, LEGACY_EXECUTOR_CALLBACK_TIMEOUT, RELAYED_FENCE_WAIT_BUDGET,
};
use cairn_common::executor_protocol::SandboxDenial;
use cairn_common::executor_protocol::{CrossingAccess, CrossingRerun};

const RUN: &str = "run-1";
const TURN: &str = "turn-1";
const TOOL_USE: &str = "toolu-run";

/// A live run on an `ask` fence whose provider turn is open, and one bound
/// batch context carrying its `run` call.
async fn fence_orchestrator(with_process: bool) -> Orchestrator {
    let orch = test_orchestrator().await;
    seed_allow_all_permission(&orch).await;
    orch.db
        .local
        .execute_script(format!(
            "DELETE FROM permission_requests;
                 INSERT INTO sessions(id, job_id, status, created_at, updated_at)
                  VALUES ('session-1','job-1','open',1,1);
                 UPDATE runs SET session_id = 'session-1' WHERE id = '{RUN}';
                 INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, updated_at)
                  VALUES ('{TURN}','session-1','{RUN}','job-1',1,'running','initial',1,1);
                 UPDATE jobs SET current_session_id = 'session-1', current_turn_id = '{TURN}';"
        ))
        .await
        .unwrap();
    if with_process {
        orch.process_state
            .register_process(
                RUN.into(),
                RunHandle::test_handle(Some("session-1"), Some("job-1")),
            )
            .unwrap();
        orch.process_state.begin_turn(RUN, TURN);
    }
    orch.fleet.runner_contexts.lock().unwrap().insert(
        "ctx".into(),
        RunnerContextEntry::new(
            "ctx",
            "request-1:attempt-1",
            RunnerCallbackContext {
                request: Some(cairn_common::protocol::CallbackRequest {
                    cwd: "/tmp/worktree".into(),
                    run_id: Some(RUN.into()),
                    tool: "run".into(),
                    payload: serde_json::json!({"commands": [{"command": "cat /etc/hosts"}]}),
                    tool_use_id: Some(TOOL_USE.into()),
                    ..Default::default()
                }),
                run_context: None,
                check_status_board: None,
                run_batch_request_id: None,
                live_checkout: false,
                item_settlements: None,
                executor_binding: Some(RunnerContextExecutorBinding {
                    executor_id: "executor-1".into(),
                    link: BatchLink::Bound(7),
                    request_id: "request-1".into(),
                    attempt_id: "attempt-1".into(),
                }),
                output: output_guard::RelayedOutput::shared(),
            },
        ),
    );
    orch
}

fn parent_answer(decision: PermissionDecision) -> PermissionAnswer {
    let (actor, appearance) = crate::mcp::handlers::run_context::AuthenticatedAgent {
        home: "cairn://p/prj/1/1/parent".to_string(),
        run_id: "run-parent".to_string(),
        job_id: "job-parent".to_string(),
    }
    .appearance(1)
    .unwrap();
    PermissionAnswer::from_surface(decision, AnswerSurface::ResourcePatch)
        .with_authenticated_appearance(actor, appearance)
        .unwrap()
}

fn no_path_rerun() -> CrossingRerun {
    CrossingRerun::Refused {
        reason: cairn_sandbox::NO_PATH_RECOVERED.into(),
    }
}

/// Raise the sandbox denial the executor relays, and wait until its
/// permission row exists.
async fn raise(orch: &Orchestrator) -> tokio::task::JoinHandle<RunnerCallbackResult> {
    raise_denial(orch, "/etc/hosts", CrossingAccess::Write).await
}

async fn raise_denial(
    orch: &Orchestrator,
    path: &str,
    access: CrossingAccess,
) -> tokio::task::JoinHandle<RunnerCallbackResult> {
    let task = {
        let orch = orch.clone();
        let path = path.to_string();
        tokio::spawn(async move {
            orch.fleet
                .handle_runner_callback(
                    &orch,
                    RunnerCallback::SandboxDenied {
                        runner_context_id: "ctx".into(),
                        command: format!("cat {path}"),
                        cwd: "/tmp/worktree".into(),
                        denial: SandboxDenial::Path(path),
                        reported_denial: None,
                        rerun: Some(CrossingRerun::Widened { access }),
                    },
                )
                .await
        })
    };
    for _ in 0..200 {
        if permission_rows(orch).await > 0 {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the sandbox denial never raised a permission");
}

async fn permission_rows(orch: &Orchestrator) -> i64 {
    orch.db
        .local
        .query_one("SELECT COUNT(*) FROM permission_requests", (), |row| {
            row.i64(0)
        })
        .await
        .unwrap()
}

async fn permission(orch: &Orchestrator) -> (String, String, Option<String>) {
    orch.db
        .local
        .query_one(
            "SELECT id, status, response FROM permission_requests",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.opt_text(2)?)),
        )
        .await
        .unwrap()
}

async fn turn_state(orch: &Orchestrator) -> String {
    orch.db
        .local
        .query_one("SELECT state FROM turns WHERE id = ?1", (TURN,), |row| {
            row.text(0)
        })
        .await
        .unwrap()
}

async fn turn_count(orch: &Orchestrator) -> i64 {
    orch.db
        .local
        .query_one("SELECT COUNT(*) FROM turns", (), |row| row.i64(0))
        .await
        .unwrap()
}

/// Close the handoff ten seconds into the inline wait, the way `close`
/// would, and return what the executor's callback was answered with.
async fn close_mid_wait(orch: &Orchestrator, close: impl FnOnce(&Fleet)) -> RunnerCallbackResult {
    let task = raise(orch).await;
    tokio::time::sleep(Duration::from_secs(10)).await;
    close(&orch.fleet);
    task.await.unwrap()
}

fn assert_withdrawn_reply(result: &RunnerCallbackResult) {
    assert!(
        matches!(result, RunnerCallbackResult::Failed { diagnostic } if diagnostic.contains("withdrawn")),
        "{result:?}"
    );
}

async fn assert_withdrawn_with(orch: &Orchestrator, cause: &str) {
    let (_, status, response) = permission(orch).await;
    assert_eq!(status, "withdrawn");
    assert_eq!(
        response.as_deref(),
        Some(format!(r#"{{"withdrawn":{{"cause":"{cause}"}}}}"#).as_str())
    );
    assert!(
        !orch.park_slots.armed(RUN),
        "a withdrawn wait must never arm the park that interrupts the turn"
    );
}

/// Acceptance 1: with no closure and no answer, the inline budget expires,
/// the executor is answered with the suspend marker, the turn is parked, and
/// exactly one permission exists.
#[tokio::test(start_paused = true)]
async fn executor_fence_wait_suspends_with_the_marker_and_raises_one_permission() {
    let orch = fence_orchestrator(true).await;
    let task = raise(&orch).await;
    tokio::time::sleep(RELAYED_FENCE_WAIT_BUDGET + Duration::from_millis(10)).await;
    let result = task.await.unwrap();

    assert_eq!(result, RunnerCallbackResult::Suspended);
    assert_eq!(permission_rows(&orch).await, 1);
    assert_eq!(permission(&orch).await.1, "pending");
    assert!(orch.park_slots.armed(RUN), "the budget path parks the turn");
}

/// The executor's denylist is the executor's: a key on a remote machine is
/// on no list this runner holds, so the operator gate cannot rest on the
/// runner's list. The relayed read carries the sensitive-read kind through
/// the stored prompt, and that is what refuses an agent's own allow.
#[tokio::test(start_paused = true)]
async fn a_relayed_read_of_a_key_only_the_executor_denies_cannot_be_self_approved() {
    let orch = fence_orchestrator(true).await;
    let key = "/Users/remote-executor-account/.ssh/id_ed25519";
    assert!(
        !orch
            .sandbox_deny_read()
            .iter()
            .any(|denied| crate::services::sandbox::paths_overlap(
                std::path::Path::new(key),
                denied
            )),
        "this case is only meaningful for a path off the runner's own denylist"
    );
    let task = raise_denial(&orch, key, CrossingAccess::Read).await;

    let (request_id, _, _) = permission(&orch).await;
    let tool_input = orch
        .db
        .local
        .query_one(
            "SELECT tool_input FROM permission_requests WHERE id = ?1",
            (request_id.clone(),),
            |row| row.text(0),
        )
        .await
        .unwrap();
    let stored: serde_json::Value = serde_json::from_str(&tool_input).unwrap();
    assert_eq!(stored["kind"], "sensitive_host_read");
    assert_eq!(stored["descriptor"], format!("read:{key}"));

    let refusal = resolve_permission_request(
        &orch,
        &request_id,
        parent_answer(PermissionDecision::Allow).with_containment_scope(PermissionScope::Session),
    )
    .await
    .expect_err("an agent may not give itself a read of the denylist");
    assert!(refusal.contains("sensitive-read denylist"), "{refusal}");
    assert_eq!(permission(&orch).await.1, "pending");
    assert!(orch.session_allowed_crossings.lock().unwrap().is_empty());
    task.abort();
}

/// A relayed command-scoped denial for a git index write is answered with
/// its in-sandbox route at once, and no permission is raised for anyone to
/// answer.
#[tokio::test]
async fn a_relayed_git_index_write_is_refused_without_raising_a_permission() {
    let orch = fence_orchestrator(true).await;
    // Native-shell items are relayed as `program` (empty) plus the raw
    // command, hence the leading space. A whole-line launcher arrives as
    // written and is read through to the script it runs.
    for command in [
        " git mv src-tauri/a src-tauri/b && git status --short",
        " sh -c 'git mv a b'",
    ] {
        let result = orch
            .fleet
            .handle_runner_callback(
                &orch,
                RunnerCallback::SandboxDenied {
                    runner_context_id: "ctx".into(),
                    command: command.into(),
                    cwd: "/tmp/worktree".into(),
                    denial: SandboxDenial::Command,
                    reported_denial: None,
                    rerun: Some(no_path_rerun()),
                },
            )
            .await;
        match result {
            RunnerCallbackResult::Rejected { diagnostic } => {
                assert!(diagnostic.contains("plain `mv`"), "{command}: {diagnostic}");
            }
            other => panic!("{command}: expected the instructive refusal, got {other:?}"),
        }
    }
    assert_eq!(permission_rows(&orch).await, 0);
    assert!(!orch.park_slots.armed(RUN));
}

/// The same refusal when the executor recovered the denied path, which on
/// macOS is the git index lock in the cell's shared repository.
#[tokio::test]
async fn a_relayed_git_index_write_with_a_recovered_path_is_refused() {
    let orch = fence_orchestrator(true).await;
    let result = orch
        .fleet
        .handle_runner_callback(
            &orch,
            RunnerCallback::SandboxDenied {
                runner_context_id: "ctx".into(),
                command: " git mv src-tauri/a src-tauri/b && git status --short | head".into(),
                cwd: "/tmp/worktree".into(),
                denial: SandboxDenial::Path(
                    "/Users/dev/.cairn-exec/managed-objects/m/repository.git/worktrees/slot-652/index.lock"
                        .into(),
                ),
                reported_denial: None,
                rerun: Some(CrossingRerun::Widened {
                    access: CrossingAccess::Write,
                }),
            },
        )
        .await;
    match result {
        RunnerCallbackResult::Rejected { diagnostic } => {
            assert!(diagnostic.contains("plain `mv`"), "{diagnostic}");
        }
        other => panic!("expected the instructive refusal, got {other:?}"),
    }
    assert_eq!(permission_rows(&orch).await, 0);
    assert!(!orch.park_slots.armed(RUN));
}

/// A relayed denial with no bounded rerun is answered with why, at once: a
/// command the refusal does not recognize, with no path to approve, and a
/// path denial from an executor too old to rerun it narrowly. No permission
/// is raised for either, and no turn is parked.
#[tokio::test]
async fn a_relayed_denial_with_no_bounded_rerun_is_refused_without_a_permission() {
    let orch = fence_orchestrator(true).await;
    for (denial, rerun, expected, offers) in [
        (
            SandboxDenial::Command,
            Some(no_path_rerun()),
            cairn_sandbox::NO_PATH_RECOVERED.to_string(),
            true,
        ),
        (
            SandboxDenial::Path("/outside/a".into()),
            None,
            "predates bounded reruns".to_string(),
            false,
        ),
    ] {
        let result = orch
            .fleet
            .handle_runner_callback(
                &orch,
                RunnerCallback::SandboxDenied {
                    runner_context_id: "ctx".into(),
                    // Positional arguments make the launcher's script
                    // unreadable, so this stays unrecognized.
                    command: " sh -c 'git mv \"$1\" b' _ a".into(),
                    cwd: "/tmp/worktree".into(),
                    denial,
                    reported_denial: None,
                    rerun,
                },
            )
            .await;
        match result {
            RunnerCallbackResult::Rejected { diagnostic } => {
                assert!(diagnostic.contains(&expected), "{diagnostic}");
                // The bound executor is not connected, so the offer says
                // why the request is unavailable on it.
                assert_eq!(
                    diagnostic.contains("(a `fenced_command` request) is not available here"),
                    offers,
                    "{diagnostic}"
                );
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
    assert_eq!(permission_rows(&orch).await, 0);
    assert!(!orch.park_slots.armed(RUN));
}

/// Acceptance 3: the batch settles before the budget, so the fence wait is
/// withdrawn and no park is armed for the turn the agent is still using.
#[tokio::test(start_paused = true)]
async fn a_fence_wait_whose_batch_settles_is_withdrawn_and_never_parks() {
    let orch = fence_orchestrator(true).await;
    let result = close_mid_wait(&orch, |fleet| {
        fleet.settle_runner_contexts_for_request("request-1", "attempt-1");
    })
    .await;

    assert_withdrawn_reply(&result);
    assert_withdrawn_with(&orch, "batch_settled").await;
    tokio::time::sleep(INLINE_DURABLE_WAIT_BUDGET * 2).await;
    assert!(!orch.park_slots.armed(RUN));
}

fn announced_batch() -> cairn_common::executor_protocol::RelayIdentity {
    cairn_common::executor_protocol::RelayIdentity::Batch {
        runner_context_id: "ctx".into(),
        request_id: "request-1".into(),
        attempt_id: "attempt-1".into(),
    }
}

/// A link close withdraws the fence wait parked then, whose answer has
/// nowhere to go, and leaves the batch's context in place: the batch runs on.
#[tokio::test(start_paused = true)]
async fn link_drop_withdraws_with_executor_link_dropped() {
    let orch = fence_orchestrator(true).await;
    let result = close_mid_wait(&orch, |fleet| {
        fleet.relay_link_closed("executor-1", 7);
    })
    .await;
    assert_withdrawn_reply(&result);
    assert_withdrawn_with(&orch, "executor_link_dropped").await;
    assert!(orch.fleet.holds_runner_context("ctx"));
}

/// The cause a past link close recorded does not stand over the context's
/// next life: after the reattach, a new fence wait on it runs its full
/// budget and suspends as any other would.
#[tokio::test(start_paused = true)]
async fn a_fence_wait_after_the_reattach_is_not_withdrawn_by_the_past_link_drop() {
    let orch = fence_orchestrator(true).await;
    orch.fleet.relay_link_closed("executor-1", 7);
    orch.fleet
        .reattach_relays("executor-1", 8, &[announced_batch()]);
    let task = raise(&orch).await;
    tokio::time::sleep(RELAYED_FENCE_WAIT_BUDGET + Duration::from_millis(10)).await;
    assert_eq!(task.await.unwrap(), RunnerCallbackResult::Suspended);
    assert_eq!(permission(&orch).await.1, "pending");
}

/// A reattach that does not name the batch ends it there, and a fence wait
/// parked on it is withdrawn saying so.
#[tokio::test(start_paused = true)]
async fn an_unannounced_batch_withdraws_with_executor_did_not_reannounce() {
    let orch = fence_orchestrator(true).await;
    let result = close_mid_wait(&orch, |fleet| {
        fleet.reattach_relays("executor-1", 8, &[]);
    })
    .await;
    assert_withdrawn_reply(&result);
    assert_withdrawn_with(&orch, "executor_did_not_reannounce").await;
    assert!(!orch.fleet.holds_runner_context("ctx"));
}

#[tokio::test(start_paused = true)]
async fn batch_cancel_withdraws_with_batch_cancelled() {
    let orch = fence_orchestrator(true).await;
    let result = close_mid_wait(&orch, |fleet| {
        fleet.cancel_runner_contexts_for_request("request-1", "attempt-1");
    })
    .await;
    assert_withdrawn_reply(&result);
    assert_withdrawn_with(&orch, "batch_cancelled").await;
}

/// A removal path that records nothing still releases the waiter, and the
/// cause it is given says only that nothing was recorded.
#[tokio::test(start_paused = true)]
async fn an_unattributed_context_drop_withdraws_with_context_dropped() {
    let orch = fence_orchestrator(true).await;
    let result = close_mid_wait(&orch, |fleet| {
        fleet.runner_contexts.lock().unwrap().remove("ctx");
    })
    .await;
    assert_withdrawn_reply(&result);
    assert_withdrawn_with(&orch, "context_dropped").await;
}

/// The handoff closes in the same instant the budget expires. The budget
/// path re-reads closure before it parks, so this still withdraws.
#[tokio::test(start_paused = true)]
async fn closure_at_the_budget_boundary_withdraws_instead_of_parking() {
    let orch = fence_orchestrator(true).await;
    let task = raise(&orch).await;
    let fleet = orch.fleet.clone();
    tokio::spawn(async move {
        tokio::time::sleep(RELAYED_FENCE_WAIT_BUDGET).await;
        fleet.settle_runner_contexts_for_request("request-1", "attempt-1");
    });
    let result = task.await.unwrap();
    // Whichever of the two the runtime observes first, closure must win
    // whenever it is visible before the park would be armed.
    if result == RunnerCallbackResult::Suspended {
        assert_eq!(permission(&orch).await.1, "pending");
    } else {
        assert_withdrawn_reply(&result);
        assert_withdrawn_with(&orch, "batch_settled").await;
    }
}

/// The provider turn is still open (the agent is waiting on the `run` call
/// whose batch just settled), so the turn is handed straight back to it and
/// no successor is minted.
#[tokio::test(start_paused = true)]
async fn withdrawal_with_an_open_provider_turn_unyields_and_mints_nothing() {
    let orch = fence_orchestrator(true).await;
    let turns_before = turn_count(&orch).await;
    let task = raise(&orch).await;
    assert_eq!(turn_state(&orch).await, "yielded");
    tokio::time::sleep(Duration::from_secs(10)).await;
    orch.fleet
        .settle_runner_contexts_for_request("request-1", "attempt-1");
    assert_withdrawn_reply(&task.await.unwrap());

    assert_eq!(turn_state(&orch).await, "running");
    assert_eq!(turn_count(&orch).await, turns_before);
    assert_eq!(
        orch.process_state.get_occupancy(RUN),
        Some(RunOccupancy::ServingTurn(TURN.into()))
    );
}

/// Another unanswered wait still holds the turn, so withdrawing this one
/// must not un-yield it out from under that wait.
#[tokio::test(start_paused = true)]
async fn withdrawal_with_another_pending_wait_on_the_turn_leaves_it_yielded() {
    let orch = fence_orchestrator(true).await;
    let task = raise(&orch).await;
    orch.db
        .local
        .execute_script(format!(
            "INSERT INTO prompts(id, run_id, questions, created_at, turn_id)
                 VALUES ('prompt-1','{RUN}','[]',2,'{TURN}');"
        ))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(10)).await;
    orch.fleet
        .settle_runner_contexts_for_request("request-1", "attempt-1");
    assert_withdrawn_reply(&task.await.unwrap());

    assert_eq!(turn_state(&orch).await, "yielded");
    assert_eq!(
        orch.process_state.get_occupancy(RUN),
        Some(RunOccupancy::AwaitingHost {
            turn_id: TURN.into()
        })
    );
}

/// A batch past its grace window had its outer durable wait taken over by
/// the permission. Withdrawal hands it back, so the batch's own result can
/// still reach the agent, and that wait keeps the turn yielded.
#[tokio::test(start_paused = true)]
async fn withdrawal_after_a_transferred_outer_wait_restores_it() {
    let orch = fence_orchestrator(true).await;
    orch.db
        .local
        .execute_script(format!(
            "INSERT INTO agent_waits(
                    id, job_id, run_id, session_id, predecessor_turn_id, tool_use_id,
                    condition_json, state, created_at
                 ) VALUES (
                    'outer-wait','job-1','{RUN}','session-1','{TURN}','{TOOL_USE}',
                    '{{\"kind\":\"run_batch\",\"request_id\":\"request-1\",\"commits\":false}}',
                    'pending',1
                 );"
        ))
        .await
        .unwrap();
    let task = raise(&orch).await;
    let transferred = orch
        .db
        .local
        .query_one(
            "SELECT state FROM agent_waits WHERE id = 'outer-wait'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(transferred, "resolved", "the permission took the call over");

    tokio::time::sleep(Duration::from_secs(10)).await;
    orch.fleet
        .settle_runner_contexts_for_request("request-1", "attempt-1");
    assert_withdrawn_reply(&task.await.unwrap());

    let (state, resolution, resolved_at) = orch
        .db
        .local
        .query_one(
            "SELECT state, resolution_json, resolved_at FROM agent_waits WHERE id = 'outer-wait'",
            (),
            |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_i64(2)?)),
        )
        .await
        .unwrap();
    assert_eq!(state, "pending");
    assert!(resolution.is_none() && resolved_at.is_none());
    assert_eq!(turn_state(&orch).await, "yielded");
}

/// Stdin of a warm process, recording every prompt it is sent.
struct RecordingStdin(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for RecordingStdin {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The provider turn ends while the fence wait is inline (the warm process
/// goes idle), so nothing holds the yielded turn when the batch settles.
/// Withdrawal continues it through the single successor owner: exactly one
/// `permission_response` successor, started on the warm process, which is
/// told the permission was withdrawn. A late answerer's notice ("no turn
/// was started") would be false in the turn it is delivered in. Withdrawing
/// the same request again, or answering it late, continues nothing further.
#[tokio::test(start_paused = true)]
async fn withdrawal_after_the_provider_turn_ended_continues_into_one_successor() {
    let orch = fence_orchestrator(false).await;
    let delivered = Arc::new(std::sync::Mutex::new(Vec::new()));
    orch.process_state
        .register_process(
            RUN.into(),
            RunHandle::new(
                Arc::new(std::sync::Mutex::new(None)),
                Arc::new(std::sync::Mutex::new(Some(
                    cairn_models::session::claude::ClaudeControl::boxed(Box::new(RecordingStdin(
                        delivered.clone(),
                    ))),
                ))),
                Some("session-1".into()),
                Some("job-1".into()),
            ),
        )
        .unwrap();
    orch.process_state.begin_turn(RUN, TURN);
    let turns_before = turn_count(&orch).await;

    let task = raise(&orch).await;
    assert_eq!(turn_state(&orch).await, "yielded");
    orch.process_state.transition_to_warm(RUN);
    tokio::time::sleep(Duration::from_secs(10)).await;
    orch.fleet
        .settle_runner_contexts_for_request("request-1", "attempt-1");
    assert_withdrawn_reply(&task.await.unwrap());
    assert_withdrawn_with(&orch, "batch_settled").await;

    let successors = orch
        .db
        .local
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id, state, start_reason, run_id, end_reason FROM turns \
                             WHERE predecessor_id = ?1",
                        (TURN,),
                    )
                    .await?;
                let mut successors = Vec::new();
                while let Some(row) = rows.next().await? {
                    successors.push((
                        row.text(0)?,
                        row.text(1)?,
                        row.opt_text(2)?,
                        row.opt_text(3)?,
                        row.opt_text(4)?,
                    ));
                }
                Ok(successors)
            })
        })
        .await
        .unwrap();
    let [(successor, state, start_reason, run_id, end_reason)] = successors.as_slice() else {
        panic!("exactly one successor of the withdrawn turn: {successors:?}");
    };
    assert_eq!(start_reason.as_deref(), Some("permission_response"));
    assert_eq!(
        (state.as_str(), run_id.as_deref(), end_reason.as_deref()),
        ("running", Some(RUN), None),
        "the successor is started on the warm run, not settled as refused"
    );
    assert_eq!(
        turn_count(&orch).await,
        turns_before + 1,
        "nothing else is minted"
    );
    assert_eq!(
        orch.process_state.get_current_turn_id(RUN).as_deref(),
        Some(successor.as_str()),
        "the warm process serves the successor"
    );
    let resume_error = orch
        .db
        .local
        .query_one("SELECT resume_error FROM permission_requests", (), |row| {
            row.opt_text(0)
        })
        .await
        .unwrap();
    assert_eq!(resume_error, None, "the continuation was not refused");

    let prompt = String::from_utf8_lossy(&delivered.lock().unwrap()).into_owned();
    assert!(
        prompt.contains("The permission you were waiting on was withdrawn before it was answered: the batch finished while this was still waiting"),
        "the agent is told why its permission was withdrawn: {prompt}"
    );
    assert!(
        !prompt.contains("no turn was started"),
        "the agent is not sent the late answerer's notice: {prompt}"
    );
    let waits = orch
        .db
        .local
        .query_one(
            "SELECT COUNT(*) FROM agent_waits \
                 WHERE predecessor_turn_id = ?1 AND state IN ('pending', 'resolving')",
            (TURN,),
            |row| row.i64(0),
        )
        .await
        .unwrap();
    assert_eq!(
        waits, 0,
        "no durable wait is left holding the withdrawn turn"
    );

    // The continuation happens once. Withdrawing the same request again,
    // or answering it late, continues nothing further.
    let (request_id, _, _) = permission(&orch).await;
    let turns_after = turn_count(&orch).await;
    let delivered_after = delivered.lock().unwrap().len();
    let again = crate::mcp::handlers::permission::withdraw_permission_request(
        &orch,
        RUN,
        &request_id,
        TOOL_USE,
        Some(TURN),
        crate::mcp::handlers::permission::WithdrawalCause::BatchSettled,
        crate::mcp::handlers::permission::HandBackPolicy::Resume,
    )
    .await
    .unwrap();
    assert_eq!(
        again,
        crate::mcp::handlers::permission::Withdrawal::NotPending
    );
    let late = resolve_permission_request(
        &orch,
        &request_id,
        PermissionAnswer::from_surface(PermissionDecision::Deny, AnswerSurface::ResourcePatch)
            .with_actor("cairn://p/prj/1/1/agent"),
    )
    .await
    .expect("a late answer is told, not refused");
    assert!(late.withdrawn.is_some() && !late.duplicate, "{late:?}");

    assert_eq!(turn_count(&orch).await, turns_after, "no second successor");
    assert_eq!(
        orch.process_state.get_current_turn_id(RUN).as_deref(),
        Some(successor.as_str()),
        "the warm process still serves the one successor"
    );
    let successor_state = orch
        .db
        .local
        .query_one(
            "SELECT state FROM turns WHERE id = ?1",
            (successor.clone(),),
            |row| row.text(0),
        )
        .await
        .unwrap();
    assert_eq!(successor_state, "running");
    assert_eq!(
        delivered.lock().unwrap().len(),
        delivered_after,
        "nothing further is prompted"
    );
}

/// An older executor gives up on every callback at a flat 30 s, while the
/// batch context the runner watches stays open as long as a sibling item
/// runs (here: nothing settles the context at all). The runner must still
/// answer the fence item's callback with the suspend marker before that
/// executor stops listening, or the park would land on a turn whose call
/// was told "runner callback timed out".
#[tokio::test(start_paused = true)]
async fn a_relayed_fence_wait_answers_before_a_legacy_executor_gives_up() {
    let orch = fence_orchestrator(true).await;
    let raised = tokio::time::Instant::now();
    let task = raise(&orch).await;
    let legacy_deadline = LEGACY_EXECUTOR_CALLBACK_TIMEOUT - Duration::from_secs(1);
    let result = tokio::time::timeout(legacy_deadline, task)
        .await
        .expect("the runner answered after a legacy executor would have stopped listening")
        .unwrap();

    assert_eq!(result, RunnerCallbackResult::Suspended);
    assert!(raised.elapsed() < LEGACY_EXECUTOR_CALLBACK_TIMEOUT);
    assert!(
        orch.fleet
            .runner_contexts
            .lock()
            .unwrap()
            .contains_key("ctx"),
        "the sibling item's batch context is still open"
    );
}

/// A stale card's "Allow all" on a withdrawn request must not turn the
/// agent's containment off before being told its answer was not taken.
#[tokio::test(start_paused = true)]
async fn allow_all_on_a_withdrawn_permission_leaves_the_fence_on() {
    let orch = fence_orchestrator(true).await;
    close_mid_wait(&orch, |fleet| {
        fleet.settle_runner_contexts_for_request("request-1", "attempt-1");
    })
    .await;
    let (request_id, _, _) = permission(&orch).await;

    let outcome = resolve_permission_request(
        &orch,
        &request_id,
        PermissionAnswer::allow_all(desktop_approval()),
    )
    .await
    .expect("a late answer is told, not refused");
    let notice = outcome.withdrawn.expect("withdrawn");
    assert!(
        notice.contains("withdrawn before it was answered"),
        "{notice}"
    );
    assert!(notice.contains("fence is unchanged"), "{notice}");
    assert_eq!(agent_fence(&orch).await, "ask");
}

async fn agent_fence(orch: &Orchestrator) -> String {
    let snapshot = orch
        .db
        .local
        .query_one(
            "SELECT snapshot FROM executions WHERE id = 'execution-1'",
            (),
            |row| row.text(0),
        )
        .await
        .unwrap();
    let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
    snapshot["agents"]["agent-1"]["fence"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

/// "Allow all" and withdrawal race for one request. The fence write rides
/// in the transaction that records the answer, so whichever commits first
/// wins outright: an accepted allow with the fence off, or a withdrawn
/// request with the fence untouched. Never a withdrawn request with the
/// sandbox off. There is no point between the fence write and the record
/// for a withdrawal to land in, because they are one transaction.
#[tokio::test(start_paused = true)]
async fn allow_all_and_withdrawal_never_leave_a_withdrawn_request_with_its_fence_off() {
    // Allow all commits first: the answer is accepted, the inline waiter is
    // answered, and the later closure of the batch finds nothing to withdraw.
    let orch = fence_orchestrator(true).await;
    let task = raise(&orch).await;
    let (request_id, _, _) = permission(&orch).await;
    let outcome = resolve_permission_request(
        &orch,
        &request_id,
        PermissionAnswer::allow_all(desktop_approval()),
    )
    .await
    .unwrap();
    assert!(outcome.withdrawn.is_none());
    orch.fleet
        .settle_runner_contexts_for_request("request-1", "attempt-1");
    assert_eq!(task.await.unwrap(), RunnerCallbackResult::Allowed);
    assert_eq!(permission(&orch).await.1, "allowed");
    assert_eq!(agent_fence(&orch).await, "allow");

    // Withdrawal commits first, after the answer's first read saw the
    // request pending: the record transaction refuses, and the fence write
    // inside it never happens.
    let orch = fence_orchestrator(true).await;
    close_mid_wait(&orch, |fleet| {
        fleet.settle_runner_contexts_for_request("request-1", "attempt-1");
    })
    .await;
    let (request_id, _, _) = permission(&orch).await;
    test_hooks::miss_early_withdrawn_read(true);
    let outcome = resolve_permission_request(
        &orch,
        &request_id,
        PermissionAnswer::allow_all(desktop_approval()),
    )
    .await;
    test_hooks::miss_early_withdrawn_read(false);
    let notice = outcome.unwrap().withdrawn.expect("withdrawn");
    assert!(notice.contains("fence is unchanged"), "{notice}");
    assert_eq!(permission(&orch).await.1, "withdrawn");
    assert_eq!(agent_fence(&orch).await, "ask");
}

/// The withdrawal commits after the resolver's first read but before it
/// records the answer. The record transaction catches it, and a session
/// allow that lost the race grants no reuse and reports no answer.
#[tokio::test(start_paused = true)]
async fn an_answer_racing_withdrawal_is_caught_at_the_record_and_grants_nothing() {
    let orch = fence_orchestrator(true).await;
    close_mid_wait(&orch, |fleet| {
        fleet.settle_runner_contexts_for_request("request-1", "attempt-1");
    })
    .await;
    let (request_id, _, _) = permission(&orch).await;
    let turns_before = turn_count(&orch).await;

    test_hooks::miss_early_withdrawn_read(true);
    let outcome = resolve_permission_request(
        &orch,
        &request_id,
        parent_answer(PermissionDecision::Allow).with_containment_scope(PermissionScope::Session),
    )
    .await;
    test_hooks::miss_early_withdrawn_read(false);
    let outcome = outcome.expect("a late answer is told, not refused");

    assert!(
        outcome
            .withdrawn
            .as_deref()
            .is_some_and(|notice| notice.starts_with("This permission was withdrawn")),
        "{:?}",
        outcome.withdrawn
    );
    assert!(
        orch.session_allowed_crossings.lock().unwrap().is_empty(),
        "a session allow that lost to withdrawal must not grant reuse"
    );
    let surface = orch
        .db
        .local
        .query_one(
            "SELECT resolution_surface FROM permission_requests WHERE id = ?1",
            (request_id.clone(),),
            |row| row.opt_text(0),
        )
        .await
        .unwrap();
    assert!(surface.is_none(), "no resolution was recorded: {surface:?}");
    assert_withdrawn_with(&orch, "batch_settled").await;
    assert_eq!(turn_count(&orch).await, turns_before);
}

/// An answer that arrives after withdrawal is told exactly what happened,
/// and records, replays, and starts nothing.
#[tokio::test(start_paused = true)]
async fn a_late_answer_to_a_withdrawn_permission_replays_nothing_and_mints_nothing() {
    let orch = fence_orchestrator(true).await;
    close_mid_wait(&orch, |fleet| {
        fleet.settle_runner_contexts_for_request("request-1", "attempt-1");
    })
    .await;
    let (request_id, _, _) = permission(&orch).await;
    let turns_before = turn_count(&orch).await;

    let outcome = resolve_permission_request(
        &orch,
        &request_id,
        PermissionAnswer::from_surface(PermissionDecision::Deny, AnswerSurface::ResourcePatch)
            .with_actor("cairn://p/prj/1/1/agent"),
    )
    .await
    .expect("a late answer is told, not refused");

    assert_eq!(
        outcome.withdrawn.as_deref(),
        Some(
            "This permission was withdrawn before it was answered: the batch finished while \
                 this was still waiting, which means the executor stopped waiting before the \
                 inline budget. Your answer was not recorded, nothing was replayed, and no \
                 turn was started."
        )
    );
    assert!(!outcome.duplicate);
    assert_withdrawn_with(&orch, "batch_settled").await;
    assert_eq!(turn_count(&orch).await, turns_before);
    let claims = orch
        .db
        .local
        .query_one("SELECT COUNT(*) FROM channel_ask_resolution", (), |row| {
            row.i64(0)
        })
        .await
        .unwrap();
    assert_eq!(claims, 0, "no answer was claimed for a withdrawn request");
}
