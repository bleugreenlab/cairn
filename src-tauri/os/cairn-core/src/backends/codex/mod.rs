//! Codex CLI backend implementation.
//!
//! Communicates with `codex app-server` over stdio using JSON-RPC.
//! Cairn starts or resumes a Codex thread, starts turns against that thread,
//! and translates app-server notifications into Cairn transcript events.

mod backend_impl;
pub(crate) use backend_impl::discover_catalog_blocking;
mod events;
mod permissions;
pub mod pool;
mod runtime;
mod thread_params;

pub(crate) use events::{
    emit_codex_run_turn_completed, store_usage_result_event, terminal_tool_called_for_run,
};

pub use cairn_models::codex::tokens::{codex_account_identity, CodexAccountIdentity};

pub(super) const CODEX_BACKEND_NAME: &str = "Codex";

/// Refusal shown only when no Codex credential is configured.
pub(crate) const NO_CODEX_CREDENTIAL: &str = "No ChatGPT account is configured. Sign in to a ChatGPT account, or add an OpenAI API key, in Settings → Providers.";

#[cfg(test)]
type ScriptedStdinFixture = (
    Box<dyn crate::agent_process::process::SessionControl>,
    std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    std::sync::Arc<std::sync::Mutex<Option<String>>>,
);

use cairn_models::wire::codex_app_server::goal;

use crate::agent_process::process::{GoalPause, GoalPauseClaim, PauseIntent};
use crate::execution::jobs::{GoalIntent, GoalReactivation, GoalReconciliation, JobRunnable};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GoalPauseCause {
    Paused(GoalPause),
    /// The operator turned the job's goal off.
    OperatorOff,
    /// The provider reports Paused and Cairn holds no typed claim.
    Unattributed,
    Complete,
}

/// What a live Codex thread's goal looks like right now: the provider's status
/// and Cairn's claim on why it is paused.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct GoalObservation {
    pub status: goal::GoalStatus,
    pub claim: Option<GoalPause>,
}

/// Read a live thread's goal and claim without changing either. `None` when the
/// session is not a Codex app-server thread or its thread carries no goal.
pub(crate) fn inspect_app_server_goal(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &GoalPauseClaim,
) -> Result<Option<GoalObservation>, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(None);
    };
    Ok(app_stdin.get_goal()?.map(|goal| GoalObservation {
        status: goal.status,
        claim: claim.inspect(),
    }))
}

/// The operator's goal intent outlives every process: a job whose goal is
/// turned off opens its thread paused, before its first turn can end and let
/// Codex continue on its own.
pub(crate) fn apply_goal_intent_at_launch(
    orch: &crate::orchestrator::Orchestrator,
    run_id: &str,
    native_goal: &cairn_models::interface::session::NativeGoal,
    control: &mut dyn crate::agent_process::process::SessionControl,
) -> Result<(), String> {
    if matches!(
        native_goal,
        cairn_models::interface::session::NativeGoal::Objective(_)
    ) && crate::execution::jobs::goal_intent_for_run(orch, run_id)? == GoalIntent::Off
    {
        pause_goal_before_first_turn(control)?;
    }
    Ok(())
}

/// Pause a freshly opened thread's goal before its first turn starts. The pause
/// needs no claim: the durable intent is what keeps every release path from
/// lifting it.
fn pause_goal_before_first_turn(
    control: &mut dyn crate::agent_process::process::SessionControl,
) -> Result<(), String> {
    let Some(app_stdin) = control
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(());
    };
    match app_stdin.get_goal()? {
        Some(goal)
            if !matches!(
                goal.status,
                goal::GoalStatus::Paused | goal::GoalStatus::Complete
            ) =>
        {
            set_goal_status(app_stdin, goal::GoalStatus::Paused)
        }
        _ => Ok(()),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum GoalReactivationResult {
    Lifted {
        kind: &'static str,
        id: Option<String>,
    },
    StillPaused {
        cause: GoalPauseCause,
    },
    Active,
}

/// The provider owns status; Cairn owns the authorization to lift a pause, and
/// the operator's durable intent decides whether any lift is allowed at all.
/// Callers hold stdin and the process-scoped claim throughout this operation.
///
/// A delivered message never changes the intent, so with the goal turned off
/// this keeps (or makes) the goal paused whatever arrived: the message still
/// runs its own turn, and the goal just does not continue past it.
pub(crate) fn reactivate_app_server_goal(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &mut GoalPauseClaim,
    trigger: Option<&GoalReactivation>,
    runnable: JobRunnable,
    intent: GoalIntent,
) -> Result<GoalReactivationResult, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(GoalReactivationResult::Active);
    };
    let Some(goal) = app_stdin.get_goal()? else {
        return Ok(GoalReactivationResult::Active);
    };
    if goal.status == goal::GoalStatus::Complete {
        return Ok(GoalReactivationResult::StillPaused {
            cause: GoalPauseCause::Complete,
        });
    }
    if intent == GoalIntent::Off {
        if let Some(GoalReactivation::OwnedConditionResolved(owner)) = trigger {
            claim.consume(owner);
        }
        if goal.status != goal::GoalStatus::Paused {
            set_goal_status(app_stdin, goal::GoalStatus::Paused)?;
        }
        return Ok(GoalReactivationResult::StillPaused {
            cause: GoalPauseCause::OperatorOff,
        });
    }
    if goal.status != goal::GoalStatus::Paused {
        return Ok(GoalReactivationResult::Active);
    }
    let activation = || set_goal_status(app_stdin, goal::GoalStatus::Active);
    let lifted = match (claim.inspect(), trigger) {
        (Some(GoalPause::Owned(_)), Some(GoalReactivation::OwnedConditionResolved(owner))) => claim
            .release(owner, activation)?
            .then(|| (owner.kind(), Some(owner.id().to_string()))),
        (None | Some(GoalPause::JobState(_) | GoalPause::Interrupted), _)
            if runnable == JobRunnable::Attested =>
        {
            claim
                .release_unowned(activation)?
                .then_some(("job_runnable", None))
        }
        _ => None,
    };
    if let Some((kind, id)) = lifted {
        Ok(GoalReactivationResult::Lifted { kind, id })
    } else {
        let cause = claim
            .inspect()
            .map_or(GoalPauseCause::Unattributed, GoalPauseCause::Paused);
        tracing::debug!(?cause, "native goal pause release refused");
        Ok(GoalReactivationResult::StillPaused { cause })
    }
}

#[cfg(test)]
pub(crate) fn scripted_owned_goal_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
    replies: Vec<Result<serde_json::Value, String>>,
) -> ScriptedStdinFixture {
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            replies,
            requests.clone(),
        ),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), requests, current_turn_id)
}

#[cfg(test)]
pub(crate) fn scripted_durable_wait_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
) -> ScriptedStdinFixture {
    let goal = |status: &str| {
        serde_json::json!({
            "result": { "goal": {
                "threadId": thread_id,
                "objective": "finish assigned issue",
                "status": status,
                "tokenBudget": null,
                "tokensUsed": 0,
                "timeUsedSeconds": 0,
                "createdAt": 1,
                "updatedAt": 2
            }}
        })
    };
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![
                Ok(goal("active")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(serde_json::Value::Null),
                Ok(goal("paused")),
                Ok(goal("active")),
                Ok(serde_json::json!({
                    "result": { "turn": { "id": "provider-wait-resolved" } }
                })),
            ],
            requests.clone(),
        ),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), requests, current_turn_id)
}

#[cfg(test)]
pub(crate) fn scripted_durable_wait_rollback_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
) -> ScriptedStdinFixture {
    let goal = |status: &str| {
        serde_json::json!({
            "result": { "goal": {
                "threadId": thread_id,
                "objective": "finish assigned issue",
                "status": status,
                "tokenBudget": null,
                "tokensUsed": 0,
                "timeUsedSeconds": 0,
                "createdAt": 1,
                "updatedAt": 2
            }}
        })
    };
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![
                Ok(goal("active")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("active")),
            ],
            requests.clone(),
        ),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), requests, current_turn_id)
}

#[cfg(test)]
pub(crate) fn scripted_delegated_wait_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
) -> ScriptedStdinFixture {
    let goal = |status: &str| {
        serde_json::json!({
            "result": { "goal": {
                "threadId": thread_id,
                "objective": "finish assigned issue",
                "status": status,
                "tokenBudget": null,
                "tokensUsed": 0,
                "timeUsedSeconds": 0,
                "createdAt": 1,
                "updatedAt": 2
            }}
        })
    };
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![
                Ok(goal("active")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(goal("paused")),
                Ok(serde_json::Value::Null),
                Ok(goal("paused")),
                Ok(goal("active")),
                Ok(serde_json::json!({
                    "result": { "turn": { "id": "provider-unblocked" } }
                })),
            ],
            requests.clone(),
        ),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), requests, current_turn_id)
}

pub(crate) fn pause_goal_and_interrupt_provider_turn(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &mut GoalPauseClaim,
    provider_turn_id: &str,
    intent: PauseIntent,
) -> Result<bool, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(false);
    };
    pause_goal(app_stdin, claim, intent)?;
    app_stdin.interrupt_untracked_turn(provider_turn_id)?;
    Ok(true)
}

#[cfg(test)]
pub(crate) fn scripted_user_stop_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
) -> ScriptedStdinFixture {
    let goal = |status: &str| {
        serde_json::json!({
            "result": { "goal": {
                "threadId": thread_id,
                "objective": "finish assigned issue",
                "status": status,
                "tokenBudget": null,
                "tokensUsed": 0,
                "timeUsedSeconds": 0,
                "createdAt": 1,
                "updatedAt": 2
            }}
        })
    };
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![
                Ok(goal("active")),
                Ok(goal("paused")),
                Ok(serde_json::Value::Null),
                Ok(goal("paused")),
                Ok(goal("active")),
                Ok(serde_json::json!({
                    "result": { "turn": { "id": "provider-resumed" } }
                })),
            ],
            requests.clone(),
        ),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), requests, current_turn_id)
}

/// Pause a native goal and interrupt its live turn as one stdin-serialized action.
///
/// Codex evaluates goal continuation when the interrupted turn becomes idle. The
/// goal therefore has to become non-continuing before `turn/interrupt` is sent;
/// issuing the two requests through separate stdin acquisitions leaves room for
/// another admitted message to reactivate the goal between them.
pub(crate) fn pause_goal_and_interrupt_app_server_turn(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &mut GoalPauseClaim,
    intent: PauseIntent,
) -> Result<bool, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(false);
    };
    pause_goal(app_stdin, claim, intent)?;
    app_stdin.interrupt()?;
    Ok(true)
}

pub(crate) fn set_app_server_goal_objective(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    objective: &str,
) -> Result<bool, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(false);
    };
    let current = app_stdin.get_goal()?;
    if current
        .as_ref()
        .is_some_and(|goal| goal.objective == objective)
    {
        return Ok(false);
    }

    let status = current.as_ref().map(|goal| goal.status);
    let updated = app_stdin.set_goal(Some(objective), None, None)?;
    if updated.objective != objective || status.is_some_and(|status| updated.status != status) {
        return Err("Codex goal objective refresh returned unexpected state".to_string());
    }
    Ok(true)
}

#[cfg(test)]
pub(crate) fn scripted_active_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
) -> ScriptedStdinFixture {
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![
                Ok(serde_json::json!({
                    "result": {
                        "goal": {
                            "threadId": thread_id,
                            "objective": "finish work",
                            "status": "active",
                            "tokenBudget": null,
                            "tokensUsed": 0,
                            "timeUsedSeconds": 0,
                            "createdAt": 1,
                            "updatedAt": 2
                        }
                    }
                })),
                Ok(serde_json::Value::Null),
                Ok(serde_json::json!({
                    "result": {
                        "goal": {
                            "threadId": thread_id,
                            "objective": "finish work",
                            "status": "active",
                            "tokenBudget": null,
                            "tokensUsed": 0,
                            "timeUsedSeconds": 0,
                            "createdAt": 1,
                            "updatedAt": 2
                        }
                    }
                })),
                Ok(serde_json::json!({
                    "result": {
                        "goal": {
                            "threadId": thread_id,
                            "objective": "finish work",
                            "status": "active",
                            "tokenBudget": null,
                            "tokensUsed": 0,
                            "timeUsedSeconds": 0,
                            "createdAt": 1,
                            "updatedAt": 2
                        }
                    }
                })),
            ],
            requests.clone(),
        ),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), requests, current_turn_id)
}

#[cfg(test)]
pub(crate) fn failing_interrupt_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
) -> Box<dyn crate::agent_process::process::SessionControl> {
    let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::for_test_scripted(
            vec![Err("fixture interrupt transport failure".to_string())],
            requests,
        ),
    );
    Box::new(cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        "/tmp/project".to_string(),
        None,
        None,
        None,
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string()))),
    ))
}

/// A real app-server process whose provider goal stays `active`, as it does
/// across a `turn/interrupt` that Cairn did not pair with a pause.
///
/// Any request the script has no arm for is answered with a JSON-RPC error
/// naming the method. Staying silent would instead block the caller for the
/// client's full request timeout before the test sees anything.
#[cfg(all(test, unix))]
pub(crate) fn process_active_stdin_for_test(
    thread_id: &str,
    provider_turn_id: &str,
    terminal_notification: Option<&str>,
) -> (
    Box<dyn crate::agent_process::process::SessionControl>,
    std::sync::Arc<std::sync::Mutex<Option<String>>>,
    tempfile::TempDir,
) {
    use std::os::unix::fs::PermissionsExt;

    let fixture = tempfile::tempdir().expect("create app-server fixture directory");
    let executable = fixture.path().join("codex-fixture");
    let notification = terminal_notification.unwrap_or("");
    let script = format!(
        r#"#!/bin/sh
goal='{{"threadId":"{thread_id}","objective":"finish work","status":"active","tokenBudget":null,"tokensUsed":0,"timeUsedSeconds":0,"createdAt":1,"updatedAt":2}}'
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -E 's/.*"id":([0-9]+).*/\1/')
  case "$line" in
    *'"method":"turn/interrupt"'*)
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{}}}}\n' "$id"
      if [ -n '{notification}' ]; then
        printf '{{"jsonrpc":"2.0","method":"turn/{notification}","params":{{"turn":{{"id":"{provider_turn_id}","status":"{notification}"}},"reason":"interrupted"}}}}\n'
      fi
      ;;
    *'"method":"thread/goal/get"'*|*'"method":"thread/goal/set"'*)
      printf '{{"jsonrpc":"2.0","id":%s,"result":{{"goal":%s}}}}\n' "$id" "$goal"
      ;;
    *'"id":'[0-9]*)
      method=$(printf '%s' "$line" | sed -E 's/.*"method":"([^"]*)".*/\1/')
      printf '{{"jsonrpc":"2.0","id":%s,"error":{{"code":-32601,"message":"codex process fixture has no reply for %s"}}}}\n' "$id" "$method"
      ;;
  esac
done
"#
    );
    std::fs::write(&executable, script).expect("write app-server fixture");
    let mut permissions = std::fs::metadata(&executable)
        .expect("stat app-server fixture")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&executable, permissions).expect("chmod app-server fixture");

    let client = std::sync::Arc::new(
        cairn_models::codex::app_server::AppServerClient::spawn(
            &crate::services::RealProcessSpawner,
            executable.to_str().expect("fixture path is utf-8"),
            &[],
            &std::collections::HashMap::new(),
            fixture.path().to_str().expect("fixture cwd is utf-8"),
        )
        .expect("spawn app-server process fixture"),
    );
    let current_turn_id =
        std::sync::Arc::new(std::sync::Mutex::new(Some(provider_turn_id.to_string())));
    let notifications = client.notifications();
    let observed_turn = current_turn_id.clone();
    std::thread::spawn(move || {
        for message in notifications {
            if matches!(
                cairn_models::wire::codex_app_server::Notification::parse(&message),
                cairn_models::wire::codex_app_server::Notification::TurnAborted { .. }
                    | cairn_models::wire::codex_app_server::Notification::TurnCompleted { .. }
            ) {
                let terminal_turn_id = runtime::provider_turn_id(&message, &observed_turn);
                runtime::clear_cached_provider_turn_if_current(
                    terminal_turn_id.as_deref(),
                    &observed_turn,
                );
            }
        }
    });
    let stdin = cairn_models::session::codex::CodexControl::new(
        client,
        thread_id.to_string(),
        fixture.path().to_string_lossy().into_owned(),
        None,
        None,
        None,
        current_turn_id.clone(),
    );
    (Box::new(stdin), current_turn_id, fixture)
}

pub(crate) fn interrupt_exact_app_server_turn(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    provider_turn_id: &str,
) -> Result<cairn_models::session::codex::ExactTurnInterrupt, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Err("Codex app-server stdin unavailable".to_string());
    };
    app_stdin.interrupt_exact_and_wait(provider_turn_id)
}

/// Reconcile the thread's goal with its authoritative job state, if it has a goal.
///
/// An objective-less `thread/goal/set` is an *update*: the app-server rejects
/// it outright when the thread carries no goal. Goal-lessness is a supported
/// state, not a fault -- a standing project thread has its goal cleared on
/// purpose, and an ephemeral call never gets one -- so a thread without a goal
/// is simply nothing to steer. Reporting an error here would take the caller
/// down with it: message delivery, durable-wait suspension, turn-end
/// reconciliation, and watchdog recovery all gate on this result.
pub(crate) fn reconcile_app_server_goal(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &mut GoalPauseClaim,
    reconciliation: GoalReconciliation,
    intent: GoalIntent,
) -> Result<(), String> {
    if reconciliation == GoalReconciliation::Active {
        return reactivate_app_server_goal(stdin, claim, None, JobRunnable::Attested, intent)
            .map(|_| ());
    }
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(());
    };
    match reconciliation {
        GoalReconciliation::Active => unreachable!("active reconciliation handled before downcast"),
        GoalReconciliation::Paused(state) => pause_goal(
            app_stdin,
            claim,
            PauseIntent::Stake(GoalPause::JobState(state)),
        ),
        GoalReconciliation::Blocked => {
            restore_nonterminal_goal_status(app_stdin, goal::GoalStatus::Blocked)
        }
        GoalReconciliation::Complete => set_goal_status(app_stdin, goal::GoalStatus::Complete),
    }
}

/// The provider state changed while Cairn establishes a durable suspension.
///
/// The prior status is intentionally opaque outside this backend. In particular,
/// rollback must restore blocked, limited, or already-paused goals exactly
/// rather than assuming every live call began with an active goal.
pub(crate) struct DurableGoalPause {
    previous_status: Option<goal::GoalStatus>,
    previous_claim: Option<GoalPause>,
    staked_claim: Option<GoalPause>,
}

fn pause_goal(
    app_stdin: &cairn_models::session::codex::CodexControl,
    claim: &mut GoalPauseClaim,
    intent: PauseIntent,
) -> Result<(), String> {
    let Some(goal) = app_stdin.get_goal()? else {
        return Ok(());
    };
    if goal.status == goal::GoalStatus::Complete {
        return Ok(());
    }
    set_goal_status(app_stdin, goal::GoalStatus::Paused)?;
    claim.stake(intent);
    Ok(())
}

pub(crate) fn pause_app_server_goal_for_durable_wait(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &mut GoalPauseClaim,
    intent: PauseIntent,
) -> Result<Option<DurableGoalPause>, String> {
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Ok(None);
    };
    let previous_status = app_stdin.get_goal()?.map(|goal| goal.status);
    // Snapshot absence honestly: rollback restores an unattributed pause, whose
    // recovery requires job admission, rather than inventing a sticky hold.
    let previous_claim = claim.inspect();
    if let Some(status) = previous_status.filter(|status| {
        !matches!(
            status,
            goal::GoalStatus::Paused | goal::GoalStatus::Complete
        )
    }) {
        if let Err(error) = set_goal_status(app_stdin, goal::GoalStatus::Paused) {
            if let Err(rollback_error) = restore_nonterminal_goal_status(app_stdin, status) {
                // An uncertain provider write must not leave a wait claim that
                // nothing will ever resolve; only the job's next admission lifts this.
                claim.stake(PauseIntent::Stake(GoalPause::Interrupted));
                return Err(format!(
                    "{error}; native goal rollback also failed: {rollback_error}"
                ));
            }
            return Err(error);
        }
    }
    if previous_status.is_some_and(|status| status != goal::GoalStatus::Complete) {
        claim.stake(intent);
    }
    Ok(Some(DurableGoalPause {
        previous_status,
        previous_claim,
        staked_claim: claim.inspect(),
    }))
}

pub(crate) fn restore_app_server_goal_after_failed_durable_wait(
    stdin: &mut dyn crate::agent_process::process::SessionControl,
    claim: &mut GoalPauseClaim,
    pause: Option<DurableGoalPause>,
) -> Result<(), String> {
    let Some(pause) = pause else {
        return Ok(());
    };
    // A later Stop or park owns both state and rollback now.
    if claim.inspect() != pause.staked_claim {
        return Ok(());
    }
    let Some(app_stdin) = stdin
        .as_any_mut()
        .downcast_mut::<cairn_models::session::codex::CodexControl>()
    else {
        return Err("Codex app-server stdin unavailable during durable-wait rollback".to_string());
    };
    if let Some(status) = pause.previous_status.filter(|status| {
        !matches!(
            status,
            goal::GoalStatus::Paused | goal::GoalStatus::Complete
        )
    }) {
        if app_stdin
            .get_goal()?
            .is_none_or(|goal| goal.status == goal::GoalStatus::Complete)
        {
            return Ok(());
        }
        set_goal_status(app_stdin, status)?;
    }
    claim.rollback(&pause.staked_claim, pause.previous_claim);
    Ok(())
}

/// Failed writes and bookkeeping must not overwrite completion observed meanwhile.
fn restore_nonterminal_goal_status(
    app_stdin: &cairn_models::session::codex::CodexControl,
    status: goal::GoalStatus,
) -> Result<(), String> {
    if app_stdin
        .get_goal()?
        .is_none_or(|goal| goal.status == goal::GoalStatus::Complete)
    {
        return Ok(());
    }
    set_goal_status(app_stdin, status)
}

fn set_goal_status(
    app_stdin: &cairn_models::session::codex::CodexControl,
    status: goal::GoalStatus,
) -> Result<(), String> {
    match app_stdin.set_goal(None, Some(status), None) {
        Ok(updated) if updated.status == status => Ok(()),
        Ok(updated) => Err(format!(
            "Codex goal status update was not acknowledged: requested {status:?}, received {:?}",
            updated.status
        )),
        // Ask the thread itself rather than matching the rejection text: a
        // goal that is genuinely absent is a no-op, anything else is real.
        Err(error) => match app_stdin.get_goal() {
            Ok(None) => Ok(()),
            _ => Err(error),
        },
    }
}

/// Deliver a warm message the way message delivery does: through the
/// session's control.
#[cfg(test)]
fn send_app_server_user_message(
    control: &mut dyn crate::agent_process::process::SessionControl,
    content: &str,
) -> Result<(), String> {
    control.send(&cairn_models::interface::session::SessionInput {
        content: &crate::agent_process::stdin::MessageContent::text(content),
        session_id: "session-1",
        parent_tool_use_id: None,
    })
}

pub struct CodexBackend;

#[cfg(test)]
mod goal_objective_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn goal(objective: &str, status: &str) -> serde_json::Value {
        serde_json::json!({
            "threadId": "thread-1", "objective": objective, "status": status,
            "tokenBudget": 500, "tokensUsed": 7, "timeUsedSeconds": 8,
            "createdAt": 1, "updatedAt": 2
        })
    }

    #[test]
    fn todo_refresh_preserves_paused_and_blocked_status_and_budget() {
        for status in ["paused", "blocked"] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let client = Arc::new(
                cairn_models::codex::app_server::AppServerClient::for_test_scripted(
                    vec![
                        Ok(serde_json::json!({"result": {"goal": goal("old", status)}})),
                        Ok(serde_json::json!({"result": {"goal": goal("new", status)}})),
                    ],
                    Arc::clone(&requests),
                ),
            );
            let mut stdin = cairn_models::session::codex::CodexControl::new(
                client,
                "thread-1".to_string(),
                "/tmp/project".to_string(),
                None,
                None,
                None,
                Arc::new(Mutex::new(None)),
            );

            assert!(set_app_server_goal_objective(&mut stdin, "new").unwrap());

            let requests = requests.lock().unwrap();
            assert_eq!(requests[1]["params"]["objective"], "new");
            assert!(requests[1]["params"].get("status").is_none());
            assert!(requests[1]["params"].get("tokenBudget").is_none());
        }
    }
}

#[cfg(test)]
mod goal_status_tests {
    use super::*;

    fn scripted_stdin(
        responses: Vec<serde_json::Value>,
    ) -> (
        Box<dyn crate::agent_process::process::SessionControl>,
        std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    ) {
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let client = std::sync::Arc::new(
            cairn_models::codex::app_server::AppServerClient::for_test_scripted(
                responses.into_iter().map(Ok).collect(),
                requests.clone(),
            ),
        );
        let stdin = cairn_models::session::codex::CodexControl::new(
            client,
            "thread-1".to_string(),
            "/tmp/project".to_string(),
            None,
            None,
            None,
            std::sync::Arc::new(std::sync::Mutex::new(None)),
        );
        (Box::new(stdin), requests)
    }

    /// A standing project thread has its goal cleared on purpose. Both queued
    /// user messages and review/attention pushes resume through the same warm
    /// message boundary, so neither may stop after the rejected status steer.
    #[test]
    fn goal_less_thread_delivers_queued_message_and_review_wakes() {
        for (wake, content) in [
            ("queued message", "please also add a regression test"),
            (
                "review attention",
                "A review is ready. Read cairn://p/proj/42/1/builder/create-pr",
            ),
        ] {
            let (mut stdin, requests) = scripted_stdin(vec![
                serde_json::json!({ "result": { "goal": null } }),
                serde_json::json!({ "result": { "turn": { "id": "provider-successor" } } }),
            ]);

            reconcile_app_server_goal(
                stdin.as_mut(),
                &mut GoalPauseClaim::default(),
                GoalReconciliation::Active,
                GoalIntent::On,
            )
            .unwrap_or_else(|error| panic!("{wake} status steer failed: {error}"));
            send_app_server_user_message(stdin.as_mut(), content)
                .unwrap_or_else(|error| panic!("{wake} did not reach turn/start: {error}"));

            let sent = requests.lock().expect("requests lock");
            assert_eq!(sent[0]["method"], "thread/goal/get", "{wake}");
            assert_eq!(sent[1]["method"], "turn/start", "{wake}");
            assert_eq!(sent[1]["params"]["input"][0]["text"], content, "{wake}");
        }
    }

    #[test]
    fn refusal_on_a_thread_that_does_have_a_goal_is_reported() {
        let (mut stdin, _requests) = scripted_stdin(vec![
            serde_json::json!({ "error": { "code": -32600, "message": "goal is budget limited" } }),
            serde_json::json!({ "result": { "goal": {
                "threadId": "thread-1", "objective": "assigned work", "status": "budgetLimited",
                "tokenBudget": 10, "tokensUsed": 10, "timeUsedSeconds": 0,
                "createdAt": 1, "updatedAt": 2
            } } }),
        ]);
        let error = reconcile_app_server_goal(
            stdin.as_mut(),
            &mut GoalPauseClaim::default(),
            GoalReconciliation::Active,
            GoalIntent::On,
        )
        .expect_err("a goal that exists but refuses the write is a real failure");
        assert!(
            error.contains("budget limited"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn passive_delivery_preserves_paused_and_complete_goals() {
        for status in ["paused", "complete"] {
            let (mut stdin, requests) = scripted_stdin(vec![
                serde_json::json!({ "result": { "goal": {
                    "threadId": "thread-1", "objective": "assigned work", "status": status,
                    "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
                    "createdAt": 1, "updatedAt": 2
                } } }),
                serde_json::json!({ "result": { "turn": { "id": "message-turn" } } }),
            ]);

            assert!(matches!(
                reactivate_app_server_goal(
                    stdin.as_mut(),
                    &mut GoalPauseClaim::default(),
                    None,
                    JobRunnable::Unknown,
                    GoalIntent::On,
                )
                .unwrap(),
                GoalReactivationResult::StillPaused { .. }
            ));
            send_app_server_user_message(stdin.as_mut(), "passive direct").unwrap();

            let sent = requests.lock().unwrap();
            assert_eq!(sent[0]["method"], "thread/goal/get");
            assert_eq!(sent[1]["method"], "turn/start");
            assert!(
                sent.iter()
                    .all(|request| request["method"] != "thread/goal/set"),
                "{status} must not be rewritten during delivery"
            );
        }
    }

    #[test]
    fn admission_lifts_an_interrupted_goal_only_while_the_operator_has_it_on() {
        let goal = |status: &str, updated_at: i64| {
            serde_json::json!({ "result": { "goal": {
                "threadId": "thread-1", "objective": "assigned work", "status": status,
                "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
                "createdAt": 1, "updatedAt": updated_at
            } } })
        };
        for (status, intent, responses, expected_methods, reactivated) in [
            (
                "paused",
                GoalIntent::On,
                vec![goal("paused", 2), goal("active", 3)],
                vec!["thread/goal/get", "thread/goal/set"],
                true,
            ),
            (
                "complete",
                GoalIntent::On,
                vec![goal("complete", 2)],
                vec!["thread/goal/get"],
                false,
            ),
            (
                "paused",
                GoalIntent::Off,
                vec![goal("paused", 2)],
                vec!["thread/goal/get"],
                false,
            ),
        ] {
            let (mut stdin, requests) = scripted_stdin(responses);
            let mut claim = GoalPauseClaim::default();
            claim.stake(PauseIntent::Stake(GoalPause::Interrupted));
            let result = reactivate_app_server_goal(
                stdin.as_mut(),
                &mut claim,
                Some(&GoalReactivation::Operator {
                    kind: "explicit_resume",
                    id: None,
                }),
                JobRunnable::Attested,
                intent,
            )
            .unwrap();
            assert_eq!(
                matches!(result, GoalReactivationResult::Lifted { .. }),
                reactivated,
                "{status} {intent:?}: {result:?}"
            );
            if intent == GoalIntent::Off {
                assert_eq!(
                    result,
                    GoalReactivationResult::StillPaused {
                        cause: GoalPauseCause::OperatorOff
                    }
                );
            }
            let methods: Vec<_> = requests
                .lock()
                .unwrap()
                .iter()
                .map(|request| request["method"].as_str().unwrap().to_string())
                .collect();
            assert_eq!(methods, expected_methods, "{status} {intent:?}");
        }
    }

    /// With the goal turned off, a delivery pauses a goal that is somehow still
    /// active, and a resolved wait spends its claim instead of activating, so
    /// turning the goal back on later does not wait on it forever.
    #[test]
    fn operator_off_pauses_an_active_goal_and_spends_a_resolved_wait() {
        let goal = |status: &str| {
            serde_json::json!({ "result": { "goal": {
                "threadId": "thread-1", "objective": "assigned work", "status": status,
                "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
                "createdAt": 1, "updatedAt": 2
            } } })
        };
        let (mut stdin, requests) = scripted_stdin(vec![goal("active"), goal("paused")]);
        let owner = crate::agent_process::process::GoalPauseOwner::DurableWait {
            wait_id: "wait-1".into(),
        };
        let mut claim = GoalPauseClaim::default();
        claim.stake(PauseIntent::Stake(GoalPause::Owned(owner.clone())));

        let result = reactivate_app_server_goal(
            stdin.as_mut(),
            &mut claim,
            Some(&GoalReactivation::OwnedConditionResolved(owner)),
            JobRunnable::Attested,
            GoalIntent::Off,
        )
        .unwrap();

        assert_eq!(
            result,
            GoalReactivationResult::StillPaused {
                cause: GoalPauseCause::OperatorOff
            }
        );
        assert_eq!(claim.inspect(), None, "the resolved wait is spent");
        let sent = requests.lock().unwrap();
        assert_eq!(sent[1]["method"], "thread/goal/set");
        assert_eq!(sent[1]["params"]["status"], "paused");
    }

    #[test]
    fn status_update_requires_the_provider_to_acknowledge_the_requested_status() {
        let active_goal = serde_json::json!({ "result": { "goal": {
            "threadId": "thread-1", "objective": "assigned work", "status": "active",
            "tokenBudget": null, "tokensUsed": 0, "timeUsedSeconds": 0,
            "createdAt": 1, "updatedAt": 2
        } } });
        let (mut stdin, requests) = scripted_stdin(vec![active_goal.clone(), active_goal]);

        let error = reconcile_app_server_goal(
            stdin.as_mut(),
            &mut GoalPauseClaim::default(),
            GoalReconciliation::Paused(crate::agent_process::process::JobPauseState::Blocked),
            GoalIntent::On,
        )
        .expect_err("an unapplied pause must not expose a suspension marker");
        assert!(error.contains("was not acknowledged"), "{error}");
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn premature_goal_turn_is_paused_and_interrupted_by_notification_identity() {
        let paused_goal = serde_json::json!({ "result": { "goal": {
            "threadId": "thread-1", "objective": "assigned work", "status": "paused",
            "tokenBudget": null, "tokensUsed": 7, "timeUsedSeconds": 8,
            "createdAt": 1, "updatedAt": 2
        } } });
        let (mut stdin, requests) = scripted_stdin(vec![
            paused_goal.clone(),
            paused_goal,
            serde_json::Value::Null,
        ]);

        assert!(pause_goal_and_interrupt_provider_turn(
            stdin.as_mut(),
            &mut GoalPauseClaim::default(),
            "provider-premature",
            PauseIntent::Reassert
        )
        .unwrap());

        let sent = requests.lock().unwrap();
        assert_eq!(sent.len(), 3);
        assert_eq!(sent[1]["method"], "thread/goal/set");
        assert_eq!(sent[1]["params"]["status"], "paused");
        assert_eq!(sent[2]["method"], "turn/interrupt");
        assert_eq!(sent[2]["params"]["threadId"], "thread-1");
        assert_eq!(sent[2]["params"]["turnId"], "provider-premature");
        assert!(
            !sent.iter().any(|request| request["method"] == "turn/start"),
            "deferral never creates a replacement provider turn"
        );
    }

    #[test]
    fn user_interrupt_pauses_goal_before_idle_and_admission_reactivates_same_thread() {
        let goal = |status: &str| {
            serde_json::json!({
                "result": { "goal": {
                    "threadId": "thread-1", "objective": "assigned work", "status": status,
                    "tokenBudget": null, "tokensUsed": 7, "timeUsedSeconds": 8,
                    "createdAt": 1, "updatedAt": 2
                }}
            })
        };
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let client = std::sync::Arc::new(
            cairn_models::codex::app_server::AppServerClient::for_test_scripted(
                vec![
                    Ok(goal("active")),
                    Ok(goal("paused")),
                    Ok(serde_json::Value::Null),
                    Ok(goal("paused")),
                    Ok(goal("active")),
                    Ok(serde_json::json!({ "result": { "turn": { "id": "turn-resumed" } } })),
                ],
                requests.clone(),
            ),
        );
        let mut stdin: Box<dyn crate::agent_process::process::SessionControl> =
            Box::new(cairn_models::session::codex::CodexControl::new(
                client,
                "thread-1".to_string(),
                "/tmp/project".to_string(),
                None,
                None,
                None,
                std::sync::Arc::new(std::sync::Mutex::new(Some("turn-live".to_string()))),
            ));

        let mut claim = GoalPauseClaim::default();
        assert!(pause_goal_and_interrupt_app_server_turn(
            stdin.as_mut(),
            &mut claim,
            PauseIntent::Stake(GoalPause::Interrupted)
        )
        .unwrap());
        {
            let sent = requests.lock().unwrap();
            assert_eq!(
                sent.len(),
                3,
                "provider idle must not receive a successor turn/start"
            );
            assert_eq!(sent[1]["method"], "thread/goal/set");
            assert_eq!(sent[1]["params"]["status"], "paused");
            assert!(
                sent[1]["params"].get("objective").is_none(),
                "interrupt preserves the unfinished objective"
            );
            assert_eq!(sent[2]["method"], "turn/interrupt");
            assert_eq!(sent[2]["params"]["threadId"], "thread-1");
            assert_eq!(sent[2]["params"]["turnId"], "turn-live");
        }

        reactivate_app_server_goal(
            stdin.as_mut(),
            &mut claim,
            None,
            JobRunnable::Attested,
            GoalIntent::On,
        )
        .unwrap();
        send_app_server_user_message(stdin.as_mut(), "continue after my interrupt").unwrap();

        let sent = requests.lock().unwrap();
        assert_eq!(sent[3]["method"], "thread/goal/get");
        assert_eq!(sent[4]["method"], "thread/goal/set");
        assert_eq!(sent[4]["params"]["status"], "active");
        assert_eq!(sent[5]["method"], "turn/start");
        assert_eq!(sent[5]["params"]["threadId"], "thread-1");
        assert_eq!(
            sent[5]["params"]["input"][0]["text"],
            "continue after my interrupt"
        );
    }
}

/// Cairn's own `CODEX_HOME`, `~/.cairn/codex`, shared by every Codex process
/// Cairn spawns so the user's `~/.codex` is never read (see
/// `cairn_models::session::codex::home`).
pub(crate) fn cairn_codex_home() -> std::path::PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| std::path::PathBuf::from("."));
    home.join(".cairn").join("codex")
}
