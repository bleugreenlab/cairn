//! Session lifecycle management
//!
//! This module handles cleanup, finalization, and stopping of Claude sessions,
//! including cascading stops to child runs.
//!
//! ## Warm Process Retention
//!
//! When a turn completes successfully, processes can be transitioned to "warm" state
//! instead of being terminated. Warm processes:
//! - Keep their stdin open for follow-up messages
//! - Retain their MCP authentication token
//! - Preserve Claude's conversation cache
//! - Can be reused for continuation without spawning a new process

use crate::claude::checkpoints::has_approval_checkpoint_slot;
use crate::models::RunStatus;
use crate::schema::{action_runs, issues, jobs, pr_data, runs};
use diesel::prelude::*;

use super::Orchestrator;

/// Transition a run to warm state after successful completion.
///
/// Unlike `finalize_run`, this:
/// - Does NOT revoke the MCP authentication token (process may need it for follow-up)
/// - Does NOT clean up the system prompt temp file
/// - DOES mark the run as "completed" so frontend knows turn is done
/// - Transitions the process to warm state for potential reuse
///
/// Returns true if the process was successfully transitioned to warm.
pub fn transition_to_warm_state(orch: &Orchestrator, run_id: &str) -> bool {
    if orch.process_state.transition_to_warm(run_id) {
        // Update run status to completed so frontend knows turn is done
        if let Ok(mut conn) = orch.db.conn.lock() {
            let now = chrono::Utc::now().timestamp() as i32;
            let _ = diesel::update(runs::table.find(run_id))
                .set((
                    runs::status.eq("completed"),
                    runs::completed_at.eq(Some(now)),
                    runs::updated_at.eq(now),
                ))
                .execute(&mut *conn);
        }

        // Emit db-change so frontend updates
        let _ = orch.services.emitter.emit(
            "db-change",
            serde_json::json!({"table": "runs", "action": "update"}),
        );

        log::info!(
            "Run {} transitioned to warm state (process retained for potential follow-up)",
            &run_id[..run_id.len().min(8)]
        );

        // If there's an open PR for this job's issue, restore issue to "waiting"/"pr_review".
        // This handles the case where a builder was resumed (e.g. for conflict resolution)
        // while a PR was already open. Once the builder finishes its turn, the issue should
        // go back to waiting for PR review.
        restore_waiting_if_open_pr(orch, run_id);

        true
    } else {
        log::warn!(
            "Failed to transition run {} to warm state (process not found)",
            &run_id[..run_id.len().min(8)]
        );
        false
    }
}

/// Finalize a run with the given status
pub fn finalize_run(orch: &Orchestrator, run_id: &str, status: RunStatus) {
    // Clean up system prompt temp file
    super::session::cleanup_prompt_file(run_id);

    let now = chrono::Utc::now().timestamp() as i32;

    // Atomically check current status + get job_id + update run status
    let job_id: Option<String> = if let Ok(mut conn) = orch.db.conn.lock() {
        // Check current status - don't overwrite terminal states
        let current_status: Option<String> = runs::table
            .find(run_id)
            .select(runs::status)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if matches!(
            current_status.as_deref(),
            Some("completed") | Some("failed")
        ) {
            log::info!(
                "Run {} already finalized as {:?}, skipping re-finalization as {:?}",
                &run_id[..run_id.len().min(8)],
                current_status,
                status
            );
            // Still emit run-completed so task handlers waiting on this event are unblocked.
            let _ = orch
                .services
                .emitter
                .emit("run-completed", serde_json::json!(run_id));
            let _ = orch.run_completions.send(run_id.to_string());
            return;
        }

        // Update run status
        let _ = diesel::update(runs::table.find(run_id))
            .set((
                runs::status.eq(status.to_string()),
                runs::completed_at.eq(Some(now)),
                runs::updated_at.eq(now),
            ))
            .execute(&mut *conn);

        // Get job_id
        runs::table
            .inner_join(jobs::table.on(runs::job_id.assume_not_null().eq(jobs::id)))
            .filter(runs::id.eq(run_id))
            .filter(runs::job_id.is_not_null())
            .select(runs::job_id.assume_not_null())
            .first::<String>(&mut *conn)
            .ok()
    } else {
        None
    };

    // After updating run status, also finalize todos
    if let Ok(mut conn) = orch.db.conn.lock() {
        let current_todos: Option<String> = runs::table
            .find(run_id)
            .select(runs::todos)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if let Some(todos_json) = current_todos {
            if let Ok(mut todos) = serde_json::from_str::<Vec<serde_json::Value>>(&todos_json) {
                let mut changed = false;
                for todo in &mut todos {
                    if let Some(status) = todo.get_mut("status") {
                        if status.as_str() == Some("in_progress") {
                            *status = serde_json::Value::String("completed".to_string());
                            changed = true;
                        }
                    }
                }
                if changed {
                    if let Ok(new_json) = serde_json::to_string(&todos) {
                        let _ = diesel::update(runs::table.find(run_id))
                            .set(runs::todos.eq(Some(new_json)))
                            .execute(&mut *conn);
                    }
                }
            }
        }
    }

    // Update job status for ALL node types
    if let Some(job_id) = job_id {
        let mut job_status = if status == RunStatus::Completed {
            "complete"
        } else {
            "failed"
        };

        // If completing, check if this job has an approval checkpoint slot
        if job_status == "complete" {
            if let Ok(mut conn) = orch.db.conn.lock() {
                if has_approval_checkpoint_slot(&mut conn, &job_id) {
                    log::info!("Job {} has checkpoint slot, blocking for approval", job_id);
                    job_status = "blocked";
                }
            }
        }

        if let Ok(mut conn) = orch.db.conn.lock() {
            let _ = diesel::update(jobs::table.find(&job_id))
                .set((jobs::status.eq(job_status), jobs::updated_at.eq(now)))
                .execute(&mut *conn);
        }

        // Emit system message for lifecycle event
        let event = match job_status {
            "complete" => Some(crate::messages::system::JobEvent::Completed),
            "failed" => Some(crate::messages::system::JobEvent::Failed),
            "blocked" => Some(crate::messages::system::JobEvent::Blocked),
            _ => None,
        };
        if let Some(event) = event {
            crate::messages::system::emit_job_event(orch, &job_id, Some(run_id), event);
        }

        // Trigger DAG advancement if job completed successfully
        if job_status == "complete" {
            if let Ok(mut conn) = orch.db.conn.lock() {
                let execution_id: Option<String> = jobs::table
                    .find(&job_id)
                    .select(jobs::execution_id)
                    .first::<Option<String>>(&mut *conn)
                    .ok()
                    .flatten();

                if let Some(exec_id) = execution_id {
                    // Notify via event — the host (Tauri/cairn-server) handles
                    // actual DAG advancement since it may need framework-specific async.
                    let _ = orch
                        .services
                        .emitter
                        .emit("dag-advance", serde_json::json!({"execution_id": exec_id}));
                }
            }
        }
    }

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "jobs", "action": "update"}),
    );

    // Emit run completed event (frontend)
    let _ = orch
        .services
        .emitter
        .emit("run-completed", serde_json::json!(run_id));

    // Signal run_completions broadcast (unblocks handle_task waiters)
    let _ = orch.run_completions.send(run_id.to_string());
}

/// Find all descendant job IDs for a given job (children, grandchildren, etc.)
fn find_descendant_job_ids(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> Vec<String> {
    let mut all_descendants = Vec::new();
    let mut current_parents = vec![job_id.to_string()];

    while !current_parents.is_empty() {
        let children: Vec<String> = jobs::table
            .filter(jobs::parent_job_id.eq_any(&current_parents))
            .select(jobs::id)
            .load(conn)
            .unwrap_or_default();

        if children.is_empty() {
            break;
        }

        all_descendants.extend(children.clone());
        current_parents = children;
    }

    all_descendants
}

/// Get all running run IDs for a set of job IDs
fn get_running_runs_for_jobs(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_ids: &[String],
) -> Vec<String> {
    runs::table
        .filter(runs::job_id.eq_any(job_ids))
        .filter(runs::status.eq("running"))
        .select(runs::id)
        .load(conn)
        .unwrap_or_default()
}

/// Stop a running Claude session, cascading to child runs
pub fn stop_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // First, collect child runs to stop
    let child_run_ids: Vec<String> = if let Ok(mut conn) = orch.db.conn.lock() {
        let job_id: Option<String> = runs::table
            .filter(runs::id.eq(run_id))
            .select(runs::job_id)
            .first::<Option<String>>(&mut *conn)
            .ok()
            .flatten();

        if let Some(job_id) = job_id {
            let descendant_job_ids = find_descendant_job_ids(&mut conn, &job_id);
            if !descendant_job_ids.is_empty() {
                get_running_runs_for_jobs(&mut conn, &descendant_job_ids)
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    // Stop child runs first
    for child_run_id in &child_run_ids {
        log::info!(
            "Stopping child run {} (parent run {} stopped)",
            child_run_id,
            run_id
        );
        let _ = stop_session_internal(orch, child_run_id);
    }

    // Stop the requested run
    stop_session_internal(orch, run_id)
}

/// Internal stop without cascading (used by cascading stop)
///
/// Sends an interrupt control request via stdin and transitions the process
/// to warm state. The process is NOT killed - it stays available for follow-up
/// messages.
fn stop_session_internal(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // Send interrupt via stdin control request
    if let Some(stdin_handle) = orch.process_state.get_stdin_handle(run_id) {
        if let Ok(mut stdin_guard) = stdin_handle.lock() {
            if let Some(ref mut stdin) = *stdin_guard {
                let request_id = uuid::Uuid::new_v4().to_string();
                match crate::claude::stdin::send_interrupt_request(stdin, &request_id) {
                    Ok(()) => {
                        log::info!(
                            "Sent interrupt request to run {} (request_id={})",
                            &run_id[..run_id.len().min(8)],
                            &request_id[..8]
                        );
                    }
                    Err(e) => {
                        log::warn!("Failed to send interrupt to run {}: {}", run_id, e);
                    }
                }
            }
        }
    }

    // Transition to warm state instead of killing
    if orch.process_state.transition_to_warm(run_id) {
        log::info!(
            "Run {} interrupted and transitioned to warm state",
            &run_id[..run_id.len().min(8)]
        );
    } else {
        log::warn!(
            "Run {} not found in process map after interrupt",
            &run_id[..run_id.len().min(8)]
        );
    }

    Ok(())
}

/// Forcefully kill a Claude session and finalize as failed.
///
/// Use this when truly terminating a session (e.g., closing an issue,
/// GC eviction, or cleanup). Unlike `stop_session`, this actually kills
/// the process and cannot be resumed.
///
/// Note: Warm processes already have their status set to "completed" when they
/// transitioned via `transition_to_warm_state`. Killing them is just OS process
/// cleanup - we don't change their status.
pub fn kill_session(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    // Only send interrupt to active processes
    let is_warm = orch
        .process_state
        .get_process_state(run_id)
        .is_some_and(|s| s == crate::claude::process::ProcessState::Warm);

    if !is_warm {
        if let Some(stdin_handle) = orch.process_state.get_stdin_handle(run_id) {
            if let Ok(mut stdin_guard) = stdin_handle.lock() {
                if let Some(ref mut stdin) = *stdin_guard {
                    let request_id = uuid::Uuid::new_v4().to_string();
                    let _ = crate::claude::stdin::send_interrupt_request(stdin, &request_id);
                }
            }
        }

        // Brief wait for graceful handling
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Kill the process
    let mut processes = orch
        .process_state
        .processes
        .lock()
        .map_err(|e| e.to_string())?;

    let was_warm = processes.get(run_id).map(|p| p.is_warm()).unwrap_or(false);

    if let Some(active_process) = processes.remove(run_id) {
        if let Ok(mut child_guard) = active_process.child.lock() {
            if let Some(mut child) = child_guard.take() {
                crate::claude::process::graceful_stop(&mut *child);
                log::info!("Killed process for run {}", &run_id[..run_id.len().min(8)]);
            }
        }
    }

    // Only finalize active processes as failed
    // Warm processes already have status "completed"
    if !was_warm {
        // Drop the lock before calling finalize_run (which also locks)
        drop(processes);
        finalize_run(orch, run_id, RunStatus::Failed);
    }

    Ok(())
}

/// If the run's job has an open PR, restore the issue to "waiting"/"pr_review".
///
/// This is general-purpose: it correctly handles any builder resume while a PR
/// is open (conflict resolution, CI fix, review feedback, etc.).
fn restore_waiting_if_open_pr(orch: &Orchestrator, run_id: &str) {
    let Ok(mut conn) = orch.db.conn.lock() else {
        return;
    };

    // Get issue_id and execution_id from run → job
    let issue_data: Option<(String, String)> = runs::table
        .inner_join(jobs::table.on(runs::job_id.assume_not_null().eq(jobs::id)))
        .filter(runs::id.eq(run_id))
        .select((
            jobs::issue_id.assume_not_null(),
            jobs::execution_id.assume_not_null(),
        ))
        .first(&mut *conn)
        .ok();

    let Some((issue_id, execution_id)) = issue_data else {
        return;
    };

    // Check for an open PR via action_runs in this execution
    let has_open_pr: bool = pr_data::table
        .inner_join(action_runs::table.on(pr_data::action_run_id.eq(action_runs::id.nullable())))
        .filter(action_runs::execution_id.eq(&execution_id))
        .filter(pr_data::pr_status.eq("open"))
        .count()
        .first::<i64>(&mut *conn)
        .unwrap_or(0)
        > 0;

    if !has_open_pr {
        return;
    }

    // Only restore if issue is currently "active" (was set active by continue_job_impl)
    let current_status: Option<String> = issues::table
        .find(&issue_id)
        .select(issues::status)
        .first(&mut *conn)
        .ok();

    if current_status.as_deref() != Some("active") {
        return;
    }

    let now = chrono::Utc::now().timestamp() as i32;
    let _ = diesel::update(issues::table.find(&issue_id))
        .set((
            issues::status.eq("waiting"),
            issues::wait_state.eq(Some("pr_review")),
            issues::updated_at.eq(now),
        ))
        .execute(&mut *conn);

    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "issues", "action": "update"}),
    );

    log::info!(
        "Restored issue {} to waiting/pr_review after builder turn completed",
        issue_id
    );
}
