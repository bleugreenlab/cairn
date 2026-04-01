//! Backend-aware stdin helpers.
//!
//! Thin wrappers that resolve the backend from `ActiveProcess` and delegate
//! to the `AgentBackend` trait implementation for message formatting.

use crate::agent_process::process::{AgentProcessState, BackendStdin};

use super::backend_for_name;

/// Resolve the backend and stdin for a run, then call `f` with them.
fn with_backend_stdin<F>(
    process_state: &AgentProcessState,
    run_id: &str,
    f: F,
) -> Result<(), String>
where
    F: FnOnce(&dyn super::AgentBackend, &mut dyn BackendStdin) -> Result<(), String>,
{
    let backend_name = process_state
        .processes
        .lock()
        .ok()
        .and_then(|p| p.get(run_id).and_then(|proc| proc.backend.clone()));

    let backend = backend_for_name(backend_name.as_deref());

    let stdin_handle = process_state
        .get_stdin_handle(run_id)
        .ok_or_else(|| format!("Process not found: {}", run_id))?;
    let mut guard = stdin_handle.lock().map_err(|e| e.to_string())?;
    let writer = guard
        .as_mut()
        .ok_or_else(|| "Stdin not available".to_string())?
        .as_mut();
    f(backend.as_ref(), writer)
}

/// Send a user message to a running agent process.
pub fn send_user_message(
    process_state: &AgentProcessState,
    run_id: &str,
    content: &str,
    session_id: &str,
    parent_tool_use_id: Option<&str>,
    working_dir: Option<&str>,
) -> Result<(), String> {
    with_backend_stdin(process_state, run_id, |backend, stdin| {
        backend.send_user_message(stdin, content, session_id, parent_tool_use_id, working_dir)
    })
}

/// Send an interrupt to a running agent process.
pub fn send_interrupt(process_state: &AgentProcessState, run_id: &str) -> Result<(), String> {
    with_backend_stdin(process_state, run_id, |backend, stdin| {
        backend.send_interrupt(stdin)
    })
}

/// Send a model change to a running agent process.
pub fn send_set_model(
    process_state: &AgentProcessState,
    run_id: &str,
    model: &str,
) -> Result<(), String> {
    with_backend_stdin(process_state, run_id, |backend, stdin| {
        backend.send_set_model(stdin, model)
    })
}

/// Send a permission mode change to a running agent process.
pub fn send_set_permission_mode(
    process_state: &AgentProcessState,
    run_id: &str,
    mode: &str,
) -> Result<(), String> {
    with_backend_stdin(process_state, run_id, |backend, stdin| {
        backend.send_set_permission_mode(stdin, mode)
    })
}
