//! Message delivery routing.
//!
//! Channel messages (project/issue) are stored in DB and pulled by the hook
//! handler using a per-process cursor — no push needed.
//!
//! Direct messages are pushed via stdin to auto-resume warm processes.
//! Active processes see direct messages on next hook via the cursor pull.

use crate::models::{ChannelType, Message};
use crate::orchestrator::Orchestrator;

/// Deliver a message after DB insertion.
///
/// For channel messages: no-op (hook pulls new messages via cursor).
/// For direct messages to warm processes: stdin push to auto-resume.
pub fn deliver(orch: &Orchestrator, message: &Message) {
    if message.channel_type == ChannelType::Direct {
        deliver_direct(orch, message);
    }
    // Channel messages: nothing to do — hook pulls from DB via cursor
}

/// Deliver a direct message via stdin if the recipient is a warm process.
/// This auto-resumes the process so it can react immediately.
fn deliver_direct(orch: &Orchestrator, message: &Message) {
    let recipient_run_id = match &message.recipient_run_id {
        Some(id) => id,
        None => return,
    };

    let is_warm = {
        let processes = match orch.process_state.processes.lock() {
            Ok(p) => p,
            Err(_) => return,
        };
        match processes.get(recipient_run_id.as_str()) {
            Some(process) => process.is_warm(),
            None => return,
        }
    };

    if is_warm {
        stdin_push(orch, recipient_run_id, message);
    }
    // Active processes will see the message on next hook via cursor pull
}

/// Send a message via stdin to a warm process to auto-resume it.
fn stdin_push(orch: &Orchestrator, run_id: &str, message: &Message) {
    // Get session_id and stdin handle together
    let session_id = {
        let processes = match orch.process_state.processes.lock() {
            Ok(p) => p,
            Err(_) => return,
        };
        match processes.get(run_id) {
            Some(p) => match &p.session_id {
                Some(sid) => sid.clone(),
                None => return,
            },
            None => return,
        }
    };

    let content = format!("[Message from {}] {}", message.sender_name, message.content);
    match crate::backends::stdin::send_user_message(
        &orch.process_state,
        run_id,
        &content,
        &session_id,
        None,
        None,
    ) {
        Ok(()) => {
            log::info!(
                "Stdin push: direct message to warm process {}",
                &run_id[..run_id.len().min(8)]
            );
            orch.process_state.transition_to_active(run_id);
        }
        Err(e) => {
            log::warn!("Failed to stdin push to {}: {}", run_id, e);
        }
    }
}
