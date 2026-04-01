//! Checkpoint detection and approval logic
//!
//! This module determines whether a job should block for approval after completion
//! based on checkpoint configuration in recipe nodes.

use crate::jobs::queries::load_node_for_job;
use crate::models::CheckpointType;

/// Check if a job's node has an approval checkpoint slot in its config.
pub fn has_approval_checkpoint_slot(
    conn: &mut diesel::sqlite::SqliteConnection,
    job_id: &str,
) -> bool {
    log::info!("Checking checkpoint slot for job {}", job_id);

    // Load node from execution snapshot
    let (node, _snapshot) = match load_node_for_job(conn, job_id) {
        Ok(Some(data)) => data,
        Ok(None) => {
            log::info!("Job {} has no node in snapshot", job_id);
            return false;
        }
        Err(e) => {
            log::info!("Failed to load node for job {}: {}", job_id, e);
            return false;
        }
    };

    let node_type = node.node_type.to_string();
    log::info!("Job {} has node type={}", job_id, node_type);

    // Only check agent and action nodes
    if node_type != "agent" && node_type != "action" {
        log::info!("Node is not agent/action, skipping");
        return false;
    }

    // Check for approval checkpoint in agent config
    if let Some(ref agent_cfg) = node.agent_config {
        if let Some(ref checkpoint) = agent_cfg.checkpoint {
            if matches!(checkpoint.checkpoint_type, CheckpointType::Approval) {
                log::info!("Job {} has_approval_checkpoint_slot = true (agent)", job_id);
                return true;
            }
        }
    }

    // Check for approval checkpoint in action config
    if let Some(ref action_cfg) = node.action_config {
        if let Some(ref checkpoint) = action_cfg.checkpoint {
            if matches!(checkpoint.checkpoint_type, CheckpointType::Approval) {
                log::info!(
                    "Job {} has_approval_checkpoint_slot = true (action)",
                    job_id
                );
                return true;
            }
        }
    }

    log::info!("Job {} has_approval_checkpoint_slot = false", job_id);
    false
}
