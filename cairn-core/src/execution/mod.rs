//! Execution engine — DAG advancement, job lifecycle, recipe execution.
//!
//! This module contains all business logic for executing recipes:
//! - DAG node/edge management and job creation
//! - Execution advancement (determining which jobs are ready)
//! - Condition evaluation for conditional edges
//! - Checkpoint approval/rejection
//! - Job lifecycle (start, continue, complete)
//! - Recipe execution startup
//!
//! All functions take `&Orchestrator` instead of framework-specific handles.
//! Host layers (Tauri, cairn-server) provide thin wrappers.

pub mod accumulator;
pub mod actions;
pub mod advancement;
pub mod cache;
pub mod checkpoint_runs;
pub mod checkpoints;
pub mod conditions;
pub mod creation;
pub mod dag;
pub mod delegation;
pub mod dispatch;
pub mod jobs;
pub mod queries;
pub mod recipe;
pub mod routing;
pub mod snapshot_edit;
pub mod step_behavior;
pub mod teardown;
pub mod triggers;

use serde::{Deserialize, Serialize};

/// Resolver key stored on the execution record. Captures enough information
/// to re-resolve credentials for auto-started DAG jobs and cold resumes.
///
/// Not identity persistence — just a key that maps to cached credentials
/// (BYOT) or a vault lookup (shared mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Initiator {
    /// JWT `sub` claim — the user who initiated this execution.
    pub sub: String,
    /// Credential mode: "byot" or "shared".
    pub auth_mode: String,
    /// Organization ID (empty string for personal accounts).
    pub org_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initiator_serde_roundtrip() {
        let initiator = Initiator {
            sub: "user-123".to_string(),
            auth_mode: "byot".to_string(),
            org_id: "org-456".to_string(),
        };
        let json = serde_json::to_string(&initiator).unwrap();
        let parsed: Initiator = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.sub, "user-123");
        assert_eq!(parsed.auth_mode, "byot");
        assert_eq!(parsed.org_id, "org-456");
    }

    #[test]
    fn initiator_serde_shared_mode() {
        let initiator = Initiator {
            sub: "user-789".to_string(),
            auth_mode: "shared".to_string(),
            org_id: "".to_string(),
        };
        let json = serde_json::to_string(&initiator).unwrap();
        let parsed: Initiator = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.auth_mode, "shared");
        assert_eq!(parsed.org_id, "");
    }
}
