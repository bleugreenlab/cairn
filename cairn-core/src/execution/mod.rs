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
pub mod check_admission;
pub mod check_isolation;
pub mod check_parsers;
pub mod checkpoint_runs;
pub mod checkpoints;
pub mod checks;
pub mod checks_status;
pub mod checks_turn_end;
pub mod conditions;
pub mod creation;
pub mod dag;
pub mod delegation;
pub mod dispatch;
pub mod jobs;
pub mod ownership;
pub mod queries;
pub mod recipe;
pub mod routing;
pub mod scheduler;
pub mod selection;
pub mod snapshot_edit;
pub mod step_behavior;
pub mod teardown;
pub mod triggers;
pub mod worktree_gc;

/// Resolver key for auto-started DAG jobs and cold resumes. Defined in
/// `models::execution`; re-exported here so existing `crate::execution::Initiator`
/// callers (and the `cairn_core::internal::execution::Initiator` seam) keep
/// resolving.
pub use crate::models::Initiator;
