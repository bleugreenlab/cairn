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

pub mod actions;
pub mod advancement;
pub mod cache;
pub mod checkpoints;
pub mod conditions;
pub mod creation;
pub mod dag;
pub mod executor;
pub mod jobs;
pub mod queries;
pub mod recipe;
pub mod step_behavior;
