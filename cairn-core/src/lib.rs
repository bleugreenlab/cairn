//! Cairn Core - Business logic library with no Tauri dependency.
//!
//! This crate contains the core domain types, database models, service traits,
//! and business logic that can be shared between the Tauri desktop app and
//! a headless server deployment.

pub mod action_configs;
pub mod action_runs;
pub mod agents;
pub mod artifacts;
pub mod chats;
pub mod claude;
pub mod condition;
pub mod config;
pub mod db;
pub mod debug;
pub mod deployment;
pub mod diesel_models;
pub mod docs;
pub mod env;
pub mod execution;
pub mod executions;
pub mod git;
pub mod image_cache;
pub mod issues;
pub mod jobs;
pub mod mcp;
pub mod memories;
pub mod messages;
pub mod models;
pub mod orchestrator;
pub mod pr_data;
pub mod projects;
pub mod runs;
pub mod schema;
pub mod schemas;
pub mod search;
pub mod services;
pub mod skills;
pub mod snapshot;
pub mod tools;

#[cfg(test)]
pub(crate) mod test_utils;
