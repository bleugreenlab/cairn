//! URI-addressable Cairn resources.
//!
//! This module owns the `cairn://` resource read domain. Protocol adapters such
//! as MCP should parse their transport payloads and delegate here.

mod actions;
mod agents;
pub mod browsers;
pub(crate) mod channels;
mod check_results;
mod codemap;
mod common;
pub(crate) mod db_rows;
mod dev_instances;
mod diff;
mod eras;
pub(crate) mod event_endpoints;
pub(crate) mod events;
pub(crate) mod executors;
pub(crate) mod feed;
mod files;
pub mod grants;
pub mod home;
mod inference;
mod inference_generate;
mod issue;
mod labels;
mod library_listing;
mod memories;
mod messages;
pub(crate) mod models;
pub mod mutations;
mod node;
pub mod packs;
pub(crate) mod paging;
pub(crate) mod posts;
mod progress;
mod project;
mod project_map;
mod queue;
mod read;
#[cfg(test)]
mod read_contract_tests;
mod rebase;
mod recipes;
pub mod references;
mod requests;
mod responses;
mod routes;
pub mod self_rebuild;
mod services;
mod settings;
mod spend;
pub(crate) mod sql_policy;
pub(crate) mod sql_schema;
pub(crate) mod symbols;
pub(crate) mod tenets;
pub(crate) mod thread_seed;
mod threads;
mod transcript;
mod vcs;
mod workflows;

pub(crate) use common::node_job_not_found_message;
pub(crate) use common::resolve_node_owner_id;
pub(crate) use common::{
    pointer_affordance_block, resolve_home_relative_against, resolve_home_relative_resource_uri,
};
pub(crate) use node::{
    reseed_verbatim_baseline_bytes, resolve_node_or_task_job_id,
    resolve_node_or_task_job_id_for_read,
};
pub(crate) use read::open_thread_store;
pub(crate) use read::run_condition_sql;
pub(crate) use read::{produce_cairn_resource, read_cairn_resource};
pub(crate) use rebase::{node_base_branch, NodeRebaseRows};
pub(crate) use thread_seed::{
    compose_node_seed, compose_thread_seed_with_policy, seed_tokens, SeedPolicy, ThreadSeed,
};
/// The one interpretation of a thread sub-path, shared by the read dispatcher,
/// the write dispatcher, and anything that resolves a thread-addressed resource.
#[cfg(test)]
pub(crate) use threads::read_thread_queue;
pub(crate) use transcript::{era_source_digest, navigation_passage_event_id};

/// One job's transcript exactly as `.../chat/raw?format=json` serves it: every
/// event reconstructed and passed through the quarantine gate. For offline
/// verification against a database file (the archive offload's clone
/// rehearsal), where there is no running app to read the resource from.
pub async fn render_job_chat_raw_json(db: &crate::storage::LocalDb, job_id: &str) -> String {
    node::render_job_chat_raw(db, job_id, transcript::RawTranscriptFormat::Json).await
}
