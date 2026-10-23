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
mod dev_instances;
mod diff;
mod eras;
pub(crate) mod event_endpoints;
pub(crate) mod events;
mod executors;
pub(crate) mod feed;
mod files;
mod grants;
pub mod home;
mod inference;
mod issue;
mod labels;
mod memories;
mod messages;
mod models;
pub mod mutations;
mod node;
pub mod packs;
pub(crate) mod posts;
mod progress;
mod project;
mod project_map;
mod read;
mod rebase;
mod recipes;
mod responses;
mod routes;
mod settings;
pub(crate) mod sql_policy;
pub(crate) mod sql_schema;
pub(crate) mod symbols;
pub(crate) mod tenets;
pub(crate) mod thread_seed;
mod threads;
mod transcript;
mod vcs;
mod workflows;

pub(crate) use common::resolve_node_owner_id;
pub(crate) use common::{connect_and_find_node_job, node_branch, node_job_not_found_message};
pub(crate) use common::{
    pointer_affordance_block, resolve_home_relative_against, resolve_home_relative_resource_uri,
};
pub(crate) use node::{
    render_reseed_digest, reseed_verbatim_baseline_bytes, resolve_node_or_task_job_id,
    resolve_node_or_task_job_id_for_read,
};
pub(crate) use read::run_condition_sql;
pub(crate) use read::{produce_cairn_resource, read_cairn_resource};
pub(crate) use rebase::node_base_branch;
pub(crate) use thread_seed::{compose_thread_seed_with_policy, SeedPolicy, ThreadSeed};
/// The one interpretation of a thread sub-path, shared by the read dispatcher,
/// the write dispatcher, and anything that resolves a thread-addressed resource.
pub(crate) use threads::delegate_thread_descendant;
pub(crate) use transcript::era_source_digest;
