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
mod executors;
mod feed;
mod files;
mod grants;
mod issue;
mod labels;
mod memories;
mod messages;
pub mod mutations;
mod node;
pub mod packs;
pub(crate) mod posts;
mod progress;
mod project;
mod read;
mod rebase;
mod recipes;
mod responses;
mod routes;
mod settings;
pub(crate) mod symbols;
mod thread_seed;
mod threads;
mod transcript;
mod workflows;

pub(crate) use common::resolve_node_owner_id;
pub(crate) use common::{connect_and_find_node_job, node_branch, node_job_not_found_message};
pub(crate) use common::{pointer_affordance_block, resolve_home_relative_resource_uri};
pub(crate) use node::{render_reseed_digest, resolve_node_or_task_job_id};
pub(crate) use read::{produce_cairn_resource, read_cairn_resource};
pub(crate) use thread_seed::{compose_thread_seed, ThreadSeed};
/// The one interpretation of a thread sub-path, shared by the read dispatcher,
/// the write dispatcher, and anything that resolves a thread-addressed resource.
pub(crate) use threads::delegate_thread_descendant;
