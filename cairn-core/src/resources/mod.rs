//! URI-addressable Cairn resources.
//!
//! This module owns the `cairn://` resource read domain. Protocol adapters such
//! as MCP should parse their transport payloads and delegate here.

mod actions;
mod agents;
pub mod browsers;
mod common;
mod dev_instances;
mod diff;
mod files;
mod issue;
mod labels;
mod memories;
mod messages;
pub(crate) mod mutations;
mod node;
mod progress;
mod project;
mod read;
mod recipes;
mod settings;
pub(crate) mod symbols;
mod transcript;
mod workflows;

pub(crate) use common::resolve_node_owner_id;
pub(crate) use common::{pointer_affordance_block, resolve_home_relative_resource_uri};
pub(crate) use node::{render_reseed_digest, resolve_todos_job_id};
pub(crate) use read::{produce_cairn_resource, read_cairn_resource};
