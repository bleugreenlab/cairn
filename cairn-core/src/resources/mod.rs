//! URI-addressable Cairn resources.
//!
//! This module owns the `cairn://` resource read domain. Protocol adapters such
//! as MCP should parse their transport payloads and delegate here.

mod actions;
mod agents;
pub mod browsers;
mod common;
mod dev_instances;
mod files;
mod issue;
mod labels;
mod memories;
mod messages;
pub(crate) mod mutations;
mod node;
mod project;
mod read;
mod recipes;
mod settings;
pub(crate) mod symbols;
mod transcript;

pub(crate) use common::resolve_node_owner_id;
pub(crate) use node::resolve_todos_job_id;
pub(crate) use read::{produce_cairn_resource, read_cairn_resource};
