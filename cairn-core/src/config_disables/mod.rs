//! Per-project disable of inherited workspace config artifacts.
//!
//! Recipes, agents, and skills inherit from the workspace and shadow by id;
//! actions inherit and shadow by name. This module records a per-project
//! decision to *suppress* an inherited workspace artifact for one project
//! without copying or redefining it — the lever the MCP `enabled: false` flag
//! already gives MCP servers, generalized to the other four config types.
//!
//! MCP servers are deliberately excluded: they keep their existing yaml
//! `enabled` mechanism (see `config::mcp_servers::resolve_mcp_servers`). Every
//! other type routes its disable state through the `config_disables` table.

pub mod queries;

pub use queries::{
    disable_config, enable_config, list_disabled_configs, list_disabled_keys, DisabledConfig,
};
