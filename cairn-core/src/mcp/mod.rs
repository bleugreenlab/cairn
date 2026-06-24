//! MCP callback server types, handlers, and helpers.
//!
//! This module contains the authentication, git helpers, shared types,
//! and framework-agnostic handler logic used by the MCP callback server.
//! Both Tauri and cairn-server dispatch to handlers in this module.

pub mod auth;
pub mod diff;
pub mod gateway;
pub mod git;
pub mod handlers;
pub mod oauth;
pub mod types;
pub mod vcs;
pub mod wildcard;

pub use auth::McpAuthState;
