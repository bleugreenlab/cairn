//! GitHub API client and credential helpers.
//!
//! Contains shared GitHub functionality that is used by both the
//! Tauri app and the headless server:
//! - Credential management (DB-backed)
//! - Repository URL parsing
//! - GitHub REST API operations (PR read/merge/close, checks, reviews, branches)

pub mod api;
pub mod credentials;
pub mod crypto;
