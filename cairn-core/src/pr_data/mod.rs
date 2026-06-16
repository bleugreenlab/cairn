//! PR data helpers — pure computation and HTTP-composed helpers.
//!
//! The `pr_data` table has been replaced by `merge_requests`.
//! This module retains the framework-agnostic helpers (no DB access)
//! that are used by both the Tauri app and headless server.

pub mod actions;
pub mod helpers;
pub mod ports;
