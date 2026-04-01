//! Shared merge request helper logic — pure computation and HTTP-composed helpers.
//!
//! Re-exports from the original pr_data::helpers for backwards compatibility.
//! These are framework-agnostic: no Tauri, no AppHandle.

// Re-export all the pure computation and HTTP helpers.
// The actual implementations stay in pr_data::helpers since they're
// framework-agnostic and don't reference the pr_data table.
pub use crate::pr_data::helpers::*;
