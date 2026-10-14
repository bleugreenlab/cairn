//! Merge request operations — Cairn-native merge lifecycle management.
//!
//! The `merge_requests` table owns the merge lifecycle. When GitHub is
//! connected, it syncs bidirectionally with a GitHub PR, but GitHub is
//! an extension, not the foundation.

pub mod helpers;
pub mod queries;
