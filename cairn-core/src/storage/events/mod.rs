//! Storage-level event-content codec and reconstruction.
//!
//! Events are recorded `full` on the hot path and rewritten to git coordinates
//! (with a zstd backstop) at worktree teardown by the archival WRITER
//! (`crate::archival`). This module is the READ/codec half every event consumer
//! shares to restore them. The gix + zstd substrate — tagged compression
//! ([`cairn_codec::codec`]), the in-memory layered git
//! ([`cairn_codec::objects::ObjectStore`]), range packfile construction
//! ([`cairn_codec::packfile`]), and its committed diff rendering
//! ([`cairn_codec::diff`]) — lives in the `cairn-codec` crate so cairn-core
//! compiles without gix or zstd; this module re-exports it beside the retained,
//! cairn-core-coupled halves: the event-column encoding contract ([`encoding`])
//! and [`reconstruct::reconstruct_events`]. The re-export keeps every existing
//! `crate::storage::events::*` / `crate::storage::*` consumer compiling
//! unchanged.

pub(crate) mod encoding;
pub mod reconstruct;

// Exposed under `test-utils` (not just `cfg(test)`) so the unfenced integration
// lane (`tests/turso_sync_roundtrip.rs`) can build the same real-anatomy event
// fixtures the in-crate tests use.
#[cfg(any(test, feature = "test-utils"))]
pub mod event_fixture;

// The gix + zstd codec/object substrate, quarantined into cairn-codec. Its API is
// keyed by hex sha strings, so gix types never re-enter cairn-core through it.
// `diff` is re-exported as a module because `reconstruct` calls
// `diff::render_commit_diff`; the rest are surfaced item-by-item.
pub use cairn_codec::codec::{compress, decompress, CODEC_NONE, CODEC_ZSTD_V1};
pub use cairn_codec::diff;
pub use cairn_codec::diff::{
    count_commits_ahead, render_range_diff, render_range_file_diffs, NodeDiffFile,
};
pub use cairn_codec::objects::{ObjectStore, ResolvePathError};
pub use cairn_codec::packfile::build_execution_pack;

// Test-only, mirroring the original `#[cfg(test)] pub(crate) mod testutil`, so
// core test modules keep resolving `crate::storage::events::testutil::*`.
#[cfg(test)]
pub(crate) use cairn_codec::testutil;

pub use reconstruct::reconstruct_events;
