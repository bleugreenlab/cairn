//! Event-payload codec and the git-object substrate it archives to, quarantined.
//!
//! This crate holds cairn-core's gix + zstd dependency edges behind one boundary
//! so the rest of cairn-core compiles without them. It is the read/codec half of
//! session archival:
//! - [`codec`] — tagged payload compression (a per-row codec marker keeps
//!   decompression stable forever).
//! - [`objects`] — the in-memory, layered git [`objects::ObjectStore`] that
//!   resolves objects from an execution range pack layered over the repo ODB.
//! - [`packfile`] — range-pack construction at worktree teardown.
//! - [`diff`] — committed range/commit diff rendering, entirely in-process (no
//!   `.git`, no shelling out).
//!
//! The public API is keyed by hex SHA-1 strings, never gix `ObjectId`/`oid`, so
//! gix types never cross this crate's boundary and cairn-core stays gix-free. The
//! dependency direction is one-way: cairn-codec never imports cairn-core.

pub mod codec;
pub mod diff;
pub mod objects;
pub mod packfile;
pub mod transfer;

// Test helpers, exposed under `test-utils` (not just `cfg(test)`) so cairn-core's
// archival tests and its unfenced integration lane build identical fixtures.
#[cfg(any(test, feature = "test-utils"))]
pub mod testutil;
