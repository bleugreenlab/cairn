//! Session archival foundations: a tagged storage codec, range packfile
//! construction at worktree teardown, and in-memory layered git object access
//! for reconstructing event content from git coordinates.
//!
//! Live entry points: worktree teardown ([`rewrite::archive_target`] via
//! `execution::teardown`), the one-time historical backfill
//! ([`backfill`] via `spawn_archival_maintenance`), and every event read path
//! ([`reconstruct::reconstruct_events`] in `runs::queries`). This module also
//! provides the storage/packing/object-access primitives those paths share, and is
//! exercised here only by its own tests against temporary git repositories.
//!
//! ## Writer coverage matrix
//!
//! Two writers archive events: [`rewrite::archive_target`] at worktree teardown
//! and [`backfill`] for executions already torn down. What each can apply turns
//! on how the path addresses its bytes — git coordinates need the live worktree,
//! content hashes do not:
//!
//! | path                  | teardown | backfill |
//! |-----------------------|----------|----------|
//! | gitcoord read         | yes      | no       |
//! | gitcoord write        | yes      | no       |
//! | blobbed system-prompt | yes      | yes      |
//! | blobbed system-init   | yes      | yes      |
//! | zstd backstop         | yes      | yes      |
//!
//! The two gitcoord paths resolve bytes from objects reachable only while the
//! worktree exists, so the backfill can never apply them and falls those rows to
//! the zstd backstop. The two blobbed paths are content-addressed into
//! `archival_blobs` independent of git, so both writers apply them identically; a
//! `system:prompt` row predating the `raw.segments` boundary map falls to zstd in
//! either writer. This matrix, not "git-addressed" as a synonym for "all content
//! addressing", is the contract — the conflation is how blobbed system-prompt
//! support was missed in the backfill until CAIRN-1569.

pub mod backfill;
pub mod codec;
mod diff;
mod encoding;
pub mod objects;
pub mod packfile;
pub mod reconstruct;
pub mod rewrite;
pub mod store;

// Exposed under `test-utils` (not just `cfg(test)`) so the unfenced integration
// lane (`tests/turso_sync_roundtrip.rs`) can build the same real-anatomy event
// fixtures the in-crate archival tests use.
#[cfg(any(test, feature = "test-utils"))]
pub mod event_fixture;
#[cfg(test)]
pub(crate) mod testutil;

pub use backfill::{run_archival_maintenance, BackfillSummary};
pub use codec::{compress, decompress, CODEC_NONE, CODEC_ZSTD_V1};
pub use diff::{render_range_diff, render_range_file_diffs, NodeDiffFile};
pub use objects::{ObjectStore, ResolvePathError};
pub use packfile::build_execution_pack;
pub use reconstruct::reconstruct_events;
pub use rewrite::{archive_target, ArchiveSummary};
pub use store::{
    BrokeredContentStore, BrokeredContentStoreFactory, ContentStore, ContentStoreFactory,
    TeamReplicaContext,
};

#[cfg(any(test, feature = "test-utils"))]
pub use store::InMemoryContentStore;
