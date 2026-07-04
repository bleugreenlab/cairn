//! Session archival subsystem: the writer that compacts a torn-down execution's
//! events to git coordinates at the worktree-teardown immutability boundary, and
//! the one-time backfill that applies the same compaction to executions already
//! torn down.
//!
//! Live entry points: worktree teardown ([`rewrite::archive_target`] via
//! `execution::teardown`) and the one-time historical backfill ([`backfill`] via
//! `spawn_archival_maintenance`). Both are WRITERS. The READ/codec half every
//! event consumer shares — reconstruction, the compression codec, the layered
//! git object store, the event-column encoding contract, and the per-team content
//! store — lives one layer down in [`crate::storage`] (`storage::events` and
//! `storage::content_store`) so readers depend on storage, not on this subsystem;
//! this module draws on that surface to write what those readers later restore.
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
pub mod rewrite;

pub use backfill::{run_archival_maintenance, BackfillSummary};
pub use rewrite::{archive_target, ArchiveSummary};
