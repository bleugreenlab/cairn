//! Session archival: rewriting a finished execution's events into durable,
//! compact forms once its issue is resolved — the immutability boundary after
//! which no agent appends to the transcript.
//!
//! There is one writer, [`rewrite::archive_execution`], with two triggers:
//!
//! 1. **Issue teardown** (`execution::teardown::cleanup_issue_jobs`), before the
//!    issue's branches are deleted. Each execution is archived with its
//!    branch's live range, so the writer can pack the branch's objects into
//!    `execution_history` and address reads and writes by git coordinate.
//! 2. **The recurring sweep** ([`sweep`]), which catches every execution whose
//!    issue was resolved more than a grace period ago and still has `full`
//!    rows: late events, failed teardowns, and history from before the writer
//!    existed. It always archives without a range.
//!
//! The READ/codec half every event consumer shares — reconstruction, the
//! compression codec, the layered git object store, the event-column encoding
//! contract, and the per-team content store — lives in [`crate::storage`]
//! (`storage::events` and `storage::content_store`), so readers depend on
//! storage, not on this subsystem.
//!
//! ## Writer coverage matrix
//!
//! What a pass can apply turns on how each path addresses its bytes: git
//! coordinates need a range whose objects still resolve, content hashes do not.
//!
//! | path                  | with range (teardown) | no range (sweep, or no live branch) |
//! |-----------------------|-----------------------|-------------------------------------|
//! | gitcoord read         | yes                   | no                                  |
//! | hybrid read           | yes                   | no                                  |
//! | gitcoord write        | yes (durable commit)  | no                                  |
//! | blobbed system-prompt | yes                   | yes                                 |
//! | blobbed system-init   | yes                   | yes                                 |
//! | zstd backstop         | yes                   | yes                                 |
//! | `execution_history`   | yes                   | no                                  |
//!
//! Both triggers run the same classifier, so a row archived without a range is
//! byte-identical to the same row archived at teardown when it falls to the
//! zstd or blobbed path. A `system:prompt` predating the `raw.segments` boundary
//! map falls to zstd either way.

pub(crate) mod rewrite;
pub mod sweep;

pub use rewrite::{archive_execution, ArchiveRange, ArchiveSummary, ArchiveTarget};
pub use sweep::{run_archival_sweep, SweepSummary};
