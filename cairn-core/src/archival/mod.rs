//! Store-native event archival maintenance.
//!
//! Durable event rows are compacted without consulting an execution checkout.
//! Static system-prompt and system-init segments are content-addressed in the
//! archival blob store; every other eligible payload uses a byte-exact zstd
//! fallback. Event reconstruction and object storage remain in `crate::storage`.

pub mod backfill;
pub(crate) mod rewrite;

pub use backfill::{run_archival_maintenance, BackfillSummary};
