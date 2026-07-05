//! Cairn's database layer — domain models, table records, and the storage
//! engine — quarantined out of cairn-core.
//!
//! This crate holds cairn-core's Turso + tantivy dependency edges behind one
//! boundary:
//! - [`models`] — the durable domain types.
//! - [`db_records`] — the table-shaped row records.
//! - [`storage`] — the `LocalDb` connection wrapper, the migration runner and
//!   its SQL, the tantivy search index, the content store, and the event
//!   encoding/reconstruction codec.
//!
//! cairn-core re-exports all three at their original crate paths, so its
//! storage/models/db_records consumers compile unchanged. The dependency
//! direction is one-way: cairn-db never imports cairn-core. The team-sync loop
//! stays in cairn-core because it drives the orchestrator's `EventEmitter`,
//! which lives above this boundary; `TeamId` is defined here (in [`storage`])
//! and re-exported by cairn-core so the two sides name the same type.

pub mod db_records;
pub mod models;
pub mod storage;

/// The embedded Turso database crate, re-exported as the workspace's single
/// canonical `turso` edge. cairn-db owns the sole `turso` manifest dependency;
/// cairn-core and cairn-transport reach every `turso::` type and the `params!`
/// macro through this `cairn_db::turso::…` path, so no crate above this boundary
/// names turso in its own manifest (CAIRN-2446).
pub use turso;
