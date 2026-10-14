//! Durable storage and database-to-domain adaptation for Cairn.
//!
//! [`models`] re-exports the storage-free values from `cairn-models`.
//! [`db_records`] owns table-shaped rows, and `conversions` owns their
//! `From`/`TryFrom` implementations into those values. [`storage`] owns Turso,
//! migrations, the Tantivy index, content storage, and event reconstruction.
//! Core re-exports the existing model, record, and storage paths.

mod conversions;
pub mod db_records;
pub mod models;
pub mod storage;

/// The embedded Turso database crate, re-exported as the workspace's single
/// canonical `turso` edge. cairn-db owns the sole `turso` manifest dependency;
/// cairn-core and cairn-transport reach every `turso::` type and the `params!`
/// macro through this `cairn_db::turso::…` path, so no crate above this boundary
/// names turso in its own manifest (CAIRN-2446).
pub use turso;

pub mod error;

pub mod artifacts;
pub mod changes;
mod condition_sql;
pub mod lineage;
pub mod relations;
pub mod todos;
