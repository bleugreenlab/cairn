//! Execution `after:` gates. Module declarations only, so that the pieces
//! landing here stay independently editable.
//!
//! The durable gate rows and their store live in `cairn_workflow::gates`,
//! beside the status projection that reads them.

#[cfg(test)]
mod admission_tests;
pub mod authoring;
pub mod capacity;
pub mod census;
pub mod pulse;
pub mod reorder;
pub mod sweep;
pub mod vocabulary;
