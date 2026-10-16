//! Execution `after:` gates. Module declarations only, so that the pieces
//! landing here stay independently editable.
//!
//! The durable gate rows and their store live in `cairn_workflow::gates`,
//! beside the status projection that reads them.

pub mod authoring;
pub mod census;
pub mod sweep;
pub mod vocabulary;
