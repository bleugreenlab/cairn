//! Execution `after:` gates: the durable rows that hold an execution before it
//! starts. Module declarations only.
//!
//! The store lives here rather than beside the predicate vocabulary in
//! `cairn-core` because the readers of these rows — the status projection in
//! [`crate::transitions`] and execution detail in [`crate::queries`] — live in
//! this crate, and `cairn-core` depends on it rather than the reverse.

pub mod store;
