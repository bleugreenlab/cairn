//! Application integration for the session-owned process runtime.
//!
//! Process handles, parsing, arguments, and turn boundaries are re-exported from
//! `cairn-session`. Core resolves attachment images and supplies workflow
//! protection and relevance policy to warm-process eviction.

pub(crate) mod completion;
pub mod gc;
pub mod memory;
pub mod orphan;
pub mod process;
pub mod stdin;
pub mod stream;
pub mod toolkits;
pub mod turn_boundary;
