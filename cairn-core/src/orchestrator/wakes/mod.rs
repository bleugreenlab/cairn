//! Wake subscriptions, mute/snooze, and side-channel delivery.
//!
//! Split from a single `wakes.rs` into cohesive submodules; see `docs/wakes.md`
//! for the conceptual model. The submodules are internal detail — the public
//! surface is re-exported here so every `crate::orchestrator::wakes::X` path
//! resolves unchanged.

mod child;
mod matching;
mod routing;
mod side_channel;
mod store;
mod terminal;
mod types;

#[cfg(test)]
mod tests;

pub use child::*;
pub use matching::*;
pub use routing::*;
pub use side_channel::*;
pub use store::*;
pub use terminal::*;
pub use types::*;
