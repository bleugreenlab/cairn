//! Wake subscriptions, mute/snooze, and side-channel delivery.
//!
//! Split from a single `wakes.rs` into cohesive submodules; see `docs/wakes.md`
//! for the conceptual model. The submodules are internal detail — the public
//! surface is re-exported here so every `crate::orchestrator::wakes::X` path
//! resolves unchanged.

mod addressed;
mod checks;
mod child;
pub mod conditions;
mod matching;
mod posts;
mod routing;
mod schedules;
mod side_channel;
mod store;
mod terminal;
mod types;

#[cfg(test)]
mod tests;

pub(crate) use addressed::*;
pub(crate) use checks::*;
pub use child::*;
pub use matching::*;
pub use posts::*;
pub use routing::*;
pub use schedules::*;
pub use side_channel::*;
pub use store::*;
pub use terminal::*;
pub use types::*;
