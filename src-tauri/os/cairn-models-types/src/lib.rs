//! The cairn-models facet's vocabulary.
//!
//! Which providers exist ([`provider_id`], [`catalog`]), how their usage is
//! reported ([`usage`]), how per-account measurements pool into one
//! provider's capacity ([`pool`]), and what an image offer ([`image`]) or a
//! speech offer ([`speech`]) can do. Pure values and pure functions only; this
//! crate depends on no other workspace crate, so anything that only needs to
//! name a provider or render a usage window can depend on it alone.
pub mod catalog;
pub mod image;
pub mod pool;
pub mod provider_id;
pub mod speech;
pub mod tokenizers;
pub mod usage;

pub use provider_id::ApiProvider;
