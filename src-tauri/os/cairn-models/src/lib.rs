//! The cairn-models facet: how tokens are produced, routed, and accounted.
//!
//! This is the one crate outside consumers import from. Its members are
//! re-exported whole:
//!
//! - [`wire`]: the three HTTP protocol families (`cairn-llm-wire`).
//! - [`types`]: provider ids, the shipped catalog, usage windows and
//!   snapshots, pooled capacity (`cairn-models-types`).
//! - [`accounts`]: the credential store, account routing, and leases
//!   (`cairn-credentials`).
//!
//! [`interface`] is what the facet answers: the [`interface::Models`] trait the
//! `cairn://models/*` resources render, and the launch side every backend adapts
//! to, [`interface::complete::Complete`] and [`interface::session`].
//! [`complete`] and [`session`] are the implementations that reach providers.
//! [`turn`] is the provider-neutral HTTP agent turn: the loop of generation
//! and host-dispatched tool calls that every HTTP backend runs.
//! [`continuation`] decides whether a session resumes natively or is rebuilt,
//! and renders the digest a rebuilt session is seeded with.
//!
//! Subscription mechanics (CLI sign-in, account-home placement, token refresh,
//! subscription usage probes) are not public; they live in the private
//! `cairn-models-subscriptions` crate outside `os/`.
pub use cairn_credentials as accounts;
pub use cairn_llm_wire as wire;
pub use cairn_models_types as types;

pub mod catalog;
pub mod claude;
pub mod codex;
pub mod complete;
pub mod continuation;
pub mod decide;
pub mod interface;
pub mod session;
pub mod sign_in;
pub mod subscription;
pub mod turn;
pub mod usage;
