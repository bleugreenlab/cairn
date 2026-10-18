//! The cairn-models facet: how tokens are produced, routed, and accounted.
//!
//! This is the one crate outside consumers import from. Its members are
//! re-exported whole:
//!
//! - [`wire`]: the three HTTP protocol families (`cairn-llm-wire`).
//! - [`image_wire`]: OpenRouter's Image API and the OpenAI Images API
//!   (`cairn-image-wire`).
//! - [`speech_wire`]: the speech protocols (`cairn-speech-wire`).
//! - [`types`]: provider ids, the shipped catalog, usage windows and
//!   snapshots, pooled capacity (`cairn-models-types`).
//! - [`accounts`]: the credential store, account routing, and leases
//!   (`cairn-credentials`).
//!
//! [`interface`] is what the facet answers: the [`interface::Models`] trait the
//! `cairn://models/*` resources render, and the launch side every backend adapts
//! to, [`interface::complete::Complete`] and [`interface::session`], and the
//! image verb, [`interface::image`].
//! [`complete`], [`session`], and [`image`] are the implementations that reach
//! providers, [`decide`] and [`speak`] the drivers of the decision and speech
//! verbs.
//! [`turn`] is the provider-neutral HTTP agent turn: the loop of generation
//! and host-dispatched tool calls that every HTTP backend runs.
//! [`continuation`] decides whether a session resumes natively or is rebuilt,
//! and renders the digest a rebuilt session is seeded with.
//! [`claude::feeds`] keeps live Claude sessions on their account's current
//! credential, for Cairn's runtime and any other host alike.
//!
//! Subscription mechanics (CLI sign-in, account-home placement, token refresh,
//! subscription usage probes) are not public; they live in the private
//! `cairn-models-subscriptions` crate outside `os/`.
pub use cairn_credentials as accounts;
pub use cairn_image_wire as image_wire;
pub use cairn_llm_wire as wire;
pub use cairn_models_types as types;
pub use cairn_speech_wire as speech_wire;

pub mod catalog;
pub mod claude;
pub mod codex;
pub mod complete;
pub mod continuation;
pub mod decide;
pub mod image;
pub mod interface;
pub mod openrouter;
pub mod session;
pub mod sign_in;
pub mod speak;
pub mod subscription;
pub mod tokenizer_store;
pub mod turn;
pub mod usage;
