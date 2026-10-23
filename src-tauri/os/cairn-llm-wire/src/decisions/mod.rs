//! The Decisions wire: typed questions in, per-question answers out, one
//! non-streamed POST to `{base}/decisions`.
//!
//! OpenAI's Decisions API speaks this wire: a `questions` array of
//! `predicate | choice | score`, answered in question order, with a refusal
//! variant carrying the declined question's name. TypeSafe and OpenRouter
//! speak System One instead, which is its own wire ([`crate::systemone`]).
//! The Vercel AI Gateway documents this shape for its OpenAI-compatible
//! decisions route; whether the models it serves there from other vendors
//! (`typesafe-ai/jev`) answer it faithfully has not been verified. Nothing
//! here branches on which provider it is talking to; the URL and
//! authentication arrive as a [`http::DecisionsEndpoint`].
//!
//! A decision generates nothing, so unlike the three turn families there is
//! no conversation, no tools, and no stream: [`http::build_body`] and
//! [`http::post_decisions`] are the whole family. The types here are the
//! provider's JSON, not Cairn's answer type; the facet's `decide` maps them
//! onto `cairn_domain::decision`, which is where the refusal rule is kept.

pub mod http;
pub mod wire;

#[cfg(test)]
mod tests;
