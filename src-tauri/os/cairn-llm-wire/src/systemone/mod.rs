//! The System One wire: named questions about a `state` in, answers keyed by
//! the same names out, one non-streamed POST.
//!
//! TypeSafe's own endpoint (`POST https://api.typesafe.ai/v1/systemone`,
//! <https://docs.typesafe.ai/api/system-one>) and OpenRouter's Decisions API
//! (`POST https://openrouter.ai/api/alpha/decisions`, which serves every model
//! in its `output_modalities=decisions` listing) both speak it. It is a
//! sibling of [`crate::decisions`], not a dialect: questions are a map keyed
//! by name rather than an array, a yes-or-no question is a `noul` rather than
//! a `predicate`, options and levels travel as `criteria`, and probabilities
//! come back as maps keyed by option value or level index.
//!
//! It takes the same [`crate::decisions::wire::WireQuestion`]s the facet
//! already builds, and posts to the same
//! [`crate::decisions::http::DecisionsEndpoint`]; only the body and the reply
//! differ.
//!
//! # Refusals
//!
//! Neither reference shows a refusal: every answer is a `noul`, `choice`,
//! `score` or `box`. So a refusal on this wire is whatever is not an answer
//! of a shape Cairn recognises. An answer with an unknown `type` or a missing
//! field decodes as [`wire::SystemOneAnswer::Unrecognised`], carrying the
//! JSON as it came, and the facet reads it (and a question with no answer
//! under its name at all) as a refusal: never a pick, never a probability
//! of 0.

pub mod http;
pub mod wire;

#[cfg(test)]
mod tests;
