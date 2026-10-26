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
//! `score` or `box`. A refusal arrives in one of two shapes.
//!
//! In band, which no provider has been seen to send: whatever is not an
//! answer of a shape Cairn recognises. An answer with an unknown `type` or a
//! missing field decodes as [`wire::SystemOneAnswer::Unrecognised`], carrying
//! the JSON as it came, and the facet reads it (and a question with no
//! answer under its name at all) as a refusal.
//!
//! Out of band, as OpenRouter does when an OpenAI decision model declines
//! one question: the whole request fails (HTTP 502) with a message reading
//! `OpenAI refused to answer question "<name>"`, and every other answer in it
//! is lost. [`http::post_systemone`] asks the remaining questions again
//! without that one and splices a [`wire::SystemOneAnswer::Refused`] carrying
//! the gateway's sentence under its name, up to a bound per call.
//!
//! Either way a refusal is never a pick and never a probability of 0.

pub mod http;
pub mod wire;

#[cfg(test)]
mod tests;
