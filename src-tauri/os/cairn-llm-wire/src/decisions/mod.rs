//! The Decisions wire: typed questions in, per-question answers out, one
//! non-streamed POST to `{base}/decisions`.
//!
//! One wire, many API-key providers: OpenAI's Decisions API, the Vercel AI
//! Gateway (which serves `openai/gpt-6-luna-decisions` and `typesafe-ai/jev`
//! through it), and TypeSafe directly all take this request and return this
//! reply. Nothing here branches on which one it is talking to; the URL and
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
