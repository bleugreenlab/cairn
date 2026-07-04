//! Native OpenRouter HTTP turn/tool loop, split into cohesive submodules.
//!
//! Cairn owns the turn/tool loop for the OpenRouter chat-completions backend so
//! models receive Cairn's direct read/write/run tools. The pieces:
//!
//! - `turn`: session entry and the tool-iteration driver.
//! - `conversation`: rebuild the message array and render tool results.
//! - `context`: trim the outgoing request to the model's real window.
//! - `tools`: classify/execute tool calls; build the tool/provider fields.
//! - `http`: the streaming chat-completions POST and SSE reader.
//! - `wire`: request/response/stream DTOs and the streaming aggregator.
//! - `persist`: the live assistant stream and transcript-event writes.

mod context;
mod conversation;
mod http;
mod persist;
mod tools;
mod turn;
mod wire;

#[cfg(test)]
mod tests;

pub(super) use turn::start_session;
