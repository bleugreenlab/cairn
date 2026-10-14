//! OpenAI Chat Completions: wire values, generation, replay, and HTTP transport.

pub mod context;
pub mod conversation;
pub mod generation;
pub mod http;
pub mod wire;

#[cfg(test)]
mod tests;
