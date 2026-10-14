//! OpenAI Responses: wire values, generation, replay, and HTTP transport.

pub mod conversation;
pub mod generation;
pub mod http;
#[cfg(test)]
mod tests;
pub mod wire;
