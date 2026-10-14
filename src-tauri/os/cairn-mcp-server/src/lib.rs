//! Cairn's MCP server.
//!
//! Agents reach Cairn through three batch verbs, `read`, `write`, and `run`.
//! This crate is the one implementation of that tool surface: descriptions,
//! input schemas and validation, target resolution, reminder assembly, and
//! result caps. It never executes a call itself. A [`CallbackSink`] does, and a
//! [`Caller`] says which run a call belongs to.
//!
//! Two transports serve it. `cairn-cmd mcp` serves it over stdio for an agent
//! the runner spawned locally, forwarding each call to the runner's `/api/mcp`
//! callback. The orchestrator serves it over streamable HTTP at `/mcp` for an
//! agent on any host, dispatching in process as the run whose bearer the
//! request presented.

mod lifecycle;
pub mod output;
mod resolve;
pub mod schemas;
mod server;
mod sink;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod transport_tests;

pub use lifecycle::classic_stdio_transport;
pub use output::CallbackOutcome;
pub use server::{CairnMcpServer, Caller};
pub use sink::{CallbackSink, RunPrincipal};
