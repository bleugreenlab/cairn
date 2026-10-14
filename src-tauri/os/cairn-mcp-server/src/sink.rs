//! The seam between the MCP server and whatever executes its calls.
use async_trait::async_trait;
use cairn_common::protocol::CallbackRequest;

use crate::output::CallbackOutcome;

/// Executes one tool call on the host and returns its outcome.
///
/// The server builds the [`CallbackRequest`] (tool, payload, run identity) and
/// renders the [`CallbackOutcome`]; a sink only carries the request to the
/// host's dispatcher and back. The stdio server's sink posts it to the runner's
/// HTTP callback, and the orchestrator's `/mcp` route calls the dispatcher in
/// process.
#[async_trait]
pub trait CallbackSink: Send + Sync + 'static {
    async fn call(&self, request: &CallbackRequest) -> CallbackOutcome;

    /// A non-secret label for the endpoint, named in transport-failure text.
    fn describe(&self) -> &str;
}

/// The run an HTTP request was authenticated as.
///
/// The orchestrator's `/mcp` route inserts this into the request's HTTP
/// extensions after verifying the request's bearer token. A
/// [`Caller::PerRequest`](crate::Caller::PerRequest) server reads it on every
/// call and refuses a request that lacks it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPrincipal {
    pub run_id: String,
}
