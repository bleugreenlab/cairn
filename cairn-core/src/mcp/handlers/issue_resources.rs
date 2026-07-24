//! MCP adapter for URI-addressable Cairn resources.

use serde::Deserialize;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;

/// Payload for read_issue_resource request
#[derive(Debug, Clone, Deserialize)]
pub struct ReadIssueResourcePayload {
    uri: String,
}

/// Handle read_issue_resource request.
pub async fn handle_read_issue_resource(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> String {
    let payload: ReadIssueResourcePayload = match super::parse_payload(request) {
        Ok(payload) => payload,
        Err(error) => return error,
    };

    crate::resources::read_cairn_resource(orch, request, &payload.uri).await
}
