//! Search MCP handler.
//!
//! Handles: search

use super::lookup_project_by_cwd;
use crate::mcp::types::McpCallbackRequest;
use crate::models::{SearchContentType, SearchFilters};
use crate::orchestrator::Orchestrator;
use serde::Deserialize;

/// Payload for search tool
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchPayload {
    pub query: String,
    pub content_types: Option<Vec<String>>,
    pub project_id: Option<String>,
    pub issue_id: Option<String>,
    pub since: Option<i64>,
    pub limit: Option<usize>,
}

/// Handle search tool call - searches across issues, comments, artifacts, and events.
pub async fn handle_search(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let payload: SearchPayload = match serde_json::from_value(request.payload.clone()) {
        Ok(p) => p,
        Err(e) => return format!("Invalid payload: {}", e),
    };

    log::info!("search called: query={}", payload.query);

    let mut conn = match orch.db.conn.lock() {
        Ok(c) => c,
        Err(e) => return format!("Failed to lock database: {}", e),
    };

    // Get project context for default project filtering
    let ctx = lookup_project_by_cwd(&mut conn, &request.cwd).ok();

    // Build filters - use provided project_id or default to current project
    let project_id = payload
        .project_id
        .or_else(|| ctx.as_ref().map(|c| c.project_id.clone()));

    let filters = SearchFilters {
        project_id,
        issue_id: payload.issue_id,
        content_types: payload.content_types,
        since: payload.since,
        limit: payload.limit,
    };

    match crate::search::search_content(&mut conn, &payload.query, Some(filters)) {
        Ok(results) => {
            format_search_results(&results, ctx.as_ref().map(|c| c.project_key.as_str()))
        }
        Err(e) => format!("Search failed: {}", e),
    }
}

/// Format search results as human-readable text for the agent.
fn format_search_results(
    results: &[crate::models::SearchResult],
    project_key: Option<&str>,
) -> String {
    if results.is_empty() {
        return "No results found.".to_string();
    }

    let mut output = format!("Found {} result(s):\n\n", results.len());

    for (i, result) in results.iter().enumerate() {
        let type_label = match result.content_type {
            SearchContentType::Issue => "Issue",
            SearchContentType::Comment => "Comment",
            SearchContentType::Artifact => "Artifact",
            SearchContentType::Event => "Event",
            SearchContentType::Message => "Message",
        };

        // Use the URI from the search result directly
        let uri = if result.uri.is_empty() {
            let key = project_key.unwrap_or("PROJECT");
            format!("cairn://{}/{}", key, result.id)
        } else {
            result.uri.clone()
        };

        output.push_str(&format!("{}. [{}] {}\n", i + 1, type_label, result.title));
        output.push_str(&format!("   URI: {}\n", uri));
        output.push_str(&format!("   {}\n\n", result.snippet));
    }

    output
}
