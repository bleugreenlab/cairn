//! Cairn MCP External Server
//!
//! A lightweight MCP server with only the tools that work outside the Cairn app:
//! - Issue management (create_issue, update_issue, add_comment)
//! - Reading issues via cairn:// URIs

use anyhow::Result;
use rmcp::{
    handler::server::tool::{Parameters, ToolRouter},
    model::{
        CallToolResult, Content, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo,
    },
    tool, tool_handler, tool_router, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::future::Future;
use std::sync::Arc;

/// Cairn MCP External Server
#[derive(Clone)]
struct CairnMcpExt {
    callback_url: Arc<String>,
    cwd: Arc<String>,
    tool_router: ToolRouter<Self>,
}

/// Input for add_comment tool (external mode requires issue_number)
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct AddCommentInput {
    /// Issue number to add comment to (e.g., 37 or "CAIRN-37")
    issue_number: String,
    /// The comment content (supports markdown)
    content: String,
}

/// Input for create_issue tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct CreateIssueInput {
    /// Issue title
    title: String,
    /// Issue description (markdown)
    description: Option<String>,
}

/// Input for update_issue tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
struct UpdateIssueInput {
    /// Issue number (e.g., 37 or "CAIRN-37")
    issue_number: String,
    /// New title (optional)
    title: Option<String>,
    /// New description (optional)
    description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CallbackRequest {
    cwd: String,
    tool: String,
    payload: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct CallbackResponse {
    result: String,
}

#[tool_router]
impl CairnMcpExt {
    fn new(callback_url: String, cwd: String) -> Self {
        Self {
            callback_url: Arc::new(callback_url),
            cwd: Arc::new(cwd),
            tool_router: Self::tool_router(),
        }
    }

    /// Add a comment to an issue.
    /// Use this to record discoveries, status updates, or important notes.
    #[tool(description = "Add a comment to an issue by issue number")]
    async fn add_comment(
        &self,
        params: Parameters<AddCommentInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("add_comment called with {} chars", input.content.len());

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            tool: "add_comment".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Create a new issue in the backlog.
    #[tool(description = "Create a new issue in the backlog")]
    async fn create_issue(
        &self,
        params: Parameters<CreateIssueInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("create_issue called: {}", input.title);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            tool: "create_issue".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    /// Update an existing issue's title and/or description.
    #[tool(description = "Update an existing issue")]
    async fn update_issue(
        &self,
        params: Parameters<UpdateIssueInput>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let input = params.0;
        tracing::info!("update_issue called: {}", input.issue_number);

        let request = CallbackRequest {
            cwd: self.cwd.to_string(),
            tool: "update_issue".to_string(),
            payload: serde_json::to_value(&input).unwrap_or_default(),
        };

        let result = self.call_tauri(&request).await;
        Ok(CallToolResult::success(vec![Content::text(result)]))
    }
}

impl CairnMcpExt {
    async fn call_tauri(&self, request: &CallbackRequest) -> String {
        let client = reqwest::Client::new();
        match client
            .post(self.callback_url.as_str())
            .json(request)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                match resp.text().await {
                    Ok(text) => match serde_json::from_str::<CallbackResponse>(&text) {
                        Ok(r) => r.result,
                        Err(e) => {
                            tracing::error!(
                                "Failed to parse response (status {}): {} - body: {}",
                                status,
                                e,
                                &text[..text.len().min(500)]
                            );
                            format!(
                                "Error parsing response: {} (body: {})",
                                e,
                                &text[..text.len().min(200)]
                            )
                        }
                    },
                    Err(e) => format!("Error reading response body: {}", e),
                }
            }
            Err(e) => format!("Error calling Tauri: {}", e),
        }
    }
}

#[tool_handler]
impl ServerHandler for CairnMcpExt {
    fn get_info(&self) -> ServerInfo {
        let instructions = "Cairn MCP server (external mode).\n\n\
             Issue tools:\n\
             - create_issue: Create a new issue in the backlog\n\
             - update_issue: Update an existing issue\n\
             - add_comment: Record discoveries, status updates, or notes on issues";

        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation {
                name: "cairn-mcp-ext".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            instructions: Some(instructions.to_string()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing to stderr (stdout is used for MCP protocol)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .with_writer(std::io::stderr)
        .init();

    // Callback URL - external mode uses /api/mcp/external endpoint
    // Port differs between debug (3857) and release (3847) builds
    let default_port = if cfg!(debug_assertions) { 3857 } else { 3847 };
    let callback_url = env::var("CAIRN_CALLBACK_URL")
        .unwrap_or_else(|_| format!("http://127.0.0.1:{}/api/mcp/external", default_port));
    // Get current working directory - backend uses this to identify the active run
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    tracing::info!("Starting cairn-mcp-ext server (external mode)");
    tracing::info!("Callback URL: {}", callback_url);
    tracing::info!("Working directory: {}", cwd);

    let service = CairnMcpExt::new(callback_url, cwd);

    // Create stdio transport and run the server
    let transport = rmcp::transport::stdio();
    let server = service.serve(transport).await?;

    // Wait for the server to complete
    server.waiting().await?;

    Ok(())
}
