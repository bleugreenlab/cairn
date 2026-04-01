//! Cairn MCP External Server
//!
//! A lightweight MCP server with only the tools that work outside the Cairn app:
//! - Issue management (create_issue, update_issue, add_comment)
//! - Reading issues via cairn:// URIs
//! - Resource discovery (list_resources, list_resource_templates, read_resource)

use anyhow::Result;
use rmcp::{
    handler::server::tool::{Parameters, ToolRouter},
    model::{
        Annotated, CallToolResult, Content, Implementation, ListResourceTemplatesResult,
        ListResourcesResult, PaginatedRequestParam, ProtocolVersion, RawResource,
        RawResourceTemplate, ReadResourceRequestParam, ReadResourceResult, ResourceContents,
        ServerCapabilities, ServerInfo,
    },
    service::RequestContext,
    tool, tool_handler, tool_router, RoleServer, ServerHandler, ServiceExt,
};
use serde::{Deserialize, Serialize};
use std::env;
use std::future::Future;
use std::sync::Arc;

use cairn_common::uri::{parse_uri, CairnResource};

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
    /// Project key, or prefix the issue number (e.g. "CAIRN-37"). Use list_resources to see available projects.
    project: Option<String>,
}

/// Input for create_issue tool
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct CreateIssueInput {
    /// Issue title
    title: String,
    /// Issue description (markdown)
    description: Option<String>,
    /// Project key. Use list_resources to see available projects.
    project: Option<String>,
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
    /// Project key, or prefix the issue number (e.g. "CAIRN-37"). Use list_resources to see available projects.
    project: Option<String>,
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

/// Resource info returned from Tauri callback
#[derive(Debug, Clone, Deserialize)]
struct ResourceInfo {
    uri: String,
    name: String,
    description: Option<String>,
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

    /// Build static resource templates for the cairn:// URI space.
    /// These describe the URI patterns available — no callback needed.
    fn resource_templates() -> Vec<Annotated<RawResourceTemplate>> {
        let templates = vec![
            (
                "cairn://{project}",
                "Project overview",
                "Project overview with recent issues and status",
            ),
            (
                "cairn://{project}/{number}",
                "Issue details",
                "Issue overview with comments, PR data, and execution history",
            ),
            (
                "cairn://{project}/{number}/files",
                "Issue files",
                "All files changed across executions for an issue",
            ),
            (
                "cairn://{project}/{number}/messages",
                "Issue messages",
                "Messages between agents working on an issue",
            ),
            (
                "cairn://{project}/messages",
                "Project messages",
                "Project-wide messages between agents",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}",
                "Node summary",
                "Execution node summary with status and metadata",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/chat",
                "Node transcript",
                "Agent conversation transcript (truncated)",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/chat/full",
                "Full transcript",
                "Complete agent conversation transcript",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/artifact",
                "Node artifact",
                "Agent output artifact (plan, PR, etc.)",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/files",
                "Node files",
                "Files changed by a specific execution node",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/task/{name}/chat",
                "Task transcript",
                "Sub-task conversation transcript",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/task/{name}/chat/full",
                "Full task transcript",
                "Complete sub-task conversation transcript",
            ),
            (
                "cairn://{project}/{number}/{exec}/{node}/task/{name}/artifact",
                "Task artifact",
                "Sub-task output artifact",
            ),
            (
                "cairn://{project}/chat/{name}",
                "Project chat",
                "Project-level chat session transcript",
            ),
        ];

        templates
            .into_iter()
            .map(|(uri_template, name, description)| {
                Annotated::new(
                    RawResourceTemplate {
                        uri_template: uri_template.to_string(),
                        name: name.to_string(),
                        description: Some(description.to_string()),
                        mime_type: Some("text/plain".to_string()),
                    },
                    None,
                )
            })
            .collect()
    }
}

#[tool_handler]
impl ServerHandler for CairnMcpExt {
    fn get_info(&self) -> ServerInfo {
        let instructions = "Cairn MCP server (external mode).\n\n\
             Resources:\n\
             - List resources to see recent issues for the current project\n\
             - List resource templates for cairn:// URI patterns\n\
             - Read resources to access issue details, transcripts, artifacts\n\n\
             Issue tools:\n\
             - create_issue: Create a new issue in the backlog\n\
             - update_issue: Update an existing issue\n\
             - add_comment: Record discoveries, status updates, or notes on issues";

        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder()
                .enable_tools()
                .enable_resources()
                .build(),
            server_info: Implementation {
                name: "cairn-mcp-ext".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            instructions: Some(instructions.to_string()),
        }
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourceTemplatesResult, rmcp::ErrorData>> + Send + '_
    {
        async move {
            tracing::info!("list_resource_templates called");
            let templates = Self::resource_templates();
            Ok(ListResourceTemplatesResult::with_all_items(templates))
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParam>,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListResourcesResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            tracing::info!("list_resources called");

            let request = CallbackRequest {
                cwd: self.cwd.to_string(),
                tool: "list_resources".to_string(),
                payload: serde_json::json!({}),
            };

            let result = self.call_tauri(&request).await;

            match serde_json::from_str::<Vec<ResourceInfo>>(&result) {
                Ok(infos) => {
                    let resources = infos
                        .into_iter()
                        .map(|info| {
                            Annotated::new(
                                RawResource {
                                    uri: info.uri,
                                    name: info.name,
                                    description: info.description,
                                    mime_type: Some("text/plain".to_string()),
                                    size: None,
                                },
                                None,
                            )
                        })
                        .collect();
                    Ok(ListResourcesResult::with_all_items(resources))
                }
                Err(e) => {
                    tracing::error!("Failed to parse resources: {} - response: {}", e, result);
                    Ok(ListResourcesResult::default())
                }
            }
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParam,
        _context: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ReadResourceResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let uri = &request.uri;
            tracing::info!("read_resource called: uri={}", uri);

            // Validate it's a cairn:// URI
            if !uri.starts_with("cairn://") {
                return Err(rmcp::ErrorData::invalid_request(
                    format!("Unknown resource scheme: {}", uri),
                    None,
                ));
            }

            // Parse and reject terminal URIs (not meaningful in external mode)
            match parse_uri(uri) {
                Some(CairnResource::NodeTerminal { .. })
                | Some(CairnResource::ProjectTerminal { .. }) => {
                    return Err(rmcp::ErrorData::invalid_request(
                        "Terminal resources are not available in external mode",
                        None,
                    ));
                }
                None => {
                    return Err(rmcp::ErrorData::invalid_request(
                        format!("Invalid cairn resource URI: {}", uri),
                        None,
                    ));
                }
                Some(_) => {} // Valid non-terminal resource, proceed
            }

            // Call Tauri to read the resource
            let callback_request = CallbackRequest {
                cwd: self.cwd.to_string(),
                tool: "read_issue_resource".to_string(),
                payload: serde_json::json!({ "uri": uri }),
            };

            let result = self.call_tauri(&callback_request).await;
            let contents = vec![ResourceContents::text(result, uri.as_str())];
            Ok(ReadResourceResult { contents })
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize unified logging (file + stderr; stdout reserved for MCP protocol)
    let _log_guard = cairn_common::logging::init(cairn_common::logging::LogConfig {
        process: cairn_common::logging::ProcessTag::Mcp,
        log_dir: None,
        stderr: true,
    })
    .expect("Failed to initialize logging");

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_templates_are_valid() {
        let templates = CairnMcpExt::resource_templates();

        // Should have templates for all non-terminal resource types
        assert!(
            templates.len() >= 14,
            "Expected at least 14 templates, got {}",
            templates.len()
        );

        // All templates should use RFC 6570 URI template syntax with {param}
        for template in &templates {
            let uri = &template.raw.uri_template;
            assert!(
                uri.starts_with("cairn://"),
                "Template should start with cairn://: {}",
                uri
            );
            assert!(
                uri.contains('{'),
                "Template should contain {{param}} syntax: {}",
                uri
            );
        }
    }

    #[test]
    fn test_resource_templates_no_terminal_uris() {
        let templates = CairnMcpExt::resource_templates();

        for template in &templates {
            let uri = &template.raw.uri_template;
            assert!(
                !uri.contains("/terminal/"),
                "Templates should not include terminal URIs: {}",
                uri
            );
        }
    }

    #[test]
    fn test_resource_templates_have_descriptions() {
        let templates = CairnMcpExt::resource_templates();

        for template in &templates {
            assert!(
                template.raw.description.is_some(),
                "Template '{}' should have a description",
                template.raw.name
            );
        }
    }
}
