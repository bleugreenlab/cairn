//! External MCP gateway trait.
//!
//! Cairn is an MCP *client/gateway*: a spawned agent reaches configured
//! external servers (Playwright, Linear, ...) through the `cairn://mcp/...` URI
//! family without those servers being injected into the agent's own MCP config.
//!
//! Core stays rmcp-free. The trait is defined here; the concrete rmcp client
//! implementation lives in the host crates (Tauri app, cairn-server) and is set
//! on the `Orchestrator` after construction, mirroring `EffectExecutor`.
//!
//! ## Connection model
//!
//! Implementations pool connections keyed by `(session_key, server)`. The
//! `session_key` is the run's job id, so two concurrent agents each get their
//! own server process (isolation — e.g. separate Playwright browsers).
//! Connections are spawned lazily on first use, kept warm across calls within a
//! session, and torn down via `close_session` when the job completes.

use async_trait::async_trait;
use cairn_common::read::ImageBlock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::security::BrokeredMcpConfig;

/// A bidirectional byte stream suitable for an MCP client transport.
pub trait DuplexIo: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

impl<T> DuplexIo for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}

/// Factory for a machine-local MCP facade reached through service placement.
///
/// The gateway asks for a stream only when its machine-scoped pool has no live
/// connection. Core owns placement; transport owns the MCP protocol lifecycle.
#[async_trait]
pub trait PlacedFacade: Send + Sync {
    /// Stable machine identity used by the gateway connection pool.
    fn key(&self) -> &str;

    /// Open a fresh byte stream to the facade process on that machine.
    async fn open(&self) -> Result<Box<dyn DuplexIo>, String>;
}

/// The result of a proxied external MCP `tools/call`.
///
/// `text` is the flattened textual content. `images` carries any image content
/// blocks the server returned, preserved rather than flattened to a placeholder
/// so they can reach the agent as real image content blocks — mirroring the read
/// path's text/image split (`cairn_common::read`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct McpToolCallResult {
    pub text: String,
    pub images: Vec<ImageBlock>,
}

/// One protocol round of an external MCP tool call. Intermediate outcomes are
/// deliberately serializable: core persists them before yielding the agent turn,
/// then a later host can reconnect and continue without retaining an rmcp value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum McpCallOutcome {
    Complete(McpToolCallResult),
    InputRequired {
        input_requests: serde_json::Value,
        request_state: Option<String>,
    },
    Task {
        task_id: String,
        poll_interval_ms: Option<u64>,
        ttl_ms: Option<u64>,
    },
}

/// Durable result of polling an MCP task.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum McpTaskOutcome {
    Working { poll_interval_ms: Option<u64> },
    InputRequired { input_requests: serde_json::Value },
    Complete(McpToolCallResult),
    Failed { message: String },
    Cancelled,
}

/// A tool advertised by an external MCP server.
///
/// `Deserialize` lets the persisted MCP tool store (`config::mcp_tools`) round-
/// trip these defs through its sidecar JSON file, so the agent affordance block
/// can render terse per-tool contracts synchronously without spawning servers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolDef {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema for the tool's arguments (the `inputSchema`), surfaced so the
    /// agent can construct a correct `run` payload.
    pub input_schema: serde_json::Value,
}

/// A complete `tools/list` catalog and its server-provided cache hints.
#[derive(Debug, Clone, Default)]
pub struct McpToolCatalog {
    pub tools: Vec<McpToolDef>,
    pub ttl_ms: Option<u64>,
    pub cache_scope: Option<String>,
}

/// A resource advertised by an external MCP server.
#[derive(Debug, Clone, Serialize)]
pub struct McpResourceDef {
    pub uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

/// Host-implemented bridge to external MCP servers.
///
/// All methods take a `session_key` (the run's job id) so connections are
/// pooled and isolated per agent session; the gateway connects lazily on first
/// use.
///
/// `config` arrives as a [`BrokeredMcpConfig`] rather than an
/// `McpServerConfig` because an expanded configuration carries resolved
/// credentials in `env`, `headers`, `url`, and `args`. The brokered carrier has
/// no `Debug`, no `serde`, and no `Clone`, so a gateway implementation can
/// connect with it but cannot log it, persist it, or hold it in a struct that
/// later gets serialized. Reach the fields with
/// [`BrokeredMcpConfig::resolved`] at the point of connecting, and no earlier.
#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait McpGateway: Send + Sync {
    /// List tools through a machine-scoped placed facade.
    async fn list_placed_tools(
        &self,
        _facade: Arc<dyn PlacedFacade>,
    ) -> Result<McpToolCatalog, String> {
        Err("this host cannot reach placed MCP facades".to_string())
    }

    /// Execute one tool-call round through a machine-scoped placed facade.
    async fn call_placed_tool_once(
        &self,
        _facade: Arc<dyn PlacedFacade>,
        _tool: &str,
        _args: serde_json::Value,
        _timeout_ms: Option<u32>,
    ) -> Result<McpCallOutcome, String> {
        Err("this host cannot reach placed MCP facades".to_string())
    }

    /// Tear down the pooled connection for one placed machine.
    async fn close_placed(&self, _key: &str) {}

    /// List the tools advertised by `server`.
    async fn list_tools(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
    ) -> Result<McpToolCatalog, String>;

    /// List the resources advertised by `server` (empty if unsupported).
    async fn list_resources(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
    ) -> Result<Vec<McpResourceDef>, String>;

    /// Proxy a `resources/read` for an external resource `uri`.
    async fn read_resource(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        uri: &str,
    ) -> Result<String, String>;

    /// Execute exactly one `tools/call` protocol round. The caller, rather than
    /// the transport, owns MRTR's persisted round bound and task lifecycle.
    async fn call_tool_once(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        input_responses: Option<serde_json::Value>,
        request_state: Option<String>,
        timeout_ms: Option<u32>,
        operation_id: Option<&str>,
    ) -> Result<McpCallOutcome, String> {
        let _ = operation_id;
        if input_responses.is_some() || request_state.is_some() {
            return Err("this MCP host does not support continuation rounds".to_string());
        }
        self.call_tool(session_key, server, config, tool, args, timeout_ms)
            .await
            .map(McpCallOutcome::Complete)
    }

    /// Compatibility convenience for callers that only accept a final result.
    async fn call_tool(
        &self,
        session_key: &str,
        server: &str,
        config: &BrokeredMcpConfig,
        tool: &str,
        args: serde_json::Value,
        timeout_ms: Option<u32>,
    ) -> Result<McpToolCallResult, String> {
        match self
            .call_tool_once(
                session_key,
                server,
                config,
                tool,
                args,
                None,
                None,
                timeout_ms,
                None,
            )
            .await?
        {
            McpCallOutcome::Complete(result) => Ok(result),
            McpCallOutcome::InputRequired { .. } => Err("MCP tool requires user input".to_string()),
            McpCallOutcome::Task { task_id, .. } => Err(format!("MCP tool started task {task_id}")),
        }
    }

    async fn get_task(
        &self,
        _session_key: &str,
        _server: &str,
        _config: &BrokeredMcpConfig,
        task_id: &str,
    ) -> Result<McpTaskOutcome, String> {
        Err(format!(
            "MCP task '{task_id}' cannot be polled by this host"
        ))
    }

    async fn update_task(
        &self,
        _session_key: &str,
        _server: &str,
        _config: &BrokeredMcpConfig,
        task_id: &str,
        input_responses: serde_json::Value,
        operation_id: Option<&str>,
    ) -> Result<(), String> {
        let _ = (input_responses, operation_id);
        Err(format!(
            "MCP task '{task_id}' cannot accept input in this host"
        ))
    }

    /// Tear down all pooled connections for a finished session.
    async fn close_session(&self, session_key: &str);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn call_outcomes_round_trip_all_continuation_fields() {
        let outcomes = [
            McpCallOutcome::InputRequired {
                input_requests: serde_json::json!([{"id": "approval", "type": "boolean"}]),
                request_state: Some("round-2".to_string()),
            },
            McpCallOutcome::Task {
                task_id: "task-7".to_string(),
                poll_interval_ms: Some(250),
                ttl_ms: Some(30_000),
            },
        ];

        for outcome in outcomes {
            let json = serde_json::to_value(&outcome).unwrap();
            let decoded: McpCallOutcome = serde_json::from_value(json.clone()).unwrap();
            match decoded {
                McpCallOutcome::InputRequired {
                    input_requests,
                    request_state,
                } => {
                    assert_eq!(json["kind"], "input_required");
                    assert_eq!(input_requests[0]["id"], "approval");
                    assert_eq!(request_state.as_deref(), Some("round-2"));
                }
                McpCallOutcome::Task {
                    task_id,
                    poll_interval_ms,
                    ttl_ms,
                } => {
                    assert_eq!(json["kind"], "task");
                    assert_eq!(task_id, "task-7");
                    assert_eq!(poll_interval_ms, Some(250));
                    assert_eq!(ttl_ms, Some(30_000));
                }
                McpCallOutcome::Complete(_) => panic!("unexpected complete outcome"),
            }
        }
    }

    #[test]
    fn task_outcomes_round_trip_non_terminal_and_terminal_states() {
        let outcomes = [
            McpTaskOutcome::Working {
                poll_interval_ms: Some(500),
            },
            McpTaskOutcome::InputRequired {
                input_requests: serde_json::json!([{"id": "choice"}]),
            },
            McpTaskOutcome::Complete(McpToolCallResult {
                text: "done".to_string(),
                images: vec![],
            }),
            McpTaskOutcome::Failed {
                message: "broken".to_string(),
            },
            McpTaskOutcome::Cancelled,
        ];

        for outcome in outcomes {
            let json = serde_json::to_value(&outcome).unwrap();
            let status = json["status"].as_str().unwrap().to_string();
            let decoded: McpTaskOutcome = serde_json::from_value(json).unwrap();
            match decoded {
                McpTaskOutcome::Working { poll_interval_ms } => {
                    assert_eq!(status, "working");
                    assert_eq!(poll_interval_ms, Some(500));
                }
                McpTaskOutcome::InputRequired { input_requests } => {
                    assert_eq!(status, "input_required");
                    assert_eq!(input_requests[0]["id"], "choice");
                }
                McpTaskOutcome::Complete(result) => {
                    assert_eq!(status, "complete");
                    assert_eq!(result.text, "done");
                }
                McpTaskOutcome::Failed { message } => {
                    assert_eq!(status, "failed");
                    assert_eq!(message, "broken");
                }
                McpTaskOutcome::Cancelled => assert_eq!(status, "cancelled"),
            }
        }
    }
}
