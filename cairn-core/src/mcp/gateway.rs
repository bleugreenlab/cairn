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
use serde::{Deserialize, Serialize};

use crate::config::mcp_servers::McpServerConfig;

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
/// pooled and isolated per agent session. `config` is the already
/// env-expanded server configuration resolved from workspace + project
/// settings; the gateway connects lazily on first use.
#[async_trait]
pub trait McpGateway: Send + Sync {
    /// List the tools advertised by `server`.
    async fn list_tools(
        &self,
        session_key: &str,
        server: &str,
        config: &McpServerConfig,
    ) -> Result<Vec<McpToolDef>, String>;

    /// List the resources advertised by `server` (empty if unsupported).
    async fn list_resources(
        &self,
        session_key: &str,
        server: &str,
        config: &McpServerConfig,
    ) -> Result<Vec<McpResourceDef>, String>;

    /// Proxy a `resources/read` for an external resource `uri`.
    async fn read_resource(
        &self,
        session_key: &str,
        server: &str,
        config: &McpServerConfig,
        uri: &str,
    ) -> Result<String, String>;

    /// Proxy a `tools/call`. `args` is the tool's argument object. Returns the
    /// composed textual result.
    async fn call_tool(
        &self,
        session_key: &str,
        server: &str,
        config: &McpServerConfig,
        tool: &str,
        args: serde_json::Value,
        timeout_ms: Option<u32>,
    ) -> Result<String, String>;

    /// Tear down all pooled connections for a finished session.
    async fn close_session(&self, session_key: &str);
}
