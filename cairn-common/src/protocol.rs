//! Shared wire format types for MCP callback communication.
//!
//! These types define the HTTP request/response contract between the
//! MCP server binary and the Tauri backend callback server.

use serde::{Deserialize, Serialize};

/// Request from MCP server to Tauri backend (HTTP callback).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackRequest {
    /// Current working directory - fallback for run identification.
    pub cwd: String,
    /// Run ID - preferred method for accurate run identification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Tool name to invoke.
    pub tool: String,
    /// Tool-specific payload.
    pub payload: serde_json::Value,
    /// Tool use ID from MCP protocol - tracks parent for batch_tasks children.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
}

/// Response from Tauri backend to MCP server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackResponse {
    pub result: String,
}
