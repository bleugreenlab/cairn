//! Shared wire format types for MCP callback communication.
//!
//! These types define the HTTP request/response contract between the
//! MCP server binary and the Tauri backend callback server.

use serde::{Deserialize, Serialize};

/// Request from MCP server to Tauri backend (HTTP callback).
///
/// Derives `Default` so adding an optional field never ripples across the ~70
/// struct-literal construction sites: existing literals keep compiling, and new
/// optional fields can be filled in only where they matter via `..Default::default()`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Codex pooled-app-server thread id (CAIRN-2549). Set by `cairn-cmd` from
    /// the `tools/call` request's `_meta.threadId` when Codex injects it (one
    /// shared app-server hosts N ephemeral call threads). The host maps this
    /// thread id back to the owning run, overriding the process-global
    /// `CAIRN_RUN_ID`/`cwd`, so each pooled call's tool results and `cairn:~/`
    /// targets attribute to the right run. `None` for every non-pooled caller
    /// (Claude, CLI, SDK, and Codex node/task sessions), which is unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
}

/// Response from Tauri backend to MCP server.
///
/// `result` is the handler's output and is NEVER mutated by the augmentation
/// layer. System-reminder augmentation (queued direct messages, the
/// dirty-worktree notice) rides separately in `reminders` as data, so a handler
/// that returns structured JSON (e.g. the read-batch envelope, a change report)
/// stays parseable end-to-end. The CLI assembles the model-visible text at the
/// transport edge: `result` first, then each reminder wrapped in a
/// `<system-reminder>` block, in delivery order.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CallbackResponse {
    pub result: String,
    /// Cairn artifact URI for this task's output, if available.
    /// Set by the task handler so batch_tasks can surface it in truncation messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_uri: Option<String>,
    /// `<system-reminder>` bodies to append after `result`, in delivery order
    /// (queued DMs, then the dirty-worktree notice). Each entry is the inner
    /// text only; the CLI wraps it in the `<system-reminder>` envelope.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reminders: Vec<String>,
}
