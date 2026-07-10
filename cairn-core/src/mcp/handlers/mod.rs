//! MCP request handlers organized by domain.
//!
//! Framework-agnostic handler logic. Both Tauri and cairn-server dispatch to these.

pub(crate) mod branch;
pub mod bug_report;
pub mod comments_artifacts;
pub mod executions;
pub mod fence;
pub mod fetch_web;
pub mod files;
pub mod issue_resources;
pub mod issues;
pub mod mcp_resources;
pub mod messages;
pub mod pdf;
pub mod permission;
pub mod planning;
pub mod read;
pub mod repl;
pub mod resources;
pub mod run;
pub(crate) mod run_context;
pub mod search;
pub(crate) mod search_translate;
pub mod search_web;
pub mod skills_resources;
pub mod slug;
pub(crate) mod target;
pub mod terminal;
pub mod warm_search;
pub mod watch;
pub mod web;
pub mod workflows;
pub mod write;

/// Payload for `agent-attention` events.
pub struct AttentionEvent<'a> {
    pub attention_type: &'a str,
    pub project_key: &'a str,
    pub issue_number: Option<i32>,
    pub issue_title: Option<&'a str>,
    pub node_name: Option<&'a str>,
    pub exec_seq: Option<i32>,
    pub tool_name: Option<&'a str>,
}

/// Emit an `agent-attention` event to notify the frontend that user attention is needed.
///
/// Used for: ask_user prompts, permission requests, job completed/failed.
pub fn emit_attention(emitter: &dyn crate::services::EventEmitter, event: &AttentionEvent) {
    let _ = emitter.emit(
        "agent-attention",
        serde_json::json!({
            "type": event.attention_type,
            "projectKey": event.project_key,
            "issueNumber": event.issue_number,
            "issueTitle": event.issue_title,
            "nodeName": event.node_name,
            "execSeq": event.exec_seq,
            "toolName": event.tool_name,
        }),
    );
}

/// Information about a run looked up by worktree path
#[derive(Clone)]
pub struct RunContext {
    pub run_id: String,
    pub job_id: String,
    pub exec_seq: Option<i32>, // Monotonic execution sequence number (for URIs)
    pub issue_id: Option<String>, // Null for project-level runs
    pub issue_number: Option<i32>, // Issue number for building issue keys (e.g., 123 for CAIRN-123)
    pub project_id: String,
    pub project_key: String,
    pub job_name: Option<String>, // Human-readable job name from execution snapshot (e.g., "builder-1")
    pub worktree_path: Option<String>, // The job's worktree checkout when it runs in one; None for no-worktree runs
}

impl RunContext {
    /// Get the issue key (e.g., "CAIRN-123") or None for project-level runs
    #[allow(dead_code)]
    pub fn issue_key(&self) -> Option<String> {
        self.issue_number
            .map(|num| format!("{}-{}", self.project_key, num))
    }
}

/// Minimal project context for external tools (no active run required)
#[derive(Debug)]
pub struct ProjectContext {
    pub project_id: String,
    pub project_key: String,
}

pub fn parse_payload<T>(request: &crate::mcp::types::McpCallbackRequest) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value(request.payload.clone()).map_err(|e| format!("Invalid payload: {e}"))
}

/// Parse issue identifier into optional project key and issue number.
/// Returns (project_key, issue_number) - project_key is None if not specified.
///
/// Supported formats:
/// - "37" -> (None, 37)
/// - "#37" -> (None, 37)
/// - "CAIRN-37" -> (Some("CAIRN"), 37)
pub fn parse_issue_identifier(input: &str) -> Option<(Option<String>, i32)> {
    let trimmed = input.trim();

    // Try direct parse first (e.g., "37")
    if let Ok(n) = trimmed.parse::<i32>() {
        return Some((None, n));
    }

    // Handle "#37" format
    if let Some(stripped) = trimmed.strip_prefix('#') {
        if let Ok(n) = stripped.parse::<i32>() {
            return Some((None, n));
        }
    }

    // Handle "PREFIX-37" format (case-insensitive)
    if let Some(pos) = trimmed.rfind('-') {
        let prefix = &trimmed[..pos];
        if let Ok(n) = trimmed[pos + 1..].parse::<i32>() {
            return Some((Some(prefix.to_uppercase()), n));
        }
    }

    None
}

/// Strip a shell launcher wrapper and return the semantic inner command when possible.
pub fn unwrap_shell_launcher(cmd: &str) -> String {
    let trimmed = cmd.trim();
    let prefixes = [
        "/bin/zsh -lc ",
        "/bin/bash -lc ",
        "/bin/sh -lc ",
        "zsh -lc ",
        "bash -lc ",
        "sh -lc ",
        "/bin/zsh -c ",
        "/bin/bash -c ",
        "/bin/sh -c ",
        "zsh -c ",
        "bash -c ",
        "sh -c ",
    ];

    for prefix in prefixes {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let rest = rest.trim();
            if rest.len() >= 2 {
                if let Some(inner) = rest
                    .strip_prefix('"')
                    .and_then(|s| s.strip_suffix('"'))
                    .map(|s| {
                        let mut out = String::with_capacity(s.len());
                        let mut chars = s.chars();
                        while let Some(ch) = chars.next() {
                            if ch == '\\' {
                                if let Some(next) = chars.next() {
                                    match next {
                                        '\\' | '"' | '$' | '`' => out.push(next),
                                        '\n' => {}
                                        other => {
                                            out.push('\\');
                                            out.push(other);
                                        }
                                    }
                                } else {
                                    out.push('\\');
                                }
                            } else {
                                out.push(ch);
                            }
                        }
                        out
                    })
                {
                    return inner;
                }

                if let Some(inner) = rest
                    .strip_prefix('\'')
                    .and_then(|s| s.strip_suffix('\''))
                    .map(ToOwned::to_owned)
                {
                    return inner;
                }
            }

            return rest.to_string();
        }
    }

    trimmed.to_string()
}

/// Normalize command for matching: strip shell launchers, trim, collapse whitespace.
pub fn normalize_command(cmd: &str) -> String {
    unwrap_shell_launcher(cmd)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}
