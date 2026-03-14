//! Project types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Project {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub key: String,
    pub repo_path: String,
    pub context: String,
    pub docs_enabled: bool,
    pub default_branch: String,
    pub next_issue_number: i32,
    pub setup_commands: Option<String>,
    pub copy_files: Option<String>,
    pub terminal_commands: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    /// When set, this project is a remote bookmark pointing to a cairn-server instance.
    pub remote_url: Option<String>,
    /// API key for authenticating with the remote server.
    pub remote_api_key: Option<String>,
}

/// Terminal shortcut command configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminalCommand {
    pub name: String,
    pub command: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProject {
    pub name: String,
    pub key: String,
    pub repo_path: String,
    /// When set, creates a remote project bookmark instead of a local project.
    pub remote_url: Option<String>,
    /// API key for authenticating with the remote server.
    pub remote_api_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateProject {
    pub id: String,
    pub setup_commands: Option<Vec<String>>,
    pub copy_files: Option<Vec<String>>,
    pub terminal_commands: Option<Vec<TerminalCommand>>,
}
