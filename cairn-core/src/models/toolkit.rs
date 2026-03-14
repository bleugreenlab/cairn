//! Toolkit and MCP server types.

use serde::{Deserialize, Serialize};

/// Tools that are always disallowed (planning mode tools managed by Cairn).
pub const ALWAYS_DISALLOWED_TOOLS: &[&str] = &["EnterPlanMode", "ExitPlanMode"];

/// A toolkit override stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct Toolkit {
    pub id: String,
    pub stage: String, // "plan", "implementation", "chat"
    pub allowed_tools: Option<Vec<String>>,
    pub disallowed_tools: Option<Vec<String>>,
    pub created_at: i64,
    pub updated_at: i64,
}
