//! Domain types for servers.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Server {
    pub id: String,
    pub name: String,
    pub url: String,
    pub org_id: Option<String>,

    // Runtime
    pub status: String, // "unknown" | "connected" | "offline" | "error"
    pub version: Option<String>,
    pub error_message: Option<String>,

    // Sync
    pub excluded_project_ids: Vec<String>,

    pub last_seen_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateServerInput {
    pub name: Option<String>,
    pub excluded_project_ids: Option<Vec<String>>,
}
