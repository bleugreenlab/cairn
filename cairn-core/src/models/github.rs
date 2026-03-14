//! GitHub App types.

use serde::{Deserialize, Serialize};

/// GitHub App settings
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct GitHubApp {
    pub id: String,
    pub app_id: Option<i64>,
    pub app_name: Option<String>,
    pub private_key: Option<String>,
    pub webhook_secret: Option<String>,
    pub smee_url: Option<String>,
    pub installation_id: Option<i64>,
    pub installed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}
