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

/// Status of the GitHub App connection.
///
/// Shared between Tauri and cairn-server so both serialize the same shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitHubStatus {
    pub app_authorization: GitHubAppAuthorizationStatus,
    pub event_delivery: EventDeliveryStatus,
}

/// Locally usable GitHub App authorization. This is independent of installations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitHubAppAuthorizationStatus {
    pub authorized: bool,
    pub app_name: Option<String>,
    pub app_slug: Option<String>,
}

/// Observable facts about the per-device event-delivery channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventDeliveryStatus {
    pub configured: bool,
    pub last_event_sync: Option<String>,
    pub health_state: String,
    pub health_reason: Option<String>,
    pub first_failure_at: Option<String>,
    pub last_failure_at: Option<String>,
    pub consecutive_failures: i64,
    pub failing_event_id: Option<String>,
    pub failing_event_at: Option<String>,
    pub last_successful_delivery_at: Option<String>,
}
