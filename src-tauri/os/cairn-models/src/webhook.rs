use serde::{Deserialize, Serialize};

/// Webhook event for timeline display
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookEvent {
    pub id: String,
    pub event_type: String,
    pub action: String,
    pub repo_full_name: String,
    pub pr_number: Option<i64>,
    pub payload_summary: String,
    pub processed_at: i64,
}
