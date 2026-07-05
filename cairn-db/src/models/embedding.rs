use serde::{Deserialize, Serialize};

/// An embedding vector associated with an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEmbedding {
    pub event_id: String,
    pub embedding: Vec<f32>,
    pub model_name: String,
    pub dimensions: i32,
    pub created_at: i64,
}
