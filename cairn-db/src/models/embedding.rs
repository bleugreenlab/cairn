use serde::{Deserialize, Serialize};

/// An embedding vector associated with an event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventEmbedding {
    event_id: String,
    embedding: Vec<f32>,
    model_name: String,
    dimensions: i32,
    created_at: i64,
}
