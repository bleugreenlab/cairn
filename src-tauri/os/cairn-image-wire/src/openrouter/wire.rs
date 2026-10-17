//! OpenRouter's image JSON, as the provider sends it.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImagesResponse {
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(default)]
    pub data: Vec<ImageData>,
    #[serde(default)]
    pub usage: Option<ImagesUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageData {
    pub b64_json: String,
    /// Present whenever OpenRouter could identify the format.
    #[serde(default)]
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImagesUsage {
    #[serde(default)]
    pub prompt_tokens: Option<u64>,
    #[serde(default)]
    pub completion_tokens: Option<u64>,
    #[serde(default)]
    pub total_tokens: Option<u64>,
    /// What the call cost in USD.
    #[serde(default)]
    pub cost: Option<f64>,
}
