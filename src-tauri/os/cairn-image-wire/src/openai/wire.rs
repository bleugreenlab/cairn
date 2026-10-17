//! The OpenAI Images API's reply JSON.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImagesResponse {
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(default)]
    pub data: Vec<ImageData>,
    /// The size every image was served at, when the server says.
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub output_format: Option<String>,
    #[serde(default)]
    pub usage: Option<ImagesUsage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageData {
    #[serde(default)]
    pub b64_json: Option<String>,
    /// Returned instead of `b64_json` by models and servers that default to
    /// URLs.
    #[serde(default)]
    pub url: Option<String>,
    /// The prompt the model actually drew, for models that rewrite it.
    #[serde(default)]
    pub revised_prompt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImagesUsage {
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    #[serde(default)]
    pub total_tokens: Option<u64>,
    #[serde(default)]
    pub input_tokens_details: Option<InputTokensDetails>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTokensDetails {
    #[serde(default)]
    pub text_tokens: Option<u64>,
    #[serde(default)]
    pub image_tokens: Option<u64>,
}
