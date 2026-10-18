//! The `imageGeneration` item: what Codex's built-in image tool
//! (`image_gen.imagegen`) returns when a turn calls it.
//!
//! It arrives as an `item/started` with an empty `result`, then an
//! `item/completed` carrying the image as base64 and the path Codex saved it
//! to under its home (`<CODEX_HOME>/generated_images/<thread>/<call>.png`).
//! A moderation block completes as `failed` with no `failure` and no image;
//! an image quota hit carries `failure`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The item type Codex gives its image tool's calls.
pub const IMAGE_GENERATION_ITEM: &str = "imageGeneration";

/// One completed `imageGeneration` item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageGenerationItem {
    pub id: String,
    /// `completed` or `failed` once the item is done.
    pub status: String,
    /// The prompt the image model drew, as the tool rewrote it.
    #[serde(default)]
    pub revised_prompt: Option<String>,
    /// The image, base64-encoded; empty when none was drawn.
    #[serde(default)]
    pub result: String,
    /// Where Codex saved the image on the app-server's machine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saved_path: Option<String>,
    #[serde(default)]
    pub failure: Option<ImageGenerationFailure>,
}

/// Why the image tool drew nothing, when Codex says. Tagged by `type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ImageGenerationFailure {
    /// The account's image allowance is spent until `resets_at` (Unix
    /// seconds).
    #[serde(rename_all = "camelCase")]
    UsageLimitExceeded {
        limit_id: String,
        #[serde(default)]
        resets_at: Option<i64>,
    },
    /// A failure kind this crate does not model yet.
    #[serde(other)]
    Other,
}

impl ImageGenerationItem {
    /// Read `item` (an `item/completed` or `item/started` payload) as an
    /// image generation, or `None` when it is another kind of item or does
    /// not parse as one.
    pub fn parse(item: &Value) -> Option<Self> {
        if item.get("type").and_then(Value::as_str) != Some(IMAGE_GENERATION_ITEM) {
            return None;
        }
        serde_json::from_value(item.clone()).ok()
    }

    pub fn is_completed(&self) -> bool {
        self.status == "completed"
    }
}
