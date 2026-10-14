//! Artifact types - typed outputs from jobs.

use serde::{Deserialize, Serialize};

/// Artifact - typed, versioned output from an agent job
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Artifact {
    pub id: String,
    pub job_id: Option<String>,
    pub artifact_type: String,
    pub schema_version: i32,
    pub data: serde_json::Value,
    pub version: i32,
    pub parent_version_id: Option<String>,
    pub output_name: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub seen_at: Option<i64>,
    #[serde(default)]
    pub confirmed: bool,
}
