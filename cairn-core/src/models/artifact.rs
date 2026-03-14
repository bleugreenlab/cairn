//! Artifact types - typed outputs from jobs.

use serde::{Deserialize, Serialize};

/// Input for adding annotations to an artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnnotationInput {
    pub source_text: String,
    pub note: String,
    pub annotation_type: String, // "correction", "question", "context"
    #[serde(default)] // Backward compat: missing = None
    pub char_offset: Option<i32>,
}

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
}

/// Convert DbArtifact to Artifact
impl TryFrom<crate::diesel_models::DbArtifact> for Artifact {
    type Error = String;

    fn try_from(db: crate::diesel_models::DbArtifact) -> Result<Self, Self::Error> {
        let data: serde_json::Value = serde_json::from_str(&db.data)
            .map_err(|e| format!("Invalid artifact data JSON: {}", e))?;

        Ok(Artifact {
            id: db.id,
            job_id: db.job_id,
            artifact_type: db.artifact_type,
            schema_version: db.schema_version,
            data,
            version: db.version,
            parent_version_id: db.parent_version_id,
            output_name: db.output_name,
            created_at: db.created_at as i64,
            updated_at: db.updated_at as i64,
        })
    }
}
