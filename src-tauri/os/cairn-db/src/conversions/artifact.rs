//! Database record conversion into domain values.
use cairn_domain::*;

/// Convert DbArtifact to Artifact
impl TryFrom<crate::db_records::DbArtifact> for Artifact {
    type Error = String;

    fn try_from(db: crate::db_records::DbArtifact) -> Result<Self, Self::Error> {
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
            seen_at: db.seen_at.map(|t| t as i64),
            confirmed: db.confirmed,
        })
    }
}
