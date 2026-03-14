//! Artifact annotation operations (mutating).

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use crate::diesel_models::DbArtifact;
use crate::models::{AnnotationInput, Artifact};
use crate::schema::artifacts;
use crate::services::Clock;

/// Add annotations to an artifact's data.annotations array.
pub fn add(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    artifact_id: &str,
    annotations: Vec<AnnotationInput>,
) -> Result<Artifact, String> {
    let db_artifact: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Artifact not found: {}", e))?;

    let mut data: serde_json::Value = serde_json::from_str(&db_artifact.data)
        .map_err(|e| format!("Invalid artifact data JSON: {}", e))?;

    let annotations_with_ids: Vec<serde_json::Value> = annotations
        .into_iter()
        .map(|a| {
            serde_json::json!({
                "id": uuid::Uuid::new_v4().to_string(),
                "sourceText": a.source_text,
                "note": a.note,
                "annotationType": a.annotation_type,
                "charOffset": a.char_offset,
            })
        })
        .collect();

    if let Some(obj) = data.as_object_mut() {
        let existing = obj
            .entry("annotations")
            .or_insert_with(|| serde_json::json!([]));
        if let Some(arr) = existing.as_array_mut() {
            arr.extend(annotations_with_ids);
        }
    }

    let now = clock.now() as i32;
    let data_str =
        serde_json::to_string(&data).map_err(|e| format!("Failed to serialize data: {}", e))?;

    diesel::update(artifacts::table.find(artifact_id))
        .set((artifacts::data.eq(&data_str), artifacts::updated_at.eq(now)))
        .execute(conn)
        .map_err(|e| format!("Failed to update artifact: {}", e))?;

    let updated: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Failed to fetch updated artifact: {}", e))?;

    Artifact::try_from(updated).map_err(|e| format!("Failed to parse artifact: {}", e))
}

/// Delete an annotation from an artifact by ID.
pub fn delete(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    artifact_id: &str,
    annotation_id: &str,
) -> Result<Artifact, String> {
    let db_artifact: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Artifact not found: {}", e))?;

    let mut data: serde_json::Value = serde_json::from_str(&db_artifact.data)
        .map_err(|e| format!("Invalid artifact data JSON: {}", e))?;

    if let Some(obj) = data.as_object_mut() {
        if let Some(annotations) = obj.get_mut("annotations").and_then(|v| v.as_array_mut()) {
            annotations.retain(|a| {
                !matches!(a.get("id").and_then(|id| id.as_str()), Some(id) if id == annotation_id)
            });
        }
    }

    let now = clock.now() as i32;
    let data_str =
        serde_json::to_string(&data).map_err(|e| format!("Failed to serialize data: {}", e))?;

    diesel::update(artifacts::table.find(artifact_id))
        .set((artifacts::data.eq(&data_str), artifacts::updated_at.eq(now)))
        .execute(conn)
        .map_err(|e| format!("Failed to update artifact: {}", e))?;

    let updated: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Failed to fetch updated artifact: {}", e))?;

    Artifact::try_from(updated).map_err(|e| format!("Failed to parse artifact: {}", e))
}

/// Update an annotation's note by ID.
pub fn update(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    artifact_id: &str,
    annotation_id: &str,
    new_note: &str,
) -> Result<Artifact, String> {
    let db_artifact: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Artifact not found: {}", e))?;

    let mut data: serde_json::Value = serde_json::from_str(&db_artifact.data)
        .map_err(|e| format!("Invalid artifact data JSON: {}", e))?;

    let mut found = false;
    if let Some(obj) = data.as_object_mut() {
        if let Some(annotations) = obj.get_mut("annotations").and_then(|v| v.as_array_mut()) {
            for annotation in annotations {
                if matches!(
                    annotation.get("id").and_then(|id| id.as_str()),
                    Some(id) if id == annotation_id
                ) {
                    if let Some(note_field) = annotation.get_mut("note") {
                        *note_field = serde_json::json!(new_note);
                        found = true;
                        break;
                    }
                }
            }
        }
    }

    if !found {
        return Err(format!("Annotation {} not found", annotation_id));
    }

    let now = clock.now() as i32;
    let data_str =
        serde_json::to_string(&data).map_err(|e| format!("Failed to serialize data: {}", e))?;

    diesel::update(artifacts::table.find(artifact_id))
        .set((artifacts::data.eq(&data_str), artifacts::updated_at.eq(now)))
        .execute(conn)
        .map_err(|e| format!("Failed to update artifact: {}", e))?;

    let updated: DbArtifact = artifacts::table
        .find(artifact_id)
        .first(conn)
        .map_err(|e| format!("Failed to fetch updated artifact: {}", e))?;

    Artifact::try_from(updated).map_err(|e| format!("Failed to parse artifact: {}", e))
}
