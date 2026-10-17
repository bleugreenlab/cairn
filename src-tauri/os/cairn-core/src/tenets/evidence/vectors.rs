//! Tenet vectors: what a tenet embeds as, and loading the vectors a project's
//! tenets and a thread's work already have.
//!
//! Vectors live in the private database's `resource_embeddings`, keyed by
//! canonical URI, like every other corpus vector; each machine holds its own
//! copy. A tenet's text is immutable once approved, so its vector never goes
//! stale, and a superseded tenet drops out through its status.

use std::collections::HashMap;

use crate::embeddings::{COHERE_DIMS, COHERE_MODEL};
use crate::storage::{DbResult, LocalDb};
use crate::tenets::TenetView;

/// What a tenet embeds as: the adopted text, then the question it answered.
pub fn tenet_embed_text(view: &TenetView) -> String {
    let text = view.text.as_deref().unwrap_or(&view.row.question);
    format!("{text}\n\n{}", view.row.question)
}

/// The URI prefix every tenet of a project shares.
pub fn project_tenet_prefix(project_key: &str) -> String {
    format!(
        "{}/",
        cairn_common::uri::build_project_tenets_uri(project_key)
    )
}

/// Every stored tenet vector of a project, by tenet URI.
pub async fn project_tenet_vectors(
    vectors: &LocalDb,
    project_key: &str,
) -> DbResult<HashMap<String, Vec<f32>>> {
    Ok(
        crate::embeddings::queries::resource_embeddings_with_prefix_async(
            vectors,
            &project_tenet_prefix(project_key),
            COHERE_MODEL,
            COHERE_DIMS as i32,
        )
        .await?
        .into_iter()
        .collect(),
    )
}

/// One resource's stored vector, when it was embedded by the current model at
/// the current width.
pub async fn resource_vector(vectors: &LocalDb, uri: &str) -> DbResult<Option<Vec<f32>>> {
    Ok(
        crate::embeddings::queries::get_resource_embedding_async(vectors, uri)
            .await?
            .filter(|record| record.model == COHERE_MODEL && record.dims == COHERE_DIMS as i32)
            .map(|record| crate::embeddings::vector::from_bytes(&record.embedding)),
    )
}

/// The database holding resource vectors for rows read from `db`: a team
/// replica's private database, or `db` itself when it is the private one.
pub fn vector_db(db: &LocalDb) -> &LocalDb {
    db.private_route_db().map(|db| db.as_ref()).unwrap_or(db)
}
