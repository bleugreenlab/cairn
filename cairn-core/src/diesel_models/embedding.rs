use diesel::prelude::*;

use crate::schema::event_embeddings;

/// Database model for reading event embeddings.
#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = event_embeddings)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbEventEmbedding {
    pub event_id: String,
    pub embedding: Vec<u8>,
    pub model_name: String,
    pub dimensions: i32,
    pub created_at: i32,
}

/// Database model for inserting event embeddings.
#[derive(Debug, Insertable)]
#[diesel(table_name = event_embeddings)]
pub struct NewEventEmbedding<'a> {
    pub event_id: &'a str,
    pub embedding: &'a [u8],
    pub model_name: &'a str,
    pub dimensions: i32,
    pub created_at: i32,
}
