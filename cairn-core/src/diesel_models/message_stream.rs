use diesel::prelude::*;

use crate::schema::{message_stream_chunks, message_streams};

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = message_streams)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbMessageStream {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub backend: String,
    pub sequence: i32,
    pub status: String,
    pub version: i32,
    pub content_chars: i32,
    pub thinking_chars: i32,
    pub chunk_count: i32,
    pub final_event_id: Option<String>,
    pub abort_reason: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub finalized_at: Option<i32>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = message_streams)]
pub struct NewMessageStream<'a> {
    pub id: &'a str,
    pub run_id: &'a str,
    pub session_id: Option<&'a str>,
    pub turn_id: Option<&'a str>,
    pub backend: &'a str,
    pub sequence: i32,
    pub status: &'a str,
    pub version: i32,
    pub content_chars: i32,
    pub thinking_chars: i32,
    pub chunk_count: i32,
    pub final_event_id: Option<&'a str>,
    pub abort_reason: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub finalized_at: Option<i32>,
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = message_stream_chunks)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbMessageStreamChunk {
    pub id: String,
    pub stream_id: String,
    pub kind: String,
    pub chunk_index: i32,
    pub data: String,
    pub char_count: i32,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = message_stream_chunks)]
pub struct NewMessageStreamChunk<'a> {
    pub id: &'a str,
    pub stream_id: &'a str,
    pub kind: &'a str,
    pub chunk_index: i32,
    pub data: &'a str,
    pub char_count: i32,
    pub created_at: i32,
}
