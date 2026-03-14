//! Run and event models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// Run models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = runs)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbRun {
    pub id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub job_id: Option<String>,
    pub status: Option<String>,
    pub claude_session_id: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<i32>,
    pub completed_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub todos: Option<String>,
    pub chat_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = runs)]
pub struct NewRun<'a> {
    pub id: &'a str,
    pub issue_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub status: Option<&'a str>,
    pub claude_session_id: Option<&'a str>,
    pub error_message: Option<&'a str>,
    pub started_at: Option<i32>,
    pub completed_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub todos: Option<&'a str>,
    pub chat_id: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = runs)]
pub struct UpdateRunChangeset<'a> {
    pub status: Option<&'a str>,
    pub claude_session_id: Option<Option<&'a str>>,
    pub error_message: Option<Option<&'a str>>,
    pub started_at: Option<Option<i32>>,
    pub completed_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
    pub todos: Option<Option<&'a str>>,
}

// ============================================================================
// Event models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = events)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbEvent {
    pub id: String,
    pub run_id: String,
    pub session_id: Option<String>,
    pub sequence: i32,
    pub timestamp: i32,
    pub event_type: String,
    pub data: String,
    pub parent_tool_use_id: Option<String>,
    pub created_at: i32,
    pub input_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = events)]
pub struct NewEvent<'a> {
    pub id: &'a str,
    pub run_id: &'a str,
    pub session_id: Option<&'a str>,
    pub sequence: i32,
    pub timestamp: i32,
    pub event_type: &'a str,
    pub data: &'a str,
    pub parent_tool_use_id: Option<&'a str>,
    pub created_at: i32,
    pub input_tokens: Option<i32>,
    pub cache_read_tokens: Option<i32>,
    pub cache_create_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
}
