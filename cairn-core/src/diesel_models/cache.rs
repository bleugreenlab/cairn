//! Checkpoint command cache and CI logs cache models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// Checkpoint Command Cache models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = checkpoint_command_cache)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbCheckpointCommandCache {
    pub id: String,
    pub job_id: String,
    pub command: String,
    pub normalized_command: String,
    pub exit_code: i32,
    pub commit_sha: String,
    pub is_dirty: i32,
    pub ran_at: i32,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = checkpoint_command_cache)]
pub struct NewCheckpointCommandCache<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub command: &'a str,
    pub normalized_command: &'a str,
    pub exit_code: i32,
    pub commit_sha: &'a str,
    pub is_dirty: i32,
    pub ran_at: i32,
    pub created_at: i32,
}

// ============================================================================
// CI Logs Cache models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = ci_logs_cache)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbCiLogsCache {
    pub id: i32,
    pub run_id: i32,
    pub job_name: String,
    pub log_content: Option<String>,
    pub fetched_at: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = ci_logs_cache)]
pub struct NewCiLogsCache<'a> {
    pub run_id: i32,
    pub job_name: &'a str,
    pub log_content: Option<&'a str>,
    pub fetched_at: &'a str,
}
