//! File change models for Diesel ORM
//!
//! Tracks files changed by jobs, captured on PR merge for file-to-issue mapping.

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// File Change models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = file_changes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbFileChange {
    pub id: String,
    pub job_id: String,
    pub file_path: String,
    pub status: String,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub previous_path: Option<String>,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = file_changes)]
pub struct NewFileChange<'a> {
    pub id: &'a str,
    pub job_id: &'a str,
    pub file_path: &'a str,
    pub status: &'a str,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub previous_path: Option<&'a str>,
    pub created_at: i32,
}
