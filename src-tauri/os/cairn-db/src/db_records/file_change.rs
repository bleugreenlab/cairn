//! File change models for database records
//!
//! Tracks files changed by jobs, captured on PR merge for file-to-issue mapping.

// ============================================================================
// File Change models
// ============================================================================

#[derive(Debug)]
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

#[derive(Debug)]
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
