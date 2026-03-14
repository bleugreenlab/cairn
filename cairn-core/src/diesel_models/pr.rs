//! PR data models for Diesel ORM
//!
//! The pr_data table consolidates what was previously split between
//! pr_data (lifecycle tracking) and pr_cache (GitHub API data).

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// PR Data models (consolidated)
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = pr_data)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbPrData {
    pub id: String,
    pub action_run_id: Option<String>,
    pub pr_number: i32,
    pub pr_url: String,
    pub pr_status: String,
    // GitHub API fields
    pub title: Option<String>,
    pub body: Option<String>,
    pub state: Option<String>,
    pub is_draft: Option<i32>,
    pub review_decision: Option<String>,
    pub mergeable: Option<String>,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub checks_status: Option<String>,
    pub checks_json: Option<String>,
    pub fetched_at: Option<i32>,
    // Timestamps
    pub opened_at: Option<i32>,
    pub merged_at: Option<i32>,
    pub closed_at: Option<i32>,
    pub updated_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = pr_data)]
pub struct NewPrData<'a> {
    pub id: &'a str,
    pub action_run_id: Option<&'a str>,
    pub pr_number: i32,
    pub pr_url: &'a str,
    pub pr_status: &'a str,
    // GitHub API fields
    pub title: Option<&'a str>,
    pub body: Option<&'a str>,
    pub state: Option<&'a str>,
    pub is_draft: Option<i32>,
    pub review_decision: Option<&'a str>,
    pub mergeable: Option<&'a str>,
    pub additions: Option<i32>,
    pub deletions: Option<i32>,
    pub checks_status: Option<&'a str>,
    pub checks_json: Option<&'a str>,
    pub fetched_at: Option<i32>,
    // Timestamps
    pub opened_at: Option<i32>,
    pub merged_at: Option<i32>,
    pub closed_at: Option<i32>,
    pub updated_at: i32,
}

/// Changeset for updating PR status and lifecycle timestamps
#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = pr_data)]
pub struct UpdatePrDataChangeset<'a> {
    pub pr_status: Option<&'a str>,
    pub merged_at: Option<Option<i32>>,
    pub closed_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}

/// Changeset for updating GitHub API fields (from webhooks or fetch)
#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = pr_data)]
pub struct UpdatePrDataGitHubChangeset<'a> {
    pub title: Option<Option<&'a str>>,
    pub body: Option<Option<&'a str>>,
    pub state: Option<Option<&'a str>>,
    pub is_draft: Option<Option<i32>>,
    pub review_decision: Option<Option<&'a str>>,
    pub mergeable: Option<Option<&'a str>>,
    pub additions: Option<Option<i32>>,
    pub deletions: Option<Option<i32>>,
    pub checks_status: Option<Option<&'a str>>,
    pub checks_json: Option<Option<&'a str>>,
    pub fetched_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}
