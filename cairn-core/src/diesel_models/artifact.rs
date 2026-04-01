//! Artifact and artifact content models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// Artifact models
// ============================================================================

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = artifacts)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbArtifact {
    pub id: String,
    pub job_id: Option<String>,
    pub artifact_type: String,
    pub schema_version: i32,
    pub data: String, // JSON
    pub version: i32,
    pub parent_version_id: Option<String>,
    pub output_name: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub seen_at: Option<i32>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = artifacts)]
pub struct NewArtifact<'a> {
    pub id: &'a str,
    pub job_id: Option<&'a str>,
    pub artifact_type: &'a str,
    pub schema_version: i32,
    pub data: &'a str,
    pub version: i32,
    pub parent_version_id: Option<&'a str>,
    pub output_name: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = artifacts)]
pub struct UpdateArtifactChangeset<'a> {
    pub data: Option<&'a str>,
    pub version: Option<i32>,
    pub updated_at: Option<i32>,
}

// ============================================================================
// Artifact Content models (execution-time artifact data)
// ============================================================================

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = artifact_content)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbArtifactContent {
    pub id: String,
    pub artifact_node_id: String,
    pub execution_id: String,
    pub job_id: Option<String>,
    pub data: String, // JSON content
    pub version: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = artifact_content)]
pub struct NewArtifactContent<'a> {
    pub id: &'a str,
    pub artifact_node_id: &'a str,
    pub execution_id: &'a str,
    pub job_id: Option<&'a str>,
    pub data: &'a str,
    pub version: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = artifact_content)]
pub struct UpdateArtifactContentChangeset<'a> {
    pub data: Option<&'a str>,
    pub version: Option<i32>,
    pub updated_at: Option<i32>,
}
