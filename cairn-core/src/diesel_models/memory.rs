//! Memory models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = memories)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbMemory {
    pub id: String,
    pub project_id: Option<String>,
    pub content: String,
    pub confidence: String,
    pub source_issue: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub surfaced_count: i32,
    pub last_surfaced_at: Option<i32>,
    pub active: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = memories)]
pub struct NewMemory<'a> {
    pub id: &'a str,
    pub project_id: Option<&'a str>,
    pub content: &'a str,
    pub confidence: &'a str,
    pub source_issue: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub surfaced_count: i32,
    pub last_surfaced_at: Option<i32>,
    pub active: i32,
}

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = memory_triggers)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbMemoryTrigger {
    pub id: i32,
    pub memory_id: String,
    pub trigger_index: i32,
    pub json_path: String,
    pub pattern: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = memory_triggers)]
pub struct NewMemoryTrigger<'a> {
    pub memory_id: &'a str,
    pub trigger_index: i32,
    pub json_path: &'a str,
    pub pattern: &'a str,
}
