//! Workspace models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = workspaces)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbWorkspace {
    pub id: String,
    pub name: String,
    pub created_at: i32,
    pub updated_at: i32,
    pub default_model: Option<String>,
    pub system_prompt: Option<String>,
    pub branch_prefix: Option<String>,
    pub max_thinking_tokens: Option<i32>,
    pub merge_type: Option<String>,
    pub pull_on_merge: Option<i32>,
    /// Agent sync preference: None (ask), "auto_update", "always_skip"
    pub agent_sync_preference: Option<String>,
    /// Auto-start agent jobs when they become ready
    pub auto_start_jobs: i32,
    /// Timezone for scheduled recipes (None/"system" = use OS timezone)
    pub timezone: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = workspaces)]
pub struct NewWorkspace<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub created_at: i32,
    pub updated_at: i32,
    pub default_model: Option<&'a str>,
    pub system_prompt: Option<&'a str>,
    pub branch_prefix: Option<&'a str>,
    pub max_thinking_tokens: Option<i32>,
    pub merge_type: Option<&'a str>,
    pub auto_start_jobs: Option<i32>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = workspaces)]
pub struct UpdateWorkspace<'a> {
    pub updated_at: Option<i32>,
    pub default_model: Option<&'a str>,
    pub system_prompt: Option<&'a str>,
    pub branch_prefix: Option<&'a str>,
    pub max_thinking_tokens: Option<Option<i32>>,
    pub merge_type: Option<&'a str>,
    pub pull_on_merge: Option<i32>,
    pub auto_start_jobs: Option<i32>,
    pub timezone: Option<&'a str>,
}
