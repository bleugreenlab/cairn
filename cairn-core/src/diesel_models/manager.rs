//! Manager models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = managers)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbManager {
    pub id: String,
    pub project_id: String,
    pub home_project_id: Option<String>,
    pub scope_kind: String,
    pub name: String,
    pub description: String,
    pub branch: Option<String>,
    pub job_id: Option<String>,
    pub status: String,
    pub current_session_id: Option<String>,
    pub current_turn_id: Option<String>,
    pub last_wake_at: Option<i32>,
    pub last_turn_completed_at: Option<i32>,
    pub last_error: Option<String>,
    pub agent_config_id: Option<String>,
    pub model: Option<String>,
    pub parent_manager_id: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub execution_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = managers)]
pub struct NewManager<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub home_project_id: Option<&'a str>,
    pub scope_kind: &'a str,
    pub name: &'a str,
    pub description: &'a str,
    pub branch: Option<&'a str>,
    pub job_id: Option<&'a str>,
    pub status: &'a str,
    pub current_session_id: Option<&'a str>,
    pub current_turn_id: Option<&'a str>,
    pub last_wake_at: Option<i32>,
    pub last_turn_completed_at: Option<i32>,
    pub last_error: Option<&'a str>,
    pub agent_config_id: Option<&'a str>,
    pub model: Option<&'a str>,
    pub parent_manager_id: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub execution_id: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = managers)]
pub struct UpdateManagerChangeset<'a> {
    pub home_project_id: Option<Option<&'a str>>,
    pub scope_kind: Option<&'a str>,
    pub name: Option<&'a str>,
    pub description: Option<&'a str>,
    pub branch: Option<Option<&'a str>>,
    pub job_id: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub current_session_id: Option<Option<&'a str>>,
    pub current_turn_id: Option<Option<&'a str>>,
    pub last_wake_at: Option<Option<i32>>,
    pub last_turn_completed_at: Option<Option<i32>>,
    pub last_error: Option<Option<&'a str>>,
    pub model: Option<Option<&'a str>>,
    pub agent_config_id: Option<Option<&'a str>>,
    pub updated_at: Option<i32>,
    pub execution_id: Option<Option<&'a str>>,
}
