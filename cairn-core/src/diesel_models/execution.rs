//! Execution models for Diesel ORM (renamed from RecipeExecution)

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = executions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbExecution {
    pub id: String,
    pub recipe_id: String,
    pub issue_id: Option<String>,
    pub project_id: Option<String>,
    pub status: String,
    pub started_at: i32,
    pub completed_at: Option<i32>,
    pub snapshot: Option<String>,
    pub seq: Option<i32>,
    pub initiator_sub: Option<String>,
    pub initiator_auth_mode: Option<String>,
    pub initiator_org_id: Option<String>,
    pub triggered_by: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = executions)]
pub struct NewExecution<'a> {
    pub id: &'a str,
    pub recipe_id: &'a str,
    pub issue_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub status: &'a str,
    pub started_at: i32,
    pub completed_at: Option<i32>,
    pub snapshot: Option<&'a str>,
    pub seq: Option<i32>,
    pub initiator_sub: Option<&'a str>,
    pub initiator_auth_mode: Option<&'a str>,
    pub initiator_org_id: Option<&'a str>,
    pub triggered_by: &'a str,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = executions)]
pub struct UpdateExecutionChangeset {
    pub status: Option<String>,
    pub completed_at: Option<Option<i32>>,
    pub snapshot: Option<String>,
}
