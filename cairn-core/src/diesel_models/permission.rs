//! Permission request models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = permission_requests)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbPermissionRequest {
    pub id: String,
    pub run_id: String,
    pub tool_use_id: String,
    pub tool_name: String,
    pub tool_input: String,
    pub status: String,
    pub response: Option<String>,
    pub created_at: i32,
    pub responded_at: Option<i32>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = permission_requests)]
pub struct NewPermissionRequest<'a> {
    pub id: &'a str,
    pub run_id: &'a str,
    pub tool_use_id: &'a str,
    pub tool_name: &'a str,
    pub tool_input: &'a str,
    pub status: &'a str,
    pub created_at: i32,
    pub turn_id: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = permission_requests)]
pub struct UpdatePermissionRequestChangeset<'a> {
    pub status: Option<&'a str>,
    pub response: Option<Option<&'a str>>,
    pub responded_at: Option<Option<i32>>,
}
