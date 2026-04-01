//! Manager scope models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = manager_scopes)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbManagerScope {
    pub id: String,
    pub manager_id: String,
    pub project_id: Option<String>,
    pub scope_kind: String,
    pub branch: Option<String>,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = manager_scopes)]
pub struct NewManagerScope<'a> {
    pub id: &'a str,
    pub manager_id: &'a str,
    pub project_id: Option<&'a str>,
    pub scope_kind: &'a str,
    pub branch: Option<&'a str>,
    pub created_at: i32,
}
