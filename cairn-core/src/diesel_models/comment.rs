//! Comment models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = comments)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbComment {
    pub id: String,
    pub issue_id: String,
    pub content: String,
    pub source: String,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = comments)]
pub struct NewComment<'a> {
    pub id: &'a str,
    pub issue_id: &'a str,
    pub content: &'a str,
    pub source: &'a str,
    pub created_at: i32,
}
