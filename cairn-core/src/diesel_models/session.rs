//! Session models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = sessions)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbSession {
    pub id: String,
    pub job_id: Option<String>,
    pub chat_id: Option<String>,
    pub backend: String,
    pub status: String,
    pub parent_session_id: Option<String>,
    pub replaced_by_id: Option<String>,
    pub terminal_reason: Option<String>,
    pub sequence: i32,
    pub created_at: i32,
    pub closed_at: Option<i32>,
    pub updated_at: i32,
    pub backend_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = sessions)]
pub struct NewSession<'a> {
    pub id: &'a str,
    pub job_id: Option<&'a str>,
    pub chat_id: Option<&'a str>,
    pub backend: &'a str,
    pub status: &'a str,
    pub parent_session_id: Option<&'a str>,
    pub replaced_by_id: Option<&'a str>,
    pub terminal_reason: Option<&'a str>,
    pub sequence: i32,
    pub created_at: i32,
    pub closed_at: Option<i32>,
    pub updated_at: i32,
    pub backend_id: Option<&'a str>,
}
