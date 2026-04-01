//! Chat models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = chats)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbChat {
    pub id: String,
    pub project_id: String,
    pub current_session_id: Option<String>,
    pub status: String,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = chats)]
pub struct NewChat<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub current_session_id: Option<&'a str>,
    pub status: &'a str,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = chats)]
pub struct UpdateChatChangeset<'a> {
    pub current_session_id: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub updated_at: Option<i32>,
}
