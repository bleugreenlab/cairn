//! Message models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = messages)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbMessage {
    pub id: String,
    pub channel_type: String,
    pub channel_id: Option<String>,
    pub sender_run_id: Option<String>,
    pub sender_name: String,
    pub recipient_run_id: Option<String>,
    pub recipient_manager_id: Option<String>,
    pub content: String,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = messages)]
pub struct NewMessage<'a> {
    pub id: &'a str,
    pub channel_type: &'a str,
    pub channel_id: Option<&'a str>,
    pub sender_run_id: Option<&'a str>,
    pub sender_name: &'a str,
    pub recipient_run_id: Option<&'a str>,
    pub recipient_manager_id: Option<&'a str>,
    pub content: &'a str,
    pub created_at: i32,
}
