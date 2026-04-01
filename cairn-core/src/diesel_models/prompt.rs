//! Prompt models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = prompts)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbPrompt {
    pub id: String,
    pub run_id: String,
    pub questions: String,
    pub response: Option<String>,
    pub created_at: i32,
    pub answered_at: Option<i32>,
    pub turn_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = prompts)]
pub struct NewPrompt<'a> {
    pub id: &'a str,
    pub run_id: &'a str,
    pub questions: &'a str,
    pub response: Option<&'a str>,
    pub created_at: i32,
    pub answered_at: Option<i32>,
    pub turn_id: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = prompts)]
pub struct UpdatePromptChangeset<'a> {
    pub response: Option<Option<&'a str>>,
    pub answered_at: Option<Option<i32>>,
}
