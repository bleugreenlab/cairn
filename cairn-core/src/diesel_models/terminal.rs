//! Job terminal models for Diesel ORM (renamed from Node Terminal)

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = job_terminals)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbJobTerminal {
    pub id: String,
    pub job_id: Option<String>,
    pub project_id: Option<String>,
    pub run_id: Option<String>,
    pub session_id: String,
    pub command: String,
    pub title: Option<String>,
    pub description: Option<String>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub created_at: i32,
    pub exited_at: Option<i32>,
    pub slug: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = job_terminals)]
pub struct NewJobTerminal<'a> {
    pub id: &'a str,
    pub job_id: Option<&'a str>,
    pub project_id: Option<&'a str>,
    pub run_id: Option<&'a str>,
    pub session_id: &'a str,
    pub command: &'a str,
    pub title: Option<&'a str>,
    pub description: Option<&'a str>,
    pub status: &'a str,
    pub exit_code: Option<i32>,
    pub created_at: i32,
    pub exited_at: Option<i32>,
    pub slug: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = job_terminals)]
pub struct UpdateJobTerminalChangeset<'a> {
    pub status: Option<&'a str>,
    pub exit_code: Option<Option<i32>>,
    pub exited_at: Option<Option<i32>>,
}
