//! Issue models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = issues)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbIssue {
    pub id: String,
    pub project_id: String,
    pub number: i32,
    pub title: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: Option<i32>,
    pub completed_at: Option<i32>,
    pub dismissed_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub wait_state: Option<String>,
    pub model: Option<String>,
    pub skills: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = issues)]
pub struct NewIssue<'a> {
    pub id: &'a str,
    pub project_id: &'a str,
    pub number: i32,
    pub title: &'a str,
    pub description: Option<&'a str>,
    pub status: &'a str,
    pub priority: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
    pub model: Option<&'a str>,
    pub skills: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = issues)]
pub struct UpdateIssueChangeset<'a> {
    pub title: Option<&'a str>,
    pub description: Option<Option<&'a str>>,
    pub status: Option<&'a str>,
    pub priority: Option<i32>,
    pub completed_at: Option<Option<i32>>,
    pub dismissed_at: Option<Option<i32>>,
    pub updated_at: Option<i32>,
    pub wait_state: Option<Option<&'a str>>,
    pub model: Option<Option<&'a str>>,
    pub skills: Option<Option<&'a str>>,
}
