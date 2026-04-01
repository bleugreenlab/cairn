//! Manager mailbox models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = manager_mailbox)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbManagerMailboxEntry {
    pub id: String,
    pub manager_id: String,
    pub cause_type: String,
    pub cause_json: String,
    pub delivery_policy: String,
    pub dedupe_key: Option<String>,
    pub priority: i32,
    pub available_at: i32,
    pub created_at: i32,
    pub claimed_at: Option<i32>,
    pub processed_at: Option<i32>,
    pub superseded_by: Option<String>,
    pub source_run_id: Option<String>,
    pub source_issue_id: Option<String>,
    pub source_project_id: Option<String>,
    pub wake_batch_id: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = manager_mailbox)]
pub struct NewManagerMailboxEntry<'a> {
    pub id: &'a str,
    pub manager_id: &'a str,
    pub cause_type: &'a str,
    pub cause_json: &'a str,
    pub delivery_policy: &'a str,
    pub dedupe_key: Option<&'a str>,
    pub priority: i32,
    pub available_at: i32,
    pub created_at: i32,
    pub claimed_at: Option<i32>,
    pub processed_at: Option<i32>,
    pub superseded_by: Option<&'a str>,
    pub source_run_id: Option<&'a str>,
    pub source_issue_id: Option<&'a str>,
    pub source_project_id: Option<&'a str>,
    pub wake_batch_id: Option<&'a str>,
}

#[derive(Debug, Default, AsChangeset)]
#[diesel(table_name = manager_mailbox)]
pub struct UpdateManagerMailboxEntryChangeset<'a> {
    pub claimed_at: Option<Option<i32>>,
    pub processed_at: Option<Option<i32>>,
    pub superseded_by: Option<Option<&'a str>>,
    pub wake_batch_id: Option<Option<&'a str>>,
}
