//! Diesel models for the execution_trigger_sources junction table.

use diesel::prelude::*;

use crate::schema::*;

/// A record linking a source job to the execution it triggered.
#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = execution_trigger_sources)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbTriggerSource {
    pub id: String,
    pub source_job_id: String,
    pub triggered_execution_id: String,
    pub created_at: i32,
}

/// Insertable record for execution_trigger_sources.
#[derive(Debug, Insertable)]
#[diesel(table_name = execution_trigger_sources)]
pub struct NewTriggerSource<'a> {
    pub id: &'a str,
    pub source_job_id: &'a str,
    pub triggered_execution_id: &'a str,
    pub created_at: i32,
}
