//! Manager wake batch models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = manager_wake_batches)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbManagerWakeBatch {
    pub id: String,
    pub manager_id: String,
    pub created_at: i32,
    pub completed_at: Option<i32>,
    pub outcome: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = manager_wake_batches)]
pub struct NewManagerWakeBatch<'a> {
    pub id: &'a str,
    pub manager_id: &'a str,
    pub created_at: i32,
    pub completed_at: Option<i32>,
    pub outcome: Option<&'a str>,
}

#[derive(Debug, Default, AsChangeset)]
#[diesel(table_name = manager_wake_batches)]
pub struct UpdateManagerWakeBatchChangeset<'a> {
    pub completed_at: Option<Option<i32>>,
    pub outcome: Option<Option<&'a str>>,
}
