//! Doc reference models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = doc_references)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbDocReference {
    pub id: String,
    pub issue_id: String,
    pub doc_path: String,
    pub created_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = doc_references)]
pub struct NewDocReference<'a> {
    pub id: &'a str,
    pub issue_id: &'a str,
    pub doc_path: &'a str,
    pub created_at: i32,
}
