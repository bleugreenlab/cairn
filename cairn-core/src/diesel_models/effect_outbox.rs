//! Effect outbox models for Diesel ORM.
//!
//! Durable outbox entries for crash-safe effect replay.

use diesel::prelude::*;

use crate::schema::*;

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = effect_outbox)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbEffectOutbox {
    pub id: String,
    pub kind: String,
    pub dedupe_key: String,
    pub payload_json: String,
    pub state: String,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = effect_outbox)]
pub struct NewEffectOutbox<'a> {
    pub id: &'a str,
    pub kind: &'a str,
    pub dedupe_key: &'a str,
    pub payload_json: &'a str,
    pub state: &'a str,
    pub created_at: i32,
    pub updated_at: i32,
}
