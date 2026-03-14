//! GitHub App and webhook event models for Diesel ORM

use diesel::prelude::*;

use crate::schema::*;

// ============================================================================
// GitHub App models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = github_app)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbGitHubApp {
    pub id: String,
    pub app_id: Option<i32>,
    pub app_name: Option<String>,
    pub app_slug: Option<String>,
    pub private_key: Option<String>,
    pub webhook_secret: Option<String>,
    pub installation_id: Option<i32>,
    pub relay_channel_id: Option<String>,
    pub relay_secret: Option<String>,
    pub last_event_sync: Option<String>,
    pub created_at: i32,
    pub updated_at: i32,
    pub relay_public_key: Option<String>,
    pub relay_private_key_encrypted: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = github_app)]
pub struct NewGitHubApp<'a> {
    pub id: &'a str,
    pub app_id: Option<i32>,
    pub app_name: Option<&'a str>,
    pub app_slug: Option<&'a str>,
    pub private_key: Option<&'a str>,
    pub webhook_secret: Option<&'a str>,
    pub installation_id: Option<i32>,
    pub relay_channel_id: Option<&'a str>,
    pub relay_secret: Option<&'a str>,
    pub last_event_sync: Option<&'a str>,
    pub created_at: i32,
    pub updated_at: i32,
    pub relay_public_key: Option<&'a str>,
    pub relay_private_key_encrypted: Option<&'a str>,
}

#[derive(Debug, AsChangeset, Default)]
#[diesel(table_name = github_app)]
pub struct UpdateGitHubAppChangeset<'a> {
    pub app_id: Option<Option<i32>>,
    pub app_name: Option<Option<&'a str>>,
    pub app_slug: Option<Option<&'a str>>,
    pub private_key: Option<Option<&'a str>>,
    pub webhook_secret: Option<Option<&'a str>>,
    pub installation_id: Option<Option<i32>>,
    pub relay_channel_id: Option<Option<&'a str>>,
    pub relay_secret: Option<Option<&'a str>>,
    pub last_event_sync: Option<Option<&'a str>>,
    pub updated_at: Option<i32>,
    pub relay_public_key: Option<Option<&'a str>>,
    pub relay_private_key_encrypted: Option<Option<&'a str>>,
}

// ============================================================================
// GitHub Installation models (multi-installation support)
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = github_installations)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbGitHubInstallation {
    pub id: String,
    pub account_login: String,
    pub account_type: String,
    pub installation_id: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = github_installations)]
pub struct NewGitHubInstallation<'a> {
    pub id: &'a str,
    pub account_login: &'a str,
    pub account_type: &'a str,
    pub installation_id: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

// ============================================================================
// Webhook Event models
// ============================================================================

#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = webhook_events)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
pub struct DbWebhookEvent {
    pub id: String,
    pub event_type: String,
    pub action: String,
    pub repo_full_name: String,
    pub pr_number: Option<i32>,
    pub payload_summary: String,
    pub processed_at: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = webhook_events)]
pub struct NewWebhookEvent<'a> {
    pub id: &'a str,
    pub event_type: &'a str,
    pub action: &'a str,
    pub repo_full_name: &'a str,
    pub pr_number: Option<i32>,
    pub payload_summary: &'a str,
    pub processed_at: i32,
}
