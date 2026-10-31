//! GitHub App and webhook event models for database records

// ============================================================================
// GitHub App models
// ============================================================================

#[derive(Debug)]
pub struct DbGitHubApp {
    pub id: String,
    pub app_id: Option<i32>,
    pub app_name: Option<String>,
    pub app_slug: Option<String>,
    pub private_key: Option<String>,
    pub webhook_secret: Option<String>,
    pub installation_id: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewGitHubApp<'a> {
    pub id: &'a str,
    pub app_id: Option<i32>,
    pub app_name: Option<&'a str>,
    pub app_slug: Option<&'a str>,
    pub private_key: Option<&'a str>,
    pub webhook_secret: Option<&'a str>,
    pub installation_id: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug, Default)]
pub struct UpdateGitHubAppChangeset<'a> {
    pub app_id: Option<Option<i32>>,
    pub app_name: Option<Option<&'a str>>,
    pub app_slug: Option<Option<&'a str>>,
    pub private_key: Option<Option<&'a str>>,
    pub webhook_secret: Option<Option<&'a str>>,
    pub installation_id: Option<Option<i32>>,
    pub updated_at: Option<i32>,
}

// ============================================================================
// GitHub Installation models (multi-installation support)
// ============================================================================

#[derive(Debug)]
pub struct DbGitHubInstallation {
    pub id: String,
    pub account_login: String,
    pub account_type: String,
    pub installation_id: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewGitHubInstallation<'a> {
    pub id: &'a str,
    pub account_login: &'a str,
    pub account_type: &'a str,
    pub installation_id: i32,
    pub created_at: i32,
    pub updated_at: i32,
}

#[derive(Debug)]
pub struct NewWebhookEvent<'a> {
    pub id: &'a str,
    pub event_type: &'a str,
    pub action: &'a str,
    pub repo_full_name: &'a str,
    pub pr_number: Option<i32>,
    pub payload_summary: &'a str,
    pub processed_at: i32,
}
