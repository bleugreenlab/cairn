//! GitHub credential management — pure DB operations.

use crate::schema::{github_app, github_installations};
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// GitHub App credentials needed for API auth.
#[derive(Debug, Clone)]
pub struct GitHubAppCredentials {
    pub app_id: i64,
    pub private_key: String,
    pub installation_id: i64,
}

/// GitHub credentials stored in DB.
#[derive(Debug, Clone, Default)]
pub struct GitHubCredentials {
    pub app_id: Option<i64>,
    pub app_name: Option<String>,
    pub app_slug: Option<String>,
    pub private_key: Option<String>,
    pub webhook_secret: Option<String>,
    pub installation_id: Option<i64>,
    pub relay_channel_id: Option<String>,
    pub relay_secret: Option<String>,
    pub last_event_sync: Option<String>,
    pub relay_public_key: Option<String>,
    pub relay_private_key_encrypted: Option<String>,
}

impl From<crate::diesel_models::DbGitHubApp> for GitHubCredentials {
    fn from(db: crate::diesel_models::DbGitHubApp) -> Self {
        Self {
            app_id: db.app_id.map(|x| x as i64),
            app_name: db.app_name,
            app_slug: db.app_slug,
            private_key: db.private_key,
            webhook_secret: db.webhook_secret,
            installation_id: db.installation_id.map(|x| x as i64),
            relay_channel_id: db.relay_channel_id,
            relay_secret: db.relay_secret,
            last_event_sync: db.last_event_sync,
            relay_public_key: db.relay_public_key,
            relay_private_key_encrypted: db.relay_private_key_encrypted,
        }
    }
}

/// Get GitHub credentials from DB.
pub fn get_github_credentials(conn: &mut SqliteConnection) -> Result<GitHubCredentials, String> {
    github_app::table
        .find("default")
        .first::<crate::diesel_models::DbGitHubApp>(conn)
        .optional()
        .map_err(|e| e.to_string())
        .map(|opt| opt.map(GitHubCredentials::from).unwrap_or_default())
}

/// Get installation ID for a repository owner (user or org).
pub fn get_installation_for_owner(
    conn: &mut SqliteConnection,
    owner: &str,
) -> Result<Option<i64>, String> {
    github_installations::table
        .filter(github_installations::account_login.eq(owner))
        .select(github_installations::installation_id)
        .first::<i32>(conn)
        .optional()
        .map(|opt| opt.map(|id| id as i64))
        .map_err(|e| e.to_string())
}

/// Get GitHub App credentials for a specific repository owner.
///
/// Looks up the installation by owner first, falls back to default installation_id.
pub fn get_credentials_for_owner(
    conn: &mut SqliteConnection,
    owner: &str,
) -> Result<GitHubAppCredentials, String> {
    let creds = get_github_credentials(conn)?;

    let app_id = creds.app_id.ok_or("GitHub App ID not configured")?;
    let private_key = creds
        .private_key
        .ok_or("GitHub App private key not configured")?;

    let installation_id = get_installation_for_owner(conn, owner)?
        .or(creds.installation_id)
        .ok_or_else(|| {
            format!(
                "GitHub App not installed for '{}'. Install the app on this account/org.",
                owner
            )
        })?;

    Ok(GitHubAppCredentials {
        app_id,
        private_key,
        installation_id,
    })
}

/// Get owner/repo from a repository path using git remote.
pub fn get_owner_repo(repo_path: &str) -> Result<(String, String), String> {
    let remote_url = super::api::get_repo_remote(repo_path)?;
    super::api::parse_repo_from_url(&remote_url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diesel_models::{NewGitHubApp, NewGitHubInstallation};
    use crate::test_utils::test_diesel_conn;

    fn insert_github_app(
        conn: &mut SqliteConnection,
        app_id: Option<i32>,
        private_key: Option<&str>,
        installation_id: Option<i32>,
    ) {
        diesel::insert_into(github_app::table)
            .values(&NewGitHubApp {
                id: "default",
                app_id,
                app_name: Some("Test App"),
                app_slug: Some("test-app"),
                private_key,
                webhook_secret: None,
                installation_id,
                relay_channel_id: None,
                relay_secret: None,
                last_event_sync: None,
                created_at: 1000,
                updated_at: 1000,
                relay_public_key: None,
                relay_private_key_encrypted: None,
            })
            .execute(conn)
            .unwrap();
    }

    fn insert_installation(conn: &mut SqliteConnection, owner: &str, installation_id: i32) {
        diesel::insert_into(github_installations::table)
            .values(&NewGitHubInstallation {
                id: &uuid::Uuid::new_v4().to_string(),
                account_login: owner,
                account_type: "Organization",
                installation_id,
                created_at: 1000,
                updated_at: 1000,
            })
            .execute(conn)
            .unwrap();
    }

    #[test]
    fn get_github_credentials_returns_default_when_no_row() {
        let mut conn = test_diesel_conn();
        let creds = get_github_credentials(&mut conn).unwrap();
        assert!(creds.app_id.is_none());
        assert!(creds.private_key.is_none());
        assert!(creds.installation_id.is_none());
    }

    #[test]
    fn get_github_credentials_returns_stored_values() {
        let mut conn = test_diesel_conn();
        insert_github_app(&mut conn, Some(42), Some("pem-data"), Some(100));

        let creds = get_github_credentials(&mut conn).unwrap();
        assert_eq!(creds.app_id, Some(42));
        assert_eq!(creds.private_key.as_deref(), Some("pem-data"));
        assert_eq!(creds.installation_id, Some(100));
    }

    #[test]
    fn get_installation_for_owner_returns_none_when_missing() {
        let mut conn = test_diesel_conn();
        let result = get_installation_for_owner(&mut conn, "unknown-org").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn get_installation_for_owner_returns_matching() {
        let mut conn = test_diesel_conn();
        insert_installation(&mut conn, "my-org", 555);

        let result = get_installation_for_owner(&mut conn, "my-org").unwrap();
        assert_eq!(result, Some(555));
    }

    #[test]
    fn get_credentials_for_owner_uses_owner_installation() {
        let mut conn = test_diesel_conn();
        insert_github_app(&mut conn, Some(10), Some("key"), Some(100));
        insert_installation(&mut conn, "specific-org", 200);

        let creds = get_credentials_for_owner(&mut conn, "specific-org").unwrap();
        // Should use the owner-specific installation, not the default
        assert_eq!(creds.installation_id, 200);
        assert_eq!(creds.app_id, 10);
    }

    #[test]
    fn get_credentials_for_owner_falls_back_to_default() {
        let mut conn = test_diesel_conn();
        insert_github_app(&mut conn, Some(10), Some("key"), Some(100));
        // No installation for "other-org" — should fall back to default 100

        let creds = get_credentials_for_owner(&mut conn, "other-org").unwrap();
        assert_eq!(creds.installation_id, 100);
    }

    #[test]
    fn get_credentials_for_owner_errors_when_no_installation_at_all() {
        let mut conn = test_diesel_conn();
        insert_github_app(&mut conn, Some(10), Some("key"), None);
        // No owner installation and no default installation_id

        let result = get_credentials_for_owner(&mut conn, "some-org");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not installed"));
    }

    #[test]
    fn get_credentials_for_owner_errors_when_no_app_id() {
        let mut conn = test_diesel_conn();
        insert_github_app(&mut conn, None, Some("key"), Some(100));

        let result = get_credentials_for_owner(&mut conn, "org");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("App ID not configured"));
    }

    #[test]
    fn get_credentials_for_owner_errors_when_no_private_key() {
        let mut conn = test_diesel_conn();
        insert_github_app(&mut conn, Some(10), None, Some(100));

        let result = get_credentials_for_owner(&mut conn, "org");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("private key not configured"));
    }
}
