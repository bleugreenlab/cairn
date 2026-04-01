//! Domain types for servers.

use crate::schema::servers;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

// ============================================================================
// API types (serialized to/from frontend)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Server {
    pub id: String,
    pub name: String,
    pub url: String,
    pub org_id: Option<String>,

    // Runtime
    pub status: String, // "unknown" | "connected" | "offline" | "error"
    pub version: Option<String>,
    pub error_message: Option<String>,

    // Sync
    pub excluded_project_ids: Vec<String>,

    pub last_seen_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateServerInput {
    pub name: Option<String>,
    pub excluded_project_ids: Option<Vec<String>>,
}

// ============================================================================
// Diesel model
// ============================================================================

#[derive(Debug, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = servers)]
pub struct DbServer {
    pub id: String,
    pub name: String,
    pub url: String,
    pub org_id: Option<String>,
    pub status: String,
    pub version: Option<String>,
    pub error_message: Option<String>,
    pub excluded_project_ids: Option<String>,
    pub last_seen_at: Option<i32>,
    pub created_at: i32,
    pub updated_at: i32,
}

impl From<DbServer> for Server {
    fn from(db: DbServer) -> Self {
        let excluded: Vec<String> = db
            .excluded_project_ids
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();

        Server {
            id: db.id,
            name: db.name,
            url: db.url,
            org_id: db.org_id,
            status: db.status,
            version: db.version,
            error_message: db.error_message,
            excluded_project_ids: excluded,
            last_seen_at: db.last_seen_at.map(|v| v as i64),
            created_at: db.created_at as i64,
            updated_at: db.updated_at as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_db_server() -> DbServer {
        DbServer {
            id: "srv-1".to_string(),
            name: "Test".to_string(),
            url: "http://example.com".to_string(),
            org_id: None,
            status: "unknown".to_string(),
            version: None,
            error_message: None,
            excluded_project_ids: None,
            last_seen_at: None,
            created_at: 1000,
            updated_at: 2000,
        }
    }

    #[test]
    fn conversion_maps_basic_fields() {
        let db = base_db_server();
        let server = Server::from(db);
        assert_eq!(server.id, "srv-1");
        assert_eq!(server.name, "Test");
        assert_eq!(server.url, "http://example.com");
        assert_eq!(server.status, "unknown");
        assert_eq!(server.created_at, 1000);
        assert_eq!(server.updated_at, 2000);
    }

    #[test]
    fn conversion_excluded_project_ids_parses_json() {
        let db = DbServer {
            excluded_project_ids: Some(r#"["a","b","c"]"#.to_string()),
            ..base_db_server()
        };
        let server = Server::from(db);
        assert_eq!(server.excluded_project_ids, vec!["a", "b", "c"]);
    }

    #[test]
    fn conversion_excluded_project_ids_empty_when_null() {
        let db = DbServer {
            excluded_project_ids: None,
            ..base_db_server()
        };
        let server = Server::from(db);
        assert!(server.excluded_project_ids.is_empty());
    }

    #[test]
    fn conversion_excluded_project_ids_empty_on_invalid_json() {
        let db = DbServer {
            excluded_project_ids: Some("not json".to_string()),
            ..base_db_server()
        };
        let server = Server::from(db);
        assert!(server.excluded_project_ids.is_empty());
    }

    #[test]
    fn conversion_last_seen_at_none_when_null() {
        let db = DbServer {
            last_seen_at: None,
            ..base_db_server()
        };
        assert!(Server::from(db).last_seen_at.is_none());
    }

    #[test]
    fn conversion_last_seen_at_widens_to_i64() {
        let db = DbServer {
            last_seen_at: Some(1710000000),
            ..base_db_server()
        };
        assert_eq!(Server::from(db).last_seen_at, Some(1710000000i64));
    }
}
