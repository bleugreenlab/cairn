//! Domain types for server deployments.

use crate::schema::server_deployments;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

// ============================================================================
// API types (serialized to/from frontend)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerDeployment {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: i32,
    pub user: String,
    pub ssh_key_path: Option<String>,
    pub container_name: String,
    pub api_key: String,
    pub server_port: i32,
    pub status: String,
    pub claude_authenticated: bool,
    pub error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployServerInput {
    pub name: String,
    pub host: String,
    pub port: Option<i32>,
    pub user: String,
    pub ssh_key_path: Option<String>,
    pub server_port: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaudeAuthSession {
    pub status: String,
    pub oauth_url: Option<String>,
    pub error_message: Option<String>,
}

// ============================================================================
// Diesel model
// ============================================================================

#[derive(Debug, Queryable, Selectable, Insertable, AsChangeset)]
#[diesel(table_name = server_deployments)]
pub struct DbServerDeployment {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: i32,
    pub user: String,
    pub ssh_key_path: Option<String>,
    pub container_name: String,
    pub api_key: String,
    pub server_port: i32,
    pub status: String,
    pub claude_authenticated: i32,
    pub error_message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl From<DbServerDeployment> for ServerDeployment {
    fn from(db: DbServerDeployment) -> Self {
        ServerDeployment {
            id: db.id,
            name: db.name,
            host: db.host,
            port: db.port,
            user: db.user,
            ssh_key_path: db.ssh_key_path,
            container_name: db.container_name,
            api_key: db.api_key,
            server_port: db.server_port,
            status: db.status,
            claude_authenticated: db.claude_authenticated != 0,
            error_message: db.error_message,
            created_at: db.created_at,
            updated_at: db.updated_at,
        }
    }
}
