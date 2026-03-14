//! Database CRUD for server deployments.

use super::models::{DbServerDeployment, ServerDeployment};
use crate::schema::server_deployments;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// List all deployments.
pub fn list(conn: &mut SqliteConnection) -> Result<Vec<ServerDeployment>, String> {
    server_deployments::table
        .order(server_deployments::created_at.desc())
        .load::<DbServerDeployment>(conn)
        .map(|rows| rows.into_iter().map(ServerDeployment::from).collect())
        .map_err(|e| e.to_string())
}

/// Get a single deployment by ID.
pub fn get(conn: &mut SqliteConnection, id: &str) -> Result<Option<ServerDeployment>, String> {
    server_deployments::table
        .find(id)
        .first::<DbServerDeployment>(conn)
        .optional()
        .map(|opt| opt.map(ServerDeployment::from))
        .map_err(|e| e.to_string())
}

/// Insert a new deployment record.
pub fn create(conn: &mut SqliteConnection, deployment: &DbServerDeployment) -> Result<(), String> {
    diesel::insert_into(server_deployments::table)
        .values(deployment)
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Update the status (and optional error message) of a deployment.
pub fn update_status(
    conn: &mut SqliteConnection,
    id: &str,
    status: &str,
    error_message: Option<&str>,
) -> Result<(), String> {
    diesel::update(server_deployments::table.find(id))
        .set((
            server_deployments::status.eq(status),
            server_deployments::error_message.eq(error_message),
            server_deployments::updated_at.eq(chrono::Utc::now().timestamp()),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Update Claude authentication status.
pub fn update_claude_auth(
    conn: &mut SqliteConnection,
    id: &str,
    authenticated: bool,
) -> Result<(), String> {
    diesel::update(server_deployments::table.find(id))
        .set((
            server_deployments::claude_authenticated.eq(authenticated as i32),
            server_deployments::updated_at.eq(chrono::Utc::now().timestamp()),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Delete a deployment record.
pub fn delete(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    diesel::delete(server_deployments::table.find(id))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}
