//! Database CRUD for servers.

use super::models::{DbServer, Server};
use crate::schema::servers;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

/// List all servers.
pub fn list(conn: &mut SqliteConnection) -> Result<Vec<Server>, String> {
    servers::table
        .order(servers::created_at.desc())
        .load::<DbServer>(conn)
        .map(|rows| rows.into_iter().map(Server::from).collect())
        .map_err(|e| e.to_string())
}

/// Get a single server by ID.
pub fn get(conn: &mut SqliteConnection, id: &str) -> Result<Option<Server>, String> {
    servers::table
        .find(id)
        .first::<DbServer>(conn)
        .optional()
        .map(|opt| opt.map(Server::from))
        .map_err(|e| e.to_string())
}

/// Insert a new server record.
pub fn create(conn: &mut SqliteConnection, server: &DbServer) -> Result<(), String> {
    diesel::insert_into(servers::table)
        .values(server)
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Update the status (and optional error message) of a server.
pub fn update_status(
    conn: &mut SqliteConnection,
    id: &str,
    status: &str,
    error_message: Option<&str>,
) -> Result<(), String> {
    diesel::update(servers::table.find(id))
        .set((
            servers::status.eq(status),
            servers::error_message.eq(error_message),
            servers::updated_at.eq(chrono::Utc::now().timestamp() as i32),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Update status + version + last_seen_at after a health check.
pub fn update_health(
    conn: &mut SqliteConnection,
    id: &str,
    status: &str,
    version: Option<&str>,
    error_message: Option<&str>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;
    diesel::update(servers::table.find(id))
        .set((
            servers::status.eq(status),
            servers::version.eq(version),
            servers::error_message.eq(error_message),
            servers::last_seen_at.eq(now),
            servers::updated_at.eq(now),
        ))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Update server name and/or excluded projects.
pub fn update(
    conn: &mut SqliteConnection,
    id: &str,
    name: Option<&str>,
    excluded_project_ids: Option<&str>,
) -> Result<(), String> {
    let now = chrono::Utc::now().timestamp() as i32;

    if let Some(name) = name {
        diesel::update(servers::table.find(id))
            .set((servers::name.eq(name), servers::updated_at.eq(now)))
            .execute(conn)
            .map_err(|e| e.to_string())?;
    }

    if let Some(excluded) = excluded_project_ids {
        diesel::update(servers::table.find(id))
            .set((
                servers::excluded_project_ids.eq(excluded),
                servers::updated_at.eq(now),
            ))
            .execute(conn)
            .map_err(|e| e.to_string())?;
    }

    Ok(())
}

/// List all servers belonging to an org.
pub fn list_by_org(conn: &mut SqliteConnection, org_id: &str) -> Result<Vec<Server>, String> {
    servers::table
        .filter(servers::org_id.eq(org_id))
        .order(servers::created_at.desc())
        .load::<DbServer>(conn)
        .map(|rows| rows.into_iter().map(Server::from).collect())
        .map_err(|e| e.to_string())
}

/// Delete a server record.
pub fn delete(conn: &mut SqliteConnection, id: &str) -> Result<(), String> {
    diesel::delete(servers::table.find(id))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Delete all projects that belong to a server.
pub fn delete_server_projects(conn: &mut SqliteConnection, server_id: &str) -> Result<(), String> {
    use crate::schema::projects;
    diesel::delete(projects::table.filter(projects::server_id.eq(server_id)))
        .execute(conn)
        .map(|_| ())
        .map_err(|e| e.to_string())
}
