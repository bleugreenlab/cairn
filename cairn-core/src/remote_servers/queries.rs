//! Database CRUD for servers.

use super::models::Server;
use crate::storage::{DbResult, LocalDb, RowExt};
use turso::params;

fn server_from_row(row: &turso::Row) -> DbResult<Server> {
    let excluded_project_ids = row
        .opt_text(7)?
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default();

    Ok(Server {
        id: row.text(0)?,
        name: row.text(1)?,
        url: row.text(2)?,
        org_id: row.opt_text(3)?,
        status: row.text(4)?,
        version: row.opt_text(5)?,
        error_message: row.opt_text(6)?,
        excluded_project_ids,
        last_seen_at: row.opt_i64(8)?,
        created_at: row.i64(9)?,
        updated_at: row.i64(10)?,
    })
}

/// List all servers.
pub async fn list(db: &LocalDb) -> DbResult<Vec<Server>> {
    db.query_all(
        "SELECT id, name, url, org_id, status, version, error_message,
                excluded_project_ids, last_seen_at, created_at, updated_at
         FROM servers
         ORDER BY created_at DESC",
        (),
        server_from_row,
    )
    .await
}

/// Get a single server by ID.
pub async fn get(db: &LocalDb, id: &str) -> DbResult<Option<Server>> {
    let id = id.to_string();
    db.query_opt(
        "SELECT id, name, url, org_id, status, version, error_message,
                excluded_project_ids, last_seen_at, created_at, updated_at
         FROM servers
         WHERE id = ?1",
        params![id.as_str()],
        server_from_row,
    )
    .await
}

/// Insert a new server record.
pub async fn create(db: &LocalDb, server: &Server) -> DbResult<()> {
    let excluded_project_ids =
        serde_json::to_string(&server.excluded_project_ids).unwrap_or_else(|_| "[]".to_string());
    let server = Server {
        id: server.id.clone(),
        name: server.name.clone(),
        url: server.url.clone(),
        org_id: server.org_id.clone(),
        status: server.status.clone(),
        version: server.version.clone(),
        error_message: server.error_message.clone(),
        excluded_project_ids: server.excluded_project_ids.clone(),
        last_seen_at: server.last_seen_at,
        created_at: server.created_at,
        updated_at: server.updated_at,
    };
    db.execute(
        "INSERT INTO servers (
            id, name, url, org_id, status, version, error_message,
            excluded_project_ids, last_seen_at, created_at, updated_at
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            server.id.as_str(),
            server.name.as_str(),
            server.url.as_str(),
            server.org_id.as_deref(),
            server.status.as_str(),
            server.version.as_deref(),
            server.error_message.as_deref(),
            excluded_project_ids.as_str(),
            server.last_seen_at,
            server.created_at,
            server.updated_at
        ],
    )
    .await
    .map(|_| ())
}

/// Update the status (and optional error message) of a server.
pub async fn update_status(
    db: &LocalDb,
    id: &str,
    status: &str,
    error_message: Option<&str>,
) -> DbResult<()> {
    let id = id.to_string();
    let status = status.to_string();
    let error_message = error_message.map(str::to_string);
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE servers
         SET status = ?2, error_message = ?3, updated_at = ?4
         WHERE id = ?1",
        params![id.as_str(), status.as_str(), error_message.as_deref(), now],
    )
    .await
    .map(|_| ())
}

/// Update status + version + last_seen_at after a health check.
pub async fn update_health(
    db: &LocalDb,
    id: &str,
    status: &str,
    version: Option<&str>,
    error_message: Option<&str>,
) -> DbResult<()> {
    let id = id.to_string();
    let status = status.to_string();
    let version = version.map(str::to_string);
    let error_message = error_message.map(str::to_string);
    let now = chrono::Utc::now().timestamp();
    db.execute(
        "UPDATE servers
         SET status = ?2,
             version = ?3,
             error_message = ?4,
             last_seen_at = ?5,
             updated_at = ?5
         WHERE id = ?1",
        params![
            id.as_str(),
            status.as_str(),
            version.as_deref(),
            error_message.as_deref(),
            now
        ],
    )
    .await
    .map(|_| ())
}

/// Update server name and/or excluded projects.
pub async fn update(
    db: &LocalDb,
    id: &str,
    name: Option<&str>,
    excluded_project_ids: Option<&str>,
) -> DbResult<()> {
    let id = id.to_string();
    let name = name.map(str::to_string);
    let excluded_project_ids = excluded_project_ids.map(str::to_string);
    let now = chrono::Utc::now().timestamp();

    db.write(|conn| {
        let id = id.clone();
        let name = name.clone();
        let excluded_project_ids = excluded_project_ids.clone();
        Box::pin(async move {
            if let Some(name) = name.as_deref() {
                conn.execute(
                    "UPDATE servers SET name = ?2, updated_at = ?3 WHERE id = ?1",
                    params![id.as_str(), name, now],
                )
                .await?;
            }

            if let Some(excluded_project_ids) = excluded_project_ids.as_deref() {
                conn.execute(
                    "UPDATE servers
                     SET excluded_project_ids = ?2, updated_at = ?3
                     WHERE id = ?1",
                    params![id.as_str(), excluded_project_ids, now],
                )
                .await?;
            }

            Ok(())
        })
    })
    .await
}

/// List all servers belonging to an org.
pub async fn list_by_org(db: &LocalDb, org_id: &str) -> DbResult<Vec<Server>> {
    let org_id = org_id.to_string();
    db.query_all(
        "SELECT id, name, url, org_id, status, version, error_message,
                excluded_project_ids, last_seen_at, created_at, updated_at
         FROM servers
         WHERE org_id = ?1
         ORDER BY created_at DESC",
        params![org_id.as_str()],
        server_from_row,
    )
    .await
}

/// Delete a server record.
pub async fn delete(db: &LocalDb, id: &str) -> DbResult<()> {
    let id = id.to_string();
    db.execute("DELETE FROM servers WHERE id = ?1", params![id.as_str()])
        .await
        .map(|_| ())
}

/// Delete all projects that belong to a server.
pub async fn delete_server_projects(db: &LocalDb, server_id: &str) -> DbResult<()> {
    let server_id = server_id.to_string();
    db.execute(
        "DELETE FROM projects WHERE server_id = ?1",
        params![server_id.as_str()],
    )
    .await
    .map(|_| ())
}
