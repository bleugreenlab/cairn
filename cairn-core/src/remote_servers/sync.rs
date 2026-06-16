//! Server project synchronization logic.
//!
//! Fetches remote project lists and creates/removes local bookmarks.

use crate::services::Clock;
use crate::storage::{DbResult, LocalDb, RowExt};
use serde::Deserialize;
use turso::params;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteProject {
    pub id: String,
    pub name: String,
    pub key: String,
}

/// Sync projects from a remote server into local bookmarks.
///
/// - Creates local bookmarks for remote projects not yet tracked.
/// - Removes local bookmarks for projects no longer on the server.
/// - Respects `excluded_project_ids` — those are skipped.
///
/// Returns the number of projects created/removed as (created, removed).
pub async fn sync_projects(
    db: &LocalDb,
    clock: &dyn Clock,
    server_id: &str,
    server_url: &str,
    remote_projects: &[RemoteProject],
    excluded_ids: &[String],
) -> DbResult<(usize, usize)> {
    let excluded_set: std::collections::HashSet<&str> =
        excluded_ids.iter().map(|s| s.as_str()).collect();

    let remote_ids: std::collections::HashSet<String> = remote_projects
        .iter()
        .filter(|p| !excluded_set.contains(p.id.as_str()))
        .map(|p| p.id.clone())
        .collect();

    let server_id = server_id.to_string();
    let server_url = server_url.to_string();
    let remote_projects: Vec<RemoteProject> = remote_projects
        .iter()
        .map(|project| RemoteProject {
            id: project.id.clone(),
            name: project.name.clone(),
            key: project.key.clone(),
        })
        .collect();
    let now = clock.now() as i32;

    db.write(|conn| {
        let server_id = server_id.clone();
        let server_url = server_url.clone();
        let remote_projects = remote_projects.clone();
        let remote_ids = remote_ids.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, name FROM projects WHERE server_id = ?1",
                    params![server_id.as_str()],
                )
                .await?;

            let mut existing = Vec::new();
            while let Some(row) = rows.next().await? {
                existing.push((row.text(0)?, row.text(1)?));
            }

            let existing_ids: std::collections::HashSet<String> =
                existing.iter().map(|(id, _)| id.clone()).collect();
            let mut created = 0;
            let mut removed = 0;

            for rp in &remote_projects {
                if !remote_ids.contains(&rp.id) {
                    continue;
                }

                if existing_ids.contains(&rp.id) {
                    let existing_name = existing
                        .iter()
                        .find(|(id, _)| id == &rp.id)
                        .map(|(_, name)| name.as_str());
                    if existing_name != Some(rp.name.as_str()) {
                        conn.execute(
                            "UPDATE projects SET name = ?2, updated_at = ?3 WHERE id = ?1",
                            params![rp.id.as_str(), rp.name.as_str(), now],
                        )
                        .await?;
                    }
                    continue;
                }

                conn.execute(
                    "INSERT INTO projects(
                        id, workspace_id, name, key, repo_path, context, docs_enabled,
                        default_branch, next_issue_number, created_at, updated_at,
                        remote_url, server_id
                     )
                     VALUES (?1, 'default', ?2, ?3, '', '', 1, 'main', 1, ?4, ?5, ?6, ?7)",
                    params![
                        rp.id.as_str(),
                        rp.name.as_str(),
                        rp.key.as_str(),
                        now,
                        now,
                        server_url.as_str(),
                        server_id.as_str()
                    ],
                )
                .await?;
                created += 1;
            }

            for (id, _) in &existing {
                if !remote_ids.contains(id) {
                    conn.execute("DELETE FROM projects WHERE id = ?1", params![id.as_str()])
                        .await?;
                    removed += 1;
                }
            }

            Ok((created, removed))
        })
    })
    .await
}
