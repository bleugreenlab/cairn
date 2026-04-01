//! Server project synchronization logic.
//!
//! Fetches remote project lists and creates/removes local bookmarks.

use crate::models::CreateProject;
use crate::services::Clock;
use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
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
pub fn sync_projects(
    conn: &mut SqliteConnection,
    clock: &dyn Clock,
    server_id: &str,
    server_url: &str,
    remote_projects: &[RemoteProject],
    excluded_ids: &[String],
) -> Result<(usize, usize), String> {
    use crate::schema::projects;

    let excluded_set: std::collections::HashSet<&str> =
        excluded_ids.iter().map(|s| s.as_str()).collect();

    // Get existing local bookmarks for this server
    let existing: Vec<(String, String)> = projects::table
        .filter(projects::server_id.eq(server_id))
        .select((projects::id, projects::name))
        .load(conn)
        .map_err(|e| e.to_string())?;

    let existing_ids: std::collections::HashSet<String> =
        existing.iter().map(|(id, _)| id.clone()).collect();

    let remote_ids: std::collections::HashSet<String> = remote_projects
        .iter()
        .filter(|p| !excluded_set.contains(p.id.as_str()))
        .map(|p| p.id.clone())
        .collect();

    let mut created = 0;
    let mut removed = 0;

    // Create bookmarks for new remote projects
    for rp in remote_projects {
        if excluded_set.contains(rp.id.as_str()) {
            continue;
        }
        if existing_ids.contains(&rp.id) {
            // Update name if changed
            let existing_name = existing
                .iter()
                .find(|(id, _)| id == &rp.id)
                .map(|(_, n)| n.as_str());
            if existing_name != Some(&rp.name) {
                diesel::update(projects::table.find(&rp.id))
                    .set(projects::name.eq(&rp.name))
                    .execute(conn)
                    .map_err(|e| e.to_string())?;
            }
            continue;
        }

        // Create new bookmark
        let input = CreateProject {
            id: Some(rp.id.clone()),
            name: rp.name.clone(),
            key: rp.key.clone(),
            repo_path: String::new(),
            remote_url: Some(server_url.to_string()),
            server_id: Some(server_id.to_string()),
        };
        crate::projects::crud::create_db(conn, clock, &input)?;
        created += 1;
    }

    // Remove bookmarks for projects no longer on the server
    for (id, _) in &existing {
        if !remote_ids.contains(id) {
            diesel::delete(projects::table.find(id))
                .execute(conn)
                .map_err(|e| e.to_string())?;
            removed += 1;
        }
    }

    Ok((created, removed))
}
