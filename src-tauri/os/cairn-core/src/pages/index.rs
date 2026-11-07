//! The project's Pages index.

use cairn_db::turso::params;
use serde::Serialize;
use serde_json::Value;

use super::data::PAGE_ARTIFACT_TYPE;
use crate::storage::{LocalDb, RowExt};

/// One row of the Pages index: the newest version of a promoted thread page.
/// The frontend's `PageSummary` (`packages/ui/src/types.ts`) is this shape.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PageSummary {
    pub name: String,
    pub title: String,
    pub description: Option<String>,
    pub thread_name: String,
    pub thread_id: String,
    pub version: i32,
    /// ISO 8601.
    pub updated_at: String,
    /// `cairn://p/{project}/{thread}/pages/{name}`.
    pub uri: String,
}

/// Narrowings of the index.
#[derive(Debug, Clone, Default)]
pub struct PageIndexFilter {
    /// Only the pages this thread (by name) owns.
    pub thread: Option<String>,
    /// Case-insensitive substring of the name, title, or description.
    pub search: Option<String>,
    pub limit: Option<usize>,
}

/// The project's promoted pages, newest first.
///
/// Only a thread owns a page that can be listed: the rows considered are those
/// under a thread's session jobs, and a page is listed when its NEWEST version
/// has `promoted: true`. A draft on an issue node never appears, and neither
/// does a page whose owner later un-promoted it.
pub async fn list_project_pages(
    db: &LocalDb,
    project_id: &str,
    filter: &PageIndexFilter,
) -> Result<Vec<PageSummary>, String> {
    let project_id = project_id.to_string();
    let rows: Vec<(String, i32, String, i64, String, String)> = db
        .query_all(
            format!(
                "SELECT a.output_name, a.version, a.data, a.updated_at, t.id, t.name
                 FROM artifacts a
                 JOIN jobs j ON j.id = a.job_id
                 JOIN threads t ON t.id = j.thread_id
                 WHERE t.project_id = ?1 AND {}
                   AND a.artifact_type = '{PAGE_ARTIFACT_TYPE}' AND a.output_name LIKE 'pages/%'
                 ORDER BY t.id, a.output_name, a.version DESC, a.created_at DESC",
                crate::threads::SESSION_JOB_SHAPE
            ),
            params![project_id.as_str()],
            |row| {
                Ok((
                    row.text(0)?,
                    row.i64(1)? as i32,
                    row.text(2)?,
                    row.i64(3)?,
                    row.text(4)?,
                    row.text(5)?,
                ))
            },
        )
        .await
        .map_err(|error| format!("Failed to load the Pages index: {error}"))?;
    let project_key = db
        .query_text(
            "SELECT key FROM projects WHERE id = ?1",
            params![project_id.as_str()],
        )
        .await
        .map_err(|error| format!("Failed to load project: {error}"))?
        .ok_or_else(|| format!("Unknown project: {project_id}"))?;

    let thread = filter
        .thread
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty());
    let search = filter
        .search
        .as_deref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty());
    let mut pages = Vec::new();
    let mut previous: Option<(String, String)> = None;
    for (output, version, data, updated_at, thread_id, thread_name) in rows {
        let key = (thread_id.clone(), output.clone());
        // Rows arrive newest-first per (thread, name): only the first counts.
        if previous.as_ref() == Some(&key) {
            continue;
        }
        previous = Some(key);
        let data: Value = match serde_json::from_str(&data) {
            Ok(data) => data,
            Err(_) => continue,
        };
        if data.get("promoted").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        if thread.is_some_and(|thread| thread != thread_name) {
            continue;
        }
        let name = output.trim_start_matches("pages/").to_string();
        let title = data
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or(&name)
            .to_string();
        let description = data
            .get("description")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(search) = &search {
            let haystack = format!(
                "{name}\n{title}\n{}",
                description.as_deref().unwrap_or_default()
            )
            .to_lowercase();
            if !haystack.contains(search.as_str()) {
                continue;
            }
        }
        pages.push((
            updated_at,
            PageSummary {
                uri: cairn_uri::build_node_page_uri(&project_key, 0, 0, &thread_name, &name),
                name,
                title,
                description,
                thread_name,
                thread_id,
                version,
                updated_at: chrono::DateTime::from_timestamp(updated_at, 0)
                    .unwrap_or_default()
                    .to_rfc3339(),
            },
        ));
    }
    pages.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.uri.cmp(&b.1.uri)));
    let limit = filter.limit.unwrap_or(usize::MAX);
    Ok(pages
        .into_iter()
        .take(limit)
        .map(|(_, page)| page)
        .collect())
}
