//! Full-text search using a local Tantivy index fed by database outbox rows.

use crate::models::{SearchContentType, SearchFilters, SearchResult};
use crate::storage::{DbError, LocalDb, RowExt, SearchIndex, SearchIndexHit};
use cairn_common::uri::{
    build_issue_messages_uri, build_issue_uri, build_node_artifact_uri, build_node_chat_uri,
    build_project_messages_uri, build_project_uri,
};
use std::collections::HashMap;

/// Build a URI for navigation based on content type and IDs.
fn build_uri(
    project_key: &str,
    content_type: &SearchContentType,
    job_info: Option<&(Option<String>, Option<i32>)>,
    issue_number: Option<i32>,
) -> String {
    match content_type {
        SearchContentType::Issue | SearchContentType::Comment => issue_number
            .map(|num| build_issue_uri(project_key, num))
            .unwrap_or_else(|| build_project_uri(project_key)),
        SearchContentType::Message => issue_number
            .map(|num| build_issue_messages_uri(project_key, num))
            .unwrap_or_else(|| build_project_messages_uri(project_key)),
        SearchContentType::Artifact => {
            if let (Some(num), Some((Some(node_name), Some(exec_seq)))) = (issue_number, job_info) {
                build_node_artifact_uri(project_key, num, *exec_seq, node_name)
            } else {
                issue_number
                    .map(|num| build_issue_uri(project_key, num))
                    .unwrap_or_else(|| build_project_uri(project_key))
            }
        }
        SearchContentType::Event => {
            if let (Some(num), Some((Some(node_name), Some(exec_seq)))) = (issue_number, job_info) {
                build_node_chat_uri(project_key, num, *exec_seq, node_name)
            } else {
                issue_number
                    .map(|num| build_issue_uri(project_key, num))
                    .unwrap_or_else(|| build_project_uri(project_key))
            }
        }
    }
}

/// Search content through the local Tantivy index.
///
pub async fn search_content(
    db: &LocalDb,
    index: &SearchIndex,
    query: &str,
    filters: Option<SearchFilters>,
) -> Result<Vec<SearchResult>, String> {
    index
        .apply_pending(db)
        .await
        .map_err(|error| format!("Search index update failed: {error}"))?;

    let hits = index
        .search(query, filters.clone())
        .map_err(|error| format!("Search failed: {error}"))?;

    enrich_search_hits(db, hits, filters.unwrap_or_default()).await
}

async fn enrich_search_hits(
    db: &LocalDb,
    hits: Vec<SearchIndexHit>,
    filters: SearchFilters,
) -> Result<Vec<SearchResult>, String> {
    let mut project_ids: Vec<String> = hits.iter().map(|hit| hit.project_id.clone()).collect();
    project_ids.sort();
    project_ids.dedup();

    let mut issue_ids: Vec<String> = hits.iter().filter_map(|hit| hit.issue_id.clone()).collect();
    issue_ids.sort();
    issue_ids.dedup();

    let mut job_ids: Vec<String> = hits.iter().filter_map(|hit| hit.job_id.clone()).collect();
    job_ids.sort();
    job_ids.dedup();

    let project_keys = load_project_keys(db, project_ids).await?;
    let issue_info = load_issue_info(db, issue_ids).await?;
    let job_info = load_job_info(db, job_ids).await?;
    let limit = filters.limit.unwrap_or(50).min(100);

    Ok(hits
        .into_iter()
        .filter_map(|hit| {
            let project_key = project_keys.get(&hit.project_id)?;
            let (issue_number, issue_title) = hit
                .issue_id
                .as_ref()
                .and_then(|id| issue_info.get(id))
                .map(|(num, title)| (Some(*num), Some(title.clone())))
                .unwrap_or((None, None));
            let job_nav = hit.job_id.as_ref().and_then(|id| job_info.get(id));
            let uri = build_uri(project_key, &hit.content_type, job_nav, issue_number);

            let (ctx_number, ctx_title) = if hit.content_type == SearchContentType::Issue {
                (None, None)
            } else {
                (issue_number, issue_title)
            };

            let (node_name, exec_seq) = hit
                .job_id
                .as_ref()
                .and_then(|id| job_info.get(id))
                .cloned()
                .unwrap_or((None, None));

            Some(SearchResult {
                id: hit.id,
                content_type: hit.content_type,
                project_id: hit.project_id,
                issue_id: hit.issue_id,
                job_id: hit.job_id,
                title: hit.title,
                snippet: hit.snippet,
                rank: hit.rank,
                created_at: hit.created_at,
                uri,
                issue_number: ctx_number,
                issue_title: ctx_title,
                node_name,
                exec_seq,
            })
        })
        .take(limit)
        .collect())
}

async fn load_project_keys(
    db: &LocalDb,
    project_ids: Vec<String>,
) -> Result<HashMap<String, String>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut map = HashMap::new();
            for project_id in project_ids {
                let mut rows = conn
                    .query(
                        "SELECT key FROM projects WHERE id = ?1",
                        (project_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    map.insert(project_id, row.text(0)?);
                }
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

async fn load_issue_info(
    db: &LocalDb,
    issue_ids: Vec<String>,
) -> Result<HashMap<String, (i32, String)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut map = HashMap::new();
            for issue_id in issue_ids {
                let mut rows = conn
                    .query(
                        "SELECT number, title FROM issues WHERE id = ?1",
                        (issue_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    map.insert(issue_id, (row.i64(0)? as i32, row.text(1)?));
                }
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

async fn load_job_info(
    db: &LocalDb,
    job_ids: Vec<String>,
) -> Result<HashMap<String, (Option<String>, Option<i32>)>, String> {
    db.read(|conn| {
        Box::pin(async move {
            let mut map = HashMap::new();
            for job_id in job_ids {
                let mut rows = conn
                    .query(
                        "SELECT j.node_name, e.seq
                         FROM jobs j
                         LEFT JOIN executions e ON e.id = j.execution_id
                         WHERE j.id = ?1",
                        (job_id.as_str(),),
                    )
                    .await?;
                if let Some(row) = rows.next().await? {
                    map.insert(
                        job_id,
                        (row.opt_text(0)?, row.opt_i64(1)?.map(|v| v as i32)),
                    );
                }
            }
            Ok(map)
        })
    })
    .await
    .map_err(storage_error)
}

fn storage_error(error: DbError) -> String {
    error.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, SearchIndex};
    use tempfile::tempdir;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("cairn-search-content-turso.db").await
    }

    #[test]
    fn test_build_uri_issue() {
        let uri = build_uri("TEST", &SearchContentType::Issue, None, Some(42));
        assert_eq!(uri, "cairn://p/TEST/42");
    }

    #[test]
    fn test_build_uri_comment() {
        let uri = build_uri("TEST", &SearchContentType::Comment, None, Some(42));
        assert_eq!(uri, "cairn://p/TEST/42");
    }

    #[test]
    fn test_build_uri_message_uses_message_resources() {
        let project_uri = build_uri("TEST", &SearchContentType::Message, None, None);
        assert_eq!(project_uri, "cairn://p/TEST/messages");

        let issue_uri = build_uri("TEST", &SearchContentType::Message, None, Some(42));
        assert_eq!(issue_uri, "cairn://p/TEST/42/messages");
    }

    #[test]
    fn test_build_uri_artifact_prefers_node_artifact_when_job_navigation_exists() {
        let uri = build_uri(
            "TEST",
            &SearchContentType::Artifact,
            Some(&(Some("builder-1".to_string()), Some(3))),
            Some(42),
        );
        assert_eq!(uri, "cairn://p/TEST/42/3/builder-1/artifact");
    }

    #[test]
    fn test_build_uri_artifact_falls_back_when_job_navigation_missing() {
        let issue_uri = build_uri(
            "TEST",
            &SearchContentType::Artifact,
            Some(&(Some("builder-1".to_string()), None)),
            Some(42),
        );
        assert_eq!(issue_uri, "cairn://p/TEST/42");

        let project_uri = build_uri("TEST", &SearchContentType::Artifact, None, None);
        assert_eq!(project_uri, "cairn://p/TEST");
    }

    #[test]
    fn test_build_uri_event_prefers_node_chat_when_job_navigation_exists() {
        let uri = build_uri(
            "TEST",
            &SearchContentType::Event,
            Some(&(Some("builder-1".to_string()), Some(3))),
            Some(42),
        );
        assert_eq!(uri, "cairn://p/TEST/42/3/builder-1/chat");
    }

    #[test]
    fn test_build_uri_event_falls_back_when_job_navigation_missing() {
        let issue_uri = build_uri(
            "TEST",
            &SearchContentType::Event,
            Some(&(None, Some(3))),
            Some(42),
        );
        assert_eq!(issue_uri, "cairn://p/TEST/42");

        let project_uri = build_uri("TEST", &SearchContentType::Event, None, None);
        assert_eq!(project_uri, "cairn://p/TEST");
    }

    #[tokio::test]
    async fn search_content_returns_existing_search_result_shape() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
             VALUES ('workspace-1', 'Workspace', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'workspace-1', 'Project', 'PROJ', '/tmp/project', 1, 1);
            INSERT INTO issues(id, project_id, number, title, description, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 7, 'Turso migration', 'issue body', 1, 1);
            INSERT INTO comments(id, issue_id, content, source, created_at)
             VALUES ('comment-1', 'issue-1', 'tantivy replacement comment', 'user', 2);
            ",
        )
        .await
        .unwrap();

        let index_dir = tempdir().unwrap();
        let index = SearchIndex::open_or_create(index_dir.path()).unwrap();
        let results = search_content(
            &db,
            &index,
            "tantivy",
            Some(SearchFilters {
                project_id: Some("project-1".to_string()),
                issue_id: Some("issue-1".to_string()),
                content_types: Some(vec!["comment".to_string()]),
                since: None,
                limit: Some(10),
            }),
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.id, "comment-1");
        assert_eq!(result.content_type, SearchContentType::Comment);
        assert_eq!(result.uri, "cairn://p/PROJ/7");
        assert_eq!(result.issue_number, Some(7));
        assert_eq!(result.issue_title.as_deref(), Some("Turso migration"));
        assert!(result.snippet.contains("<mark>tantivy</mark>"));
    }
}
