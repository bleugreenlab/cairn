//! Full-text search using SQLite FTS5.
//!
//! Provides BM25-ranked search with recency boost across issues,
//! comments, artifacts, and events.

use crate::models::{SearchContentType, SearchFilters, SearchResult};
use crate::schema::{executions, jobs, projects};
use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{Integer, Nullable, Text};
use diesel::sqlite::SqliteConnection;

/// Raw row from FTS5 query
#[derive(QueryableByName, Debug)]
struct FtsRow {
    #[diesel(sql_type = Text)]
    source_id: String,
    #[diesel(sql_type = Text)]
    content_type: String,
    #[diesel(sql_type = Text)]
    project_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    issue_id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    job_id: Option<String>,
    #[diesel(sql_type = Text)]
    title: String,
    #[diesel(sql_type = Text)]
    snippet: String,
    #[diesel(sql_type = diesel::sql_types::Double)]
    rank: f64,
    #[diesel(sql_type = Integer)]
    created_at: i32,
}

/// Build a URI for navigation based on content type and IDs.
fn build_uri(
    project_key: &str,
    content_type: &SearchContentType,
    _issue_id: Option<&str>,
    job_id: Option<&str>,
    issue_number: Option<i32>,
) -> String {
    match content_type {
        SearchContentType::Issue => {
            if let Some(num) = issue_number {
                format!("cairn://{}/{}", project_key, num)
            } else {
                format!("cairn://{}", project_key)
            }
        }
        SearchContentType::Comment => {
            if let Some(num) = issue_number {
                format!("cairn://{}/{}/comments", project_key, num)
            } else {
                format!("cairn://{}", project_key)
            }
        }
        SearchContentType::Message => {
            if let Some(num) = issue_number {
                format!("cairn://{}/{}/messages", project_key, num)
            } else {
                format!("cairn://{}/messages", project_key)
            }
        }
        SearchContentType::Artifact | SearchContentType::Event => {
            if let (Some(_job), Some(num)) = (job_id, issue_number) {
                format!("cairn://{}/{}", project_key, num)
            } else {
                format!("cairn://{}", project_key)
            }
        }
    }
}

/// Search content across issues, comments, artifacts, and events.
///
/// Uses FTS5 with BM25 ranking combined with recency boost.
/// Supports filtering by project, issue, content types, and time range.
pub fn search_content(
    conn: &mut SqliteConnection,
    query: &str,
    filters: Option<SearchFilters>,
) -> Result<Vec<SearchResult>, String> {
    let filters = filters.unwrap_or_default();
    let limit = filters.limit.unwrap_or(50).min(100);

    let safe_query = escape_fts_query(query);

    if safe_query.is_empty() {
        return Ok(vec![]);
    }

    // Build WHERE clauses for filters
    let mut where_clauses = vec!["content_fts MATCH ?1".to_string()];
    let mut param_index = 2;

    if filters.project_id.is_some() {
        where_clauses.push(format!("project_id = ?{}", param_index));
        param_index += 1;
    }

    if filters.issue_id.is_some() {
        where_clauses.push(format!("issue_id = ?{}", param_index));
        param_index += 1;
    }

    if let Some(ref content_types) = filters.content_types {
        if !content_types.is_empty() {
            let placeholders: Vec<String> = content_types
                .iter()
                .enumerate()
                .map(|(i, _)| format!("?{}", param_index + i))
                .collect();
            where_clauses.push(format!("content_type IN ({})", placeholders.join(", ")));
            param_index += content_types.len();
        }
    }

    if filters.since.is_some() {
        where_clauses.push(format!("created_at >= ?{}", param_index));
    }

    let where_clause = where_clauses.join(" AND ");

    let sql = format!(
        r#"
        SELECT 
            source_id,
            content_type,
            project_id,
            issue_id,
            job_id,
            title,
            snippet(content_fts, 1, '<mark>', '</mark>', '...', 32) AS snippet,
            (-bm25(content_fts) + (1.0 / (1.0 + ((strftime('%s', 'now') - created_at) / 31536000.0))) * 5.0) AS rank,
            created_at
        FROM content_fts
        WHERE {}
        ORDER BY rank DESC, created_at DESC
        LIMIT {}
        "#,
        where_clause, limit
    );

    // Execute based on which filters are present
    let rows: Vec<FtsRow> = match (
        &filters.project_id,
        &filters.issue_id,
        &filters.content_types,
        &filters.since,
    ) {
        (None, None, None, None) => sql_query(&sql)
            .bind::<Text, _>(&safe_query)
            .load(conn)
            .map_err(|e| format!("Search failed: {}", e))?,

        (Some(project_id), None, None, None) => sql_query(&sql)
            .bind::<Text, _>(&safe_query)
            .bind::<Text, _>(project_id)
            .load(conn)
            .map_err(|e| format!("Search failed: {}", e))?,

        (Some(project_id), Some(issue_id), None, None) => sql_query(&sql)
            .bind::<Text, _>(&safe_query)
            .bind::<Text, _>(project_id)
            .bind::<Text, _>(issue_id)
            .load(conn)
            .map_err(|e| format!("Search failed: {}", e))?,

        (Some(project_id), None, Some(content_types), None) if content_types.len() == 1 => {
            sql_query(&sql)
                .bind::<Text, _>(&safe_query)
                .bind::<Text, _>(project_id)
                .bind::<Text, _>(&content_types[0])
                .load(conn)
                .map_err(|e| format!("Search failed: {}", e))?
        }

        (None, None, Some(content_types), None) if content_types.len() == 1 => sql_query(&sql)
            .bind::<Text, _>(&safe_query)
            .bind::<Text, _>(&content_types[0])
            .load(conn)
            .map_err(|e| format!("Search failed: {}", e))?,

        // For complex filter combinations, fall back to simpler query
        _ => {
            let simple_sql = r#"
                SELECT 
                    source_id,
                    content_type,
                    project_id,
                    issue_id,
                    job_id,
                    title,
                    snippet(content_fts, 1, '<mark>', '</mark>', '...', 32) AS snippet,
                    (-bm25(content_fts) + (1.0 / (1.0 + ((strftime('%s', 'now') - created_at) / 31536000.0))) * 5.0) AS rank,
                    created_at
                FROM content_fts
                WHERE content_fts MATCH ?1
                ORDER BY rank DESC, created_at DESC
                LIMIT ?2
            "#;
            sql_query(simple_sql)
                .bind::<Text, _>(&safe_query)
                .bind::<Integer, _>(limit as i32)
                .load(conn)
                .map_err(|e| format!("Search failed: {}", e))?
        }
    };

    // Get project keys for URI building
    let project_ids: Vec<String> = rows.iter().map(|r| r.project_id.clone()).collect();
    let project_keys: std::collections::HashMap<String, String> = if !project_ids.is_empty() {
        projects::table
            .filter(projects::id.eq_any(&project_ids))
            .select((projects::id, projects::key))
            .load::<(String, String)>(conn)
            .map_err(|e| format!("Failed to load project keys: {}", e))?
            .into_iter()
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    // Get issue numbers and titles
    let issue_ids: Vec<String> = rows.iter().filter_map(|r| r.issue_id.clone()).collect();
    let issue_info: std::collections::HashMap<String, (i32, String)> = if !issue_ids.is_empty() {
        use crate::schema::issues;
        issues::table
            .filter(issues::id.eq_any(&issue_ids))
            .select((issues::id, issues::number, issues::title))
            .load::<(String, i32, String)>(conn)
            .map_err(|e| format!("Failed to load issue info: {}", e))?
            .into_iter()
            .map(|(id, num, title)| (id, (num, title)))
            .collect()
    } else {
        std::collections::HashMap::new()
    };

    // Get job info (node_name, exec_seq) for navigation
    let job_ids: Vec<String> = rows.iter().filter_map(|r| r.job_id.clone()).collect();
    let job_info: std::collections::HashMap<String, (Option<String>, Option<i32>)> =
        if !job_ids.is_empty() {
            jobs::table
                .left_join(executions::table)
                .filter(jobs::id.eq_any(&job_ids))
                .select((
                    jobs::id,
                    jobs::node_name.nullable(),
                    executions::seq.nullable(),
                ))
                .load::<(String, Option<String>, Option<i32>)>(conn)
                .map_err(|e| format!("Failed to load job info: {}", e))?
                .into_iter()
                .map(|(id, name, seq)| (id, (name, seq)))
                .collect()
        } else {
            std::collections::HashMap::new()
        };

    // Convert to SearchResult
    let results: Vec<SearchResult> = rows
        .into_iter()
        .filter_map(|row| {
            let content_type: SearchContentType = row.content_type.parse().ok()?;
            let project_key = project_keys.get(&row.project_id)?;

            let (issue_number, issue_title) = row
                .issue_id
                .as_ref()
                .and_then(|id| issue_info.get(id))
                .map(|(num, title)| (Some(*num), Some(title.clone())))
                .unwrap_or((None, None));

            let uri = build_uri(
                project_key,
                &content_type,
                row.issue_id.as_deref(),
                row.job_id.as_deref(),
                issue_number,
            );

            // For issues, don't include issue context (it's redundant)
            let (ctx_number, ctx_title) = if content_type == SearchContentType::Issue {
                (None, None)
            } else {
                (issue_number, issue_title)
            };

            // Get job navigation info
            let (node_name, exec_seq) = row
                .job_id
                .as_ref()
                .and_then(|id| job_info.get(id))
                .cloned()
                .unwrap_or((None, None));

            Some(SearchResult {
                id: row.source_id,
                content_type,
                project_id: row.project_id,
                issue_id: row.issue_id,
                job_id: row.job_id,
                title: row.title,
                snippet: row.snippet,
                rank: row.rank,
                created_at: row.created_at as i64,
                uri,
                issue_number: ctx_number,
                issue_title: ctx_title,
                node_name,
                exec_seq,
            })
        })
        .collect();

    // Apply post-query filters if we couldn't apply them in SQL
    let results = if filters.project_id.is_some()
        || filters.issue_id.is_some()
        || filters.content_types.is_some()
        || filters.since.is_some()
    {
        results
            .into_iter()
            .filter(|r| {
                if let Some(ref pid) = filters.project_id {
                    if &r.project_id != pid {
                        return false;
                    }
                }
                if let Some(ref iid) = filters.issue_id {
                    if r.issue_id.as_ref() != Some(iid) {
                        return false;
                    }
                }
                if let Some(ref types) = filters.content_types {
                    if !types.contains(&r.content_type.to_string()) {
                        return false;
                    }
                }
                if let Some(since) = filters.since {
                    if r.created_at < since {
                        return false;
                    }
                }
                true
            })
            .take(limit)
            .collect()
    } else {
        results
    };

    Ok(results)
}

/// Escape special FTS5 query characters to prevent syntax errors.
pub fn escape_fts_query(query: &str) -> String {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    // If query already contains quotes, assume user knows FTS5 syntax
    if trimmed.contains('"') {
        return trimmed.to_string();
    }

    let words: Vec<&str> = trimmed.split_whitespace().collect();
    if words.len() == 1 {
        // Single word: use prefix match
        format!("\"{}\"*", words[0])
    } else {
        // Multiple words: search for all terms
        words
            .iter()
            .map(|w| format!("\"{}\"", w))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_fts_query_single_word() {
        assert_eq!(escape_fts_query("hello"), "\"hello\"*");
    }

    #[test]
    fn test_escape_fts_query_multiple_words() {
        assert_eq!(escape_fts_query("hello world"), "\"hello\" \"world\"");
    }

    #[test]
    fn test_escape_fts_query_preserves_quotes() {
        assert_eq!(escape_fts_query("\"exact phrase\""), "\"exact phrase\"");
    }

    #[test]
    fn test_escape_fts_query_empty() {
        assert_eq!(escape_fts_query(""), "");
        assert_eq!(escape_fts_query("   "), "");
    }

    #[test]
    fn test_build_uri_issue() {
        let uri = build_uri(
            "TEST",
            &SearchContentType::Issue,
            Some("id"),
            None,
            Some(42),
        );
        assert_eq!(uri, "cairn://TEST/42");
    }

    #[test]
    fn test_build_uri_comment() {
        let uri = build_uri(
            "TEST",
            &SearchContentType::Comment,
            Some("id"),
            None,
            Some(42),
        );
        assert_eq!(uri, "cairn://TEST/42/comments");
    }
}
