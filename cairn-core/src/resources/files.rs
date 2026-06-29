//! File-change resource readers and projections.

use super::common::{
    connect_and_find_node_job, connect_for_read, find_query_value, parse_optional_usize_param,
    resolve_issue_id,
};

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::query::QueryParam;
use std::path::Path;

// ============================================================================
// File Changes Readers
// ============================================================================

#[derive(Clone)]
struct FileProjectionEntry {
    path: String,
    display: String,
}

fn parse_file_projection_lines(
    params: &[QueryParam],
    entries: Vec<FileProjectionEntry>,
    resource_name: &str,
) -> Result<String, String> {
    // `glob` is this resource's own pushdown filter; `grep` is now a universal
    // view projection applied upstream over this rendered body (the
    // glob-filtered file list), so grep-family params are stripped before this
    // renderer runs and are not accepted here. A projection with only modifiers
    // (output_mode, offset/limit) lists every changed entry in the chosen mode;
    // `output_mode=files_with_matches` (the default) is the common case and must
    // never error. `resource_name` still labels the empty-match note.
    let glob = find_query_value(params, "glob");

    if let Some(unsupported) = params.iter().find(|param| {
        !["glob", "output_mode", "offset", "limit", "head_limit"].contains(&param.key.as_str())
    }) {
        return Err(format!(
            "Unsupported query parameter '{}' for {} projection",
            unsupported.key, resource_name
        ));
    }

    let offset = parse_optional_usize_param(params, "offset")?.unwrap_or(0);
    let limit = parse_optional_usize_param(params, "head_limit")?
        .or(parse_optional_usize_param(params, "limit")?)
        .unwrap_or(usize::MAX);
    let output_mode = find_query_value(params, "output_mode").unwrap_or("files_with_matches");
    if !matches!(output_mode, "files_with_matches" | "content" | "count") {
        return Err(format!(
            "Invalid output_mode '{}'. Must be 'content', 'files_with_matches', or 'count'.",
            output_mode
        ));
    }

    let glob_matcher = match glob {
        Some(pattern) => Some(
            globset::GlobBuilder::new(pattern)
                .literal_separator(false)
                .build()
                .map_err(|error| format!("Invalid glob '{}': {}", pattern, error))?
                .compile_matcher(),
        ),
        None => None,
    };

    let filtered = entries
        .into_iter()
        .filter(|entry| {
            glob_matcher
                .as_ref()
                .map(|matcher| matcher.is_match(&entry.path))
                .unwrap_or(true)
        })
        .map(|entry| entry.display)
        .collect::<Vec<_>>();

    if filtered.is_empty() {
        return Ok(format!(
            "No file changes matched {} projection.",
            resource_name
        ));
    }

    let sliced = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    Ok(match output_mode {
        "count" => sliced
            .into_iter()
            .map(|line| format!("{}:1", line))
            .collect::<Vec<_>>()
            .join("\n"),
        _ => sliced.join("\n"),
    })
}

type FileChangeRow = (String, String, Option<i32>, Option<i32>, Option<String>);
type IssueFileChangeRow = (
    String,
    String,
    Option<i32>,
    Option<i32>,
    Option<String>,
    String,
);
async fn issue_has_jobs(conn: &turso::Connection, issue_id: &str) -> bool {
    match conn
        .query(
            "SELECT id FROM jobs WHERE issue_id = ?1 LIMIT 1",
            (issue_id,),
        )
        .await
    {
        Ok(mut rows) => matches!(rows.next().await, Ok(Some(_))),
        Err(_) => false,
    }
}

async fn load_issue_file_changes_with_agents(
    conn: &turso::Connection,
    issue_id: &str,
) -> Vec<IssueFileChangeRow> {
    let mut results = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT fc.file_path, fc.status, fc.additions, fc.deletions,
                   fc.previous_path, COALESCE(j.agent_config_id, j.node_name, 'unknown')
            FROM file_changes fc
            JOIN jobs j ON fc.job_id = j.id
            WHERE j.issue_id = ?1
            ORDER BY j.created_at ASC, fc.file_path ASC
            ",
            (issue_id,),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (
                Ok(file_path),
                Ok(status),
                Ok(additions),
                Ok(deletions),
                Ok(previous_path),
                Ok(agent_name),
            ) = (
                row.text(0),
                row.text(1),
                row.opt_i64(2),
                row.opt_i64(3),
                row.opt_text(4),
                row.text(5),
            )
            else {
                continue;
            };
            results.push((
                file_path,
                status,
                additions.map(|value| value as i32),
                deletions.map(|value| value as i32),
                previous_path,
                agent_name,
            ));
        }
    }
    results
}

async fn load_issue_file_changes(conn: &turso::Connection, issue_id: &str) -> Vec<FileChangeRow> {
    let mut results = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT fc.file_path, fc.status, fc.additions, fc.deletions, fc.previous_path
            FROM file_changes fc
            JOIN jobs j ON fc.job_id = j.id
            WHERE j.issue_id = ?1
            ORDER BY fc.file_path ASC
            ",
            (issue_id,),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(file_path), Ok(status), Ok(additions), Ok(deletions), Ok(previous_path)) = (
                row.text(0),
                row.text(1),
                row.opt_i64(2),
                row.opt_i64(3),
                row.opt_text(4),
            ) else {
                continue;
            };
            results.push((
                file_path,
                status,
                additions.map(|value| value as i32),
                deletions.map(|value| value as i32),
                previous_path,
            ));
        }
    }
    results
}

async fn load_job_file_changes(conn: &turso::Connection, job_id: &str) -> Vec<FileChangeRow> {
    let mut results = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT file_path, status, additions, deletions, previous_path
            FROM file_changes
            WHERE job_id = ?1
            ORDER BY file_path ASC
            ",
            (job_id,),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(file_path), Ok(status), Ok(additions), Ok(deletions), Ok(previous_path)) = (
                row.text(0),
                row.text(1),
                row.opt_i64(2),
                row.opt_i64(3),
                row.opt_text(4),
            ) else {
                continue;
            };
            results.push((
                file_path,
                status,
                additions.map(|value| value as i32),
                deletions.map(|value| value as i32),
                previous_path,
            ));
        }
    }
    results
}

/// The node job's VCS anchors `(worktree_path, base_branch, base_commit)`, the
/// inputs [`crate::jj::node_changed_files`] needs to diff the workspace against
/// its recorded base. All optional: a job may predate a column, carry an empty
/// worktree, or have no recorded base — each case falls the projection back to
/// the recorded `file_changes` cache.
async fn load_job_vcs_anchors(
    conn: &turso::Connection,
    job_id: &str,
) -> (Option<String>, Option<String>, Option<String>) {
    let Ok(mut rows) = conn
        .query(
            "SELECT worktree_path, base_branch, base_commit FROM jobs WHERE id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
    else {
        return (None, None, None);
    };
    match rows.next().await {
        Ok(Some(row)) => (
            row.opt_text(0).ok().flatten().filter(|s| !s.is_empty()),
            row.opt_text(1).ok().flatten().filter(|s| !s.is_empty()),
            row.opt_text(2).ok().flatten().filter(|s| !s.is_empty()),
        ),
        _ => (None, None, None),
    }
}

/// The node's changed-file rows for the `/changed` projection, derived from the
/// live sealed jj graph when the workspace is present, falling back to the
/// recorded `file_changes` cache otherwise.
///
/// The graph is authoritative — it reflects exactly what the workspace sealed
/// (plus loose `@` edits) — so a just-sealed commit's file is never omitted the
/// way the best-effort async cache insert could lag or drop it (CAIRN-2101). The
/// cache stays the fallback for a torn-down workspace (the DB is then the only
/// record) and for a non-jj checkout. A jj workspace that resolves its base but
/// has changed nothing returns an empty set from the graph and is NOT a fallback
/// case: the graph correctly says "nothing changed," overriding any phantom
/// cache rows from an abandoned seal.
///
/// The issue-level [`read_issue_changed`] deliberately keeps using the cache: it
/// aggregates across executions including torn-down ones where the workspaces no
/// longer exist, and the live-review lag this fixes bites at the node level.
async fn load_node_changed_rows(
    orch: &Orchestrator,
    conn: &turso::Connection,
    job_id: &str,
) -> Vec<FileChangeRow> {
    let (worktree_path, base_branch, base_commit) = load_job_vcs_anchors(conn, job_id).await;
    if let Some(worktree_path) = worktree_path {
        let jj = crate::jj::JjEnv::resolve(&orch.jj_binary_path, &orch.config_dir);
        if let Some(changes) = crate::jj::node_changed_files(
            &jj,
            Path::new(&worktree_path),
            base_branch.as_deref(),
            base_commit.as_deref(),
        ) {
            return changes
                .into_iter()
                .map(|change| {
                    (
                        change.path,
                        change.status,
                        Some(change.additions),
                        Some(change.deletions),
                        change.previous_path,
                    )
                })
                .collect();
        }
    }
    load_job_file_changes(conn, job_id).await
}

fn merge_optional_counts(existing: Option<i32>, next: Option<i32>) -> Option<i32> {
    match (existing, next) {
        (Some(existing), Some(next)) => Some(existing + next),
        (Some(existing), None) => Some(existing),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn dedupe_file_changes_by_path(rows: &[FileChangeRow]) -> Vec<FileChangeRow> {
    let mut deduped: Vec<FileChangeRow> = Vec::new();

    for (file_path, status, additions, deletions, previous_path) in rows {
        if let Some((
            _,
            existing_status,
            existing_additions,
            existing_deletions,
            existing_previous_path,
        )) = deduped
            .iter_mut()
            .find(|(existing_path, _, _, _, _)| existing_path == file_path)
        {
            *existing_status = status.clone();
            *existing_additions = merge_optional_counts(*existing_additions, *additions);
            *existing_deletions = merge_optional_counts(*existing_deletions, *deletions);
            if previous_path.is_some() {
                *existing_previous_path = previous_path.clone();
            }
            continue;
        }

        deduped.push((
            file_path.clone(),
            status.clone(),
            *additions,
            *deletions,
            previous_path.clone(),
        ));
    }

    deduped
}

fn file_projection_entries(rows: Vec<FileChangeRow>) -> Vec<FileProjectionEntry> {
    dedupe_file_changes_by_path(&rows)
        .into_iter()
        .map(|(path, _, _, _, previous_path)| FileProjectionEntry {
            display: previous_path
                .map(|previous| format!("{} → {}", previous, path))
                .unwrap_or_else(|| path.clone()),
            path,
        })
        .collect()
}

fn push_file_change_table(output: &mut String, rows: &[FileChangeRow]) {
    output.push_str("| File | Status | +/- |\n");
    output.push_str("|------|--------|-----|\n");

    for (file_path, status, additions, deletions, previous_path) in
        dedupe_file_changes_by_path(rows)
    {
        let changes = match (additions, deletions) {
            (Some(a), Some(d)) => format!("+{} -{}", a, d),
            _ => "-".to_string(),
        };

        let file_display = if let Some(prev) = previous_path {
            format!("{} → {}", prev, file_path)
        } else {
            file_path
        };

        output.push_str(&format!(
            "| {} | {} | {} |\n",
            file_display, status, changes
        ));
    }
}

/// Read all file changes for an issue (aggregated across all jobs)
pub(super) async fn read_issue_changed(db: &LocalDb, project_key: &str, number: i32) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let (_, issue_id) = match resolve_issue_id(&conn, project_key, number).await {
        Ok(resolved) => resolved,
        Err(error) => return error,
    };

    if !issue_has_jobs(&conn, &issue_id).await {
        return format!("No jobs found for issue {}-{}", project_key, number);
    }

    let results = load_issue_file_changes_with_agents(&conn, &issue_id).await;

    if results.is_empty() {
        return format!(
            "No file changes recorded for issue {}-{}",
            project_key, number
        );
    }

    let mut output = format!("# Files Changed - {}-{}\n\n", project_key, number);
    let mut current_agent: Option<&str> = None;
    let mut current_agent_rows: Vec<FileChangeRow> = Vec::new();

    let flush_agent_rows =
        |output: &mut String, agent_name: &str, rows: &mut Vec<FileChangeRow>| {
            if rows.is_empty() {
                return;
            }

            output.push_str(&format!("\n## {}\n\n", agent_name));
            push_file_change_table(output, rows);

            rows.clear();
        };

    for (file_path, status, additions, deletions, previous_path, agent_name) in &results {
        if current_agent != Some(agent_name.as_str()) {
            if let Some(existing_agent) = current_agent {
                flush_agent_rows(&mut output, existing_agent, &mut current_agent_rows);
            }
            current_agent = Some(agent_name);
        }

        current_agent_rows.push((
            file_path.clone(),
            status.clone(),
            *additions,
            *deletions,
            previous_path.clone(),
        ));
    }

    if let Some(agent_name) = current_agent {
        flush_agent_rows(&mut output, agent_name, &mut current_agent_rows);
    }

    output
}

/// Read file changes for a specific node (job)
pub(super) async fn read_issue_changed_projection(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    params: &[QueryParam],
) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let (_, issue_id) = match resolve_issue_id(&conn, project_key, number).await {
        Ok(resolved) => resolved,
        Err(error) => return error,
    };
    if !issue_has_jobs(&conn, &issue_id).await {
        return format!("No jobs found for issue {}-{}", project_key, number);
    }
    let entries = file_projection_entries(load_issue_file_changes(&conn, &issue_id).await);

    parse_file_projection_lines(params, entries, "issue files").unwrap_or_else(|error| error)
}

pub(super) async fn read_node_changed(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    let db = orch.db.for_project(project_key).await;
    let (conn, job) =
        match connect_and_find_node_job(&db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };

    let results = load_node_changed_rows(orch, &conn, &job.id).await;

    if results.is_empty() {
        return format!(
            "No file changes recorded for node '{}' in issue {}-{}",
            node_name, project_key, number
        );
    }

    let mut output = format!(
        "# Files Changed - {}-{} / {}\n\n",
        project_key, number, node_name
    );
    push_file_change_table(&mut output, &results);

    output
}

pub(super) async fn read_node_changed_projection(
    orch: &Orchestrator,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    params: &[QueryParam],
) -> String {
    let db = orch.db.for_project(project_key).await;
    let (conn, job) =
        match connect_and_find_node_job(&db, project_key, number, exec_seq, node_name).await {
            Ok(resolved) => resolved,
            Err(error) => return error,
        };
    let entries = file_projection_entries(load_node_changed_rows(orch, &conn, &job.id).await);

    parse_file_projection_lines(params, entries, "node files").unwrap_or_else(|error| error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::query::parse_query_params;

    fn entries() -> Vec<FileProjectionEntry> {
        ["src/a.rs", "src/b.mjs", "README.md"]
            .into_iter()
            .map(|p| FileProjectionEntry {
                path: p.to_string(),
                display: p.to_string(),
            })
            .collect()
    }

    fn project(query: &str) -> Result<String, String> {
        let params = parse_query_params(query).unwrap();
        parse_file_projection_lines(&params, entries(), "node files")
    }

    #[test]
    fn output_mode_only_lists_all_entries() {
        // Modifier-only projection (no grep/glob): the default mode must list
        // every entry rather than erroring on a missing filter.
        let out = project("output_mode=files_with_matches").unwrap();
        assert!(out.contains("src/a.rs"), "{out}");
        assert!(out.contains("src/b.mjs"), "{out}");
        assert!(out.contains("README.md"), "{out}");
    }

    #[test]
    fn offset_limit_only_slices_all_entries() {
        let out = project("limit=1").unwrap();
        assert_eq!(out, "src/a.rs");
    }

    #[test]
    fn glob_filter_still_applies() {
        let out = project("glob=**/*.mjs").unwrap();
        assert_eq!(out, "src/b.mjs");
    }

    #[test]
    fn grep_is_not_accepted_by_the_projection() {
        // grep is now a universal view projection applied upstream over the
        // glob-filtered rendered body, so the projection itself rejects a
        // leftover grep param rather than filtering on it.
        let err = project("grep=readme").unwrap_err();
        assert!(err.contains("Unsupported"), "{err}");
    }

    #[test]
    fn count_mode_annotates_each_path() {
        let out = project("glob=**/*.rs&output_mode=count").unwrap();
        assert_eq!(out, "src/a.rs:1");
    }

    #[test]
    fn changed_table_sums_counts_when_duplicate_rows_include_nulls() {
        let rows = vec![
            (
                "src/a.rs".to_string(),
                "modified".to_string(),
                None,
                None,
                None,
            ),
            (
                "src/a.rs".to_string(),
                "modified".to_string(),
                Some(3),
                Some(1),
                None,
            ),
            (
                "src/a.rs".to_string(),
                "modified".to_string(),
                Some(2),
                Some(0),
                None,
            ),
        ];
        let mut output = String::new();
        push_file_change_table(&mut output, &rows);
        assert!(
            output.contains("| src/a.rs | modified | +5 -1 |"),
            "{output}"
        );
    }

    #[test]
    fn unsupported_parameter_is_rejected() {
        let err = project("bogus=1").unwrap_err();
        assert!(err.contains("Unsupported"), "{err}");
    }
}
