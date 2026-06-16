//! Project pull-request (merge request) collection reader.
//!
//! `cairn://p/PROJECT/prs` renders a read-only, newest-first list of the
//! project's pull requests. Mutations stay out of scope here on purpose: per-PR
//! actions (merge / close / refresh) live on the `pr` action node
//! (`cairn://p/PROJECT/N/EXEC/pr`). Each row links to that canonical URI so an
//! agent can act on a PR straight from the list.

use turso::params;

use super::common::{connect_for_read, lookup_project_by_key};
use crate::merge_requests::queries::get_project_merge_requests;
use crate::models::ProjectPrEntry;
use crate::storage::{LocalDb, RowExt};
use cairn_common::uri::build_node_uri;

/// Resolve the `executions.seq` (the `EXEC` segment of a node URI) for an
/// execution id. Returns `None` when the id is empty or the row has no seq, in
/// which case the per-PR action URI is omitted for that row.
async fn execution_seq(conn: &turso::Connection, execution_id: &str) -> Option<i32> {
    if execution_id.is_empty() {
        return None;
    }
    let mut rows = conn
        .query(
            "SELECT seq FROM executions WHERE id = ?1 LIMIT 1",
            params![execution_id],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok().flatten()?;
    row.opt_i64(0).ok()?.map(|value| value as i32)
}

/// Format one PR row: issue provenance, PR status, GitHub number/URL when
/// present, the PR title, and the canonical per-PR action-node URI.
async fn format_pr_row(
    conn: &turso::Connection,
    project_key: &str,
    entry: &ProjectPrEntry,
) -> String {
    let issue_ref = match entry.issue_number {
        Some(number) => format!("{}-{}", project_key, number),
        None => "(unlinked)".to_string(),
    };
    let issue_suffix = entry
        .issue_title
        .as_deref()
        .map(|title| format!(" ({title})"))
        .unwrap_or_default();
    let pr_title = entry.title.as_deref().unwrap_or("(untitled)");
    // pr_number is 0 when the merge request has no GitHub PR number yet (a local
    // MR not pushed). Such a row still lists with its status.
    let pr_number = if entry.pr_number > 0 {
        format!(" #{}", entry.pr_number)
    } else {
        String::new()
    };
    let url_suffix = if entry.pr_url.is_empty() {
        String::new()
    } else {
        format!(" {}", entry.pr_url)
    };

    let mut row = format!(
        "- {issue_ref}{issue_suffix} — PR \"{pr_title}\"{pr_number} [{status}]{url_suffix}\n",
        status = entry.pr_status,
    );

    // Link the row to the per-PR action node so merge/close/refresh stay one hop
    // away. Requires both the issue number and the execution seq to address it.
    if let (Some(number), Some(exec_seq)) = (
        entry.issue_number,
        execution_seq(conn, &entry.execution_id).await,
    ) {
        let uri = build_node_uri(project_key, number, exec_seq, "pr");
        row.push_str(&format!("  action: `{}`\n", uri));
    }

    row
}

/// Render the project's PR collection, newest-first (by merge-request update).
pub(crate) async fn read_project_prs(db: &LocalDb, project_key: &str) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project = match lookup_project_by_key(&conn, project_key).await {
        Ok(ctx) => ctx,
        Err(error) => return error,
    };
    let entries = match get_project_merge_requests(db, &project.project_id).await {
        Ok(entries) => entries,
        Err(error) => return error,
    };

    let mut out = format!("# Pull requests — {}\n\n", project.project_key);
    out.push_str(&format!("{} PR(s)\n\n", entries.len()));

    if entries.is_empty() {
        out.push_str("No pull requests.\n");
        return out;
    }

    for entry in &entries {
        out.push_str(&format_pr_row(&conn, &project.project_key, entry).await);
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Seed a project with two PRs on one issue: a GitHub-backed PR on exec seq 1
    /// and a local-only PR (no GitHub number) on exec seq 2. The local PR has the
    /// newer `updated_at`, so it must sort first.
    async fn seed(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Cairn', 'CAIRN', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, status, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 42, 'My Issue', 'active', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-1', 'recipe-1', 'issue-1', 'proj-1', 'complete', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-2', 'recipe-1', 'issue-1', 'proj-1', 'running', 2, 2)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, status, issue_id, project_id, created_at, updated_at)
                     VALUES ('job-1', 'exec-1', 'builder', 'complete', 'issue-1', 'proj-1', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, status, issue_id, project_id, created_at, updated_at)
                     VALUES ('job-2', 'exec-2', 'builder', 'complete', 'issue-1', 'proj-1', 2, 2)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at, github_pr_number, github_pr_url)
                     VALUES ('mr-1', 'job-1', 'proj-1', 'issue-1', 'Fix bug', 'feature', 'main', 'open', 10, 10, 42, 'https://github.com/test/test/pull/42')",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO merge_requests (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
                     VALUES ('mr-2', 'job-2', 'proj-1', 'issue-1', 'Local PR', 'feature2', 'main', 'open', 20, 20)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn lists_project_prs_with_status_url_and_action_uri() {
        let db = crate::storage::migrated_test_db("project-prs-read.db").await;
        seed(&db).await;

        let out = read_project_prs(&db, "CAIRN").await;

        assert!(out.contains("2 PR(s)"), "output: {out}");
        // GitHub-backed PR: number, status, URL, and per-PR action node URI.
        assert!(out.contains("#42"), "output: {out}");
        assert!(out.contains("[open]"), "output: {out}");
        assert!(
            out.contains("https://github.com/test/test/pull/42"),
            "output: {out}"
        );
        assert!(out.contains("cairn://p/CAIRN/42/1/pr"), "output: {out}");
        // Local PR without a GitHub number still lists with its status + action URI.
        assert!(out.contains("Local PR"), "output: {out}");
        assert!(out.contains("cairn://p/CAIRN/42/2/pr"), "output: {out}");
        // Issue provenance is surfaced.
        assert!(out.contains("CAIRN-42"), "output: {out}");
        assert!(out.contains("My Issue"), "output: {out}");

        // Newest-first: the local PR (updated_at 20) sorts before the GitHub PR (10).
        let local_idx = out.find("Local PR").unwrap();
        let github_idx = out.find("Fix bug").unwrap();
        assert!(local_idx < github_idx, "expected newest-first order: {out}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn empty_project_lists_no_prs() {
        let db = crate::storage::migrated_test_db("project-prs-empty.db").await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, default_branch, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Cairn', 'CAIRN', '/repo', 'main', 1, 1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let out = read_project_prs(&db, "CAIRN").await;
        assert!(out.contains("0 PR(s)"), "output: {out}");
        assert!(out.contains("No pull requests."), "output: {out}");
    }
}
