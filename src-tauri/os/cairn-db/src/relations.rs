use std::collections::HashSet;

use crate::turso::params;
use cairn_common::uri::{build_issue_uri, parse_uri, CairnResource};

use crate::models::IssueStatus;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

#[derive(Debug, Clone, PartialEq)]
pub struct IssueRef {
    pub uri: String,
    pub project_key: String,
    pub issue_id: String,
    pub number: i32,
    pub title: String,
    pub status: IssueStatus,
}

fn issue_ref_from_row(
    row: &crate::turso::Row,
    uri: String,
    project_key: String,
    offset: usize,
) -> DbResult<IssueRef> {
    Ok(IssueRef {
        uri,
        project_key,
        issue_id: row.text(offset)?,
        number: row.i64(offset + 1)? as i32,
        title: row.text(offset + 2)?,
        status: row
            .text(offset + 3)?
            .parse()
            .unwrap_or(IssueStatus::Backlog),
    })
}

pub fn canonicalize_issue_uri(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("issue URI must be a non-empty string".to_string());
    }

    match parse_uri(trimmed) {
        Some(CairnResource::Issue { project, number }) => Ok(build_issue_uri(&project, number)),
        _ => Err(format!(
            "issue URI must be a canonical issue URI like cairn://p/CAIRN/123: {trimmed}"
        )),
    }
}

pub async fn resolve_issue_uri(
    conn: &crate::turso::Connection,
    uri: &str,
) -> DbResult<Option<IssueRef>> {
    let canonical = canonicalize_issue_uri(uri).map_err(DbError::Row)?;
    let Some(CairnResource::Issue { project, number }) = parse_uri(&canonical) else {
        return Ok(None);
    };
    let project_key = cairn_common::uri::canonical_project(project);
    let mut rows = conn
        .query(
            "
            SELECT i.id, i.number, i.title, i.status
            FROM issues i
            JOIN projects p ON p.id = i.project_id
            WHERE p.key = ?1 COLLATE NOCASE AND i.number = ?2
            LIMIT 1
            ",
            params![project_key.as_str(), number as i64],
        )
        .await?;

    rows.next()
        .await?
        .map(|row| issue_ref_from_row(&row, canonical.clone(), project_key.clone(), 0))
        .transpose()
}

pub async fn issue_uri_for_id(conn: &crate::turso::Connection, issue_id: &str) -> DbResult<String> {
    let mut rows = conn
        .query(
            "
            SELECT p.key, i.number
            FROM issues i
            JOIN projects p ON p.id = i.project_id
            WHERE i.id = ?1
            LIMIT 1
            ",
            params![issue_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("issue not found: {issue_id}")))?;
    Ok(build_issue_uri(&row.text(0)?, row.i64(1)? as i32))
}

pub async fn issue_uri_for_id_db(db: &LocalDb, issue_id: &str) -> DbResult<String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move { issue_uri_for_id(conn, &issue_id).await })
    })
    .await
}

/// Resolve a `(project key, issue number)` pair to its issue id, if it exists.
/// The project key is matched case-insensitively, mirroring issue-URI lookups.
pub async fn issue_id_for_project_number(
    db: &LocalDb,
    project_key: &str,
    number: i32,
) -> DbResult<Option<String>> {
    let project_key = cairn_common::uri::canonical_project(project_key);
    db.read(|conn| {
        let project_key = project_key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.id
                     FROM issues i
                     JOIN projects p ON p.id = i.project_id
                     WHERE p.key = ?1 AND i.number = ?2
                     LIMIT 1",
                    params![project_key.as_str(), number],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

pub async fn issue_key_for_messages(db: &LocalDb, issue_id: &str) -> DbResult<String> {
    let uri = issue_uri_for_id_db(db, issue_id).await?;
    Ok(uri.trim_start_matches("cairn://p/").to_string())
}

/// The durable integration branch a child issue should inherit from its parent.
///
/// `issues.parent_issue_id` carries two independent meanings, and only one of
/// them is this function's business. It routes attention (a child wakes its
/// parent) AND it derives branches (a child branches from, and merges into, its
/// parent's integration branch). A thread parent takes the routing meaning and
/// refuses the derivation one: a thread owns no branch and never terminates via a
/// pull request, so a child of a thread derives exactly as if it had no parent —
/// `None` here is what makes the caller stamp the project default, which is in
/// turn the base ref the child's own pull request targets.
///
/// Keyed on the IMMEDIATE parent's kind, and it stops there: derivation never
/// walks up, so a thread that itself has a parent does not pass a grandparent's
/// branch through to its children. Symmetrically the refusal does not spread
/// downward — an ordinary issue living under a thread still confers its own
/// integration branch to its own children.
///
/// A child linked through parent_thread_id branches from the project base.
///
/// For an ordinary parent: the exact spawning job wins when it belongs to the
/// declared parent issue, regardless of terminal state. Otherwise the newest live
/// branch-bearing job on the parent issue is used. A missing branch falls back to
/// the project default at the caller; filesystem residence is never part of
/// branch authority.
pub async fn resolve_parent_branch(
    conn: &crate::turso::Connection,
    child_issue_id: &str,
) -> DbResult<Option<String>> {
    let mut parent_rows = conn
        .query(
            "
            SELECT child.parent_issue_id, child.parent_job_id, child.parent_thread_id
            FROM issues child
            WHERE child.id = ?1
            LIMIT 1
            ",
            params![child_issue_id],
        )
        .await?;
    let Some(parent_row) = parent_rows.next().await? else {
        return Ok(None);
    };
    if parent_row.opt_text(2)?.is_some() {
        return Ok(None);
    }
    let Some(parent_issue_id) = parent_row.opt_text(0)? else {
        return Ok(None);
    };
    let parent_job_id = parent_row.opt_text(1)?;

    // 1. Prefer the exact spawning coordinator job. Its durable branch remains
    // authoritative regardless of the job's current status.
    if let Some(parent_job_id) = parent_job_id.as_deref() {
        let mut job_rows = conn
            .query(
                "
                SELECT branch
                FROM jobs
                WHERE id = ?1
                  AND issue_id = ?2
                  AND branch IS NOT NULL
                LIMIT 1
                ",
                params![parent_job_id, parent_issue_id.as_str()],
            )
            .await?;
        if let Some(row) = job_rows.next().await? {
            return Ok(Some(row.text(0)?));
        }
    }

    // 2. Fall back to the newest live branch-bearing job on the parent issue.
    let mut branch_rows = conn
        .query(
            "
            SELECT branch
            FROM jobs
            WHERE issue_id = ?1
              AND branch IS NOT NULL
              AND status NOT IN ('complete', 'failed')
            ORDER BY created_at DESC
            LIMIT 1
            ",
            params![parent_issue_id.as_str()],
        )
        .await?;

    let Some(row) = branch_rows.next().await? else {
        return Ok(None);
    };
    Ok(Some(row.text(0)?))
}

/// Reject setting `child_issue_id`'s parent to `proposed_parent_id` when that
/// would form a parent-chain cycle. Each issue has at most one parent, so this
/// is a bounded linear walk up from the proposed parent; a self-parent is caught
/// on the first iteration.
pub async fn validate_no_parent_cycle(
    conn: &crate::turso::Connection,
    child_issue_id: &str,
    proposed_parent_id: &str,
) -> Result<(), String> {
    let mut current = Some(proposed_parent_id.to_string());
    let mut visited = HashSet::new();
    while let Some(id) = current {
        if id == child_issue_id {
            return Err("re-parenting would create a parent cycle".to_string());
        }
        if !visited.insert(id.clone()) {
            // Pre-existing data cycle that does not involve the child; stop.
            break;
        }
        let mut rows = conn
            .query(
                "SELECT parent_issue_id FROM issues WHERE id = ?1 LIMIT 1",
                params![id.as_str()],
            )
            .await
            .map_err(|e| e.to_string())?;
        current = match rows.next().await.map_err(|e| e.to_string())? {
            Some(row) => row.opt_text(0).map_err(|e| e.to_string())?,
            None => None,
        };
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("relations.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_issue(
        conn: &crate::turso::Connection,
        project_id: &str,
        id: &str,
        number: i32,
        title: &str,
        status: &str,
    ) {
        conn.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, '', ?5, ?5, 'none', 0, 1, 1)",
            params![id, project_id, number, title, status],
        )
        .await
        .unwrap();
    }

    async fn seed_project(conn: &crate::turso::Connection, id: &str, key: &str) {
        conn.execute(
            "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
            params![format!("w-{id}"), format!("Workspace {key}")],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, 1, 1)",
            params![id, format!("w-{id}"), format!("Project {key}"), key, format!("/tmp/{key}")],
        )
        .await
        .unwrap();
    }

    #[test]
    fn canonicalize_issue_uri_rejects_non_issue_uri() {
        assert_eq!(
            canonicalize_issue_uri("cairn://p/cairn/12").unwrap(),
            "cairn://p/cairn/12"
        );
        assert!(canonicalize_issue_uri("cairn://p/cairn/messages").is_err());
        assert!(canonicalize_issue_uri("").is_err());
    }

    #[tokio::test]
    async fn validate_no_parent_cycle_allows_acyclic() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "cairn").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-cairn", "i-b", 2, "B", "backlog").await;
                // No parent links yet: adopting B under A is acyclic.
                validate_no_parent_cycle(conn, "i-b", "i-a").await.unwrap();
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validate_no_parent_cycle_detects_cycle() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "cairn").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-cairn", "i-b", 2, "B", "backlog").await;
                // A's parent is B; adopting B under A would close the loop.
                conn.execute(
                    "UPDATE issues SET parent_issue_id = 'i-b' WHERE id = 'i-a'",
                    (),
                )
                .await
                .unwrap();
                let err = validate_no_parent_cycle(conn, "i-b", "i-a")
                    .await
                    .unwrap_err();
                assert!(err.contains("cycle"), "got: {err}");
                // A self-parent is caught on the first iteration.
                let err = validate_no_parent_cycle(conn, "i-a", "i-a")
                    .await
                    .unwrap_err();
                assert!(err.contains("cycle"), "got: {err}");
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
