use std::collections::HashSet;

use cairn_common::uri::{build_issue_uri, parse_uri, CairnResource};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyRef {
    pub uri: String,
    pub project_key: String,
    pub number: i32,
    pub title: String,
    pub status: IssueStatus,
    pub met: bool,
}

pub fn canonicalize_issue_uri(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("dependency URI must be a non-empty string".to_string());
    }

    match parse_uri(trimmed) {
        Some(CairnResource::Issue { project, number }) => Ok(build_issue_uri(&project, number)),
        _ => Err(format!(
            "dependency URI must be a canonical issue URI like cairn://p/CAIRN/123: {trimmed}"
        )),
    }
}

pub fn is_complete_status(status: &IssueStatus) -> bool {
    matches!(status, IssueStatus::Closed | IssueStatus::Merged)
}

pub async fn list_dependency_uris(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT depends_on_uri FROM issue_dependencies WHERE issue_id = ?1 ORDER BY created_at ASC, depends_on_uri ASC",
            params![issue_id],
        )
        .await?;
    let mut dependencies = Vec::new();
    while let Some(row) = rows.next().await? {
        dependencies.push(row.text(0)?);
    }
    Ok(dependencies)
}

pub async fn list_dependent_issue_ids(
    conn: &cairn_db::turso::Connection,
    depends_on_uri: &str,
) -> DbResult<Vec<String>> {
    let canonical = canonicalize_issue_uri(depends_on_uri).map_err(DbError::Row)?;
    let mut rows = conn
        .query(
            "SELECT DISTINCT issue_id FROM issue_dependencies WHERE depends_on_uri = ?1 ORDER BY issue_id ASC",
            params![canonical.as_str()],
        )
        .await?;
    let mut issue_ids = Vec::new();
    while let Some(row) = rows.next().await? {
        issue_ids.push(row.text(0)?);
    }
    Ok(issue_ids)
}

pub async fn list_issue_dependencies(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<DependencyRef>> {
    let mut dependencies = Vec::new();
    for uri in list_dependency_uris(conn, issue_id).await? {
        let canonical = canonicalize_issue_uri(&uri).map_err(DbError::Row)?;
        let Some(CairnResource::Issue { project, number }) = parse_uri(&canonical) else {
            continue;
        };
        let project_key = project.to_uppercase();
        match resolve_issue_uri(conn, &canonical).await? {
            Some(resolved) => dependencies.push(DependencyRef {
                uri: resolved.uri,
                project_key: resolved.project_key,
                number: resolved.number,
                title: resolved.title,
                met: is_complete_status(&resolved.status),
                status: resolved.status,
            }),
            None => dependencies.push(DependencyRef {
                uri: canonical,
                project_key,
                number,
                title: "Missing issue".to_string(),
                status: IssueStatus::Backlog,
                met: false,
            }),
        }
    }
    Ok(dependencies)
}

pub async fn resolve_issue_uri(
    conn: &cairn_db::turso::Connection,
    uri: &str,
) -> DbResult<Option<IssueRef>> {
    let canonical = canonicalize_issue_uri(uri).map_err(DbError::Row)?;
    let Some(CairnResource::Issue { project, number }) = parse_uri(&canonical) else {
        return Ok(None);
    };
    let project_key = project.to_uppercase();
    let mut rows = conn
        .query(
            "
            SELECT i.id, i.number, i.title, i.status
            FROM issues i
            JOIN projects p ON p.id = i.project_id
            WHERE p.key = ?1 AND i.number = ?2
            LIMIT 1
            ",
            params![project_key.as_str(), number as i64],
        )
        .await?;

    rows.next()
        .await?
        .map(|row| {
            Ok(IssueRef {
                uri: canonical.clone(),
                project_key: project_key.clone(),
                issue_id: row.text(0)?,
                number: row.i64(1)? as i32,
                title: row.text(2)?,
                status: row.text(3)?.parse().unwrap_or(IssueStatus::Backlog),
            })
        })
        .transpose()
}

pub async fn issue_uri_for_id(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<String> {
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
    let project_key = project_key.to_uppercase();
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

/// The integration branch a child issue's job should base off — the branch of
/// the coordinator that spawned it — or `None` when there is none to use.
///
/// Resolution order:
///
/// 1. The child's recorded spawner (`issues.parent_job_id`), but only when that
///    job actually belongs to the declared `parent_issue_id`. `parent_job_id`
///    is primarily a wake-routing pointer to the CALLER's root job, which is not
///    always a job on the parent issue — a run on issue A can reparent a child
///    under issue B, recording A's job. The `AND issue_id = parent_issue_id`
///    guard keeps the branch authority tied to the declared parent so an adopted
///    child branches from the parent it was placed under, not the caller. For a
///    Feature coordinator this always matches: the coordinator runs on the
///    parent issue in its own worktree, so its job carries the worktree-backed
///    integration branch and the child inherits it directly. This path is not
///    gated on the spawner still being non-terminal: the coordinator's branch
///    stays the integration branch while the parent issue is open, even if the
///    coordinator agent is between turns or has finished its last turn. The
///    `worktree_path IS NOT NULL` guard is also load-bearing — a Manager
///    (ambient) coordinator has `worktree_path = NULL` and `branch = NULL`, so
///    it never matches and its children fall through to the default branch.
/// 2. Otherwise, the newest live (non-terminal) worktree-backed job on the
///    parent *issue*. This fallback covers manual adoption and older rows where
///    `parent_job_id` was not recorded.
///
/// `None` is returned when the issue has no parent, or neither lookup finds a
/// worktree-backed branch. Every consumer (child base-branch resolution, PR
/// target, pack anchor) then routes the child onto the project default branch.
pub async fn resolve_parent_branch(
    conn: &cairn_db::turso::Connection,
    child_issue_id: &str,
) -> DbResult<Option<String>> {
    let mut parent_rows = conn
        .query(
            "SELECT parent_issue_id, parent_job_id FROM issues WHERE id = ?1 LIMIT 1",
            params![child_issue_id],
        )
        .await?;
    let Some(parent_row) = parent_rows.next().await? else {
        return Ok(None);
    };
    let Some(parent_issue_id) = parent_row.opt_text(0)? else {
        return Ok(None);
    };
    let parent_job_id = parent_row.opt_text(1)?;

    // 1. Prefer the exact spawning coordinator job. Its branch is the
    //    integration branch regardless of the job's current status, as long as
    //    it is worktree-backed (Feature coordinator, not ambient Manager).
    if let Some(parent_job_id) = parent_job_id.as_deref() {
        let mut job_rows = conn
            .query(
                "
                SELECT branch
                FROM jobs
                WHERE id = ?1
                  AND issue_id = ?2
                  AND branch IS NOT NULL
                  AND worktree_path IS NOT NULL
                LIMIT 1
                ",
                params![parent_job_id, parent_issue_id.as_str()],
            )
            .await?;
        if let Some(row) = job_rows.next().await? {
            return Ok(Some(row.text(0)?));
        }
    }

    // 2. Fall back to the newest live worktree-backed job on the parent issue.
    let mut branch_rows = conn
        .query(
            "
            SELECT branch
            FROM jobs
            WHERE issue_id = ?1
              AND branch IS NOT NULL
              AND worktree_path IS NOT NULL
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

pub async fn validate_no_cycle(
    conn: &cairn_db::turso::Connection,
    current_uri: &str,
    proposed_dependencies: &[String],
) -> Result<(), String> {
    let current_uri = canonicalize_issue_uri(current_uri)?;
    let mut visited = HashSet::new();
    let mut stack: Vec<(String, Vec<String>)> = proposed_dependencies
        .iter()
        .map(|dependency| {
            let canonical = canonicalize_issue_uri(dependency)?;
            Ok((canonical.clone(), vec![current_uri.clone(), canonical]))
        })
        .collect::<Result<_, String>>()?;

    while let Some((uri, path)) = stack.pop() {
        if uri == current_uri {
            return Err(format!("dependency cycle: {}", path.join(" -> ")));
        }
        if !visited.insert(uri.clone()) {
            continue;
        }

        let Some(resolved) = resolve_issue_uri(conn, &uri)
            .await
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        let outgoing = list_dependency_uris(conn, &resolved.issue_id)
            .await
            .map_err(|error| error.to_string())?;
        for next in outgoing {
            let canonical_next = canonicalize_issue_uri(&next)?;
            let mut next_path = path.clone();
            next_path.push(canonical_next.clone());
            stack.push((canonical_next, next_path));
        }
    }

    Ok(())
}

/// Reject setting `child_issue_id`'s parent to `proposed_parent_id` when that
/// would form a parent-chain cycle. Each issue has at most one parent, so this
/// is a bounded linear walk up from the proposed parent; a self-parent is caught
/// on the first iteration.
pub async fn validate_no_parent_cycle(
    conn: &cairn_db::turso::Connection,
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

pub async fn replace_dependencies(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    dependencies: &[String],
    now: i64,
) -> Result<Vec<String>, String> {
    let current_uri = issue_uri_for_id(conn, issue_id)
        .await
        .map_err(|error| error.to_string())?;
    let mut canonical = Vec::with_capacity(dependencies.len());
    let mut seen = HashSet::new();
    for dependency in dependencies {
        let uri = canonicalize_issue_uri(dependency)?;
        if uri == current_uri {
            return Err(format!("dependency cycle: {current_uri} -> {uri}"));
        }
        if seen.insert(uri.clone()) {
            canonical.push(uri);
        }
    }

    validate_no_cycle(conn, &current_uri, &canonical).await?;

    conn.execute(
        "DELETE FROM issue_dependencies WHERE issue_id = ?1",
        params![issue_id],
    )
    .await
    .map_err(|error| error.to_string())?;

    for uri in &canonical {
        conn.execute(
            "INSERT INTO issue_dependencies (issue_id, depends_on_uri, created_at) VALUES (?1, ?2, ?3)",
            params![issue_id, uri.as_str(), now],
        )
        .await
        .map_err(|error| error.to_string())?;
    }

    Ok(canonical)
}

/// Filter a pre-listed set of dependency URIs down to those that have not yet
/// reached a complete status (Merged/Closed), preserving order. Unresolvable
/// URIs count as unmet and are returned in canonical form for display.
pub async fn filter_unmet_dependencies(
    conn: &cairn_db::turso::Connection,
    uris: &[String],
) -> DbResult<Vec<String>> {
    let mut unmet = Vec::new();
    for uri in uris {
        match resolve_issue_uri(conn, uri).await? {
            Some(resolved) if is_complete_status(&resolved.status) => {}
            Some(resolved) => unmet.push(resolved.uri),
            None => unmet.push(canonicalize_issue_uri(uri).unwrap_or_else(|_| uri.clone())),
        }
    }
    Ok(unmet)
}
#[cfg(test)]
mod parent_tests {
    use cairn_db::turso::params;

    use super::*;
    use crate::issues::crud;
    use crate::storage::LocalDb;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("parent-issue-relations.db").await
    }

    async fn seed_parent_child(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent', 'p', 1, 'Parent', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
            VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'parent');
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('orphan', 'p', 3, 'Orphan', 'backlog', 'backlog', 'none', 3, 3);
            ",
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn resolve_parent_branch_uses_live_parent_job_branch() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        assert!(db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap()
            .is_none());
        assert!(db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "orphan").await }))
            .await
            .unwrap()
            .is_none());

        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'blocked', 'agent/parent', '/tmp/parent', 10, 10)",
                    params!["parent-job"],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/parent"));
    }

    #[tokio::test]
    async fn resolve_parent_branch_ignores_terminal_parent_jobs() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'complete', 'agent/stale', '/tmp/parent', 10, 10)",
                    params!["terminal-parent-job"],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert!(branch.is_none());
    }

    #[tokio::test]
    async fn resolve_parent_branch_none_when_parent_job_has_no_worktree() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        // An ambient (Branch: main / worktreeMode: none) coordinator's live job
        // carries a branch but no worktree_path. The `worktree_path IS NOT NULL`
        // filter excludes it, so the child routes to the default branch — the
        // structural replacement for the deleted childBase mechanism.
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'blocked', 'agent/parent', NULL, 10, 10)",
                    params!["ambient-parent-job"],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert!(branch.is_none());
    }

    #[tokio::test]
    async fn resolve_parent_branch_ignores_parent_job_on_a_different_issue() {
        // `issues.parent_job_id` primarily records the CALLER's root job for wake
        // routing, which is not necessarily a job on the declared parent issue: a
        // run on issue A can reparent a child under issue B, recording A's job.
        // The exact-job fast path must NOT hand the caller's (issue A's) branch to
        // a child declared under issue B — it is gated on the job's `issue_id`
        // matching `parent_issue_id`. Here parent-b has no worktree-backed job, so
        // the child correctly resolves to no integration branch (default fallback).
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent-a', 'p', 1, 'Parent A', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent-b', 'p', 2, 'Parent B', 'backlog', 'backlog', 'none', 2, 2);
            INSERT INTO jobs(id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
            VALUES('job-a', 'p', 'parent-a', 'blocked', 'agent/parent-a', '/tmp/parent-a', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id, parent_job_id)
            VALUES('child', 'p', 3, 'Child', 'backlog', 'backlog', 'none', 3, 3, 'parent-b', 'job-a');
            ",
        )
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert!(
            branch.is_none(),
            "a child under parent-b must not inherit the caller job's branch on parent-a: {branch:?}"
        );
    }

    #[tokio::test]
    async fn resolve_parent_branch_uses_matching_parent_job_even_when_terminal() {
        // The Feature coordinator case: the spawner job is on the parent issue
        // itself. The child inherits its worktree-backed integration branch
        // through `parent_job_id` even after that coordinator job goes terminal,
        // which the non-terminal parent-issue fallback would miss.
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent', 'p', 1, 'Parent', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, branch, worktree_path, created_at, updated_at)
            VALUES('coord-job', 'p', 'parent', 'complete', 'agent/coord', '/tmp/coord', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id, parent_job_id)
            VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'parent', 'coord-job');
            ",
        )
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "child").await }))
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/coord"));
    }

    #[tokio::test]
    async fn list_children_returns_children_for_parent() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        let children = crud::list_children(&db, "parent").await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, "child");
        assert_eq!(children[0].parent_issue_id.as_deref(), Some("parent"));
    }
}

/// Canonical issue URIs of this issue's dependencies that have not yet reached
/// Merged or Closed. These are what the issue is currently "blocked on".
pub async fn unmet_dependency_uris(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let uris = list_dependency_uris(conn, issue_id).await?;
    filter_unmet_dependencies(conn, &uris).await
}

pub async fn unmet_dependency_count(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<i64> {
    Ok(unmet_dependency_uris(conn, issue_id).await?.len() as i64)
}

pub async fn dependencies_ready(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<bool> {
    Ok(unmet_dependency_count(conn, issue_id).await? == 0)
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
        conn: &cairn_db::turso::Connection,
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

    async fn seed_project(conn: &cairn_db::turso::Connection, id: &str, key: &str) {
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
            canonicalize_issue_uri("cairn://p/CAIRN/12").unwrap(),
            "cairn://p/CAIRN/12"
        );
        assert!(canonicalize_issue_uri("cairn://p/CAIRN/messages").is_err());
        assert!(canonicalize_issue_uri("").is_err());
    }

    #[tokio::test]
    async fn replace_dependencies_deduplicates_canonical_uris() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-cairn", "i-b", 2, "B", "backlog").await;
                seed_issue(conn, "p-cairn", "i-c", 3, "C", "backlog").await;

                let replaced = replace_dependencies(
                    conn,
                    "i-a",
                    &[
                        " cairn://p/CAIRN/2 ".to_string(),
                        "cairn://p/CAIRN/3".to_string(),
                        "cairn://p/CAIRN/2".to_string(),
                    ],
                    2,
                )
                .await
                .unwrap();

                assert_eq!(replaced, vec!["cairn://p/CAIRN/2", "cairn://p/CAIRN/3"]);
                assert_eq!(list_dependency_uris(conn, "i-a").await.unwrap(), replaced);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn replace_dependencies_rejects_cross_project_cycle() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
                seed_project(conn, "p-agg", "AGG").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-agg", "i-b", 2, "B", "backlog").await;
                replace_dependencies(conn, "i-a", &["cairn://p/AGG/2".to_string()], 2)
                    .await
                    .unwrap();
                let error =
                    replace_dependencies(conn, "i-b", &["cairn://p/CAIRN/1".to_string()], 3)
                        .await
                        .unwrap_err();
                assert!(error.contains("dependency cycle"));
                assert!(error.contains("cairn://p/AGG/2"));
                assert!(error.contains("cairn://p/CAIRN/1"));
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn dependencies_ready_requires_resolved_complete_dependencies() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-cairn", "i-b", 2, "B", "active").await;
                replace_dependencies(conn, "i-a", &["cairn://p/CAIRN/2".to_string()], 2)
                    .await
                    .unwrap();
                assert!(!dependencies_ready(conn, "i-a").await.unwrap());
                assert_eq!(unmet_dependency_count(conn, "i-a").await.unwrap(), 1);
                conn.execute("UPDATE issues SET status = 'closed' WHERE id = 'i-b'", ())
                    .await
                    .unwrap();
                assert!(dependencies_ready(conn, "i-a").await.unwrap());
                assert_eq!(unmet_dependency_count(conn, "i-a").await.unwrap(), 0);
                replace_dependencies(conn, "i-a", &["cairn://p/MISSING/99".to_string()], 3)
                    .await
                    .unwrap();
                assert!(!dependencies_ready(conn, "i-a").await.unwrap());
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn unmet_dependency_uris_returns_only_incomplete_and_missing_dependencies() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-cairn", "i-b", 2, "B", "merged").await;
                seed_issue(conn, "p-cairn", "i-c", 3, "C", "active").await;
                replace_dependencies(
                    conn,
                    "i-a",
                    &[
                        "cairn://p/CAIRN/2".to_string(),
                        "cairn://p/CAIRN/3".to_string(),
                        "cairn://p/CAIRN/99".to_string(),
                    ],
                    2,
                )
                .await
                .unwrap();

                // #2 is merged (met); #3 is active and #99 is missing (both unmet).
                let unmet = unmet_dependency_uris(conn, "i-a").await.unwrap();
                assert_eq!(
                    unmet,
                    vec![
                        "cairn://p/CAIRN/3".to_string(),
                        "cairn://p/CAIRN/99".to_string(),
                    ]
                );
                assert_eq!(unmet_dependency_count(conn, "i-a").await.unwrap(), 2);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn list_dependent_issue_ids_finds_same_and_cross_project_dependents() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
                seed_project(conn, "p-agg", "AGG").await;
                seed_issue(conn, "p-cairn", "i-a", 1, "A", "backlog").await;
                seed_issue(conn, "p-cairn", "i-b", 2, "B", "backlog").await;
                seed_issue(conn, "p-agg", "i-c", 3, "C", "backlog").await;
                replace_dependencies(conn, "i-b", &["cairn://p/CAIRN/1".to_string()], 2)
                    .await
                    .unwrap();
                replace_dependencies(conn, "i-c", &["cairn://p/CAIRN/1".to_string()], 2)
                    .await
                    .unwrap();

                assert_eq!(
                    list_dependent_issue_ids(conn, "cairn://p/CAIRN/1")
                        .await
                        .unwrap(),
                    vec!["i-b".to_string(), "i-c".to_string()]
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn validate_no_parent_cycle_allows_acyclic() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
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
                seed_project(conn, "p-cairn", "CAIRN").await;
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
