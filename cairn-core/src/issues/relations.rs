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

/// The integration branch a child issue's job should base off — the parent
/// issue's live (non-terminal) job branch — or `None` when there is none to use.
///
/// `None` is returned in two cases, both of which route the child onto the
/// project default branch at every consumer (child base-branch resolution, PR
/// target, pack anchor): the issue has no parent; or the parent has no live job
/// with a worktree. The `worktree_path IS NOT NULL` filter is load-bearing: it
/// is what makes an ambient coordinator (Branch: main / `worktreeMode: none`)
/// route its children to the default branch. Such a coordinator has no worktree,
/// so its live job never matches and there is no parent integration branch to
/// hand down — the routing the deleted `childBase` flag used to force explicitly
/// now falls out of the worktree topology itself.
pub async fn resolve_parent_branch(
    conn: &cairn_db::turso::Connection,
    child_issue_id: &str,
) -> DbResult<Option<String>> {
    let mut parent_rows = conn
        .query(
            "SELECT parent_issue_id FROM issues WHERE id = ?1 LIMIT 1",
            params![child_issue_id],
        )
        .await?;
    let Some(parent_issue_id) = parent_rows
        .next()
        .await?
        .map(|row| row.opt_text(0))
        .transpose()?
        .flatten()
    else {
        return Ok(None);
    };

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
