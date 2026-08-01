use std::collections::{HashMap, HashSet};

use cairn_common::uri::{build_issue_uri, parse_uri, CairnResource};
use cairn_db::turso::params;
use serde::{Deserialize, Serialize};

use crate::models::{IssueKind, IssueStatus};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

#[derive(Debug, Clone, PartialEq)]
pub struct IssueRef {
    pub(crate) uri: String,
    pub(crate) project_key: String,
    pub(crate) issue_id: String,
    pub(crate) number: i32,
    pub(crate) title: String,
    pub(crate) status: IssueStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DependencyRef {
    uri: String,
    project_key: String,
    number: i32,
    title: String,
    status: IssueStatus,
    met: bool,
}

fn issue_ref_from_row(
    row: &cairn_db::turso::Row,
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

pub(crate) fn canonicalize_issue_uri(value: &str) -> Result<String, String> {
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

pub(crate) fn is_complete_status(status: &IssueStatus) -> bool {
    matches!(status, IssueStatus::Closed | IssueStatus::Merged)
}

pub(crate) async fn list_dependency_uris(
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

pub(crate) async fn list_dependency_uris_for_issues(
    conn: &cairn_db::turso::Connection,
    issue_ids: &[String],
) -> DbResult<HashMap<String, Vec<String>>> {
    if issue_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let issue_ids_json = serde_json::to_string(issue_ids)
        .map_err(|error| DbError::internal(format!("failed to serialize issue ids: {error}")))?;
    let mut rows = conn
        .query(
            "SELECT issue_id, depends_on_uri
             FROM issue_dependencies
             WHERE issue_id IN (SELECT value FROM json_each(?1))
             ORDER BY issue_id ASC, created_at ASC, depends_on_uri ASC",
            params![issue_ids_json],
        )
        .await?;
    let mut dependencies = HashMap::<String, Vec<String>>::new();
    while let Some(row) = rows.next().await? {
        dependencies
            .entry(row.text(0)?)
            .or_default()
            .push(row.text(1)?);
    }
    Ok(dependencies)
}

pub(crate) async fn list_dependent_issue_ids(
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

pub(crate) async fn resolve_issue_uri(
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
        .map(|row| issue_ref_from_row(&row, canonical.clone(), project_key.clone(), 0))
        .transpose()
}

pub(crate) async fn resolve_issue_uris(
    conn: &cairn_db::turso::Connection,
    uris: &[String],
) -> DbResult<HashMap<String, IssueRef>> {
    let mut requested = Vec::new();
    let mut seen = HashSet::new();
    for uri in uris {
        let canonical = canonicalize_issue_uri(uri).map_err(DbError::Row)?;
        if !seen.insert(canonical.clone()) {
            continue;
        }
        let Some(CairnResource::Issue { project, number }) = parse_uri(&canonical) else {
            continue;
        };
        requested.push(serde_json::json!({
            "uri": canonical,
            "project_key": project.to_uppercase(),
            "number": number,
        }));
    }
    if requested.is_empty() {
        return Ok(HashMap::new());
    }

    let requested_json = serde_json::to_string(&requested).map_err(|error| {
        DbError::internal(format!("failed to serialize dependency lookups: {error}"))
    })?;
    let mut rows = conn
        .query(
            "WITH requested AS (
                 SELECT json_extract(value, '$.uri') AS uri,
                        json_extract(value, '$.project_key') AS project_key,
                        CAST(json_extract(value, '$.number') AS INTEGER) AS number
                 FROM json_each(?1)
             )
             SELECT requested.uri, requested.project_key,
                    i.id, i.number, i.title, i.status
             FROM requested
             JOIN projects p ON p.key = requested.project_key
             JOIN issues i ON i.project_id = p.id AND i.number = requested.number",
            params![requested_json],
        )
        .await?;
    let mut resolved = HashMap::new();
    while let Some(row) = rows.next().await? {
        let uri = row.text(0)?;
        let project_key = row.text(1)?;
        resolved.insert(uri.clone(), issue_ref_from_row(&row, uri, project_key, 2)?);
    }
    Ok(resolved)
}

pub(crate) fn filter_unmet_dependencies_from_resolved(
    uris: &[String],
    resolved: &HashMap<String, IssueRef>,
) -> DbResult<Vec<String>> {
    let mut unmet = Vec::new();
    for uri in uris {
        let canonical = canonicalize_issue_uri(uri).map_err(DbError::Row)?;
        match resolved.get(&canonical) {
            Some(issue) if is_complete_status(&issue.status) => {}
            Some(issue) => unmet.push(issue.uri.clone()),
            None => unmet.push(canonical),
        }
    }
    Ok(unmet)
}

pub(crate) async fn issue_uri_for_id(
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

pub(crate) async fn issue_uri_for_id_db(db: &LocalDb, issue_id: &str) -> DbResult<String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move { issue_uri_for_id(conn, &issue_id).await })
    })
    .await
}

/// Resolve a `(project key, issue number)` pair to its issue id, if it exists.
/// The project key is matched case-insensitively, mirroring issue-URI lookups.
pub(crate) async fn issue_id_for_project_number(
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

/// Every thread in `project_key` whose title slugifies to `slug`, by issue
/// number ascending.
///
/// The slug is computed here rather than in SQL because
/// [`slugify_resource_segment`] is the single normalization rule shared with
/// every other cairn:// segment, and a second spelling of it in SQL would be a
/// second rule. Only rows whose kind is `thread` are candidates, so an ordinary
/// issue whose title slugifies identically is neither addressable by name nor
/// able to make one ambiguous. Status is not a filter: a resolved thread is
/// still a thread, and hiding it would turn a refusal into a wrong answer.
async fn thread_numbers_for_slug(
    db: &LocalDb,
    project_key: &str,
    slug: &str,
) -> DbResult<Vec<i32>> {
    let project_key = project_key.to_uppercase();
    let slug = slug.to_string();
    db.read(|conn| {
        let project_key = project_key.clone();
        let slug = slug.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT i.number, i.title
                     FROM issues i
                     JOIN projects p ON p.id = i.project_id
                     WHERE p.key = ?1 AND LOWER(i.kind) = 'thread'
                     ORDER BY i.number ASC",
                    params![project_key.as_str()],
                )
                .await?;
            let mut numbers = Vec::new();
            while let Some(row) = rows.next().await? {
                let title_slug = crate::config::slugify_resource_segment(&row.text(1)?);
                if !title_slug.is_empty() && title_slug == slug {
                    numbers.push(row.i64(0)? as i32);
                }
            }
            Ok(numbers)
        })
    })
    .await
}

/// Resolve a thread alias (`cairn://p/PROJECT/t/NAME`) to the canonical numbered
/// issue URI it addresses.
///
/// A thread's name is its title slugified, by the same ASCII rule that names
/// every other cairn:// segment, so `Design Review` answers to `design-review`
/// and the name follows the title through a retitle while the number — the
/// identity — never moves. The requested name is normalized the same way, so a
/// caller may write it either as typed or as a slug.
///
/// The rule keeps only `a-z0-9`, which has two consequences worth stating in
/// the refusal rather than leaving a caller to discover: a non-ASCII character
/// is a separator rather than a letter (`Café Sync` is named `caf-sync`, the
/// same name `Caf Sync` carries), and a title with no ASCII letters or digits
/// has no name at all and is reachable only by number.
///
/// A name no thread answers to, or one several answer to, is refused. Choosing
/// among several would make an address a coin flip; the refusal lists the
/// numbered URIs instead, which are the identities that cannot be ambiguous.
pub(crate) async fn resolve_thread_alias(
    db: &LocalDb,
    project_key: &str,
    name: &str,
) -> Result<String, String> {
    let project_key = project_key.to_uppercase();
    let slug = crate::config::slugify_resource_segment(name);
    let numbers = thread_numbers_for_slug(db, &project_key, &slug)
        .await
        .map_err(|error| format!("Failed to resolve thread name '{name}': {error}"))?;

    match numbers.as_slice() {
        [number] => Ok(build_issue_uri(&project_key, *number)),
        [] => Err(format!(
            "No thread in {project_key} is named '{name}'. A thread's name is its title \
             slugified: lowercased, with every run of characters outside a-z0-9 collapsed to a \
             single '-' — so 'Café Sync' is named 'caf-sync', and a title with no ASCII letters \
             or digits has no name and is reachable only by its number. Only threads are \
             addressable by name. List this project's threads with \
             cairn://p/{project_key}/issues?kind=thread."
        )),
        several => {
            let uris = several
                .iter()
                .map(|number| build_issue_uri(&project_key, *number))
                .collect::<Vec<_>>()
                .join(", ");
            Err(format!(
                "'{name}' names {} threads in {project_key}: {uris}. A name is an alias, not an \
                 identity, so Cairn will not pick one — read the thread you mean by its number, \
                 or retitle one so the name is unique.",
                several.len()
            ))
        }
    }
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
/// This is keyed on the kind rather than left to the fact that a thread cannot
/// currently run an execution (the guard in [`crate::execution::recipe`]). That
/// guard makes the right branch fall out by accident today; when threads gain
/// sessions of their own the accident stops holding, and the invariant has to be
/// written down where derivation happens rather than inferred from the absence of
/// jobs elsewhere.
///
/// For an ordinary parent: the exact spawning job wins when it belongs to the
/// declared parent issue, regardless of terminal state. Otherwise the newest live
/// branch-bearing job on the parent issue is used. A missing branch falls back to
/// the project default at the caller; filesystem residence is never part of
/// branch authority.
pub(crate) async fn resolve_parent_branch(
    conn: &cairn_db::turso::Connection,
    child_issue_id: &str,
) -> DbResult<Option<String>> {
    let mut parent_rows = conn
        .query(
            "
            SELECT child.parent_issue_id, child.parent_job_id, parent.kind
            FROM issues child
            LEFT JOIN issues parent ON parent.id = child.parent_issue_id
            WHERE child.id = ?1
            LIMIT 1
            ",
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

    // An unrecognized or absent kind reads as an ordinary issue, matching how
    // `crud` classifies a row it cannot parse: the column is a discriminator, and
    // failing to read it must not silently strip a real parent's branch.
    let parent_kind: IssueKind = parent_row
        .opt_text(2)?
        .and_then(|kind| kind.parse().ok())
        .unwrap_or_default();
    match parent_kind {
        // Exhaustive on purpose: a future kind is a compile error here, where the
        // question "does this kind confer a branch?" has to be answered, rather
        // than a silent fallthrough into an ordinary issue's derivation.
        IssueKind::Thread => return Ok(None),
        IssueKind::Issue => {}
    }

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

async fn validate_no_cycle(
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
pub(crate) async fn validate_no_parent_cycle(
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

pub(crate) async fn replace_dependencies(
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
async fn filter_unmet_dependencies(
    conn: &cairn_db::turso::Connection,
    uris: &[String],
) -> DbResult<Vec<String>> {
    let resolved = resolve_issue_uris(conn, uris).await?;
    filter_unmet_dependencies_from_resolved(uris, &resolved)
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
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'blocked', 'agent/parent', 10, 10)",
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
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'complete', 'agent/stale', 10, 10)",
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
    async fn resolve_parent_branch_uses_parent_coordinate_without_checkout() {
        let db = migrated_db().await;
        seed_parent_child(&db).await;

        // Filesystem residence is irrelevant: a branch-bearing parent job is a
        // complete inheritance coordinate even when it has no checkout.
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
                     VALUES(?1, 'p', 'parent', 'blocked', 'agent/parent', 10, 10)",
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
        assert_eq!(branch.as_deref(), Some("agent/parent"));
    }

    #[tokio::test]
    async fn resolve_parent_branch_ignores_parent_job_on_a_different_issue() {
        // `issues.parent_job_id` primarily records the CALLER's root job for wake
        // routing, which is not necessarily a job on the declared parent issue: a
        // run on issue A can reparent a child under issue B, recording A's job.
        // The exact-job fast path must NOT hand the caller's (issue A's) branch to
        // a child declared under issue B — it is gated on the job's `issue_id`
        // matching `parent_issue_id`. Here parent-b has no branch-bearing job, so
        // the child correctly resolves to no integration branch.
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
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('job-a', 'p', 'parent-a', 'blocked', 'agent/parent-a', 10, 10);
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
        // The coordinator-on-a-new-branch case: the spawner job is on the parent
        // issue itself. The child inherits its durable integration branch through
        // `parent_job_id` even after that coordinator job goes terminal,
        // which the non-terminal parent-issue fallback would miss. Under the base
        // branch target the coordinator job has no branch at all, so this lookup
        // finds none and the child falls through to the project default — the
        // whole child-branching difference between the two targets, with no
        // branch-target-specific code anywhere in this resolver.
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent', 'p', 1, 'Parent', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('coord-job', 'p', 'parent', 'complete', 'agent/coord', 10, 10);
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

    /// A thread confers attention, never a branch. The parent edge carries two
    /// independent meanings and a thread parent takes only the routing one, so a
    /// child of a thread derives exactly as if it had no parent — `None` here is
    /// what makes the caller stamp the project default.
    ///
    /// The thread is seeded WITH a live branch-bearing job on purpose. A thread
    /// cannot run an execution today (the guard in `execution::recipe`), so
    /// without this seeding the assertion would pass on the accident of a thread
    /// having no jobs rather than on the kind. This is the state a later slice
    /// makes reachable, and the invariant has to already hold there.
    #[tokio::test]
    async fn resolve_parent_branch_returns_none_for_a_thread_parent() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, kind)
            VALUES('thread', 'p', 1, 'Thread', 'backlog', 'backlog', 'none', 1, 1, 'thread');
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('thread-job', 'p', 'thread', 'blocked', 'agent/thread', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
            VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'thread');
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
            "a child of a thread must derive from the project default, not the thread's branch: {branch:?}"
        );
    }

    /// The exact-spawning-job fast path is keyed on the kind too. A job on the
    /// thread itself, named by `parent_job_id`, is the one coordinate that
    /// bypasses the live-job fallback — so a thread has to be refused before that
    /// lookup runs, not after it.
    #[tokio::test]
    async fn resolve_parent_branch_ignores_a_thread_parents_spawning_job() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, kind)
            VALUES('thread', 'p', 1, 'Thread', 'backlog', 'backlog', 'none', 1, 1, 'thread');
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('thread-job', 'p', 'thread', 'complete', 'agent/thread', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id, parent_job_id)
            VALUES('child', 'p', 2, 'Child', 'backlog', 'backlog', 'none', 2, 2, 'thread', 'thread-job');
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
            "the spawning-job fast path must not hand a thread's branch to its child: {branch:?}"
        );
    }

    /// Nothing forbids a thread row from having a parent of its own. Derivation
    /// reads the IMMEDIATE parent's kind and stops — it never walks through a
    /// thread to reach a grandparent's integration branch.
    #[tokio::test]
    async fn resolve_parent_branch_does_not_walk_past_a_thread_to_its_own_parent() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('grandparent', 'p', 1, 'Grandparent', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('grandparent-job', 'p', 'grandparent', 'blocked', 'agent/grandparent', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id, kind)
            VALUES('thread', 'p', 2, 'Thread', 'backlog', 'backlog', 'none', 2, 2, 'grandparent', 'thread');
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
            VALUES('child', 'p', 3, 'Child', 'backlog', 'backlog', 'none', 3, 3, 'thread');
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
            "derivation must stop at the thread, not inherit its parent's branch: {branch:?}"
        );
    }

    /// The rule is scoped to the immediate parent and does not spread down the
    /// chain. An ordinary issue that happens to live under a thread still confers
    /// its own integration branch to its own children — being a thread's child is
    /// not contagious.
    #[tokio::test]
    async fn resolve_parent_branch_still_inherits_from_an_ordinary_child_of_a_thread() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w', 'W', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p', 'w', 'Project', 'PROJ', '/tmp/repo', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, kind)
            VALUES('thread', 'p', 1, 'Thread', 'backlog', 'backlog', 'none', 1, 1, 'thread');
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
            VALUES('coordinator', 'p', 2, 'Coordinator', 'backlog', 'backlog', 'none', 2, 2, 'thread');
            INSERT INTO jobs(id, project_id, issue_id, status, branch, created_at, updated_at)
            VALUES('coordinator-job', 'p', 'coordinator', 'blocked', 'agent/coordinator', 10, 10);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at, parent_issue_id)
            VALUES('grandchild', 'p', 3, 'Grandchild', 'backlog', 'backlog', 'none', 3, 3, 'coordinator');
            ",
        )
        .await
        .unwrap();

        let branch = db
            .read(|conn| Box::pin(async move { resolve_parent_branch(conn, "grandchild").await }))
            .await
            .unwrap();
        assert_eq!(branch.as_deref(), Some("agent/coordinator"));
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
async fn unmet_dependency_uris(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<Vec<String>> {
    let uris = list_dependency_uris(conn, issue_id).await?;
    filter_unmet_dependencies(conn, &uris).await
}

async fn unmet_dependency_count(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
) -> DbResult<i64> {
    Ok(unmet_dependency_uris(conn, issue_id).await?.len() as i64)
}

pub(crate) async fn dependencies_ready(
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

    async fn seed_thread(
        conn: &cairn_db::turso::Connection,
        project_id: &str,
        id: &str,
        number: i32,
        title: &str,
    ) {
        seed_issue(conn, project_id, id, number, title, "backlog").await;
        conn.execute(
            "UPDATE issues SET kind = 'thread' WHERE id = ?1",
            params![id],
        )
        .await
        .unwrap();
    }

    async fn thread_fixture() -> LocalDb {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn, "p-cairn", "CAIRN").await;
                seed_thread(conn, "p-cairn", "t-design", 12, "Design Review").await;
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    /// The name is the title slugified, so both the slug and the title as typed
    /// reach the same thread — and what comes back is the numbered URI, which is
    /// the only identity the rest of the system ever sees.
    #[tokio::test]
    async fn thread_alias_resolves_to_the_numbered_issue_uri() {
        let db = thread_fixture().await;
        assert_eq!(
            resolve_thread_alias(&db, "CAIRN", "design-review")
                .await
                .unwrap(),
            "cairn://p/CAIRN/12"
        );
        assert_eq!(
            resolve_thread_alias(&db, "cairn", "Design Review")
                .await
                .unwrap(),
            "cairn://p/CAIRN/12"
        );
    }

    /// A name nothing answers to names the project and the attempt, explains how
    /// a name is derived, and points at the collection that lists the threads.
    #[tokio::test]
    async fn thread_alias_refuses_an_unknown_name_and_points_at_the_thread_list() {
        let db = thread_fixture().await;
        let error = resolve_thread_alias(&db, "CAIRN", "standup")
            .await
            .unwrap_err();
        assert!(error.contains("CAIRN"), "{error}");
        assert!(error.contains("'standup'"), "{error}");
        assert!(error.contains("slugified"), "{error}");
        // The refusal teaches the rule the resolver actually applies, including
        // its ASCII-only edges — a caller told "non-alphanumeric" would derive a
        // name for a title that in fact has none.
        assert!(error.contains("a-z0-9"), "{error}");
        assert!(error.contains("caf-sync"), "{error}");
        assert!(
            error.contains("cairn://p/CAIRN/issues?kind=thread"),
            "{error}"
        );
    }

    /// Two threads answering to one name is a question, not a coin flip: the
    /// refusal hands back every numbered URI rather than picking.
    #[tokio::test]
    async fn thread_alias_refuses_an_ambiguous_name_listing_every_match() {
        let db = thread_fixture().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_thread(conn, "p-cairn", "t-design-2", 40, "design review").await;
                Ok(())
            })
        })
        .await
        .unwrap();

        let error = resolve_thread_alias(&db, "CAIRN", "design-review")
            .await
            .unwrap_err();
        assert!(error.contains("cairn://p/CAIRN/12"), "{error}");
        assert!(error.contains("cairn://p/CAIRN/40"), "{error}");
        assert!(error.contains("2 threads"), "{error}");
    }

    /// Only threads live in the name space. An ordinary issue with the very same
    /// title is not addressable by name and does not make the name ambiguous.
    #[tokio::test]
    async fn an_ordinary_issue_is_not_addressable_by_name() {
        let db = thread_fixture().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "p-cairn", "i-design", 41, "Design Review", "backlog").await;
                Ok(())
            })
        })
        .await
        .unwrap();

        assert_eq!(
            resolve_thread_alias(&db, "CAIRN", "design-review")
                .await
                .unwrap(),
            "cairn://p/CAIRN/12"
        );

        db.write(|conn| {
            Box::pin(async move {
                seed_issue(conn, "p-cairn", "i-standup", 42, "Standup", "backlog").await;
                Ok(())
            })
        })
        .await
        .unwrap();
        assert!(resolve_thread_alias(&db, "CAIRN", "standup").await.is_err());
    }

    /// The slug rule keeps only `a-z0-9`, so a non-ASCII character is a
    /// separator rather than a letter: `Café Sync` is named `caf-sync`. Both
    /// sides go through the same normalizer, so the accented spelling a caller
    /// might reach for lands on the same thread — but a title that merely drops
    /// the accented character carries the identical name and becomes ambiguous
    /// with it, which is why the refusal states the rule rather than calling it
    /// "non-alphanumeric".
    #[tokio::test]
    async fn a_non_ascii_title_is_named_by_its_ascii_slug() {
        let db = thread_fixture().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_thread(conn, "p-cairn", "t-cafe", 20, "Caf\u{e9} Sync").await;
                Ok(())
            })
        })
        .await
        .unwrap();

        for written in ["caf-sync", "Caf\u{e9} Sync", "caf\u{e9}-sync"] {
            assert_eq!(
                resolve_thread_alias(&db, "CAIRN", written).await.unwrap(),
                "cairn://p/CAIRN/20",
                "{written}"
            );
        }

        // The accented character is a separator, so a title that simply omits
        // it carries the very same name and the two become ambiguous.
        db.write(|conn| {
            Box::pin(async move {
                seed_thread(conn, "p-cairn", "t-caf-sync", 21, "Caf Sync").await;
                Ok(())
            })
        })
        .await
        .unwrap();
        let error = resolve_thread_alias(&db, "CAIRN", "Caf\u{e9} Sync")
            .await
            .unwrap_err();
        assert!(error.contains("cairn://p/CAIRN/20"), "{error}");
        assert!(error.contains("cairn://p/CAIRN/21"), "{error}");
    }

    /// A title with no ASCII letters or digits slugifies to nothing, so it has no
    /// name. The empty slug must not become a wildcard that every such thread
    /// answers to — the thread stays reachable by its number alone.
    #[tokio::test]
    async fn a_title_with_no_ascii_characters_has_no_name() {
        let db = thread_fixture().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_thread(conn, "p-cairn", "t-jp", 30, "\u{65e5}\u{672c}\u{8a9e}").await;
                Ok(())
            })
        })
        .await
        .unwrap();

        for written in ["\u{65e5}\u{672c}\u{8a9e}", "", "---"] {
            let error = resolve_thread_alias(&db, "CAIRN", written)
                .await
                .unwrap_err();
            assert!(error.contains("reachable only by its number"), "{error}");
        }
    }

    /// The name follows the title, because it IS the title. A retitle retires the
    /// old name and mints the new one; the number the alias resolves to is the
    /// same number before and after, which is the whole reason the number is the
    /// identity.
    #[tokio::test]
    async fn a_retitled_thread_answers_to_its_new_name_only() {
        let db = thread_fixture().await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "UPDATE issues SET title = 'Architecture Review' WHERE id = 't-design'",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        assert!(resolve_thread_alias(&db, "CAIRN", "design-review")
            .await
            .is_err());
        assert_eq!(
            resolve_thread_alias(&db, "CAIRN", "architecture-review")
                .await
                .unwrap(),
            "cairn://p/CAIRN/12"
        );
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
