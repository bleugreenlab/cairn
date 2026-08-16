//! Issue CRUD operations.

use crate::error::CairnError;
use crate::issues::relations;
use crate::labels::attach;
use crate::models::{
    CreateIssue, Issue, IssueAttention, IssueProgress, IssueStatus, Label, UpdateIssue,
};
use crate::services::Clock;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use crate::transitions::Resolution;
use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalPosition,
    PrincipalRef, VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
};
use cairn_common::ids;
use cairn_db::turso::params;
use std::sync::Arc;

const ISSUE_COLUMNS: &str = "id, project_id, number, title, description, status, progress,
    attention, priority, completed_at, dismissed_at, created_at, updated_at, model,
    merged_at, closed_at, parent_issue_id, parent_thread_id, author_principal_json,
    appearance_snapshot_json";

fn db_internal(message: impl Into<String>) -> DbError {
    DbError::internal(message.into())
}

/// Capture authorship for a decision made autonomously by this Cairn installation.
///
/// The stable installation device is the actor. `LocalInvoke` records that the
/// decision arose inside the local process, while an unverified record avoids
/// fabricating a human, agent, credential, or external authentication event.
pub fn installation_machine_authorship(
    device_id: impl Into<String>,
    decided_at: i64,
) -> DbResult<IssueAuthorship> {
    let author = PrincipalRef::Machine {
        device_id: device_id.into(),
    };
    let verification = VerificationRecord::new(
        VerificationMethod::DesktopCredential,
        VerificationStatus::None,
        None,
        None,
        None,
        None,
        VerificationStrength::new("local_process")
            .map_err(|e| db_internal(format!("invalid installation verification: {e}")))?,
        decided_at,
    )
    .map_err(|e| db_internal(format!("invalid installation verification: {e}")))?;
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::LocalInvoke,
        Address::None,
        verification,
        decided_at,
        None,
    )
    .map_err(|e| db_internal(format!("invalid installation appearance evidence: {e}")))?;
    let appearance = AppearanceSnapshot::new(author.clone(), evidence, vec![], None)
        .map_err(|e| db_internal(format!("invalid installation appearance snapshot: {e}")))?;
    IssueAuthorship::new(author, appearance)
}

/// Validated, immutable provenance captured when an issue is created.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssueAuthorship {
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
}

impl IssueAuthorship {
    pub fn new(author: PrincipalRef, appearance: AppearanceSnapshot) -> DbResult<Self> {
        author
            .validate_at(PrincipalPosition::DecisionActor)
            .map_err(|e| db_internal(format!("invalid issue author: {e}")))?;
        appearance
            .validate()
            .map_err(|e| db_internal(format!("invalid issue appearance snapshot: {e}")))?;
        if appearance.principal() != &author {
            return Err(db_internal(
                "issue author does not match appearance snapshot principal",
            ));
        }
        Ok(Self { author, appearance })
    }
}

pub fn encode_issue_authorship(authorship: &IssueAuthorship) -> DbResult<(String, String)> {
    let validated = IssueAuthorship::new(authorship.author.clone(), authorship.appearance.clone())?;
    let author_json = serde_json::to_string(&validated.author)
        .map_err(|e| db_internal(format!("issue author is not serializable: {e}")))?;
    let appearance_json = serde_json::to_string(&validated.appearance).map_err(|e| {
        db_internal(format!(
            "issue appearance snapshot is not serializable: {e}"
        ))
    })?;
    Ok((author_json, appearance_json))
}

pub fn decode_issue_authorship(
    author_json: Option<String>,
    appearance_json: Option<String>,
) -> DbResult<Option<IssueAuthorship>> {
    match (author_json, appearance_json) {
        (None, None) => Ok(None),
        (Some(author_json), Some(appearance_json)) => {
            let author = serde_json::from_str::<PrincipalRef>(&author_json)
                .map_err(|e| db_internal(format!("unreadable issue author: {e}")))?;
            let appearance = serde_json::from_str::<AppearanceSnapshot>(&appearance_json)
                .map_err(|e| db_internal(format!("unreadable issue appearance snapshot: {e}")))?;
            IssueAuthorship::new(author, appearance).map(Some)
        }
        _ => Err(db_internal(
            "issue authorship must contain both author and appearance snapshot or neither",
        )),
    }
}

/// Return only the rows that can contribute to the Nav/tray active projection.
///
/// Callers merge one bounded result from each open database and truncate the
/// global ordering. Keeping the visibility predicate and ordering in this one
/// query prevents the tray from developing a second definition of the Nav list.
pub async fn list_sidebar_active(
    db: &LocalDb,
    limit: usize,
) -> Result<Vec<crate::models::SidebarActiveIssue>, CairnError> {
    db.query_all(
        "SELECT p.id, p.name, i.number, i.title,
                CASE i.status WHEN 'active' THEN 0 ELSE 1 END
         FROM projects p
         JOIN issues i ON i.project_id = p.id
         WHERE p.hidden = 0
           AND NOT (p.is_workspace != 0 AND p.workspace_id != 'default')
           AND i.dismissed_at IS NULL
           AND i.status IN ('active', 'waiting')
         ORDER BY p.name ASC,
                  CASE i.status WHEN 'active' THEN 0 ELSE 1 END ASC,
                  i.number DESC
         LIMIT ?1",
        params![limit as i64],
        |row| {
            Ok(crate::models::SidebarActiveIssue {
                project_id: row.text(0)?,
                project_name: row.text(1)?,
                issue_number: row.i64(2)? as i32,
                issue_title: row.text(3)?,
                status_rank: row.i64(4)? as i32,
            })
        },
    )
    .await
    .map_err(CairnError::from)
}

/// Resolve the database that owns the issue with this `id`. An O(1) prefix parse,
/// fail-closed.
///
/// Delegates to [`crate::execution::routing::routing_db_for_id`]: a bare (local)
/// issue id routes to the private database exactly as the prior `&db.local` path
/// did, while a `{team}~…` id routes to that team's open replica. Fail-closed — a
/// team-prefixed id whose replica is not open returns an error rather than
/// silently falling back to the private database (the CAIRN-2170 split-brain
/// class).
pub async fn owning_db_for_issue(
    dbs: &crate::db::DbState,
    issue_id: &str,
) -> Result<Arc<LocalDb>, CairnError> {
    crate::execution::routing::routing_db_for_id(dbs, issue_id).await
}

/// Resolve the database that owns `input.project_id` and create the issue there.
///
/// The desktop `create_issue` Tauri command historically wrote straight to the
/// private database, so creating an issue in a team project — whose `projects`
/// row lives only in the team replica — failed `project not found` (CAIRN-2184).
/// This mirrors the agent path's `for_project` routing (CAIRN-2181) and
/// [`crate::projects::crud::create_routed`]: scan for the project's owning
/// database (the private DB for a local project, the team replica for a team
/// one) and insert there. Returns the created issue alongside the database it
/// landed in so the caller embeds and reads it back from the same place. For a
/// local project this is a strict no-op — `owning_db` short-circuits on the
/// private database.
pub async fn create_routed(
    dbs: &crate::db::DbState,
    clock: &dyn Clock,
    input: CreateIssue,
    authorship: IssueAuthorship,
) -> Result<(Issue, Arc<LocalDb>), CairnError> {
    let owning_db = crate::projects::crud::owning_db(dbs, &input.project_id).await?;
    let issue = create(&owning_db, clock, input, authorship).await?;
    Ok((issue, owning_db))
}

pub async fn list_children(db: &LocalDb, parent_issue_id: &str) -> Result<Vec<Issue>, CairnError> {
    let parent_issue_id = parent_issue_id.to_string();
    db.read(|conn| {
        let parent_issue_id = parent_issue_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {ISSUE_COLUMNS}
                 FROM issues
                 WHERE parent_issue_id = ?1
                 ORDER BY number DESC"
            );
            let mut rows = conn.query(&sql, params![parent_issue_id.as_str()]).await?;
            let mut issues = Vec::new();
            while let Some(row) = rows.next().await? {
                issues.push(issue_from_row(&row)?);
            }
            hydrate_issue_relations(conn, &mut issues).await?;
            Ok(issues)
        })
    })
    .await
    .map_err(CairnError::from)
}

fn issue_from_row(row: &cairn_db::turso::Row) -> DbResult<Issue> {
    let authorship = decode_issue_authorship(row.opt_text(18)?, row.opt_text(19)?)?;
    Ok(Issue {
        id: row.text(0)?,
        project_id: row.text(1)?,
        number: row.i64(2)? as i32,
        title: row.text(3)?,
        description: row.opt_text(4)?.unwrap_or_default(),
        status: row.text(5)?.parse().unwrap_or(IssueStatus::Backlog),
        progress: row.text(6)?.parse().unwrap_or(IssueProgress::Backlog),
        attention: row.text(7)?.parse().unwrap_or(IssueAttention::None),
        priority: row.opt_i64(8)?.unwrap_or(0) as i32,
        completed_at: row.opt_i64(9)?,
        dismissed_at: row.opt_i64(10)?,
        created_at: row.i64(11)?,
        updated_at: row.i64(12)?,
        author: authorship.map(|value| value.author),
        backend_override: row.opt_text(13)?,
        merged_at: row.opt_i64(14)?,
        closed_at: row.opt_i64(15)?,
        parent_issue_id: row.opt_text(16)?,
        parent_thread_id: row.opt_text(17)?,
        unmet_dependency_count: 0,
        depends_on: Vec::new(),
        unmet_depends_on: Vec::new(),
        labels: Vec::new(),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelationQuery {
    Dependencies,
    ResolvedDependencies,
    Labels,
}

async fn hydrate_issue_relations(
    conn: &cairn_db::turso::Connection,
    issues: &mut [Issue],
) -> DbResult<()> {
    hydrate_issue_relations_with_observer(conn, issues, &mut |_| {}).await
}

async fn hydrate_issue_relations_with_observer(
    conn: &cairn_db::turso::Connection,
    issues: &mut [Issue],
    observe_query: &mut impl FnMut(RelationQuery),
) -> DbResult<()> {
    if issues.is_empty() {
        return Ok(());
    }

    let issue_ids = issues
        .iter()
        .map(|issue| issue.id.clone())
        .collect::<Vec<_>>();
    observe_query(RelationQuery::Dependencies);
    let mut dependencies = relations::list_dependency_uris_for_issues(conn, &issue_ids).await?;
    let dependency_uris = dependencies
        .values()
        .flat_map(|uris| uris.iter().cloned())
        .collect::<Vec<_>>();
    let resolved = if dependency_uris.is_empty() {
        Default::default()
    } else {
        observe_query(RelationQuery::ResolvedDependencies);
        relations::resolve_issue_uris(conn, &dependency_uris).await?
    };
    observe_query(RelationQuery::Labels);
    let mut labels = attach::list_labels_for_issues(conn, &issue_ids).await?;

    for issue in issues {
        issue.depends_on = dependencies.remove(&issue.id).unwrap_or_default();
        issue.unmet_depends_on =
            relations::filter_unmet_dependencies_from_resolved(&issue.depends_on, &resolved)?;
        issue.unmet_dependency_count = issue.unmet_depends_on.len() as i64;
        issue.labels = labels.remove(&issue.id).unwrap_or_default();
    }
    Ok(())
}

async fn load_optional_conn(
    conn: &cairn_db::turso::Connection,
    id: &str,
) -> DbResult<Option<Issue>> {
    let sql = format!("SELECT {ISSUE_COLUMNS} FROM issues WHERE id = ?1");
    let mut rows = conn.query(&sql, params![id]).await?;
    let mut issue = rows
        .next()
        .await?
        .map(|row| issue_from_row(&row))
        .transpose()?;
    if let Some(issue) = &mut issue {
        hydrate_issue_relations(conn, std::slice::from_mut(issue)).await?;
    }
    Ok(issue)
}

pub(crate) async fn load_conn(conn: &cairn_db::turso::Connection, id: &str) -> DbResult<Issue> {
    load_optional_conn(conn, id)
        .await?
        .ok_or_else(|| db_internal(format!("issue not found: {id}")))
}

/// Additional immutable relationships established by the canonical issue insert.
#[derive(Debug, Clone, Default)]
pub struct IssueCreationContext {
    pub parent_uri: Option<String>,
    pub inferred_parent_thread_id: Option<String>,
    pub parent_job_id: Option<String>,
}

/// The parent an issue hangs from: exactly one of another issue or a thread.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParentEdge {
    Issue(String),
    Thread(String),
}

pub(crate) async fn resolve_parent_edge(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    parent_uri: &str,
) -> DbResult<ParentEdge> {
    let resolved_issue = match cairn_common::uri::parse_uri(parent_uri) {
        Some(cairn_common::uri::CairnResource::Issue { .. }) => {
            relations::resolve_issue_uri(conn, parent_uri).await?
        }
        _ => None,
    };
    if let Some(resolved) = resolved_issue {
        let mut rows = conn
            .query(
                "SELECT project_id FROM issues WHERE id = ?1 LIMIT 1",
                params![resolved.issue_id.as_str()],
            )
            .await?;
        let parent_project_id = rows
            .next()
            .await?
            .ok_or_else(|| DbError::Row(format!("parent issue or thread not found: {parent_uri}")))?
            .text(0)?;
        if parent_project_id != project_id {
            return Err(DbError::Row(format!(
                "parent issue or thread must be in the same project: {parent_uri}"
            )));
        }
        return Ok(ParentEdge::Issue(resolved.issue_id));
    }
    if let Some((thread_id, thread_project_id, _)) =
        crate::threads::resolve_parent_thread_uri_conn(conn, parent_uri).await?
    {
        if thread_project_id != project_id {
            return Err(DbError::Row(format!(
                "parent issue or thread must be in the same project: {parent_uri}"
            )));
        }
        return Ok(ParentEdge::Thread(thread_id));
    }
    Err(DbError::Row(format!(
        "parent issue or thread not found: {parent_uri}"
    )))
}

pub async fn create(
    db: &LocalDb,
    clock: &dyn Clock,
    input: CreateIssue,
    authorship: IssueAuthorship,
) -> Result<Issue, CairnError> {
    let (issue, _) = create_with_context(
        db,
        clock,
        input,
        authorship,
        IssueCreationContext::default(),
    )
    .await?;
    Ok(issue)
}

/// Allocate and insert one issue, including provenance, parent edges and labels,
/// in a single transaction. All issue creation paths terminate here.
pub async fn create_with_context(
    db: &LocalDb,
    clock: &dyn Clock,
    input: CreateIssue,
    authorship: IssueAuthorship,
    context: IssueCreationContext,
) -> Result<(Issue, Vec<Label>), CairnError> {
    let CreateIssue {
        project_id,
        title,
        description,
        backend_override,
        label_ids,
    } = input;
    let id = ids::mint_child(&project_id);
    let now = clock.now();
    let (author_json, appearance_json) = encode_issue_authorship(&authorship)?;

    db.write(|conn| {
        let id = id.clone();
        let project_id = project_id.clone();
        let title = title.clone();
        let description = description.clone();
        let backend_override = backend_override.clone();
        let label_ids = label_ids.clone();
        let context = context.clone();
        let author_json = author_json.clone();
        let appearance_json = appearance_json.clone();
        Box::pin(async move {
            let (parent_issue_id, parent_thread_id) = match context.parent_uri.as_deref() {
                Some(parent_uri) => match resolve_parent_edge(conn, &project_id, parent_uri).await?
                {
                    ParentEdge::Issue(id) => (Some(id), None),
                    ParentEdge::Thread(id) => (None, Some(id)),
                },
                None => (None, context.inferred_parent_thread_id),
            };
            let parent_job_id = if parent_issue_id.is_some() {
                context.parent_job_id
            } else {
                None
            };

            let mut rows = conn
                .query(
                    "SELECT next_issue_number FROM projects WHERE id = ?1",
                    params![project_id.as_str()],
                )
                .await?;
            let row = rows.next().await?.ok_or_else(|| {
                DbError::Row(format!("project not found: {}", project_id.as_str()))
            })?;
            let number = row.opt_i64(0)?.unwrap_or(1) as i32;
            conn.execute(
                "UPDATE projects SET next_issue_number = ?1, updated_at = ?2 WHERE id = ?3",
                params![number + 1, now, project_id.as_str()],
            )
            .await?;
            conn.execute(
                "INSERT INTO issues (
                    id, project_id, number, title, description, status, progress, attention,
                    priority, created_at, updated_at, model, parent_issue_id, parent_job_id,
                    parent_thread_id, author_principal_json, appearance_snapshot_json
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, 'backlog', 'backlog', 'none', 0, ?6, ?6, ?7,
                    ?8, ?9, ?10, ?11, ?12
                 )",
                params![
                    id.as_str(),
                    project_id.as_str(),
                    number,
                    title.as_str(),
                    description.as_deref(),
                    now,
                    backend_override.as_deref(),
                    parent_issue_id.as_deref(),
                    parent_job_id.as_deref(),
                    parent_thread_id.as_deref(),
                    author_json.as_str(),
                    appearance_json.as_str()
                ],
            )
            .await?;
            let created_labels = match label_ids {
                Some(labels) => attach::replace_issue_labels(conn, &id, &labels, now)
                    .await
                    .map_err(DbError::Row)?,
                None => Vec::new(),
            };
            let issue = load_conn(conn, &id).await?;
            Ok((issue, created_labels))
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(message) if message.starts_with("project not found: ") => {
            CairnError::NotFound {
                entity: "project",
                id: project_id,
            }
        }
        error => CairnError::from(error),
    })
}

pub async fn get(db: &LocalDb, id: &str) -> Result<Option<Issue>, CairnError> {
    let id = id.to_string();
    db.read(|conn| {
        let id = id.clone();
        Box::pin(async move { load_optional_conn(conn, &id).await })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn list(db: &LocalDb, project_id: &str) -> Result<Vec<Issue>, CairnError> {
    let project_id = project_id.to_string();
    db.read(|conn| {
        let project_id = project_id.clone();
        Box::pin(async move {
            let sql = format!(
                "SELECT {ISSUE_COLUMNS}
                 FROM issues
                 WHERE project_id = ?1
                 ORDER BY number DESC"
            );
            let mut rows = conn.query(&sql, params![project_id.as_str()]).await?;
            let mut issues = Vec::new();
            while let Some(row) = rows.next().await? {
                issues.push(issue_from_row(&row)?);
            }
            hydrate_issue_relations(conn, &mut issues).await?;
            Ok(issues)
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn update(
    db: &LocalDb,
    clock: &dyn Clock,
    input: UpdateIssue,
) -> Result<Issue, CairnError> {
    let UpdateIssue {
        id,
        title,
        description,
        backend_override,
        depends_on,
        label_ids,
    } = input;
    let backend_present = backend_override.is_some();
    let backend_value = backend_override.flatten();
    let now = clock.now();

    db.write(|conn| {
        let id = id.clone();
        let title = title.clone();
        let description = description.clone();
        let backend_value = backend_value.clone();
        let depends_on = depends_on.clone();
        let label_ids = label_ids.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues
                 SET title = COALESCE(?1, title),
                     description = CASE WHEN ?2 IS NULL THEN description ELSE ?2 END,
                     model = CASE WHEN ?3 = 0 THEN model WHEN ?4 IS NULL THEN NULL ELSE ?4 END,
                     updated_at = ?5
                 WHERE id = ?6",
                params![
                    title.as_deref(),
                    description.as_deref(),
                    if backend_present { 1 } else { 0 },
                    backend_value.as_deref(),
                    now,
                    id.as_str()
                ],
            )
            .await?;
            if let Some(dependencies) = depends_on {
                relations::replace_dependencies(conn, &id, &dependencies, now)
                    .await
                    .map_err(DbError::Row)?;
            }
            if let Some(labels) = label_ids {
                attach::replace_issue_labels(conn, &id, &labels, now)
                    .await
                    .map_err(DbError::Row)?;
            }
            load_conn(conn, &id).await
        })
    })
    .await
    .map_err(|error| match error {
        DbError::Row(message) if message.starts_with("issue not found: ") => CairnError::NotFound {
            entity: "issue",
            id,
        },
        error => CairnError::from(error),
    })
}

pub async fn dismiss(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    set_dismissed(db, clock, id, Some(clock.now())).await
}

pub async fn restore(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    set_dismissed(db, clock, id, None).await
}

async fn set_dismissed(
    db: &LocalDb,
    clock: &dyn Clock,
    id: &str,
    dismissed_at: Option<i64>,
) -> Result<(), CairnError> {
    let id = id.to_string();
    let now = clock.now();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues SET dismissed_at = ?1, updated_at = ?2 WHERE id = ?3",
                params![dismissed_at, now, id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn complete(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    resolve(db, clock, id, Resolution::Merged).await.map(|_| ())
}

pub async fn resolve(
    db: &LocalDb,
    clock: &dyn Clock,
    id: &str,
    resolution: Resolution,
) -> Result<Vec<String>, CairnError> {
    let id = id.to_string();
    let now = clock.now();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            let reason = match resolution {
                Resolution::Merged => {
                    conn.execute(
                        "UPDATE issues
                         SET merged_at = ?1, completed_at = ?2, progress = 'merged',
                             attention = 'none', status = 'merged', updated_at = ?3
                         WHERE id = ?4",
                        params![now, now, now, id.as_str()],
                    )
                    .await?;
                    "issue_merged"
                }
                Resolution::Closed => {
                    conn.execute(
                        "UPDATE issues
                         SET closed_at = ?1, completed_at = ?2, progress = 'closed',
                             attention = 'none', status = 'closed', updated_at = ?3
                         WHERE id = ?4",
                        params![now, now, now, id.as_str()],
                    )
                    .await?;
                    "issue_closed"
                }
            };

            close_sessions_for_issue_conn(conn, &id, reason, now).await
        })
    })
    .await
    .map_err(CairnError::from)
}

pub async fn unresolve(db: &LocalDb, clock: &dyn Clock, id: &str) -> Result<(), CairnError> {
    let id = id.to_string();
    let now = clock.now();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "UPDATE issues
                 SET merged_at = NULL, closed_at = NULL, completed_at = NULL, updated_at = ?1
                 WHERE id = ?2",
                params![now, id.as_str()],
            )
            .await?;
            crate::transitions::outcome::recompute_issue_status_conn(conn, &id).await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}

async fn close_sessions_for_issue_conn(
    conn: &cairn_db::turso::Connection,
    issue_id: &str,
    reason: &str,
    now: i64,
) -> DbResult<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT s.id
             FROM sessions s
             INNER JOIN jobs j ON s.job_id = j.id
             WHERE j.issue_id = ?1
               AND s.status = 'open'
               AND COALESCE(j.memory_review_state, '') != 'sent'",
            params![issue_id],
        )
        .await?;

    let mut session_ids = Vec::new();
    while let Some(row) = rows.next().await? {
        session_ids.push(row.text(0)?);
    }

    for session_id in &session_ids {
        conn.execute(
            "UPDATE sessions
             SET status = 'closed', terminal_reason = ?1, closed_at = ?2, updated_at = ?3
             WHERE id = ?4",
            params![reason, now, now, session_id.as_str()],
        )
        .await?;
    }

    Ok(session_ids)
}

pub async fn delete_db(db: &LocalDb, issue_id: &str) -> Result<(), CairnError> {
    let issue_id = issue_id.to_string();
    db.write(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            // Owner-specific references that are not rooted at a job.
            conn.execute(
                "DELETE FROM execution_trigger_sources
                 WHERE triggered_execution_id IN (SELECT id FROM executions WHERE issue_id = ?1)",
                params![issue_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM merge_requests WHERE issue_id = ?1",
                params![issue_id.as_str()],
            )
            .await?;
            crate::jobs::cleanup::delete_owned_jobs(
                conn,
                crate::jobs::cleanup::JobOwner::Issue,
                &issue_id,
            )
            .await?;
            conn.execute(
                "DELETE FROM issues WHERE id = ?1",
                params![issue_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(CairnError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceTransport, VerificationMethod, VerificationRecord,
        VerificationStatus, VerificationStrength,
    };

    fn authorship(author: PrincipalRef) -> IssueAuthorship {
        let verification = VerificationRecord::new(
            VerificationMethod::NodeSession,
            VerificationStatus::Verified,
            None,
            None,
            Some("session-1".into()),
            None,
            VerificationStrength::new("session-bound").unwrap(),
            1,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::ResourcePatch,
            Address::Resource {
                node: "cairn://p/demo/1/1/builder".into(),
            },
            verification,
            2,
            None,
        )
        .unwrap();
        IssueAuthorship::new(
            author.clone(),
            AppearanceSnapshot::new(author, evidence, Vec::new(), None).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn installation_machine_authorship_is_local_and_decision_timed() {
        let authorship = installation_machine_authorship("installation-1", 42).unwrap();
        assert_eq!(
            authorship.author,
            PrincipalRef::Machine {
                device_id: "installation-1".into()
            }
        );
        let evidence = authorship.appearance.evidence();
        assert_eq!(evidence.transport, AppearanceTransport::LocalInvoke);
        assert_eq!(evidence.address, Address::None);
        assert_eq!(evidence.at, 42);
        assert_eq!(evidence.verification.status(), VerificationStatus::None);
        assert_eq!(evidence.verification.verified_at(), 42);
        assert_eq!(evidence.verification.strength().as_str(), "local_process");
        assert!(authorship.appearance.delegation().is_empty());
        assert!(authorship.appearance.terminal_represented().is_none());

        assert!(installation_machine_authorship("", 42).is_err());
        assert!(installation_machine_authorship("installation-1", -1).is_err());
    }

    #[test]
    fn issue_authorship_codec_is_paired_and_fail_closed() {
        let valid = authorship(PrincipalRef::Agent {
            node: "cairn://p/demo/1/1/builder".into(),
            run_id: Some("run-1".into()),
        });
        let (author_json, appearance_json) = encode_issue_authorship(&valid).unwrap();
        assert_eq!(
            decode_issue_authorship(Some(author_json.clone()), Some(appearance_json.clone()))
                .unwrap(),
            Some(valid)
        );
        assert_eq!(decode_issue_authorship(None, None).unwrap(), None);
        assert!(decode_issue_authorship(Some(author_json.clone()), None).is_err());
        assert!(decode_issue_authorship(None, Some(appearance_json.clone())).is_err());
        assert!(decode_issue_authorship(Some("{}".into()), Some(appearance_json.clone())).is_err());
        assert!(decode_issue_authorship(
            Some(
                serde_json::to_string(&PrincipalRef::Machine {
                    device_id: "other".into()
                })
                .unwrap()
            ),
            Some(appearance_json)
        )
        .is_err());
        assert!(IssueAuthorship::new(
            PrincipalRef::Agent {
                node: "cairn://p/demo/1/1/builder".into(),
                run_id: None,
            },
            authorship(PrincipalRef::Machine {
                device_id: "machine".into(),
            })
            .appearance,
        )
        .is_err());
    }

    #[tokio::test]
    async fn sidebar_active_query_is_visible_ordered_and_bounded() {
        let db = crate::storage::migrated_test_db("sidebar-active-query.db").await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at)
            VALUES('team-1', 'Team', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, hidden, is_workspace, created_at, updated_at)
            VALUES
              ('alpha', 'default', 'Alpha', 'ALP', '/tmp/alpha', 0, 0, 1, 1),
              ('hidden', 'default', 'Hidden', 'HID', '/tmp/hidden', 1, 0, 1, 1),
              ('team-home', 'team-1', 'Team config', 'TEAM', '/tmp/team', 0, 1, 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES
              ('waiting-high', 'alpha', 99, 'Waiting high', 'waiting', 'waiting', 'none', 1, 1),
              ('active-low', 'alpha', 1, 'Active low', 'active', 'active', 'none', 1, 1),
              ('dismissed', 'alpha', 2, 'Dismissed', 'active', 'active', 'none', 1, 1),
              ('hidden-issue', 'hidden', 1, 'Hidden', 'active', 'active', 'none', 1, 1),
              ('team-home-issue', 'team-home', 1, 'Config', 'active', 'active', 'none', 1, 1);
            UPDATE issues SET dismissed_at = 2 WHERE id = 'dismissed';",
        )
        .await
        .unwrap();

        let rows = list_sidebar_active(&db, 2).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].issue_title, "Active low");
        assert_eq!(rows[0].status_rank, 0);
        assert_eq!(rows[1].issue_title, "Waiting high");
        assert_eq!(rows[1].status_rank, 1);
    }

    async fn seeded_relation_db() -> LocalDb {
        let db = crate::storage::migrated_test_db("issue-list-relations.db").await;
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p-one', 'default', 'One', 'ONE', '/tmp/one', 1, 1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p-two', 'default', 'Two', 'TWO', '/tmp/two', 1, 1);

            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('i-one', 'p-one', 1, 'One', 'backlog', 'backlog', 'none', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('i-two', 'p-one', 2, 'Two', 'merged', 'complete', 'none', 2, 2);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('i-three', 'p-one', 3, 'Three', 'closed', 'complete', 'none', 3, 3);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('parent', 'p-one', 4, 'Parent', 'backlog', 'backlog', 'none', 4, 4);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('x-one', 'p-two', 1, 'External', 'active', 'active', 'none', 5, 5);
            UPDATE issues SET parent_issue_id = 'parent' WHERE id IN ('i-one', 'i-two');

            INSERT INTO issue_dependencies(issue_id, depends_on_uri, created_at)
            VALUES('i-one', 'cairn://p/ONE/2', 10);
            INSERT INTO issue_dependencies(issue_id, depends_on_uri, created_at)
            VALUES('i-one', 'cairn://p/TWO/1', 20);
            INSERT INTO issue_dependencies(issue_id, depends_on_uri, created_at)
            VALUES('i-one', 'cairn://p/MISSING/9', 30);
            INSERT INTO issue_dependencies(issue_id, depends_on_uri, created_at)
            VALUES('i-two', 'cairn://p/ONE/3', 40);

            INSERT INTO labels(id, workspace_id, name, color, created_at, updated_at)
            VALUES('label-zulu', 'default', 'zulu', '#111111', 1, 1);
            INSERT INTO labels(id, workspace_id, name, color, created_at, updated_at)
            VALUES('label-alpha', 'default', 'Alpha', '#222222', 2, 2);
            INSERT INTO labels(id, workspace_id, name, color, created_at, updated_at)
            VALUES('label-beta', 'default', 'Beta', '#333333', 3, 3);
            INSERT INTO issue_labels(issue_id, label_id, created_at)
            VALUES('i-one', 'label-zulu', 1);
            INSERT INTO issue_labels(issue_id, label_id, created_at)
            VALUES('i-one', 'label-alpha', 2);
            INSERT INTO issue_labels(issue_id, label_id, created_at)
            VALUES('i-two', 'label-beta', 3);
            ",
        )
        .await
        .unwrap();
        db
    }

    #[tokio::test]
    async fn list_and_list_children_preserve_relation_semantics_and_ordering() {
        let db = seeded_relation_db().await;

        let issues = list(&db, "p-one").await.unwrap();
        assert_eq!(
            issues
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["parent", "i-three", "i-two", "i-one"]
        );

        let issue_one = issues.iter().find(|issue| issue.id == "i-one").unwrap();
        assert_eq!(
            issue_one.depends_on,
            vec!["cairn://p/ONE/2", "cairn://p/TWO/1", "cairn://p/MISSING/9"]
        );
        assert_eq!(
            issue_one.unmet_depends_on,
            vec!["cairn://p/two/1", "cairn://p/missing/9"]
        );
        assert_eq!(issue_one.unmet_dependency_count, 2);
        assert_eq!(
            issue_one
                .labels
                .iter()
                .map(|label| label.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Alpha", "zulu"]
        );

        let issue_two = issues.iter().find(|issue| issue.id == "i-two").unwrap();
        assert_eq!(issue_two.depends_on, vec!["cairn://p/ONE/3"]);
        assert!(issue_two.unmet_depends_on.is_empty());
        assert_eq!(issue_two.unmet_dependency_count, 0);
        assert_eq!(issue_two.labels[0].name, "Beta");

        let single = get(&db, "i-one").await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_string(&single).unwrap(),
            serde_json::to_string(issue_one).unwrap()
        );

        let children = list_children(&db, "parent").await.unwrap();
        assert_eq!(
            children
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["i-two", "i-one"]
        );
        assert_eq!(children[1].depends_on, issue_one.depends_on);
        assert_eq!(children[1].unmet_depends_on, issue_one.unmet_depends_on);
        assert_eq!(children[1].labels, issue_one.labels);
    }

    /// The desktop's `Issue` type is hand-maintained TypeScript, so nothing but
    /// a test holds the two shapes together. The threads cutover added the
    /// column and the TypeScript field but not the Rust one, so every client
    /// read `undefined` for every thread child from the cutover onward with no
    /// compiler or suite noticing. Assert the serialized payload rather than
    /// only the struct field: the camelCase key is what crosses the wire.
    #[tokio::test]
    async fn listed_issues_carry_their_parent_thread_on_the_wire() {
        let db = crate::storage::migrated_test_db("issue-parent-thread.db").await;
        db.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
            VALUES('p-one', 'default', 'One', 'ONE', '/tmp/one', 1, 1);
            INSERT INTO threads(id, project_id, name, created_at, updated_at)
            VALUES('t-one', 'p-one', 'thread-ux', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_thread_id, created_at, updated_at)
            VALUES('child', 'p-one', 1, 'Thread child', 'active', 'active', 'none', 't-one', 1, 1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
            VALUES('loose', 'p-one', 2, 'Unparented', 'active', 'active', 'none', 2, 2);
            ",
        )
        .await
        .unwrap();

        let issues = list(&db, "p-one").await.unwrap();
        let child = issues.iter().find(|issue| issue.id == "child").unwrap();
        let loose = issues.iter().find(|issue| issue.id == "loose").unwrap();
        assert_eq!(child.parent_thread_id.as_deref(), Some("t-one"));
        assert_eq!(child.parent_issue_id, None);
        assert_eq!(loose.parent_thread_id, None);

        let wire = serde_json::to_value(child).unwrap();
        assert_eq!(wire["parentThreadId"], serde_json::json!("t-one"));
        assert_eq!(wire["parentIssueId"], serde_json::Value::Null);

        // `get` reads through the same column list: loading one issue must show
        // the same edge the list does.
        let single = get(&db, "child").await.unwrap().unwrap();
        assert_eq!(single.parent_thread_id.as_deref(), Some("t-one"));
    }

    #[tokio::test]
    async fn relation_hydration_query_count_is_bounded_by_families() {
        let db = seeded_relation_db().await;

        let (all_queries, one_query) = db
            .read(|conn| {
                Box::pin(async move {
                    let sql = format!(
                        "SELECT {ISSUE_COLUMNS} FROM issues WHERE project_id = ?1 ORDER BY number DESC"
                    );
                    let mut rows = conn.query(&sql, params!["p-one"]).await?;
                    let mut issues = Vec::new();
                    while let Some(row) = rows.next().await? {
                        issues.push(issue_from_row(&row)?);
                    }

                    let mut all_queries = Vec::new();
                    hydrate_issue_relations_with_observer(conn, &mut issues, &mut |query| {
                        all_queries.push(query)
                    })
                    .await?;

                    let mut one_issue = vec![issues
                        .into_iter()
                        .find(|issue| issue.id == "i-one")
                        .unwrap()];
                    let mut one_query = Vec::new();
                    hydrate_issue_relations_with_observer(conn, &mut one_issue, &mut |query| {
                        one_query.push(query)
                    })
                    .await?;
                    Ok((all_queries, one_query))
                })
            })
            .await
            .unwrap();

        let expected = vec![
            RelationQuery::Dependencies,
            RelationQuery::ResolvedDependencies,
            RelationQuery::Labels,
        ];
        assert_eq!(all_queries, expected);
        assert_eq!(one_query, expected);
    }

    #[tokio::test]
    async fn delete_db_detaches_memories_before_deleting_jobs() {
        let db = crate::storage::migrated_test_db("issue-delete-memory-detach.db").await;
        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'prj', '/tmp/prj', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'builder', 'builder', 'complete', 1, 1);
            INSERT INTO memories(id, name, project_id, content, status, scope, scope_value, job_id, node_seq, created_at, updated_at)
             VALUES ('draft-1', 'Draft', 'project-1', 'draft content', 'draft', 'project', 'project-1', 'job-1', 1, 1, 1);
            ",
        )
        .await
        .unwrap();

        delete_db(&db, "issue-1").await.unwrap();

        let count = db
            .query_one(
                "SELECT COUNT(*) FROM memories WHERE id = 'draft-1'",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn delete_db_removes_full_run_subtree() {
        // Regression: a worked-on issue has runs/sessions/turns plus a PR and a
        // trigger source. Those reference the subtree without ON DELETE CASCADE,
        // so a naive delete hit "foreign key constraint failed".
        let db = crate::storage::migrated_test_db("issue-delete-full-subtree.db").await;
        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-1', 'default', 'Project', 'prj', '/tmp/prj', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 1, 'Issue', 1, 1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'project-1', 'running', 1, 1);
            INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('job-1', 'exec-1', 'issue-1', 'project-1', 'builder', 'builder', 'complete', 1, 1);
            INSERT INTO runs(id, project_id, issue_id, job_id, status, created_at, updated_at, start_mode)
             VALUES ('run-1', 'project-1', 'issue-1', 'job-1', 'live', 1, 1, 'resume');
            INSERT INTO sessions(id, job_id, created_at, updated_at)
             VALUES ('sess-1', 'job-1', 1, 1);
            INSERT INTO turns(id, session_id, run_id, sequence, created_at, updated_at)
             VALUES ('turn-1', 'sess-1', 'run-1', 1, 1, 1);
            INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES ('mr-1', 'job-1', 'project-1', 'issue-1', 'PR', 'src', 'dst', 'open', 1, 1);
            INSERT INTO execution_trigger_sources(id, source_job_id, triggered_execution_id, created_at)
             VALUES ('ets-1', 'job-1', 'exec-1', 1);
            INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at, turn_id)
             VALUES ('ev-1', 'run-1', 1, 1, 'message', '{}', 1, 'turn-1');
            INSERT INTO prompts(id, run_id, questions, created_at, turn_id)
             VALUES ('prompt-1', 'run-1', '[]', 1, 'turn-1');
            INSERT INTO permission_requests(id, run_id, tool_use_id, tool_name, tool_input, created_at, turn_id)
             VALUES ('perm-1', 'run-1', 'tu-1', 'run', '{}', 1, 'turn-1');
            INSERT INTO artifacts(id, job_id, artifact_type, data, created_at, updated_at)
             VALUES ('art-1', 'job-1', 'plan', '{}', 1, 1);
            INSERT INTO artifacts(id, job_id, artifact_type, data, parent_version_id, created_at, updated_at)
             VALUES ('art-2', 'job-1', 'plan', '{}', 'art-1', 1, 1);
            UPDATE jobs SET current_turn_id = 'turn-1', resume_session_id = 'sess-1' WHERE id = 'job-1';
            ",
        )
        .await
        .unwrap();

        delete_db(&db, "issue-1").await.unwrap();

        for (table, sql) in [
            ("issues", "SELECT COUNT(*) FROM issues WHERE id = 'issue-1'"),
            (
                "jobs",
                "SELECT COUNT(*) FROM jobs WHERE issue_id = 'issue-1'",
            ),
            (
                "runs",
                "SELECT COUNT(*) FROM runs WHERE issue_id = 'issue-1'",
            ),
            (
                "executions",
                "SELECT COUNT(*) FROM executions WHERE issue_id = 'issue-1'",
            ),
            (
                "sessions",
                "SELECT COUNT(*) FROM sessions WHERE id = 'sess-1'",
            ),
            ("turns", "SELECT COUNT(*) FROM turns WHERE id = 'turn-1'"),
            ("events", "SELECT COUNT(*) FROM events WHERE id = 'ev-1'"),
            (
                "prompts",
                "SELECT COUNT(*) FROM prompts WHERE id = 'prompt-1'",
            ),
            (
                "permission_requests",
                "SELECT COUNT(*) FROM permission_requests WHERE id = 'perm-1'",
            ),
            (
                "artifacts",
                "SELECT COUNT(*) FROM artifacts WHERE job_id = 'job-1'",
            ),
            (
                "merge_requests",
                "SELECT COUNT(*) FROM merge_requests WHERE id = 'mr-1'",
            ),
            (
                "execution_trigger_sources",
                "SELECT COUNT(*) FROM execution_trigger_sources WHERE id = 'ets-1'",
            ),
        ] {
            let count = db.query_one(sql, (), |row| row.i64(0)).await.unwrap();
            assert_eq!(count, 0, "{table} rows should be gone after delete");
        }
    }
    #[tokio::test]
    async fn delete_db_removes_issue_run_without_job() {
        let db = crate::storage::migrated_test_db("issue-delete-jobless-run.db").await;
        db.execute_script(
            "
            INSERT OR IGNORE INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('project-jobless', 'default', 'Project', 'JOBLESS', '/tmp/jobless', 1, 1);
            INSERT INTO issues(id, project_id, number, title, created_at, updated_at)
             VALUES ('issue-jobless', 'project-jobless', 1, 'Issue', 1, 1);
            INSERT INTO runs(id, project_id, issue_id, status, created_at, updated_at, start_mode)
             VALUES ('run-jobless', 'project-jobless', 'issue-jobless', 'live', 1, 1, 'resume');
            INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
             VALUES ('event-jobless', 'run-jobless', 1, 1, 'message', '{}', 1);
            ",
        )
        .await
        .unwrap();

        delete_db(&db, "issue-jobless").await.unwrap();

        for table in ["events", "runs", "issues"] {
            let count = db
                .query_one(
                    &format!("SELECT COUNT(*) FROM {table} WHERE id LIKE '%jobless'"),
                    (),
                    |row| row.i64(0),
                )
                .await
                .unwrap();
            assert_eq!(count, 0, "{table}");
        }
    }
}
