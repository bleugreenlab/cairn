//! Project, project-issues, and project-search resource readers.

use cairn_db::turso::params;

use super::common::{
    connect_for_read, find_query_value, lookup_project_by_key, parse_optional_i64_param,
    parse_optional_usize_param, storage_error,
};

use crate::attachments::{AttachmentKind, AttachmentRow};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_issue_uri, build_project_issues_uri, build_project_reference_uri,
    build_project_references_uri, build_project_terminal_uri, build_project_uri,
    AttachmentScopeRef,
};

#[derive(Debug, Default)]
struct ProjectIssueStats {
    total: usize,
    open: usize,
    waiting: usize,
    merged: usize,
    closed: usize,
}

async fn load_project_issue_stats(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
) -> ProjectIssueStats {
    let mut rows = match conn
        .query(
            "SELECT status FROM issues WHERE project_id = ?1",
            (project_id,),
        )
        .await
    {
        Ok(rows) => rows,
        Err(_) => return ProjectIssueStats::default(),
    };

    let mut stats = ProjectIssueStats::default();
    while let Ok(Some(row)) = rows.next().await {
        let Ok(status) = row.text(0) else {
            continue;
        };
        stats.total += 1;
        match status.to_ascii_lowercase().as_str() {
            "active" | "open" => stats.open += 1,
            "waiting" => stats.waiting += 1,
            "merged" => stats.merged += 1,
            "closed" => stats.closed += 1,
            _ => {}
        }
    }

    stats
}

async fn load_recent_project_issue_summaries(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    limit: i64,
) -> Vec<(i32, String, String, Option<String>, i32)> {
    let mut rows = match conn
        .query(
            "
            SELECT number, title, status, description, updated_at
            FROM issues
            WHERE project_id = ?1
            ORDER BY updated_at DESC, created_at DESC
            LIMIT ?2
            ",
            params![project_id, limit],
        )
        .await
    {
        Ok(rows) => rows,
        Err(_) => return Vec::new(),
    };

    let mut issues = Vec::new();
    while let Ok(Some(row)) = rows.next().await {
        let (Ok(number), Ok(title), Ok(status), Ok(description), Ok(updated_at)) = (
            row.i64(0),
            row.text(1),
            row.text(2),
            row.opt_text(3),
            row.i64(4),
        ) else {
            continue;
        };
        issues.push((number as i32, title, status, description, updated_at as i32));
    }

    issues
}

#[derive(Debug, Clone)]
struct ProjectIssueSummary {
    id: String,
    number: i32,
    title: String,
    status: String,
    labels: Vec<String>,
    /// Gate-census lines for this issue's held executions.
    held: Vec<String>,
    /// Gate-census lines for its gated executions that can never start,
    /// printed after the held ones and reading `won't start: ...`.
    wont_start: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectIssuesSortField {
    Created,
    Updated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectIssuesSortDir {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectIssuesQuery {
    statuses: Option<Vec<String>>,
    limit: i64,
    offset: i64,
    sort_field: ProjectIssuesSortField,
    sort_dir: ProjectIssuesSortDir,
    label: Option<String>,
    labels: Option<Vec<String>>,
}

/// Query keys this reader once accepted or advertised, each with the refusal
/// that points the caller at what replaced it.
const PROJECT_ISSUES_RETIRED_PARAMS: &[(&str, &str)] = &[
    (
        "ready",
        "Unsupported query parameter 'ready' for project issues: it filtered on issue dependencies, which are retired into `after:` gates on executions. An issue whose execution is waiting shows it as held, with each gate named, in this list and in the issue overview.",
    ),
    (
        "kind",
        "Unsupported query parameter 'kind' for project issues: threads are no longer issues, so this collection lists only issues. Threads are listed at cairn://p/PROJECT/threads.",
    ),
];

/// The accepted query keys are exactly the filters the resource contract
/// advertises, so the affordance block and this reader's refusal cannot name
/// different sets.
fn project_issues_supported_params() -> impl Iterator<Item = &'static str> {
    cairn_common::contract::contract_for(cairn_common::contract::ResourceKind::ProjectIssues)
        .map(|contract| contract.read_projections)
        .unwrap_or_default()
        .iter()
        .map(|projection| projection.key)
}

const PROJECT_ISSUES_ALLOWED_STATUSES: &[&str] = &[
    "backlog", "active", "waiting", "merged", "closed", "failed", "complete",
];

impl Default for ProjectIssuesQuery {
    fn default() -> Self {
        Self {
            statuses: None,
            limit: 20,
            offset: 0,
            sort_field: ProjectIssuesSortField::Updated,
            sort_dir: ProjectIssuesSortDir::Desc,
            label: None,
            labels: None,
        }
    }
}

impl ProjectIssuesQuery {
    fn parse(params: &[QueryParam]) -> Result<Self, String> {
        if let Some((_, refusal)) = PROJECT_ISSUES_RETIRED_PARAMS
            .iter()
            .find(|(key, _)| params.iter().any(|param| param.key == *key))
        {
            return Err((*refusal).to_string());
        }
        if let Some(unsupported) = params
            .iter()
            .find(|param| !project_issues_supported_params().any(|key| key == param.key))
        {
            return Err(format!(
                "Unsupported query parameter '{}' for project issues. Supported parameters: {}",
                unsupported.key,
                project_issues_supported_params()
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        let mut query = Self::default();
        if let Some(value) = find_query_value(params, "status") {
            query.statuses = Some(parse_project_issue_statuses(value)?);
        }
        if let Some(value) = find_query_value(params, "limit") {
            query.limit = parse_project_issues_limit(value)?;
        }
        if let Some(value) = find_query_value(params, "offset") {
            query.offset = parse_project_issues_offset(value)?;
        }
        if let Some(value) = find_query_value(params, "sort") {
            (query.sort_field, query.sort_dir) = parse_project_issues_sort(value)?;
        }
        if let Some(value) = find_query_value(params, "label") {
            if value.trim().is_empty() {
                return Err("Invalid label query parameter: value must not be empty".to_string());
            }
            query.label = Some(value.trim().to_string());
        }
        if let Some(value) = find_query_value(params, "labels") {
            let labels = value
                .split(',')
                .map(|label| label.trim().to_string())
                .filter(|label| !label.is_empty())
                .collect::<Vec<_>>();
            if labels.is_empty() {
                return Err(
                    "Invalid labels query parameter: expected at least one label".to_string(),
                );
            }
            query.labels = Some(labels);
        }
        Ok(query)
    }

    fn has_filters(&self) -> bool {
        self.statuses.is_some() || self.label.is_some() || self.labels.is_some()
    }
}

fn parse_project_issue_statuses(value: &str) -> Result<Vec<String>, String> {
    let statuses = value
        .split(',')
        .map(|status| status.trim().to_ascii_lowercase())
        .filter(|status| !status.is_empty())
        .collect::<Vec<_>>();
    if statuses.is_empty() {
        return Err("Invalid status query parameter: expected at least one status".to_string());
    }
    if let Some(invalid) = statuses
        .iter()
        .find(|status| !PROJECT_ISSUES_ALLOWED_STATUSES.contains(&status.as_str()))
    {
        return Err(format!(
            "Invalid status query parameter: {invalid}. Supported statuses: {}",
            PROJECT_ISSUES_ALLOWED_STATUSES.join(", ")
        ));
    }
    Ok(statuses)
}

fn parse_project_issues_limit(value: &str) -> Result<i64, String> {
    let limit = value.parse::<i64>().map_err(|_| {
        format!("Invalid limit query parameter: {value}; expected a positive integer")
    })?;
    if limit <= 0 {
        return Err(format!(
            "Invalid limit query parameter: {value}; expected a positive integer"
        ));
    }
    Ok(limit)
}

fn parse_project_issues_offset(value: &str) -> Result<i64, String> {
    let offset = value.parse::<i64>().map_err(|_| {
        format!("Invalid offset query parameter: {value}; expected a non-negative integer")
    })?;
    if offset < 0 {
        return Err(format!(
            "Invalid offset query parameter: {value}; expected a non-negative integer"
        ));
    }
    Ok(offset)
}

fn parse_project_issues_sort(
    value: &str,
) -> Result<(ProjectIssuesSortField, ProjectIssuesSortDir), String> {
    match value {
        "created_asc" => Ok((ProjectIssuesSortField::Created, ProjectIssuesSortDir::Asc)),
        "created_desc" => Ok((ProjectIssuesSortField::Created, ProjectIssuesSortDir::Desc)),
        "updated_asc" => Ok((ProjectIssuesSortField::Updated, ProjectIssuesSortDir::Asc)),
        "updated_desc" => Ok((ProjectIssuesSortField::Updated, ProjectIssuesSortDir::Desc)),
        _ => Err(format!(
            "Invalid sort query parameter: {value}. Supported sorts: created_asc, created_desc, updated_asc, updated_desc"
        )),
    }
}

fn quoted_sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Build the dynamic `WHERE` filter suffix (everything after
/// `WHERE i.project_id = ?1`) shared by the page query and its `COUNT(*)`. All
/// filter values are inlined as quoted SQL literals; only `project_id` (?1),
/// `limit` (?2), and `offset` (?3) are bound.
fn issue_filter_clause(query: &ProjectIssuesQuery) -> String {
    let mut clause = String::new();
    if let Some(statuses) = &query.statuses {
        let status_list = statuses
            .iter()
            .map(|status| quoted_sql_literal(status))
            .collect::<Vec<_>>()
            .join(", ");
        clause.push_str(&format!(" AND LOWER(i.status) IN ({status_list})"));
    }
    let label_filters = query
        .label
        .iter()
        .chain(query.labels.iter().flatten())
        .collect::<Vec<_>>();
    for label in label_filters {
        let label = quoted_sql_literal(label);
        clause.push_str(&format!(
            " AND EXISTS (SELECT 1 FROM issue_labels filter_il JOIN labels filter_l ON filter_l.id = filter_il.label_id WHERE filter_il.issue_id = i.id AND (filter_l.id = {label} OR filter_l.name = {label} COLLATE NOCASE))"
        ));
    }
    clause
}

/// Load one page of issue summaries plus the total matching count, both over the
/// same filter clause. `limit`/`offset` push down to SQL so the producer can
/// report a truthful `[N of M issues]` header and a paging continue URI.
async fn load_project_issue_summaries(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    query: &ProjectIssuesQuery,
) -> Result<(Vec<ProjectIssueSummary>, usize), String> {
    let filter_clause = issue_filter_clause(query);

    let count_sql = format!(
        "SELECT COUNT(*) FROM (SELECT i.id FROM issues i WHERE i.project_id = ?1{filter_clause} GROUP BY i.id)"
    );
    let mut count_rows = conn
        .query(&count_sql, params![project_id])
        .await
        .map_err(|error| storage_error("Failed to count project issues", error.into()))?;
    let total = count_rows
        .next()
        .await
        .map_err(|error| storage_error("Failed to count project issues", error.into()))?
        .and_then(|row| row.i64(0).ok())
        .unwrap_or(0)
        .max(0) as usize;

    let sort_field = match query.sort_field {
        ProjectIssuesSortField::Created => "i.created_at",
        ProjectIssuesSortField::Updated => "i.updated_at",
    };
    let sort_dir = match query.sort_dir {
        ProjectIssuesSortDir::Asc => "ASC",
        ProjectIssuesSortDir::Desc => "DESC",
    };
    let sql = format!(
        "
        SELECT i.number, i.title, i.status, GROUP_CONCAT(l.name, char(31)), i.id
        FROM issues i
        LEFT JOIN issue_labels il ON il.issue_id = i.id
        LEFT JOIN labels l ON l.id = il.label_id
        WHERE i.project_id = ?1{filter_clause}
        GROUP BY i.id ORDER BY {sort_field} {sort_dir}, i.number DESC LIMIT ?2 OFFSET ?3"
    );

    let mut rows = conn
        .query(&sql, params![project_id, query.limit, query.offset])
        .await
        .map_err(|error| storage_error("Failed to load project issues", error.into()))?;

    let mut issues = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| storage_error("Failed to load project issues", error.into()))?
    {
        let (number, title, status, labels, id) = (
            row.i64(0),
            row.text(1),
            row.text(2),
            row.opt_text(3),
            row.text(4),
        );
        let (Ok(number), Ok(title), Ok(status), Ok(labels), Ok(id)) =
            (number, title, status, labels, id)
        else {
            continue;
        };
        let labels = labels
            .unwrap_or_default()
            .split('\u{1f}')
            .filter(|label| !label.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        issues.push(ProjectIssueSummary {
            id,
            number: number as i32,
            title,
            status,
            labels,
            held: Vec::new(),
            wont_start: Vec::new(),
        });
    }

    let issue_ids: Vec<String> = issues.iter().map(|issue| issue.id.clone()).collect();
    let mut gated =
        crate::execution::gates::census::gated_executions_for_issues_conn(conn, &issue_ids)
            .await
            .map_err(|error| storage_error("Failed to load held executions", error))?;
    for issue in &mut issues {
        let gated = gated.remove(&issue.id).unwrap_or_default();
        let lines = |executions: Vec<crate::models::HeldExecution>| {
            executions
                .into_iter()
                .map(|execution| execution.line)
                .collect()
        };
        issue.held = lines(gated.held);
        issue.wont_start = lines(gated.wont_start);
    }

    Ok((issues, total))
}

fn render_project_issue_entries(
    project_key: &str,
    project_issues: &[ProjectIssueSummary],
) -> String {
    if project_issues.is_empty() {
        return "No issues found.\n".to_string();
    }

    let mut output = String::new();
    for issue in project_issues {
        let status_indicator = match issue.status.to_ascii_lowercase().as_str() {
            "active" => "◐",
            "merged" => "✓",
            "closed" => "✗",
            "waiting" => "⏳",
            _ => "○",
        };
        let labels = if issue.labels.is_empty() {
            String::new()
        } else {
            format!(
                "  {}",
                issue
                    .labels
                    .iter()
                    .map(|label| format!("·{}", label))
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        };
        output.push_str(&format!(
            "- [{}-{}]({}) [{}] {}{}\n",
            cairn_common::uri::canonical_project(project_key),
            issue.number,
            build_issue_uri(project_key, issue.number),
            status_indicator,
            issue.title,
            labels
        ));
        for line in issue.held.iter().chain(&issue.wont_start) {
            output.push_str(&format!("  - {line}\n"));
        }
    }

    output
}

fn render_recent_project_issue_entries(
    project_key: &str,
    project_issues: &[(i32, String, String, Option<String>, i32)],
) -> String {
    if project_issues.is_empty() {
        return "No issues found.\n".to_string();
    }

    let mut output = String::new();
    for (number, title, status, _description, _created_at) in project_issues {
        let status_indicator = match status.to_ascii_lowercase().as_str() {
            "active" => "◐",
            "merged" => "✓",
            "closed" => "✗",
            "waiting" => "⏳",
            _ => "○",
        };
        output.push_str(&format!(
            "- [{}-{}]({}) [{}] {}\n",
            cairn_common::uri::canonical_project(project_key),
            number,
            build_issue_uri(project_key, *number),
            status_indicator,
            title
        ));
    }

    output
}

/// One item-windowed page of a project's `/issues` collection: the rendered
/// list body plus the natural-unit counts the producer needs for a `Record`
/// segment (`[shown of total issues]` header + paging continue URI).
pub(super) struct ProjectIssuesPage {
    pub body: String,
    pub total: usize,
    pub shown: usize,
    pub offset: usize,
}

/// Produce one page of issues with pushdown paging and a true total count. The
/// affordance is attached centrally by the resource producer, so the body here
/// is content only.
pub(super) async fn produce_project_issues(
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> Result<ProjectIssuesPage, String> {
    let conn = connect_for_read(db).await?;
    let project_ctx = lookup_project_by_key(&conn, project_key).await?;
    let query = ProjectIssuesQuery::parse(params)?;
    let (project_issues, total) =
        load_project_issue_summaries(&conn, &project_ctx.project_id, &query).await?;
    let shown = project_issues.len();
    let offset = query.offset.max(0) as usize;

    let mut body = format!("# Issues — {}\n\n", project_ctx.project_key);
    if query.has_filters() {
        let filters = params
            .iter()
            .filter(|param| param.key != "offset" && param.key != "limit")
            .map(|param| format!("{}={}", param.key, param.value))
            .collect::<Vec<_>>()
            .join(", ");
        body.push_str(&format!("{shown} issue(s), filtered by {filters}\n\n"));
    } else {
        body.push_str(&format!("{shown} issue(s)\n\n"));
    }
    body.push_str(&render_project_issue_entries(
        &project_ctx.project_key,
        &project_issues,
    ));

    Ok(ProjectIssuesPage {
        body,
        total,
        shown,
        offset,
    })
}

/// Body-only `/issues` render for the flattened-String resource path.
pub(super) async fn read_project_issues(
    db: &LocalDb,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    match produce_project_issues(db, project_key, params).await {
        Ok(page) => page.body,
        Err(error) => error,
    }
}

/// How an attachment scope reads in prose.
fn attachment_scope_label(project_key: &str, scope: &AttachmentScopeRef) -> String {
    let project = cairn_common::uri::canonical_project(project_key);
    match scope {
        AttachmentScopeRef::Issue(number) => format!("{project}/{number}"),
        AttachmentScopeRef::Thread(name) => format!("{project}/{name}"),
        AttachmentScopeRef::Project => project,
    }
}

async fn scope_rows(
    db: &LocalDb,
    project_key: &str,
    scope: &AttachmentScopeRef,
) -> Result<(String, Vec<AttachmentRow>), String> {
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(db, project_key).await?;
    let rows = crate::attachments::list_attachments(db, &project_id, scope)
        .await
        .map_err(|error| error.to_string())?;
    Ok((project_id, rows))
}

/// The enumeration behind an image scope.
///
/// Every member's address is this collection's URI plus `/{ordinal}`, and the
/// listing says so, because the point of a scoped ordinal is that an agent can
/// construct a sibling's address without asking. The rows are the reference
/// rows, not the objects: an image's bytes are fetched by reading its own URI.
///
/// Images and files share one ordinal sequence, so this collection is a filtered
/// view of the attachment collection beside it and its ordinals are not dense.
pub(super) async fn read_project_images(
    db: &LocalDb,
    project_key: &str,
    scope: &AttachmentScopeRef,
) -> String {
    let rows = match scope_rows(db, project_key, scope).await {
        Ok((_, rows)) => rows,
        Err(error) => return error,
    };
    let images: Vec<_> = rows
        .into_iter()
        .filter(|row| row.kind == AttachmentKind::Image && row.retired_at.is_none())
        .collect();

    let base = cairn_common::uri::build_project_images_uri(project_key, scope);
    let attachments = cairn_common::uri::build_project_attachments_uri(project_key, scope);
    let mut body = format!(
        "# Images — {}\n\n",
        attachment_scope_label(project_key, scope)
    );
    if images.is_empty() {
        body.push_str("No images have been stored in this scope.\n");
        body.push_str(&format!(
            "\nEvery uploaded object in this scope, images and files alike, is at `{attachments}`.\n"
        ));
        return body;
    }
    body.push_str(&format!("{} image(s)\n\n", images.len()));
    for row in &images {
        body.push_str(&format!("- `{base}/{}` — {}\n", row.ordinal, row.file_name));
    }
    body.push_str(&format!(
        "\nRead any of these URIs to see the image itself. Ordinals are stable and never\nreused, and images share one sequence with files, so a gap here is a file rather\nthan a missing image — `{attachments}` lists them all.\n"
    ));
    body
}

/// The enumeration behind an attachment scope: every durable object uploaded
/// into this issue, thread, or project, in ordinal order.
///
/// A retired reference is listed as retired rather than omitted. Ordinals are
/// never reused, so hiding one would make the sequence lie about what its
/// neighbours mean.
pub(super) async fn read_project_attachments(
    db: &LocalDb,
    project_key: &str,
    scope: &AttachmentScopeRef,
) -> String {
    let rows = match scope_rows(db, project_key, scope).await {
        Ok((_, rows)) => rows,
        Err(error) => return error,
    };

    let base = cairn_common::uri::build_project_attachments_uri(project_key, scope);
    let mut body = format!(
        "# Attachments — {}\n\n",
        attachment_scope_label(project_key, scope)
    );
    if rows.is_empty() {
        body.push_str("Nothing has been attached in this scope.\n");
        return body;
    }
    body.push_str(&format!("{} attachment(s)\n\n", rows.len()));
    for row in &rows {
        let media = row.media_type.as_deref().unwrap_or("not yet measured");
        let size = row
            .byte_size
            .map(|bytes| format!(", {}", human_bytes(bytes)))
            .unwrap_or_default();
        let retired = if row.retired_at.is_some() {
            " — RETIRED, this address no longer resolves"
        } else {
            ""
        };
        body.push_str(&format!(
            "- `{base}/{}` — {} ({media}{size}){retired}\n",
            row.ordinal, row.file_name
        ));
        if let Some(origin) = &row.origin_node_uri {
            body.push_str(&format!("  attached in {origin}\n"));
        }
    }
    body.push_str(
        "\nRead any of these URIs for the object itself: an image returns an image block, a\nPDF returns extracted text, text-shaped content returns text. If a tool needs a\nfilesystem path, materialize the attachment into your current cell:\nwrite({changes:[{target:\"‹uri›\",mode:\"patch\",payload:{action:\"materialize\"}}]}).\n",
    );
    body
}

fn human_bytes(bytes: i64) -> String {
    const KIB: f64 = 1024.0;
    let bytes = bytes as f64;
    if bytes < KIB {
        format!("{bytes:.0} B")
    } else if bytes < KIB * KIB {
        format!("{:.0} KB", bytes / KIB)
    } else if bytes < KIB * KIB * KIB {
        format!("{:.1} MB", bytes / (KIB * KIB))
    } else {
        format!("{:.1} GB", bytes / (KIB * KIB * KIB))
    }
}

// ============================================================================
// Project Readers
// ============================================================================

/// Read project overview with stats, recent activity, and terminals
pub(super) async fn read_project(orch: &Orchestrator, project_key: &str) -> String {
    // Route to the project's owning database: the private DB for a local project,
    // the team replica for a shared one. PR #1953 refactored read_project to take
    // `orch` and read `orch.db.local`; resolving the owning DB by key here keeps
    // CAIRN-2129 read-routing so a team project's overview reads from its replica.
    let routed_db = orch.db.for_project(project_key).await;
    let db = &*routed_db;
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let lookup_key = cairn_common::uri::canonical_project(project_key);
    let mut project_rows = match conn
        .query(
            "
            SELECT id, name, context, key, repo_path
            FROM projects
            WHERE key = ?1
            LIMIT 1
            ",
            (lookup_key.as_str(),),
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => return storage_error("Failed to load project", error.into()),
    };

    let (project_id, project_name, project_context, canonical_key, repo_path): (
        String,
        String,
        Option<String>,
        String,
        String,
    ) = match project_rows.next().await {
        Ok(Some(row)) => match (
            row.text(0),
            row.text(1),
            row.opt_text(2),
            row.text(3),
            row.text(4),
        ) {
            (Ok(id), Ok(name), Ok(context), Ok(key), Ok(repo_path)) => {
                (id, name, context, key, repo_path)
            }
            _ => return "Failed to decode project".to_string(),
        },
        _ => return format!("Project '{}' not found", project_key),
    };

    let issue_stats = load_project_issue_stats(&conn, &project_id).await;
    let recent_issues = load_recent_project_issue_summaries(&conn, &project_id, 5).await;
    let labels =
        crate::labels::crud::list_labels_conn(&conn, crate::labels::crud::DEFAULT_WORKSPACE_ID)
            .await
            .unwrap_or_default();

    let mut terminals: Vec<(String, Option<String>, Option<String>, String)> = Vec::new();
    if let Ok(mut rows) = conn
        .query(
            "
            SELECT slug, title, description, status
            FROM job_terminals
            WHERE project_id = ?1
              AND job_id IS NULL
              AND slug IS NOT NULL
            ORDER BY created_at DESC
            ",
            (project_id.as_str(),),
        )
        .await
    {
        while let Ok(Some(row)) = rows.next().await {
            let (Ok(Some(slug)), Ok(title), Ok(description), Ok(status)) = (
                row.opt_text(0),
                row.opt_text(1),
                row.opt_text(2),
                row.text(3),
            ) else {
                continue;
            };
            terminals.push((slug, title, description, status));
        }
    }

    // Format as markdown
    let mut output = format!("# {}\n\n", project_name);

    if let Some(ctx) = project_context {
        if !ctx.is_empty() {
            output.push_str(&ctx);
            output.push_str("\n\n");
        }
    }

    output.push_str("## Stats\n\n");
    output.push_str(&format!(
        "- Total issues: {}\n- Open: {}\n- Waiting: {}\n- Merged: {}\n- Closed: {}\n\n",
        issue_stats.total,
        issue_stats.open,
        issue_stats.waiting,
        issue_stats.merged,
        issue_stats.closed
    ));

    if let Ok(pending) = crate::tenets::pending_count(db, &project_id).await {
        let inbox = format!(
            "{}?status=pending",
            cairn_common::uri::build_project_tenets_uri(&canonical_key)
        );
        output.push_str(&format!(
            "{}: [{inbox}]({inbox})\n\n",
            super::tenets::awaiting(pending)
        ));
    }

    output.push_str(&render_held_executions(&conn, &canonical_key, &project_id).await);

    output.push_str(&render_main_attestation(orch, &project_id));

    output.push_str("## [Labels](cairn://labels)\n\n");
    if labels.is_empty() {
        output.push_str("No labels defined.\n\n");
    } else {
        output.push_str(&format!(
            "{}\n\n",
            labels
                .iter()
                .map(|label| label.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    output.push_str("## References\n\n");
    let config = crate::config::project_settings::load_project_settings_read_only(
        std::path::Path::new(&repo_path),
    );
    let references = config.references.clone().unwrap_or_default();
    let statuses = crate::references::list_reference_status(&orch.config_dir, &references);
    if statuses.is_empty() {
        output.push_str("None.\n");
    } else {
        for status in &statuses {
            let description = if status.description.is_empty() {
                String::new()
            } else {
                format!(" — {}", status.description)
            };
            output.push_str(&format!(
                "- `{}` [{:?}] exists={}{} (`{}`)\n",
                status.name,
                status.reference_type,
                status.exists,
                description,
                build_project_reference_uri(&canonical_key, &status.name)
            ));
        }
    }
    output.push_str(&format!(
        "\nManage references: `{}`\n\n",
        build_project_references_uri(&canonical_key)
    ));
    if !repo_path.trim().is_empty() {
        let overview = crate::vcs_maintenance::ref_store_overview(
            orch,
            &project_id,
            std::path::Path::new(&repo_path),
        )
        .await;
        output.push_str(&render_ref_store(
            &overview,
            &canonical_key,
            chrono::Utc::now().timestamp(),
        ));
    }
    // Project terminals section (if any)
    if !terminals.is_empty() {
        output.push_str("## Terminals\n\n");
        for (slug, title, _description, status) in &terminals {
            let status_icon = match status.as_str() {
                "running" => "🟢",
                "exited" => "⚫",
                _ => "○",
            };
            let display_name = title.as_ref().unwrap_or(slug);
            output.push_str(&format!(
                "- `{}` {} {}\n",
                build_project_terminal_uri(&canonical_key, slug),
                status_icon,
                display_name
            ));
        }
        output.push('\n');
    }

    output.push_str("## Recent Activity\n\n");
    output.push_str(&format!(
        "Recent issue updates (latest {}). See the full listing at [{}]({}).\n\n",
        recent_issues.len().min(5),
        build_project_issues_uri(&canonical_key),
        build_project_issues_uri(&canonical_key)
    ));
    output.push_str(&render_recent_project_issue_entries(
        &canonical_key,
        &recent_issues,
    ));
    if !output.ends_with('\n') {
        output.push('\n');
    }
    output.push('\n');

    output
}

/// The `## Repository ref store` section: how big the project's shared ref
/// store is on this machine, how fast it is growing, and what maintenance last
/// did about it. Every state is said in words; a prune never renders as a bare
/// status code.
pub(super) fn render_ref_store(
    overview: &crate::vcs_maintenance::RefStoreOverview,
    project_key: &str,
    now: i64,
) -> String {
    use crate::vcs_maintenance::{
        PrunePolicy, KEEP_REF_THRESHOLD, PACKED_BYTES_THRESHOLD, PRUNE_INTERVAL, PRUNE_OVERDUE,
        RETENTION, TRUNCATION_OVERDUE,
    };
    const NAMESPACES: [(&str, &str); 8] = [
        ("jj/keep", "jj keep refs (`refs/jj/keep`)"),
        ("heads", "branches (`refs/heads`)"),
        ("remotes", "remote-tracking branches (`refs/remotes`)"),
        ("tags", "tags (`refs/tags`)"),
        ("cairn/checks", "check commits (`refs/cairn/checks`)"),
        ("cairn/stranded", "stranded deltas (`refs/cairn/stranded`)"),
        ("cairn/rescued", "rescued cell trees (`refs/cairn/rescued`)"),
        ("other", "other refs"),
    ];
    let ago = |at: i64| crate::clock::format_elapsed((now - at).max(0));
    let mut out = String::from("## Repository ref store\n\n");

    let mut over_threshold = Vec::new();
    match &overview.latest {
        None => out.push_str(
            "Not measured on this machine yet. The maintenance pass measures every project's \
             ref store within an hour of the runner starting.\n\n",
        ),
        Some(latest) => {
            out.push_str(&format!(
                "Measured {} ago: `packed-refs` holds {} refs in {}, and {} more refs are loose \
                 files.\n\n| Refs | Packed | Loose |\n|---|---|---|\n",
                ago(latest.at),
                thousands(latest.packed_entries),
                human_bytes(latest.packed_bytes as i64),
                thousands(latest.loose_refs),
            ));
            for (namespace, label) in NAMESPACES {
                let (packed, loose) = latest.namespace(namespace);
                out.push_str(&format!(
                    "| {label} | {} | {} |\n",
                    thousands(packed),
                    thousands(loose)
                ));
            }
            out.push('\n');
            let keep = |sample: &crate::vcs_maintenance::RefStoreSample| {
                let (packed, loose) = sample.namespace("jj/keep");
                packed + loose
            };
            let growth = [
                ("24 hours", &overview.day_ago),
                ("7 days", &overview.week_ago),
            ]
            .into_iter()
            .filter_map(|(window, sample)| {
                sample.as_ref().map(|sample| {
                    format!(
                        "{} refs ({} keep refs) than {window} ago",
                        difference(latest.total_refs(), sample.total_refs()),
                        difference(keep(latest), keep(sample)),
                    )
                })
            })
            .collect::<Vec<_>>();
            if growth.is_empty() {
                out.push_str(
                    "Growth appears here once there are 24 hours of measurements to compare \
                     against.\n\n",
                );
            } else {
                out.push_str(&format!("Growth: {}.\n\n", growth.join(", and ")));
            }
            if latest.ref_locks > 0 {
                out.push_str(&format!(
                    "{} leftover ref lock files under `refs/` each block every update of their \
                     ref; the maintenance sweep skips those refs.\n\n",
                    thousands(latest.ref_locks)
                ));
            }
            if keep(latest) > KEEP_REF_THRESHOLD {
                over_threshold.push(format!(
                    "{} jj keep refs is above the {} threshold",
                    thousands(keep(latest)),
                    thousands(KEEP_REF_THRESHOLD)
                ));
            }
            if latest.packed_bytes > PACKED_BYTES_THRESHOLD {
                over_threshold.push(format!(
                    "a {} `packed-refs` is above the {} threshold",
                    human_bytes(latest.packed_bytes as i64),
                    human_bytes(PACKED_BYTES_THRESHOLD as i64)
                ));
            }
        }
    }

    let awaiting_confirmation = overview.policy == PrunePolicy::Default
        && overview.last_ok_prune_at.is_none()
        && !over_threshold.is_empty();
    out.push_str(&render_object_store(overview, awaiting_confirmation, now));

    let confirm = format!(
        "`write({{changes:[{{target:\"cairn://p/{project_key}\",mode:\"patch\",\
         payload:{{refStorePrune:true}}}}]}})`"
    );
    if overview.policy == PrunePolicy::OptedOut {
        out.push_str(&format!(
            "**This store is opted out of pruning on this machine.** It keeps being measured, \
             and nothing removes old jj keep refs, operations, or finished Cairn refs. To prune \
             it again: {confirm}.\n\n"
        ));
        if !over_threshold.is_empty() {
            out.push_str(&format!(
                "This store needs a prune: {}.\n\n",
                over_threshold.join(", and ")
            ));
        }
        return out;
    }
    if awaiting_confirmation {
        out.push_str(&format!(
            "**The first prune of this store is waiting for confirmation**: {}. A first prune \
             of a store this size rewrites a large, long-lived ref store and object database in \
             one go, so it runs only once the operator has confirmed it, with {confirm}. Until \
             then the store keeps being measured and is never pruned. A store under both \
             thresholds is pruned without asking.\n\n",
            over_threshold.join(", and ")
        ));
        return out;
    }

    match &overview.last_prune {
        None => out.push_str(
            "Pruning is on (the default). No prune has run yet; the next hourly pass runs one.\n\n",
        ),
        Some(prune) => {
            let took = crate::clock::format_elapsed(prune.duration_ms / 1000);
            let attempt = format!("The last prune started {} ago", ago(prune.started_at));
            let removed = [
                format!(
                    "{} jj keep refs",
                    thousands(prune.removed_keep_refs.unwrap_or(0) as u64)
                ),
                format!(
                    "{} finished Cairn refs",
                    thousands(prune.removed_cairn_refs.unwrap_or(0) as u64)
                ),
                format!(
                    "{} operations",
                    thousands(prune.ops_abandoned.unwrap_or(0) as u64)
                ),
            ]
            .join(", ");
            match prune.outcome.split_once(':') {
                None if prune.outcome == "ok" => {
                    let truncated = if prune.truncated {
                        ", and truncated the operation log"
                    } else {
                        ""
                    };
                    out.push_str(&format!(
                        "{attempt} and succeeded in {took}. It removed {removed}{truncated}.\n\n"
                    ));
                }
                Some(("partial", failure)) if failure.starts_with("reclaim-failed:") => {
                    out.push_str(&format!(
                        "{attempt} and only partly succeeded, in {took}. It removed {removed}, but \
                         its reclaim of unreferenced objects failed; the object store above says \
                         why. The next prune retries the reclaim.\n\n"
                    ))
                }
                Some(("partial", refusal)) => out.push_str(&format!(
                    "{attempt} and only partly succeeded, in {took}. It removed {removed}, but it \
                     could not truncate the operation log, so hidden history is not being \
                     bounded: {refusal}. Forget or remove those workspaces and the next prune \
                     truncates.\n\n"
                )),
                Some(("skipped", reason)) => out.push_str(&format!(
                    "{attempt} and was skipped because {}. It retries at the next hourly pass.\n\n",
                    skip_reason(reason)
                )),
                Some(("failed", error)) => out.push_str(&format!(
                    "{attempt} and failed after {took}: {error}. It retries at the next hourly pass.\n\n"
                )),
                _ => out.push_str(&format!(
                    "{attempt} and recorded an outcome this page does not recognize: `{}`.\n\n",
                    prune.outcome
                )),
            }
        }
    }

    match overview.last_ok_prune_at {
        Some(last) if now - last > PRUNE_OVERDUE.as_secs() as i64 => out.push_str(&format!(
            "**Pruning is overdue:** the last successful prune was {} ago, more than 48 hours.\n\n",
            ago(last)
        )),
        None if overview.last_prune.is_some() => {
            out.push_str("**No prune of this store has succeeded yet.**\n\n")
        }
        _ => {}
    }
    // Ref sweeps can keep succeeding while the operation log, and with it every
    // hidden head it pins, goes untruncated; that is the larger bound.
    if overview.last_ok_prune_at.is_some() {
        match overview.last_truncation_at {
            None => out.push_str(
                "**The operation log has never been truncated on this machine**, so its hidden \
                 history keeps growing until a prune truncates it.\n\n",
            ),
            Some(last) if now - last > TRUNCATION_OVERDUE.as_secs() as i64 => {
                out.push_str(&format!(
                    "**The operation log was last truncated {} ago**, more than 14 days, so its \
                     hidden history is growing past the retention window.\n\n",
                    ago(last)
                ))
            }
            _ => {}
        }
    }
    if !over_threshold.is_empty() {
        out.push_str(&format!(
            "A prune is due at the next hourly pass: {}.\n\n",
            over_threshold.join(", and ")
        ));
    } else if let Some(last) = overview.last_ok_prune_at {
        let due = last + PRUNE_INTERVAL.as_secs() as i64;
        if due > now {
            out.push_str(&format!(
                "The next prune is due in {}.\n\n",
                crate::clock::format_elapsed(due - now)
            ));
        } else {
            out.push_str("A prune is due at the next hourly pass.\n\n");
        }
    }
    out.push_str(&format!(
        "Hidden history, operations, and unreferenced objects are kept for {} days; an older \
         commit that nothing references any more stops resolving.\n\n",
        RETENTION.as_secs() / 86_400
    ));
    out
}

/// The object-store paragraph of the ref-store section: how many packs and
/// loose objects the store holds, when it was last compacted and reclaimed,
/// and, when something is wrong, what.
fn render_object_store(
    overview: &crate::vcs_maintenance::RefStoreOverview,
    awaiting_confirmation: bool,
    now: i64,
) -> String {
    use crate::vcs_maintenance::{
        PrunePolicy, LOOSE_OBJECT_LIMIT, PACK_LIMIT, RECLAIM_INTERVAL, RETENTION,
    };
    let ago = |at: i64| crate::clock::format_elapsed((now - at).max(0));
    let Some(census) = overview.objects else {
        return if overview.latest.is_some() {
            "Object store: not measured on this machine yet; the next hourly pass measures it.\n\n"
                .to_string()
        } else {
            String::new()
        };
    };
    let kept = if census.kept_packs > 0 {
        format!(
            " ({} held by in-flight publications)",
            thousands(census.kept_packs)
        )
    } else {
        String::new()
    };
    let mut out = format!(
        "Object store: {} packs{kept} in {}, about {} loose objects, {}.",
        thousands(census.packs),
        human_bytes(census.pack_bytes as i64),
        thousands(census.loose_objects),
        if census.multi_pack_index {
            "with a multi-pack index"
        } else {
            "without a multi-pack index"
        }
    );
    let opted_out = overview.policy == PrunePolicy::OptedOut;

    let compaction_problem = match &overview.last_compaction {
        Some(step) if step.outcome == "ok" => {
            let packs = match (step.before, step.after) {
                (Some(before), Some(after)) => format!(
                    "{} packs to {}, ",
                    thousands(before.packs),
                    thousands(after.packs)
                ),
                _ => String::new(),
            };
            out.push_str(&format!(
                " Last compacted {} ago ({packs}in {}).",
                ago(step.started_at),
                duration_words(step.duration_ms)
            ));
            None
        }
        Some(step) => {
            let what = match step.outcome.split_once(':') {
                Some(("skipped", reason)) => format!("was skipped because {}", skip_reason(reason)),
                Some(("failed", error)) => format!("failed: {error}"),
                _ => format!("recorded `{}`", step.outcome),
            };
            out.push_str(&format!(
                " The last compaction, {} ago, {what}.",
                ago(step.started_at)
            ));
            Some(what)
        }
        None if !opted_out => {
            out.push_str(&format!(
                " Not compacted on this machine yet; it compacts whenever the store passes {} \
                 packs or {} loose objects.",
                PACK_LIMIT,
                thousands(LOOSE_OBJECT_LIMIT)
            ));
            None
        }
        None => None,
    };

    if !opted_out {
        let days = RETENTION.as_secs() / 86_400;
        match &overview.last_reclaim {
            Some(step) => {
                let bytes = match (step.before, step.after) {
                    (Some(before), Some(after)) => format!(
                        "{} to {}, ",
                        human_bytes(before.pack_bytes as i64),
                        human_bytes(after.pack_bytes as i64)
                    ),
                    _ => String::new(),
                };
                let due = step.started_at + RECLAIM_INTERVAL.as_secs() as i64;
                let next = if due > now {
                    format!(
                        "the next reclaim is due in {}",
                        crate::clock::format_elapsed(due - now)
                    )
                } else {
                    "a reclaim is due at the next prune".to_string()
                };
                out.push_str(&format!(
                    " Unreferenced objects older than {days} days were last reclaimed {} ago \
                     ({bytes}in {}); {next}.",
                    ago(step.started_at),
                    duration_words(step.duration_ms)
                ));
            }
            None if awaiting_confirmation => out.push_str(
                " Unreferenced objects have never been reclaimed on this machine. The reclaim \
                 runs inside the prune, so it waits on the same confirmation.",
            ),
            None => out.push_str(
                " Unreferenced objects have never been reclaimed on this machine; the next prune \
                 reclaims those older than the retention window.",
            ),
        }
        if let Some(prune) = &overview.last_prune {
            if let Some(reason) = prune.reclaim_skipped.as_deref() {
                out.push_str(&format!(
                    " The reclaim due at the last prune was skipped because {}; the next prune \
                     retries it.",
                    skip_reason(reason)
                ));
            }
            if let Some(error) = prune.reclaim_failed.as_deref() {
                out.push_str(&format!(
                    " **The reclaim at the last prune failed:** {error}. The next prune retries \
                     it."
                ));
            }
        }
    }
    out.push_str("\n\n");

    let unkept = census.packs.saturating_sub(census.kept_packs);
    if opted_out {
        out.push_str(
            "Because this store is opted out, its objects are neither compacted nor reclaimed.\n\n",
        );
    } else if unkept > PACK_LIMIT {
        let why = match compaction_problem {
            Some(what) => format!("the last compaction {what}"),
            None => "a compaction runs at the next hourly pass".to_string(),
        };
        out.push_str(&format!(
            "**The object store is over git's {PACK_LIMIT}-pack limit** with {} packs not held \
             by a publication, and {why}. Until it is compacted, every git and jj process pays \
             to open each pack.\n\n",
            thousands(unkept)
        ));
    }
    out
}

/// A step's duration in words, to the second below a minute.
fn duration_words(ms: i64) -> String {
    if ms < 60_000 {
        format!("{}s", ms.max(0) / 1000)
    } else {
        crate::clock::format_elapsed(ms / 1000)
    }
}

fn skip_reason(reason: &str) -> String {
    if let Some(held) = reason.strip_prefix("operator-git-busy:") {
        return match held.split_once(' ') {
            Some((lock, age)) => format!("the operator's git held `{lock}` {age}"),
            None => format!("the operator's git held `{held}`"),
        };
    }
    match reason {
        "reconcile-in-flight" => "a base-advance reconcile was in flight for this store".into(),
        "operator-git-busy" => {
            "the operator's git held `index.lock` or `packed-refs.lock` on the checkout".into()
        }
        "store-busy" => "the store lock stayed busy for ten minutes".into(),
        "awaiting-first-prune-confirmation" => {
            "its first prune is waiting for the operator's confirmation".into()
        }
        other => format!("`{other}`"),
    }
}

fn thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

fn difference(now: u64, then: u64) -> String {
    match now.cmp(&then) {
        std::cmp::Ordering::Greater => format!("{} more", thousands(now - then)),
        std::cmp::Ordering::Less => format!("{} fewer", thousands(then - now)),
        std::cmp::Ordering::Equal => "the same number of".to_string(),
    }
}

/// Every held execution on an open issue, each naming what it waits on and what
/// the evidence says, and apart from them every gated execution on an open
/// issue that can never start (superseded while it waited).
///
/// A held execution has no jobs and its issue derives Backlog, so it appears in
/// no activity listing and raises no attention — a gate that never comes true is
/// otherwise silent. This section is how the project overview keeps it visible.
/// The won't-start list is what keeps a dead execution from posing as a
/// waiting one. Silent only when there is neither.
async fn render_held_executions(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    project_id: &str,
) -> String {
    let held =
        match crate::execution::gates::census::held_executions_for_project_conn(conn, project_id)
            .await
        {
            Ok(held) => held,
            // Saying the census could not be read is better than omitting it:
            // an absent section reads as "nothing is held".
            Err(error) => {
                return format!("## Held executions\n\nCould not read held executions: {error}\n\n")
            }
        };
    let (waiting, wont_start): (Vec<_>, Vec<_>) =
        held.iter().partition(|entry| entry.execution.census.held);
    let entry_line = |entry: &crate::execution::gates::census::ProjectHeldExecution| {
        format!(
            "- [{}-{}]({}) {} — {}\n",
            cairn_common::uri::canonical_project(project_key),
            entry.issue_number,
            build_issue_uri(project_key, entry.issue_number as i32),
            entry.issue_title,
            entry.execution.line
        )
    };
    let mut out = String::new();
    if !waiting.is_empty() {
        out.push_str("## Held executions\n\n");
        out.push_str("Filed with `after:` gates and not yet started.\n\n");
        for entry in waiting {
            out.push_str(&entry_line(entry));
        }
        out.push('\n');
    }
    if !wont_start.is_empty() {
        out.push_str("## Won't start\n\n");
        out.push_str(
            "Filed with `after:` gates but superseded before release: these will never start.\n\n",
        );
        for entry in wont_start {
            out.push_str(&entry_line(entry));
        }
        out.push('\n');
    }
    out
}

// ============================================================================
// Projects collection + project settings readers
// ============================================================================

/// The main-cadence pass running right now, if one is.
///
/// A pass records nothing until its whole-suite lanes return — most of an hour
/// for the Rust suite — so for that whole window the store holds no cadence
/// observation for the freshly advanced head. Read alone, that is the same
/// emptiness a cadence that never ran would leave behind, and CAIRN-3823 was
/// filed on exactly that reading. The line below is the difference: while it is
/// present, the absence of rows for the current head is a pass still working,
/// not a gap.
///
/// Silent when nothing is running, because then the rows and the cadence's
/// thread messages already say everything there is to say.
fn render_main_attestation(orch: &Orchestrator, project_id: &str) -> String {
    let Some((branch, armed_at_unix_ms)) =
        crate::execution::checks_main::in_flight(orch, project_id)
    else {
        return String::new();
    };
    let elapsed_seconds = (chrono::Utc::now().timestamp_millis() - armed_at_unix_ms).max(0) / 1000;
    format!(
        "## Main cadence\n\nA whole-suite pass over `{branch}` was armed {} ago. Its rows for \
         the current default head are recorded when it finishes, so rows missing for it now are \
         pending, not absent.\n\n",
        crate::clock::format_elapsed(elapsed_seconds)
    )
}

pub(super) async fn read_projects(db: &LocalDb) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let mut rows = match conn
        .query(
            "SELECT key, name, hidden FROM projects ORDER BY name ASC",
            (),
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => return storage_error("Failed to list projects", error.into()),
    };

    let mut out = String::from("# Projects\n\n");
    let mut any = false;
    while let Ok(Some(row)) = rows.next().await {
        let (Ok(key), Ok(name), Ok(hidden)) = (row.text(0), row.text(1), row.i64(2)) else {
            continue;
        };
        any = true;
        let hidden_mark = if hidden != 0 { " (hidden)" } else { "" };
        out.push_str(&format!(
            "- [{}]({}) {}{}\n",
            key,
            build_project_uri(&key),
            name,
            hidden_mark
        ));
    }
    if !any {
        out.push_str("No projects.\n");
    }
    out.push('\n');
    out
}

fn render_configured_reference(
    project_key: &str,
    reference: &crate::references::ProjectReference,
    status: &crate::references::ReferenceStatus,
    detailed: bool,
) -> String {
    let mut out = String::new();
    if detailed {
        out.push_str(&format!("# Project reference — {}\n\n", reference.name));
        out.push_str(&format!("- project: `{}`\n", project_key));
        out.push_str(&format!(
            "- uri: `{}`\n",
            build_project_reference_uri(project_key, &reference.name)
        ));
        out.push_str(&format!("- type: {:?}\n", status.reference_type));
        out.push_str(&format!("- exists: {}\n", status.exists));
        if let Some(git) = &reference.git {
            out.push_str(&format!("- git: `{}`\n", git));
        }
        if let Some(path) = &reference.path {
            out.push_str(&format!("- path: `{}`\n", path));
        }
        if let Some(branch) = &reference.branch {
            out.push_str(&format!("- branch: `{}`\n", branch));
        }
        if let Some(path) = &status.resolved_path {
            out.push_str(&format!("- resolvedPath: `{}`\n", path));
        } else {
            out.push_str("- resolvedPath: null\n");
        }
        if status.description.is_empty() {
            out.push_str("- description: \n");
        } else {
            out.push_str(&format!("- description: {}\n", status.description));
        }
        return out;
    }

    out.push_str(&format!(
        "- `{}` [{:?}] exists={}\n",
        reference.name, status.reference_type, status.exists
    ));
    out.push_str(&format!(
        "  - uri: `{}`\n",
        build_project_reference_uri(project_key, &reference.name)
    ));
    if let Some(git) = &reference.git {
        out.push_str(&format!("  - git: `{}`\n", git));
    }
    if let Some(path) = &reference.path {
        out.push_str(&format!("  - path: `{}`\n", path));
    }
    if let Some(branch) = &reference.branch {
        out.push_str(&format!("  - branch: `{}`\n", branch));
    }
    if let Some(path) = &status.resolved_path {
        out.push_str(&format!("  - resolvedPath: `{}`\n", path));
    } else {
        out.push_str("  - resolvedPath: null\n");
    }
    if !status.description.is_empty() {
        out.push_str(&format!("  - description: {}\n", status.description));
    }
    out
}

pub(super) async fn read_project_references(orch: &Orchestrator, project_key: &str) -> String {
    let conn = match connect_for_read(&orch.db.local).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let lookup = cairn_common::uri::canonical_project(project_key);
    let mut rows = match conn
        .query(
            "SELECT repo_path FROM projects WHERE key = ?1 LIMIT 1",
            (lookup.as_str(),),
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => return storage_error("Failed to load project", error.into()),
    };
    let repo_path = match rows.next().await {
        Ok(Some(row)) => match row.text(0) {
            Ok(repo) => repo,
            _ => return "Failed to decode project".to_string(),
        },
        _ => return format!("Project '{}' not found", project_key),
    };
    let config = crate::config::project_settings::load_project_settings_read_only(
        std::path::Path::new(&repo_path),
    );
    let references = config.references.clone().unwrap_or_default();
    let statuses = crate::references::list_reference_status(&orch.config_dir, &references);

    let mut out = format!("# Project references — {}\n\n", lookup);
    if references.is_empty() {
        out.push_str("None.\n\n");
        out.push_str(&format!(
            "Add references at `{}`.\n",
            build_project_references_uri(&lookup)
        ));
        return out;
    }
    for reference in &references {
        if let Some(status) = statuses.iter().find(|status| status.name == reference.name) {
            out.push_str(&render_configured_reference(
                &lookup, reference, status, false,
            ));
        }
    }
    out
}

pub(super) async fn read_project_reference(
    orch: &Orchestrator,
    project_key: &str,
    name: &str,
) -> String {
    let conn = match connect_for_read(&orch.db.local).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let lookup = cairn_common::uri::canonical_project(project_key);
    let mut rows = match conn
        .query(
            "SELECT repo_path FROM projects WHERE key = ?1 LIMIT 1",
            (lookup.as_str(),),
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => return storage_error("Failed to load project", error.into()),
    };
    let repo_path = match rows.next().await {
        Ok(Some(row)) => match row.text(0) {
            Ok(repo) => repo,
            _ => return "Failed to decode project".to_string(),
        },
        _ => return format!("Project '{}' not found", project_key),
    };
    let config = crate::config::project_settings::load_project_settings_read_only(
        std::path::Path::new(&repo_path),
    );
    let references = config.references.clone().unwrap_or_default();
    let Some(reference) = references.iter().find(|reference| reference.name == name) else {
        return format!("Project reference '{}' not found in {}", name, lookup);
    };
    let statuses = crate::references::list_reference_status(&orch.config_dir, &references);
    let Some(status) = statuses.iter().find(|status| status.name == name) else {
        return format!("Project reference '{}' not found in {}", name, lookup);
    };
    render_configured_reference(&lookup, reference, status, true)
}

pub(super) async fn read_project_settings(orch: &Orchestrator, project_key: &str) -> String {
    let routed_db = orch.db.for_project(project_key).await;
    let conn = match connect_for_read(&routed_db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let lookup = cairn_common::uri::canonical_project(project_key);
    let mut rows = match conn
        .query(
            "SELECT id, repo_path, default_branch FROM projects WHERE key = ?1 LIMIT 1",
            (lookup.as_str(),),
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) => return storage_error("Failed to load project", error.into()),
    };
    let (project_id, repo_path, default_branch): (String, String, Option<String>) =
        match rows.next().await {
            Ok(Some(row)) => match (row.text(0), row.text(1), row.opt_text(2)) {
                (Ok(id), Ok(repo), Ok(branch)) => (id, repo, branch),
                _ => return "Failed to decode project".to_string(),
            },
            _ => return format!("Project '{}' not found", project_key),
        };

    let repo = std::path::Path::new(&repo_path);
    let config = crate::config::project_settings::load_project_settings_read_only(repo);
    let resolved_branch =
        crate::config::project_settings::resolve_default_branch(&config, default_branch.as_deref());

    let mut out = format!("# Project settings — {}\n\n", lookup);
    out.push_str(&format!("- repoPath: `{}`\n", repo_path));
    out.push_str(&format!("- defaultBranch: `{}`\n\n", resolved_branch));

    out.push_str("## Setup commands\n\n");
    match &config.setup_commands {
        Some(cmds) if !cmds.is_empty() => {
            for command in cmds {
                out.push_str(&format!("- `{}`\n", command));
            }
        }
        _ => out.push_str("None.\n"),
    }
    out.push('\n');

    out.push_str("## Terminal commands\n\n");
    match &config.terminal_commands {
        Some(cmds) if !cmds.is_empty() => {
            for command in cmds {
                out.push_str(&format!("- {} — `{}`\n", command.name, command.command));
            }
        }
        _ => out.push_str("None.\n"),
    }
    out.push('\n');

    let populate = config.populate_config();
    out.push_str("## Worktree populate\n\n");
    if populate.is_empty() {
        out.push_str("None (worktrees start clean).\n");
    } else {
        out.push_str(&format!("- copy: {}\n", populate.copy.join(", ")));
    }
    out.push('\n');

    out.push_str("## Identity overrides\n\n");
    match orch.get_project_overrides(&project_id) {
        Some(overrides) => {
            let fields = [
                ("anthropicAccountId", &overrides.anthropic_account_id),
                ("openaiAccountId", &overrides.openai_account_id),
                ("githubAccountId", &overrides.github_account_id),
                ("gitIdentityId", &overrides.git_identity_id),
                ("gitName", &overrides.git_name),
                ("gitEmail", &overrides.git_email),
            ];
            let mut printed = false;
            for (label, value) in fields {
                if let Some(value) = value {
                    out.push_str(&format!("- {label}: {value}\n"));
                    printed = true;
                }
            }
            if !printed {
                out.push_str("None.\n");
            }
        }
        None => out.push_str("None.\n"),
    }
    out.push('\n');

    out.push_str("## References\n\n");
    let references = config.references.clone().unwrap_or_default();
    let statuses = crate::references::list_reference_status(&orch.config_dir, &references);
    if statuses.is_empty() {
        out.push_str("None.\n");
    } else {
        for status in &statuses {
            out.push_str(&format!(
                "- `{}` [{:?}] exists={} {}\n",
                status.name, status.reference_type, status.exists, status.description
            ));
        }
    }
    out.push('\n');

    out
}

// ============================================================================
// Project Search Reader
// ============================================================================

pub(super) async fn read_project_search(
    orch: &Orchestrator,
    project_key: &str,
    params: &[QueryParam],
) -> String {
    let query = match find_query_value(params, "search") {
        Some(query) if !query.is_empty() => query,
        _ => return "Query parameter 'search' is required for projected project reads".to_string(),
    };

    if let Some(unsupported) = params.iter().find(|param| {
        ![
            "search",
            "limit",
            "since",
            "content_types",
            "role",
            "in",
            "issue",
        ]
        .contains(&param.key.as_str())
    }) {
        return format!(
            "Unsupported query parameter '{}' for project search projection",
            unsupported.key
        );
    }

    let routed_db = orch.db.for_project(project_key).await;
    let conn = match connect_for_read(&routed_db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let project_ctx = match lookup_project_by_key(&conn, project_key).await {
        Ok(ctx) => ctx,
        Err(error) => return error,
    };

    let limit = match parse_optional_usize_param(params, "limit") {
        Ok(limit) => limit,
        Err(error) => return error,
    };
    let since = match parse_optional_i64_param(params, "since") {
        Ok(since) => since,
        Err(error) => return error,
    };
    let content_types = find_query_value(params, "content_types").map(|value| {
        value
            .split(',')
            .map(|entry| entry.trim().to_string())
            .filter(|entry| !entry.is_empty())
            .collect::<Vec<_>>()
    });

    let role = find_query_value(params, "role").map(str::to_string);

    // `in=title` is the only supported axis for now (title-only matching).
    let title_only = match find_query_value(params, "in") {
        Some("title") => true,
        None => false,
        Some(other) => {
            return format!(
                "Unsupported 'in' value '{other}' for project search projection (expected 'title')"
            )
        }
    };

    // `issue=NUMBER` scopes the search to one issue's accumulated history
    // (executions, transcripts, comments) rather than finding the issue itself.
    let issue_id = match find_query_value(params, "issue") {
        Some(raw) => match raw.trim().parse::<i32>() {
            Ok(number) => {
                match super::common::issue_id_for_number(&conn, &project_ctx.project_id, number)
                    .await
                {
                    Some(id) => Some(id),
                    None => {
                        return format!("Issue {}/{} not found", project_ctx.project_key, number)
                    }
                }
            }
            Err(_) => {
                return format!("Invalid 'issue' value '{raw}' for project search projection")
            }
        },
        None => None,
    };

    let filters = crate::models::SearchFilters {
        project_id: Some(project_ctx.project_id),
        issue_id,
        content_types,
        role,
        title_only,
        since,
        limit,
    };

    // Drain every open database's outbox so a shared project's locally-originated
    // writes are indexed before this query (the index is URI-keyed and shared).
    if let Err(error) = orch.db.apply_pending_search().await {
        return format!("Search index update failed: {error}");
    }
    let embedder = orch.embedder();
    match crate::search::search_content(
        &routed_db,
        &orch.db.search_index,
        query,
        Some(filters),
        Some(crate::search::SemanticLane {
            search: orch.semantic_search(),
            embedder: embedder.as_ref(),
            vectors: &orch.db.local,
        }),
    )
    .await
    {
        Ok(results) => crate::mcp::handlers::search::format_search_results(
            &results,
            Some(project_ctx.project_key.as_str()),
        ),
        Err(error) => format!("Search failed: {error}"),
    }
}

#[cfg(test)]
mod project_issues_query_tests {
    use super::*;
    use crate::labels::{
        attach::replace_issue_labels,
        crud::{create_label_conn, DEFAULT_WORKSPACE_ID},
    };
    use crate::models::CreateLabel;
    use crate::storage::LocalDb;
    use tempfile::tempdir;

    fn param(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn parses_status_limit_and_sort() {
        let query = ProjectIssuesQuery::parse(&[
            param("status", "backlog,active"),
            param("limit", "10"),
            param("sort", "updated_desc"),
        ])
        .unwrap();

        assert_eq!(
            query.statuses,
            Some(vec!["backlog".to_string(), "active".to_string()])
        );
        assert_eq!(query.limit, 10);
        assert_eq!(query.sort_field, ProjectIssuesSortField::Updated);
        assert_eq!(query.sort_dir, ProjectIssuesSortDir::Desc);
    }

    #[test]
    fn defaults_limit_and_sort() {
        let query = ProjectIssuesQuery::parse(&[]).unwrap();
        assert_eq!(query.limit, 20);
        assert_eq!(query.sort_field, ProjectIssuesSortField::Updated);
        assert_eq!(query.sort_dir, ProjectIssuesSortDir::Desc);
    }

    #[test]
    fn rejects_invalid_values() {
        assert!(ProjectIssuesQuery::parse(&[param("sort", "number_desc")])
            .unwrap_err()
            .contains("Invalid sort query parameter"));
        assert!(ProjectIssuesQuery::parse(&[param("limit", "0")])
            .unwrap_err()
            .contains("Invalid limit query parameter"));
        assert!(ProjectIssuesQuery::parse(&[param("status", "open")])
            .unwrap_err()
            .contains("Invalid status query parameter"));
    }

    #[test]
    fn rejects_unknown_params_with_supported_list() {
        let error = ProjectIssuesQuery::parse(&[param("foo", "bar")]).unwrap_err();
        assert!(error.contains("Unsupported query parameter 'foo' for project issues"));
        assert!(error.contains("Supported parameters: status, limit, offset, sort, label, labels"));
    }

    #[test]
    fn the_retired_ready_filter_points_to_after_gates() {
        let error = ProjectIssuesQuery::parse(&[param("ready", "true")]).unwrap_err();
        assert!(error.contains("'ready'"), "{error}");
        assert!(error.contains("after:"), "{error}");
    }

    #[test]
    fn the_retired_kind_filter_points_to_the_threads_collection() {
        let error = ProjectIssuesQuery::parse(&[param("kind", "thread")]).unwrap_err();
        assert!(error.contains("'kind'"), "{error}");
        assert!(error.contains("cairn://p/PROJECT/threads"), "{error}");
    }

    /// Every filter the affordance block advertises must be accepted by this
    /// reader and change the query it builds. A key added to the contract
    /// without a parser arm would be accepted and silently ignored; one listed
    /// without a sample here fails the key-set comparison first.
    #[test]
    fn every_advertised_filter_is_accepted_and_takes_effect() {
        let samples: &[(&str, &str)] = &[
            ("status", "active"),
            ("limit", "5"),
            ("offset", "1"),
            ("sort", "created_asc"),
            ("label", "bug"),
            ("labels", "bug,ui"),
        ];
        let advertised = cairn_common::contract::contract_for(
            cairn_common::contract::ResourceKind::ProjectIssues,
        )
        .expect("project issues contract")
        .read_projections
        .iter()
        .map(|projection| projection.key)
        .collect::<Vec<_>>();
        assert_eq!(
            advertised,
            samples.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            "each advertised filter needs a sample value here"
        );

        for (key, value) in samples {
            let query = ProjectIssuesQuery::parse(&[param(key, value)])
                .unwrap_or_else(|error| panic!("advertised filter `{key}` refused: {error}"));
            assert_ne!(
                query,
                ProjectIssuesQuery::default(),
                "advertised filter `{key}={value}` is accepted but has no effect"
            );
        }
    }

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(temp.path().join("project-labels.db"))
            .await
            .unwrap();
        db
    }

    async fn seed_project(conn: &cairn_db::turso::Connection) {
        conn.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-labels', 'default', 'Labels', 'LBL', '/tmp/lbl', 1, 1)",
            (),
        )
        .await
        .unwrap();
    }

    async fn seed_issue(
        conn: &cairn_db::turso::Connection,
        issue_id: &str,
        number: i32,
        title: &str,
    ) {
        conn.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at) VALUES (?1, 'p-labels', ?2, ?3, '', 'backlog', 'backlog', 'none', 0, ?2, ?2)",
            params![issue_id, number, title],
        )
        .await
        .unwrap();
    }

    async fn seed_label(conn: &cairn_db::turso::Connection, name: &str) {
        create_label_conn(
            conn,
            DEFAULT_WORKSPACE_ID,
            CreateLabel {
                name: name.to_string(),
                color: None,
            },
            2,
        )
        .await
        .unwrap();
    }

    /// A held execution's gates and their evidence reach both the project
    /// overview and the `/issues` collection, and an issue whose execution
    /// finished normally reads as nothing held.
    #[tokio::test]
    async fn held_executions_reach_the_project_overview_and_issue_list() {
        use crate::db::DbState;
        use crate::execution::gates::census::test_support::{hold_execution, HELD_SUMMARY};
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn).await;
                // Project lookup is by canonical (lowercase) key.
                conn.execute("UPDATE projects SET key = 'lbl'", ()).await?;
                seed_issue(conn, "i-held", 1, "Rerun after the fix").await;
                seed_issue(conn, "i-done", 2, "Finished work").await;
                conn.execute(
                    "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('e-held','r','i-held','p-labels','held',1,1),
                            ('e-done','r','i-done','p-labels','complete',1,1)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
        hold_execution(&db, "e-held").await;

        let issues = produce_project_issues(&db, "lbl", &[]).await.unwrap().body;
        assert!(
            issues.contains(&format!(
                "Rerun after the fix\n  - exec 1 — {HELD_SUMMARY}\n"
            )),
            "{issues}"
        );
        assert!(issues.contains("Finished work\n"), "{issues}");
        assert_eq!(issues.matches("exec 1 —").count(), 1, "{issues}");

        let root = tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(db), search_index)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .build();
        let overview = read_project(&orch, "lbl").await;
        assert!(
            overview.contains(
                "## Held executions\n\nFiled with `after:` gates and not yet started.\n\n- ["
            ),
            "{overview}"
        );
        assert!(
            overview.contains(&format!(
                "(cairn://p/lbl/1) Rerun after the fix — exec 1 — {HELD_SUMMARY}\n"
            )),
            "{overview}"
        );
        assert!(!overview.contains("Finished work —"), "{overview}");
    }

    #[tokio::test]
    async fn filters_by_single_label_ref() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn).await;
                seed_issue(conn, "i-bug", 1, "Bug issue").await;
                seed_issue(conn, "i-ui", 2, "UI issue").await;
                seed_label(conn, "Bug").await;
                seed_label(conn, "UI").await;
                replace_issue_labels(conn, "i-bug", &["bug".to_string()], 3)
                    .await
                    .unwrap();
                replace_issue_labels(conn, "i-ui", &["ui".to_string()], 3)
                    .await
                    .unwrap();

                let query = ProjectIssuesQuery::parse(&[param("label", "Bug")]).unwrap();
                let (issues, total) = load_project_issue_summaries(conn, "p-labels", &query)
                    .await
                    .unwrap();
                assert_eq!(total, 1);
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0].title, "Bug issue");
                assert_eq!(issues[0].labels, vec!["Bug".to_string()]);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn filters_by_multiple_labels_with_and_semantics() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn).await;
                seed_issue(conn, "i-bug", 1, "Bug only").await;
                seed_issue(conn, "i-both", 2, "Bug and UI").await;
                seed_label(conn, "Bug").await;
                seed_label(conn, "UI").await;
                replace_issue_labels(conn, "i-bug", &["bug".to_string()], 3)
                    .await
                    .unwrap();
                replace_issue_labels(conn, "i-both", &["bug".to_string(), "ui".to_string()], 3)
                    .await
                    .unwrap();

                let query = ProjectIssuesQuery::parse(&[param("labels", "bug,ui")]).unwrap();
                let (issues, total) = load_project_issue_summaries(conn, "p-labels", &query)
                    .await
                    .unwrap();
                assert_eq!(total, 1);
                assert_eq!(issues.len(), 1);
                assert_eq!(issues[0].title, "Bug and UI");
                assert_eq!(issues[0].labels, vec!["Bug".to_string(), "UI".to_string()]);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn paging_limit_offset_returns_window_and_total() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn).await;
                seed_issue(conn, "i-1", 1, "One").await;
                seed_issue(conn, "i-2", 2, "Two").await;
                seed_issue(conn, "i-3", 3, "Three").await;

                let query = ProjectIssuesQuery::parse(&[param("limit", "1"), param("offset", "1")])
                    .unwrap();
                let (issues, total) = load_project_issue_summaries(conn, "p-labels", &query)
                    .await
                    .unwrap();
                // The window is one issue but the total counts all three.
                assert_eq!(total, 3);
                assert_eq!(issues.len(), 1);
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn unknown_label_filter_returns_no_matches() {
        let db = test_db().await;
        db.write(|conn| {
            Box::pin(async move {
                seed_project(conn).await;
                seed_issue(conn, "i-bug", 1, "Bug only").await;
                seed_label(conn, "Bug").await;
                replace_issue_labels(conn, "i-bug", &["bug".to_string()], 3)
                    .await
                    .unwrap();

                let query = ProjectIssuesQuery::parse(&[param("label", "missing")]).unwrap();
                let (issues, total) = load_project_issue_summaries(conn, "p-labels", &query)
                    .await
                    .unwrap();
                assert_eq!(total, 0);
                assert!(issues.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
mod ref_store_render_tests {
    use super::*;
    use crate::db::DbState;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::SearchIndex;
    use crate::vcs_maintenance::{
        ObjectStepSample, PrunePolicy, PruneSample, RefStoreOverview, RefStoreSample,
    };
    use cairn_db::turso::params;
    use std::sync::Arc;

    const NOW: i64 = 1_800_000_000;
    const HOUR: i64 = 3600;

    fn sample(at: i64, packed_entries: u64, keep: u64) -> RefStoreSample {
        RefStoreSample {
            at,
            packed_bytes: 14_021_377,
            packed_entries,
            loose_refs: 68_189,
            ref_locks: 5,
            namespaces: vec![
                ("jj/keep".into(), keep, 66_386),
                ("cairn/checks".into(), 1_922, 1_001),
            ],
        }
    }

    #[test]
    fn an_unmeasured_store_says_when_it_will_be_measured() {
        let text = render_ref_store(&RefStoreOverview::default(), "CAIRN", NOW);
        assert!(text.starts_with("## Repository ref store"));
        assert!(text.contains("Not measured on this machine yet"));
        assert!(text.contains("Pruning is on (the default)"), "{text}");
    }

    #[test]
    fn an_opted_out_store_says_it_is_measured_and_never_pruned() {
        let overview = RefStoreOverview {
            policy: PrunePolicy::OptedOut,
            latest: Some(sample(NOW - 600, 147_863, 143_681)),
            ..Default::default()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(
            text.contains("**This store is opted out of pruning on this machine.**"),
            "{text}"
        );
        assert!(text.contains("refStorePrune:true"));
        assert!(text.contains("This store needs a prune"));
    }

    #[test]
    fn a_large_unpruned_store_waits_for_its_first_prune_confirmation() {
        let overview = RefStoreOverview {
            latest: Some(sample(NOW - 600, 147_863, 143_681)),
            day_ago: Some(sample(NOW - 25 * HOUR, 143_722, 139_540)),
            ..Default::default()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(
            text.contains("`packed-refs` holds 147,863 refs in 13.4 MB"),
            "{text}"
        );
        assert!(text.contains("| jj keep refs (`refs/jj/keep`) | 143,681 | 66,386 |"));
        assert!(text.contains("4,141 more refs (4,141 more keep refs) than 24 hours ago"));
        assert!(text.contains("5 leftover ref lock files"));
        assert!(text.contains("**The first prune of this store is waiting for confirmation**"));
        assert!(text.contains("refStorePrune:true"));
        assert!(text.contains("cairn://p/CAIRN"));
        assert!(text.contains("above the 50,000 threshold"));
        assert!(text.contains("keeps being measured and is never pruned"));

        // Once the store has been pruned, growth past a threshold needs nothing.
        let pruned_before = RefStoreOverview {
            last_ok_prune_at: Some(NOW - 2 * HOUR),
            last_truncation_at: Some(NOW - 2 * HOUR),
            ..overview
        };
        let text = render_ref_store(&pruned_before, "CAIRN", NOW);
        assert!(!text.contains("waiting for confirmation"), "{text}");
        assert!(text.contains("A prune is due at the next hourly pass"));
    }

    #[test]
    fn a_successful_prune_is_said_in_words_with_the_next_due_time() {
        let overview = RefStoreOverview {
            latest: Some(RefStoreSample {
                at: NOW - 60,
                packed_bytes: 540_779,
                packed_entries: 5_965,
                loose_refs: 1_200,
                ref_locks: 0,
                namespaces: vec![("jj/keep".into(), 1_783, 900)],
            }),
            last_prune: Some(PruneSample {
                started_at: NOW - 3 * HOUR,
                outcome: "ok".into(),
                duration_ms: 12 * 60 * 1000,
                removed_keep_refs: Some(192_364),
                removed_cairn_refs: Some(2_950),
                ops_abandoned: Some(146_083),
                truncated: true,
                reclaim_skipped: None,
                reclaim_failed: None,
            }),
            last_ok_prune_at: Some(NOW - 3 * HOUR),
            last_truncation_at: Some(NOW - 3 * HOUR),
            ..Default::default()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(text.contains("succeeded in 12m"), "{text}");
        assert!(text.contains("192,364 jj keep refs, 2,950 finished Cairn refs, 146,083 operations, and truncated the operation log"));
        assert!(!text.contains("never been truncated"));
        assert!(text.contains("The next prune is due in 21h"));
        assert!(text.contains("kept for 30 days"));
        assert!(!text.contains("overdue"));
        assert!(!text.contains(" Ok"), "never a bare status code");
    }

    #[test]
    fn an_overdue_and_skipping_store_says_why() {
        let overview = RefStoreOverview {
            latest: Some(sample(NOW - 60, 147_863, 143_681)),
            last_prune: Some(PruneSample {
                started_at: NOW - HOUR,
                outcome: "skipped:reconcile-in-flight".into(),
                ..Default::default()
            }),
            last_ok_prune_at: Some(NOW - 3 * 24 * HOUR),
            ..Default::default()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(
            text.contains("was skipped because a base-advance reconcile was in flight"),
            "{text}"
        );
        assert!(text.contains("**Pruning is overdue:** the last successful prune was 3d ago"));
        assert!(text.contains("A prune is due at the next hourly pass"));
    }

    /// A truncation a live workspace refused is partial maintenance, never a
    /// success: the page names the refusal and the workspaces, and says the
    /// history is not being bounded even though the ref sweeps keep succeeding.
    #[test]
    fn a_refused_truncation_is_partial_and_names_the_workspaces() {
        let refusal = "operation-log truncation refused: workspaces other than the store's own \
                       still exist and would be left recording rewritten operation ids: \
                       agent-job (/home/op/.cairn/worktrees/job); nothing was changed";
        let overview = RefStoreOverview {
            latest: Some(sample(NOW - 60, 5_965, 1_783)),
            last_prune: Some(PruneSample {
                started_at: NOW - HOUR,
                outcome: format!("partial:{refusal}"),
                duration_ms: 90_000,
                removed_keep_refs: Some(12),
                removed_cairn_refs: Some(3),
                ops_abandoned: Some(0),
                truncated: false,
                reclaim_skipped: None,
                reclaim_failed: None,
            }),
            last_ok_prune_at: Some(NOW - HOUR),
            last_truncation_at: None,
            ..Default::default()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(text.contains("only partly succeeded"), "{text}");
        assert!(text.contains("could not truncate the operation log"));
        assert!(text.contains("agent-job (/home/op/.cairn/worktrees/job)"));
        assert!(text.contains("**The operation log has never been truncated on this machine**"));
        assert!(!text.contains("and succeeded in"));

        let stale = RefStoreOverview {
            last_truncation_at: Some(NOW - 20 * 24 * HOUR),
            ..overview
        };
        let text = render_ref_store(&stale, "CAIRN", NOW);
        assert!(
            text.contains("**The operation log was last truncated 20d ago**"),
            "{text}"
        );
    }

    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    fn objects(packs: u64, kept_packs: u64, gib: f64) -> crate::jj::ObjectStoreCensus {
        crate::jj::ObjectStoreCensus {
            packs,
            kept_packs,
            pack_bytes: (gib * GIB) as u64,
            loose_objects: 1_200,
            multi_pack_index: true,
            ..Default::default()
        }
    }

    fn step(
        started_at: i64,
        outcome: &str,
        duration_ms: i64,
        before: crate::jj::ObjectStoreCensus,
        after: crate::jj::ObjectStoreCensus,
    ) -> ObjectStepSample {
        ObjectStepSample {
            started_at,
            outcome: outcome.into(),
            duration_ms,
            before: Some(before),
            after: Some(after),
        }
    }

    fn healthy_objects() -> RefStoreOverview {
        RefStoreOverview {
            latest: Some(sample(NOW - 60, 5_965, 1_783)),
            objects: Some(objects(23, 2, 3.6)),
            last_compaction: Some(step(
                NOW - 2 * HOUR,
                "ok",
                14_000,
                objects(61, 2, 3.7),
                objects(23, 2, 3.6),
            )),
            last_reclaim: Some(step(
                NOW - 4 * 24 * HOUR,
                "ok",
                26 * 60 * 1000,
                objects(40, 0, 5.1),
                objects(3, 0, 3.7),
            )),
            last_ok_prune_at: Some(NOW - 3 * HOUR),
            last_truncation_at: Some(NOW - 3 * HOUR),
            ..Default::default()
        }
    }

    #[test]
    fn a_healthy_object_store_is_said_in_words() {
        let text = render_ref_store(&healthy_objects(), "CAIRN", NOW);
        assert!(
            text.contains(
                "Object store: 23 packs (2 held by in-flight publications) in 3.6 GB, about 1,200 \
                 loose objects, with a multi-pack index."
            ),
            "{text}"
        );
        assert!(text.contains("Last compacted 2h ago (61 packs to 23, in 14s)."));
        assert!(text.contains(
            "Unreferenced objects older than 30 days were last reclaimed 4d ago (5.1 GB to 3.7 \
             GB, in 26m); the next reclaim is due in 26d."
        ));
        assert!(!text.contains("pack limit"));
        assert!(!text.contains("never been reclaimed"));
    }

    #[test]
    fn the_object_paragraph_survives_the_awaiting_confirmation_and_opted_out_states() {
        let awaiting = RefStoreOverview {
            latest: Some(sample(NOW - 600, 147_863, 143_681)),
            objects: Some(objects(23, 0, 3.6)),
            ..Default::default()
        };
        let text = render_ref_store(&awaiting, "CAIRN", NOW);
        assert!(text.contains("waiting for confirmation"), "{text}");
        assert!(text.contains("Object store: 23 packs in 3.6 GB"), "{text}");
        assert!(text.contains(
            "Unreferenced objects have never been reclaimed on this machine. The reclaim runs \
             inside the prune, so it waits on the same confirmation."
        ));
        assert!(text.contains("Not compacted on this machine yet"));

        let opted_out = RefStoreOverview {
            policy: PrunePolicy::OptedOut,
            ..awaiting
        };
        let text = render_ref_store(&opted_out, "CAIRN", NOW);
        assert!(text.contains("opted out of pruning"), "{text}");
        assert!(text.contains("Object store: 23 packs in 3.6 GB"));
        assert!(text.contains(
            "Because this store is opted out, its objects are neither compacted nor reclaimed."
        ));
        assert!(!text.contains("never been reclaimed"));
    }

    #[test]
    fn an_object_store_over_the_pack_limit_says_why_compaction_did_not_help() {
        let overview = RefStoreOverview {
            objects: Some(objects(4_802, 0, 4.2)),
            last_compaction: Some(ObjectStepSample {
                started_at: NOW - HOUR,
                outcome: "failed:git repack --geometric: fatal: out of memory".into(),
                duration_ms: 3_000,
                ..Default::default()
            }),
            ..healthy_objects()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(
            text.contains("The last compaction, 1h ago, failed: git repack --geometric"),
            "{text}"
        );
        assert!(text.contains(
            "**The object store is over git's 50-pack limit** with 4,802 packs not held by a \
             publication, and the last compaction failed: git repack --geometric: fatal: out of \
             memory."
        ));

        let skipped = RefStoreOverview {
            last_compaction: Some(ObjectStepSample {
                started_at: NOW - HOUR,
                outcome: "skipped:operator-git-busy:gc.pid (3m old)".into(),
                ..Default::default()
            }),
            ..overview
        };
        let text = render_ref_store(&skipped, "CAIRN", NOW);
        assert!(
            text.contains(
                "the last compaction was skipped because the operator's git held `gc.pid` (3m \
                 old)"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_failed_reclaim_is_a_partial_prune_and_says_why() {
        let overview = RefStoreOverview {
            last_prune: Some(PruneSample {
                started_at: NOW - HOUR,
                outcome: "partial:reclaim-failed:git repack --cruft: No space left on device"
                    .into(),
                duration_ms: 90_000,
                reclaim_failed: Some("git repack --cruft: No space left on device".into()),
                ..Default::default()
            }),
            last_ok_prune_at: Some(NOW - HOUR),
            ..healthy_objects()
        };
        let text = render_ref_store(&overview, "CAIRN", NOW);
        assert!(
            text.contains(
                "**The reclaim at the last prune failed:** git repack --cruft: No space left on \
                 device. The next prune retries it."
            ),
            "{text}"
        );
        assert!(text.contains("its reclaim of unreferenced objects failed"));
        assert!(
            !text.contains("could not truncate the operation log"),
            "a reclaim failure is not a truncation refusal"
        );
    }

    /// The section reaches the project page from what the maintenance pass
    /// recorded, not from a hand-built overview.
    #[tokio::test]
    async fn the_project_page_renders_recorded_samples() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalDb::open_migrated_for_test(temp.path().join("ref-store-page.db"))
            .await
            .unwrap();
        let search = Arc::new(SearchIndex::open_or_create(temp.path().join("search")).unwrap());
        let orch = Orchestrator::builder(
            Arc::new(DbState::new(Arc::new(local), search)),
            Arc::new(TestServicesBuilder::new().build()),
            temp.path().to_path_buf(),
        )
        .build();
        let repo = temp.path().join("repo");
        let db = &orch.db.local;
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-page', 'default', 'Page', 'page', ?1, 1, 1)",
            params![repo.to_string_lossy().into_owned()],
        )
        .await
        .unwrap();
        let now = chrono::Utc::now().timestamp();
        let store = crate::jj::project_store_dir(temp.path(), &repo);
        db.execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome, packed_bytes,
                packed_entries, loose_refs, ref_locks, namespaces_json, duration_ms)
             VALUES ('m', 'p-page', ?1, 'measure', ?2, ?2, 'ok', 2048, 12, 3, 0,
                     '{\"jj/keep\":{\"packed\":9,\"loose\":2}}', 5)",
            params![store.to_string_lossy().into_owned(), now - 120],
        )
        .await
        .unwrap();
        crate::vcs_maintenance::set_prune_confirmed(db, "p-page", &store, true)
            .await
            .unwrap();
        db.execute(
            "INSERT INTO ref_store_samples
               (id, project_id, store_path, kind, started_at, finished_at, outcome, duration_ms)
             VALUES ('p', 'p-page', ?1, 'prune', ?2, ?2, 'failed:git gc: out of disk', 9000)",
            params![store.to_string_lossy().into_owned(), now - 4 * 24 * 3600],
        )
        .await
        .unwrap();

        let page = read_project(&orch, "page").await;
        assert!(page.contains("## Repository ref store"), "{page}");
        assert!(page.contains("`packed-refs` holds 12 refs in 2 KB"));
        assert!(page.contains("| jj keep refs (`refs/jj/keep`) | 9 | 2 |"));
        assert!(page.contains("failed after 0m: git gc: out of disk"));
        assert!(page.contains("**No prune of this store has succeeded yet.**"));
    }
}
