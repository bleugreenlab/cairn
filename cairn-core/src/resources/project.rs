//! Project, project-issues, project-chat, and project-search resource readers.

use turso::params;

use super::common::{
    connect_for_read, find_query_value, lookup_project_by_key, parse_optional_bool_param,
    parse_optional_i64_param, parse_optional_usize_param, storage_error,
};

use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};
use cairn_common::query::QueryParam;
use cairn_common::uri::{
    build_issue_uri, build_project_issues_uri, build_project_terminal_uri, build_project_uri,
};

#[derive(Debug, Default)]
struct ProjectIssueStats {
    total: usize,
    open: usize,
    waiting: usize,
    merged: usize,
    closed: usize,
}

async fn load_project_issue_stats(conn: &turso::Connection, project_id: &str) -> ProjectIssueStats {
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
    conn: &turso::Connection,
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
    number: i32,
    title: String,
    status: String,
    labels: Vec<String>,
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
    ready: Option<bool>,
    label: Option<String>,
    labels: Option<Vec<String>>,
}

const PROJECT_ISSUES_SUPPORTED_PARAMS: &[&str] = &[
    "status", "limit", "offset", "sort", "ready", "label", "labels",
];
const PROJECT_ISSUES_SUPPORTED_PARAMS_TEXT: &str =
    "status, limit, offset, sort, ready, label, labels";
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
            ready: None,
            label: None,
            labels: None,
        }
    }
}

impl ProjectIssuesQuery {
    fn parse(params: &[QueryParam]) -> Result<Self, String> {
        if let Some(unsupported) = params
            .iter()
            .find(|param| !PROJECT_ISSUES_SUPPORTED_PARAMS.contains(&param.key.as_str()))
        {
            return Err(format!(
                "Unsupported query parameter '{}' for project issues. Supported parameters: {}",
                unsupported.key, PROJECT_ISSUES_SUPPORTED_PARAMS_TEXT
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
        query.ready = parse_optional_bool_param(params, "ready").map_err(|error| {
            error.replace(
                "Invalid boolean for query parameter 'ready'",
                "Invalid ready query parameter",
            )
        })?;
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
        self.statuses.is_some()
            || self.ready.is_some()
            || self.label.is_some()
            || self.labels.is_some()
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

fn ready_sql_condition(ready: bool) -> &'static str {
    if ready {
        "AND NOT EXISTS (
            SELECT 1
            FROM issue_dependencies d
            LEFT JOIN projects dep_p ON d.depends_on_uri LIKE 'cairn://p/' || dep_p.key || '/%'
            LEFT JOIN issues dep_i
                ON dep_i.project_id = dep_p.id
                AND d.depends_on_uri = 'cairn://p/' || dep_p.key || '/' || dep_i.number
            WHERE d.issue_id = i.id
              AND (dep_i.id IS NULL OR LOWER(dep_i.status) NOT IN ('closed', 'merged'))
        )"
    } else {
        "AND EXISTS (
            SELECT 1
            FROM issue_dependencies d
            LEFT JOIN projects dep_p ON d.depends_on_uri LIKE 'cairn://p/' || dep_p.key || '/%'
            LEFT JOIN issues dep_i
                ON dep_i.project_id = dep_p.id
                AND d.depends_on_uri = 'cairn://p/' || dep_p.key || '/' || dep_i.number
            WHERE d.issue_id = i.id
              AND (dep_i.id IS NULL OR LOWER(dep_i.status) NOT IN ('closed', 'merged'))
        )"
    }
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
    if let Some(ready) = query.ready {
        clause.push(' ');
        clause.push_str(ready_sql_condition(ready));
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
    conn: &turso::Connection,
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
        SELECT i.number, i.title, i.status, GROUP_CONCAT(l.name, char(31))
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
        let (number, title, status, labels) =
            (row.i64(0), row.text(1), row.text(2), row.opt_text(3));
        let (Ok(number), Ok(title), Ok(status), Ok(labels)) = (number, title, status, labels)
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
            number: number as i32,
            title,
            status,
            labels,
        });
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
            project_key.to_uppercase(),
            issue.number,
            build_issue_uri(project_key, issue.number),
            status_indicator,
            issue.title,
            labels
        ));
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
            project_key.to_uppercase(),
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

// ============================================================================
// Project Readers
// ============================================================================

/// Read project overview with stats, recent activity, and terminals
pub(super) async fn read_project(db: &LocalDb, project_key: &str) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let lookup_key = project_key.to_uppercase();
    let mut project_rows = match conn
        .query(
            "
            SELECT id, name, context, key
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

    let (project_id, project_name, project_context, canonical_key): (
        String,
        String,
        Option<String>,
        String,
    ) = match project_rows.next().await {
        Ok(Some(row)) => match (row.text(0), row.text(1), row.opt_text(2), row.text(3)) {
            (Ok(id), Ok(name), Ok(context), Ok(key)) => (id, name, context, key),
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

// ============================================================================
// Projects collection + project settings readers
// ============================================================================

pub(super) async fn read_projects(db: &LocalDb) -> String {
    let conn = match connect_for_read(db).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let mut rows = match conn
        .query(
            "SELECT key, name, hidden, remote_url FROM projects ORDER BY name ASC",
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
        let (Ok(key), Ok(name), Ok(hidden), Ok(remote)) =
            (row.text(0), row.text(1), row.i64(2), row.opt_text(3))
        else {
            continue;
        };
        any = true;
        let hidden_mark = if hidden != 0 { " (hidden)" } else { "" };
        let remote_mark = remote
            .filter(|r| !r.is_empty())
            .map(|r| format!(" — {r}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "- [{}]({}) {}{}{}\n",
            key,
            build_project_uri(&key),
            name,
            hidden_mark,
            remote_mark
        ));
    }
    if !any {
        out.push_str("No projects.\n");
    }
    out.push('\n');
    out
}

pub(super) async fn read_project_settings(orch: &Orchestrator, project_key: &str) -> String {
    let conn = match connect_for_read(&orch.db.local).await {
        Ok(conn) => conn,
        Err(error) => return error,
    };
    let lookup = project_key.to_uppercase();
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
    let config = crate::config::project_settings::load_project_settings(repo);
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
        if !populate.copy.is_empty() {
            out.push_str(&format!("- copy: {}\n", populate.copy.join(", ")));
        }
        if !populate.symlink.is_empty() {
            out.push_str(&format!("- symlink: {}\n", populate.symlink.join(", ")));
        }
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

    if let Some(unsupported) = params
        .iter()
        .find(|param| !["search", "limit", "since", "content_types"].contains(&param.key.as_str()))
    {
        return format!(
            "Unsupported query parameter '{}' for project search projection",
            unsupported.key
        );
    }

    let conn = match connect_for_read(&orch.db.local).await {
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

    let filters = crate::models::SearchFilters {
        project_id: Some(project_ctx.project_id),
        issue_id: None,
        content_types,
        since,
        limit,
    };

    match crate::search::search_content(&orch.db.local, &orch.db.search_index, query, Some(filters))
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
    use crate::storage::{LocalDb, MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    fn param(key: &str, value: &str) -> QueryParam {
        QueryParam {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[test]
    fn parses_status_limit_sort_and_ready() {
        let query = ProjectIssuesQuery::parse(&[
            param("status", "backlog,active"),
            param("limit", "10"),
            param("sort", "updated_desc"),
            param("ready", "1"),
        ])
        .unwrap();

        assert_eq!(
            query.statuses,
            Some(vec!["backlog".to_string(), "active".to_string()])
        );
        assert_eq!(query.limit, 10);
        assert_eq!(query.sort_field, ProjectIssuesSortField::Updated);
        assert_eq!(query.sort_dir, ProjectIssuesSortDir::Desc);
        assert_eq!(query.ready, Some(true));
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
        assert!(ProjectIssuesQuery::parse(&[param("ready", "maybe")])
            .unwrap_err()
            .contains("Invalid ready query parameter"));
        assert!(ProjectIssuesQuery::parse(&[param("status", "open")])
            .unwrap_err()
            .contains("Invalid status query parameter"));
    }

    #[test]
    fn rejects_unknown_params_with_supported_list() {
        let error = ProjectIssuesQuery::parse(&[param("foo", "bar")]).unwrap_err();
        assert!(error.contains("Unsupported query parameter 'foo' for project issues"));
        assert!(error
            .contains("Supported parameters: status, limit, offset, sort, ready, label, labels"));
    }

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("project-labels.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_project(conn: &turso::Connection) {
        conn.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at) VALUES ('p-labels', 'default', 'Labels', 'LBL', '/tmp/lbl', 1, 1)",
            (),
        )
        .await
        .unwrap();
    }

    async fn seed_issue(conn: &turso::Connection, issue_id: &str, number: i32, title: &str) {
        conn.execute(
            "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at) VALUES (?1, 'p-labels', ?2, ?3, '', 'backlog', 'backlog', 'none', 0, ?2, ?2)",
            params![issue_id, number, title],
        )
        .await
        .unwrap();
    }

    async fn seed_label(conn: &turso::Connection, name: &str) {
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
