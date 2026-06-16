//! Shared support for URI resource readers.

use turso::params;

use crate::mcp::types::McpCallbackRequest;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::contract::{contract_for, MutationSpec, ResourceContract, ResourceKind};
use cairn_common::query::QueryParam;

#[derive(Debug)]
pub(super) struct ProjectContext {
    pub(super) project_id: String,
    pub(super) project_key: String,
}

#[derive(Debug, Clone)]
pub(super) struct ResourceJob {
    pub(super) id: String,
    pub(super) parent_job_id: Option<String>,
    pub(super) status: String,
    pub(super) completed_at: Option<i32>,
    pub(super) started_at: Option<i32>,
    pub(super) uri_segment: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct ResourceArtifact {
    pub(super) data: String,
    pub(super) output_name: Option<String>,
    pub(super) artifact_type: String,
}

impl ResourceArtifact {
    /// The canonical schema-named URI segment for this artifact: its resolved
    /// output name (e.g. "create-pr", "plan"), falling back to the artifact
    /// type when the output name is empty/absent. Both are written identically
    /// at store time and `artifact_type` is NOT NULL, so this is essentially
    /// always `Some` — the generic `/artifact` alias is never surfaced for a
    /// stored artifact whose schema name is known (CAIRN-1219).
    pub(super) fn schema_name(&self) -> Option<&str> {
        self.output_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .or(Some(self.artifact_type.as_str()))
            .filter(|name| !name.is_empty())
    }
}

/// An action_run resolved by node segment — the action-node analogue of
/// `ResourceJob`. A `pr` action node has no job; it is addressed by its stored
/// `uri_segment` and its id is the owner key (`merge_requests.job_id`) the
/// owner-generic PR machinery already uses (CAIRN-1220).
#[derive(Debug, Clone)]
pub(super) struct ResourceActionRun {
    pub(super) id: String,
    pub(super) status: String,
    #[allow(dead_code)]
    pub(super) uri_segment: Option<String>,
    pub(super) created_at: i64,
    pub(super) started_at: Option<i64>,
    pub(super) completed_at: Option<i64>,
}

pub(super) const JOB_COLUMNS: &str = "
    id, parent_job_id, status, completed_at, started_at, uri_segment
";

const ARTIFACT_COLUMNS: &str = "data, output_name, artifact_type";

pub(super) fn storage_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

pub(super) async fn resolve_home_relative_resource_uri(
    db: &LocalDb,
    request: &McpCallbackRequest,
    uri: &str,
) -> Result<String, String> {
    let Some(suffix) = uri
        .strip_prefix("cairn:~/")
        .or_else(|| (uri == "cairn:~").then_some(""))
    else {
        return Ok(uri.to_string());
    };

    let home_uri = crate::mcp::handlers::run_context::lookup_home_uri(db, request).await?;
    if suffix.is_empty() {
        Ok(home_uri)
    } else {
        Ok(format!("{}/{}", home_uri.trim_end_matches('/'), suffix))
    }
}

pub(super) async fn connect_for_read(db: &LocalDb) -> Result<turso::Connection, String> {
    let conn = db
        .connect()
        .await
        .map_err(|error| storage_error("Database error", error))?;
    conn.execute("BEGIN CONCURRENT", ())
        .await
        .map_err(|error| storage_error("Database error", error.into()))?;
    Ok(conn)
}

pub(super) fn resource_job_from_row(row: &turso::Row) -> DbResult<ResourceJob> {
    Ok(ResourceJob {
        id: row.text(0)?,
        parent_job_id: row.opt_text(1)?,
        status: row.text(2)?,
        completed_at: row.opt_i64(3)?.map(|value| value as i32),
        started_at: row.opt_i64(4)?.map(|value| value as i32),
        uri_segment: row.opt_text(5)?,
    })
}

fn resource_artifact_from_row(row: &turso::Row) -> DbResult<ResourceArtifact> {
    Ok(ResourceArtifact {
        data: row.text(0)?,
        output_name: row.opt_text(1)?,
        artifact_type: row.text(2)?,
    })
}

pub(super) async fn lookup_project_by_key(
    conn: &turso::Connection,
    project_key: &str,
) -> Result<ProjectContext, String> {
    let key = project_key.to_uppercase();
    let mut rows = conn
        .query(
            "SELECT id, key FROM projects WHERE key = ?1 LIMIT 1",
            (key.as_str(),),
        )
        .await
        .map_err(|error| storage_error("Failed to load project", error.into()))?;

    rows.next()
        .await
        .map_err(|error| storage_error("Failed to load project", error.into()))?
        .map(|row| {
            Ok::<_, DbError>(ProjectContext {
                project_id: row.text(0)?,
                project_key: row.text(1)?,
            })
        })
        .transpose()
        .map_err(|error| storage_error("Failed to decode project", error))?
        .ok_or_else(|| format!("No project found with key '{}'", key))
}

async fn issue_id_for_number(
    conn: &turso::Connection,
    project_id: &str,
    number: i32,
) -> Option<String> {
    let mut rows = conn
        .query(
            "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2 LIMIT 1",
            params![project_id, number as i64],
        )
        .await
        .ok()?;

    rows.next().await.ok().flatten()?.text(0).ok()
}

pub(super) async fn resolve_issue_id(
    conn: &turso::Connection,
    project_key: &str,
    number: i32,
) -> Result<(ProjectContext, String), String> {
    let project_ctx = lookup_project_by_key(conn, project_key).await?;
    let issue_id = issue_id_for_number(conn, &project_ctx.project_id, number)
        .await
        .ok_or_else(|| format!("Issue {}-{} not found", project_key, number))?;
    Ok((project_ctx, issue_id))
}

pub(super) async fn visible_job_node_segment(
    conn: &turso::Connection,
    job: &ResourceJob,
) -> String {
    if let Some(segment) = job
        .uri_segment
        .as_deref()
        .filter(|segment| !segment.is_empty())
    {
        return segment.to_string();
    }

    let _ = conn;
    job.id.clone()
}

/// Render a resource kind's affordance block from the contract table: related
/// links, read-query projections as filters, and supported mutations as actions.
///
/// Everything is rendered from the contract `uri_template` placeholders
/// (`cairn://p/{project}/{number}/messages`), not concrete instance URIs. This
/// reads as "how to act on any resource of this kind" and, crucially, makes the
/// block byte-identical across every instance of a kind so the batch assembler's
/// `(kind, block)` dedupe collapses same-kind instances to a single block.
///
/// The block is titled with the resource's contract `name` (`## Issue messages`)
/// so that when several distinct-kind affordances concatenate at the tail of a
/// batch, each `links`/`filters`/`actions` group is unambiguously attributed to
/// the resource it affords.
/// The single universal-grep filter note appended to every resource's affordance
/// filters section: grep is a view projection over the rendered body, not a
/// per-resource feature. Documents the modifiers, the line-number-prefixed match
/// contract, and the tree-only limits in one place (resolving the prior
/// `-A`/`-B`/`-C` documentation gap).
const UNIVERSAL_GREP_FILTER: &str = "- `grep=REGEX` (universal) · `-i` · `-A`/`-B`/`-C`/`context=N` · `head_limit=N` — line-number-prefixed matches over the rendered body; `offset` not allowed with grep; `files_with_matches`/`count` need a tree\n";

pub(super) fn affordance_for_kind(kind: ResourceKind) -> String {
    let Some(contract) = contract_for(kind) else {
        return String::new();
    };

    let mut sections = String::new();

    if !contract.related.is_empty() {
        sections.push_str("### links\n");
        for spec in contract.related {
            let target = contract_for(spec.kind)
                .map(|related| related.uri_template)
                .unwrap_or("");
            sections.push_str(&format!("- [{}]({})\n", spec.label, target));
        }
        sections.push('\n');
    }

    // grep is a universal read filter over any rendered body, so every resource
    // advertises it once here alongside its own pushdown projections (if any).
    // The note dedupes per `(kind, block)` like the rest of the affordance.
    sections.push_str("### filters\n");
    for proj in contract.read_projections {
        sections.push_str(&format!("- `{}={}`\n", proj.key, proj.values));
    }
    sections.push_str(UNIVERSAL_GREP_FILTER);
    sections.push('\n');

    let mut action_lines: Vec<String> = Vec::new();
    push_mutation_actions(&mut action_lines, contract);
    for spec in contract.related {
        if spec.actions {
            if let Some(related) = contract_for(spec.kind) {
                push_mutation_actions(&mut action_lines, related);
            }
        }
    }
    // Cross-resource actions: mutations owned by another resource that take this
    // one as input. Rendered from the target's `uri_template` + example, but
    // labeled from this resource's perspective (e.g. a recipe "starts an
    // execution" via the executions resource). Restores a workflow hint the
    // contract-derived affordances would otherwise drop.
    for cross in contract.cross_actions {
        if let Some(target) = contract_for(cross.kind) {
            if let Some(spec) = target.mutation(cross.mode) {
                action_lines.push(format!(
                    "- [{}]({}): {}",
                    cross.label,
                    target.uri_template,
                    action_summary(spec)
                ));
            }
        }
    }

    if !action_lines.is_empty() {
        sections.push_str("### actions\n");
        for line in action_lines {
            sections.push_str(&line);
            sections.push('\n');
        }
        sections.push('\n');
    }

    if sections.is_empty() {
        return String::new();
    }

    format!("## {}\n\n{}", contract.name, sections)
}

fn push_mutation_actions(action_lines: &mut Vec<String>, contract: &ResourceContract) {
    for spec in contract.mutations {
        action_lines.push(format!(
            "- [{}]({}): {}",
            spec.label,
            contract.uri_template,
            action_summary(spec)
        ));
    }
}

/// One-line payload guidance for an action: required/optional keys + example.
fn action_summary(spec: &MutationSpec) -> String {
    fn keys(specs: &[cairn_common::contract::KeySpec]) -> String {
        specs
            .iter()
            .map(|k| format!("`{}`", k.display()))
            .collect::<Vec<_>>()
            .join(", ")
    }
    let mut parts: Vec<String> = Vec::new();
    if !spec.required.is_empty() {
        parts.push(format!("required {}", keys(spec.required)));
    }
    if !spec.optional.is_empty() {
        parts.push(format!("optional {}", keys(spec.optional)));
    }
    let head = if parts.is_empty() {
        "no payload".to_string()
    } else {
        parts.join("; ")
    };
    format!("{}. e.g. {}", head, spec.example)
}

/// Get todo progress string like "3/5 todos"
pub(super) async fn get_todo_progress(conn: &turso::Connection, job_id: &str) -> Option<String> {
    let mut rows = conn
        .query("SELECT status FROM todos WHERE job_id = ?1", (job_id,))
        .await
        .ok()?;

    let mut total = 0usize;
    let mut completed = 0usize;
    while let Ok(Some(row)) = rows.next().await {
        let status = row.text(0).ok()?;
        total += 1;
        if status == "completed" {
            completed += 1;
        }
    }

    (total > 0).then(|| format!("{completed}/{total} todos"))
}

pub(super) async fn connect_and_find_node_job(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> Result<(turso::Connection, ResourceJob), String> {
    let conn = connect_for_read(db).await?;
    let (_, issue_id) = resolve_issue_id(&conn, project_key, number).await?;
    let job = find_job_by_node_name(&conn, &issue_id, node_name, exec_seq)
        .await
        .ok_or_else(|| {
            format!(
                "Node '{}' not found for issue {}-{}",
                node_name, project_key, number
            )
        })?;
    Ok((conn, job))
}

pub(super) async fn connect_and_find_task_job(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> Result<(turso::Connection, ResourceJob, ResourceJob), String> {
    let (conn, parent_job) =
        connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await?;
    let task_job = find_task_by_name(&conn, &parent_job.id, task_name).await?;
    Ok((conn, parent_job, task_job))
}

/// Check if a job or any of its child jobs have an artifact
pub(super) async fn has_artifact_for_job(conn: &turso::Connection, job_id: &str) -> bool {
    // Check the job itself
    let direct = match conn
        .query(
            "SELECT id FROM artifacts WHERE job_id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
    {
        Ok(mut rows) => matches!(rows.next().await, Ok(Some(_))),
        Err(_) => false,
    };
    if direct {
        return true;
    }

    match conn
        .query(
            "
            SELECT a.id
            FROM artifacts a
            JOIN jobs j ON j.id = a.job_id
            WHERE j.parent_job_id = ?1
            LIMIT 1
            ",
            (job_id,),
        )
        .await
    {
        Ok(mut rows) => matches!(rows.next().await, Ok(Some(_))),
        Err(_) => false,
    }
}

/// Check if a job has any terminals
pub(super) async fn has_terminal_for_job(conn: &turso::Connection, job_id: &str) -> bool {
    match conn
        .query(
            "SELECT id FROM job_terminals WHERE job_id = ?1 LIMIT 1",
            (job_id,),
        )
        .await
    {
        Ok(mut rows) => matches!(rows.next().await, Ok(Some(_))),
        Err(_) => false,
    }
}

pub(super) async fn get_direct_artifact_for_job(
    conn: &turso::Connection,
    job_id: &str,
) -> Option<ResourceArtifact> {
    let sql = format!(
        "
        SELECT {ARTIFACT_COLUMNS}
        FROM artifacts
        WHERE job_id = ?1
        ORDER BY version DESC
        LIMIT 1
        "
    );
    let mut rows = conn.query(sql.as_str(), (job_id,)).await.ok()?;
    rows.next()
        .await
        .ok()
        .flatten()
        .and_then(|row| resource_artifact_from_row(&row).ok())
}

/// Get artifact for a job (checking job itself and child jobs)
pub(super) async fn get_artifact_for_job(
    conn: &turso::Connection,
    job_id: &str,
) -> Option<ResourceArtifact> {
    if let Some(artifact) = get_direct_artifact_for_job(conn, job_id).await {
        return Some(artifact);
    }

    let sql = format!(
        "
        SELECT {ARTIFACT_COLUMNS}
        FROM artifacts
        WHERE job_id IN (SELECT id FROM jobs WHERE parent_job_id = ?1)
        ORDER BY version DESC
        LIMIT 1
        "
    );
    let mut rows = conn.query(sql.as_str(), (job_id,)).await.ok()?;
    rows.next()
        .await
        .ok()
        .flatten()
        .and_then(|row| resource_artifact_from_row(&row).ok())
}

pub(super) async fn find_task_by_name(
    conn: &turso::Connection,
    parent_job_id: &str,
    task_name: &str,
) -> Result<ResourceJob, String> {
    let sql = format!(
        "
        SELECT {JOB_COLUMNS}
        FROM jobs
        WHERE parent_job_id = ?1
          AND uri_segment = ?2
        LIMIT 1
        "
    );
    let mut rows = conn
        .query(sql.as_str(), params![parent_job_id, task_name])
        .await
        .map_err(|error| storage_error("Failed to load task", error.into()))?;
    if let Some(row) = rows
        .next()
        .await
        .map_err(|error| storage_error("Failed to load task", error.into()))?
    {
        return resource_job_from_row(&row)
            .map_err(|error| storage_error("Failed to decode task", error));
    }

    let _ = conn;
    Err(format!("Task '{}' not found", task_name))
}

/// Get the execution_id for a given exec_seq (1-based index stored in seq column)
async fn get_execution_id_for_seq(
    conn: &turso::Connection,
    issue_id: &str,
    exec_seq: i32,
) -> Option<String> {
    let mut rows = conn
        .query(
            "
            SELECT id
            FROM executions
            WHERE issue_id = ?1 AND seq = ?2
            LIMIT 1
            ",
            params![issue_id, exec_seq as i64],
        )
        .await
        .ok()?;

    rows.next().await.ok().flatten()?.text(0).ok()
}

pub(super) async fn find_job_by_node_name(
    conn: &turso::Connection,
    issue_id: &str,
    node_name: &str,
    exec_seq: i32,
) -> Option<ResourceJob> {
    let exec_id = get_execution_id_for_seq(conn, issue_id, exec_seq).await?;
    let sql = format!(
        "
        SELECT {JOB_COLUMNS}
        FROM jobs
        WHERE issue_id = ?1
          AND execution_id = ?2
          AND parent_job_id IS NULL
          AND uri_segment = ?3
        LIMIT 1
        "
    );
    if let Ok(mut rows) = conn
        .query(sql.as_str(), params![issue_id, exec_id.as_str(), node_name])
        .await
    {
        if let Ok(Some(row)) = rows.next().await {
            if let Ok(job) = resource_job_from_row(&row) {
                return Some(job);
            }
        }
    }

    let _ = issue_id;
    None
}

/// Resolve a top-level action_run by its stored `uri_segment` within an
/// execution — the action-node analogue of `find_job_by_node_name`. Keys on the
/// same `(executions.seq, uri_segment)` pair via the shared
/// `get_execution_id_for_seq`, so action nodes resolve through the exact key the
/// node-tree emits and `blocked_node_artifact_uri` uses (CAIRN-1222).
pub(super) async fn find_action_run_by_node_name(
    conn: &turso::Connection,
    issue_id: &str,
    node_name: &str,
    exec_seq: i32,
) -> Option<ResourceActionRun> {
    let exec_id = get_execution_id_for_seq(conn, issue_id, exec_seq).await?;
    let mut rows = conn
        .query(
            "
            SELECT id, status, uri_segment, created_at, started_at, completed_at
            FROM action_runs
            WHERE execution_id = ?1
              AND uri_segment = ?2
            LIMIT 1
            ",
            params![exec_id.as_str(), node_name],
        )
        .await
        .ok()?;
    let row = rows.next().await.ok().flatten()?;
    Some(ResourceActionRun {
        id: row.text(0).ok()?,
        status: row.text(1).ok()?,
        uri_segment: row.opt_text(2).ok()?,
        created_at: row.i64(3).ok()?,
        started_at: row.opt_i64(4).ok()?,
        completed_at: row.opt_i64(5).ok()?,
    })
}

/// Resolve a node segment to its owner id: a job id when an agent node matches,
/// else an action_run id when an action node matches, else a "node not found"
/// error. The returned id is the owner key threaded into the owner-generic PR
/// machinery (`merge_requests.job_id`) and the todos/artifact paths.
pub(crate) async fn resolve_node_owner_id(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> Result<String, String> {
    let conn = connect_for_read(db).await?;
    let (_, issue_id) = resolve_issue_id(&conn, project_key, number).await?;
    if let Some(job) = find_job_by_node_name(&conn, &issue_id, node_name, exec_seq).await {
        return Ok(job.id);
    }
    if let Some(action_run) =
        find_action_run_by_node_name(&conn, &issue_id, node_name, exec_seq).await
    {
        return Ok(action_run.id);
    }
    Err(format!(
        "Node '{}' not found for issue {}-{}",
        node_name, project_key, number
    ))
}

pub(super) fn find_query_value<'a>(params: &'a [QueryParam], key: &str) -> Option<&'a str> {
    params
        .iter()
        .rev()
        .find(|param| param.key == key)
        .map(|param| param.value.as_str())
}

pub(super) fn reject_query_params(resource_name: &str, params: &[QueryParam]) -> Option<String> {
    (!params.is_empty()).then(|| {
        format!(
            "Query parameters are not supported on {} resources: {}",
            resource_name,
            params
                .iter()
                .map(|param| param.key.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

pub(super) fn parse_optional_i64_param(
    params: &[QueryParam],
    key: &str,
) -> Result<Option<i64>, String> {
    find_query_value(params, key)
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| format!("Invalid integer for query parameter '{key}': {value}"))
        })
        .transpose()
}

pub(super) fn parse_optional_usize_param(
    params: &[QueryParam],
    key: &str,
) -> Result<Option<usize>, String> {
    find_query_value(params, key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("Invalid integer for query parameter '{key}': {value}"))
        })
        .transpose()
}

pub(super) fn parse_optional_bool_param(
    params: &[QueryParam],
    key: &str,
) -> Result<Option<bool>, String> {
    find_query_value(params, key)
        .map(|value| match value {
            "" | "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => Err(format!(
                "Invalid boolean for query parameter '{key}': {value}"
            )),
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::types::McpCallbackRequest;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    async fn exec(db: &LocalDb, sql: &'static str) {
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(sql, ()).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    async fn test_db(name: &str) -> LocalDb {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(name);
        std::mem::forget(temp);
        let db = LocalDb::open(path).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    async fn seed_two_node_runs(db: &LocalDb) {
        exec(
            db,
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('proj-1', 'default', 'Test', 'BCMD', '/tmp/repo', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO issues(id, project_id, number, title, status, created_at, updated_at)
             VALUES ('issue-1', 'proj-1', 2, 'T', 'active', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('exec-1', 'recipe', 'issue-1', 'proj-1', 'running', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-planner', 'exec-1', 'issue-1', 'proj-1', 'Planner', 'running', 1, 1, 'planner', '/tmp/repo-planner')",
        )
        .await;
        exec(
            db,
            "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path)
             VALUES ('job-builder', 'exec-1', 'issue-1', 'proj-1', 'Builder', 'running', 1, 1, 'builder', '/tmp/repo-builder')",
        )
        .await;
        exec(
            db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-planner', 'job-planner', 'issue-1', 1, 1)",
        )
        .await;
        exec(
            db,
            "INSERT INTO runs(id, job_id, issue_id, created_at, updated_at)
             VALUES ('run-builder', 'job-builder', 'issue-1', 1, 1)",
        )
        .await;
    }

    fn request(run_id: &str) -> McpCallbackRequest {
        McpCallbackRequest {
            cwd: String::new(),
            run_id: Some(run_id.to_string()),
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        }
    }

    #[test]
    fn affordance_for_issue_kind_uses_templated_links_and_actions() {
        let output = affordance_for_kind(ResourceKind::Issue);

        // The block is titled with the resource's contract name so a batch of
        // mixed-kind affordances stays attributable.
        assert!(output.starts_with("## Issue details\n"));
        // Links and actions render from the contract `uri_template`
        // placeholders, not a concrete issue number, so the block is
        // byte-identical across every issue instance and dedupes in a batch.
        assert!(output.contains("- [messages](cairn://p/{project}/{number}/messages)"));
        assert!(output.contains("- [changed](cairn://p/{project}/{number}/changed)"));
        assert!(output.contains("- [append comment](cairn://p/{project}/{number}):"));
        assert!(output.contains("- [append message](cairn://p/{project}/{number}/messages):"));
    }

    #[test]
    fn affordance_for_recipe_kind_carries_start_execution_cross_action() {
        let output = affordance_for_kind(ResourceKind::Recipe);

        assert!(output.starts_with("## Recipe\n"));
        // The recipe owns edit/delete on itself...
        assert!(output.contains(
            "- [edit recipe (full content replace or targeted text replacement)](cairn://recipes/{recipe_id}):"
        ));
        // ...and surfaces the start-execution mutation that lives on the
        // executions resource but takes this recipe as input, rendered from the
        // executions `uri_template` and example.
        assert!(output.contains(
            "- [start an execution with this recipe](cairn://p/{project}/{number}/executions):"
        ));
        // The rendered action carries the executions resource's own example,
        // not the recipe's, so the agent sees exactly how to start the run.
        assert!(output.contains("mode:\"append\""));
        assert!(
            output.contains("cairn://p/PROJECT/NUMBER/executions"),
            "cross-action must inline the executions example"
        );
    }

    #[test]
    fn affordance_for_project_issues_kind_uses_templated_parent_link() {
        let output = affordance_for_kind(ResourceKind::ProjectIssues);

        assert!(output.starts_with("## Project issues\n"));
        assert!(output.contains("- [up](cairn://p/{project})"));
        assert!(output.contains("`status=backlog,active`"));
        assert!(output.contains("- [create issue](cairn://p/{project}/issues):"));
    }
}
