//! Shared support for URI resource readers.

use turso::params;

use crate::mcp::types::McpCallbackRequest;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};
use cairn_common::contract::{
    contract_for, ChangeMode, KeyType, MutationSpec, ResourceContract, ResourceKind,
};
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
/// `uri_segment`, while persisted PR ownership lives on the producing job and is
/// reached from the action run through `parent_job_id` (CAIRN-1220).
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
    dbs: &crate::db::DbState,
    request: &McpCallbackRequest,
    uri: &str,
) -> Result<String, String> {
    let Some(suffix) = uri
        .strip_prefix("cairn:~/")
        .or_else(|| (uri == "cairn:~").then_some(""))
    else {
        return Ok(uri.to_string());
    };

    // Route the run lookup across every open database so a `cairn:~/` target
    // resolves to the home URI in whichever DB the run lives (CAIRN-2132).
    let home_uri = crate::mcp::handlers::run_context::lookup_home_uri_routed(dbs, request).await?;
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
    // Backend-aware: a synced (non-MVCC) replica cannot run `BEGIN CONCURRENT`,
    // so route through the one source of truth for the begin statement on this
    // handle (plain `BEGIN` on synced, `BEGIN CONCURRENT` on local).
    conn.execute(db.concurrent_begin(), ())
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

    push_links_section(&mut sections, contract);
    push_filters_section(&mut sections, contract);

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

fn push_links_section(sections: &mut String, contract: &ResourceContract) {
    if contract.related.is_empty() {
        return;
    }
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
// advertises it once here alongside its own pushdown projections (if any). The
// note dedupes per `(kind, block)` like the rest of the affordance.
fn push_filters_section(sections: &mut String, contract: &ResourceContract) {
    sections.push_str("### filters\n");
    for proj in contract.read_projections {
        sections.push_str(&format!("- `{}={}`\n", proj.key, proj.values));
    }
    sections.push_str(UNIVERSAL_GREP_FILTER);
    sections.push('\n');
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

/// Render a node/task artifact's affordance block, deriving the `create`
/// example's payload keys from the artifact's resolved JSON Schema. The static
/// contract example uses generic placeholder keys (`{title, content}`) that don't
/// match a custom artifact's real schema, so copying it bounces (CAIRN #170).
/// Returns `None` when the schema has no usable top-level `properties`, leaving
/// the caller to fall back to the contract-derived `affordance_for_kind` block.
pub(super) fn artifact_affordance_with_schema(
    kind: ResourceKind,
    addressed_name: Option<&str>,
    schema: &serde_json::Value,
) -> Option<String> {
    let contract = contract_for(kind)?;
    let props = schema.get("properties").and_then(|p| p.as_object())?;
    if props.is_empty() {
        return None;
    }
    let required: Vec<&str> = schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    // Required keys first (declared order), then the remaining properties — so a
    // copied example leads with what the schema demands.
    let mut ordered: Vec<&str> = Vec::new();
    for key in &required {
        if props.contains_key(*key) && !ordered.contains(key) {
            ordered.push(key);
        }
    }
    for key in props.keys() {
        if !ordered.contains(&key.as_str()) {
            ordered.push(key.as_str());
        }
    }

    let key_display = |name: &str| -> String {
        let ty = props
            .get(name)
            .and_then(|p| p.get("type"))
            .and_then(|t| t.as_str())
            .map(schema_type_label)
            .unwrap_or(KeyType::Str.as_str());
        format!("`{name}({ty})`")
    };
    let required_display: Vec<String> = ordered
        .iter()
        .filter(|k| required.contains(*k))
        .map(|k| key_display(k))
        .collect();
    let optional_display: Vec<String> = ordered
        .iter()
        .filter(|k| !required.contains(*k))
        .map(|k| key_display(k))
        .collect();

    let example_uri = match addressed_name {
        Some(name) => format!("cairn:~/{name}"),
        None => "cairn:~/<name>".to_string(),
    };
    let payload = schema_example_payload(props, &ordered);
    let create_example = format!(
        "write({{changes:[{{target:\"{example_uri}\",mode:\"create\",payload:{payload}}}]}})"
    );

    let mut head_parts: Vec<String> = Vec::new();
    if !required_display.is_empty() {
        head_parts.push(format!("required {}", required_display.join(", ")));
    }
    if !optional_display.is_empty() {
        head_parts.push(format!("optional {}", optional_display.join(", ")));
    }
    let head = if head_parts.is_empty() {
        "no payload".to_string()
    } else {
        head_parts.join("; ")
    };

    let mut sections = String::new();
    push_links_section(&mut sections, contract);
    push_filters_section(&mut sections, contract);
    sections.push_str("### actions\n");
    let create_label = contract
        .mutation(ChangeMode::Create)
        .map(|spec| spec.label)
        .unwrap_or("write artifact");
    sections.push_str(&format!(
        "- [{create_label}]({example_uri}): {head}. e.g. {create_example}\n"
    ));
    // The patch action is schema-agnostic (field merge / text replacement /
    // confirm / PR ops), so its contract example stands as written.
    if let Some(patch) = contract.mutation(ChangeMode::Patch) {
        sections.push_str(&format!(
            "- [{}]({}): {}\n",
            patch.label,
            example_uri,
            action_summary(patch)
        ));
    }
    sections.push('\n');

    Some(format!("## {}\n\n{}", contract.name, sections))
}

/// Map a JSON Schema `type` to the `KeyType` label used in affordance key specs.
fn schema_type_label(json_type: &str) -> &'static str {
    match json_type {
        "string" => KeyType::Str.as_str(),
        "boolean" => KeyType::Bool.as_str(),
        "number" | "integer" => KeyType::Int.as_str(),
        "array" => KeyType::Array.as_str(),
        "object" => KeyType::Object.as_str(),
        _ => KeyType::Str.as_str(),
    }
}

/// A type-appropriate placeholder value for a schema property in an example
/// payload.
fn schema_placeholder(json_type: Option<&str>) -> &'static str {
    match json_type {
        Some("number") | Some("integer") => "0",
        Some("boolean") => "true",
        Some("array") => "[...]",
        Some("object") => "{...}",
        _ => "\"...\"",
    }
}

/// Build a `{key:placeholder,...}` example payload from a schema's top-level
/// properties, in the supplied key order.
fn schema_example_payload(
    props: &serde_json::Map<String, serde_json::Value>,
    ordered: &[&str],
) -> String {
    let pairs: Vec<String> = ordered
        .iter()
        .map(|name| {
            let ty = props
                .get(*name)
                .and_then(|p| p.get("type"))
                .and_then(|t| t.as_str());
            format!("{name}:{}", schema_placeholder(ty))
        })
        .collect();
    format!("{{{}}}", pairs.join(","))
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
    // Most recently written name wins. `version` is per-name, so ordering by it
    // would compare versions across unrelated output names; order by write
    // recency (`created_at`, then `rowid` as the insertion-order tiebreaker).
    let sql = format!(
        "
        SELECT {ARTIFACT_COLUMNS}
        FROM artifacts
        WHERE job_id = ?1
        ORDER BY created_at DESC, rowid DESC
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
        ORDER BY created_at DESC, rowid DESC
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

/// Get the latest version of a specifically-named artifact for a job (checking
/// the job itself, then its child jobs). Unlike [`get_artifact_for_job`], this
/// filters by `output_name`, so a named read (`.../{node}/plan`) returns that
/// name's own version chain rather than whatever name carries the highest
/// version across the job.
pub(super) async fn get_named_artifact_for_job(
    conn: &turso::Connection,
    job_id: &str,
    output_name: &str,
) -> Option<ResourceArtifact> {
    let direct = format!(
        "
        SELECT {ARTIFACT_COLUMNS}
        FROM artifacts
        WHERE job_id = ?1 AND output_name = ?2
        ORDER BY version DESC
        LIMIT 1
        "
    );
    let mut rows = conn
        .query(direct.as_str(), params![job_id, output_name])
        .await
        .ok()?;
    if let Some(row) = rows.next().await.ok().flatten() {
        return resource_artifact_from_row(&row).ok();
    }

    let child = format!(
        "
        SELECT {ARTIFACT_COLUMNS}
        FROM artifacts
        WHERE job_id IN (SELECT id FROM jobs WHERE parent_job_id = ?1)
          AND output_name = ?2
        ORDER BY version DESC
        LIMIT 1
        "
    );
    let mut rows = conn
        .query(child.as_str(), params![job_id, output_name])
        .await
        .ok()?;
    rows.next()
        .await
        .ok()
        .flatten()
        .and_then(|row| resource_artifact_from_row(&row).ok())
}

/// List the latest version of every distinct named artifact a job has produced,
/// ordered by name. Each `output_name` chain contributes exactly one row (its
/// highest version). A single-artifact node yields exactly one entry — the same
/// artifact [`get_artifact_for_job`] would surface.
pub(super) async fn list_named_artifacts_for_job(
    conn: &turso::Connection,
    job_id: &str,
) -> Vec<ResourceArtifact> {
    let sql = format!(
        "
        SELECT {ARTIFACT_COLUMNS}
        FROM (
            SELECT {ARTIFACT_COLUMNS},
                   ROW_NUMBER() OVER (
                       PARTITION BY output_name
                       ORDER BY version DESC
                   ) AS name_rank
            FROM artifacts
            WHERE job_id = ?1
        ) ranked
        WHERE name_rank = 1
        ORDER BY output_name
        "
    );
    let mut artifacts = Vec::new();
    if let Ok(mut rows) = conn.query(sql.as_str(), (job_id,)).await {
        while let Ok(Some(row)) = rows.next().await {
            if let Ok(artifact) = resource_artifact_from_row(&row) {
                artifacts.push(artifact);
            }
        }
    }
    artifacts
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
/// error. For PR actions this id is a lookup handle; the durable PR owner is the
/// producing job stored in `merge_requests.job_id`, resolved through
/// `action_runs.parent_job_id`.
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
    fn artifact_affordance_uses_schema_keys_not_generic_example() {
        // The coordinator board's custom schema: required `title`, plus `scratch`
        // and `action_items`. The generic contract example (`{title, content}`)
        // documents a `content` key the board never declares, so copying it
        // bounced (CAIRN #170). The schema-aware block must instead lead with the
        // board's own keys.
        let schema = serde_json::json!({
            "type": "object",
            "required": ["title"],
            "properties": {
                "title": { "type": "string" },
                "scratch": { "type": "string" },
                "action_items": { "type": "array" }
            }
        });
        let block =
            artifact_affordance_with_schema(ResourceKind::NodeArtifact, Some("board"), &schema)
                .expect("a schema with properties yields a block");

        // The example addresses the artifact by its real name and lists the
        // required key in the head.
        assert!(block.contains("target:\"cairn:~/board\""));
        assert!(block.contains("required `title(str)`"));

        // Scope the key checks to the `create` example's payload (the `patch`
        // example legitimately mentions operation keys like `content`).
        let payload = block
            .split_once("mode:\"create\",payload:{")
            .and_then(|(_, rest)| rest.split_once('}'))
            .map(|(inner, _)| inner)
            .expect("create example must contain a payload object");
        assert!(payload.contains("title:"));
        assert!(
            !payload.contains("content:"),
            "schema-aware create example must not document undeclared keys: {payload}"
        );
        let declared = ["title", "scratch", "action_items"];
        for field in payload.split(',') {
            let key = field.split(':').next().unwrap_or("").trim();
            assert!(
                declared.contains(&key),
                "create example key `{key}` is not a schema property: {payload}"
            );
        }
    }

    #[test]
    fn artifact_affordance_falls_back_without_properties() {
        // A schema with no usable `properties` yields `None`, so the read path
        // falls back to the static contract affordance.
        let schema = serde_json::json!({ "type": "object" });
        assert!(artifact_affordance_with_schema(
            ResourceKind::NodeArtifact,
            Some("board"),
            &schema
        )
        .is_none());
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
