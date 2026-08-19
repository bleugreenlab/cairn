//! Shared support for URI resource readers.

use cairn_db::turso::params;

use crate::mcp::types::McpCallbackRequest;
use crate::storage::{DbError, DbResult, LocalDb, RowExt, TrackedConnection};
use cairn_common::contract::{
    contract_for, ChangeMode, KeyType, MutationSpec, ResourceContract, ResourceKind,
};
use cairn_common::query::QueryParam;
use cairn_common::read::{ActionSpec, AffordanceSpec, FilterSpec, KeyInfo, LinkSpec};
use cairn_common::uri::CairnResource;

#[derive(Debug)]
pub(super) struct ProjectContext {
    pub(super) project_id: String,
    pub(super) project_key: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ResourceJob {
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
    output_name: Option<String>,
    artifact_type: String,
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

pub(crate) async fn resolve_home_relative_resource_uri(
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
        Ok(resolve_home_suffix(&home_uri, suffix))
    }
}

/// Home-relative suffixes that a delegated task resolves against its OWNING
/// NODE rather than itself.
///
/// Both are properties of the branch, not of the agent reading them: a task edits
/// its owning node's worktree, so the workspace diff is the node's, and a
/// conflict resolution session belongs to the branch the node owns. A task has
/// neither of its own, and leaving these task-scoped would produce a URI that
/// names nothing.
const BRANCH_SCOPED_HOME_SUFFIXES: &[&str] = &["diff", "rebase"];

fn resolve_home_suffix(home_uri: &str, suffix: &str) -> String {
    // Project only these capabilities upward; every other home-relative resource
    // remains task-owned.
    let branch_scoped = BRANCH_SCOPED_HOME_SUFFIXES
        .iter()
        .any(|name| suffix == *name || suffix.starts_with(&format!("{name}?")));
    if branch_scoped {
        if let Some((node_uri, task_name)) = home_uri.rsplit_once("/task/") {
            if !task_name.is_empty() && !task_name.contains('/') {
                return format!("{node_uri}/{suffix}");
            }
        }
    }
    format!("{}/{}", home_uri.trim_end_matches('/'), suffix)
}

/// Open one read transaction for a resource render, on a connection of its own.
///
/// The transaction ends when the returned connection drops at the end of the
/// read — nothing here issues a `ROLLBACK`, and it does not need to: the engine
/// rolls an active MVCC transaction back when its connection is dropped,
/// precisely so its entries cannot leak and block a checkpoint.
///
/// It is a [`TrackedConnection`], so the transaction counts toward the quiet
/// instant `LocalDb`'s connection gate waits for. That matters more here than
/// anywhere else in the codebase: these reads back the desktop UI's status
/// polling, which is the bulk of the database traffic on an idle machine, and
/// they are out of the pool. A quiesce that did not see them would drain to zero
/// and still lose the checkpoint lock (CAIRN-4167).
pub(super) async fn connect_for_read(db: &LocalDb) -> Result<TrackedConnection, String> {
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

/// The one-glyph rendering of a node's run state, shared by every surface that
/// draws one. An unrecognized status renders `?` rather than borrowing the glyph
/// of a state it might not be in: a node whose status Cairn cannot name is not
/// thereby pending.
pub(super) fn job_status_icon(status: &str) -> &'static str {
    match status {
        "complete" => "✓",
        "running" => "◐",
        "failed" => "✗",
        "blocked" => "◼",
        "pending" => "○",
        _ => "?",
    }
}

pub(super) fn resource_job_from_row(row: &cairn_db::turso::Row) -> DbResult<ResourceJob> {
    Ok(ResourceJob {
        id: row.text(0)?,
        parent_job_id: row.opt_text(1)?,
        status: row.text(2)?,
        completed_at: row.opt_i64(3)?.map(|value| value as i32),
        started_at: row.opt_i64(4)?.map(|value| value as i32),
        uri_segment: row.opt_text(5)?,
    })
}

fn resource_artifact_from_row(row: &cairn_db::turso::Row) -> DbResult<ResourceArtifact> {
    Ok(ResourceArtifact {
        data: row.text(0)?,
        output_name: row.opt_text(1)?,
        artifact_type: row.text(2)?,
    })
}

pub(super) async fn lookup_project_by_key(
    conn: &cairn_db::turso::Connection,
    project_key: &str,
) -> Result<ProjectContext, String> {
    let key = cairn_common::uri::canonical_project(project_key);
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

pub(super) async fn issue_id_for_number(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
    project_key: &str,
    number: i32,
) -> Result<(ProjectContext, String), String> {
    let project_ctx = lookup_project_by_key(conn, project_key).await?;
    let issue_id = issue_id_for_number(conn, &project_ctx.project_id, number)
        .await
        .ok_or_else(|| format!("Issue {}/{} not found", project_key, number))?;
    Ok((project_ctx, issue_id))
}

pub(super) async fn visible_job_node_segment(
    conn: &cairn_db::turso::Connection,
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

pub(super) fn affordance_spec_for_kind(
    kind: ResourceKind,
    current: Option<&CairnResource>,
) -> Option<AffordanceSpec> {
    let contract = contract_for(kind)?;
    let links = link_specs(contract, current);
    let filters = filter_specs(contract);
    let mut actions = Vec::new();
    push_mutation_specs(&mut actions, contract, current);
    for related in contract.related {
        if related.actions {
            if let Some(target) = contract_for(related.kind) {
                push_mutation_specs(&mut actions, target, current);
            }
        }
    }
    for cross in contract.cross_actions {
        if let Some(target) = contract_for(cross.kind) {
            if let Some(mutation) = target.mutation(cross.mode) {
                actions.push(action_spec(cross.label, target, mutation, current));
            }
        }
    }
    Some(AffordanceSpec {
        kind: kind.slug().to_string(),
        name: contract.name.to_string(),
        links,
        filters,
        actions,
    })
}

pub(super) fn affordance_for_kind(kind: ResourceKind) -> String {
    affordance_spec_for_kind(kind, None)
        .map(|spec| render_affordance(&spec))
        .unwrap_or_default()
}

pub(super) fn render_affordance(spec: &AffordanceSpec) -> String {
    let mut sections = String::new();
    if !spec.links.is_empty() {
        sections.push_str("### links\n");
        for link in &spec.links {
            sections.push_str(&format!("- [{}]({})\n", link.label, link.uri_template));
        }
        sections.push('\n');
    }
    sections.push_str("### filters\n");
    for filter in &spec.filters {
        sections.push_str(&format!("- `{}={}`\n", filter.key, filter.values));
    }
    sections.push_str(UNIVERSAL_GREP_FILTER);
    sections.push('\n');
    if !spec.actions.is_empty() {
        sections.push_str("### actions\n");
        for action in &spec.actions {
            sections.push_str(&format!(
                "- [{}]({}): {}\n",
                action.label,
                action.uri_template,
                action_summary_from_spec(action)
            ));
        }
        sections.push('\n');
    }
    format!("## {}\n\n{}", spec.name, sections)
}

fn link_specs(contract: &ResourceContract, current: Option<&CairnResource>) -> Vec<LinkSpec> {
    contract
        .related
        .iter()
        .filter_map(|related| {
            let target = contract_for(related.kind)?;
            Some(LinkSpec {
                label: related.label.to_string(),
                uri_template: target.uri_template.to_string(),
                uri: current.and_then(|resource| bind_uri_template(resource, target.uri_template)),
            })
        })
        .collect()
}

fn filter_specs(contract: &ResourceContract) -> Vec<FilterSpec> {
    contract
        .read_projections
        .iter()
        .map(|projection| FilterSpec {
            key: projection.key.to_string(),
            values: projection.values.to_string(),
        })
        .collect()
}

fn key_info(key: &cairn_common::contract::KeySpec) -> KeyInfo {
    KeyInfo {
        key: key.key.to_string(),
        ty: key.ty.as_str().to_string(),
        note: key.note().to_string(),
        aliases: key
            .aliases
            .iter()
            .map(|alias| (*alias).to_string())
            .collect(),
    }
}

fn action_spec(
    label: &str,
    contract: &ResourceContract,
    mutation: &MutationSpec,
    current: Option<&CairnResource>,
) -> ActionSpec {
    ActionSpec {
        label: label.to_string(),
        mode: mutation.mode.as_str().to_string(),
        uri_template: contract.uri_template.to_string(),
        uri: current.and_then(|resource| bind_uri_template(resource, contract.uri_template)),
        required: mutation.required.iter().map(key_info).collect(),
        optional: mutation.optional.iter().map(key_info).collect(),
        example: mutation.example.to_string(),
        guidance: None,
    }
}

fn push_mutation_specs(
    actions: &mut Vec<ActionSpec>,
    contract: &ResourceContract,
    current: Option<&CairnResource>,
) {
    actions.extend(
        contract
            .mutations
            .iter()
            .map(|mutation| action_spec(mutation.label, contract, mutation, current)),
    );
}

/// `` `key(type)` `` / `` `key(type, note)` `` — the backticked key display the
/// write gate's unknown-key rejection also renders (`KeySpec::display`), so an
/// advertised key reads identically wherever it appears.
fn key_display(key: &KeyInfo) -> String {
    if key.note.is_empty() {
        format!("`{}({})`", key.key, key.ty)
    } else {
        format!("`{}({}, {})`", key.key, key.ty, key.note)
    }
}

fn action_summary_from_spec(spec: &ActionSpec) -> String {
    let mut parts = Vec::new();
    if !spec.required.is_empty() {
        parts.push(format!(
            "required {}",
            spec.required
                .iter()
                .map(key_display)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !spec.optional.is_empty() {
        parts.push(format!(
            "optional {}",
            spec.optional
                .iter()
                .map(key_display)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let head = if parts.is_empty() {
        "no payload".to_string()
    } else {
        parts.join("; ")
    };
    format!(
        "{}{}. e.g. {}",
        head,
        spec.guidance.as_deref().unwrap_or_default(),
        spec.example
    )
}

fn bind_uri_template(current: &CairnResource, target_template: &str) -> Option<String> {
    let current_contract = contract_for(current.kind())?;
    let current_uri = current.to_uri();
    let template_segments = current_contract
        .uri_template
        .strip_prefix("cairn://")?
        .split('/');
    let concrete_segments = current_uri.strip_prefix("cairn://")?.split('/');
    let mut bindings = std::collections::HashMap::new();
    for (template, concrete) in template_segments.zip(concrete_segments) {
        if let Some(name) = template
            .strip_prefix('{')
            .and_then(|part| part.strip_suffix('}'))
        {
            bindings.insert(name, concrete);
        }
    }
    let mut bound = target_template.to_string();
    let mut rest = target_template;
    while let Some(open) = rest.find('{') {
        let after_open = &rest[open + 1..];
        let close = after_open.find('}')?;
        let name = &after_open[..close];
        bound = bound.replace(&format!("{{{name}}}"), bindings.get(name)?);
        rest = &after_open[close + 1..];
    }
    Some(bound)
}

/// Collapse an already-shown affordance block to a one-line session pointer,
/// or `None` when the block cannot be faithfully re-rendered from the help
/// projection (leave it full).
///
/// The full block was rendered earlier this session (CAIRN-2592 session-scoped
/// affordance dedup); every later occurrence of identical block content that
/// *round-trips* collapses to this pointer, which targets the on-demand per-kind
/// help projection (`cairn://help?kind=<slug>`) that re-renders the exact block
/// from the same single source ([`affordance_for_kind`]). The full reference
/// thus stays one cheap read away for the rest of the run, including right after
/// context compaction.
///
/// Returns `None` (block stays full) when the first line is not a resolvable
/// `## {contract name}`, OR when `affordance_for_kind(kind)` does not reproduce
/// the block byte-for-byte. The round-trip guard is what keeps schema-derived
/// node/task artifact affordances full: they share the generic artifact contract
/// title but carry schema-specific create-payload keys the static block lacks,
/// so collapsing them to `help?kind=node-artifact` would advertise the generic
/// `{title, content}` example and reintroduce CAIRN #170. Kept a single short
/// stable line so it itself dedupes cleanly under the batch assembler's
/// `(kind, block)` collapse.
pub(crate) fn pointer_affordance_block(block: &str) -> Option<String> {
    let name = block
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("## "))
        .map(str::trim)?;
    let kind = cairn_common::contract::RESOURCE_CONTRACTS
        .iter()
        .find(|contract| contract.name == name)
        .map(|contract| contract.kind)?;
    // Only collapse when the help projection re-renders this exact block; a
    // schema-derived artifact block diverges from the static contract block and
    // is therefore left full (see doc comment / CAIRN #170).
    if affordance_for_kind(kind) != block {
        return None;
    }
    Some(format!("\u{2014} ref: cairn://help?kind={}", kind.slug()))
}

/// Build a node/task artifact's affordance spec, deriving the `create` action's
/// payload keys and example from the artifact's resolved JSON Schema. The static
/// contract example uses generic placeholder keys (`{title, content}`) that don't
/// match a custom artifact's real schema, so copying it bounces (CAIRN #170).
/// Returns `None` when the schema has no usable top-level `properties`, leaving
/// the caller to fall back to the contract-derived spec.
///
/// Schema-derived keys are exactly what a static per-kind table cannot describe,
/// which is why the *spec* has to come from here and not just the rendered
/// markdown: a consumer assembling a write from `ActionSpec.required` would
/// otherwise be handed `{title, content}` for an artifact that declares neither.
pub(super) fn artifact_affordance_spec_with_schema(
    kind: ResourceKind,
    addressed_name: Option<&str>,
    schema: &serde_json::Value,
    current: Option<&CairnResource>,
) -> Option<AffordanceSpec> {
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

    let schema_key = |name: &str| KeyInfo {
        key: name.to_string(),
        ty: props
            .get(name)
            .and_then(|p| p.get("type"))
            .and_then(|t| t.as_str())
            .map(schema_type_label)
            .unwrap_or(KeyType::Str.as_str())
            .to_string(),
        note: String::new(),
        aliases: Vec::new(),
    };

    let example_uri = match addressed_name {
        Some(name) => format!("cairn:~/{name}"),
        None => "cairn:~/<name>".to_string(),
    };
    // Every artifact action targets the artifact being read, so the concrete
    // binding is simply its own URI — no template to bind.
    let target_uri = current.map(|resource| resource.to_uri());
    let payload = schema_example_payload(props, &ordered);
    let create_label = contract
        .mutation(ChangeMode::Create)
        .map(|spec| spec.label)
        .unwrap_or("write artifact");
    let mut actions = vec![ActionSpec {
        label: create_label.to_string(),
        mode: ChangeMode::Create.as_str().to_string(),
        uri_template: example_uri.clone(),
        uri: target_uri.clone(),
        required: ordered
            .iter()
            .filter(|key| required.contains(*key))
            .map(|key| schema_key(key))
            .collect(),
        optional: ordered
            .iter()
            .filter(|key| !required.contains(*key))
            .map(|key| schema_key(key))
            .collect(),
        example: format!(
            "write({{changes:[{{target:\"{example_uri}\",mode:\"create\",payload:{payload}}}]}})"
        ),
        guidance: None,
    }];

    // The arc's item-level actions come first: they append or amend one ruling,
    // where the generic field merge would replace the whole set.
    if addressed_name == Some(crate::threads::ARC_ARTIFACT_NAME) {
        actions.push(ActionSpec {
            label: "append one ruling".to_string(),
            mode: ChangeMode::Patch.as_str().to_string(),
            uri_template: example_uri.clone(),
            uri: target_uri.clone(),
            required: vec![KeyInfo {
                key: "ruling".to_string(),
                ty: KeyType::Object.as_str().to_string(),
                note: String::new(),
                aliases: Vec::new(),
            }],
            optional: Vec::new(),
            example: "write({changes:[{target:\"cairn:~/arc\",mode:\"patch\",payload:{ruling:{text:\"...\",status:\"accepted\",rationale:\"...\",provenance:[\"cairn://p/PROJECT/NUMBER\"]}}}]})".to_string(),
            guidance: Some(" with text, status, rationale, and canonical provenance; Cairn mints its stable slug. This does not resend or replace any other ruling".to_string()),
        });
        actions.push(ActionSpec {
            label: "patch one ruling by slug".to_string(),
            mode: ChangeMode::Patch.as_str().to_string(),
            uri_template: example_uri.clone(),
            uri: target_uri.clone(),
            required: vec![
                KeyInfo {
                    key: "ruling_slug".to_string(),
                    ty: KeyType::Str.as_str().to_string(),
                    note: String::new(),
                    aliases: Vec::new(),
                },
                KeyInfo {
                    key: "patch".to_string(),
                    ty: KeyType::Object.as_str().to_string(),
                    note: String::new(),
                    aliases: Vec::new(),
                },
            ],
            optional: Vec::new(),
            example: "write({changes:[{target:\"cairn:~/arc\",mode:\"patch\",payload:{ruling_slug:\"no-budget-kills\",patch:{status:\"superseded\",rationale:\"...\"}}}]})".to_string(),
            guidance: Some("; the slug is immutable. This does not resend or replace any other ruling".to_string()),
        });
    }

    // Ordinary field merge remains available after the safer arc item actions.
    if let Some(patch) = contract.mutation(ChangeMode::Patch) {
        let mut spec = action_spec(patch.label, contract, patch, current);
        spec.uri_template = example_uri.clone();
        spec.uri = target_uri.clone();
        actions.push(spec);
    }

    Some(AffordanceSpec {
        kind: kind.slug().to_string(),
        name: contract.name.to_string(),
        links: link_specs(contract, current),
        filters: filter_specs(contract),
        actions,
    })
}

#[cfg(test)]
fn artifact_affordance_with_schema(
    kind: ResourceKind,
    addressed_name: Option<&str>,
    schema: &serde_json::Value,
) -> Option<String> {
    artifact_affordance_spec_with_schema(kind, addressed_name, schema, None)
        .map(|spec| render_affordance(&spec))
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
pub(super) async fn get_todo_progress(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Option<String> {
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

/// What to say when a node coordinate resolves to no job, in the terms of
/// whoever owns it.
///
/// A thread that has never run has no session job, which is not an error but a
/// state: reads degrade to pointing at the thread overview rather than reporting
/// a missing node under issue zero.
pub(crate) fn node_job_not_found_message(
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> String {
    match cairn_common::uri::NodeAddress::new(number, exec_seq, node_name) {
        cairn_common::uri::NodeAddress::Thread { name } => format!(
            "Thread '{name}' has no session yet. Read {} for the thread overview.",
            cairn_common::uri::build_thread_uri(project_key, name)
        ),
        cairn_common::uri::NodeAddress::Node { .. } => {
            format!("Node '{node_name}' not found for issue {project_key}/{number}")
        }
    }
}

pub(super) async fn find_job_by_id(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Option<ResourceJob> {
    let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = ?1 LIMIT 1");
    let mut rows = conn.query(sql.as_str(), params![job_id]).await.ok()?;
    resource_job_from_row(&rows.next().await.ok()??).ok()
}

/// The job a node coordinate names, loaded read-only.
///
/// Owner-aware through `job_id_for_node_coordinate_conn`, so this resolves an
/// execution node under an issue and a thread's session job through one call —
/// and every collection built on it (tasks, questions, todos) works from a
/// thread without knowing a thread exists.
pub(crate) async fn connect_and_find_node_job(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
) -> Result<(TrackedConnection, ResourceJob), String> {
    let conn = connect_for_read(db).await?;
    // A node coordinate names an issue, so an unknown project key or a
    // nonexistent issue is reported as itself rather than collapsed into "node
    // not found", which would be false for the first and misleading for the
    // second. A thread coordinate names no issue and goes straight to the job.
    if let cairn_common::uri::NodeAddress::Node { .. } =
        cairn_common::uri::NodeAddress::new(number, exec_seq, node_name)
    {
        resolve_issue_id(&conn, project_key, number).await?;
    }
    let job_id = crate::jobs::queries::job_id_for_node_coordinate_conn(
        &conn,
        project_key,
        number,
        exec_seq,
        node_name,
        None,
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| node_job_not_found_message(project_key, number, exec_seq, node_name))?;
    let job = find_job_by_id(&conn, &job_id)
        .await
        .ok_or_else(|| node_job_not_found_message(project_key, number, exec_seq, node_name))?;
    Ok((conn, job))
}

/// The bookmark a node's work lives on, or `None` when it has no branch yet.
pub(crate) async fn node_branch(
    conn: &cairn_db::turso::Connection,
    job_id: &str,
) -> Result<Option<String>, String> {
    let mut rows = conn
        .query("SELECT branch FROM jobs WHERE id = ?1 LIMIT 1", (job_id,))
        .await
        .map_err(|error| format!("Failed to load node branch: {error}"))?;
    let Some(row) = rows
        .next()
        .await
        .map_err(|error| format!("Failed to load node branch: {error}"))?
    else {
        return Ok(None);
    };
    Ok(row
        .opt_text(0)
        .ok()
        .flatten()
        .filter(|value| !value.is_empty()))
}

pub(super) async fn connect_and_find_task_job(
    db: &LocalDb,
    project_key: &str,
    number: i32,
    exec_seq: i32,
    node_name: &str,
    task_name: &str,
) -> Result<(TrackedConnection, ResourceJob, ResourceJob), String> {
    // The parent resolves first so a thread with no session reports that rather
    // than reporting a missing task; from there a task is a child job by segment
    // whichever kind of parent owns it.
    let (conn, parent_job) =
        connect_and_find_node_job(db, project_key, number, exec_seq, node_name).await?;
    let task_job = find_task_by_name(&conn, &parent_job.id, task_name).await?;
    Ok((conn, parent_job, task_job))
}

/// Check if a job or any of its child jobs have an artifact
pub(super) async fn has_artifact_for_job(conn: &cairn_db::turso::Connection, job_id: &str) -> bool {
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
pub(super) async fn has_terminal_for_job(conn: &cairn_db::turso::Connection, job_id: &str) -> bool {
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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

async fn find_task_by_name(
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
    conn: &cairn_db::turso::Connection,
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
          -- Top-level nodes have no parent; a workflow is a child job (for the
          -- delegation tree) yet is addressable as a node by its segment, so its
          -- `cairn:~/calls` and child-call `?wait` URIs resolve.
          AND (parent_job_id IS NULL OR agent_config_id = 'workflow')
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
    conn: &cairn_db::turso::Connection,
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
        "Node '{}' not found for issue {}/{}",
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
    fn task_home_diff_projects_to_the_owning_node() {
        let home = "cairn://p/cairn/2691/1/builder/task/review";
        assert_eq!(
            resolve_home_suffix(home, "diff"),
            "cairn://p/cairn/2691/1/builder/diff"
        );
        assert_eq!(
            resolve_home_suffix(home, "diff?view=check"),
            "cairn://p/cairn/2691/1/builder/diff?view=check"
        );
        assert_eq!(
            resolve_home_suffix(home, "messages"),
            "cairn://p/cairn/2691/1/builder/task/review/messages"
        );
    }

    /// A conflict resolution session belongs to the branch, and a delegated task
    /// has no branch of its own. Left task-scoped, a sub-agent following the wake
    /// would build a URI that names nothing — and it is sub-agents who most often
    /// meet the conflict, since they do the editing.
    #[test]
    fn task_home_rebase_projects_to_the_owning_node() {
        let home = "cairn://p/cairn/2691/1/builder/task/review";
        assert_eq!(
            resolve_home_suffix(home, "rebase"),
            "cairn://p/cairn/2691/1/builder/rebase"
        );
        assert_eq!(
            resolve_home_suffix(home, "rebase?view=base-theirs&file=a.rs"),
            "cairn://p/cairn/2691/1/builder/rebase?view=base-theirs&file=a.rs"
        );
        // The projection is a short, deliberate list, not a general rule.
        assert_eq!(
            resolve_home_suffix(home, "todos"),
            "cairn://p/cairn/2691/1/builder/task/review/todos"
        );
    }

    #[test]
    fn node_home_diff_stays_on_the_node() {
        assert_eq!(
            resolve_home_suffix("cairn://p/cairn/2691/1/builder", "diff?view=patch"),
            "cairn://p/cairn/2691/1/builder/diff?view=patch"
        );
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
    fn issue_changed_affordance_links_to_node_diff() {
        let output = affordance_for_kind(ResourceKind::Changed);
        assert!(output.contains("- [node diff](cairn://p/{project}/{number}/{exec}/{node}/diff)"));
    }

    #[test]
    fn pointer_collapses_full_block_to_help_slug() {
        // Built against a real `affordance_for_kind` output so the first-line
        // parse can't silently drift from the block format it consumes, and so
        // the round-trip guard sees an identical block and collapses it.
        let full = affordance_for_kind(ResourceKind::Issue);
        assert!(full.starts_with("## Issue details\n"));
        let pointer =
            pointer_affordance_block(&full).expect("static block round-trips and collapses");
        // One short line, pointing at the per-kind help projection, body dropped.
        assert_eq!(pointer.lines().count(), 1, "{pointer}");
        assert!(pointer.contains("cairn://help?kind=issue"), "{pointer}");
        assert!(!pointer.contains("### actions"), "{pointer}");
    }

    #[test]
    fn pointer_passes_through_unresolvable_block() {
        // A first line that isn't a `## {contract name}` can't resolve to a kind,
        // so there is no faithful pointer target and the block stays full.
        let block = "## Not A Real Contract Name\n\n### actions\n- do thing\n";
        assert!(pointer_affordance_block(block).is_none());
    }

    #[test]
    fn pointer_does_not_collapse_schema_aware_artifact() {
        // A schema-derived artifact block carries the same `## {contract name}`
        // title as the static block but a schema-specific create payload.
        // Collapsing it to `help?kind=node-artifact` (which only re-renders the
        // static contract block) would strip the schema keys and reintroduce
        // CAIRN #170, so the round-trip guard must leave it full even though its
        // title resolves to a kind.
        let schema = serde_json::json!({
            "type": "object",
            "required": ["title"],
            "properties": {
                "title": { "type": "string" },
                "scratch": { "type": "string" }
            }
        });
        let schema_block =
            artifact_affordance_with_schema(ResourceKind::NodeArtifact, Some("board"), &schema)
                .expect("schema with properties yields a block");
        assert!(
            pointer_affordance_block(&schema_block).is_none(),
            "schema-aware artifact block must not collapse: {schema_block}"
        );

        // The static contract block for the same kind round-trips and does
        // collapse, confirming the guard discriminates on block content, not kind.
        let static_block = affordance_for_kind(ResourceKind::NodeArtifact);
        assert!(pointer_affordance_block(&static_block).is_some());
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
    fn arc_affordance_leads_with_item_level_ruling_actions() {
        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../../../resources/schemas/arc.json")).unwrap();
        let block =
            artifact_affordance_with_schema(ResourceKind::NodeArtifact, Some("arc"), &schema)
                .unwrap();
        let append = block.find("[append one ruling]").unwrap();
        let edit = block.find("[patch one ruling by slug]").unwrap();
        let generic = block
            .find("[edit, confirm, or act on a PR artifact]")
            .unwrap();
        assert!(append < edit && edit < generic, "{block}");
        assert!(block.contains("payload:{ruling:{text:\"...\",status:\"accepted\",rationale:\"...\",provenance:[\"cairn://p/PROJECT/NUMBER\"]}}"));
        assert!(block.contains("payload:{ruling_slug:\"no-budget-kills\",patch:{status:\"superseded\",rationale:\"...\"}}"));
        assert_eq!(
            block
                .matches("does not resend or replace any other ruling")
                .count(),
            2
        );
        assert!(block.contains("the slug is immutable"));
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
    fn project_issues_affordance_uses_templated_parent_link() {
        let output = affordance_for_kind(ResourceKind::ProjectIssues);

        assert!(output.starts_with("## Project issues\n"));
        assert!(output.contains("- [up](cairn://p/{project})"));
        assert!(output.contains("`status=backlog,active`"));
        assert!(output.contains("- [create issue](cairn://p/{project}/issues):"));
    }
}
