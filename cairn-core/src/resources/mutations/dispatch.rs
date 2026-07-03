use super::actions::{apply_action_create, apply_action_delete, apply_action_patch};
use super::agents::{apply_agent_create, apply_agent_delete, apply_agent_patch};
use super::browsers::{
    apply_browser_action, apply_browser_delete, apply_browser_ensure, BrowserInteractionArgs,
};
use super::labels::{apply_label_create, apply_label_delete, apply_label_patch};
use super::mcp::{apply_mcp_create, apply_mcp_delete, apply_mcp_patch};
use super::memories::{
    apply_memory_triage_action, apply_node_memory_append, apply_node_memory_delete,
    apply_node_memory_patch, MemoryCreateTarget, MemoryResourceTarget,
};
use super::projects::{
    apply_project_patch, apply_project_reference_create, apply_project_reference_delete,
    apply_project_reference_patch, apply_project_settings_patch, apply_projects_create,
};
use super::recipes::{apply_recipe_create, apply_recipe_delete, apply_recipe_patch};
use super::settings::apply_settings_patch;
use super::skills::{apply_skill_create, apply_skill_delete, apply_skill_patch};
use super::{
    build_failure, mode_name, payload_bool, payload_non_empty_str, payload_str,
    payload_trimmed_non_empty_str, payload_value, target_resource_for_request,
    ResourceAppliedChange, ResourceMutationResult,
};
use crate::mcp::handlers::{
    bug_report, comments_artifacts, executions, issues, messages, planning, terminal,
};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::contract::{contract_for, mutation_spec, MutationSpec, ResourceKind};
use cairn_common::uri::CairnResource;
use serde::de::DeserializeOwned;

fn parse_todo_write_items(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Vec<crate::todos::TodoWriteItem>> {
    parse_required_payload_field_describing(
        index,
        item,
        payload,
        "todos",
        crate::todos::TODO_WRITE_ITEM_KEYS,
    )
}

/// Parse a required payload field, enumerating the item's accepted keys when
/// deserialization fails. A mis-keyed array item (e.g. a todo keyed `title`
/// instead of `content`) yields a serde error that names at most the first
/// missing field; appending the accepted-key list turns the rejection into a
/// complete, self-correcting message (CAIRN #164).
fn parse_required_payload_field_describing<T>(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
    key: &str,
    accepted_keys: &str,
) -> ResourceMutationResult<T>
where
    T: DeserializeOwned,
{
    let value = payload
        .get(key)
        .ok_or_else(|| build_failure(index, item, format!("payload.{key} is required")))?;
    serde_json::from_value(value.clone()).map_err(|e| {
        build_failure(
            index,
            item,
            format!("Invalid payload.{key}: {e}. Each item accepts: {accepted_keys}."),
        )
    })
}

fn parse_string_array_field(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
    key: &str,
    aliases: &[&str],
) -> ResourceMutationResult<Option<Vec<String>>> {
    let value = payload_value(payload, key, aliases);
    let Some(value) = value else {
        return Ok(None);
    };
    let values = value.as_array().ok_or_else(|| {
        build_failure(
            index,
            item,
            format!("payload.{key} must be an array of non-empty strings"),
        )
    })?;
    let mut parsed = Vec::with_capacity(values.len());
    for value in values {
        let item_value = value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.{key} must be an array of non-empty strings"),
                )
            })?;
        parsed.push(item_value.to_string());
    }
    Ok(Some(parsed))
}

fn parse_todo_update_items(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Vec<crate::todos::TodoUpdateItem>> {
    parse_required_payload_field_describing(
        index,
        item,
        payload,
        "updates",
        crate::todos::TODO_UPDATE_ITEM_KEYS,
    )
}

fn append_payload(index: usize, item: &ChangeItem) -> ResourceMutationResult<&serde_json::Value> {
    item.payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=append requires payload"))
}

/// Resolve a per-issue comment `seq` to its stable comment id, mapping a missing
/// issue or comment to a clean not-found failure. The member URI embeds the
/// issue number, so a seq that belongs to a different issue is rejected too.
async fn resolve_issue_comment_id(
    db: &crate::storage::LocalDb,
    index: usize,
    item: &ChangeItem,
    project: &str,
    number: i32,
    comment_seq: i32,
) -> ResourceMutationResult<String> {
    let issue_id = crate::issues::relations::issue_id_for_project_number(db, project, number)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| build_failure(index, item, format!("Issue {project}-{number} not found")))?;
    crate::issues::comments::id_for_issue_seq(db, &issue_id, comment_seq as i64)
        .await
        .map_err(|error| build_failure(index, item, error.to_string()))?
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                format!("Comment {comment_seq} not found on issue {project}-{number}"),
            )
        })
}

struct DirectMessageTarget<'a> {
    project: &'a str,
    number: i32,
    exec_seq: i32,
    node_id: &'a str,
    task_name: Option<&'a str>,
}

async fn append_node_or_task_message(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    target: DirectMessageTarget<'_>,
    dry_run: bool,
) -> ResourceMutationResult<String> {
    let payload = append_payload(index, item)?;
    let content = payload_non_empty_str(payload, "content", &[])
        .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
    let escalate = payload
        .get("escalate")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    let target_uri = match target.task_name {
        Some(task_name) => format!(
            "{}-{}/{}/{}/task/{}",
            target.project, target.number, target.exec_seq, target.node_id, task_name
        ),
        None => format!(
            "{}-{}/{}/{}",
            target.project, target.number, target.exec_seq, target.node_id
        ),
    };
    if dry_run {
        Ok(format!(
            "Would send {} chars to {target_uri}",
            content.len()
        ))
    } else {
        messages::append_direct_message(
            orch,
            request,
            target.project,
            target.number,
            target.exec_seq,
            target.node_id,
            target.task_name,
            content,
            escalate,
        )
        .await
        .map_err(|error| build_failure(index, item, error))
    }
}

#[derive(Debug)]
struct WakeFilterPayload {
    kind: String,
    reference: Option<String>,
    fact_kinds: Option<Vec<String>>,
    /// Terminal subscriptions only: `"exit"` (default) or `"output"`.
    on: Option<String>,
    /// Terminal `on:"output"` subscriptions only: the literal phrase to watch for.
    phrase: Option<String>,
}

fn parse_wake_filter(
    index: usize,
    item: &ChangeItem,
    value: &serde_json::Value,
    field: &str,
) -> ResourceMutationResult<WakeFilterPayload> {
    let obj = value
        .as_object()
        .ok_or_else(|| build_failure(index, item, format!("payload.{field} must be an object")))?;
    let kind = obj
        .get("kind")
        .or_else(|| obj.get("sourceKind"))
        .or_else(|| obj.get("source_kind"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| build_failure(index, item, format!("payload.{field}.kind is required")))?
        .to_string();
    let reference = obj
        .get("ref")
        .or_else(|| obj.get("sourceRef"))
        .or_else(|| obj.get("source_ref"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let fact_kinds = obj
        .get("factKinds")
        .or_else(|| obj.get("fact_kinds"))
        .or_else(|| obj.get("kinds"))
        .map(|value| {
            let values = value.as_array().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.{field}.factKinds must be an array"),
                )
            })?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .map(ToString::to_string)
                        .ok_or_else(|| {
                            build_failure(
                                index,
                                item,
                                format!("payload.{field}.factKinds entries must be strings"),
                            )
                        })
                })
                .collect::<ResourceMutationResult<Vec<_>>>()
        })
        .transpose()?;
    let on = obj
        .get("on")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let phrase = obj
        .get("phrase")
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    Ok(WakeFilterPayload {
        kind,
        reference,
        fact_kinds,
        on,
        phrase,
    })
}

/// Resolve a terminal-exit wake `ref` to its bare slug. Accepts a bare slug,
/// `cairn:~/terminal/<slug>`, or a canonical `.../terminal/<slug>` URI.
fn terminal_slug_from_ref(reference: &str) -> &str {
    let trimmed = reference.trim();
    match trimmed.rfind("terminal/") {
        Some(pos) => &trimmed[pos + "terminal/".len()..],
        None => trimmed,
    }
}

/// Subscribe an agent-facing `kind:"terminal"` wake. Normalizes to the internal
/// `process` source with a `terminal_exit` fact kind and one-shot semantics,
/// validates the terminal exists in the caller's job scope, and fires
/// immediately when the terminal has already exited.
#[allow(clippy::too_many_arguments)]
async fn subscribe_terminal_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    filter: &WakeFilterPayload,
    created_by: &str,
    dry_run: bool,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
) -> ResourceMutationResult<String> {
    let reference = filter.reference.as_deref().ok_or_else(|| {
        build_failure(
            index,
            item,
            "terminal subscribe requires ref (e.g. cairn:~/terminal/<slug> or a bare slug)",
        )
    })?;
    let slug = terminal_slug_from_ref(reference).to_string();
    if slug.is_empty() {
        return Err(build_failure(
            index,
            item,
            "terminal subscribe ref resolved to an empty slug",
        ));
    }

    let row =
        crate::mcp::handlers::terminal::lookup_terminal_for_wake(&orch.db.local, job_id, &slug)
            .await
            .map_err(|error| build_failure(index, item, error.to_string()))?;
    let Some(row) = row else {
        let slugs = crate::mcp::handlers::terminal::list_job_terminal_slugs(&orch.db.local, job_id)
            .await
            .unwrap_or_default();
        let listed = if slugs.is_empty() {
            "no terminals exist in this scope".to_string()
        } else {
            format!("existing terminals in this scope: {}", slugs.join(", "))
        };
        return Err(build_failure(
            index,
            item,
            format!("Terminal '{slug}' not found; {listed}."),
        ));
    };

    let uri = cairn_common::uri::build_node_terminal_uri(project, number, exec_seq, node_id, &slug);

    match filter.on.as_deref().unwrap_or("exit") {
        "exit" => {
            subscribe_terminal_exit_wake(
                orch, index, item, job_id, created_by, dry_run, &slug, &uri, &row,
            )
            .await
        }
        "output" => {
            subscribe_terminal_output_wake(
                orch, index, item, job_id, filter, created_by, dry_run, &slug, &uri, &row,
            )
            .await
        }
        other => Err(build_failure(
            index,
            item,
            format!("terminal subscribe `on` must be \"exit\" or \"output\" (got \"{other}\")"),
        )),
    }
}

/// Subscribe a one-shot terminal-**exit** wake: resume the node when the terminal
/// finishes (fires immediately if it already exited).
#[allow(clippy::too_many_arguments)]
async fn subscribe_terminal_exit_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    created_by: &str,
    dry_run: bool,
    slug: &str,
    uri: &str,
    row: &crate::mcp::handlers::terminal::TerminalWakeRow,
) -> ResourceMutationResult<String> {
    if dry_run {
        return Ok(format!("Would subscribe terminal-exit wake: {slug}"));
    }

    match crate::mcp::handlers::terminal::subscribe_terminal_exit_wake_once(
        orch,
        job_id,
        slug,
        uri,
        Some(row),
        created_by,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?
    {
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::ExitAlreadyQueued => Ok(
            format!("Terminal '{slug}' already exited; resume queued for turn end ({uri})"),
        ),
        _ => Ok(format!(
            "Subscribed to terminal '{slug}' exit; end your turn to resume when it finishes ({uri})"
        )),
    }
}

/// Subscribe a one-shot terminal-**output** phrase wake: resume the node when a
/// literal phrase appears in the running terminal's output. The subscription
/// also fires on terminal exit, so a process that dies before printing the
/// phrase still wakes the waiting agent. Only meaningful on a running terminal.
#[allow(clippy::too_many_arguments)]
async fn subscribe_terminal_output_wake(
    orch: &Orchestrator,
    index: usize,
    item: &ChangeItem,
    job_id: &str,
    filter: &WakeFilterPayload,
    created_by: &str,
    dry_run: bool,
    slug: &str,
    uri: &str,
    row: &crate::mcp::handlers::terminal::TerminalWakeRow,
) -> ResourceMutationResult<String> {
    let phrase = filter
        .phrase
        .as_deref()
        .map(str::trim)
        .filter(|phrase| !phrase.is_empty())
        .ok_or_else(|| {
            build_failure(
                index,
                item,
                "terminal output subscribe requires a non-empty phrase, e.g. {subscribe:{kind:\"terminal\",ref:\"cairn:~/terminal/dev\",on:\"output\",phrase:\"ready\"}}",
            )
        })?;

    if dry_run {
        return Ok(format!(
            "Would subscribe terminal-output wake on '{slug}' for phrase \"{phrase}\""
        ));
    }

    match crate::mcp::handlers::terminal::subscribe_terminal_output_wake_once(
        orch,
        job_id,
        slug,
        uri,
        phrase,
        Some(row),
        None,
        created_by,
    )
    .await
    .map_err(|error| build_failure(index, item, error))?
    {
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::OutputAlreadyQueued => {
            Ok(format!(
                "Terminal '{slug}' already printed \"{phrase}\"; resume queued for turn end ({uri})"
            ))
        }
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::OutputRegistered => {
            Ok(format!(
                "Subscribed to terminal '{slug}' output for phrase \"{phrase}\"; end your turn to resume when it appears ({uri})"
            ))
        }
        crate::mcp::handlers::terminal::TerminalWakeSubscriptionOutcome::ExitAlreadyQueued => Ok(
            format!(
                "Terminal '{slug}' already exited before \"{phrase}\" appeared; resume queued for turn end ({uri})"
            ),
        ),
        _ => Ok(format!(
            "Subscribed to terminal '{slug}' output for phrase \"{phrase}\"; end your turn to resume when it next prints, across restarts ({uri})"
        )),
    }
}

/// Parse the optional `execution` object on an issue-create payload into a
/// create+start spec. Absent or null -> None (create only). When present it must
/// be an object whose `recipe`/`backend`, if set, are strings.
fn parse_create_execution_spec(
    index: usize,
    item: &ChangeItem,
    payload: &serde_json::Value,
) -> ResourceMutationResult<Option<issues::CreateExecutionSpec>> {
    let value = match payload.get("execution") {
        None | Some(serde_json::Value::Null) => return Ok(None),
        Some(value) => value,
    };
    let obj = value.as_object().ok_or_else(|| {
        build_failure(
            index,
            item,
            "payload.execution must be an object {recipe?, backend?}",
        )
    })?;
    let str_field = |key: &str| -> ResourceMutationResult<Option<String>> {
        match obj.get(key) {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(value) => value.as_str().map(|s| Some(s.to_string())).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("payload.execution.{key} must be a string"),
                )
            }),
        }
    };
    Ok(Some(issues::CreateExecutionSpec {
        recipe: str_field("recipe")?,
        backend: str_field("backend")?,
    }))
}

/// Build the "unsupported mutation" rejection by enumerating the resource's
/// valid mutations from the contract table.
fn render_unsupported(kind: ResourceKind, mode: ChangeMode) -> String {
    let mut out = format!(
        "Unsupported resource mutation: mode '{}' is not valid for this resource.",
        mode_name(mode)
    );
    match contract_for(kind) {
        Some(contract) if !contract.mutations.is_empty() => {
            out.push_str(" Supported mutations:");
            for spec in contract.mutations {
                out.push_str(&format!(
                    "\n- {} (mode={}): {}",
                    spec.label,
                    mode_name(spec.mode),
                    spec.example
                ));
            }
        }
        _ => out.push_str(" This resource is read-only."),
    }
    out.push_str(" See cairn://help for the full (resource, mode) mutation matrix.");
    out
}

/// Build the "missing required key" rejection naming the absent keys + example.
fn render_missing_keys(spec: &MutationSpec, missing: &[&str]) -> String {
    format!(
        "Missing required payload key(s) for '{}': {}. Example: {}",
        spec.label,
        missing.join(", "),
        spec.example
    )
}

/// Collect the required keys (by canonical name) absent from the payload.
/// Aliases count as present; an empty `required` set never reports a miss.
fn missing_required_keys<'a>(
    spec: &'a MutationSpec,
    payload: Option<&serde_json::Value>,
) -> Vec<&'a str> {
    if spec.required.is_empty() {
        return Vec::new();
    }
    let keys: Vec<&str> = payload
        .and_then(|p| p.as_object())
        .map(|map| map.keys().map(String::as_str).collect())
        .unwrap_or_default();
    spec.required
        .iter()
        .filter(|req| !req.satisfied_by(keys.iter().copied()))
        .map(|req| req.key)
        .collect()
}

/// Table-authoritative gate: confirm the (kind, mode) pair is supported and
/// shallow-check required payload keys. Deep validation happens in the dispatch
/// arm afterwards.
fn gate_resource_change(
    index: usize,
    item: &ChangeItem,
    resource: &CairnResource,
) -> ResourceMutationResult<&'static MutationSpec> {
    let kind = resource.kind();
    let spec = mutation_spec(kind, item.mode)
        .ok_or_else(|| build_failure(index, item, render_unsupported(kind, item.mode)))?;
    let missing = missing_required_keys(spec, item.payload.as_ref());
    if !missing.is_empty() {
        return Err(build_failure(
            index,
            item,
            render_missing_keys(spec, &missing),
        ));
    }
    Ok(spec)
}

/// Single dispatcher for resource-target mutations. `dry_run` selects between
/// computing a preview summary (no side effects) and executing the mutation.
///
/// Gate-first: the contract table decides whether a `(kind, mode)` pair is
/// routable before any typed parser runs. A rejection here enumerates the
/// resource's valid mutations; the typed arms below still perform deep
/// validation.
pub(crate) async fn dispatch_resource_change(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
) -> ResourceMutationResult<ResourceAppliedChange> {
    let resource = target_resource_for_request(orch, request, item)
        .await
        .map_err(|e| build_failure(index, item, e))?;
    gate_resource_change(index, item, &resource)?;

    // Optional structured echo of the post-mutation state, surfaced to UI
    // renderers via the change result. Currently set only by the todos arms.
    let mut applied_data: Option<serde_json::Value> = None;
    let mut promoted_memory = None;
    let summary = match (&resource, item.mode) {
        (CairnResource::ProjectIssues { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let title = payload_trimmed_non_empty_str(payload, "title", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.title is required and must be a non-empty string",
                )
            })?;
            if let Some(description) = payload.get("description") {
                if !description.is_string() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.description must be a string",
                    ));
                }
            }
            let parent = if let Some(parent) = payload.get("parent") {
                let Some(parent) = parent
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.parent must be a non-empty string",
                    ));
                };
                Some(parent.to_string())
            } else {
                None
            };
            let execution = parse_create_execution_spec(index, item, payload)?;
            let labels = parse_string_array_field(index, item, payload, "labels", &[])?;
            if dry_run {
                match &execution {
                    Some(spec) => format!(
                        "Would create issue in project {project}: {title} and start an execution{}",
                        spec.recipe
                            .as_deref()
                            .map(|r| format!(" (recipe '{r}')"))
                            .unwrap_or_default()
                    ),
                    None => format!("Would create issue in project {project}: {title}"),
                }
            } else {
                let description = payload_str(payload, "description", &[]).map(ToOwned::to_owned);
                let outcome = issues::create_issue_in_project(
                    orch,
                    project,
                    title.to_string(),
                    description,
                    labels,
                    execution,
                    parent,
                    request.run_id.clone(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                applied_data = Some(serde_json::json!({
                    "projectKey": outcome.project_key,
                    "number": outcome.number,
                    "uri": outcome.uri,
                }));
                outcome.summary
            }
        }
        (CairnResource::ProjectMessages { project }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars to project channel {project}",
                    content.len()
                )
            } else {
                messages::append_project_or_issue_message(orch, request, project, None, content)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeTerminal { slug, .. }
            | CairnResource::ProjectTerminal { slug, .. }
            | CairnResource::TaskTerminal { slug, .. },
            ChangeMode::Create,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            let command =
                payload_trimmed_non_empty_str(payload, "command", &[]).ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.command is required and must be a non-empty string",
                    )
                })?;
            let description = match payload.get("description") {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.description must be a string")
                })?),
                None => None,
            };
            let wake = match payload.get("wake") {
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            "payload.wake must be a string: \"exit\" or a literal output phrase",
                        )
                    })?;
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        return Err(build_failure(index, item, "payload.wake must not be empty; use \"exit\" or a literal output phrase"));
                    }
                    Some(if trimmed == "exit" {
                        terminal::TerminalWakeSpec::Exit
                    } else {
                        terminal::TerminalWakeSpec::Output(trimmed.to_string())
                    })
                }
                None => None,
            };
            if wake.is_some()
                && matches!(resource, CairnResource::ProjectTerminal { .. })
                && request.run_id.is_none()
            {
                return Err(build_failure(
                    index,
                    item,
                    "payload.wake requires an agent caller when creating a project terminal",
                ));
            }
            if dry_run {
                let wake_suffix = wake
                    .as_ref()
                    .map_or_else(String::new, terminal::TerminalWakeSpec::dry_run_suffix);
                format!("Would start terminal {slug}: {command}{wake_suffix}")
            } else {
                terminal::create_terminal_from_resource(
                    orch,
                    &resource,
                    command,
                    description,
                    wake,
                    request.run_id.as_deref(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeTerminal { slug, .. }
            | CairnResource::ProjectTerminal { slug, .. }
            | CairnResource::TaskTerminal { slug, .. },
            ChangeMode::Append,
        ) => {
            let payload = append_payload(index, item)?;
            let content = payload_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            let submit = payload_bool(payload, "submit").unwrap_or(true);
            if dry_run {
                if submit {
                    format!("Would submit {} chars to terminal {slug}", content.len())
                } else {
                    format!("Would send {} raw chars to terminal {slug}", content.len())
                }
            } else {
                terminal::append_terminal_input(orch, &resource, content, submit)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeTerminal { slug, .. }
            | CairnResource::ProjectTerminal { slug, .. }
            | CairnResource::TaskTerminal { slug, .. },
            ChangeMode::Delete,
        ) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            if dry_run {
                format!("Would stop terminal {slug}")
            } else {
                terminal::delete_terminal_by_resource(orch, &resource)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Create,
        ) => {
            let url = item
                .payload
                .as_ref()
                .and_then(|payload| payload_trimmed_non_empty_str(payload, "url", &["navigate"]))
                .map(ToOwned::to_owned);
            if dry_run {
                match &url {
                    Some(url) => format!("Would open browser {slug} at {url}"),
                    None => format!("Would open browser {slug}"),
                }
            } else {
                apply_browser_ensure(orch, &resource, url)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Replace,
        ) => {
            // `replace` is the agent-intuitive "go to a URL / set the page":
            // an idempotent ensure + navigate, identical to `patch` with a url
            // but with the url required. Missing url is a hard error with guidance.
            let url = item
                .payload
                .as_ref()
                .and_then(|payload| payload_trimmed_non_empty_str(payload, "url", &["navigate"]))
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "mode=replace sets the page URL; provide payload.url",
                    )
                })?;
            if dry_run {
                format!("Would navigate browser {slug} to {url}")
            } else {
                apply_browser_ensure(orch, &resource, Some(url))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let url =
                payload_trimmed_non_empty_str(payload, "url", &["navigate"]).map(ToOwned::to_owned);
            let action =
                payload_trimmed_non_empty_str(payload, "action", &[]).map(ToOwned::to_owned);
            if url.is_none() && action.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload requires url/navigate or action (back|forward|reload|click|type|scroll|waitFor|waitForNavigation|waitForLoad)",
                ));
            }
            let args = BrowserInteractionArgs {
                selector: payload_trimmed_non_empty_str(payload, "selector", &[])
                    .map(ToOwned::to_owned),
                text: payload_trimmed_non_empty_str(payload, "text", &[]).map(ToOwned::to_owned),
                handle: payload_trimmed_non_empty_str(payload, "handle", &["ref"])
                    .map(ToOwned::to_owned),
                // value may legitimately be empty (clearing a field), so it is
                // not trimmed-non-empty filtered.
                value: payload_str(payload, "value", &[]).map(ToOwned::to_owned),
                to: payload_trimmed_non_empty_str(payload, "to", &[]).map(ToOwned::to_owned),
                by: payload.get("by").and_then(serde_json::Value::as_i64),
                timeout_ms: payload
                    .get("timeoutMs")
                    .or_else(|| payload.get("timeout_ms"))
                    .and_then(serde_json::Value::as_u64),
                submit: payload_bool(payload, "submit"),
                kinds: payload
                    .get("kinds")
                    .and_then(serde_json::Value::as_array)
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                            .collect()
                    }),
            };
            if dry_run {
                match (&url, &action) {
                    (Some(url), _) => format!("Would navigate browser {slug} to {url}"),
                    (None, Some(action)) => format!("Would {action} browser {slug}"),
                    (None, None) => unreachable!("guarded above"),
                }
            } else {
                match url {
                    // navigate (idempotent ensure) vs drive an action.
                    Some(url) => apply_browser_ensure(orch, &resource, Some(url)).await,
                    None => {
                        let action = action.expect("guarded: url or action is present");
                        apply_browser_action(orch, &resource, action, args).await
                    }
                }
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeBrowser { slug, .. }
            | CairnResource::TaskBrowser { slug, .. }
            | CairnResource::ProjectBrowser { slug, .. },
            ChangeMode::Delete,
        ) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            if dry_run {
                format!("Would close browser {slug}")
            } else {
                apply_browser_delete(orch, &resource)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::IssueExecutions { project, number }, ChangeMode::Append) => {
            let recipe = match item.payload.as_ref().and_then(|p| p.get("recipe")) {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.recipe must be a string")
                })?),
                None => None,
            };
            let backend = match item.payload.as_ref().and_then(|p| p.get("backend")) {
                Some(value) => Some(value.as_str().ok_or_else(|| {
                    build_failure(index, item, "payload.backend must be a string")
                })?),
                None => None,
            };
            if dry_run {
                format!(
                    "Would start an execution for {project}-{number}{}",
                    recipe
                        .map(|r| format!(" (recipe '{r}')"))
                        .unwrap_or_default()
                )
            } else {
                executions::start_execution_from_collection(orch, project, *number, recipe, backend)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::IssueExecution {
                project,
                number,
                exec_seq,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let agent = payload_non_empty_str(payload, "agent", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.agent is required and must be a non-empty string",
                )
            })?;
            let snapshot_patch = payload
                .get("snapshot")
                .ok_or_else(|| build_failure(index, item, "payload.snapshot is required"))?
                .clone();
            executions::edit_execution_agent(
                orch,
                request,
                project,
                *number,
                *exec_seq,
                agent,
                snapshot_patch,
                dry_run,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?
        }
        (CairnResource::IssueMessages { project, number }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars to issue channel {project}-{number}",
                    content.len()
                )
            } else {
                messages::append_project_or_issue_message(
                    orch,
                    request,
                    project,
                    Some(*number),
                    content,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::IssueComment {
                project,
                number,
                comment_seq,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!("Would edit comment {comment_seq} on issue {project}-{number}")
            } else {
                // Route to the owning project's database (CAIRN-2132); a local
                // project resolves to the private DB, a shared one to its replica.
                let db = orch.db.for_project(project).await;
                let comment_id =
                    resolve_issue_comment_id(&db, index, item, project, *number, *comment_seq)
                        .await?;
                crate::issues::comments::update(&db, &comment_id, content)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                format!("Edited comment {comment_seq} on issue {project}-{number}")
            }
        }
        (
            CairnResource::IssueComment {
                project,
                number,
                comment_seq,
            },
            ChangeMode::Delete,
        ) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            if dry_run {
                format!("Would delete comment {comment_seq} on issue {project}-{number}")
            } else {
                let db = orch.db.for_project(project).await;
                let comment_id =
                    resolve_issue_comment_id(&db, index, item, project, *number, *comment_seq)
                        .await?;
                crate::issues::comments::delete(&db, &comment_id)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?;
                format!("Deleted comment {comment_seq} on issue {project}-{number}")
            }
        }
        (
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: None,
                },
                dry_run,
            )
            .await?
        }
        (
            CairnResource::NodeMessages {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            // Canonical node messaging target — identical delivery to the
            // bare-node append, which remains a backward-compatible alias.
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: None,
                },
                dry_run,
            )
            .await?
        }
        (
            CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Patch,
        ) => {
            // A bare-node patch carries an `action`. `stop` interrupts the
            // node's active turn and parks the session warm (resumable, not a
            // kill), so it works on ANY running node and branches BEFORE the PR
            // gate. merge/close/refresh operate on the PR a `pr` action node
            // owns (the action analogue of the NodeArtifact action patch,
            // CAIRN-1222) and stay behind the merge_requests gate.
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let action = payload_str(payload, "action", &[]).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    "payload.action must be a string (stop|merge|close|refresh)",
                )
            })?;
            let owner_id = crate::resources::resolve_node_owner_id(
                &orch.db.local,
                project,
                *number,
                *exec_seq,
                node_id,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if action == "stop" {
                // Interrupt the node's live run. `stop_session` cascades to
                // child runs and parks the session warm rather than killing it,
                // so the node can be resumed by a later message.
                match crate::orchestrator::lifecycle::live_run_id_for_job(orch, &owner_id) {
                    Some(run_id) => {
                        if dry_run {
                            format!(
                                "Would stop {node_id}: interrupt run {run_id}'s active turn and park the session warm (resumable)"
                            )
                        } else {
                            crate::orchestrator::lifecycle::stop_session(orch, &run_id)
                                .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Stopped {node_id}: interrupted run {run_id}'s active turn and parked the session warm (resumable; cascades to child runs)"
                            )
                        }
                    }
                    None => {
                        // No live run attached. A job can still be non-terminal yet
                        // runless when it suspended on a foreground question or an
                        // inline delegated task and its run finalized (the
                        // OpenRouter owned loop keeps no warm process). Idle it at
                        // the job level (CAIRN-1907): cancel the open prompt, drop
                        // the pending successor, cascade-stop children, and recompute
                        // to a steerable state. A genuinely terminal job has nothing
                        // to stop.
                        let job = crate::jobs::queries::get_job(&orch.db.local, &owner_id)
                            .await
                            .map_err(|error| build_failure(index, item, error.to_string()))?;
                        if job.status.is_terminal() {
                            format!("node {node_id} has no active run to stop")
                        } else if dry_run {
                            format!(
                                "Would stop {node_id}: cancel its pending prompt, drop the pending successor, stop child runs, and idle the job (no live run attached)"
                            )
                        } else {
                            crate::orchestrator::lifecycle::stop_job(orch, &owner_id)
                                .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Stopped {node_id}: cancelled pending input, stopped child runs, and idled the job (steerable for a follow-up)"
                            )
                        }
                    }
                }
            } else {
                let mr_context = crate::pr_data::actions::try_resolve_mr_context_for_job(
                    &orch.db.local,
                    &owner_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let Some(mr_context) = mr_context else {
                    return Err(build_failure(
                        index,
                        item,
                        format!(
                            "node {node_id} has no PR yet; merge/close/refresh require a merge_requests row for the node"
                        ),
                    ));
                };
                // Drive merge/close/refresh through the PR's TRUE owner id
                // (merge_requests.job_id). For a first-class `pr` node that is
                // the producing action_run, not this node's job — passing the
                // wrong id misses the owner-keyed action_run and reports "no PR"
                // (CAIRN-2287). Shadowing keeps the match arms below unchanged.
                let owner_id = mr_context.job_id;
                match action {
                    "merge" => {
                        let method =
                            payload_str(payload, "method", &[]).map(|value| value.to_string());
                        if dry_run {
                            let suffix = method
                                .as_deref()
                                .map(|m| format!(" (method={m})"))
                                .unwrap_or_default();
                            format!("Would merge PR for {node_id}{suffix}")
                        } else {
                            crate::pr_data::actions::merge_pr_for_job(orch, &owner_id, method)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "close" => {
                        if dry_run {
                            format!("Would close PR for {node_id}")
                        } else {
                            crate::pr_data::actions::close_pr_for_job(orch, &owner_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "refresh" => {
                        if dry_run {
                            format!("Would refresh PR for {node_id}")
                        } else {
                            let cache =
                                crate::pr_data::actions::refresh_pr_for_job(orch, &owner_id)
                                    .await
                                    .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Refreshed PR #{} for {node_id} (state {}, +{} -{})",
                                cache.pr_number,
                                cache.state,
                                cache.additions.unwrap_or(0),
                                cache.deletions.unwrap_or(0)
                            )
                        }
                    }
                    other => {
                        return Err(build_failure(
                            index,
                            item,
                            format!(
                                "unknown node action '{other}'; expected stop|merge|close|refresh"
                            ),
                        ))
                    }
                }
            }
        }
        (
            CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            ChangeMode::Append,
        ) => {
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: Some(task_name),
                },
                dry_run,
            )
            .await?
        }
        (
            CairnResource::TaskMessages {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            ChangeMode::Append,
        ) => {
            // Canonical sub-task messaging target — identical delivery to the
            // bare-task append, which remains a backward-compatible alias.
            append_node_or_task_message(
                orch,
                request,
                index,
                item,
                DirectMessageTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    task_name: Some(task_name),
                },
                dry_run,
            )
            .await?
        }
        (CairnResource::Issue { project, number }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            if let Some(title) = payload.get("title") {
                if !title.is_string() {
                    return Err(build_failure(index, item, "payload.title must be a string"));
                }
            }
            if let Some(description) = payload.get("description") {
                if !description.is_string() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.description must be a string",
                    ));
                }
            }
            let depends_on =
                parse_string_array_field(index, item, payload, "depends_on", &["dependsOn"])?;
            let labels = parse_string_array_field(index, item, payload, "labels", &[])?;
            // `status` is the resolution the UI's IssueMenu sets. Only the two
            // settable resolutions are accepted here: `backlog` is a derived
            // state, not a value a user sets, so it is rejected like any other.
            let status = match payload.get("status") {
                None | Some(serde_json::Value::Null) => None,
                Some(value) => {
                    let status = value.as_str().ok_or_else(|| {
                        build_failure(index, item, "payload.status must be a string")
                    })?;
                    if !matches!(status, "merged" | "closed") {
                        return Err(build_failure(
                            index,
                            item,
                            format!("Invalid status '{status}'. Allowed values: merged, closed"),
                        ));
                    }
                    Some(status.to_string())
                }
            };
            // Re-parenting: absent leaves the parent untouched, null/empty
            // orphans the issue, a string adopts it under that canonical issue
            // URI. Existence, same-project, and cycle checks happen in the txn.
            let parent = match payload.get("parent") {
                None => None,
                Some(serde_json::Value::Null) => Some(None),
                Some(value) => {
                    let raw = value.as_str().ok_or_else(|| {
                        build_failure(
                            index,
                            item,
                            "payload.parent must be an issue URI string or null",
                        )
                    })?;
                    if raw.trim().is_empty() {
                        Some(None)
                    } else {
                        let canonical = crate::issues::relations::canonicalize_issue_uri(raw)
                            .map_err(|e| build_failure(index, item, e))?;
                        Some(Some(canonical))
                    }
                }
            };
            if dry_run {
                let mut details = Vec::new();
                if let Some(status) = status.as_deref() {
                    details.push(format!("status={status}"));
                }
                match &parent {
                    None => {}
                    Some(None) => details.push("parent=cleared".to_string()),
                    Some(Some(uri)) => details.push(format!("parent={uri}")),
                }
                if details.is_empty() {
                    format!("Would patch issue {project}-{number}")
                } else {
                    format!(
                        "Would patch issue {project}-{number} ({})",
                        details.join(", ")
                    )
                }
            } else {
                let title = payload_str(payload, "title", &[]).map(ToOwned::to_owned);
                let description = payload_str(payload, "description", &[]).map(ToOwned::to_owned);
                issues::update_issue_by_project_number(
                    orch,
                    request,
                    project,
                    *number,
                    issues::IssuePatchFields {
                        title,
                        description,
                        depends_on,
                        labels,
                        status,
                        parent,
                    },
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Delete) => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            // Resolve against the owning DB (CAIRN-2181): a team project's issue
            // row lives in its team replica, so the lookup must route there or a
            // team-project delete would falsely report "not found".
            let owning_db = orch.db.for_project(project).await;
            let issue_id =
                crate::issues::relations::issue_id_for_project_number(&owning_db, project, *number)
                    .await
                    .map_err(|error| build_failure(index, item, error.to_string()))?
                    .ok_or_else(|| {
                        build_failure(index, item, format!("Issue {project}-{number} not found"))
                    })?;
            if dry_run {
                format!("Would delete issue {project}-{number}")
            } else {
                crate::issues::delete::delete_issue(orch, &issue_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                format!("Deleted issue {project}-{number}")
            }
        }
        (CairnResource::Issue { project, number }, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let content = payload_non_empty_str(payload, "content", &[])
                .ok_or_else(|| build_failure(index, item, "payload.content is required"))?;
            if dry_run {
                format!(
                    "Would append {} chars as a comment to issue {project}-{number}",
                    content.len()
                )
            } else {
                comments_artifacts::append_issue_comment(orch, request, project, *number, content)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Skills, ChangeMode::Create) => {
            if dry_run {
                preview_skill_create(index, item, "workspace")?
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_skill_create(orch, request, payload, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectSkills { project }, ChangeMode::Create) => {
            if dry_run {
                preview_skill_create(index, item, project)?
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_skill_create(orch, request, payload, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectReferences { project }, ChangeMode::Create) => {
            if dry_run {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                let name = payload_non_empty_str(payload, "name", &[])
                    .ok_or_else(|| build_failure(index, item, "payload.name is required"))?;
                format!("Would create project reference '{project}/{name}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_project_reference_create(orch, project, payload, false)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectReference { project, name }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch project reference '{project}/{name}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_project_reference_patch(orch, project, name, payload, false)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectReference { project, name }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete project reference '{project}/{name}'")
            } else {
                apply_project_reference_delete(orch, project, name, false)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Skill { skill_id, path }, ChangeMode::Patch) => {
            require_skill_root(index, item, path)?;
            if dry_run {
                format!("Would patch skill '{skill_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_skill_patch(orch, request, payload, skill_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::ProjectSkill {
                project,
                skill_id,
                path,
            },
            ChangeMode::Patch,
        ) => {
            require_skill_root(index, item, path)?;
            if dry_run {
                format!("Would patch project skill '{project}/{skill_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_skill_patch(orch, request, payload, skill_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Skill { skill_id, path }, ChangeMode::Delete) => {
            require_skill_root(index, item, path)?;
            if dry_run {
                format!("Would delete skill '{skill_id}'")
            } else {
                apply_skill_delete(orch, request, item.payload.as_ref(), skill_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::ProjectSkill {
                project,
                skill_id,
                path,
            },
            ChangeMode::Delete,
        ) => {
            require_skill_root(index, item, path)?;
            if dry_run {
                format!("Would delete project skill '{project}/{skill_id}'")
            } else {
                apply_skill_delete(
                    orch,
                    request,
                    item.payload.as_ref(),
                    skill_id,
                    Some(project),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeMemories {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            if dry_run {
                format!("Would append draft memory for node {node_id}")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=append requires payload"))?;
                apply_node_memory_append(
                    orch,
                    request,
                    payload,
                    MemoryCreateTarget {
                        project,
                        number: *number,
                        exec_seq: *exec_seq,
                        node_id,
                    },
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeMemory {
                project,
                number,
                exec_seq,
                node_id,
                memory_seq,
            },
            ChangeMode::Patch,
        ) => {
            if dry_run {
                format!("Would patch node memory {node_id}/{memory_seq}")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                let target = MemoryResourceTarget {
                    project,
                    number: *number,
                    exec_seq: *exec_seq,
                    node_id,
                    memory_seq: *memory_seq,
                };
                if payload.get("action").is_some() {
                    let (summary, promoted) = apply_memory_triage_action(orch, payload, target)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    promoted_memory = promoted;
                    summary
                } else {
                    apply_node_memory_patch(orch, payload, target)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                }
            }
        }
        (
            CairnResource::NodeMemory {
                project,
                number,
                exec_seq,
                node_id,
                memory_seq,
            },
            ChangeMode::Delete,
        ) => {
            if dry_run {
                format!("Would delete node memory {node_id}/{memory_seq}")
            } else {
                apply_node_memory_delete(
                    orch,
                    MemoryResourceTarget {
                        project,
                        number: *number,
                        exec_seq: *exec_seq,
                        node_id,
                        memory_seq: *memory_seq,
                    },
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Labels, ChangeMode::Create) => {
            if dry_run {
                "Would create workspace label".to_string()
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_label_create(orch, payload)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Label { label_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch label '{label_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_label_patch(orch, payload, label_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Label { label_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete label '{label_id}'")
            } else {
                apply_label_delete(orch, label_id)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::JobTodos {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            },
            mode @ (ChangeMode::Replace | ChangeMode::Append | ChangeMode::Patch),
        ) => {
            let payload = item.payload.as_ref().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires payload", mode_name(mode)),
                )
            })?;
            let label = match task_name.as_deref() {
                Some(task_name) => format!("{}/task/{}", node_id, task_name),
                None => node_id.clone(),
            };
            if dry_run {
                match mode {
                    ChangeMode::Patch => {
                        let updates = parse_todo_update_items(index, item, payload)?;
                        format!("Would patch {} todos for {}", updates.len(), label)
                    }
                    ChangeMode::Append => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        format!("Would append {} todos for {}", items.len(), label)
                    }
                    _ => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        format!(
                            "Would replace todos for {} with {} items",
                            label,
                            items.len()
                        )
                    }
                }
            } else {
                // Route todos writes to the owning project's database (CAIRN-2132);
                // resolution and all three mutations share the routed handle.
                let db = orch.db.for_project(project).await;
                let job_id = crate::resources::resolve_todos_job_id(
                    &db,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    task_name.as_deref(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;

                let summary = match mode {
                    ChangeMode::Replace => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        let todos = crate::todos::replace_todos(&db, &job_id, &items)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                        let completed = todos.iter().filter(|t| t.status == "completed").count();
                        applied_data = Some(serde_json::to_value(&todos).unwrap_or_default());
                        format!(
                            "Replaced {} todos for {} ({} completed)\n{}",
                            todos.len(),
                            label,
                            completed,
                            crate::todos::format_todos_compact(&todos)
                        )
                    }
                    ChangeMode::Append => {
                        let items = parse_todo_write_items(index, item, payload)?;
                        let appended = items.len();
                        let todos = crate::todos::append_todos(&db, &job_id, &items)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                        applied_data = Some(serde_json::to_value(&todos).unwrap_or_default());
                        format!(
                            "Appended {} todos for {} ({} total)\n{}",
                            appended,
                            label,
                            todos.len(),
                            crate::todos::format_todos_compact(&todos)
                        )
                    }
                    ChangeMode::Patch => {
                        let updates = parse_todo_update_items(index, item, payload)?;
                        let patched = updates.len();
                        let todos = crate::todos::update_todos(&db, &job_id, &updates)
                            .await
                            .map_err(|error| build_failure(index, item, error))?;
                        let completed = todos.iter().filter(|t| t.status == "completed").count();
                        applied_data = Some(serde_json::to_value(&todos).unwrap_or_default());
                        format!(
                            "Patched {} todos for {} ({}/{} completed)\n{}",
                            patched,
                            label,
                            completed,
                            todos.len(),
                            crate::todos::format_todos_compact(&todos)
                        )
                    }
                    _ => unreachable!("mode guarded by match pattern"),
                };

                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "todos", "action": mode_name(mode)}),
                );

                summary
            }
        }
        (
            CairnResource::NodeArtifact {
                project,
                number,
                exec_seq,
                node_id,
                name,
            },
            mode @ (ChangeMode::Create | ChangeMode::Patch),
        ) => {
            let payload = item.payload.as_ref().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires an artifact payload", mode_name(mode)),
                )
            })?;
            let artifact_label = name.as_deref().unwrap_or("artifact");
            // A `patch` carrying the reserved `confirmed` key resolves the user
            // gate (confirm the latest artifact + advance the DAG) instead of
            // editing artifact data. `confirmed` is a column, never a real
            // artifact schema field, so treating it as reserved can't collide
            // with a data edit.
            if matches!(mode, ChangeMode::Patch) && payload.get("action").is_some() {
                // A `patch` carrying the reserved `action` key operates on the
                // PR that this artifact's job produced (merge/close/refresh),
                // rather than editing artifact data. PR-ness is detected at
                // runtime by a merge_requests row for the job — no new URI or
                // ChangeMode. `action` and `confirmed` are mutually exclusive.
                if payload.get("confirmed").is_some() {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.action and payload.confirmed are mutually exclusive",
                    ));
                }
                let action = payload_str(payload, "action", &[]).ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.action must be a string (merge|close|refresh)",
                    )
                })?;
                let job_id = crate::resources::resolve_todos_job_id(
                    &orch.db.local,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    None,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let mr_context = crate::pr_data::actions::try_resolve_mr_context_for_job(
                    &orch.db.local,
                    &job_id,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let Some(mr_context) = mr_context else {
                    return Err(build_failure(
                        index,
                        item,
                        format!(
                            "artifact {node_id}/{artifact_label} has no PR yet; merge/close/refresh require a merge_requests row for the producing job"
                        ),
                    ));
                };
                // Merge/close/refresh through the PR's TRUE owner id
                // (merge_requests.job_id). A build recipe owns the child PR on a
                // `pr` action_run, so the create-pr artifact's builder job id is
                // NOT the owner; resolving it here (via the snapshot-walk
                // fallback) and using mr_context.job_id keeps the owner-keyed
                // resolution correct (CAIRN-2287). Shadow to keep the arms below.
                let job_id = mr_context.job_id;
                match action {
                    "merge" => {
                        let method =
                            payload_str(payload, "method", &[]).map(|value| value.to_string());
                        if dry_run {
                            let suffix = method
                                .as_deref()
                                .map(|m| format!(" (method={m})"))
                                .unwrap_or_default();
                            format!("Would merge PR for {node_id}/{artifact_label}{suffix}")
                        } else {
                            crate::pr_data::actions::merge_pr_for_job(orch, &job_id, method)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "close" => {
                        if dry_run {
                            format!("Would close PR for {node_id}/{artifact_label}")
                        } else {
                            crate::pr_data::actions::close_pr_for_job(orch, &job_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?
                        }
                    }
                    "refresh" => {
                        if dry_run {
                            format!("Would refresh PR for {node_id}/{artifact_label}")
                        } else {
                            let cache = crate::pr_data::actions::refresh_pr_for_job(orch, &job_id)
                                .await
                                .map_err(|error| build_failure(index, item, error))?;
                            format!(
                                "Refreshed PR #{} for {node_id}/{artifact_label} (state {}, +{} -{})",
                                cache.pr_number,
                                cache.state,
                                cache.additions.unwrap_or(0),
                                cache.deletions.unwrap_or(0)
                            )
                        }
                    }
                    other => {
                        return Err(build_failure(
                            index,
                            item,
                            format!("unknown PR action '{other}'; expected merge|close|refresh"),
                        ))
                    }
                }
            } else if matches!(mode, ChangeMode::Patch) && payload.get("confirmed").is_some() {
                let confirmed = payload
                    .get("confirmed")
                    .and_then(|value| value.as_bool())
                    .ok_or_else(|| {
                        build_failure(index, item, "payload.confirmed must be a boolean")
                    })?;
                if !confirmed {
                    return Err(build_failure(
                        index,
                        item,
                        "payload.confirmed must be true to confirm a gated artifact; there is no 'unconfirm' (omit the key to edit artifact data)",
                    ));
                }
                if dry_run {
                    format!("Would confirm artifact {node_id}/{artifact_label}")
                } else {
                    let job_id = crate::resources::resolve_todos_job_id(
                        &orch.db.local,
                        project,
                        *number,
                        *exec_seq,
                        node_id,
                        None,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    crate::execution::checkpoints::approve_job_inner(orch, &job_id)
                        .await
                        .map_err(|error| build_failure(index, item, error))?;
                    format!("Confirmed artifact {node_id}/{artifact_label}; gate resolved")
                }
            } else if dry_run {
                format!(
                    "Would {} artifact {node_id}/{artifact_label}",
                    mode_name(mode)
                )
            } else {
                comments_artifacts::write_artifact_change(
                    orch,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    None,
                    name.as_deref(),
                    payload,
                    matches!(mode, ChangeMode::Patch),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::TaskArtifact {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                name,
            },
            mode @ (ChangeMode::Create | ChangeMode::Patch),
        ) => {
            let payload = item.payload.as_ref().ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!("mode={} requires an artifact payload", mode_name(mode)),
                )
            })?;
            if dry_run {
                format!(
                    "Would {} artifact {}/task/{}/{}",
                    mode_name(mode),
                    node_id,
                    task_name,
                    name.as_deref().unwrap_or("artifact")
                )
            } else {
                comments_artifacts::write_artifact_change(
                    orch,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    Some(task_name),
                    name.as_deref(),
                    payload,
                    matches!(mode, ChangeMode::Patch),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Bug, ChangeMode::Append) => {
            let payload = append_payload(index, item)?;
            let title = payload_non_empty_str(payload, "title", &[])
                .ok_or_else(|| build_failure(index, item, "payload.title is required"))?;
            if dry_run {
                format!("Would submit bug report: {title}")
            } else {
                bug_report::submit_bug_report(orch, request, payload)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (
            CairnResource::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "wakes append requires payload"))?;
            let job_id = crate::resources::node::resolve_todos_job_id(
                &orch.db.local,
                project,
                *number,
                *exec_seq,
                node_id,
                None,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            let created_by = if request.run_id.is_some() {
                "agent"
            } else {
                "user"
            };
            if let Some(value) = payload.get("subscribe") {
                let filter = parse_wake_filter(index, item, value, "subscribe")?;
                if filter.kind == "terminal" {
                    subscribe_terminal_wake(
                        orch, index, item, &job_id, &filter, created_by, dry_run, project, *number,
                        *exec_seq, node_id,
                    )
                    .await?
                } else if dry_run {
                    format!(
                        "Would subscribe wake: {} {:?}",
                        filter.kind, filter.reference
                    )
                } else {
                    let sub = crate::orchestrator::wakes::subscribe(
                        &orch.db.local,
                        &job_id,
                        &filter.kind,
                        filter.reference.as_deref(),
                        filter.fact_kinds.as_deref(),
                        created_by,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    format!(
                        "Subscribed wake {} {}",
                        sub.source_kind,
                        sub.source_ref.unwrap_or_else(|| "*".to_string())
                    )
                }
            } else if let Some(value) = payload.get("mute") {
                let filter = parse_wake_filter(index, item, value, "mute")?;
                let until = payload
                    .get("until")
                    .map(|value| parse_wake_filter(index, item, value, "until"))
                    .transpose()?;
                if dry_run {
                    format!("Would mute wake: {} {:?}", filter.kind, filter.reference)
                } else {
                    let sub = crate::orchestrator::wakes::mute(
                        &orch.db.local,
                        &job_id,
                        &filter.kind,
                        filter.reference.as_deref(),
                        filter.fact_kinds.as_deref(),
                        until.as_ref().map(|until| until.kind.as_str()),
                        until.as_ref().and_then(|until| until.reference.as_deref()),
                        created_by,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    format!(
                        "Muted wake {} {}",
                        sub.source_kind,
                        sub.source_ref.unwrap_or_else(|| "*".to_string())
                    )
                }
            } else {
                return Err(build_failure(
                    index,
                    item,
                    "payload.subscribe or payload.mute is required",
                ));
            }
        }
        (
            CairnResource::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "wakes patch requires payload"))?;
            let value = payload
                .get("unmute")
                .ok_or_else(|| build_failure(index, item, "payload.unmute is required"))?;
            let filter = parse_wake_filter(index, item, value, "unmute")?;
            let job_id = crate::resources::node::resolve_todos_job_id(
                &orch.db.local,
                project,
                *number,
                *exec_seq,
                node_id,
                None,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                format!("Would unmute wake: {} {:?}", filter.kind, filter.reference)
            } else {
                let count = crate::orchestrator::wakes::unmute_matching(
                    &orch.db.local,
                    &job_id,
                    &filter.kind,
                    filter.reference.as_deref(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                format!("Unmuted {count} wake subscription(s)")
            }
        }
        (
            CairnResource::NodeWakes {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Delete,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "wakes delete requires payload"))?;
            let value = payload
                .get("unsubscribe")
                .ok_or_else(|| build_failure(index, item, "payload.unsubscribe is required"))?;
            let filter = parse_wake_filter(index, item, value, "unsubscribe")?;
            let job_id = crate::resources::node::resolve_todos_job_id(
                &orch.db.local,
                project,
                *number,
                *exec_seq,
                node_id,
                None,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            if dry_run {
                format!(
                    "Would unsubscribe wake: {} {:?}",
                    filter.kind, filter.reference
                )
            } else {
                let count = crate::orchestrator::wakes::unsubscribe_matching(
                    &orch.db.local,
                    &job_id,
                    &filter.kind,
                    filter.reference.as_deref(),
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                format!("Unsubscribed {count} wake subscription(s)")
            }
        }
        (CairnResource::NodeTasks { .. }, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "task append requires payload"))?;
            let subagent = payload_non_empty_str(payload, "subagentType", &["subagent_type"])
                .ok_or_else(|| build_failure(index, item, "payload.subagentType is required"))?;
            if dry_run {
                format!("Would spawn task: {subagent}")
            } else {
                // Apply routes task appends through the blocking group before reaching
                // dispatch; arriving here means the caller bypassed that path.
                return Err(build_failure(
                    index,
                    item,
                    "internal: task append must run through the blocking group, not dispatch",
                ));
            }
        }
        (
            CairnResource::NodeQuestion {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            },
            ChangeMode::Patch | ChangeMode::Append,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "question answer requires payload"))?;
            if payload.get("answer").is_none() && payload.get("answers").is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload.answer or payload.answers is required",
                ));
            }
            if dry_run {
                format!(
                    "Would answer question {} for {}-{}/{}/{}",
                    segment, project, number, exec_seq, node_id
                )
            } else {
                let outcome = planning::answer_node_question(
                    orch, project, *number, *exec_seq, node_id, segment, payload,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if outcome.duplicate {
                    format!("Question {} was already answered", segment)
                } else {
                    format!("Answered question {}", segment)
                }
            }
        }
        (
            CairnResource::NodePermission {
                project,
                number,
                exec_seq,
                node_id,
                segment,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "permission answer requires payload"))?;
            let decision_str = payload_str(payload, "decision", &[]).ok_or_else(|| {
                build_failure(index, item, "payload.decision is required (allow|deny)")
            })?;
            let decision = match decision_str {
                "allow" => crate::mcp::handlers::permission::PermissionDecision::Allow,
                "deny" => crate::mcp::handlers::permission::PermissionDecision::Deny,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid decision '{other}'; expected allow or deny"),
                    ))
                }
            };
            let scope = match payload_str(payload, "scope", &[]).unwrap_or("once") {
                "once" => crate::mcp::handlers::permission::PermissionScope::Once,
                "session" => crate::mcp::handlers::permission::PermissionScope::Session,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid scope '{other}'; expected once or session"),
                    ))
                }
            };
            if dry_run {
                format!(
                    "Would answer permission {} for {}-{}/{}/{}",
                    segment, project, number, exec_seq, node_id
                )
            } else {
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch, project, *number, *exec_seq, node_id, segment, decision, scope,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if outcome.duplicate {
                    format!("Permission {} was already answered", segment)
                } else {
                    format!("Answered permission {}: {}", segment, decision_str)
                }
            }
        }
        (
            CairnResource::TaskPermission {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
                segment,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "permission answer requires payload"))?;
            let decision_str = payload_str(payload, "decision", &[]).ok_or_else(|| {
                build_failure(index, item, "payload.decision is required (allow|deny)")
            })?;
            let decision = match decision_str {
                "allow" => crate::mcp::handlers::permission::PermissionDecision::Allow,
                "deny" => crate::mcp::handlers::permission::PermissionDecision::Deny,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid decision '{other}'; expected allow or deny"),
                    ))
                }
            };
            let scope = match payload_str(payload, "scope", &[]).unwrap_or("once") {
                "once" => crate::mcp::handlers::permission::PermissionScope::Once,
                "session" => crate::mcp::handlers::permission::PermissionScope::Session,
                other => {
                    return Err(build_failure(
                        index,
                        item,
                        format!("invalid scope '{other}'; expected once or session"),
                    ))
                }
            };
            if dry_run {
                format!(
                    "Would answer permission {} for {}-{}/{}/{}/task/{}",
                    segment, project, number, exec_seq, node_id, task_name
                )
            } else {
                // The permission resource keys on the OWNING job's own
                // `uri_segment`; for a sub-agent task that is the task segment,
                // so the task name addresses the request directly (issue #143).
                let outcome = crate::mcp::handlers::permission::answer_node_permission(
                    orch, project, *number, *exec_seq, task_name, segment, decision, scope,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                if outcome.duplicate {
                    format!("Permission {} was already answered", segment)
                } else {
                    format!("Answered permission {}: {}", segment, decision_str)
                }
            }
        }
        (CairnResource::NodeQuestions { .. }, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "question append requires payload"))?;
            let questions = payload
                .get("questions")
                .and_then(|value| value.as_array())
                .ok_or_else(|| build_failure(index, item, "payload.questions must be an array"))?;
            if dry_run {
                format!("Would ask {} question(s)", questions.len())
            } else {
                return Err(build_failure(
                    index,
                    item,
                    "internal: question append must run through the blocking group, not dispatch",
                ));
            }
        }
        (CairnResource::Recipes, ChangeMode::Create) => {
            if dry_run {
                "Would create workspace recipe".to_string()
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_recipe_create(orch, request, payload, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectRecipes { project }, ChangeMode::Create) => {
            if dry_run {
                format!("Would create project recipe in {project}")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_recipe_create(orch, request, payload, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Recipe { recipe_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch recipe '{recipe_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_recipe_patch(orch, request, payload, recipe_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectRecipe { project, recipe_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch project recipe '{project}/{recipe_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_recipe_patch(orch, request, payload, recipe_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Recipe { recipe_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete recipe '{recipe_id}'")
            } else {
                apply_recipe_delete(orch, request, recipe_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectRecipe { project, recipe_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete project recipe '{project}/{recipe_id}'")
            } else {
                apply_recipe_delete(orch, request, recipe_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Agents, ChangeMode::Create) => {
            if dry_run {
                "Would create workspace agent".to_string()
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_agent_create(orch, request, payload, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectAgents { project }, ChangeMode::Create) => {
            if dry_run {
                format!("Would create project agent in {project}")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_agent_create(orch, request, payload, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Agent { agent_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch agent '{agent_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_agent_patch(orch, request, payload, agent_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectAgent { project, agent_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch project agent '{project}/{agent_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_agent_patch(orch, request, payload, agent_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Agent { agent_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete agent '{agent_id}'")
            } else {
                apply_agent_delete(orch, request, agent_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectAgent { project, agent_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete project agent '{project}/{agent_id}'")
            } else {
                apply_agent_delete(orch, request, agent_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Actions, ChangeMode::Create) => {
            if dry_run {
                "Would create workspace action".to_string()
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_action_create(orch, payload, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectActions { project }, ChangeMode::Create) => {
            if dry_run {
                format!("Would create project action in {project}")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
                apply_action_create(orch, payload, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Action { action_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch action '{action_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_action_patch(orch, payload, action_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectAction { project, action_id }, ChangeMode::Patch) => {
            if dry_run {
                format!("Would patch project action '{project}/{action_id}'")
            } else {
                let payload = item
                    .payload
                    .as_ref()
                    .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
                apply_action_patch(orch, payload, action_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Action { action_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete action '{action_id}'")
            } else {
                apply_action_delete(orch, action_id, None)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::ProjectAction { project, action_id }, ChangeMode::Delete) => {
            if dry_run {
                format!("Would delete project action '{project}/{action_id}'")
            } else {
                apply_action_delete(orch, action_id, Some(project))
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        (CairnResource::Settings, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            apply_settings_patch(orch, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (CairnResource::Projects, ChangeMode::Create) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            apply_projects_create(orch, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (CairnResource::Project { project }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            apply_project_patch(orch, project, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (CairnResource::ProjectSettings { project }, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            apply_project_settings_patch(orch, project, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        // External MCP server registry (cairn://mcp write CRUD). create targets
        // the family root (cairn://mcp); patch/delete name one server
        // (cairn://mcp/<server>). Workspace-scope writes are routed through the
        // worktree fence in the change handler BEFORE this dispatch runs.
        (
            CairnResource::Mcp {
                server: None,
                resource: None,
            },
            ChangeMode::Create,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            apply_mcp_create(orch, request, payload, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (
            CairnResource::Mcp {
                server: Some(server),
                resource: None,
            },
            ChangeMode::Patch,
        ) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            apply_mcp_patch(orch, request, payload, server, dry_run)
                .await
                .map_err(|error| build_failure(index, item, error))?
        }
        (
            CairnResource::Mcp {
                server: Some(server),
                resource: None,
            },
            ChangeMode::Delete,
        ) => apply_mcp_delete(orch, request, item.payload.as_ref(), server, dry_run)
            .await
            .map_err(|error| build_failure(index, item, error))?,
        // Mcp shape mismatches: create must target cairn://mcp (no server);
        // patch/delete must name a server (cairn://mcp/<server>).
        (
            CairnResource::Mcp {
                server: Some(_),
                resource: None,
            },
            ChangeMode::Create,
        ) => {
            return Err(build_failure(
                index,
                item,
                "mode=create targets cairn://mcp and names the server in payload.name; it does not take a server in the URI",
            ));
        }
        (
            CairnResource::Mcp {
                server: None,
                resource: None,
            },
            ChangeMode::Patch | ChangeMode::Delete,
        ) => {
            return Err(build_failure(
                index,
                item,
                "patch/delete target one server: cairn://mcp/<server>",
            ));
        }
        (
            CairnResource::Mcp {
                resource: Some(_), ..
            },
            _,
        ) => {
            return Err(build_failure(
                index,
                item,
                "cairn://mcp/<server>/<tool-or-resource> is a tool/resource target (use run/read), not a registry write",
            ));
        }
        _ => {
            // The gate accepted this (kind, mode) from the table, but no arm
            // handles it. That is a table/dispatch parity bug, pinned by tests.
            return Err(build_failure(
                index,
                item,
                format!(
                    "internal: contract allows mode '{}' on this resource but no dispatch arm handles it",
                    mode_name(item.mode)
                ),
            ));
        }
    };

    // Browser writes can opt into an inline post-action page read via
    // `?return_content=true` on the target URI, halving the round-trip for a
    // "navigate/click then read". Browser-scoped and best-effort: the mutation
    // already applied, so a render failure rides along as appended text rather
    // than failing the change.
    let summary = if !dry_run
        && wants_return_content(&item.target)
        && matches!(
            resource,
            CairnResource::NodeBrowser { .. }
                | CairnResource::TaskBrowser { .. }
                | CairnResource::ProjectBrowser { .. }
        ) {
        let page = crate::resources::browsers::render_browser(
            orch,
            &resource,
            crate::browsers::BridgeFormat::Markdown,
        )
        .await;
        format!("{summary}\n\n{page}")
    } else {
        summary
    };

    if !dry_run {
        let resource_uri = resource.to_uri();
        if let Err(error) = crate::orchestrator::wakes::route_resource_updated(orch, &resource_uri)
        {
            log::warn!("failed to route resource wake for {resource_uri}: {error}");
        }
    }

    Ok(ResourceAppliedChange {
        index,
        target: item.target.clone(),
        mode: mode_name(item.mode).to_string(),
        kind: "resource".to_string(),
        summary,
        data: applied_data,
        promoted_memory,
    })
}

/// Whether a browser write target opted into an inline post-action page read
/// via `?return_content=true` on the target URI. The mutation path parses the
/// resource without its query string, so the flag is read off the raw target.
/// `return_content`, `return_content=true`, and `return_content=1` all enable it.
fn wants_return_content(target: &str) -> bool {
    target
        .split_once('?')
        .map(|(_, query)| {
            query.split('&').any(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                key == "return_content" && matches!(value, "" | "true" | "1")
            })
        })
        .unwrap_or(false)
}

/// Reject skill mutations that target a package sub-path (only the skill root is mutable).
fn require_skill_root(
    index: usize,
    item: &ChangeItem,
    path: &[String],
) -> ResourceMutationResult<()> {
    if path.is_empty() {
        Ok(())
    } else {
        Err(build_failure(
            index,
            item,
            "Skill mutations target the skill root (cairn://skills/ID); authoring package files is not supported",
        ))
    }
}

fn preview_skill_create(
    index: usize,
    item: &ChangeItem,
    scope: &str,
) -> ResourceMutationResult<String> {
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
    let name = payload_trimmed_non_empty_str(payload, "name", &[]).ok_or_else(|| {
        build_failure(
            index,
            item,
            "payload.name is required and must be non-empty",
        )
    })?;
    if payload_non_empty_str(payload, "description", &[]).is_none() {
        return Err(build_failure(
            index,
            item,
            "payload.description is required and must be non-empty",
        ));
    }
    if payload_str(payload, "prompt", &[]).is_none() {
        return Err(build_failure(index, item, "payload.prompt is required"));
    }
    Ok(format!("Would create {scope} skill '{name}'"))
}

#[cfg(test)]
mod resource_gate_tests {
    use super::*;

    fn item(target: &str, mode: ChangeMode, payload: Option<serde_json::Value>) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode,
            payload,
        }
    }

    fn gate(item: &ChangeItem) -> ResourceMutationResult<&'static MutationSpec> {
        let resource = cairn_common::uri::parse_uri(&item.target).unwrap();
        gate_resource_change(0, item, &resource)
    }

    #[test]
    fn gate_rejects_apply_mode_with_enumeration() {
        let it = item(
            "cairn://p/CAIRN/1/1/builder/chat/1/2",
            ChangeMode::Apply,
            None,
        );
        let failure = gate(&it).unwrap_err();
        assert!(failure.error.contains("Unsupported resource mutation"));
    }

    #[test]
    fn gate_rejects_unsupported_mode_and_lists_valid_ones() {
        // Issue supports patch/append/delete, not replace.
        let it = item("cairn://p/CAIRN/1", ChangeMode::Replace, None);
        let failure = gate(&it).unwrap_err();
        assert!(failure.error.contains("Unsupported resource mutation"));
        assert!(failure.error.contains("patch issue"));
        assert!(failure.error.contains("append comment"));
        assert!(failure.error.contains("delete issue"));
    }

    #[test]
    fn gate_marks_read_only_resource() {
        let it = item("cairn://p/CAIRN/1/1/builder/chat", ChangeMode::Append, None);
        let failure = gate(&it).unwrap_err();
        assert!(failure.error.contains("read-only"));
    }

    #[test]
    fn gate_names_missing_required_keys_with_example() {
        // Terminal create requires `command`.
        let it = item(
            "cairn://p/CAIRN/1/1/builder/terminal/dev",
            ChangeMode::Create,
            Some(serde_json::json!({ "description": "d" })),
        );
        let failure = gate(&it).unwrap_err();
        assert!(failure.error.contains("Missing required payload key"));
        assert!(failure.error.contains("command"));
        assert!(failure.error.contains("Example:"));
    }

    #[test]
    fn todo_append_mis_keyed_item_enumerates_accepted_keys() {
        // A todo item keyed `title` (the message/artifact spelling) instead of
        // `content` clears the top-level `todos` gate but fails item
        // deserialization. The rejection must name the accepted item keys so the
        // agent self-corrects without a discovery round-trip (CAIRN #164).
        let payload = serde_json::json!({ "todos": [{ "title": "do the thing" }] });
        let it = item("cairn:~/todos", ChangeMode::Append, Some(payload.clone()));
        let failure = parse_todo_write_items(0, &it, &payload).unwrap_err();
        assert!(
            failure.error.contains("content"),
            "rejection must name the canonical `content` key: {}",
            failure.error
        );
        assert!(failure.error.contains("status"));
        assert!(failure.error.contains("Each item accepts:"));
    }

    #[test]
    fn gate_accepts_alias_for_required_key() {
        // tasks append requires subagentType; the snake_case alias must satisfy it.
        let it = item(
            "cairn://p/CAIRN/1/1/builder/tasks",
            ChangeMode::Append,
            Some(
                serde_json::json!({ "subagent_type": "Explore", "description": "map parser flow" }),
            ),
        );
        assert!(gate(&it).is_ok());
    }

    #[test]
    fn gate_requires_task_description() {
        // tasks append requires both subagentType and description.
        let it = item(
            "cairn://p/CAIRN/1/1/builder/tasks",
            ChangeMode::Append,
            Some(serde_json::json!({ "subagentType": "Explore", "prompt": "do the thing" })),
        );
        let failure = gate(&it).unwrap_err();
        assert!(failure.error.contains("Missing required payload key"));
        assert!(failure.error.contains("description"));
    }

    #[test]
    fn gate_accepts_supported_mutation() {
        // The issues collection creates via `append`, not `create`.
        let it = item(
            "cairn://p/CAIRN/issues",
            ChangeMode::Append,
            Some(serde_json::json!({ "title": "hi" })),
        );
        let spec = gate(&it).unwrap();
        assert_eq!(spec.label, "create issue");
    }

    #[test]
    fn terminal_slug_from_ref_strips_prefixes() {
        assert_eq!(terminal_slug_from_ref("run-1"), "run-1");
        assert_eq!(terminal_slug_from_ref("cairn:~/terminal/run-1"), "run-1");
        assert_eq!(
            terminal_slug_from_ref("cairn://p/CAIRN/1/1/builder/terminal/run-1"),
            "run-1"
        );
        assert_eq!(terminal_slug_from_ref("  dev  "), "dev");
    }

    #[test]
    fn return_content_flag_parses_off_the_target_query() {
        assert!(wants_return_content("cairn:~/browser?return_content=true"));
        assert!(wants_return_content("cairn:~/browser?return_content=1"));
        assert!(wants_return_content("cairn:~/browser?return_content"));
        assert!(wants_return_content(
            "cairn:~/browser?format=markdown&return_content=true"
        ));
        // Absent, false, or a different key does not enable it.
        assert!(!wants_return_content("cairn:~/browser"));
        assert!(!wants_return_content(
            "cairn:~/browser?return_content=false"
        ));
        assert!(!wants_return_content("cairn:~/browser?other=true"));
    }

    #[test]
    fn parse_wake_filter_reads_terminal_output_on_and_phrase() {
        let it = item(
            "cairn://p/CAIRN/1/1/builder/wakes",
            ChangeMode::Append,
            None,
        );
        let value = serde_json::json!({
            "kind": "terminal",
            "ref": "cairn:~/terminal/dev",
            "on": "output",
            "phrase": "ready",
        });
        let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
        assert_eq!(filter.kind, "terminal");
        assert_eq!(filter.reference.as_deref(), Some("cairn:~/terminal/dev"));
        assert_eq!(filter.on.as_deref(), Some("output"));
        assert_eq!(filter.phrase.as_deref(), Some("ready"));
    }

    #[test]
    fn parse_wake_filter_defaults_output_fields_to_none() {
        let it = item(
            "cairn://p/CAIRN/1/1/builder/wakes",
            ChangeMode::Append,
            None,
        );
        let value = serde_json::json!({ "kind": "terminal", "ref": "cairn:~/terminal/dev" });
        let filter = parse_wake_filter(0, &it, &value, "subscribe").unwrap();
        assert!(
            filter.on.is_none(),
            "on defaults to exit semantics downstream"
        );
        assert!(filter.phrase.is_none());
    }

    #[test]
    fn gate_allows_payloadless_mutations() {
        // Issue patch has no required keys, but the arm still wants a payload;
        // the gate itself must not reject the missing payload.
        let it = item("cairn://p/CAIRN/1", ChangeMode::Patch, None);
        assert!(gate(&it).is_ok());
        // Terminal delete takes no payload and has no required keys.
        let it = item(
            "cairn://p/CAIRN/1/1/builder/terminal/dev",
            ChangeMode::Delete,
            None,
        );
        assert!(gate(&it).is_ok());
    }
}

#[cfg(test)]
mod issue_mutation_tests {
    use super::*;
    use crate::db::DbState;
    use crate::issues::comments;
    use crate::issues::crud as issue_crud;
    use crate::models::{CommentSource, CreateComment, CreateIssue, CreateProject, IssueStatus};
    use crate::orchestrator::OrchestratorBuilder;
    use crate::projects::crud as project_crud;
    use crate::services::testing::TestServicesBuilder;
    use crate::services::RealClock;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use std::sync::Arc;

    async fn seeded_orch() -> Orchestrator {
        let local = LocalDb::open(tempfile::tempdir().unwrap().keep().join("t.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&local)
            .await
            .unwrap();
        let search =
            Arc::new(SearchIndex::open_or_create(tempfile::tempdir().unwrap().keep()).unwrap());
        let db = Arc::new(DbState::new(Arc::new(local), search));
        OrchestratorBuilder::new(
            db,
            Arc::new(TestServicesBuilder::new().build()),
            tempfile::tempdir().unwrap().keep(),
        )
        .build()
    }

    /// Create a `CAIRN` project plus one issue; returns the issue id and number.
    async fn seed_issue(orch: &Orchestrator) -> (String, i32) {
        let clock = RealClock;
        let repo_path = tempfile::tempdir()
            .unwrap()
            .keep()
            .to_string_lossy()
            .to_string();
        let project = project_crud::create_db(
            &orch.db.local,
            &clock,
            &CreateProject {
                id: None,
                name: "Cairn".to_string(),
                key: "CAIRN".to_string(),
                repo_path,
                team_id: None,
            },
        )
        .await
        .unwrap();
        let issue = issue_crud::create(
            &orch.db.local,
            &clock,
            CreateIssue {
                project_id: project.id.clone(),
                title: "Test issue".to_string(),
                description: Some("body".to_string()),
                backend_override: None,
                label_ids: None,
            },
        )
        .await
        .unwrap();
        (issue.id, issue.number)
    }

    fn request() -> McpCallbackRequest {
        McpCallbackRequest {
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "change".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        }
    }

    async fn seed_comment(
        orch: &Orchestrator,
        issue_id: &str,
        content: &str,
    ) -> crate::models::Comment {
        comments::create(
            &orch.db.local,
            &RealClock,
            CreateComment {
                issue_id: issue_id.to_string(),
                content: content.to_string(),
                source: CommentSource::User,
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn comments_get_sequential_per_issue_seqs() {
        let orch = seeded_orch().await;
        let (issue_id, _number) = seed_issue(&orch).await;
        let c1 = seed_comment(&orch, &issue_id, "first").await;
        let c2 = seed_comment(&orch, &issue_id, "second").await;
        let c3 = seed_comment(&orch, &issue_id, "third").await;
        assert_eq!((c1.seq, c2.seq, c3.seq), (1, 2, 3));
    }

    #[tokio::test]
    async fn read_collection_lists_comments_with_seq_source_and_content() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let c1 = seed_comment(&orch, &issue_id, "first comment").await;
        let _c2 = seed_comment(&orch, &issue_id, "second comment").await;
        let rendered =
            crate::resources::issue::read_issue_comments(&orch.db.local, "CAIRN", number).await;
        assert!(rendered.contains("### comment 1"), "in: {rendered}");
        assert!(rendered.contains("### comment 2"), "in: {rendered}");
        assert!(rendered.contains("first comment"));
        assert!(rendered.contains("second comment"));
        assert!(rendered.contains("[user]"));
        assert!(rendered.contains("2 comment(s)"));
        // The raw UUID must NOT be surfaced as the comment identifier.
        assert!(!rendered.contains(&c1.id), "uuid leaked into: {rendered}");
        // Each comment surfaces its addressable member URI so edit/delete are
        // discoverable from the collection view.
        assert!(
            rendered.contains(&format!("cairn://p/CAIRN/{number}/comments/1")),
            "missing member URI in: {rendered}"
        );
        assert!(rendered.contains("edit/delete:"), "in: {rendered}");
    }

    #[test]
    fn comments_collection_affordance_advertises_edit_and_delete() {
        let block = crate::resources::common::affordance_for_kind(
            cairn_common::contract::ResourceKind::IssueComments,
        );
        assert!(block.contains("edit comment"), "block: {block}");
        assert!(block.contains("delete comment"), "block: {block}");
    }

    #[tokio::test]
    async fn edit_comment_by_seq_updates_only_that_comment() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let c1 = seed_comment(&orch, &issue_id, "first").await;
        let c2 = seed_comment(&orch, &issue_id, "second").await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/comments/{}", c1.seq),
            ChangeMode::Patch,
            Some(serde_json::json!({"content": "edited"})),
        );
        apply(&orch, &item).await.unwrap();
        let listed = comments::list(&orch.db.local, &issue_id).await.unwrap();
        assert_eq!(
            listed.iter().find(|c| c.id == c1.id).unwrap().content,
            "edited"
        );
        assert_eq!(
            listed.iter().find(|c| c.id == c2.id).unwrap().content,
            "second"
        );
    }

    #[tokio::test]
    async fn delete_comment_by_seq_removes_only_that_comment() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let c1 = seed_comment(&orch, &issue_id, "first").await;
        let c2 = seed_comment(&orch, &issue_id, "second").await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/comments/{}", c1.seq),
            ChangeMode::Delete,
            None,
        );
        apply(&orch, &item).await.unwrap();
        let listed = comments::list(&orch.db.local, &issue_id).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, c2.id);
    }

    #[tokio::test]
    async fn edit_missing_comment_seq_is_clean_not_found() {
        let orch = seeded_orch().await;
        let (_issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/comments/999"),
            ChangeMode::Patch,
            Some(serde_json::json!({"content": "edited"})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("not found"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn delete_missing_comment_seq_is_clean_not_found() {
        let orch = seeded_orch().await;
        let (_issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/comments/999"),
            ChangeMode::Delete,
            None,
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("not found"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn issue_uri_append_still_creates_a_comment() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Append,
            Some(serde_json::json!({"content": "a fresh comment"})),
        );
        apply(&orch, &item).await.unwrap();
        let listed = comments::list(&orch.db.local, &issue_id).await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].content, "a fresh comment");
    }

    fn change_item(
        target: &str,
        mode: ChangeMode,
        payload: Option<serde_json::Value>,
    ) -> ChangeItem {
        ChangeItem {
            target: target.to_string(),
            mode,
            payload,
        }
    }

    async fn apply(orch: &Orchestrator, item: &ChangeItem) -> ResourceMutationResult<String> {
        dispatch_resource_change(orch, &request(), 0, item, false)
            .await
            .map(|change| change.summary)
    }

    /// Apply a change as if it came from `run_id`, so re-parenting records the
    /// caller's root job in `parent_job_id`.
    async fn apply_as_run(
        orch: &Orchestrator,
        item: &ChangeItem,
        run_id: &str,
    ) -> ResourceMutationResult<String> {
        let req = McpCallbackRequest {
            cwd: "/tmp".to_string(),
            run_id: Some(run_id.to_string()),
            tool: "change".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        };
        dispatch_resource_change(orch, &req, 0, item, false)
            .await
            .map(|change| change.summary)
    }

    /// Create an extra issue in `project_id`; returns (issue id, number).
    async fn add_issue(orch: &Orchestrator, project_id: &str, title: &str) -> (String, i32) {
        let issue = issue_crud::create(
            &orch.db.local,
            &RealClock,
            CreateIssue {
                project_id: project_id.to_string(),
                title: title.to_string(),
                description: None,
                backend_override: None,
                label_ids: None,
            },
        )
        .await
        .unwrap();
        (issue.id, issue.number)
    }

    async fn project_id_of(orch: &Orchestrator, issue_id: &str) -> String {
        issue_crud::get(&orch.db.local, issue_id)
            .await
            .unwrap()
            .unwrap()
            .project_id
    }

    async fn parent_issue_id_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
        issue_crud::get(&orch.db.local, issue_id)
            .await
            .unwrap()
            .unwrap()
            .parent_issue_id
    }

    async fn parent_job_id_of(orch: &Orchestrator, issue_id: &str) -> Option<String> {
        let issue_id = issue_id.to_string();
        orch.db
            .local
            .read(move |conn| {
                let issue_id = issue_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT parent_job_id FROM issues WHERE id = ?1",
                            (issue_id.as_str(),),
                        )
                        .await?;
                    crate::storage::next_opt_text(&mut rows, 0).await
                })
            })
            .await
            .unwrap()
    }

    /// Run a SQL statement against the local db in a test.
    async fn exec_sql(orch: &Orchestrator, sql: String) {
        orch.db
            .local
            .write(move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    conn.execute(&sql, ()).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    /// Read a single run's status string.
    async fn run_status_for(orch: &Orchestrator, run_id: &str) -> Option<String> {
        let run_id = run_id.to_string();
        orch.db
            .local
            .read(move |conn| {
                let run_id = run_id.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT status FROM runs WHERE id = ?1", (run_id.as_str(),))
                        .await?;
                    crate::storage::next_opt_text(&mut rows, 0).await
                })
            })
            .await
            .unwrap()
    }

    /// Seed a `CAIRN` project + issue + execution (seq 1) + agent job
    /// (uri_segment `builder`) + a `live` run. Returns (issue number, job id,
    /// run id).
    async fn seed_running_node(orch: &Orchestrator) -> (i32, String, String) {
        let (issue_id, number) = seed_issue(orch).await;
        // The project created by seed_issue keys on CAIRN; recover its id from
        // the issue so the execution/job/run FKs all resolve.
        let issue_id_for_lookup = issue_id.clone();
        let project_id = orch
            .db
            .local
            .read(move |conn| {
                let issue_id = issue_id_for_lookup.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT project_id FROM issues WHERE id = ?1",
                            (issue_id.as_str(),),
                        )
                        .await?;
                    crate::storage::next_opt_text(&mut rows, 0).await
                })
            })
            .await
            .unwrap()
            .unwrap();

        exec_sql(
            orch,
            format!(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq) \
                 VALUES ('exec-stop', 'recipe', '{issue_id}', '{project_id}', 'running', 1, 1)"
            ),
        )
        .await;
        exec_sql(
            orch,
            format!(
                "INSERT INTO jobs(id, execution_id, issue_id, project_id, node_name, status, created_at, updated_at, uri_segment, worktree_path) \
                 VALUES ('job-stop', 'exec-stop', '{issue_id}', '{project_id}', 'Builder', 'running', 1, 1, 'builder', '/tmp/repo-builder')"
            ),
        )
        .await;
        exec_sql(
            orch,
            format!(
                "INSERT INTO runs(id, issue_id, project_id, job_id, status, created_at, updated_at) \
                 VALUES ('run-stop', '{issue_id}', '{project_id}', 'job-stop', 'live', 1, 1)"
            ),
        )
        .await;

        (number, "job-stop".to_string(), "run-stop".to_string())
    }

    #[tokio::test]
    async fn node_patch_stop_interrupts_live_run() {
        let orch = seeded_orch().await;
        let (number, _job_id, run_id) = seed_running_node(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/1/builder"),
            ChangeMode::Patch,
            Some(serde_json::json!({"action": "stop"})),
        );
        let summary = apply(&orch, &item).await.unwrap();
        assert!(
            summary.contains("Stopped") && summary.contains(&run_id),
            "got: {summary}"
        );
        // With no live backend process registered, stop's warm-park fallback
        // finalizes the stale live run off the active set, so it is no longer
        // 'live' — evidence the stop path actually ran against the resolved run.
        let status = run_status_for(&orch, &run_id).await;
        assert_ne!(
            status.as_deref(),
            Some("live"),
            "run should no longer be live"
        );
    }

    #[tokio::test]
    async fn node_patch_stop_without_live_run_idles_nonterminal_job() {
        // A non-terminal job with no live run (suspended/waiting — e.g. an
        // OpenRouter agent that finalized its run on a foreground question) is
        // idled at the job level rather than rejected (CAIRN-1907).
        let orch = seeded_orch().await;
        let (number, _job_id, run_id) = seed_running_node(&orch).await;
        // No live run: mark the only run exited first. The job stays 'running'.
        exec_sql(
            &orch,
            format!("UPDATE runs SET status = 'exited' WHERE id = '{run_id}'"),
        )
        .await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/1/builder"),
            ChangeMode::Patch,
            Some(serde_json::json!({"action": "stop"})),
        );
        let summary = apply(&orch, &item).await.unwrap();
        assert!(
            summary.contains("Stopped") && summary.contains("idled"),
            "got: {summary}"
        );
    }

    #[tokio::test]
    async fn node_patch_stop_terminal_job_reports_no_active_run() {
        // A genuinely terminal job with no live run has nothing to stop.
        let orch = seeded_orch().await;
        let (number, job_id, run_id) = seed_running_node(&orch).await;
        exec_sql(
            &orch,
            format!("UPDATE runs SET status = 'exited' WHERE id = '{run_id}'"),
        )
        .await;
        exec_sql(
            &orch,
            format!("UPDATE jobs SET status = 'complete' WHERE id = '{job_id}'"),
        )
        .await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/1/builder"),
            ChangeMode::Patch,
            Some(serde_json::json!({"action": "stop"})),
        );
        let summary = apply(&orch, &item).await.unwrap();
        assert!(summary.contains("no active run"), "got: {summary}");
    }

    #[tokio::test]
    async fn node_patch_merge_without_pr_still_errors() {
        // Reordering stop before the PR gate must not regress the PR-action
        // path: merge/close/refresh on a node with no merge_requests row still
        // returns the 'no PR yet' error.
        let orch = seeded_orch().await;
        let (number, _job_id, _run_id) = seed_running_node(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/1/builder"),
            ChangeMode::Patch,
            Some(serde_json::json!({"action": "merge"})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("no PR yet"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn node_patch_stop_dry_run_describes_without_stopping() {
        let orch = seeded_orch().await;
        let (number, _job_id, run_id) = seed_running_node(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/1/builder"),
            ChangeMode::Patch,
            Some(serde_json::json!({"action": "stop"})),
        );
        let change = dispatch_resource_change(&orch, &request(), 0, &item, true)
            .await
            .unwrap();
        assert!(
            change.summary.contains("Would stop"),
            "got: {}",
            change.summary
        );
        // A dry run leaves the run untouched.
        assert_eq!(
            run_status_for(&orch, &run_id).await.as_deref(),
            Some("live")
        );
    }

    #[tokio::test]
    async fn patch_status_closed_resolves_issue() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"status": "closed"})),
        );
        apply(&orch, &item).await.unwrap();
        let issue = issue_crud::get(&orch.db.local, &issue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(issue.status, IssueStatus::Closed);
        assert!(issue.closed_at.is_some());
    }

    #[tokio::test]
    async fn patch_status_merged_resolves_issue() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"status": "merged"})),
        );
        apply(&orch, &item).await.unwrap();
        let issue = issue_crud::get(&orch.db.local, &issue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(issue.status, IssueStatus::Merged);
        assert!(issue.merged_at.is_some());
    }

    #[tokio::test]
    async fn patch_invalid_status_is_rejected() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        // `backlog` is derived, not settable, and must be rejected alongside any
        // other unknown value.
        for bad in ["backlog", "active", "frobnicate"] {
            let item = change_item(
                &format!("cairn://p/CAIRN/{number}"),
                ChangeMode::Patch,
                Some(serde_json::json!({"status": bad})),
            );
            let err = apply(&orch, &item).await.unwrap_err();
            assert!(
                err.error.contains("merged") && err.error.contains("closed"),
                "expected allowed-set message, got: {}",
                err.error
            );
        }
        // The issue was never resolved by a rejected patch.
        let issue = issue_crud::get(&orch.db.local, &issue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(issue.status, IssueStatus::Backlog);
    }

    #[tokio::test]
    async fn patch_title_leaves_status_untouched() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"title": "Renamed"})),
        );
        apply(&orch, &item).await.unwrap();
        let issue = issue_crud::get(&orch.db.local, &issue_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(issue.title, "Renamed");
        assert_eq!(issue.status, IssueStatus::Backlog);
    }

    #[tokio::test]
    async fn patch_parent_adopts_issue() {
        let orch = seeded_orch().await;
        let (child_id, child_num) = seed_issue(&orch).await;
        let project_id = project_id_of(&orch, &child_id).await;
        let (parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{parent_num}")})),
        );
        apply(&orch, &item).await.unwrap();
        assert_eq!(
            parent_issue_id_of(&orch, &child_id).await.as_deref(),
            Some(parent_id.as_str())
        );
    }

    #[tokio::test]
    async fn patch_parent_records_parent_job_id_from_caller() {
        let orch = seeded_orch().await;
        let (number, job_id, run_id) = seed_running_node(&orch).await;
        let running_issue =
            crate::issues::relations::issue_id_for_project_number(&orch.db.local, "CAIRN", number)
                .await
                .unwrap()
                .unwrap();
        let project_id = project_id_of(&orch, &running_issue).await;
        let (parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
        let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{parent_num}")})),
        );
        apply_as_run(&orch, &item, &run_id).await.unwrap();
        assert_eq!(
            parent_issue_id_of(&orch, &child_id).await.as_deref(),
            Some(parent_id.as_str())
        );
        // run-stop's job is a recipe-root, so its own job is the recorded spawner.
        assert_eq!(
            parent_job_id_of(&orch, &child_id).await.as_deref(),
            Some(job_id.as_str())
        );
    }

    #[tokio::test]
    async fn patch_parent_null_orphans_issue() {
        let orch = seeded_orch().await;
        let (number, _job_id, run_id) = seed_running_node(&orch).await;
        let running_issue =
            crate::issues::relations::issue_id_for_project_number(&orch.db.local, "CAIRN", number)
                .await
                .unwrap()
                .unwrap();
        let project_id = project_id_of(&orch, &running_issue).await;
        let (_parent_id, parent_num) = add_issue(&orch, &project_id, "Parent").await;
        let (child_id, child_num) = add_issue(&orch, &project_id, "Child").await;
        // Adopt as a job-bound run so both parent fields are populated.
        let adopt = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{parent_num}")})),
        );
        apply_as_run(&orch, &adopt, &run_id).await.unwrap();
        assert!(parent_issue_id_of(&orch, &child_id).await.is_some());
        assert!(parent_job_id_of(&orch, &child_id).await.is_some());
        // Orphan clears both the parent and its now-meaningless spawner.
        let orphan = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": serde_json::Value::Null})),
        );
        apply(&orch, &orphan).await.unwrap();
        assert!(parent_issue_id_of(&orch, &child_id).await.is_none());
        assert!(parent_job_id_of(&orch, &child_id).await.is_none());
    }

    #[tokio::test]
    async fn patch_parent_self_rejected() {
        let orch = seeded_orch().await;
        let (_child_id, child_num) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{child_num}")})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("its own parent"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn patch_parent_unknown_uri_rejected() {
        let orch = seeded_orch().await;
        let (_child_id, child_num) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "cairn://p/CAIRN/9999"})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(
            err.error.contains("parent issue not found"),
            "got: {}",
            err.error
        );
    }

    #[tokio::test]
    async fn patch_parent_cross_project_rejected() {
        let orch = seeded_orch().await;
        let (_child_id, child_num) = seed_issue(&orch).await;
        let repo_path = tempfile::tempdir()
            .unwrap()
            .keep()
            .to_string_lossy()
            .to_string();
        let other = project_crud::create_db(
            &orch.db.local,
            &RealClock,
            &CreateProject {
                id: None,
                name: "Agg".to_string(),
                key: "AGG".to_string(),
                repo_path,
                team_id: None,
            },
        )
        .await
        .unwrap();
        let (_agg_id, agg_num) = add_issue(&orch, &other.id, "AggParent").await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/AGG/{agg_num}")})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("same project"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn patch_parent_cycle_rejected() {
        let orch = seeded_orch().await;
        let (a_id, a_num) = seed_issue(&orch).await;
        let project_id = project_id_of(&orch, &a_id).await;
        let (_b_id, b_num) = add_issue(&orch, &project_id, "B").await;
        // A adopts B as its parent.
        let adopt = change_item(
            &format!("cairn://p/CAIRN/{a_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{b_num}")})),
        );
        apply(&orch, &adopt).await.unwrap();
        // Adopting B under A would close the loop A -> B -> A.
        let cycle = change_item(
            &format!("cairn://p/CAIRN/{b_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": format!("cairn://p/CAIRN/{a_num}")})),
        );
        let err = apply(&orch, &cycle).await.unwrap_err();
        assert!(err.error.contains("cycle"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn patch_parent_malformed_uri_rejected() {
        let orch = seeded_orch().await;
        let (_child_id, child_num) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{child_num}"),
            ChangeMode::Patch,
            Some(serde_json::json!({"parent": "not-a-uri"})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("issue URI"), "got: {}", err.error);
    }

    #[tokio::test]
    async fn delete_removes_issue() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Delete,
            None,
        );
        apply(&orch, &item).await.unwrap();
        assert!(issue_crud::get(&orch.db.local, &issue_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_rejects_payload() {
        let orch = seeded_orch().await;
        let (_, number) = seed_issue(&orch).await;
        let item = change_item(
            &format!("cairn://p/CAIRN/{number}"),
            ChangeMode::Delete,
            Some(serde_json::json!({"force": true})),
        );
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(
            err.error.contains("does not accept payload"),
            "got: {}",
            err.error
        );
    }

    #[tokio::test]
    async fn delete_unknown_issue_errors() {
        let orch = seeded_orch().await;
        seed_issue(&orch).await;
        let item = change_item("cairn://p/CAIRN/9999", ChangeMode::Delete, None);
        let err = apply(&orch, &item).await.unwrap_err();
        assert!(err.error.contains("not found"), "got: {}", err.error);
    }

    /// End-to-end through `dispatch_resource_change`: a patch on
    /// `.../executions/{seq}` routes to the agent-edit arm (not the parity-bug
    /// catch-all) and persists the edited agent snapshot. The test caller has no
    /// resolvable run, so the self-edit guard allows it.
    #[tokio::test]
    async fn patch_execution_agent_snapshot_updates_stored_snapshot() {
        let orch = seeded_orch().await;
        let (issue_id, number) = seed_issue(&orch).await;
        let snapshot_json = serde_json::json!({
            "recipe": {"id":"r","name":"R","description":null,"trigger":"manual","nodes":[],"edges":[]},
            "agents": {"builder": {"id":"builder","name":"Builder","description":"","prompt":"old","tools":[],"selection":{"backend":"claude","model":"sonnet"},"disallowedTools":null,"skills":null,"fence":"ask"}},
            "skills": {},
            "triggerContext": {"issueId": issue_id, "projectId":"p","triggerType":"manual"},
            "createdAt": 1
        })
        .to_string();
        let issue_id_for_insert = issue_id.clone();
        orch.db
            .local
            .write(|conn| {
                let issue_id = issue_id_for_insert.clone();
                let snapshot_json = snapshot_json.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq, snapshot)
                         VALUES ('exec-x','r',?1,(SELECT project_id FROM issues WHERE id=?1),'running',1,1,?2)",
                        (issue_id.as_str(), snapshot_json.as_str()),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let item = change_item(
            &format!("cairn://p/CAIRN/{number}/executions/1"),
            ChangeMode::Patch,
            Some(serde_json::json!({
                "agent": "builder",
                "snapshot": {"id":"builder","name":"Builder","description":"","prompt":"new","tools":[],"selection":{"backend":"claude","model":"sonnet"},"disallowedTools":null,"skills":null,"fence":"ask"}
            })),
        );
        let summary = apply(&orch, &item).await.unwrap();
        assert!(summary.contains("Edited agent 'builder'"), "got: {summary}");

        let json = orch
            .db
            .local
            .query_opt_text("SELECT snapshot FROM executions WHERE id='exec-x'", ())
            .await
            .unwrap()
            .unwrap();
        let snap = crate::models::ExecutionSnapshot::from_json(&json).unwrap();
        assert_eq!(snap.agents["builder"].prompt, "new");
    }

    fn dummy_value(ty: cairn_common::contract::KeyType) -> serde_json::Value {
        use cairn_common::contract::KeyType;
        match ty {
            KeyType::Str => serde_json::json!("sample"),
            KeyType::Bool => serde_json::json!(true),
            KeyType::Int => serde_json::json!(1),
            KeyType::Array => serde_json::json!([]),
            KeyType::Object => serde_json::json!({}),
        }
    }

    /// Payload satisfying a mutation's required keys in their canonical spelling,
    /// so the gate passes and dispatch reaches the real arm instead of the
    /// gate's missing-key rejection.
    fn required_payload(spec: &cairn_common::contract::MutationSpec) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        for key in spec.required {
            map.insert(key.key.to_string(), dummy_value(key.ty));
        }
        serde_json::Value::Object(map)
    }

    /// A parseable sample URI for every resource kind that carries a mutation, so
    /// the parity test can build a representative `CairnResource` for each
    /// advertised `(kind, mode)`. Only `Mcp` is mode-sensitive — its dispatch
    /// arms split on whether the URI names a server (create targets the bare
    /// registry; patch/delete name one server). A kind that gains a mutation
    /// without a sample here trips the explicit panic, telling the next builder
    /// to add one.
    fn sample_resource(
        kind: cairn_common::contract::ResourceKind,
        mode: ChangeMode,
    ) -> CairnResource {
        use cairn_common::contract::ResourceKind as K;
        let uri = match kind {
            K::Mcp => {
                if matches!(mode, ChangeMode::Create) {
                    "cairn://mcp"
                } else {
                    "cairn://mcp/playwright"
                }
            }
            K::Project => "cairn://p/CAIRN",
            K::Settings => "cairn://settings",
            K::Projects => "cairn://projects",
            K::ProjectSettings => "cairn://p/CAIRN/settings",
            K::ProjectIssues => "cairn://p/CAIRN/issues",
            K::ProjectMessages => "cairn://p/CAIRN/messages",
            K::ProjectTerminal => "cairn://p/CAIRN/terminal/dev",
            K::ProjectBrowser => "cairn://p/CAIRN/browser/main",
            K::NodeBrowser => "cairn://p/CAIRN/1/1/builder/browser/main",
            K::TaskBrowser => "cairn://p/CAIRN/1/1/builder/task/sub/browser/main",
            K::Issue => "cairn://p/CAIRN/1",
            K::IssueExecutions => "cairn://p/CAIRN/1/executions",
            K::IssueExecution => "cairn://p/CAIRN/1/executions/2",
            K::IssueMessages => "cairn://p/CAIRN/1/messages",
            K::IssueComment => "cairn://p/CAIRN/1/comments/1",
            K::Node => "cairn://p/CAIRN/1/1/builder",
            K::NodeMessages => "cairn://p/CAIRN/1/1/builder/messages",
            K::NodeArtifact => "cairn://p/CAIRN/1/1/builder/plan",
            K::NodeTerminal => "cairn://p/CAIRN/1/1/builder/terminal/dev",
            K::TaskTerminal => "cairn://p/CAIRN/1/1/builder/task/sub/terminal/dev",
            K::TaskMessages => "cairn://p/CAIRN/1/1/builder/task/sub/messages",
            K::TaskArtifact => "cairn://p/CAIRN/1/1/builder/task/sub/result",
            K::JobTodos => "cairn://p/CAIRN/1/1/builder/todos",
            K::NodeWakes => "cairn://p/CAIRN/1/1/builder/wakes",
            K::NodeTasks => "cairn://p/CAIRN/1/1/builder/tasks",
            K::NodeQuestions => "cairn://p/CAIRN/1/1/builder/questions",
            K::NodeQuestion => "cairn://p/CAIRN/1/1/builder/questions/q-1",
            K::NodePermission => "cairn://p/CAIRN/1/1/builder/permissions/perm-1",
            K::TaskPermission => "cairn://p/CAIRN/1/1/builder/task/sub/permissions/perm-1",
            K::TaskPermissions => "cairn://p/CAIRN/1/1/builder/task/sub/permissions",
            K::Bug => "cairn://bug",
            K::Skills => "cairn://skills",
            K::Skill => "cairn://skills/testing",
            K::ProjectSkills => "cairn://p/CAIRN/skills",
            K::ProjectSkill => "cairn://p/CAIRN/skills/testing",
            K::ProjectReferences => "cairn://p/CAIRN/references",
            K::ProjectReference => "cairn://p/CAIRN/references/openpnp",
            K::Labels => "cairn://labels",
            K::Label => "cairn://labels/bug",
            K::NodeMemories => "cairn://p/CAIRN/1/1/builder/memories",
            K::NodeMemory => "cairn://p/CAIRN/1/1/builder/memories/1",
            K::Recipes => "cairn://recipes",
            K::Recipe => "cairn://recipes/build",
            K::ProjectRecipes => "cairn://p/CAIRN/recipes",
            K::ProjectRecipe => "cairn://p/CAIRN/recipes/build",
            K::Agents => "cairn://agents",
            K::Agent => "cairn://agents/build",
            K::ProjectAgents => "cairn://p/CAIRN/agents",
            K::ProjectAgent => "cairn://p/CAIRN/agents/build",
            K::Actions => "cairn://actions",
            K::Action => "cairn://actions/example",
            K::ProjectActions => "cairn://p/CAIRN/actions",
            K::ProjectAction => "cairn://p/CAIRN/actions/example",
            other => panic!(
                "sample_resource: {other:?} carries a mutation but has no sample URI; add one"
            ),
        };
        let resource = cairn_common::uri::parse_uri(uri)
            .unwrap_or_else(|| panic!("sample_resource URI failed to parse: {uri}"));
        assert_eq!(
            resource.kind(),
            kind,
            "sample_resource URI {uri} parsed to a different kind",
        );
        resource
    }

    /// Parity backstop for the claim in `cairn-common/src/contract.rs`: every
    /// `(kind, mode)` the contract table advertises must be handled by a real
    /// dispatch arm, never falling through to the catch-all. Runtime parity (a
    /// dry-run dispatch per advertised mutation) rather than a duplicated static
    /// arm table, which would be a second source of truth that can itself drift.
    /// The mutation need not succeed: any error other than the catch-all sentinel
    /// (not-found, deep validation) proves an arm exists, and dry_run suppresses
    /// side effects.
    #[tokio::test]
    async fn contract_mutations_all_have_dispatch_arms() {
        const SENTINEL: &str = "no dispatch arm handles it";
        let orch = seeded_orch().await;
        for contract in cairn_common::contract::RESOURCE_CONTRACTS {
            for spec in contract.mutations {
                let resource = sample_resource(contract.kind, spec.mode);
                let item = change_item(&resource.to_uri(), spec.mode, Some(required_payload(spec)));
                if let Err(failure) =
                    dispatch_resource_change(&orch, &request(), 0, &item, true).await
                {
                    assert!(
                        !failure.error.contains(SENTINEL),
                        "contract advertises {:?} mode={} but no dispatch arm handles it: {}",
                        contract.kind,
                        mode_name(spec.mode),
                        failure.error
                    );
                }
            }
        }
    }

    /// Alias analogue of the parity test. Every alias a mutation advertises must
    /// be honored end-to-end (gate + dispatch arm), not merely matched by the
    /// gate's `satisfied_by`: dispatch each owning mutation with the aliased key
    /// in its ALIAS spelling (other required keys canonical) and assert it is
    /// never rejected for a missing required key — exactly what a gate or handler
    /// that ignores the alias would produce.
    ///
    /// This bites on *required* aliased keys, where alias-honoring is gate-
    /// observable. An *optional* aliased key deserialized into a struct can still
    /// be silently dropped without erroring; full per-mutation coverage of that
    /// is out of scope, so the one advertised serde-alias case is pinned
    /// separately by `agent_frontmatter_honors_model_alias_for_tier`.
    #[tokio::test]
    async fn advertised_aliases_are_honored_by_dispatch() {
        const MISSING: &str = "Missing required payload key";
        let orch = seeded_orch().await;
        for contract in cairn_common::contract::RESOURCE_CONTRACTS {
            for spec in contract.mutations {
                let aliased = spec
                    .required
                    .iter()
                    .chain(spec.optional.iter())
                    .filter(|k| !k.aliases.is_empty());
                for key in aliased {
                    let mut map = serde_json::Map::new();
                    for req in spec.required {
                        map.insert(req.key.to_string(), dummy_value(req.ty));
                    }
                    // Re-spell the targeted key with its first alias.
                    map.remove(key.key);
                    let alias = key.aliases[0];
                    map.insert(alias.to_string(), dummy_value(key.ty));
                    let resource = sample_resource(contract.kind, spec.mode);
                    let item = change_item(
                        &resource.to_uri(),
                        spec.mode,
                        Some(serde_json::Value::Object(map)),
                    );
                    if let Err(failure) =
                        dispatch_resource_change(&orch, &request(), 0, &item, true).await
                    {
                        assert!(
                            !failure.error.contains(MISSING),
                            "{:?} mode={} does not honor alias '{}' for key '{}': {}",
                            contract.kind,
                            mode_name(spec.mode),
                            alias,
                            key.key,
                            failure.error
                        );
                    }
                }
            }
        }
    }

    /// `AGENT_TIER` advertises `model` as an alias for `tier`. Unlike a gate-
    /// checked required key, this optional field deserializes into a struct, so a
    /// missing serde alias would silently drop it rather than erroring. Pin it:
    /// agent frontmatter carrying `model` must populate `tier`.
    #[test]
    fn agent_frontmatter_honors_model_alias_for_tier() {
        let front: crate::agents::AgentFrontmatter = serde_json::from_value(serde_json::json!({
            "name": "Demo",
            "description": "demo agent",
            "tools": [],
            "model": "md",
        }))
        .expect("frontmatter with model alias should deserialize");
        assert_eq!(front.tier.as_deref(), Some("md"));
    }
}
