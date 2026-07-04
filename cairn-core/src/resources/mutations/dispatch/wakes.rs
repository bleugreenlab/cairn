//! Node wake subscription resource mutation dispatch, relocated from dispatch.rs.

use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
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
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

#[derive(Debug)]
pub(super) struct WakeFilterPayload {
    pub(super) kind: String,
    pub(super) reference: Option<String>,
    pub(super) fact_kinds: Option<Vec<String>>,
    /// Terminal subscriptions only: `"exit"` (default) or `"output"`.
    pub(super) on: Option<String>,
    /// Terminal `on:"output"` subscriptions only: the literal phrase to watch for.
    pub(super) phrase: Option<String>,
}

pub(super) fn parse_wake_filter(
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
pub(super) fn terminal_slug_from_ref(reference: &str) -> &str {
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
