//! REPL resource mutation dispatch: create spawns a live eval-server into the
//! in-memory `repl_state`; delete kills it. There is no durable row — the
//! registry is the single source of truth. Input (code sends) arrives through
//! the run tool's `repl` key, not a resource append, so this advertises no
//! Append (see `NODE_REPL_CONTRACT`).

use super::super::{build_failure, payload_trimmed_non_empty_str, ResourceMutationResult};
use crate::mcp::handlers::repl::{self, ReplLang};
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
    let CairnResource::NodeRepl {
        project,
        number,
        exec_seq,
        node_id,
        slug,
    } = resource
    else {
        return Ok(None);
    };

    let summary = match item.mode {
        ChangeMode::Create => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| build_failure(index, item, "mode=create requires payload"))?;
            let interpreter_raw = payload_trimmed_non_empty_str(payload, "interpreter", &[])
                .ok_or_else(|| {
                    build_failure(index, item, "payload.interpreter is required (python)")
                })?;
            let interpreter = ReplLang::parse(interpreter_raw).ok_or_else(|| {
                build_failure(
                    index,
                    item,
                    format!(
                        "payload.interpreter '{interpreter_raw}' is not supported; use python (py)"
                    ),
                )
            })?;
            let deps = parse_deps(index, item)?;
            if dry_run {
                format!("Would start {} REPL {slug}", interpreter.label())
            } else {
                create_repl(
                    orch,
                    request,
                    project,
                    *number,
                    *exec_seq,
                    node_id,
                    slug,
                    interpreter,
                    &deps,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        ChangeMode::Delete => {
            if item.payload.is_some() {
                return Err(build_failure(
                    index,
                    item,
                    "mode=delete does not accept payload",
                ));
            }
            if dry_run {
                format!("Would stop REPL {slug}")
            } else {
                delete_repl(orch, project, *number, *exec_seq, node_id, slug)
                    .await
                    .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}

fn parse_deps(index: usize, item: &ChangeItem) -> ResourceMutationResult<Vec<String>> {
    let Some(payload) = item.payload.as_ref() else {
        return Ok(Vec::new());
    };
    match payload.get("deps") {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(values)) => {
            let mut deps = Vec::with_capacity(values.len());
            for value in values {
                let dep = value.as_str().ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "payload.deps must be an array of package-name strings",
                    )
                })?;
                deps.push(dep.to_string());
            }
            Ok(deps)
        }
        Some(_) => Err(build_failure(
            index,
            item,
            "payload.deps must be an array of package-name strings",
        )),
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_repl(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
    interpreter: ReplLang,
    deps: &[String],
) -> Result<String, String> {
    let target_job =
        repl::resolve_node_repl_job_id(&orch.db.local, project, number, exec_seq, node_id)
            .await
            .ok_or_else(|| {
                format!("No node found for cairn://p/{project}/{number}/{exec_seq}/{node_id}")
            })?;

    // A REPL is created by (and keyed to) the node's own agent: the run context
    // supplies the worktree cwd and the env the eval-server inherits, and its
    // job id must be the URI-resolved node so read/delete/send all key alike.
    let ctx = crate::mcp::handlers::run_context::lookup_run(&orch.db.local, request)
        .await
        .map_err(|_| {
            "A REPL can only be created by the node's own agent (no run context found).".to_string()
        })?;
    if ctx.job_id != target_job {
        return Err(format!(
            "A REPL can only be created on your own node; '{slug}' targets a different node."
        ));
    }
    let cwd = ctx.worktree_path.clone().ok_or_else(|| {
        "A REPL requires a worktree; this run has no worktree checkout.".to_string()
    })?;

    if orch.repl_state.contains(&target_job, slug) {
        return Ok(format!(
            "REPL {slug} is already running ({})",
            interpreter.label()
        ));
    }

    let session = repl::spawn_session(orch, &ctx, &cwd, interpreter, slug, deps).await?;
    orch.repl_state
        .insert(target_job, slug.to_string(), session);
    Ok(format!("Started {} REPL {slug}", interpreter.label()))
}

async fn delete_repl(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    slug: &str,
) -> Result<String, String> {
    let target_job =
        repl::resolve_node_repl_job_id(&orch.db.local, project, number, exec_seq, node_id)
            .await
            .ok_or_else(|| {
                format!("No node found for cairn://p/{project}/{number}/{exec_seq}/{node_id}")
            })?;
    match orch.repl_state.remove(&target_job, slug) {
        Some(session) => {
            session.kill();
            Ok(format!("Stopped REPL {slug}"))
        }
        None => Ok(format!(
            "No REPL named '{slug}' for this node (already stopped or never created)"
        )),
    }
}
