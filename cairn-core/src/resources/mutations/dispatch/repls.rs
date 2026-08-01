//! REPL resource mutation dispatch.
//!
//! `create` opens a generation: it spawns an eval-server into the in-memory
//! `repl_state` and, on a slug whose durable row already exists, *resumes* that
//! REPL rather than duplicating it. `delete` is two-stage — stop, then discard —
//! because the transcript now outlives the process. Input (code sends) arrives
//! through the run tool's `repl` key, not a resource append, so this advertises
//! no Append (see `NODE_REPL_CONTRACT`).

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
            // Both keys are optional on a slug that already has a row: a resume
            // inherits the recorded interpreter and deps, which is what makes
            // reopening an exited REPL a one-liner.
            let interpreter = match item
                .payload
                .as_ref()
                .and_then(|payload| payload_trimmed_non_empty_str(payload, "interpreter", &[]))
            {
                Some(raw) => Some(ReplLang::parse(raw).ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        format!(
                            "payload.interpreter '{raw}' is not supported; use python (py) | typescript (ts)"
                        ),
                    )
                })?),
                None => None,
            };
            let deps = parse_deps(index, item)?;
            if dry_run {
                match interpreter {
                    Some(lang) => format!("Would open {} REPL {slug}", lang.label()),
                    None => format!("Would resume REPL {slug}"),
                }
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
                    deps,
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
                format!("Would stop or discard REPL {slug}")
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

/// `None` means "the payload said nothing about deps", which on a resume inherits
/// the recorded set; `Some(vec![])` is an explicit empty set.
fn parse_deps(index: usize, item: &ChangeItem) -> ResourceMutationResult<Option<Vec<String>>> {
    let Some(payload) = item.payload.as_ref() else {
        return Ok(None);
    };
    match payload.get("deps") {
        None | Some(serde_json::Value::Null) => Ok(None),
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
            Ok(Some(deps))
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
    interpreter: Option<ReplLang>,
    deps: Option<Vec<String>>,
) -> Result<String, String> {
    let target_job =
        repl::resolve_node_repl_job_id(&orch.db.local, project, number, exec_seq, node_id)
            .await
            .ok_or_else(|| {
                format!("No node found for cairn://p/{project}/{number}/{exec_seq}/{node_id}")
            })?;

    // A REPL is created by (and keyed to) the node's own agent. Its controller
    // uses the job scratch residence while the interpreter runs in an executor
    // held cell at the resolved logical coordinate.
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
    let cwd = crate::scratch::ensure_job_scratch_dir(&ctx.job_id, None)
        .to_string_lossy()
        .into_owned();

    crate::repl_host::open_repl(
        orch,
        &ctx.job_id,
        &ctx.project_id,
        &cwd,
        Some(&ctx),
        slug,
        interpreter,
        deps,
    )
    .await
    .map(|opened| opened.summary())
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
    crate::repl_host::close_job_repl(orch, target_job, slug.to_string()).await
}
