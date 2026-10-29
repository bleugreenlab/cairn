//! The store-side replay action on `cairn:~/rebase`.
//!
//! An agent's slot is a plain git worktree whose refs are downstream exports of
//! the runner's private jj store, so nothing done there can move a branch's
//! ancestry. This is the sanctioned request: it enqueues durable reconcile work
//! and returns. No jj runs in the agent's slot, and there is deliberately no
//! git/jj escape hatch beside it — the shared store stays the single authority on
//! ancestry.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;

pub(super) async fn dispatch(
    orch: &Orchestrator,
    _request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
        (
            CairnResource::NodeRebase {
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
                .ok_or_else(|| build_failure(index, item, "mode=patch requires payload"))?;
            let action = payload_non_empty_str(payload, "action", &[])
                .ok_or_else(|| build_failure(index, item, "payload.action is required"))?;
            if !matches!(action, "replay" | "fast-forward-from-origin") {
                return Err(build_failure(
                    index,
                    item,
                    format!(
                        "payload.action must be 'replay' or 'fast-forward-from-origin', got '{action}'"
                    ),
                ));
            }
            let fast_forward_branch = payload_non_empty_str(payload, "branch", &[]);
            if fast_forward_branch.is_some() && action != "fast-forward-from-origin" {
                return Err(build_failure(
                    index,
                    item,
                    "payload.branch applies only to fast-forward-from-origin",
                ));
            }
            if fast_forward_branch.is_some_and(|value| value != "base") {
                return Err(build_failure(
                    index,
                    item,
                    "payload.branch must be 'base' when provided: fast-forward-from-origin acts on this node's branch, or with branch:'base' on the branch it is cut from",
                ));
            }
            let fingerprint = payload_non_empty_str(payload, "fingerprint", &[]);
            let resolution = payload_non_empty_str(payload, "resolution", &[]);
            if resolution.is_some_and(|value| value != "take-committed-tip") {
                return Err(build_failure(
                    index,
                    item,
                    "payload.resolution must be 'take-committed-tip' when provided",
                ));
            }
            // The documented escape from the loss guard. It only means anything
            // alongside the resolution whose whole-file restore does the
            // dropping, so a request carrying it without one is confused about
            // what it is asking for and is told so rather than obeyed.
            let drop_incoming_reason = payload_non_empty_str(payload, "drop_incoming_reason", &[]);
            if drop_incoming_reason.is_some() && resolution.is_none() {
                return Err(build_failure(
                    index,
                    item,
                    "payload.drop_incoming_reason only applies to \
                     resolution:'take-committed-tip', which is what discards incoming hunks; a \
                     plain replay drops nothing",
                ));
            }

            let db = orch.db.for_project(project).await;
            let (conn, job) = crate::resources::connect_and_find_node_job(
                &db, project, *number, *exec_seq, node_id,
            )
            .await
            .map_err(|error| build_failure(index, item, error))?;
            let branch = crate::resources::node_branch(&conn, &job.id)
                .await
                .map_err(|error| build_failure(index, item, error))?
                .ok_or_else(|| {
                    build_failure(
                        index,
                        item,
                        "This node has no branch to replay.".to_string(),
                    )
                })?;

            if action == "fast-forward-from-origin" {
                let target = if fast_forward_branch.is_some() {
                    crate::resources::node_base_branch(orch, &conn, &job.id, &branch)
                        .await
                        .map_err(|error| build_failure(index, item, error))?
                } else {
                    branch.clone()
                };
                if dry_run {
                    format!("Would take origin's copy of `{target}` if it is a pure fast-forward")
                } else {
                    let project_id = crate::mcp::handlers::run_context::project_id_by_key(
                        &orch.db.local,
                        project,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                    crate::orchestrator::base_branch_sync::fast_forward_now(
                        orch,
                        &project_id,
                        &target,
                    )
                    .await
                    .map_err(|error| build_failure(index, item, error))?
                }
            } else if dry_run {
                format!("Would request a store-side replay of `{branch}`")
            } else {
                crate::orchestrator::base_advance::request_branch_replay(
                    orch,
                    &db,
                    &job.id,
                    &branch,
                    fingerprint,
                    resolution.is_some(),
                    drop_incoming_reason,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
