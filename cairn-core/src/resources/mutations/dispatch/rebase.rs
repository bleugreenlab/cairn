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
            if action != "replay" {
                return Err(build_failure(
                    index,
                    item,
                    format!("payload.action must be 'replay', got '{action}'"),
                ));
            }
            let fingerprint = payload_non_empty_str(payload, "fingerprint", &[]);

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

            if dry_run {
                format!("Would request a store-side replay of `{branch}`")
            } else {
                crate::orchestrator::base_advance::request_branch_replay(
                    orch,
                    &db,
                    &job.id,
                    &branch,
                    fingerprint,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
