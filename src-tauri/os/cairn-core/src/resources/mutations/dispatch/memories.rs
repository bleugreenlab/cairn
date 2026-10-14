//! Memory resource mutation dispatch, relocated from dispatch.rs.

use super::super::memories::{
    apply_memory_triage_action, apply_node_memory_append, apply_node_memory_delete,
    apply_node_memory_patch, MemoryCreateTarget, MemoryResourceTarget,
};
use super::super::{build_failure, PromotedMemoryRef, ResourceMutationResult};
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
    promoted_memory: &mut Option<PromotedMemoryRef>,
) -> ResourceMutationResult<Option<String>> {
    let summary = match (resource, item.mode) {
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
                    *promoted_memory = promoted;
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
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
