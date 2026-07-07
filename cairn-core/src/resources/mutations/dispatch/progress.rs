//! Workflow-progress resource mutation dispatch: the typed append that the
//! harness `phase()` / `log()` verbs land on `cairn://.../{node}/progress`.

use super::super::{build_failure, payload_non_empty_str, ResourceMutationResult};
use super::append_payload;
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
            CairnResource::NodeProgress {
                project,
                number,
                exec_seq,
                node_id,
            },
            ChangeMode::Append,
        ) => {
            let payload = append_payload(index, item)?;
            let kind = payload_non_empty_str(payload, "kind", &[])
                .ok_or_else(|| build_failure(index, item, "payload.kind is required"))?;
            if kind != "phase" && kind != "log" {
                return Err(build_failure(
                    index,
                    item,
                    format!("payload.kind must be 'phase' or 'log', got '{kind}'"),
                ));
            }
            let text = payload_non_empty_str(payload, "text", &[])
                .ok_or_else(|| build_failure(index, item, "payload.text is required"))?;
            if dry_run {
                format!(
                    "Would append {kind} entry ({} chars) to {project}-{number}/{exec_seq}/{node_id} progress",
                    text.len()
                )
            } else {
                // Progress is durable per-project state (like todos): route to
                // the owning project's database and resolve the workflow node's
                // job the same way.
                let db = orch.db.for_project(project).await;
                let job_id = crate::resources::resolve_todos_job_id(
                    &db, project, *number, *exec_seq, node_id, None,
                )
                .await
                .map_err(|error| build_failure(index, item, error))?;
                let entry = crate::workflow_progress::append_entry(&db, &job_id, kind, text)
                    .await
                    .map_err(|error| build_failure(index, item, error))?;
                let _ = orch.services.emitter.emit(
                    "db-change",
                    serde_json::json!({"table": "workflow_progress", "action": "append"}),
                );
                format!(
                    "Appended {} entry #{} to {project}-{number}/{exec_seq}/{node_id} progress",
                    entry.kind, entry.seq
                )
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(summary))
}
