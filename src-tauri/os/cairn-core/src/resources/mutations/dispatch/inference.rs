//! The inference facet's writes. The pool controller's: a pool patch (take or
//! hand back control, the windows, a pin, a release or revocation) and the
//! request queue (ask for a model, cancel a request). The act side's: one
//! deployment's `patch {action: start|stop}`, which is parsed here and decided
//! by the deployment controller (`orchestrator::inference_control`).
//!
//! Who acts is the authenticated agent run, named by its home, so a pin's
//! holder and a request's requester cannot be claimed by another run. A write
//! that no agent run makes (the operator's) is the operator's, or the
//! `requester` its payload names.

use super::super::{build_failure, payload_str, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::pool_control::{cancel_request, patch_pool, request_model, PoolPatch};
use crate::orchestrator::Orchestrator;
use cairn_common::uri::CairnResource;
use cairn_inference::control::Request;

async fn actor(orch: &Orchestrator, request: &McpCallbackRequest, named: Option<&str>) -> String {
    match crate::mcp::handlers::run_context::authenticated_agent(&orch.db, request).await {
        Ok(agent) => agent.home,
        Err(_) => named
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .unwrap_or("operator")
            .to_string(),
    }
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
) -> ResourceMutationResult<Option<String>> {
    let fail = |error: String| build_failure(index, item, error);
    match (resource, item.mode) {
        (CairnResource::InferencePools, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            let patch = PoolPatch::parse(payload).map_err(fail)?;
            if dry_run {
                return Ok(Some(format!("Would patch pool {}", patch.pool)));
            }
            let actor = actor(orch, request, None).await;
            patch_pool(orch, &patch, &actor)
                .await
                .map(Some)
                .map_err(fail)
        }
        (CairnResource::InferenceRequests, ChangeMode::Append) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=append requires payload".into()))?;
            let model = payload_str(payload, "model", &[])
                .ok_or_else(|| fail("payload.model must name the model to load".into()))?;
            if dry_run {
                return Ok(Some(format!("Would request {model}")));
            }
            let requester = actor(orch, request, payload_str(payload, "requester", &[])).await;
            let handle = request_model(orch, model, &requester).await.map_err(fail)?;
            let status = handle.status(orch).await.map_err(fail)?;
            Ok(Some(format!(
                "Request #{} for {} on pool {}: {}. Read cairn://inference/requests for the queue.",
                handle.id,
                handle.deployment,
                handle.pool,
                status.describe()
            )))
        }
        (CairnResource::InferenceRequests, ChangeMode::Patch) => {
            let payload = item
                .payload
                .as_ref()
                .ok_or_else(|| fail("mode=patch requires payload".into()))?;
            let id = payload
                .get("id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| fail("payload.id must be the request's number".into()))?;
            match payload_str(payload, "action", &[]) {
                Some("cancel") => {}
                _ => return Err(fail("payload.action must be \"cancel\"".into())),
            }
            if dry_run {
                return Ok(Some(format!("Would cancel request #{id}")));
            }
            let by = actor(orch, request, None).await;
            cancel_request(orch, id, &by).await.map(Some).map_err(fail)
        }
        (CairnResource::InferenceDeployment { id }, ChangeMode::Patch) => {
            let action = match item
                .payload
                .as_ref()
                .and_then(|payload| payload_str(payload, "action", &[]))
            {
                Some("start") => Request::Start,
                Some("stop") => Request::Stop,
                Some(other) => {
                    return Err(fail(format!(
                        "payload.action `{other}` must be start or stop"
                    )))
                }
                None => return Err(fail("payload.action must be start or stop".into())),
            };
            if dry_run {
                return Ok(Some(format!("Would {} deployment {id}", action.as_str())));
            }
            crate::orchestrator::inference_control::request_deployment(orch, id, action)
                .await
                .map(Some)
                .map_err(fail)
        }
        _ => Ok(None),
    }
}
