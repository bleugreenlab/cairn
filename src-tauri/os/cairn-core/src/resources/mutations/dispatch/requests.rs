//! Filing and withdrawal carry the live authenticated callback into the service.
use super::super::{build_failure, ResourceMutationResult};
use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
use crate::orchestrator::Orchestrator;
use cairn_common::requests::FileOperatorRequest;
use cairn_common::uri::CairnResource;

fn file_input(payload: &serde_json::Value) -> Result<FileOperatorRequest, String> {
    serde_json::from_value(payload.clone())
        .map_err(|error| format!("Invalid request filing: {error}"))
}

fn check_withdraw(payload: &serde_json::Value) -> Result<(), String> {
    let object = payload
        .as_object()
        .ok_or("Request withdrawal requires an object")?;
    if object.len() != 1
        || object.get("withdraw").and_then(serde_json::Value::as_bool) != Some(true)
    {
        return Err("Only withdraw:true is allowed; acceptance, decisions, status and actor facts require their registered authority boundary".into());
    }
    Ok(())
}

pub(super) async fn dispatch(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    index: usize,
    item: &ChangeItem,
    dry_run: bool,
    resource: &CairnResource,
    applied_data: &mut Option<serde_json::Value>,
) -> ResourceMutationResult<Option<String>> {
    if !matches!(
        (resource, item.mode),
        (CairnResource::Requests, ChangeMode::Append)
            | (CairnResource::Request { .. }, ChangeMode::Patch)
    ) {
        return Ok(None);
    }
    let payload = item
        .payload
        .as_ref()
        .ok_or_else(|| build_failure(index, item, "Request mutation requires payload"))?;
    // Filing derives live target requirements and origin inside one service.
    // A preview must never create a contribution to obtain that validation.
    if dry_run {
        return Err(build_failure(
            index,
            item,
            "Operator request mutations do not support previews; nothing was filed or withdrawn",
        ));
    }
    let summary = match resource {
        CairnResource::Requests => {
            let input = file_input(payload).map_err(|error| build_failure(index, item, error))?;
            let filed = crate::operator_requests::file(orch, request, input)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let summary = format!(
                "{} operator request {}",
                if filed.created { "Filed" } else { "Joined" },
                filed.request.uri
            );
            *applied_data = Some(
                serde_json::to_value(filed)
                    .map_err(|error| build_failure(index, item, error.to_string()))?,
            );
            summary
        }
        CairnResource::Request { id } => {
            check_withdraw(payload).map_err(|error| build_failure(index, item, error))?;
            let updated = crate::operator_requests::withdraw(orch, request, id)
                .await
                .map_err(|error| build_failure(index, item, error))?;
            let summary = format!("Withdrew your open contribution to {}", updated.uri);
            *applied_data = Some(
                serde_json::to_value(updated)
                    .map_err(|error| build_failure(index, item, error.to_string()))?,
            );
            summary
        }
        _ => unreachable!(),
    };
    Ok(Some(summary))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn filing_cannot_supply_requester_or_operator_identity() {
        for key in [
            "requested_by",
            "requester",
            "principal",
            "actor",
            "approver",
            "wake_destination",
            "status",
            "facts",
        ] {
            let mut input = json!({"kind":"rebuild","rationale":"test","waiting":[]});
            input[key] = json!("forged");
            assert!(file_input(&input).is_err(), "{key}");
        }
        let error = file_input(&json!({"kind":"unknown","rationale":"test"})).unwrap_err();
        for kind in ["rebuild", "bump_version", "lift_drain"] {
            assert!(error.contains(kind), "{error}");
        }
    }
    #[test]
    fn withdrawal_cannot_accept_or_change_actor_state() {
        assert!(check_withdraw(&json!({"withdraw":true})).is_ok());
        for payload in [
            json!({"withdraw":false}),
            json!({"accept":true}),
            json!({"withdraw":true,"status":"done"}),
            json!({"withdraw":true,"facts":{}}),
            json!({"withdraw":true,"requested_by":"another"}),
        ] {
            assert!(check_withdraw(&payload).is_err());
        }
    }
}
