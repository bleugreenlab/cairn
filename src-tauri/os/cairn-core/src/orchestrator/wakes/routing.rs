use super::types::*;
use crate::models::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
#[cfg(test)]
pub(super) use cairn_attention::wakes::routing::child_attention_message;
pub fn route_child_attention(
    orch: &Orchestrator,
    _child_issue_id: &str,
    issue_uri: &str,
    attention: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    cairn_attention::wakes::route_child_attention(
        &crate::attention_host::runtime(orch),
        _child_issue_id,
        issue_uri,
        attention,
        fact_kind,
        detail_uri,
        urgency,
    )
}

pub(super) fn route_wake_sync(
    orch: &Orchestrator,
    event: WakeEvent,
) -> Result<WakeRouteAction, String> {
    cairn_attention::wakes::route_wake_sync(&crate::attention_host::runtime(orch), event)
}

pub fn route_resource_updated(
    orch: &Orchestrator,
    resource_uri: &str,
) -> Result<WakeRouteAction, String> {
    cairn_attention::wakes::route_resource_updated(
        &crate::attention_host::runtime(orch),
        resource_uri,
    )
}

pub fn route_process_event(
    orch: &Orchestrator,
    process_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    cairn_attention::wakes::route_process_event(
        &crate::attention_host::runtime(orch),
        process_ref,
        fact_kind,
        detail_uri,
    )
}

pub fn route_condition_event(
    orch: &Orchestrator,
    condition_ref: &str,
    fact_kind: &str,
    detail_uri: Option<&str>,
) -> Result<WakeRouteAction, String> {
    cairn_attention::wakes::route_condition_event(
        &crate::attention_host::runtime(orch),
        condition_ref,
        fact_kind,
        detail_uri,
    )
}

pub async fn route_wake(orch: &Orchestrator, event: WakeEvent) -> Result<WakeRouteAction, String> {
    cairn_attention::wakes::route_wake(&crate::attention_host::runtime(orch), event).await
}
