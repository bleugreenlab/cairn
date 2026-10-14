//! Raising a named authority boundary as an operator prompt.
//!
//! This is the authorization service's interactive edge. It reuses the existing
//! durable permission request/resolution machinery wholesale — owning-database
//! routing, the inline wait, durable suspend/resume, duplicate answers,
//! successor turns — rather than growing a second prompt lifecycle beside it.
//! What is new is only the *content*: instead of a host path descriptor, the
//! stored request carries the normalized scope, the concrete mutation, the
//! audience, and the policy reason, so the prompt can be rendered by the system
//! from structured facts. No part of an authority prompt's security meaning
//! comes from agent-authored prose.
//!
//! An authority request is deliberately a different stored shape from a fence
//! [`super::fence::Crossing`]: it carries no `descriptor`, so it can never be
//! mistaken for a containment exception, and answering it grants no path.

use cairn_common::authorization::{
    AuthorityAudience, AuthorityDecision, AuthorityPrincipal, AuthorityReason, AuthorityRequest,
};
use serde::{Deserialize, Serialize};

use crate::authorization::{self, AuthorityActor};
use crate::mcp::types::McpCallbackRequest;
use crate::models::Fence;
use crate::orchestrator::Orchestrator;

use super::permission::{await_permission_decision, PermissionWait};

/// Tag stored in `tool_input.kind`, distinguishing an authority request from
/// every other kind of permission row.
pub const AUTHORITY_KIND: &str = "authority_request";

/// What an authority prompt stores, and what the UI renders from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityPromptDetail {
    /// Always [`AUTHORITY_KIND`].
    pub kind: String,
    /// The verb to re-dispatch on allow.
    pub verb: String,
    /// The normalized scope plus the concrete mutation and its system-rendered
    /// summary.
    pub authority: AuthorityRequest,
    /// Stable policy reason code.
    pub reason: AuthorityReason,
    pub principal: AuthorityPrincipal,
    pub audience: AuthorityAudience,
    /// The originating verb request, re-dispatched verbatim once a grant is
    /// minted.
    pub request: McpCallbackRequest,
}

impl AuthorityPromptDetail {
    /// The scope shorthand the prompt and the grant list both display.
    pub fn scope_shorthand(&self) -> String {
        self.authority.scope.shorthand()
    }
}

/// Parse a stored `tool_input` as an authority request. Returns `None` for a
/// fence crossing or a legacy tool prompt.
pub fn parse_authority_detail(tool_input: &str) -> Option<AuthorityPromptDetail> {
    let detail: AuthorityPromptDetail = serde_json::from_str(tool_input).ok()?;
    (detail.kind == AUTHORITY_KIND).then_some(detail)
}

/// The outcome of raising an authority boundary.
#[derive(Debug)]
pub enum AuthorityGate {
    /// Proceed — either ordinary work, or an active grant covers it.
    Allow,
    /// Refused, with an operator-facing reason.
    Deny(String),
    /// The run durably suspended awaiting approval; the verb handler returns a
    /// suspend marker and the run re-drives the verb on resume.
    Suspended,
    /// Approval could not be requested because its durable backing service was
    /// unavailable. This is not an operator decision.
    Unavailable(String),
}

/// Adjudicate a normalized authority request, prompting the operator when
/// policy requires approval and no active grant covers it.
///
/// `fence` is the acting agent's containment policy, consulted for exactly one
/// thing: whether there is anyone to ask. A `Fence::Deny` agent is an explicitly
/// noninteractive run, so an approval boundary is refused outright rather than
/// suspending forever on a prompt nobody will see. A `Fence::Allow` agent still
/// gets the prompt — "allow all" is a decision about containment escapes, and
/// letting it silently confer workspace authority is precisely the conflation
/// this model exists to end. The operator's remedy is strictly better than the
/// old one: a standing grant, which is listable and revocable.
pub async fn raise_authority(
    orch: &Orchestrator,
    actor: &AuthorityActor,
    fence: Fence,
    request: &McpCallbackRequest,
    verb: &'static str,
    authority: &AuthorityRequest,
) -> AuthorityGate {
    let decision = match authorization::gate(actor, authority).await {
        Ok(decision) => decision,
        // A grant store we cannot read is not a reason to proceed.
        Err(error) => return AuthorityGate::Deny(format!("Denied: {error}")),
    };

    let reason = match decision {
        AuthorityDecision::Direct | AuthorityDecision::AllowedByGrant { .. } => {
            return AuthorityGate::Allow
        }
        AuthorityDecision::Forbidden(reason) => {
            return AuthorityGate::Deny(authorization::refusal_message(authority, reason))
        }
        AuthorityDecision::ApprovalRequired(reason) => reason,
    };

    if matches!(fence, Fence::Deny) {
        return AuthorityGate::Deny(format!(
            "{} This run is noninteractive (fence: deny), so there is no one to approve it.",
            authorization::refusal_message(authority, reason)
        ));
    }

    let Some(run_id) = actor.principal.run_id.clone() else {
        return AuthorityGate::Deny(authorization::refusal_message(authority, reason));
    };

    // Stable tool_use id so the resume attaches the synthetic result to the verb
    // call the agent is waiting on.
    let tool_use_id = request
        .tool_use_id
        .clone()
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let mut embedded = request.clone();
    embedded.run_id = Some(run_id.clone());
    embedded.tool_use_id = Some(tool_use_id.clone());

    let detail = AuthorityPromptDetail {
        kind: AUTHORITY_KIND.to_string(),
        verb: verb.to_string(),
        authority: authority.clone(),
        reason,
        principal: actor.principal.clone(),
        audience: actor.audience.clone(),
        request: embedded,
    };
    let tool_input = match serde_json::to_value(&detail) {
        Ok(value) => value,
        Err(error) => {
            return AuthorityGate::Deny(format!(
                "Denied: could not describe the authority request: {error}"
            ))
        }
    };

    match await_permission_decision(orch, &run_id, &tool_use_id, verb, &tool_input).await {
        PermissionWait::Decided(response) => {
            // An approval whose grant did not persist is not an approval, and
            // saying so beats letting the pre-persist check refuse with
            // "requires operator approval" a moment after the operator
            // approved.
            if let Some(error) = grant_error(&response) {
                return AuthorityGate::Deny(format!(
                    "Approval could not be recorded, so it did not take effect: {error}. \
                     Nothing was changed; ask again."
                ));
            }
            if response_is_allow(&response) {
                AuthorityGate::Allow
            } else {
                AuthorityGate::Deny(format!(
                    "Denied by operator: {} (scope: {})",
                    authority.summary,
                    authority.scope.shorthand()
                ))
            }
        }
        PermissionWait::Suspended => AuthorityGate::Suspended,
        PermissionWait::Unavailable(error) => AuthorityGate::Unavailable(error),
    }
}

fn grant_error(response_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(response_json)
        .ok()?
        .get(super::permission::GRANT_ERROR_KEY)?
        .as_str()
        .map(ToOwned::to_owned)
}

fn response_is_allow(response_json: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(response_json)
        .ok()
        .and_then(|value| {
            value
                .get("behavior")
                .and_then(|b| b.as_str())
                .map(|b| b == "allow")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::authorization::{
        AuthorityAction, AuthorityMutation, AuthorityPlace, AuthorityScope, ToolKind,
    };

    fn detail() -> AuthorityPromptDetail {
        AuthorityPromptDetail {
            kind: AUTHORITY_KIND.to_string(),
            verb: "write".to_string(),
            authority: AuthorityRequest::new(
                AuthorityScope::new(
                    AuthorityPlace::Tool {
                        workspace_id: "default".to_string(),
                        kind: ToolKind::McpServer,
                        canonical_name: "linear".to_string(),
                    },
                    AuthorityAction::Write,
                ),
                AuthorityMutation::Create,
                "install workspace MCP server 'linear'".to_string(),
            ),
            reason: AuthorityReason::WorkspaceToolCapability,
            principal: AuthorityPrincipal::default(),
            audience: AuthorityAudience::workspace("default"),
            request: McpCallbackRequest::default(),
        }
    }

    #[test]
    fn authority_detail_round_trips_and_carries_its_scope() {
        let json = serde_json::to_string(&detail()).unwrap();
        let parsed = parse_authority_detail(&json).expect("parses as authority");
        assert_eq!(
            parsed.scope_shorthand(),
            "workspace/default/tool/mcp/linear:write"
        );
        assert_eq!(parsed.reason, AuthorityReason::WorkspaceToolCapability);
    }

    #[test]
    fn an_authority_request_never_parses_as_a_fence_crossing() {
        // The two shapes must stay mutually exclusive: an authority approval
        // grants a scope, and must never be mistaken for a containment
        // exception that would insert a host path into the session grant set.
        let json = serde_json::to_string(&detail()).unwrap();
        assert!(super::super::permission::parses_as_crossing(&json).is_none());
    }

    #[test]
    fn a_fence_crossing_never_parses_as_an_authority_request() {
        let crossing = serde_json::json!({
            "kind": "external_host_write",
            "verb": "write",
            "descriptor": "/etc/hosts",
            "summary": "write an external host path: /etc/hosts",
            "request": McpCallbackRequest::default(),
        })
        .to_string();
        assert!(parse_authority_detail(&crossing).is_none());
    }

    #[test]
    fn an_allow_whose_grant_failed_to_persist_is_not_read_as_an_allow() {
        // The operator allowed it, so the stored answer stays an allow — but the
        // waiter has to learn the approval did not take effect, or the agent is
        // told "requires operator approval" a moment after approval.
        let ordinary = serde_json::json!({"behavior": "allow"}).to_string();
        assert!(response_is_allow(&ordinary));
        assert!(grant_error(&ordinary).is_none());

        let failed = serde_json::json!({
            "behavior": "allow",
            "grantError": "could not record authority grant: disk full",
        })
        .to_string();
        assert!(response_is_allow(&failed));
        assert_eq!(
            grant_error(&failed).as_deref(),
            Some("could not record authority grant: disk full")
        );
    }

    #[test]
    fn a_legacy_tool_prompt_is_neither() {
        let legacy = serde_json::json!({"command": "ls -la"}).to_string();
        assert!(parse_authority_detail(&legacy).is_none());
    }
}
