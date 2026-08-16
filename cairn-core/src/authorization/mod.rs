//! The authorization service: one matcher, two boundary adapters.
//!
//! Everything an authorization decision needs passes through here —
//! normalization, policy classification, grant matching, atomic once
//! consumption, grant issue/revoke/list, and the decision journal. There is
//! deliberately no second path: a caller that wants to know whether something
//! is allowed asks [`gate`] or [`authorize`], and nothing else reads the grant
//! tables.
//!
//! # Two check points, one matcher
//!
//! [`gate`] runs after a mutation's target has been resolved and validated but
//! before any side effect, so that a suspend for approval leaves no partial
//! batch behind. [`authorize`] runs immediately before the mutation persists,
//! re-matching against live state and atomically consuming a once-grant. The
//! second call is not redundant: between the two a grant can expire, be
//! revoked, or be consumed by a concurrent authorization, and the thing that
//! must be authorized is the write that actually happens.
//!
//! # Relationship to the fence
//!
//! This service replaced the gate that used to stand in front of workspace
//! settings and workspace MCP writes. That gate borrowed the fence's host-path
//! machinery to express an authority concern — it prompted about
//! `~/.cairn/settings.yaml` as a path when the real question was whether every
//! future agent should gain a capability. The fence still owns genuine
//! containment (kernel sandbox denials, sensitive host reads, writes escaping
//! the project namespace); it no longer stands in for authority.

pub mod normalize;
pub mod policy;
pub mod prepare;
pub mod protected;

use std::sync::Arc;

use cairn_common::authorization::{
    AuthorityAudience, AuthorityConstraintSet, AuthorityContext, AuthorityDecision, AuthorityGrant,
    AuthorityLifetime, AuthorityLifetimeKind, AuthorityPolicy, AuthorityPrincipal,
    AuthorityProvenance, AuthorityReason, AuthorityRequest,
};
use cairn_db::storage::authority as store;
use cairn_db::storage::authority::NewAuthorizationEvent;

use crate::mcp::types::McpCallbackRequest;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

pub use normalize::WORKSPACE_ID;

/// Who is asking, in which database, bound to which live anchors.
///
/// Resolved once per verb call and threaded to every check in that call, so the
/// gate and the pre-persist re-check cannot disagree about who the actor is.
#[derive(Debug, Clone)]
pub struct AuthorityActor {
    pub principal: AuthorityPrincipal,
    pub audience: AuthorityAudience,
    pub context: AuthorityContext,
    /// Always the private database. Grants are this install's own authorization
    /// state and are never replicated (see `0166_authority_grants.sql`), so the
    /// mint, the check, the listing, and revocation all address the same rows.
    /// Routing this to a team replica the way run-owned rows are routed is what
    /// would make a grant enforceable but invisible and unrevocable.
    pub db: Arc<LocalDb>,
    pub run_id: Option<String>,
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

/// Resolve the acting principal, audience, and live lifetime anchors for a verb
/// request.
///
/// Returns `None` when the request carries no resolvable run identity. A caller
/// must treat that as "no authority to adjudicate this" and refuse, rather than
/// proceeding unauthenticated.
pub async fn resolve_actor(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Option<AuthorityActor> {
    // The run's own rows may live in a team replica, so identity is resolved
    // through the router; the grants are not, so they are read from the private
    // database.
    let (ctx, run_db) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request)
        .await
        .ok()?;
    let run_id = ctx.run_id.clone();
    let node_uri = crate::mcp::handlers::run_context::lookup_home_uri_routed(&orch.db, request)
        .await
        .ok();
    let session_id = session_id_for_run(&run_db, &run_id).await;
    let turn_id = current_turn_for_run(orch, &run_db, &run_id).await;

    Some(AuthorityActor {
        principal: AuthorityPrincipal {
            node_uri,
            run_id: Some(run_id.clone()),
            agent_id: ctx.agent_config_id.clone(),
        },
        audience: AuthorityAudience::workspace(WORKSPACE_ID),
        context: AuthorityContext {
            audience: Some(AuthorityAudience::workspace(WORKSPACE_ID)),
            run_id: Some(run_id.clone()),
            turn_id,
            session_id,
            request_id: None,
        },
        db: orch.db.local.clone(),
        run_id: Some(run_id),
    })
}

/// The turn a `Turn` grant is compared against.
///
/// This reads `jobs.current_turn_id` rather than the in-memory process state,
/// and the distinction is load-bearing. When an approval arrives after the
/// inline wait budget the run has durably suspended: the successor turn is
/// created as a database row and `jobs.current_turn_id` is moved to it, but
/// nothing updates `process_state` before the verb is re-dispatched. Reading the
/// process would compare against the turn the run suspended FROM, so a `Turn`
/// grant would never authorize the very write it was minted for — the operator
/// would click "this turn" and be re-prompted. The durable row is the value that
/// is correct on both the inline and the suspended path.
async fn current_turn_for_run(
    orch: &Orchestrator,
    run_db: &LocalDb,
    run_id: &str,
) -> Option<String> {
    // A run with no owning job (a project-level chat) has no durable turn; fall
    // back to the live process so those runs can still hold a turn grant.
    durable_turn_for_run(run_db, run_id)
        .await
        .or_else(|| orch.process_state.get_current_turn_id(run_id))
}

/// The turn recorded on the run's owning job.
async fn durable_turn_for_run(run_db: &LocalDb, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    run_db
        .read(|conn| {
            let run_id = run_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT j.current_turn_id FROM runs r \
                         JOIN jobs j ON j.id = r.job_id WHERE r.id = ?1 LIMIT 1",
                        cairn_db::turso::params![run_id],
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                })
            })
        })
        .await
        .ok()
        .flatten()
}

async fn session_id_for_run(db: &LocalDb, run_id: &str) -> Option<String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT session_id FROM runs WHERE id = ?1 LIMIT 1",
                    cairn_db::turso::params![run_id],
                )
                .await?;
            Ok(match rows.next().await? {
                Some(row) => row.opt_text(0)?,
                None => None,
            })
        })
    })
    .await
    .ok()
    .flatten()
}

/// Classify a request and look for an active matching grant, without consuming
/// anything.
///
/// A prompt or a refusal IS the outcome of this attempt, so those are journaled
/// here. An allow is not: the allow only becomes a fact when [`authorize`]
/// admits the write that actually persists.
pub async fn gate(
    actor: &AuthorityActor,
    request: &AuthorityRequest,
) -> Result<AuthorityDecision, String> {
    let decision = adjudicate(actor, request, false).await?;
    if !decision.is_allowed() {
        journal(actor, request, &decision).await;
    }
    Ok(decision)
}

/// The final check, immediately before the mutation persists. Atomically
/// consumes a once-grant and journals the outcome.
pub async fn authorize(
    actor: &AuthorityActor,
    request: &AuthorityRequest,
) -> Result<AuthorityDecision, String> {
    let decision = adjudicate(actor, request, true).await?;
    journal(actor, request, &decision).await;
    Ok(decision)
}

async fn adjudicate(
    actor: &AuthorityActor,
    request: &AuthorityRequest,
    consume: bool,
) -> Result<AuthorityDecision, String> {
    // Structural invariants first: a Forbidden classification is not something a
    // grant can override, so it must be settled before any grant is read.
    match policy::classify(&request.scope, false) {
        AuthorityPolicy::Forbidden(reason) => Ok(AuthorityDecision::Forbidden(reason)),
        // Ordinary work: no grant lookup, no journal entry. Keeping this path
        // free of authorization bookkeeping is what stops the model from
        // taxing every project edit.
        AuthorityPolicy::Direct => Ok(AuthorityDecision::Direct),
        AuthorityPolicy::RequiresApproval(reason) => {
            let at = now();
            let candidates = store::candidate_grants(
                &actor.db,
                &actor.audience.workspace_id,
                &request.scope.shorthand(),
            )
            .await
            .map_err(|e| format!("could not read authority grants: {e}"))?;

            for grant in candidates {
                if !grant.matches(request, &actor.context, at) {
                    continue;
                }
                if consume && matches!(grant.lifetime, AuthorityLifetime::Once { .. }) {
                    // Whoever wins this UPDATE owns the single use. A loser
                    // keeps scanning: another grant may still cover the
                    // request, and if none does the operator is asked again.
                    let won = store::consume_grant(&actor.db, &grant.id, at)
                        .await
                        .map_err(|e| format!("could not consume authority grant: {e}"))?;
                    if !won {
                        continue;
                    }
                }
                return Ok(AuthorityDecision::AllowedByGrant {
                    grant_id: grant.id,
                    reason,
                });
            }
            Ok(AuthorityDecision::ApprovalRequired(reason))
        }
    }
}

/// Append a decision to the journal. Journaling must never be the reason a
/// legitimate mutation fails, so a write failure is logged rather than
/// propagated — but a missing row is a real gap, so it is logged at warn.
async fn journal(actor: &AuthorityActor, request: &AuthorityRequest, decision: &AuthorityDecision) {
    let Some(reason) = decision.reason() else {
        return;
    };
    let grant_id = match decision {
        AuthorityDecision::AllowedByGrant { grant_id, .. } => Some(grant_id.clone()),
        _ => None,
    };
    let event = NewAuthorizationEvent {
        scope: request.scope.clone(),
        mutation: request.mutation.as_str().to_string(),
        summary: request.summary.clone(),
        outcome: decision.as_str().to_string(),
        reason: reason.as_str().to_string(),
        principal: actor.principal.clone(),
        audience: actor.audience.clone(),
        run_id: actor.run_id.clone(),
        request_uri: actor.principal.node_uri.clone(),
        grant_id,
        decision_actor: None,
        appearance_snapshot: None,
        decided_at: now(),
    };
    if let Err(error) = store::append_event(&actor.db, event).await {
        log::warn!("failed to journal authorization decision: {error}");
    }
}

// ============================================================================
// Issue, revoke, list
// ============================================================================

/// Everything an operator's approval decides, resolved into a grant.
///
/// The caller supplies the lifetime the operator chose; the anchors come from
/// context here, never from the caller, so no surface can mint a grant anchored
/// to a turn or session it does not belong to.
#[derive(Debug, Clone)]
pub struct GrantIssue {
    pub request: AuthorityRequest,
    pub principal: AuthorityPrincipal,
    pub audience: AuthorityAudience,
    pub lifetime: AuthorityLifetimeKind,
    /// Anchor for a `Once` lifetime: the permission request being answered.
    pub request_id: Option<String>,
    /// Anchor for a `Turn` lifetime. This is the turn the agent will CONTINUE
    /// in, not the one it was suspended from: an approval that expired the
    /// instant the agent resumed would never authorize anything.
    pub turn_id: Option<String>,
    /// Anchor for a `Session` lifetime.
    pub session_id: Option<String>,
    pub expires_at: Option<i64>,
    pub provenance: AuthorityProvenance,
    /// Typed narrowings. A grant with none covers its whole scope.
    pub constraints: AuthorityConstraintSet,
}

/// Mint and persist a grant.
///
/// A lifetime whose anchor cannot be resolved is an error rather than a silent
/// downgrade: quietly turning an unanchorable `Session` approval into a
/// `Standing` one would hand the operator far more than they agreed to, and
/// quietly turning it into `Once` would strand the run at the same prompt.
pub async fn issue_grant(db: &LocalDb, issue: GrantIssue) -> Result<AuthorityGrant, String> {
    let lifetime = match issue.lifetime {
        AuthorityLifetimeKind::Once => AuthorityLifetime::Once {
            request_id: issue.request_id.clone().ok_or_else(|| {
                "a once grant must be anchored to the request that approved it".to_string()
            })?,
        },
        AuthorityLifetimeKind::Turn => AuthorityLifetime::Turn {
            turn_id: issue.turn_id.clone().ok_or_else(|| {
                "a turn grant needs the turn the run continues in; none was resolvable".to_string()
            })?,
        },
        AuthorityLifetimeKind::Session => AuthorityLifetime::Session {
            session_id: issue.session_id.clone().ok_or_else(|| {
                "a session grant needs the run's durable session id; none was resolvable"
                    .to_string()
            })?,
        },
        AuthorityLifetimeKind::Standing => AuthorityLifetime::Standing,
    };

    let grant = AuthorityGrant {
        id: uuid::Uuid::new_v4().to_string(),
        scope: issue.request.scope.clone(),
        principal: issue.principal,
        audience: issue.audience,
        lifetime,
        constraints: issue.constraints,
        provenance: issue.provenance,
        created_at: now(),
        expires_at: issue.expires_at,
        consumed_at: None,
        revoked_at: None,
    };
    store::insert_grant(db, &grant)
        .await
        .map_err(|e| format!("could not record authority grant: {e}"))?;
    Ok(grant)
}

/// Revoke a grant. Takes effect on the next authorization check — there is no
/// cached copy anywhere, because every check reads the grant rows fresh.
pub async fn revoke_grant(
    db: &LocalDb,
    grant_id: &str,
    revoked_by: Option<&str>,
) -> Result<bool, String> {
    store::revoke_grant(db, grant_id, revoked_by, now())
        .await
        .map_err(|e| format!("could not revoke authority grant: {e}"))
}

pub async fn get_grant(db: &LocalDb, grant_id: &str) -> Result<Option<AuthorityGrant>, String> {
    store::get_grant(db, grant_id)
        .await
        .map_err(|e| format!("could not read authority grant: {e}"))
}

pub async fn list_grants(db: &LocalDb, limit: i64) -> Result<Vec<AuthorityGrant>, String> {
    store::list_grants(db, WORKSPACE_ID, limit)
        .await
        .map_err(|e| format!("could not list authority grants: {e}"))
}

pub async fn events_citing(
    db: &LocalDb,
    grant_id: &str,
) -> Result<Vec<store::AuthorizationEventRecord>, String> {
    store::events_citing_grant(db, grant_id)
        .await
        .map_err(|e| format!("could not read authorization decisions: {e}"))
}

pub async fn recent_decisions(
    db: &LocalDb,
    limit: i64,
) -> Result<Vec<store::AuthorizationEventRecord>, String> {
    store::list_events(db, WORKSPACE_ID, limit)
        .await
        .map_err(|e| format!("could not read authorization decisions: {e}"))
}

/// The operator-facing sentence for a refusal, naming the scope so the message
/// says what would need approving rather than just that something failed.
pub fn refusal_message(request: &AuthorityRequest, reason: AuthorityReason) -> String {
    match reason {
        AuthorityReason::StructurallyInvalid => format!(
            "Refused: {} is not a structurally valid mutation, so no approval can permit it.",
            request.summary
        ),
        _ => format!(
            "Denied: {} requires operator approval ({}). Scope: {}",
            request.summary,
            reason.as_str(),
            request.scope.shorthand()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::authorization::{AuthorityConstraint, AuthorityMutation, AuthorityReason};

    /// An actor is a plain value, so the whole decision loop is exercisable
    /// against a real database without standing up an orchestrator. That is
    /// deliberate: the matcher is the part that must be right, and it should be
    /// testable without the machinery around it.
    async fn actor(name: &str) -> AuthorityActor {
        let db = crate::storage::migrated_test_db(name).await;
        AuthorityActor {
            principal: AuthorityPrincipal {
                node_uri: Some("cairn://p/cairn/1/1/builder".to_string()),
                run_id: Some("run-1".to_string()),
                agent_id: Some("build".to_string()),
            },
            audience: AuthorityAudience::workspace(WORKSPACE_ID),
            context: AuthorityContext {
                audience: Some(AuthorityAudience::workspace(WORKSPACE_ID)),
                run_id: Some("run-1".to_string()),
                turn_id: Some("turn-1".to_string()),
                session_id: Some("sess-1".to_string()),
                request_id: None,
            },
            db: std::sync::Arc::new(db),
            run_id: Some("run-1".to_string()),
        }
    }

    /// A stand-in configuration identity. Real ones come from
    /// `fingerprint_mcp_config`; these tests only need two digests that differ
    /// when the configuration differs.
    fn fingerprint(digest: &str) -> cairn_common::authorization::McpConfigFingerprint {
        cairn_common::authorization::McpConfigFingerprint {
            algorithm: "sha256".to_string(),
            encoding_version: 1,
            digest: digest.to_string(),
        }
    }

    fn mcp_request(server: &str, mutation: AuthorityMutation) -> AuthorityRequest {
        mcp_request_with(server, mutation, "config-a")
    }

    fn mcp_request_with(
        server: &str,
        mutation: AuthorityMutation,
        digest: &str,
    ) -> AuthorityRequest {
        normalize::workspace_mcp_write(WORKSPACE_ID, server, mutation, fingerprint(digest)).unwrap()
    }

    fn issue(request: &AuthorityRequest, lifetime: AuthorityLifetimeKind) -> GrantIssue {
        GrantIssue {
            request: request.clone(),
            // An anchored grant is bound to the run it was minted for, so the
            // fixture has to mint as the run that will later ask.
            principal: AuthorityPrincipal {
                node_uri: Some("cairn://p/cairn/1/1/builder".to_string()),
                run_id: Some("run-1".to_string()),
                agent_id: Some("build".to_string()),
            },
            audience: AuthorityAudience::workspace(WORKSPACE_ID),
            lifetime,
            request_id: Some("perm-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            session_id: Some("sess-1".to_string()),
            expires_at: None,
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".to_string(),
                ..Default::default()
            },
            // Mirrors what `mint_authority_grant` actually attaches: the
            // mutation the operator was shown, plus — for an MCP write — the
            // configuration identity that write is bound to. A fixture that
            // minted less than the real path does would test a grant the system
            // never issues.
            constraints: AuthorityConstraintSet::new({
                let mut constraints = vec![AuthorityConstraint::MutationModes {
                    modes: vec![request.mutation],
                }];
                if let Some(fingerprint) = request.facts.mcp_config.clone() {
                    constraints.push(AuthorityConstraint::McpConfig { fingerprint });
                }
                constraints
            }),
        }
    }

    /// The turn a `Turn` grant is compared against must come from the durable
    /// row, not the live process.
    ///
    /// This is the regression for the suspended-approval path: `ensure_successor_turn`
    /// moves `jobs.current_turn_id` to the successor and never touches
    /// `process_state`, so an authorization that read the process would compare
    /// against the turn the run suspended FROM and refuse the very write the
    /// operator approved "for this turn".
    #[tokio::test]
    async fn the_turn_anchor_comes_from_the_durable_row() {
        let db = crate::storage::migrated_test_db("authz-turn-anchor.db").await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, \
                     updated_at) VALUES ('p','default','P','p','/tmp',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs(id, project_id, status, created_at, updated_at) \
                     VALUES ('j','p','running',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs(id, job_id, status, session_id, created_at, updated_at) \
                     VALUES ('run-1','j','live','sess-1',1,1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, \
                     start_reason, created_at, updated_at) \
                     VALUES ('turn-successor','sess-1','run-1','j',2,'running',\
                     'permission_response',1,1)",
                    (),
                )
                .await?;
                // The successor turn is recorded on the job exactly as
                // `ensure_successor_turn` records it on the suspended path.
                conn.execute(
                    "UPDATE jobs SET current_turn_id = 'turn-successor' WHERE id = 'j'",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .expect("seed the run and its owning job");

        assert_eq!(
            durable_turn_for_run(&db, "run-1").await.as_deref(),
            Some("turn-successor")
        );
        assert_eq!(durable_turn_for_run(&db, "run-missing").await, None);
    }

    #[tokio::test]
    async fn ordinary_work_is_direct_and_leaves_no_authorization_trace() {
        let actor = actor("authz-direct.db").await;
        // A project-scoped place is not a boundary in v1.
        let project = AuthorityRequest::new(
            cairn_common::authorization::AuthorityScope::new(
                cairn_common::authorization::AuthorityPlace::Project {
                    project_id: "proj".to_string(),
                },
                cairn_common::authorization::AuthorityAction::Write,
            ),
            AuthorityMutation::Update,
            "edit the project".to_string(),
        );
        assert_eq!(
            gate(&actor, &project).await.unwrap(),
            AuthorityDecision::Direct
        );
        assert_eq!(
            authorize(&actor, &project).await.unwrap(),
            AuthorityDecision::Direct
        );
        // Direct work must not pay for authorization bookkeeping.
        assert!(recent_decisions(&actor.db, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_boundary_asks_then_a_standing_grant_authorizes_it_repeatedly() {
        let actor = actor("authz-standing.db").await;
        let request = mcp_request("linear", AuthorityMutation::Update);

        assert_eq!(
            gate(&actor, &request).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
        // The prompt is journaled with its stable reason and cites no grant.
        let prompted = recent_decisions(&actor.db, 10).await.unwrap();
        assert_eq!(prompted.len(), 1);
        assert_eq!(prompted[0].outcome, "approval_required");
        assert_eq!(prompted[0].reason, "workspace_tool_capability");
        assert!(prompted[0].grant_id.is_none());

        let grant = issue_grant(&actor.db, issue(&request, AuthorityLifetimeKind::Standing))
            .await
            .unwrap();

        // Standing authority is reusable, and every allow cites the grant.
        for _ in 0..2 {
            let decision = authorize(&actor, &request).await.unwrap();
            assert_eq!(
                decision,
                AuthorityDecision::AllowedByGrant {
                    grant_id: grant.id.clone(),
                    reason: AuthorityReason::WorkspaceToolCapability,
                }
            );
        }
        let cited = events_citing(&actor.db, &grant.id).await.unwrap();
        assert_eq!(cited.len(), 2);
        assert!(cited
            .iter()
            .all(|event| event.outcome == "allowed_by_grant"));
    }

    #[tokio::test]
    async fn revoking_a_standing_grant_restores_the_prompt() {
        let actor = actor("authz-revoke.db").await;
        let request = mcp_request("linear", AuthorityMutation::Update);
        let grant = issue_grant(&actor.db, issue(&request, AuthorityLifetimeKind::Standing))
            .await
            .unwrap();
        assert!(gate(&actor, &request).await.unwrap().is_allowed());

        assert!(revoke_grant(&actor.db, &grant.id, Some("mitch"))
            .await
            .unwrap());
        assert_eq!(
            gate(&actor, &request).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn a_once_grant_authorizes_exactly_one_mutation() {
        let actor = actor("authz-once.db").await;
        let request = mcp_request("linear", AuthorityMutation::Create);
        issue_grant(&actor.db, issue(&request, AuthorityLifetimeKind::Once))
            .await
            .unwrap();

        // The gate peeks without spending it, so deciding whether to prompt does
        // not consume the operator's single use.
        assert!(gate(&actor, &request).await.unwrap().is_allowed());
        assert!(gate(&actor, &request).await.unwrap().is_allowed());

        // The pre-persist check is what spends it.
        assert!(authorize(&actor, &request).await.unwrap().is_allowed());
        assert_eq!(
            authorize(&actor, &request).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn a_turn_grant_stops_at_the_turn_boundary() {
        let mut actor = actor("authz-turn.db").await;
        let request = mcp_request("linear", AuthorityMutation::Update);
        issue_grant(&actor.db, issue(&request, AuthorityLifetimeKind::Turn))
            .await
            .unwrap();
        assert!(authorize(&actor, &request).await.unwrap().is_allowed());

        actor.context.turn_id = Some("turn-2".to_string());
        assert_eq!(
            authorize(&actor, &request).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn a_session_grant_survives_the_turn_but_not_the_session() {
        let mut actor = actor("authz-session.db").await;
        let request = mcp_request("linear", AuthorityMutation::Update);
        issue_grant(&actor.db, issue(&request, AuthorityLifetimeKind::Session))
            .await
            .unwrap();

        actor.context.turn_id = Some("turn-9".to_string());
        assert!(authorize(&actor, &request).await.unwrap().is_allowed());

        actor.context.session_id = Some("sess-2".to_string());
        assert_eq!(
            authorize(&actor, &request).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn a_grant_authorizes_only_the_mutation_the_operator_was_shown() {
        let actor = actor("authz-narrow.db").await;
        // Approving "reconfigure linear" must not also approve deleting it.
        let reconfigure = mcp_request("linear", AuthorityMutation::Update);
        issue_grant(
            &actor.db,
            issue(&reconfigure, AuthorityLifetimeKind::Standing),
        )
        .await
        .unwrap();

        assert!(authorize(&actor, &reconfigure).await.unwrap().is_allowed());
        let remove = mcp_request("linear", AuthorityMutation::Delete);
        assert_eq!(
            authorize(&actor, &remove).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn a_grant_for_one_tool_never_authorizes_another() {
        let actor = actor("authz-other-tool.db").await;
        let linear = mcp_request("linear", AuthorityMutation::Update);
        issue_grant(&actor.db, issue(&linear, AuthorityLifetimeKind::Standing))
            .await
            .unwrap();

        let github = mcp_request("github", AuthorityMutation::Update);
        assert_eq!(
            authorize(&actor, &github).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn capability_bearing_settings_ask_while_preferences_stay_direct() {
        let actor = actor("authz-settings.db").await;
        let backends = normalize::workspace_settings_write(WORKSPACE_ID, "backends").unwrap();
        assert_eq!(
            gate(&actor, &backends).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceSettingsCapability)
        );

        let keybinds = normalize::workspace_settings_write(WORKSPACE_ID, "keybinds").unwrap();
        assert_eq!(
            gate(&actor, &keybinds).await.unwrap(),
            AuthorityDecision::Direct
        );

        // An unclassified section fails closed rather than defaulting to direct.
        let unknown = normalize::workspace_settings_write(WORKSPACE_ID, "futureThing").unwrap();
        assert_eq!(
            gate(&actor, &unknown).await.unwrap(),
            AuthorityDecision::ApprovalRequired(
                AuthorityReason::UnclassifiedWorkspaceSettingsSection
            )
        );
    }

    #[tokio::test]
    async fn an_expired_standing_grant_stops_authorizing() {
        let actor = actor("authz-expiry.db").await;
        let request = mcp_request("linear", AuthorityMutation::Update);
        let mut spec = issue(&request, AuthorityLifetimeKind::Standing);
        // Already past by the time it is written.
        spec.expires_at = Some(1);
        issue_grant(&actor.db, spec).await.unwrap();

        assert_eq!(
            authorize(&actor, &request).await.unwrap(),
            AuthorityDecision::ApprovalRequired(AuthorityReason::WorkspaceToolCapability)
        );
    }

    #[tokio::test]
    async fn a_lifetime_whose_anchor_is_missing_is_refused_not_downgraded() {
        let actor = actor("authz-anchor.db").await;
        let request = mcp_request("linear", AuthorityMutation::Update);
        let mut spec = issue(&request, AuthorityLifetimeKind::Session);
        spec.session_id = None;
        // Quietly widening this to standing would hand over far more than the
        // operator agreed to; quietly narrowing it would strand the run.
        assert!(issue_grant(&actor.db, spec).await.is_err());
    }
}
