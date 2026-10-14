//! Storage for journaled authority grants and the authorization journal.
//!
//! This module owns the on-disk encoding of [`AuthorityGrant`] and nothing else:
//! normalization, policy, and matching live in `cairn-core`'s authorization
//! service. The split matters because it keeps the encoding in one place — a
//! grant is written and read by exactly these functions, so a column and its
//! typed meaning cannot drift apart.
//!
//! Decoding is strict. A row whose constraint envelope carries an encoding
//! version this build does not understand is an error, not a skipped row: the
//! caller's authorization check then fails closed and asks the operator again,
//! which is the only safe reading of "there is an approval here that I cannot
//! interpret".

use cairn_common::authorization::{
    AuthorityAudience, AuthorityConstraintSet, AuthorityGrant, AuthorityLifetime,
    AuthorityPrincipal, AuthorityProvenance, AuthorityScope,
};

use super::{DbError, DbResult, LocalDb, RowExt};
use crate::turso::params;
use cairn_common::identity::{AppearanceSnapshot, PrincipalPosition, PrincipalRef};

const GRANT_COLUMNS: &str = "id,scope_json,constraints_json,principal_json,audience_json,\
     lifetime_kind,lifetime_anchor,provenance_json,created_at,expires_at,consumed_at,revoked_at,\
     actor_principal_json,appearance_snapshot_json";

fn decode<T: serde::de::DeserializeOwned>(what: &str, json: &str) -> DbResult<T> {
    serde_json::from_str(json)
        .map_err(|e| DbError::internal(format!("unreadable authority {what}: {e}")))
}

fn decode_attribution(
    actor_json: Option<String>,
    snapshot_json: Option<String>,
) -> DbResult<(Option<PrincipalRef>, Option<AppearanceSnapshot>)> {
    match (actor_json, snapshot_json) {
        (None, None) => Ok((None, None)),
        (Some(actor_json), Some(snapshot_json)) => {
            let actor = decode::<PrincipalRef>("decision actor", &actor_json)?;
            actor
                .validate_at(PrincipalPosition::DecisionActor)
                .map_err(|e| DbError::internal(format!("invalid authority decision actor: {e}")))?;
            let snapshot = decode::<AppearanceSnapshot>("appearance snapshot", &snapshot_json)?;
            snapshot.validate().map_err(|e| {
                DbError::internal(format!("invalid authority appearance snapshot: {e}"))
            })?;
            if snapshot.principal() != &actor {
                return Err(DbError::internal(
                    "authority decision actor does not match appearance snapshot principal",
                ));
            }
            Ok((Some(actor), Some(snapshot)))
        }
        _ => Err(DbError::internal(
            "authority attribution must contain both actor and appearance snapshot or neither",
        )),
    }
}

fn encode_attribution(
    actor: Option<&PrincipalRef>,
    snapshot: Option<&AppearanceSnapshot>,
) -> DbResult<(Option<String>, Option<String>)> {
    match (actor, snapshot) {
        (None, None) => Ok((None, None)),
        (Some(actor), Some(snapshot)) => {
            actor
                .validate_at(PrincipalPosition::DecisionActor)
                .map_err(|e| DbError::internal(format!("invalid authority decision actor: {e}")))?;
            snapshot.validate().map_err(|e| {
                DbError::internal(format!("invalid authority appearance snapshot: {e}"))
            })?;
            if snapshot.principal() != actor {
                return Err(DbError::internal(
                    "authority decision actor does not match appearance snapshot principal",
                ));
            }
            Ok((
                Some(serde_json::to_string(actor).map_err(|e| {
                    DbError::internal(format!("decision actor is not serializable: {e}"))
                })?),
                Some(serde_json::to_string(snapshot).map_err(|e| {
                    DbError::internal(format!("appearance snapshot is not serializable: {e}"))
                })?),
            ))
        }
        _ => Err(DbError::internal(
            "authority attribution must contain both actor and appearance snapshot or neither",
        )),
    }
}

fn grant_from_row(row: &crate::turso::Row) -> DbResult<AuthorityGrant> {
    let lifetime_kind: String = row.text(5)?;
    let lifetime_anchor: Option<String> = row.opt_text(6)?;
    let lifetime = match (lifetime_kind.as_str(), lifetime_anchor) {
        ("once", Some(request_id)) => AuthorityLifetime::Once { request_id },
        ("turn", Some(turn_id)) => AuthorityLifetime::Turn { turn_id },
        ("session", Some(session_id)) => AuthorityLifetime::Session { session_id },
        ("standing", None) => AuthorityLifetime::Standing,
        (kind, anchor) => {
            return Err(DbError::internal(format!(
                "authority grant has lifetime '{kind}' with anchor {anchor:?}, which is not a \
                 lifetime this build understands"
            )))
        }
    };
    let mut provenance = decode::<AuthorityProvenance>("provenance", &row.text(7)?)?;
    let (decision_actor, appearance_snapshot) =
        decode_attribution(row.opt_text(12)?, row.opt_text(13)?)?;
    if provenance.decision_actor != decision_actor
        || provenance.appearance_snapshot != appearance_snapshot
    {
        return Err(DbError::internal(
            "authority grant attribution columns do not match provenance",
        ));
    }
    provenance.decision_actor = decision_actor;
    provenance.appearance_snapshot = appearance_snapshot;
    Ok(AuthorityGrant {
        id: row.text(0)?,
        scope: decode::<AuthorityScope>("scope", &row.text(1)?)?,
        constraints: AuthorityConstraintSet::parse(&row.text(2)?).map_err(DbError::internal)?,
        principal: decode::<AuthorityPrincipal>("principal", &row.text(3)?)?,
        audience: decode::<AuthorityAudience>("audience", &row.text(4)?)?,
        lifetime,
        provenance,
        created_at: row.i64(8)?,
        expires_at: row.opt_i64(9)?,
        consumed_at: row.opt_i64(10)?,
        revoked_at: row.opt_i64(11)?,
    })
}

/// Persist a newly minted grant.
pub async fn insert_grant(db: &LocalDb, grant: &AuthorityGrant) -> DbResult<()> {
    let scope_json = serde_json::to_string(&grant.scope)
        .map_err(|e| DbError::internal(format!("scope is not serializable: {e}")))?;
    let constraints_json = serde_json::to_string(&grant.constraints)
        .map_err(|e| DbError::internal(format!("constraints are not serializable: {e}")))?;
    let principal_json = serde_json::to_string(&grant.principal)
        .map_err(|e| DbError::internal(format!("principal is not serializable: {e}")))?;
    let audience_json = serde_json::to_string(&grant.audience)
        .map_err(|e| DbError::internal(format!("audience is not serializable: {e}")))?;
    let provenance_json = serde_json::to_string(&grant.provenance)
        .map_err(|e| DbError::internal(format!("provenance is not serializable: {e}")))?;
    let (actor_principal_json, appearance_snapshot_json) = encode_attribution(
        grant.provenance.decision_actor.as_ref(),
        grant.provenance.appearance_snapshot.as_ref(),
    )?;

    let id = grant.id.clone();
    let scope_key = grant.scope.shorthand();
    let place_kind = grant.scope.place_kind().to_string();
    let action = grant.scope.action.as_str().to_string();
    let workspace_id = grant.audience.workspace_id.clone();
    let lifetime_kind = grant.lifetime.kind().as_str().to_string();
    let lifetime_anchor = grant.lifetime.anchor().map(str::to_string);
    let created_at = grant.created_at;
    let expires_at = grant.expires_at;

    db.write(move |conn| {
        let values = (
            id.clone(),
            scope_key.clone(),
            place_kind.clone(),
            action.clone(),
            scope_json.clone(),
            constraints_json.clone(),
            principal_json.clone(),
            audience_json.clone(),
            workspace_id.clone(),
            lifetime_kind.clone(),
            lifetime_anchor.clone(),
            provenance_json.clone(),
        );
        let actor_principal_json = actor_principal_json.clone();
        let appearance_snapshot_json = appearance_snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO authority_grants (id,scope_key,place_kind,action,scope_json,\
                 constraints_json,principal_json,audience_json,workspace_id,lifetime_kind,\
                 lifetime_anchor,provenance_json,created_at,expires_at,actor_principal_json,\
                 appearance_snapshot_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
                params![
                    values.0,
                    values.1,
                    values.2,
                    values.3,
                    values.4,
                    values.5,
                    values.6,
                    values.7,
                    values.8,
                    values.9,
                    values.10,
                    values.11,
                    created_at,
                    expires_at,
                    actor_principal_json,
                    appearance_snapshot_json
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Every grant that could possibly authorize this scope in this workspace.
///
/// Narrowing is by the injective scope shorthand only; the caller confirms the
/// match structurally against the full [`AuthorityScope`], audience, and
/// constraints. Spent grants come back too, so a caller can tell "revoked"
/// apart from "never granted" when it journals the reason.
pub async fn candidate_grants(
    db: &LocalDb,
    workspace_id: &str,
    scope_key: &str,
) -> DbResult<Vec<AuthorityGrant>> {
    db.query_all(
        format!(
            "SELECT {GRANT_COLUMNS} FROM authority_grants \
             WHERE workspace_id=?1 AND scope_key=?2 ORDER BY created_at ASC"
        ),
        (workspace_id.to_string(), scope_key.to_string()),
        grant_from_row,
    )
    .await
}

pub async fn grants_by_actor(
    db: &LocalDb,
    workspace_id: &str,
    actor: &PrincipalRef,
    limit: i64,
) -> DbResult<Vec<AuthorityGrant>> {
    actor
        .validate_at(PrincipalPosition::DecisionActor)
        .map_err(|e| DbError::internal(format!("invalid authority decision actor: {e}")))?;
    let actor_json = serde_json::to_string(actor)
        .map_err(|e| DbError::internal(format!("decision actor is not serializable: {e}")))?;
    let grants = db
        .query_all(
            format!(
                "SELECT {GRANT_COLUMNS} FROM authority_grants \
                 WHERE workspace_id=?1 AND actor_principal_json=?2 \
                 ORDER BY created_at DESC, rowid DESC LIMIT ?3"
            ),
            (workspace_id.to_string(), actor_json, limit.clamp(1, 500)),
            grant_from_row,
        )
        .await?;
    if grants
        .iter()
        .any(|grant| grant.provenance.decision_actor.as_ref() != Some(actor))
    {
        return Err(DbError::internal(
            "authority actor index returned a grant for a different typed actor",
        ));
    }
    Ok(grants)
}

pub async fn get_grant(db: &LocalDb, id: &str) -> DbResult<Option<AuthorityGrant>> {
    db.query_opt(
        format!("SELECT {GRANT_COLUMNS} FROM authority_grants WHERE id=?1"),
        (id.to_string(),),
        grant_from_row,
    )
    .await
}

/// Every grant in a workspace, newest first.
pub async fn list_grants(
    db: &LocalDb,
    workspace_id: &str,
    limit: i64,
) -> DbResult<Vec<AuthorityGrant>> {
    db.query_all(
        format!(
            "SELECT {GRANT_COLUMNS} FROM authority_grants \
             WHERE workspace_id=?1 ORDER BY created_at DESC LIMIT ?2"
        ),
        (workspace_id.to_string(), limit.clamp(1, 500)),
        grant_from_row,
    )
    .await
}

/// Atomically consume a once-grant, returning whether THIS caller consumed it.
///
/// The `consumed_at IS NULL` predicate lives in the UPDATE rather than in a
/// read-then-write, so two concurrent authorizations of the same once-grant
/// cannot both observe it unconsumed and both proceed. Exactly one sees a
/// non-zero row count; the loser is told no and asks again.
pub async fn consume_grant(db: &LocalDb, id: &str, now: i64) -> DbResult<bool> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE authority_grants SET consumed_at=?2 \
                     WHERE id=?1 AND consumed_at IS NULL AND revoked_at IS NULL",
                    params![id, now],
                )
                .await?;
            Ok(changed > 0)
        })
    })
    .await
}

/// Revoke a grant. Returns false when it was already revoked, so a caller can
/// report "already revoked" instead of claiming it did something.
pub async fn revoke_grant(
    db: &LocalDb,
    id: &str,
    revoked_by: Option<&str>,
    now: i64,
) -> DbResult<bool> {
    let id = id.to_string();
    let revoked_by = revoked_by.map(str::to_string);
    db.write(move |conn| {
        let id = id.clone();
        let revoked_by = revoked_by.clone();
        Box::pin(async move {
            let changed = conn
                .execute(
                    "UPDATE authority_grants SET revoked_at=?2, revoked_by=?3 \
                     WHERE id=?1 AND revoked_at IS NULL",
                    params![id, now, revoked_by],
                )
                .await?;
            Ok(changed > 0)
        })
    })
    .await
}

// ============================================================================
// Authorization journal
// ============================================================================

/// One decision to append to the journal.
///
/// Only approval-required decisions are journaled. Recording ordinary direct
/// work here would bury the boundary crossings the journal exists to surface,
/// and would put an authorization write in the hot path of every project edit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAuthorizationEvent {
    pub scope: AuthorityScope,
    pub mutation: String,
    pub summary: String,
    /// `allowed_by_grant` | `approval_required` | `forbidden`.
    pub outcome: String,
    /// Stable policy reason code.
    pub reason: String,
    pub principal: AuthorityPrincipal,
    pub audience: AuthorityAudience,
    pub run_id: Option<String>,
    pub request_uri: Option<String>,
    /// Required for `allowed_by_grant`: the grant the decision cites.
    pub grant_id: Option<String>,
    pub decision_actor: Option<PrincipalRef>,
    pub appearance_snapshot: Option<AppearanceSnapshot>,
    pub decided_at: i64,
}

/// A journal row as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationEventRecord {
    pub id: String,
    pub scope: AuthorityScope,
    pub mutation: String,
    pub summary: String,
    pub outcome: String,
    pub reason: String,
    pub principal: AuthorityPrincipal,
    pub audience: AuthorityAudience,
    pub run_id: Option<String>,
    pub request_uri: Option<String>,
    pub grant_id: Option<String>,
    pub decision_actor: Option<PrincipalRef>,
    pub appearance_snapshot: Option<AppearanceSnapshot>,
    pub decided_at: i64,
}

const EVENT_COLUMNS: &str = "id,scope_json,mutation,summary,outcome,reason,principal_json,\
     audience_json,run_id,request_uri,grant_id,decided_at,actor_principal_json,\
     appearance_snapshot_json";

fn event_from_row(row: &crate::turso::Row) -> DbResult<AuthorizationEventRecord> {
    let (decision_actor, appearance_snapshot) =
        decode_attribution(row.opt_text(12)?, row.opt_text(13)?)?;
    Ok(AuthorizationEventRecord {
        id: row.text(0)?,
        scope: decode::<AuthorityScope>("scope", &row.text(1)?)?,
        mutation: row.text(2)?,
        summary: row.text(3)?,
        outcome: row.text(4)?,
        reason: row.text(5)?,
        principal: decode::<AuthorityPrincipal>("principal", &row.text(6)?)?,
        audience: decode::<AuthorityAudience>("audience", &row.text(7)?)?,
        run_id: row.opt_text(8)?,
        request_uri: row.opt_text(9)?,
        grant_id: row.opt_text(10)?,
        decision_actor,
        appearance_snapshot,
        decided_at: row.i64(11)?,
    })
}

/// Append a decision to the journal, returning its id.
///
/// An `allowed_by_grant` outcome with no cited grant is refused rather than
/// written: an allow whose authority cannot be named is exactly the record an
/// audit needs to be able to trust.
pub async fn append_event(db: &LocalDb, event: NewAuthorizationEvent) -> DbResult<String> {
    if event.outcome == "allowed_by_grant" && event.grant_id.is_none() {
        return Err(DbError::internal(
            "an allowed_by_grant authorization event must cite the grant that authorized it",
        ));
    }
    let scope_json = serde_json::to_string(&event.scope)
        .map_err(|e| DbError::internal(format!("scope is not serializable: {e}")))?;
    let principal_json = serde_json::to_string(&event.principal)
        .map_err(|e| DbError::internal(format!("principal is not serializable: {e}")))?;
    let audience_json = serde_json::to_string(&event.audience)
        .map_err(|e| DbError::internal(format!("audience is not serializable: {e}")))?;
    let (actor_principal_json, appearance_snapshot_json) = encode_attribution(
        event.decision_actor.as_ref(),
        event.appearance_snapshot.as_ref(),
    )?;

    let id = uuid::Uuid::new_v4().to_string();
    let result = id.clone();
    let scope_key = event.scope.shorthand();
    let place_kind = event.scope.place_kind().to_string();
    let action = event.scope.action.as_str().to_string();
    let workspace_id = event.audience.workspace_id.clone();

    db.write(move |conn| {
        let values = (
            id.clone(),
            scope_key.clone(),
            place_kind.clone(),
            action.clone(),
            scope_json.clone(),
            event.mutation.clone(),
            event.summary.clone(),
            event.outcome.clone(),
            event.reason.clone(),
            principal_json.clone(),
            audience_json.clone(),
            workspace_id.clone(),
        );
        let run_id = event.run_id.clone();
        let request_uri = event.request_uri.clone();
        let grant_id = event.grant_id.clone();
        let decided_at = event.decided_at;
        let actor_principal_json = actor_principal_json.clone();
        let appearance_snapshot_json = appearance_snapshot_json.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT INTO authorization_events (id,scope_key,place_kind,action,scope_json,\
                 mutation,summary,outcome,reason,principal_json,audience_json,workspace_id,\
                 run_id,request_uri,grant_id,decided_at,actor_principal_json,\
                 appearance_snapshot_json) \
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18)",
                params![
                    values.0,
                    values.1,
                    values.2,
                    values.3,
                    values.4,
                    values.5,
                    values.6,
                    values.7,
                    values.8,
                    values.9,
                    values.10,
                    values.11,
                    run_id,
                    request_uri,
                    grant_id,
                    decided_at,
                    actor_principal_json,
                    appearance_snapshot_json
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await?;
    Ok(result)
}

/// The most recent decisions in a workspace, newest first.
pub async fn list_events(
    db: &LocalDb,
    workspace_id: &str,
    limit: i64,
) -> DbResult<Vec<AuthorizationEventRecord>> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS} FROM authorization_events \
             WHERE workspace_id=?1 ORDER BY decided_at DESC, rowid DESC LIMIT ?2"
        ),
        (workspace_id.to_string(), limit.clamp(1, 500)),
        event_from_row,
    )
    .await
}

pub async fn events_by_actor(
    db: &LocalDb,
    workspace_id: &str,
    actor: &PrincipalRef,
    limit: i64,
) -> DbResult<Vec<AuthorizationEventRecord>> {
    actor
        .validate_at(PrincipalPosition::DecisionActor)
        .map_err(|e| DbError::internal(format!("invalid authority decision actor: {e}")))?;
    let actor_json = serde_json::to_string(actor)
        .map_err(|e| DbError::internal(format!("decision actor is not serializable: {e}")))?;
    let events = db
        .query_all(
            format!(
                "SELECT {EVENT_COLUMNS} FROM authorization_events \
                 WHERE workspace_id=?1 AND actor_principal_json=?2 \
                 ORDER BY decided_at DESC, rowid DESC LIMIT ?3"
            ),
            (workspace_id.to_string(), actor_json, limit.clamp(1, 500)),
            event_from_row,
        )
        .await?;
    if events
        .iter()
        .any(|event| event.decision_actor.as_ref() != Some(actor))
    {
        return Err(DbError::internal(
            "authority actor index returned an event for a different typed actor",
        ));
    }
    Ok(events)
}

/// Every decision that cited a given grant — the audit trail for one approval.
pub async fn events_citing_grant(
    db: &LocalDb,
    grant_id: &str,
) -> DbResult<Vec<AuthorizationEventRecord>> {
    db.query_all(
        format!(
            "SELECT {EVENT_COLUMNS} FROM authorization_events \
             WHERE grant_id=?1 ORDER BY decided_at ASC, rowid ASC"
        ),
        (grant_id.to_string(),),
        event_from_row,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::authorization::{
        AuthorityAction, AuthorityConstraint, AuthorityContext, AuthorityMutation, AuthorityPlace,
        AuthorityRequest, AuthorityScope, McpConfigFingerprint, ToolKind,
    };
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceTransport, VerificationMethod, VerificationRecord,
        VerificationStatus, VerificationStrength,
    };

    fn tool_scope(name: &str) -> AuthorityScope {
        AuthorityScope::new(
            AuthorityPlace::Tool {
                workspace_id: "default".to_string(),
                kind: ToolKind::McpServer,
                canonical_name: name.to_string(),
            },
            AuthorityAction::Write,
        )
    }

    /// The configuration identity an MCP write is bound to. [`tool_scope`] is an
    /// MCP server write, and such a request may only be authorized by a grant
    /// that names the configuration it approved — so the fixtures below carry
    /// this on both sides. Without it every grant here fails to cover its own
    /// request, and the lifetime assertions this module is actually about hold
    /// for the wrong reason. The rule itself is covered in `cairn-common`.
    fn mcp_config() -> McpConfigFingerprint {
        McpConfigFingerprint {
            algorithm: "sha256".to_string(),
            encoding_version: 1,
            digest: "a".repeat(64),
        }
    }

    fn grant(id: &str, scope: AuthorityScope, lifetime: AuthorityLifetime) -> AuthorityGrant {
        AuthorityGrant {
            id: id.to_string(),
            scope,
            principal: AuthorityPrincipal {
                node_uri: Some("cairn://p/CAIRN/1/1/builder".to_string()),
                run_id: Some("run-1".to_string()),
                agent_id: Some("build".to_string()),
            },
            audience: AuthorityAudience::workspace("default"),
            lifetime,
            constraints: AuthorityConstraintSet::new(vec![AuthorityConstraint::McpConfig {
                fingerprint: mcp_config(),
            }]),
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".to_string(),
                ..Default::default()
            },
            created_at: 1000,
            expires_at: None,
            consumed_at: None,
            revoked_at: None,
        }
    }

    fn request(scope: AuthorityScope) -> AuthorityRequest {
        AuthorityRequest::new(scope, AuthorityMutation::Update, "summary".to_string())
            .with_mcp_config(mcp_config())
    }

    async fn db() -> LocalDb {
        crate::storage::migrated_test_db("authority-grants.db").await
    }

    fn attribution(subject: &str) -> (PrincipalRef, AppearanceSnapshot) {
        let actor = PrincipalRef::Human {
            issuer: "https://identity.example".to_string(),
            subject: subject.to_string(),
            organization: Some("acme".to_string()),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            Some("https://identity.example".to_string()),
            Some(subject.to_string()),
            None,
            None,
            VerificationStrength::new("strong").unwrap(),
            900,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedOperator,
            Address::Invoke { origin: None },
            verification,
            900,
            None,
        )
        .unwrap();
        let snapshot = AppearanceSnapshot::new(actor.clone(), evidence, vec![], None).unwrap();
        (actor, snapshot)
    }

    #[tokio::test]
    async fn a_grant_round_trips_through_storage() {
        let db = db().await;
        let mut original = grant(
            "g1",
            tool_scope("linear"),
            AuthorityLifetime::Session {
                session_id: "sess-1".to_string(),
            },
        );
        original.constraints =
            AuthorityConstraintSet::new(vec![AuthorityConstraint::MutationModes {
                modes: vec![AuthorityMutation::Update],
            }]);
        original.expires_at = Some(9999);
        insert_grant(&db, &original).await.unwrap();

        let read_back = get_grant(&db, "g1").await.unwrap().expect("grant exists");
        assert_eq!(read_back, original);
    }

    #[tokio::test]
    async fn typed_grant_attribution_round_trips_and_queries_by_actor() {
        let db = db().await;
        let (actor, snapshot) = attribution("operator-1");
        let mut original = grant("typed", tool_scope("linear"), AuthorityLifetime::Standing);
        original.provenance.decision_actor = Some(actor.clone());
        original.provenance.appearance_snapshot = Some(snapshot.clone());
        insert_grant(&db, &original).await.unwrap();

        assert_eq!(get_grant(&db, "typed").await.unwrap(), Some(original));
        let found = grants_by_actor(&db, "default", &actor, 10).await.unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].provenance.appearance_snapshot.as_ref(),
            Some(&snapshot)
        );

        let (other, _) = attribution("operator-2");
        assert!(grants_by_actor(&db, "default", &other, 10)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn mismatched_or_partial_grant_attribution_is_refused() {
        let db = db().await;
        let (actor, snapshot) = attribution("operator-1");
        let mut partial = grant("partial", tool_scope("linear"), AuthorityLifetime::Standing);
        partial.provenance.decision_actor = Some(actor.clone());
        assert!(insert_grant(&db, &partial).await.is_err());

        let mut mismatched = grant(
            "mismatch",
            tool_scope("linear"),
            AuthorityLifetime::Standing,
        );
        let (other, _) = attribution("operator-2");
        mismatched.provenance.decision_actor = Some(other);
        mismatched.provenance.appearance_snapshot = Some(snapshot);
        assert!(insert_grant(&db, &mismatched).await.is_err());
    }

    #[tokio::test]
    async fn historical_null_attribution_decodes_without_fabrication() {
        let db = db().await;
        let original = grant("legacy", tool_scope("linear"), AuthorityLifetime::Standing);
        insert_grant(&db, &original).await.unwrap();
        let decoded = get_grant(&db, "legacy").await.unwrap().unwrap();
        assert!(decoded.provenance.decision_actor.is_none());
        assert!(decoded.provenance.appearance_snapshot.is_none());
    }

    #[tokio::test]
    async fn candidates_narrow_by_scope_key_and_workspace() {
        let db = db().await;
        insert_grant(
            &db,
            &grant(
                "g-linear",
                tool_scope("linear"),
                AuthorityLifetime::Standing,
            ),
        )
        .await
        .unwrap();
        insert_grant(
            &db,
            &grant(
                "g-github",
                tool_scope("github"),
                AuthorityLifetime::Standing,
            ),
        )
        .await
        .unwrap();

        let linear = candidate_grants(&db, "default", &tool_scope("linear").shorthand())
            .await
            .unwrap();
        assert_eq!(linear.len(), 1);
        assert_eq!(linear[0].id, "g-linear");

        // A different workspace shares nothing, even for the same tool name.
        let elsewhere = candidate_grants(&db, "other", &tool_scope("linear").shorthand())
            .await
            .unwrap();
        assert!(elsewhere.is_empty());
    }

    #[tokio::test]
    async fn concurrent_use_of_a_once_grant_authorizes_exactly_one_mutation() {
        let db = db().await;
        insert_grant(
            &db,
            &grant(
                "g-once",
                tool_scope("linear"),
                AuthorityLifetime::Once {
                    request_id: "perm-1".to_string(),
                },
            ),
        )
        .await
        .unwrap();

        // Both racers see an unconsumed grant; the conditional UPDATE is what
        // decides. Exactly one may win, or the whole single-use guarantee is a
        // suggestion.
        let (first, second) = tokio::join!(
            consume_grant(&db, "g-once", 2000),
            consume_grant(&db, "g-once", 2000)
        );
        let winners = [first.unwrap(), second.unwrap()]
            .into_iter()
            .filter(|won| *won)
            .count();
        assert_eq!(winners, 1, "exactly one caller may consume a once grant");

        let after = get_grant(&db, "g-once").await.unwrap().unwrap();
        assert!(after.consumed_at.is_some());
        assert!(!after.matches(&request(tool_scope("linear")), &live_context(), 2001));
    }

    fn live_context() -> AuthorityContext {
        AuthorityContext {
            audience: Some(AuthorityAudience::workspace("default")),
            run_id: Some("run-1".to_string()),
            turn_id: Some("turn-1".to_string()),
            session_id: Some("sess-1".to_string()),
            request_id: None,
        }
    }

    #[tokio::test]
    async fn revocation_immediately_blocks_a_standing_grant() {
        let db = db().await;
        insert_grant(
            &db,
            &grant(
                "g-standing",
                tool_scope("linear"),
                AuthorityLifetime::Standing,
            ),
        )
        .await
        .unwrap();

        let before = get_grant(&db, "g-standing").await.unwrap().unwrap();
        assert!(before.matches(&request(tool_scope("linear")), &live_context(), 2000));

        assert!(revoke_grant(&db, "g-standing", Some("mitch"), 2001)
            .await
            .unwrap());
        // Revoking twice reports that nothing changed rather than claiming a
        // second revocation happened.
        assert!(!revoke_grant(&db, "g-standing", Some("mitch"), 2002)
            .await
            .unwrap());

        let after = get_grant(&db, "g-standing").await.unwrap().unwrap();
        assert!(!after.matches(&request(tool_scope("linear")), &live_context(), 2002));
        assert_eq!(after.status(2002), "revoked");
    }

    #[tokio::test]
    async fn a_revoked_grant_can_never_be_consumed() {
        let db = db().await;
        insert_grant(
            &db,
            &grant(
                "g-once",
                tool_scope("linear"),
                AuthorityLifetime::Once {
                    request_id: "perm-1".to_string(),
                },
            ),
        )
        .await
        .unwrap();
        revoke_grant(&db, "g-once", None, 2000).await.unwrap();
        assert!(!consume_grant(&db, "g-once", 2001).await.unwrap());
    }

    #[tokio::test]
    async fn an_unreadable_constraint_version_fails_closed_instead_of_being_ignored() {
        let db = db().await;
        let mut g = grant(
            "g-future",
            tool_scope("linear"),
            AuthorityLifetime::Standing,
        );
        g.constraints = AuthorityConstraintSet::new(vec![]);
        insert_grant(&db, &g).await.unwrap();
        // Simulate a grant written by a future encoding.
        db.write(move |conn| {
            Box::pin(async move {
                conn.execute(
                    "UPDATE authority_grants SET constraints_json = ?1 WHERE id = 'g-future'",
                    params![r#"{"version":99,"constraints":[]}"#],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        // Reading errors rather than yielding a grant with its constraints
        // quietly dropped, so the caller re-prompts instead of over-authorizing.
        let error = candidate_grants(&db, "default", &tool_scope("linear").shorthand())
            .await
            .expect_err("an unreadable grant must not parse");
        assert!(error.to_string().contains("not understood"), "got: {error}");
    }

    #[tokio::test]
    async fn an_allow_event_must_cite_its_grant() {
        let db = db().await;
        let base = NewAuthorizationEvent {
            scope: tool_scope("linear"),
            mutation: "update".to_string(),
            summary: "reconfigure workspace MCP server 'linear'".to_string(),
            outcome: "allowed_by_grant".to_string(),
            reason: "workspace_tool_capability".to_string(),
            principal: AuthorityPrincipal::default(),
            audience: AuthorityAudience::workspace("default"),
            run_id: Some("run-1".to_string()),
            request_uri: None,
            grant_id: None,
            decision_actor: None,
            appearance_snapshot: None,
            decided_at: 3000,
        };
        assert!(append_event(&db, base.clone()).await.is_err());

        let cited = NewAuthorizationEvent {
            grant_id: Some("g1".to_string()),
            ..base
        };
        append_event(&db, cited).await.unwrap();
        let events = events_citing_grant(&db, "g1").await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].outcome, "allowed_by_grant");
        assert_eq!(events[0].scope, tool_scope("linear"));
    }

    #[tokio::test]
    async fn typed_event_attribution_round_trips_and_queries_by_actor() {
        let db = db().await;
        let (actor, snapshot) = attribution("operator-1");
        append_event(
            &db,
            NewAuthorizationEvent {
                scope: tool_scope("linear"),
                mutation: "create".to_string(),
                summary: "install linear".to_string(),
                outcome: "approval_required".to_string(),
                reason: "workspace_tool_capability".to_string(),
                principal: AuthorityPrincipal::default(),
                audience: AuthorityAudience::workspace("default"),
                run_id: Some("run-1".to_string()),
                request_uri: None,
                grant_id: None,
                decision_actor: Some(actor.clone()),
                appearance_snapshot: Some(snapshot.clone()),
                decided_at: 3000,
            },
        )
        .await
        .unwrap();

        let events = events_by_actor(&db, "default", &actor, 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].appearance_snapshot.as_ref(), Some(&snapshot));
    }

    #[tokio::test]
    async fn invalid_stored_snapshot_transport_fails_closed() {
        let db = db().await;
        let (actor, snapshot) = attribution("operator-1");
        let mut original = grant(
            "invalid-snapshot",
            tool_scope("linear"),
            AuthorityLifetime::Standing,
        );
        original.provenance.decision_actor = Some(actor);
        original.provenance.appearance_snapshot = Some(snapshot);
        insert_grant(&db, &original).await.unwrap();
        db.write(move |conn| Box::pin(async move {
            conn.execute(
                "UPDATE authority_grants SET appearance_snapshot_json = replace(appearance_snapshot_json, 'authenticated_operator', 'future_transport') WHERE id='invalid-snapshot'",
                (),
            ).await?;
            Ok(())
        })).await.unwrap();
        assert!(get_grant(&db, "invalid-snapshot").await.is_err());
    }

    #[tokio::test]
    async fn a_prompt_is_journaled_with_its_stable_reason_and_no_grant() {
        let db = db().await;
        append_event(
            &db,
            NewAuthorizationEvent {
                scope: tool_scope("linear"),
                mutation: "create".to_string(),
                summary: "install workspace MCP server 'linear'".to_string(),
                outcome: "approval_required".to_string(),
                reason: "workspace_tool_capability".to_string(),
                principal: AuthorityPrincipal::default(),
                audience: AuthorityAudience::workspace("default"),
                run_id: Some("run-1".to_string()),
                request_uri: None,
                grant_id: None,
                decision_actor: None,
                appearance_snapshot: None,
                decided_at: 3000,
            },
        )
        .await
        .unwrap();
        let events = list_events(&db, "default", 10).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "workspace_tool_capability");
        assert!(events[0].grant_id.is_none());
    }

    #[tokio::test]
    async fn an_anchored_lifetime_cannot_be_stored_without_its_anchor() {
        // The table's CHECK is the backstop for the invariant the type system
        // states: only a standing grant is unanchored. Without it a corrupted
        // row could read back as broader authority than was ever granted.
        let db = db().await;
        let result = db
            .write(move |conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO authority_grants (id,scope_key,place_kind,action,scope_json,\
                         constraints_json,principal_json,audience_json,workspace_id,lifetime_kind,\
                         lifetime_anchor,provenance_json,created_at) \
                         VALUES ('bad','k','tool','write','{}','{}','{}','{}','default','session',\
                         NULL,'{}',1)",
                        (),
                    )
                    .await?;
                    Ok(())
                })
            })
            .await;
        assert!(
            result.is_err(),
            "an unanchored session grant must be refused"
        );
    }
}
