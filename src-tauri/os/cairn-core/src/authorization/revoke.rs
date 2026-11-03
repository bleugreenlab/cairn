//! Who may revoke an authority grant.
//!
//! Revoking only ever removes authority, so it is never an escalation. It can
//! still undo the operator's decision for someone else: one agent removing
//! another principal's standing grant takes away what the operator chose to
//! give that principal. So a principal revokes only the grants it holds, and
//! anyone else's is the operator's to revoke.
//!
//! The operator never revokes through `cairn://grants`. Every write there
//! arrives over the agent callback, where a caller with no run presents only
//! the session token, which every local process can present (agents
//! included). The operator's door is the `revoke_authority_grant` invoke,
//! which the runner admits only with operator authority and the desktop
//! reaches through its native proxy (Settings → Grants).

use cairn_common::authorization::{AuthorityGrant, AuthorityLifetime};
use cairn_common::identity::PrincipalRef;

use crate::storage::LocalDb;

/// Where an agent is sent to have a grant it does not hold revoked. Named
/// once so the refusal and the affordance cannot drift apart from the page.
pub const OPERATOR_REVOKE_SURFACE: &str = "Settings → Grants in the Cairn desktop app";

/// Whose a grant is, for revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantHolder {
    /// A once, turn, or session grant: bound to the run it was minted for.
    Run {
        run_id: String,
        node_uri: Option<String>,
    },
    /// A standing grant bound to one actor, such as a served-API client.
    Actor(PrincipalRef),
    /// A grant an `ActingThread` constraint binds to one thread (an
    /// execution-queue grant), by thread id: the thread holds it whatever its
    /// lifetime, and a once, turn or session one is also held by the run it
    /// was minted for.
    Thread {
        thread_id: String,
        run_id: Option<String>,
    },
    /// A standing grant with no principal, or a grant naming no one to hold
    /// it. It is workspace-wide, so it is nobody's but the operator's.
    Operator,
}

impl GrantHolder {
    /// The holder recorded on `grant`.
    ///
    /// A standing grant's `run_id` is provenance (the run that asked when the
    /// operator chose "always"), not a binding: a standing grant answers for
    /// every run, so that run does not hold it.
    pub fn of(grant: &AuthorityGrant) -> Self {
        if let Some(thread_id) = grant.constraints.acting_thread() {
            let run_id = match &grant.lifetime {
                AuthorityLifetime::Standing => None,
                AuthorityLifetime::Once { .. }
                | AuthorityLifetime::Turn { .. }
                | AuthorityLifetime::Session { .. } => grant
                    .principal
                    .run_id
                    .clone()
                    .filter(|run_id| !run_id.is_empty()),
            };
            return GrantHolder::Thread {
                thread_id: thread_id.to_string(),
                run_id,
            };
        }
        match &grant.lifetime {
            AuthorityLifetime::Standing => match &grant.principal.actor {
                Some(actor) => GrantHolder::Actor(actor.clone()),
                None => GrantHolder::Operator,
            },
            AuthorityLifetime::Once { .. }
            | AuthorityLifetime::Turn { .. }
            | AuthorityLifetime::Session { .. } => match grant.principal.run_id.as_deref() {
                Some(run_id) if !run_id.is_empty() => GrantHolder::Run {
                    run_id: run_id.to_string(),
                    node_uri: grant.principal.node_uri.clone(),
                },
                _ => GrantHolder::Operator,
            },
        }
    }

    /// The holder as a refusal or a listing names it.
    pub fn describe(&self) -> String {
        match self {
            GrantHolder::Run {
                run_id,
                node_uri: Some(node),
            } => format!("{node} (run {run_id})"),
            GrantHolder::Run {
                run_id,
                node_uri: None,
            } => format!("run {run_id}"),
            GrantHolder::Actor(actor) => describe_principal(actor),
            GrantHolder::Thread { thread_id, .. } => format!("thread {thread_id}"),
            GrantHolder::Operator => "the operator (a workspace-wide standing grant)".to_string(),
        }
    }
}

/// A principal as a sentence names it.
pub fn describe_principal(principal: &PrincipalRef) -> String {
    match principal {
        PrincipalRef::Human {
            issuer, subject, ..
        } => format!("person {subject} ({issuer})"),
        PrincipalRef::Agent { node, .. } => node.clone(),
        PrincipalRef::Machine { device_id } => format!("machine {device_id}"),
        PrincipalRef::External {
            provider,
            namespace,
            id,
        } => format!("{provider} {namespace} {id}"),
    }
}

/// Who is asking to revoke, as the runner resolved it from the request (never
/// from anything the request body says).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Revoker {
    pub run_id: Option<String>,
    pub node_uri: Option<String>,
    pub actor: Option<PrincipalRef>,
    /// The thread the run acts for, resolved as a queue request resolves it.
    pub acting_thread: Option<String>,
}

impl Revoker {
    /// The revoker as the grant's `revoked_by` records it.
    pub fn label(&self) -> String {
        match (&self.node_uri, &self.run_id, &self.actor) {
            (Some(node), Some(run), _) => format!("{node} (run {run})"),
            (None, Some(run), _) => format!("run {run}"),
            (_, None, Some(actor)) => describe_principal(actor),
            _ => "a caller with no run".to_string(),
        }
    }

    /// Whether this caller holds `holder`'s grant. The operator holds nothing
    /// here: an unbound grant is refused to every caller of this check, and
    /// the operator revokes it through [`revoke_as_operator`].
    pub fn holds(&self, holder: &GrantHolder) -> bool {
        match holder {
            GrantHolder::Run { run_id, .. } => self.run_id.as_deref() == Some(run_id.as_str()),
            GrantHolder::Actor(actor) => self.actor.as_ref() == Some(actor),
            GrantHolder::Thread { thread_id, run_id } => {
                self.acting_thread.as_ref() == Some(thread_id)
                    || run_id
                        .as_deref()
                        .is_some_and(|run_id| self.run_id.as_deref() == Some(run_id))
            }
            GrantHolder::Operator => false,
        }
    }
}

/// Decide whether `revoker` may revoke `grant` through `cairn://grants`, and
/// say why not when it may not.
pub fn check_holder(
    grant_label: &str,
    grant: &AuthorityGrant,
    revoker: &Revoker,
) -> Result<(), String> {
    let holder = GrantHolder::of(grant);
    if revoker.holds(&holder) {
        return Ok(());
    }
    Err(match holder {
        GrantHolder::Operator => format!(
            "Refused: authority grant '{grant_label}' is held by {}, so only the operator may \
             revoke it, at {OPERATOR_REVOKE_SURFACE}. This caller is {}.",
            holder.describe(),
            revoker.label()
        ),
        _ => format!(
            "Refused: authority grant '{grant_label}' is held by {}, not by this caller ({}). \
             A principal revokes only the grants it holds; the operator revokes any grant at \
             {OPERATOR_REVOKE_SURFACE}.",
            holder.describe(),
            revoker.label()
        ),
    })
}

/// Why the operator's revoke did not happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorRevokeRefusal {
    NotFound,
    /// The grant is no longer active (`revoked`, `consumed`, or `expired`),
    /// whether it was before the operator asked or became so in between.
    NotActive(&'static str),
    Storage(String),
}

impl std::fmt::Display for OperatorRevokeRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorRevokeRefusal::NotFound => write!(f, "No authority grant has that id."),
            OperatorRevokeRefusal::NotActive(status) => write!(
                f,
                "That grant is already {status}, so there is nothing to revoke."
            ),
            OperatorRevokeRefusal::Storage(error) => write!(f, "{error}"),
        }
    }
}

/// The operator's revoke: any grant, re-read first so a grant that stopped
/// authorizing since the listing was drawn is reported as such rather than
/// "revoked" again. `operator` is the label the runner derived from the
/// verified invoke identity, never a name the caller supplied.
///
/// The store's revoke is conditional on the grant being unrevoked, so two
/// racing revokes cannot both succeed; the loser is told the grant is
/// already revoked.
pub async fn revoke_as_operator(
    db: &LocalDb,
    grant_id: &str,
    operator: &str,
) -> Result<AuthorityGrant, OperatorRevokeRefusal> {
    let stored = crate::resources::grants::stored_grant_id(grant_id);
    let grant = super::get_grant(db, &stored)
        .await
        .map_err(OperatorRevokeRefusal::Storage)?
        .ok_or(OperatorRevokeRefusal::NotFound)?;
    let at = chrono::Utc::now().timestamp();
    if grant.is_spent(at) {
        return Err(OperatorRevokeRefusal::NotActive(grant.status(at)));
    }
    if !super::revoke_grant(db, &stored, Some(operator))
        .await
        .map_err(OperatorRevokeRefusal::Storage)?
    {
        return Err(OperatorRevokeRefusal::NotActive("revoked"));
    }
    super::get_grant(db, &stored)
        .await
        .map_err(OperatorRevokeRefusal::Storage)?
        .ok_or(OperatorRevokeRefusal::NotFound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_common::authorization::{
        AuthorityAction, AuthorityAudience, AuthorityConstraint, AuthorityConstraintSet,
        AuthorityPlace, AuthorityPrincipal, AuthorityProvenance, AuthorityScope,
    };

    fn grant(lifetime: AuthorityLifetime, principal: AuthorityPrincipal) -> AuthorityGrant {
        AuthorityGrant {
            id: "g".into(),
            scope: AuthorityScope::new(
                AuthorityPlace::WorkspaceSettings {
                    workspace_id: "default".into(),
                    section: "tools".into(),
                },
                AuthorityAction::Write,
            ),
            principal,
            audience: AuthorityAudience::workspace("default"),
            lifetime,
            constraints: AuthorityConstraintSet::default(),
            provenance: AuthorityProvenance {
                issuer: "operator_prompt".into(),
                ..Default::default()
            },
            created_at: 1,
            expires_at: None,
            consumed_at: None,
            revoked_at: None,
        }
    }

    fn run_principal(run: &str) -> AuthorityPrincipal {
        AuthorityPrincipal {
            node_uri: Some(format!("cairn://p/demo/1/1/{run}")),
            run_id: Some(run.into()),
            ..Default::default()
        }
    }

    fn run(run: &str) -> Revoker {
        Revoker {
            run_id: Some(run.into()),
            node_uri: Some(format!("cairn://p/demo/1/1/{run}")),
            actor: None,
            acting_thread: None,
        }
    }

    fn thread_run(run: &str, thread: &str) -> Revoker {
        Revoker {
            acting_thread: Some(thread.into()),
            ..self::run(run)
        }
    }

    fn thread_bound(lifetime: AuthorityLifetime, thread: &str) -> AuthorityGrant {
        let mut grant = grant(lifetime, run_principal("run-a"));
        grant.constraints = AuthorityConstraintSet::new(vec![AuthorityConstraint::ActingThread {
            thread_id: thread.into(),
        }]);
        grant
    }

    /// A queue grant is the thread's, whatever its lifetime: any run acting
    /// for that thread revokes it, another thread's run is refused and told
    /// whose it is, and an anchored one is still its own run's.
    #[test]
    fn a_thread_bound_grant_is_its_threads() {
        for lifetime in anchored().into_iter().chain([AuthorityLifetime::Standing]) {
            let anchored = !matches!(lifetime, AuthorityLifetime::Standing);
            let grant = thread_bound(lifetime, "t-judgment");
            assert_eq!(
                check_holder("g", &grant, &thread_run("run-z", "t-judgment")),
                Ok(())
            );
            let refusal = check_holder("g", &grant, &thread_run("run-b", "t-ui")).unwrap_err();
            assert!(refusal.contains("held by thread t-judgment"), "{refusal}");
            assert!(refusal.contains(OPERATOR_REVOKE_SURFACE), "{refusal}");
            assert_eq!(
                check_holder("g", &grant, &run("run-a")).is_ok(),
                anchored,
                "a standing grant's run is provenance, not a holder"
            );
            assert!(check_holder("g", &grant, &Revoker::default()).is_err());
        }
    }

    fn client(id: &str) -> PrincipalRef {
        PrincipalRef::External {
            provider: "cairn".into(),
            namespace: "served-api-client".into(),
            id: id.into(),
        }
    }

    fn anchored() -> [AuthorityLifetime; 3] {
        [
            AuthorityLifetime::Once {
                request_id: "req".into(),
            },
            AuthorityLifetime::Turn {
                turn_id: "turn".into(),
            },
            AuthorityLifetime::Session {
                session_id: "session".into(),
            },
        ]
    }

    #[test]
    fn a_run_revokes_its_own_once_turn_and_session_grants() {
        for lifetime in anchored() {
            let grant = grant(lifetime, run_principal("run-a"));
            assert_eq!(check_holder("g", &grant, &run("run-a")), Ok(()));
        }
    }

    #[test]
    fn another_run_is_refused_and_told_whose_grant_it_is() {
        for lifetime in anchored() {
            let grant = grant(lifetime, run_principal("run-a"));
            let refusal = check_holder("g", &grant, &run("run-b")).unwrap_err();
            assert!(
                refusal.contains("held by cairn://p/demo/1/1/run-a (run run-a)"),
                "{refusal}"
            );
            assert!(refusal.contains("cairn://p/demo/1/1/run-b"), "{refusal}");
            assert!(refusal.contains(OPERATOR_REVOKE_SURFACE), "{refusal}");
        }
    }

    /// A standing grant's run is provenance, and an unbound standing grant is
    /// workspace-wide: not even the run that asked for it holds it.
    #[test]
    fn an_unbound_standing_grant_is_refused_to_every_caller() {
        let grant = grant(AuthorityLifetime::Standing, run_principal("run-a"));
        for caller in [run("run-a"), run("run-b"), Revoker::default()] {
            let refusal = check_holder("g", &grant, &caller).unwrap_err();
            assert!(
                refusal.contains("only the operator may revoke it"),
                "{refusal}"
            );
            assert!(refusal.contains(OPERATOR_REVOKE_SURFACE), "{refusal}");
        }
        assert!(check_holder("g", &grant, &Revoker::default())
            .unwrap_err()
            .contains("This caller is a caller with no run"));
    }

    #[test]
    fn an_actor_bound_standing_grant_is_its_actors_alone() {
        let grant = grant(
            AuthorityLifetime::Standing,
            AuthorityPrincipal {
                actor: Some(client("cli_dig")),
                ..Default::default()
            },
        );
        let dig = Revoker {
            actor: Some(client("cli_dig")),
            ..Default::default()
        };
        let sitrep = Revoker {
            actor: Some(client("cli_sitrep")),
            ..Default::default()
        };
        assert_eq!(check_holder("g", &grant, &dig), Ok(()));
        let refusal = check_holder("g", &grant, &sitrep).unwrap_err();
        assert!(
            refusal.contains("held by cairn served-api-client cli_dig"),
            "{refusal}"
        );
        assert!(check_holder("g", &grant, &run("run-a")).is_err());
    }

    /// An anchored grant that names no run has no holder but the operator; a
    /// run-less caller must not match its absent run.
    #[test]
    fn an_anchored_grant_with_no_run_is_the_operators() {
        let grant = grant(
            AuthorityLifetime::Session {
                session_id: "s".into(),
            },
            AuthorityPrincipal::default(),
        );
        assert_eq!(GrantHolder::of(&grant), GrantHolder::Operator);
        assert!(check_holder("g", &grant, &Revoker::default()).is_err());
    }
}
