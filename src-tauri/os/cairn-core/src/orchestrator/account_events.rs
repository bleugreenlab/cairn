//! The provider sign-in lifecycle journal, `account_credential_events`.
//!
//! Every door that signs a subscription account in — Settings → Providers and
//! `cairn auth login` through the transport's invoke commands, and
//! `cairn://models/accounts` — starts it through
//! [`Orchestrator::start_provider_sign_in`], which records who started it
//! before anything starts. Its terminal outcome arrives through
//! `ModelHost::sign_in_settled`, exactly once per sign-in. The two rows are
//! correlated by the sign-in's opaque reference, so a reader can say "the flow
//! started by X reached outcome Y" without claiming X performed the provider's
//! browser step.
//!
//! The table is append-only by trigger and private, like the authorization
//! journal (`docs/authorization.md`).

use std::sync::Arc;

use cairn_common::identity::{AppearanceSnapshot, PrincipalPosition, PrincipalRef};
use cairn_db::turso::params;
use cairn_models::accounts::RoutedProvider;
use cairn_models::subscription::{
    SignInError, SignInFailureKind, SignInOutcome, SignInRequest, SignInStart, SignInTarget,
};

use super::Orchestrator;
use crate::storage::{LocalDb, RowExt};

/// Who started a sign-in, as the door that started it resolved them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignInInitiator {
    attribution: Option<(PrincipalRef, AppearanceSnapshot)>,
    authorization_event_id: Option<String>,
}

impl SignInInitiator {
    /// An initiator the door authenticated. The principal must be valid as a
    /// decision actor (an agent carries its run) and be the one the
    /// appearance names.
    pub fn attributed(
        principal: PrincipalRef,
        appearance: AppearanceSnapshot,
        authorization_event_id: Option<String>,
    ) -> Result<Self, String> {
        principal
            .validate_at(PrincipalPosition::DecisionActor)
            .map_err(|error| format!("invalid sign-in initiator: {error}"))?;
        appearance
            .validate()
            .map_err(|error| format!("invalid sign-in appearance: {error}"))?;
        if appearance.principal() != &principal {
            return Err("the sign-in initiator must be the appearance's principal".to_string());
        }
        Ok(Self {
            attribution: Some((principal, appearance)),
            authorization_event_id,
        })
    }

    /// The truthful answer for a door whose caller carried no attributable
    /// appearance (an anonymous local invoke). Never a door's exemption.
    pub fn unattributed() -> Self {
        Self {
            attribution: None,
            authorization_event_id: None,
        }
    }
}

/// The kind of one journal row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialEventKind {
    Started,
    Completed,
    Failed,
    Cancelled,
}

impl CredentialEventKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "sign_in_started",
            Self::Completed => "sign_in_completed",
            Self::Failed => "sign_in_failed",
            Self::Cancelled => "sign_in_cancelled",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "sign_in_started" => Self::Started,
            "sign_in_completed" => Self::Completed,
            "sign_in_failed" => Self::Failed,
            "sign_in_cancelled" => Self::Cancelled,
            _ => return None,
        })
    }
}

/// One journal row, as a reader may see it: no appearance, no reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialEvent {
    pub kind: CredentialEventKind,
    pub provider: String,
    pub account_id: Option<String>,
    /// On a start only.
    pub initiator: Option<PrincipalRef>,
    pub failure_kind: Option<SignInFailureKind>,
    pub email: Option<String>,
    pub plan: Option<String>,
    pub occurred_at: i64,
}

fn encode(value: &impl serde::Serialize, what: &str) -> Result<String, String> {
    serde_json::to_string(value).map_err(|error| format!("{what} is not serializable: {error}"))
}

/// Record that a door started a sign-in.
pub(crate) async fn append_started(
    db: &LocalDb,
    sign_in_ref: &str,
    provider: RoutedProvider,
    account_id: Option<&str>,
    initiator: &SignInInitiator,
    at: i64,
) -> Result<(), String> {
    let (initiator_json, appearance_json) = match &initiator.attribution {
        Some((principal, appearance)) => (
            Some(encode(principal, "initiator")?),
            Some(encode(appearance, "appearance")?),
        ),
        None => (None, None),
    };
    db.execute(
        "INSERT INTO account_credential_events \
         (id, sign_in_ref, provider, account_id, kind, initiator_json, appearance_json, \
          authorization_event_id, occurred_at) \
         VALUES (?1, ?2, ?3, ?4, 'sign_in_started', ?5, ?6, ?7, ?8)",
        params![
            uuid::Uuid::new_v4().to_string(),
            sign_in_ref,
            provider.backend(),
            account_id.map(ToString::to_string),
            initiator_json,
            appearance_json,
            initiator.authorization_event_id.clone(),
            at
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("could not record the sign-in's start: {error}"))
}

/// Record how a sign-in ended. The journal's unique index refuses a second
/// end for one sign-in.
pub(crate) async fn append_settled(
    db: &LocalDb,
    sign_in_ref: &str,
    provider: RoutedProvider,
    outcome: &SignInOutcome,
    at: i64,
) -> Result<(), String> {
    let (kind, account_id, failure_kind, email, plan) = match outcome {
        SignInOutcome::Completed {
            account_id,
            email,
            plan,
        } => (
            CredentialEventKind::Completed,
            Some(account_id.clone()),
            None,
            email.clone(),
            plan.clone(),
        ),
        SignInOutcome::Failed { kind, account_id } => (
            CredentialEventKind::Failed,
            account_id.clone(),
            Some(kind.as_str()),
            None,
            None,
        ),
        SignInOutcome::Cancelled { account_id } => (
            CredentialEventKind::Cancelled,
            account_id.clone(),
            None,
            None,
            None,
        ),
    };
    db.execute(
        "INSERT INTO account_credential_events \
         (id, sign_in_ref, provider, account_id, kind, failure_kind, email, plan, occurred_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            uuid::Uuid::new_v4().to_string(),
            sign_in_ref,
            provider.backend(),
            account_id,
            kind.as_str(),
            failure_kind.map(ToString::to_string),
            email,
            plan,
            at
        ],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("could not record the sign-in's end: {error}"))
}

/// Every row of every sign-in that touched `account_id`, oldest first. A new
/// account's start row precedes its id, so rows are gathered by the sign-in
/// references the account appears under.
pub(crate) async fn history_for_account(
    db: &LocalDb,
    account_id: &str,
) -> Result<Vec<CredentialEvent>, String> {
    db.query_all(
        "SELECT kind, provider, account_id, initiator_json, failure_kind, email, plan, occurred_at \
         FROM account_credential_events \
         WHERE sign_in_ref IN (SELECT sign_in_ref FROM account_credential_events WHERE account_id = ?1) \
         ORDER BY occurred_at, rowid",
        (account_id.to_string(),),
        |row| {
            Ok((
                row.text(0)?,
                row.text(1)?,
                row.opt_text(2)?,
                row.opt_text(3)?,
                row.opt_text(4)?,
                row.opt_text(5)?,
                row.opt_text(6)?,
                row.i64(7)?,
            ))
        },
    )
    .await
    .map_err(|error| format!("could not read sign-in history: {error}"))?
    .into_iter()
    .map(
        |(kind, provider, account_id, initiator, failure_kind, email, plan, occurred_at)| {
            Ok(CredentialEvent {
                kind: CredentialEventKind::parse(&kind)
                    .ok_or_else(|| format!("unknown sign-in event kind '{kind}'"))?,
                provider,
                account_id,
                initiator: initiator
                    .map(|json| serde_json::from_str::<PrincipalRef>(&json))
                    .transpose()
                    .map_err(|error| format!("unreadable sign-in initiator: {error}"))?,
                failure_kind: failure_kind.as_deref().and_then(SignInFailureKind::parse),
                email,
                plan,
                occurred_at,
            })
        },
    )
    .collect()
}

impl Orchestrator {
    /// Start signing a subscription account in: the one entry every door uses.
    ///
    /// Records who started it first, under a reference minted here, and starts
    /// nothing if that cannot be recorded: a credential-bearing act with no
    /// provenance is not one this install performs. The flow's end is recorded
    /// when the subscription mechanics report it.
    pub async fn start_provider_sign_in(
        &self,
        provider: RoutedProvider,
        target: SignInTarget,
        project_id: Option<String>,
        initiator: SignInInitiator,
    ) -> Result<SignInStart, SignInError> {
        let sign_in_ref = uuid::Uuid::new_v4().to_string();
        append_started(
            &self.db.local,
            &sign_in_ref,
            provider,
            target.account_id(),
            &initiator,
            chrono::Utc::now().timestamp(),
        )
        .await
        .map_err(|error| SignInError::Failed(format!("{error}. Nothing was started.")))?;
        self.subscriptions
            .start_sign_in(
                Arc::new(self.clone()),
                SignInRequest {
                    sign_in_ref,
                    provider,
                    target,
                    project_id,
                },
            )
            .await
    }

    /// Record a sign-in's end. Never the reason anything fails: a missing row
    /// is a real gap, so it is logged at warn.
    pub(crate) fn record_sign_in_settled(
        &self,
        sign_in_ref: &str,
        provider: RoutedProvider,
        outcome: SignInOutcome,
    ) {
        let db = self.db.local.clone();
        let sign_in_ref = sign_in_ref.to_string();
        let recorded = crate::storage::run_db_blocking(move || async move {
            append_settled(
                &db,
                &sign_in_ref,
                provider,
                &outcome,
                chrono::Utc::now().timestamp(),
            )
            .await
        });
        if let Err(error) = recorded {
            log::warn!("{error}");
        }
    }

    /// The sign-in history of one account, oldest first.
    pub async fn account_sign_in_history(
        &self,
        account_id: &str,
    ) -> Result<Vec<CredentialEvent>, String> {
        history_for_account(&self.db.local, account_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent() -> (PrincipalRef, AppearanceSnapshot) {
        crate::mcp::handlers::run_context::AuthenticatedAgent {
            home: "cairn://p/cairn/1/1/builder".to_string(),
            run_id: "run-1".to_string(),
            job_id: "job-1".to_string(),
        }
        .appearance(1)
        .unwrap()
    }

    async fn count(db: &LocalDb) -> i64 {
        db.query_opt_i64("SELECT COUNT(*) FROM account_credential_events", ())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn the_journal_is_append_only() {
        let db = crate::storage::migrated_test_db("account-events-immutable.db").await;
        let (principal, appearance) = agent();
        let initiator =
            SignInInitiator::attributed(principal, appearance, Some("authz-1".to_string()))
                .unwrap();
        append_started(&db, "ref-1", RoutedProvider::Claude, None, &initiator, 1)
            .await
            .unwrap();

        assert!(db
            .execute(
                "UPDATE account_credential_events SET account_id = 'acc_x'",
                ()
            )
            .await
            .is_err());
        assert!(db
            .execute("DELETE FROM account_credential_events", ())
            .await
            .is_err());
        assert_eq!(count(&db).await, 1);
    }

    #[tokio::test]
    async fn one_start_and_one_end_per_sign_in() {
        let db = crate::storage::migrated_test_db("account-events-unique.db").await;
        let initiator = SignInInitiator::unattributed();
        append_started(&db, "ref-1", RoutedProvider::Codex, None, &initiator, 1)
            .await
            .unwrap();
        assert!(
            append_started(&db, "ref-1", RoutedProvider::Codex, None, &initiator, 2)
                .await
                .is_err()
        );
        let cancelled = SignInOutcome::Cancelled { account_id: None };
        append_settled(&db, "ref-1", RoutedProvider::Codex, &cancelled, 3)
            .await
            .unwrap();
        let completed = SignInOutcome::Completed {
            account_id: "acc_1".to_string(),
            email: None,
            plan: None,
        };
        assert!(
            append_settled(&db, "ref-1", RoutedProvider::Codex, &completed, 4)
                .await
                .is_err(),
            "a contradictory second end is refused"
        );
        assert_eq!(count(&db).await, 2);
    }

    /// The database itself holds the attribution and reason rules, whatever a
    /// writer does.
    #[tokio::test]
    async fn the_schema_holds_attribution_and_reason_rules() {
        let db = crate::storage::migrated_test_db("account-events-checks.db").await;
        let insert = |kind: &str,
                      initiator: Option<&str>,
                      appearance: Option<&str>,
                      failure: Option<&str>| {
            let db = &db;
            let sql = "INSERT INTO account_credential_events \
                 (id, sign_in_ref, provider, kind, initiator_json, appearance_json, failure_kind, occurred_at) \
                 VALUES (?1, ?2, 'claude', ?3, ?4, ?5, ?6, 1)";
            let values = params![
                uuid::Uuid::new_v4().to_string(),
                uuid::Uuid::new_v4().to_string(),
                kind.to_string(),
                initiator.map(ToString::to_string),
                appearance.map(ToString::to_string),
                failure.map(ToString::to_string)
            ];
            async move { db.execute(sql, values).await }
        };
        // Both-or-neither.
        assert!(insert("sign_in_started", Some("{}"), None, None)
            .await
            .is_err());
        assert!(insert("sign_in_started", None, Some("{}"), None)
            .await
            .is_err());
        assert!(insert("sign_in_started", Some("{}"), Some("{}"), None)
            .await
            .is_ok());
        assert!(insert("sign_in_started", None, None, None).await.is_ok());
        // Only a start names an initiator.
        assert!(insert("sign_in_cancelled", Some("{}"), Some("{}"), None)
            .await
            .is_err());
        // A failure says why, from the closed set; nothing else does.
        assert!(insert("sign_in_failed", None, None, None).await.is_err());
        assert!(insert("sign_in_failed", None, None, Some("it broke"))
            .await
            .is_err());
        assert!(insert("sign_in_failed", None, None, Some("timeout"))
            .await
            .is_ok());
        assert!(insert("sign_in_cancelled", None, None, Some("timeout"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn history_follows_a_new_account_back_to_its_start() {
        let db = crate::storage::migrated_test_db("account-events-history.db").await;
        let (principal, appearance) = agent();
        let initiator = SignInInitiator::attributed(principal.clone(), appearance, None).unwrap();
        append_started(&db, "ref-new", RoutedProvider::Claude, None, &initiator, 10)
            .await
            .unwrap();
        append_settled(
            &db,
            "ref-new",
            RoutedProvider::Claude,
            &SignInOutcome::Completed {
                account_id: "acc_1".to_string(),
                email: Some("op@example.com".to_string()),
                plan: Some("max".to_string()),
            },
            11,
        )
        .await
        .unwrap();
        append_started(
            &db,
            "ref-other",
            RoutedProvider::Claude,
            Some("acc_2"),
            &SignInInitiator::unattributed(),
            12,
        )
        .await
        .unwrap();

        let history = history_for_account(&db, "acc_1").await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].kind, CredentialEventKind::Started);
        assert_eq!(history[0].initiator.as_ref(), Some(&principal));
        assert_eq!(history[0].account_id, None, "the start is never backfilled");
        assert_eq!(history[1].kind, CredentialEventKind::Completed);
        assert_eq!(history[1].initiator, None, "an end names no principal");
        assert_eq!(history[1].email.as_deref(), Some("op@example.com"));
    }

    #[test]
    fn an_initiator_must_be_its_appearance_s_principal() {
        let (_, appearance) = agent();
        let other = PrincipalRef::Agent {
            node: "cairn://p/cairn/2/1/builder".to_string(),
            run_id: Some("run-2".to_string()),
        };
        assert!(SignInInitiator::attributed(other, appearance.clone(), None).is_err());
        let runless = PrincipalRef::Agent {
            node: "cairn://p/cairn/1/1/builder".to_string(),
            run_id: None,
        };
        assert!(SignInInitiator::attributed(runless, appearance, None).is_err());
    }
}
