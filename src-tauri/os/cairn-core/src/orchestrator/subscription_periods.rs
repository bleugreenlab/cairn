//! Keeping `subscription_periods` in step with the credential store.
//!
//! The credential lifecycle calls [`Orchestrator::reconcile_subscription_periods`]
//! after every change that can move an account's plan: an account added or
//! removed, a sign-in recorded, a credential rotated (a new ChatGPT id token
//! can name a new plan), Claude profile metadata measured with usage, a plan
//! chosen, a rename. Startup calls it too, which
//! is how accounts that predate the table get their first period, backdated
//! to when each was added.

use std::sync::{Arc, Mutex};

use cairn_analytics::subscriptions::{reconcile_periods, HeldSubscription};
use cairn_models::accounts::CredentialStore;

use super::Orchestrator;

/// What the store held when periods were last reconciled, so a lifecycle
/// event that moved no plan costs no database write.
pub(crate) type ReconciledSubscriptions = Arc<Mutex<Option<Vec<HeldSubscription>>>>;

/// Every subscription account in `store`, with the plan it holds now.
/// Project-private accounts are included: the workspace pays for them too.
pub(crate) fn held_subscriptions(store: &CredentialStore) -> Vec<HeldSubscription> {
    store
        .accounts
        .iter()
        .filter_map(|account| {
            let plan = account.account_plan()?;
            Some(HeldSubscription {
                account_id: account.id.clone(),
                account_label: account
                    .email
                    .clone()
                    .unwrap_or_else(|| account.label.clone()),
                provider: plan.provider,
                plan_key: plan.key,
                plan_label: plan.label,
                created_at: account.created_at,
            })
        })
        .collect()
}

impl Orchestrator {
    /// Bring `subscription_periods` in line with what the store holds now.
    /// Never the reason a lifecycle step fails: a write that does not land is
    /// logged and retried by the next call, since the memo is only advanced
    /// on success.
    pub(crate) fn reconcile_subscription_periods(&self) {
        let held = {
            let Ok(mut guard) = self.credential_store.lock() else {
                return;
            };
            let Some(store) = guard.as_mut() else {
                return;
            };
            // A choice the plan outgrew is dropped first, so no period opens
            // on it.
            if store.clear_stale_plan_choices() {
                if let Err(error) = self.credential_store.persist_locked(store) {
                    log::warn!("Could not drop stale plan choices: {error}");
                }
            }
            held_subscriptions(store)
        };
        let Ok(mut reconciled) = self.reconciled_subscriptions.lock() else {
            return;
        };
        if reconciled.as_ref() == Some(&held) {
            return;
        }
        let db = self.db.local.clone();
        let now = chrono::Utc::now().timestamp();
        let pending = held.clone();
        let written = crate::storage::run_db_blocking(move || async move {
            reconcile_periods(&db, &pending, now)
                .await
                .map_err(|error| format!("could not record subscription periods: {error}"))
        });
        match written {
            Ok(changed) => {
                *reconciled = Some(held);
                if changed {
                    let _ = self.services.emitter.emit(
                        "db-change",
                        serde_json::json!({"table": "subscription_periods", "action": "update"}),
                    );
                }
            }
            Err(error) => log::warn!("{error}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_analytics::subscriptions::{plan_periods, PlanPeriod};
    use cairn_models::accounts::{ApiProvider, ProviderAuth};
    use cairn_models::subscription::ModelHost;

    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};

    #[tokio::test(flavor = "multi_thread")]
    async fn publishing_claude_plan_metadata_opens_history_at_detection_time() {
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let local = Arc::new(
            LocalDb::open_migrated_for_test(root.path().join("orch.db"))
                .await
                .unwrap(),
        );
        let search = Arc::new(SearchIndex::open_or_create(root.path().join("search")).unwrap());
        let orch = OrchestratorBuilder::new(
            Arc::new(DbState::new(local.clone(), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir.clone(),
        )
        .build();
        let created_at = chrono::Utc::now().timestamp() - 86_400;
        let mut store = CredentialStore {
            user_id: "subscription-history-test".into(),
            accounts: Vec::new(),
            git_identities: Vec::new(),
            project_overrides: Default::default(),
            retired_logins: Vec::new(),
            config_dir,
        };
        let account = store.add_account(
            ApiProvider::Anthropic,
            "Claude subscription".into(),
            ProviderAuth::ClaudeProfile {
                oauth: Some(
                    serde_json::json!({
                        "accessToken": "fixture-access-token",
                        "subscriptionType": "max",
                        "rateLimitTier": "default_claude_max_5x",
                    })
                    .to_string(),
                ),
            },
            None,
            created_at,
        );
        ModelHost::store(&orch).save(store).unwrap();
        ModelHost::credentials_published(&orch);
        let initial = plan_periods(&local).await.unwrap();
        assert_eq!(
            initial,
            vec![PlanPeriod {
                account_id: account.id.clone(),
                account_label: "Claude subscription".into(),
                provider: "claude".into(),
                plan_key: Some("max_5x".into()),
                plan_label: "Max 5x".into(),
                started_at: created_at,
                ended_at: None,
            }]
        );

        // Model the publication boundary, not the private provider probe:
        // persist the metadata it read, then call the real host hook.
        let update_metadata = || {
            ModelHost::store(&orch)
                .update(|store| {
                    let account = store
                        .accounts
                        .iter_mut()
                        .find(|stored| stored.id == account.id)
                        .unwrap();
                    let ProviderAuth::ClaudeProfile { oauth: Some(oauth) } = &mut account.auth
                    else {
                        panic!("expected stored Claude OAuth");
                    };
                    let mut metadata: serde_json::Value = serde_json::from_str(oauth).unwrap();
                    metadata["subscriptionType"] = "max".into();
                    metadata["rateLimitTier"] = "default_claude_max_20x".into();
                    *oauth = metadata.to_string();
                    Ok(())
                })
                .unwrap();
        };
        update_metadata();
        assert_eq!(plan_periods(&local).await.unwrap(), initial);
        let before_publication = chrono::Utc::now().timestamp();
        ModelHost::credentials_published(&orch);
        let after_publication = chrono::Utc::now().timestamp();
        let periods = plan_periods(&local).await.unwrap();
        assert_eq!(periods.len(), 2);
        let detected_at = periods[1].started_at;
        assert!((before_publication..=after_publication).contains(&detected_at));
        assert!(detected_at > created_at);
        let mut closed = initial[0].clone();
        closed.ended_at = Some(detected_at);
        assert_eq!(periods[0], closed);
        assert_eq!(
            periods[1],
            PlanPeriod {
                plan_key: Some("max_20x".into()),
                plan_label: "Max 20x".into(),
                started_at: detected_at,
                ended_at: None,
                ..initial[0].clone()
            }
        );

        // A successful read publishes even when the provider reports the same plan.
        update_metadata();
        ModelHost::credentials_published(&orch);
        ModelHost::credentials_published(&orch);
        assert_eq!(plan_periods(&local).await.unwrap(), periods);
    }
}
