//! Keeping `subscription_periods` in step with the credential store.
//!
//! The credential lifecycle calls [`Orchestrator::reconcile_subscription_periods`]
//! after every change that can move an account's plan: an account added or
//! removed, a sign-in recorded, a credential rotated (a new ChatGPT id token
//! can name a new plan), a plan chosen, a rename. Startup calls it too, which
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
