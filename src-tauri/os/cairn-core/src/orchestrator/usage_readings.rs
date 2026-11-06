//! Persisting usage refreshes as a per-account time series
//! (`usage_window_readings`), so how fast each window drains can be measured
//! later. The in-memory `UsageBook` stays the panel's source; this only
//! appends what a refresh measured.

use cairn_db::storage::usage_readings::{record_usage_readings, UsageReadingInput};
use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageSnapshot};

use super::Orchestrator;

/// The windows of a stored snapshot that are a reading of `account_id`, or
/// `None` when the snapshot is not one: an unknown account (the live Claude
/// rate-limit event is stored under none), a failed probe (the book keeps the
/// older windows beside the error, and those were already recorded when they
/// were measured), or a snapshot with no windows (the credential-generation
/// placeholder).
pub(crate) fn readings_of(
    account_id: Option<&str>,
    stored: &ProviderUsageSnapshot,
) -> Option<Vec<UsageReadingInput>> {
    account_id?;
    if stored.error.is_some() || stored.windows.is_empty() {
        return None;
    }
    Some(
        stored
            .windows
            .iter()
            .map(|window| UsageReadingInput {
                window_id: window.id.clone(),
                window_name: window.family_name(),
                scope: scope_name(window.scope).to_string(),
                scope_target: window.scope_target.clone(),
                remaining_percent: window.remaining_percent,
                window_duration_mins: window.window_duration_mins.map(i64::from),
                resets_at: window.resets_at,
            })
            .collect(),
    )
}

fn scope_name(scope: ProviderUsageScope) -> &'static str {
    match scope {
        ProviderUsageScope::Session => "session",
        ProviderUsageScope::Weekly => "weekly",
        ProviderUsageScope::RollingWindow => "rolling_window",
        ProviderUsageScope::Custom => "custom",
    }
}

impl Orchestrator {
    /// Append a stored snapshot's windows to the account's reading series, in
    /// the background. The store path runs synchronously on backend event
    /// threads, so this never blocks it, and a failed write is only logged:
    /// the series is evidence for later analysis, not state anything waits on.
    pub(super) fn record_usage_readings(
        &self,
        account_id: Option<&str>,
        stored: &ProviderUsageSnapshot,
    ) {
        let Some(windows) = readings_of(account_id, stored) else {
            return;
        };
        let Some(account_id) = account_id.map(str::to_string) else {
            return;
        };
        let db = self.db.local.clone();
        let provider = stored.backend.clone();
        let source = stored.source.clone();
        let captured_at = stored.captured_at;
        self.spawn_detached(async move {
            if let Err(error) =
                record_usage_readings(&db, &provider, &account_id, &source, captured_at, windows)
                    .await
            {
                log::warn!("Failed to record {provider} usage readings for {account_id}: {error}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_db::storage::usage_readings::list_usage_readings;
    use cairn_models::types::usage::ProviderUsageWindow;
    use std::sync::Arc;

    async fn test_orchestrator() -> Orchestrator {
        let root = tempfile::tempdir().unwrap().keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        let search = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    fn opus_window(remaining_percent: f64) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: "weekly_opus-opus".to_string(),
            label: "Current week (Opus)".to_string(),
            scope: ProviderUsageScope::Weekly,
            scope_target: Some("Opus".to_string()),
            used_percent: 100.0 - remaining_percent,
            remaining_percent,
            resets_at: Some(2_000_000_000),
            reset_at_text: None,
            window_duration_mins: None,
        }
    }

    fn measured(remaining_percent: f64) -> ProviderUsageSnapshot {
        ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "claude_usage_oauth".to_string(),
            captured_at: 1_000,
            windows: vec![opus_window(remaining_percent)],
            ..Default::default()
        }
    }

    #[test]
    fn only_a_known_accounts_measured_windows_are_readings() {
        let readings = readings_of(Some("acct-a"), &measured(40.0)).unwrap();
        assert_eq!(readings.len(), 1);
        assert_eq!(readings[0].window_id, "weekly_opus-opus");
        assert_eq!(readings[0].window_name, "Opus");
        assert_eq!(readings[0].scope, "weekly");
        assert_eq!(readings[0].scope_target.as_deref(), Some("Opus"));

        assert!(readings_of(None, &measured(40.0)).is_none());
        let mut outranked_failure = measured(40.0);
        outranked_failure.error = Some("probe failed".to_string());
        assert!(readings_of(Some("acct-a"), &outranked_failure).is_none());
        let placeholder = ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "credential_generation".to_string(),
            ..Default::default()
        };
        assert!(readings_of(Some("acct-a"), &placeholder).is_none());
    }

    async fn readings_eventually(orch: &Orchestrator, account: &str, count: usize) -> usize {
        for _ in 0..100 {
            let rows = list_usage_readings(&orch.db.local, account, None, None)
                .await
                .unwrap();
            if rows.len() >= count {
                return rows.len();
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        list_usage_readings(&orch.db.local, account, None, None)
            .await
            .unwrap()
            .len()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_store_path_records_readings_and_skips_non_readings() {
        let orch = test_orchestrator().await;
        orch.store_provider_account_usage_snapshot(Some("acct-a".to_string()), measured(40.0));
        assert_eq!(readings_eventually(&orch, "acct-a", 1).await, 1);

        // A failed probe outranked by the stored snapshot is not a reading.
        orch.store_provider_account_usage_snapshot(
            Some("acct-a".to_string()),
            ProviderUsageSnapshot::error("claude", "provider_usage", "probe failed", None),
        );
        // An account-less snapshot is not one either.
        orch.store_provider_account_usage_snapshot(None, measured(10.0));
        // A later measurement is, and is what the series waits on.
        orch.store_provider_account_usage_snapshot(Some("acct-a".to_string()), measured(35.0));
        assert_eq!(readings_eventually(&orch, "acct-a", 2).await, 2);
        let rows = list_usage_readings(&orch.db.local, "acct-a", Some("Opus"), None)
            .await
            .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| row.remaining_percent)
                .collect::<Vec<_>>(),
            [40.0, 35.0]
        );
    }
}
