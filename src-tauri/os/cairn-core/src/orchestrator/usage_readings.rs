//! Persisting usage refreshes as a per-account time series
//! (`usage_window_readings`), and refitting how fast each window drains from
//! it (`cairn_models::types::drain`) once new readings land. The in-memory
//! `UsageBook` stays the panel's source; this appends what a refresh measured
//! and keeps the `DrainBook` current.

use std::sync::Arc;

use cairn_analytics::drain_inputs::{drain_inputs, drain_watermark};
use cairn_db::storage::usage_readings::{record_usage_readings, UsageReadingInput};
use cairn_db::storage::LocalDb;
use cairn_models::types::drain::DrainFit;
use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageSnapshot};
use cairn_models::usage::drain::DrainBook;

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

/// Read the inputs and fit them, off the async runtime.
async fn fit_now(
    db: &LocalDb,
    now: i64,
) -> Result<(DrainFit, cairn_models::types::drain::Watermark), String> {
    let watermark = drain_watermark(db)
        .await
        .map_err(|error| error.to_string())?;
    let inputs = drain_inputs(db, now)
        .await
        .map_err(|error| error.to_string())?;
    let fit = tokio::task::spawn_blocking(move || cairn_models::types::drain::fit(&inputs))
        .await
        .map_err(|error| error.to_string())?;
    Ok((fit, watermark))
}

/// Refit the window drain when it is due (see `DrainBook::due`), no other
/// refit is running, and the inputs moved past the held watermark. Answers
/// the new fit, or `None` when nothing was refitted.
pub(crate) async fn refit_drain(
    db: &LocalDb,
    book: &Arc<DrainBook>,
    now: i64,
) -> Result<Option<Arc<DrainFit>>, String> {
    if !book.due(now) {
        return Ok(None);
    }
    let Some(_slot) = book.claim() else {
        return Ok(None);
    };
    let watermark = drain_watermark(db)
        .await
        .map_err(|error| error.to_string())?;
    if book.watermark() == Some(watermark) {
        book.unchanged(now);
        return Ok(None);
    }
    let (fit, watermark) = fit_now(db, now).await?;
    Ok(Some(book.store(fit, watermark, now)))
}

impl Orchestrator {
    /// The served window-drain fit, fitted here once when none is held yet.
    pub(crate) async fn drain_fit(&self) -> Result<Arc<DrainFit>, String> {
        if let Some(fit) = self.drain_book.fit() {
            return Ok(fit);
        }
        let now = chrono::Utc::now().timestamp();
        let (fit, watermark) = fit_now(&self.db.local, now).await?;
        Ok(self.drain_book.store(fit, watermark, now))
    }

    /// Append a stored snapshot's windows to the account's reading series, in
    /// the background, then refit the window drain if that is due. The refit
    /// is attempted after every recorded refresh, including one that changed
    /// nothing: a reading that landed while the last refit was too recent
    /// moved the watermark, and only a later refresh comes back for it. The
    /// store path runs synchronously on backend event threads, so this never
    /// blocks it, and a failed write or refit is only logged: nothing waits on
    /// either.
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
        let book = self.drain_book.clone();
        self.spawn_detached(async move {
            match record_usage_readings(&db, &provider, &account_id, &source, captured_at, windows)
                .await
            {
                Err(error) => {
                    log::warn!(
                        "Failed to record {provider} usage readings for {account_id}: {error}"
                    );
                }
                Ok(_) => {
                    let now = chrono::Utc::now().timestamp();
                    if let Err(error) = refit_drain(&db, &book, now).await {
                        log::warn!("Failed to refit window drain: {error}");
                    }
                }
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
        let db = LocalDb::open_migrated_for_test(root.join("orch.db"))
            .await
            .unwrap();
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

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refit_waits_for_its_interval_and_for_inputs_that_moved() {
        let orch = test_orchestrator().await;
        let book = orch.drain_book.clone();
        let db = orch.db.local.clone();
        // Empty: the first refit runs.
        let first = refit_drain(&db, &book, 10_000).await.unwrap();
        assert!(first.is_some());
        // Too soon: nothing is read.
        assert!(refit_drain(&db, &book, 10_001).await.unwrap().is_none());
        // Due, but the inputs have not moved: the fit is kept.
        let later = 10_000 + cairn_models::usage::drain::REFIT_INTERVAL_SECS;
        assert!(refit_drain(&db, &book, later).await.unwrap().is_none());
        // A new reading moves the watermark, and the next due refit runs.
        record_usage_readings(
            &db,
            "claude",
            "acct-a",
            "claude_usage_oauth",
            1_000,
            readings_of(Some("acct-a"), &measured(40.0)).unwrap(),
        )
        .await
        .unwrap();
        let again = later + cairn_models::usage::drain::REFIT_INTERVAL_SECS;
        let refit = refit_drain(&db, &book, again).await.unwrap().unwrap();
        assert_eq!(refit.readings, 1);
        assert_eq!(orch.drain_fit().await.unwrap().readings, 1);
    }

    async fn fitted_eventually(book: &DrainBook) -> bool {
        for _ in 0..200 {
            if book.fit().is_some() {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        false
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_reading_inside_the_refit_interval_is_served_at_once() {
        let orch = test_orchestrator().await;
        orch.store_provider_account_usage_snapshot(Some("acct-a".to_string()), measured(80.0));
        assert!(fitted_eventually(&orch.drain_book).await);
        let fitted = orch.drain_book.watermark();

        // 90% used lands well inside the refit interval: the coefficients
        // are kept, and the capacity read serves the new reading anyway.
        let mut later = measured(10.0);
        later.captured_at = 2_000;
        orch.store_provider_account_usage_snapshot(Some("acct-a".to_string()), later);
        assert_eq!(readings_eventually(&orch, "acct-a", 2).await, 2);
        assert_eq!(orch.drain_book.watermark(), fitted);
        let current = cairn_models::types::drain::current_windows(
            &cairn_analytics::drain_inputs::current_readings(&orch.db.local)
                .await
                .unwrap(),
        );
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].used_pct, 90.0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_refresh_that_records_nothing_new_still_refits_when_due() {
        let orch = test_orchestrator().await;
        // The reading is already stored, so the refresh below inserts no row.
        record_usage_readings(
            &orch.db.local,
            "claude",
            "acct-a",
            "claude_usage_oauth",
            1_000,
            readings_of(Some("acct-a"), &measured(40.0)).unwrap(),
        )
        .await
        .unwrap();
        assert!(orch.drain_book.fit().is_none());
        orch.store_provider_account_usage_snapshot(Some("acct-a".to_string()), measured(40.0));
        assert!(fitted_eventually(&orch.drain_book).await);
        assert_eq!(orch.drain_book.fit().unwrap().readings, 1);
        assert_eq!(readings_eventually(&orch, "acct-a", 1).await, 1);
    }
}
