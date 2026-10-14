//! One provider's pooled capacity. The join itself lives in
//! `cairn_llm::usage::pool`; the orchestrator supplies its usage book and
//! credential store.

use cairn_llm::types::pool::ProviderPoolSnapshot;

use super::Orchestrator;

impl Orchestrator {
    /// One provider's pooled capacity, as settings renders it.
    ///
    /// A pure read: it reports what has already been measured and never probes.
    pub fn provider_usage_pool(
        &self,
        backend: &str,
        project_id: Option<&str>,
    ) -> ProviderPoolSnapshot {
        cairn_llm::usage::pool::provider_usage_pool(
            &self.provider_usage_snapshots,
            self.get_credential_store().as_ref(),
            backend,
            project_id,
            chrono::Utc::now().timestamp(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{ApiProvider, ProviderAuth};
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, SearchIndex};
    use cairn_llm::types::pool::{ProviderPoolAccountState, ProviderPoolSegmentState};
    use cairn_llm::types::usage::ProviderUsageSnapshot;
    use cairn_llm::types::usage::{ProviderUsageScope, ProviderUsageWindow};
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

    fn session_window(remaining_percent: f64) -> ProviderUsageWindow {
        ProviderUsageWindow {
            id: "session".to_string(),
            label: "Current session".to_string(),
            scope: ProviderUsageScope::Session,
            scope_target: None,
            used_percent: 100.0 - remaining_percent,
            remaining_percent,
            resets_at: Some(i64::MAX),
            reset_at_text: None,
            window_duration_mins: None,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_pool_excludes_a_credential_with_no_metered_window() {
        let orch = test_orchestrator().await;
        let profile = orch
            .add_account(
                ApiProvider::Anthropic,
                "Profile".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.add_account(
            ApiProvider::Anthropic,
            "Key".into(),
            ProviderAuth::ApiKey {
                value: "sk-test".into(),
            },
            None,
        )
        .unwrap();

        let pool = orch.provider_usage_pool("claude", None);
        assert_eq!(
            pool.accounts
                .iter()
                .map(|account| account.account_id.as_str())
                .collect::<Vec<_>>(),
            vec![profile.id.as_str()],
            "an API key bills per token and has no allowance to pool"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persisted_health_carries_the_pool_when_no_snapshot_is_in_memory() {
        let orch = test_orchestrator().await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Profile".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(&account.id, vec![session_window(42.0)], None)
            .unwrap();

        // The snapshot map is in-memory only; health is what survives a
        // restart, and the pool has to read it or capacity vanishes on boot.
        orch.provider_usage_snapshots.clear();

        let pool = orch.provider_usage_pool("claude", None);
        let session = &pool.families[0];
        assert_eq!(session.key, "session");
        assert_eq!(session.remaining_percent, 42.0);
        assert_eq!(session.capacity_percent, 100.0);
        assert_eq!(pool.accounts[0].state, ProviderPoolAccountState::Measured);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removing_an_account_shrinks_the_denominator_and_leaves_no_stale_segment() {
        let orch = test_orchestrator().await;
        let first = orch
            .add_account(
                ApiProvider::Anthropic,
                "First".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let second = orch
            .add_account(
                ApiProvider::Anthropic,
                "Second".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(&first.id, vec![session_window(60.0)], None)
            .unwrap();
        orch.record_account_health(&second.id, vec![session_window(40.0)], None)
            .unwrap();

        let pool = orch.provider_usage_pool("claude", None);
        assert_eq!(pool.families[0].capacity_percent, 200.0);
        assert_eq!(pool.families[0].remaining_percent, 100.0);

        orch.remove_account(&second.id).unwrap();

        let pool = orch.provider_usage_pool("claude", None);
        assert_eq!(pool.accounts.len(), 1);
        assert_eq!(pool.families[0].capacity_percent, 100.0);
        assert_eq!(pool.families[0].remaining_percent, 60.0);
        assert_eq!(pool.families[0].segments.len(), 1);
        assert_eq!(pool.families[0].segments[0].account_id, first.id);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_account_that_has_never_been_measured_is_an_unknown_segment() {
        let orch = test_orchestrator().await;
        let measured = orch
            .add_account(
                ApiProvider::Anthropic,
                "Measured".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.add_account(
            ApiProvider::Anthropic,
            "Fresh".into(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
        )
        .unwrap();
        orch.record_account_health(&measured.id, vec![session_window(60.0)], None)
            .unwrap();

        let pool = orch.provider_usage_pool("claude", None);
        let session = &pool.families[0];
        assert_eq!(session.capacity_percent, 200.0);
        assert_eq!(session.remaining_percent, 60.0);
        assert_eq!(session.unknown_count, 1);
        assert_eq!(session.segments[1].state, ProviderPoolSegmentState::Unknown);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_error_snapshot_does_not_erase_the_last_measured_windows() {
        let orch = test_orchestrator().await;
        let account = orch
            .add_account(
                ApiProvider::Anthropic,
                "Profile".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        orch.record_account_health(&account.id, vec![session_window(75.0)], None)
            .unwrap();
        orch.store_provider_account_usage_snapshot(
            Some(account.id.clone()),
            ProviderUsageSnapshot::error("claude", "provider_usage", "probe failed", None),
        );

        let pool = orch.provider_usage_pool("claude", None);
        // The error is reported on the account, and the windows it last
        // measured still stand behind the failed probe.
        assert_eq!(pool.accounts[0].error.as_deref(), Some("probe failed"));
        assert_eq!(pool.accounts[0].state, ProviderPoolAccountState::Error);
        assert_eq!(pool.families[0].remaining_percent, 75.0);
    }

    /// A canonical snapshot outranks a failed probe, so the failure has to be
    /// recorded against the windows it could not refresh rather than dropped.
    /// Without that, the panel keeps presenting the previous success as though
    /// the refresh had worked, which is the exact opposite of what the account
    /// state promises.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_refresh_is_visible_even_after_an_earlier_success() {
        let orch = test_orchestrator().await;
        let failing = orch
            .add_account(
                ApiProvider::Anthropic,
                "Failing".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();
        let healthy = orch
            .add_account(
                ApiProvider::Anthropic,
                "Healthy".into(),
                ProviderAuth::ClaudeProfile { oauth: None },
                None,
            )
            .unwrap();

        for (id, remaining) in [(&failing.id, 60.0), (&healthy.id, 40.0)] {
            orch.store_provider_account_usage_snapshot(
                Some(id.clone()),
                ProviderUsageSnapshot {
                    backend: "claude".to_string(),
                    source: "claude_usage_oauth".to_string(),
                    captured_at: chrono::Utc::now().timestamp(),
                    windows: vec![session_window(remaining)],
                    ..Default::default()
                },
            );
        }
        assert_eq!(
            orch.provider_usage_pool("claude", None).accounts[0].state,
            ProviderPoolAccountState::Measured
        );

        orch.store_provider_account_usage_snapshot(
            Some(failing.id.clone()),
            ProviderUsageSnapshot::error(
                "claude",
                "provider_usage",
                "Timed out after 30s waiting for claude usage data.",
                None,
            ),
        );

        let pool = orch.provider_usage_pool("claude", None);
        assert_eq!(pool.accounts[0].state, ProviderPoolAccountState::Error);
        assert_eq!(
            pool.accounts[0].error.as_deref(),
            Some("Timed out after 30s waiting for claude usage data.")
        );
        // The other account is untouched by its neighbour's failure.
        assert_eq!(pool.accounts[1].state, ProviderPoolAccountState::Measured);
        assert_eq!(pool.accounts[1].error, None);
        // Neither account's last measured capacity was discarded.
        assert_eq!(pool.families[0].remaining_percent, 100.0);
        assert_eq!(pool.families[0].unknown_count, 0);

        // A later success clears the failure rather than leaving it stuck.
        orch.store_provider_account_usage_snapshot(
            Some(failing.id.clone()),
            ProviderUsageSnapshot {
                backend: "claude".to_string(),
                source: "claude_usage_oauth".to_string(),
                captured_at: chrono::Utc::now().timestamp(),
                windows: vec![session_window(55.0)],
                ..Default::default()
            },
        );
        let pool = orch.provider_usage_pool("claude", None);
        assert_eq!(pool.accounts[0].state, ProviderPoolAccountState::Measured);
        assert_eq!(pool.accounts[0].error, None);
        assert_eq!(pool.families[0].remaining_percent, 95.0);
    }
}
