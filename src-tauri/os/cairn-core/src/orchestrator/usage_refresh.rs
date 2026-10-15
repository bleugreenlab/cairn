//! Re-measuring a provider's usage on demand: the one path behind the
//! Settings card's refresh button, the per-account usage refresh, and
//! `cairn://models/accounts` `refresh_usage`.
//!
//! A Claude or Codex account is probed under the same single-flight
//! reservation a background capture takes, its result is kept only if the
//! credential it read was not rewritten meanwhile, and a kept measurement is
//! persisted as the account's health, so every surface that reads usage agrees
//! and the measurement survives a restart. An account measured in the last
//! [`USAGE_FRESHNESS_FLOOR_SECS`] is answered from that measurement without a
//! probe: single flight bounds concurrency, not rate, and a run looping on a
//! refresh would otherwise send a stream of usage requests on a subscription.

use std::time::Duration;

use cairn_models::types::usage::ProviderUsageSnapshot;

use super::identity::ProviderUsageCaptureReservation;
use super::Orchestrator;
use crate::backends::opencode::collect_opencode_usage_snapshot;
use crate::backends::openrouter::collect_openrouter_usage_snapshot;
use crate::credentials::RoutedProvider;

/// Hard ceiling for one refresh. Both the Claude PTY probe and the Codex
/// app-server handshake can hang (a wedged child, a stalled read); without a
/// backstop the caller never hears back.
pub const USAGE_REFRESH_TIMEOUT: Duration = Duration::from_secs(30);

/// How old an account's persisted measurement must be before a refresh probes
/// it again.
pub const USAGE_FRESHNESS_FLOOR_SECS: i64 = 60;

/// What a refresh re-measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageRefreshTarget<'a> {
    /// Every account in the provider's pool as `project_id` sees it.
    Pool { project_id: Option<&'a str> },
    /// One account, or the provider's default account when `None`.
    Account(Option<&'a str>),
}

/// How a refresh answered for one account.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageMeasurement {
    /// Probed now. The snapshot may itself carry the probe's error.
    Probed,
    /// Measured recently enough that it was not probed again.
    Fresh { age_secs: i64 },
    /// Another measurement of this account was already running.
    AlreadyMeasuring,
    /// Probed, but the credential was rewritten while the probe ran, so the
    /// result was discarded.
    Discarded,
}

/// One account's answer.
#[derive(Debug, Clone)]
pub struct UsageRefresh {
    pub account_id: Option<String>,
    pub snapshot: ProviderUsageSnapshot,
    pub measurement: UsageMeasurement,
}

/// What a backend with no provider-side allowance says when asked for one.
pub fn unsupported_usage_snapshot(backend: &str) -> ProviderUsageSnapshot {
    match backend {
        "ollama" => ProviderUsageSnapshot::unsupported(
            "ollama",
            "ollama_usage",
            "Local models have no provider-side usage or cost.",
        ),
        // Actual runs inference on hardware the user owns, and the relay
        // exposes no quota endpoint (`/v1/usage` is a 404), so there is no
        // budget to report. Counting local tokens and pricing them would invent
        // a bill nobody is sending.
        "actual" => ProviderUsageSnapshot::unsupported(
            "actual",
            "actual_usage",
            "Inference runs on hardware you own, so Actual reports no token budget or cost.",
        ),
        other => ProviderUsageSnapshot::unsupported(
            other,
            "unsupported_provider_usage",
            format!("Unsupported backend: {other}"),
        ),
    }
}

fn timed_out(backend: &str) -> ProviderUsageSnapshot {
    ProviderUsageSnapshot::error(
        backend,
        "provider_usage",
        format!(
            "Timed out after {}s waiting for {backend} usage data.",
            USAGE_REFRESH_TIMEOUT.as_secs()
        ),
        None,
    )
}

impl Orchestrator {
    /// Re-measure a provider's usage now. Every result is recorded against the
    /// account it belongs to and nothing else: one failed probe never hides the
    /// capacity the others still report.
    pub async fn refresh_provider_usage(
        &self,
        backend: &str,
        target: UsageRefreshTarget<'_>,
    ) -> Vec<UsageRefresh> {
        let accounts: Vec<Option<String>> = match target {
            UsageRefreshTarget::Account(account_id) => vec![account_id.map(ToString::to_string)],
            UsageRefreshTarget::Pool { project_id } => self
                .provider_usage_pool(backend, project_id)
                .accounts
                .into_iter()
                .map(|account| Some(account.account_id))
                .collect(),
        };

        // The metered HTTP providers report usage from Cairn's own recorded
        // cost rather than a per-account child-process probe, so one call
        // answers for the whole provider.
        if backend == "openrouter" || backend == "opencode-go" {
            let snapshot = if backend == "openrouter" {
                collect_openrouter_usage_snapshot(self).await
            } else {
                collect_opencode_usage_snapshot(self).await
            };
            let account_id = accounts.into_iter().next().flatten();
            self.store_provider_account_usage_snapshot(account_id.clone(), snapshot.clone());
            return vec![UsageRefresh {
                account_id,
                snapshot,
                measurement: UsageMeasurement::Probed,
            }];
        }

        let Some(provider) = RoutedProvider::for_backend(backend) else {
            let snapshot = unsupported_usage_snapshot(backend);
            let accounts = if accounts.is_empty() {
                vec![None]
            } else {
                accounts
            };
            return accounts
                .into_iter()
                .map(|account_id| {
                    self.store_provider_account_usage_snapshot(
                        account_id.clone(),
                        snapshot.clone(),
                    );
                    UsageRefresh {
                        account_id,
                        snapshot: snapshot.clone(),
                        measurement: UsageMeasurement::Probed,
                    }
                })
                .collect();
        };

        let deadline = tokio::time::Instant::now() + USAGE_REFRESH_TIMEOUT;
        let refreshes = accounts
            .into_iter()
            .map(|account_id| self.refresh_subscription_account(provider, account_id, deadline));
        futures_util::future::join_all(refreshes).await
    }

    async fn refresh_subscription_account(
        &self,
        provider: RoutedProvider,
        account_id: Option<String>,
        deadline: tokio::time::Instant,
    ) -> UsageRefresh {
        let backend = provider.backend();
        // The default account is the first in the pool; with none at all
        // there is nothing to reserve or persist, and the probe says so.
        let account_id = account_id.or_else(|| {
            self.provider_usage_pool(backend, None)
                .accounts
                .into_iter()
                .next()
                .map(|account| account.account_id)
        });
        let Some(account_id) = account_id else {
            let snapshot = self.subscriptions.probe_usage(self, provider, None).0;
            self.store_provider_account_usage_snapshot(None, snapshot.clone());
            return UsageRefresh {
                account_id: None,
                snapshot,
                measurement: UsageMeasurement::Probed,
            };
        };

        let now = chrono::Utc::now().timestamp();
        if let Some((age_secs, snapshot)) = self.fresh_measurement(backend, &account_id, now) {
            return UsageRefresh {
                account_id: Some(account_id),
                snapshot,
                measurement: UsageMeasurement::Fresh { age_secs },
            };
        }
        let Some(reservation) = ProviderUsageCaptureReservation::acquire(
            self.provider_usage_captures.clone(),
            provider,
            &account_id,
        ) else {
            let snapshot = self
                .current_measurement(backend, &account_id)
                .unwrap_or_else(|| {
                    ProviderUsageSnapshot::error(
                        backend,
                        "provider_usage",
                        "This account is already being measured; its result arrives shortly.",
                        None,
                    )
                });
            return UsageRefresh {
                account_id: Some(account_id),
                snapshot,
                measurement: UsageMeasurement::AlreadyMeasuring,
            };
        };

        // Orchestrator: Clone, so the probe owns what it needs and outlives
        // this frame. On timeout the orphaned blocking task runs to completion,
        // kills its own child, and releases the reservation; its result is
        // dropped.
        let orch = self.clone();
        let probe_account = account_id.clone();
        let probe = tokio::task::spawn_blocking(move || {
            let _reservation = reservation;
            orch.subscriptions
                .probe_usage(&orch, provider, Some(&probe_account))
        });
        let (snapshot, generation) = match tokio::time::timeout_at(deadline, probe).await {
            Ok(Ok(measured)) => measured,
            Ok(Err(join_err)) => (
                ProviderUsageSnapshot::error(
                    backend,
                    "provider_usage",
                    format!("usage probe panicked: {join_err}"),
                    None,
                ),
                None,
            ),
            Err(_) => (timed_out(backend), None),
        };
        // A failed probe takes the same path: it is recorded against the
        // account it belongs to, and the health it last measured stands.
        let measurement = match self.apply_captured_account_usage_if_current(
            provider,
            &account_id,
            generation,
            snapshot.clone(),
        ) {
            Ok(true) => UsageMeasurement::Probed,
            Ok(false) => UsageMeasurement::Discarded,
            Err(error) => {
                log::warn!("Failed to record {backend} usage for {account_id}: {error}");
                UsageMeasurement::Probed
            }
        };
        UsageRefresh {
            account_id: Some(account_id),
            snapshot,
            measurement,
        }
    }

    /// The account's persisted measurement, if it is younger than the floor.
    fn fresh_measurement(
        &self,
        backend: &str,
        account_id: &str,
        now: i64,
    ) -> Option<(i64, ProviderUsageSnapshot)> {
        let health = self
            .get_credential_store()?
            .accounts
            .into_iter()
            .find(|account| account.id == account_id)?
            .health?;
        let age_secs = now.saturating_sub(health.captured_at);
        if !(0..USAGE_FRESHNESS_FLOOR_SECS).contains(&age_secs) {
            return None;
        }
        let snapshot = self
            .current_measurement(backend, account_id)
            .filter(|snapshot| !snapshot.windows.is_empty())
            .unwrap_or_else(|| health_snapshot(backend, health));
        Some((age_secs, snapshot))
    }

    /// What is already known about an account: its live snapshot, else its
    /// persisted health.
    fn current_measurement(
        &self,
        backend: &str,
        account_id: &str,
    ) -> Option<ProviderUsageSnapshot> {
        self.provider_usage_snapshots
            .get(backend, Some(account_id))
            .or_else(|| {
                self.get_credential_store()?
                    .accounts
                    .into_iter()
                    .find(|account| account.id == account_id)?
                    .health
                    .map(|health| health_snapshot(backend, health))
            })
    }
}

/// An account's persisted health as a snapshot, for a caller that asked for
/// one and was answered without a probe.
fn health_snapshot(
    backend: &str,
    health: crate::credentials::ProviderAccountHealth,
) -> ProviderUsageSnapshot {
    ProviderUsageSnapshot {
        backend: backend.to_string(),
        source: "account_health".to_string(),
        captured_at: health.captured_at,
        windows: health.windows,
        credits: None,
        reset_credits: None,
        error: None,
        error_kind: None,
        unsupported_reason: None,
        raw: None,
        model_breakdown: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::{ApiProvider, ProviderAuth};
    use crate::db::DbState;
    use crate::orchestrator::OrchestratorBuilder;
    use crate::services::testing::TestServicesBuilder;
    use crate::storage::{LocalDb, MigrationRunner, SearchIndex, TURSO_MIGRATIONS};
    use cairn_models::subscription::testing::TempHomes;
    use cairn_models::types::usage::{ProviderUsageScope, ProviderUsageWindow};
    use std::path::Path;
    use std::sync::Arc;

    /// An orchestrator over the database and configuration under `root`, so a
    /// second one built over the same root is a restart.
    async fn orchestrator_at(root: &Path, homes: &TempHomes) -> Orchestrator {
        let config_dir = root.join("config");
        std::fs::create_dir_all(&config_dir).unwrap();
        let db = LocalDb::open(root.join("orch.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        // The search index is not what a restart has to keep, and the first
        // orchestrator's writer may still hold its lock.
        let search = Arc::new(
            SearchIndex::open_or_create(root.join(format!("search-{}", uuid::Uuid::new_v4())))
                .unwrap(),
        );
        // Loaded from disk the way the runtime starts.
        let credentials = crate::credentials::store::load_credential_store(&config_dir)
            .ok()
            .flatten();
        OrchestratorBuilder::new(
            Arc::new(DbState::new(Arc::new(db), search)),
            Arc::new(TestServicesBuilder::new().build()),
            config_dir,
        )
        .credential_store(credentials)
        .subscriptions(Arc::new(homes.clone()))
        .build()
    }

    fn measured(remaining: f64) -> ProviderUsageSnapshot {
        ProviderUsageSnapshot {
            backend: "claude".to_string(),
            source: "claude_usage_oauth".to_string(),
            captured_at: chrono::Utc::now().timestamp(),
            windows: vec![ProviderUsageWindow {
                id: "session".to_string(),
                label: "session".to_string(),
                scope: ProviderUsageScope::Session,
                scope_target: None,
                used_percent: 100.0 - remaining,
                remaining_percent: remaining,
                resets_at: Some(chrono::Utc::now().timestamp() + 3_600),
                reset_at_text: None,
                window_duration_mins: None,
            }],
            credits: None,
            reset_credits: None,
            error: None,
            error_kind: None,
            unsupported_reason: None,
            raw: None,
            model_breakdown: None,
        }
    }

    fn managed_claude(orch: &Orchestrator) -> String {
        orch.add_account(
            ApiProvider::Anthropic,
            "Max".to_string(),
            ProviderAuth::ClaudeProfile { oauth: None },
            None,
        )
        .unwrap()
        .id
    }

    async fn page(orch: &Orchestrator, uri: &str) -> String {
        let request = crate::mcp::types::McpCallbackRequest {
            thread_id: None,
            cwd: "/tmp".to_string(),
            run_id: None,
            tool: "read".to_string(),
            payload: serde_json::json!({}),
            tool_use_id: None,
        };
        crate::resources::read_cairn_resource(orch, &request, uri).await
    }

    fn health_windows(orch: &Orchestrator, id: &str) -> Vec<ProviderUsageWindow> {
        orch.get_credential_store()
            .unwrap()
            .accounts
            .into_iter()
            .find(|account| account.id == id)
            .and_then(|account| account.health)
            .map(|health| health.windows)
            .unwrap_or_default()
    }

    /// A refresh persists what it measured, so the account page and the usage
    /// page agree with each other, and with a restarted orchestrator.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_refresh_persists_health_every_surface_reads() {
        let root = tempfile::tempdir().unwrap().keep();
        let homes = TempHomes::new(root.join("homes"));
        let orch = orchestrator_at(&root, &homes).await;
        let id = managed_claude(&orch);
        homes.script_usage(measured(42.0), None);

        let refreshed = orch
            .refresh_provider_usage("claude", UsageRefreshTarget::Account(Some(&id)))
            .await;
        assert_eq!(refreshed.len(), 1);
        assert_eq!(refreshed[0].measurement, UsageMeasurement::Probed);
        assert_eq!(health_windows(&orch, &id).len(), 1);

        let account = page(&orch, &format!("cairn://models/accounts/{id}")).await;
        assert!(account.contains("session: 42% left"), "{account}");
        let usage = page(&orch, "cairn://models/usage?provider=claude").await;
        assert!(usage.contains("42%"), "{usage}");

        drop(orch);
        let restarted = orchestrator_at(&root, &homes).await;
        let account = page(&restarted, &format!("cairn://models/accounts/{id}")).await;
        assert!(account.contains("session: 42% left"), "{account}");
    }

    /// An account measured within the floor answers from that measurement
    /// without another provider request.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_fresh_measurement_is_not_re_probed() {
        let root = tempfile::tempdir().unwrap().keep();
        let homes = TempHomes::new(root.join("homes"));
        let orch = orchestrator_at(&root, &homes).await;
        let id = managed_claude(&orch);
        homes.script_usage(measured(42.0), None);
        orch.refresh_provider_usage("claude", UsageRefreshTarget::Account(Some(&id)))
            .await;
        assert_eq!(homes.probe_count(), 1);

        let again = orch
            .refresh_provider_usage("claude", UsageRefreshTarget::Pool { project_id: None })
            .await;
        assert_eq!(homes.probe_count(), 1, "the floor held");
        assert!(matches!(
            again[0].measurement,
            UsageMeasurement::Fresh { age_secs } if (0..USAGE_FRESHNESS_FLOOR_SECS).contains(&age_secs)
        ));
        assert_eq!(again[0].snapshot.windows.len(), 1);
    }

    /// An account already being measured is not probed a second time.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_held_reservation_reports_already_measuring() {
        let root = tempfile::tempdir().unwrap().keep();
        let homes = TempHomes::new(root.join("homes"));
        let orch = orchestrator_at(&root, &homes).await;
        let id = managed_claude(&orch);
        homes.script_usage(measured(42.0), None);
        let _held = ProviderUsageCaptureReservation::acquire(
            orch.provider_usage_captures.clone(),
            RoutedProvider::Claude,
            &id,
        )
        .expect("free");

        let refreshed = orch
            .refresh_provider_usage("claude", UsageRefreshTarget::Account(Some(&id)))
            .await;
        assert_eq!(refreshed[0].measurement, UsageMeasurement::AlreadyMeasuring);
        assert_eq!(homes.probe_count(), 0);
    }

    /// A measurement that crossed a credential rewrite is discarded rather
    /// than recorded against the new credential.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_crossed_credential_generation_is_discarded() {
        let root = tempfile::tempdir().unwrap().keep();
        let homes = TempHomes::new(root.join("homes"));
        let orch = orchestrator_at(&root, &homes).await;
        let id = managed_claude(&orch);
        homes.script_usage(measured(42.0), Some("generation-before".to_string()));
        homes.set_generation(Some("generation-after".to_string()));

        let refreshed = orch
            .refresh_provider_usage("claude", UsageRefreshTarget::Account(Some(&id)))
            .await;
        assert_eq!(refreshed[0].measurement, UsageMeasurement::Discarded);
        assert!(health_windows(&orch, &id).is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_provider_without_usage_answers_with_its_own_reason() {
        let root = tempfile::tempdir().unwrap().keep();
        let homes = TempHomes::new(root.join("homes"));
        let orch = orchestrator_at(&root, &homes).await;
        let refreshed = orch
            .refresh_provider_usage("ollama", UsageRefreshTarget::Pool { project_id: None })
            .await;
        assert_eq!(
            refreshed[0].snapshot.unsupported_reason.as_deref(),
            Some("Local models have no provider-side usage or cost.")
        );
    }
}
