//! Independent recovery of durable Codex watchdog leases.
//!
//! Eligibility lives here; ownership and provider-specific recovery do not. The
//! callback owns the durable claim so worker expiry, concurrent sweeps, and
//! startup recovery all enter the same exactly-once boundary.

use async_trait::async_trait;

use crate::runs::watchdog_ledger::{list_open_watchdog_leases, WatchdogLease};
use crate::storage::{DbResult, LocalDb};

pub const DEFAULT_HEARTBEAT_GRACE_SECS: i64 = 90;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileScope<'a> {
    Periodic,
    Startup { current_boot_id: &'a str },
}

#[async_trait]
impl WatchdogRecovery for crate::orchestrator::Orchestrator {
    async fn recover(&self, lease: &WatchdogLease) -> Result<RecoveryOutcome, String> {
        use crate::orchestrator::lifecycle::{recover_provider_watchdog, ProviderWatchdogRecovery};

        match recover_provider_watchdog(self, &lease.identity, chrono::Utc::now().timestamp())? {
            ProviderWatchdogRecovery::LostClaim => Ok(RecoveryOutcome::LostClaim),
            ProviderWatchdogRecovery::Recovered { reason, successor } => {
                Ok(RecoveryOutcome::Recovered { reason, successor })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryOutcome {
    LostClaim,
    Recovered {
        reason: String,
        successor: Option<(String, String)>,
    },
}

#[async_trait]
pub trait WatchdogRecovery: Send + Sync {
    async fn recover(&self, lease: &WatchdogLease) -> Result<RecoveryOutcome, String>;
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileReport {
    pub inspected: usize,
    pub ambiguous_clock: usize,
    pub claimed: usize,
    pub recovered: usize,
    pub recovery_failed: usize,
}

/// Reconcile one snapshot using an injected wall clock.
///
/// A lease is destructive-action eligible only when both its phase deadline and
/// heartbeat grace have elapsed. On startup, leases from this boot are excluded:
/// transport readiness can race new work into the database before startup
/// maintenance begins. If wall time is behind any timestamp already observed on
/// a lease, the lease is conservatively deferred until a later sweep.
pub async fn reconcile_watchdog_leases(
    db: &LocalDb,
    scope: ReconcileScope<'_>,
    now: i64,
    heartbeat_grace_secs: i64,
    recovery: &dyn WatchdogRecovery,
) -> DbResult<ReconcileReport> {
    let leases = list_open_watchdog_leases(db).await?;
    let mut report = ReconcileReport::default();
    for lease in leases {
        if matches!(scope, ReconcileScope::Startup { current_boot_id } if lease.runner_boot_id == current_boot_id)
        {
            continue;
        }
        report.inspected += 1;

        let latest_observation = lease
            .armed_at
            .max(lease.updated_at)
            .max(lease.last_provider_progress_at)
            .max(lease.last_worker_heartbeat_at);
        if now < latest_observation {
            report.ambiguous_clock += 1;
            log::warn!(
                "watchdog reconciler: wall clock moved behind durable lease observations; deferring run {} generation {}",
                lease.identity.run_id,
                lease.identity.generation
            );
            continue;
        }
        if now < lease.phase_deadline_at
            || now.saturating_sub(lease.last_worker_heartbeat_at) <= heartbeat_grace_secs
        {
            continue;
        }

        match recovery.recover(&lease).await {
            Ok(RecoveryOutcome::LostClaim) => {}
            Ok(RecoveryOutcome::Recovered { .. }) => {
                report.claimed += 1;
                report.recovered += 1;
            }
            Err(error) => {
                log::error!(
                    "watchdog reconciler: recovery failed for run {} generation {}: {}",
                    lease.identity.run_id,
                    lease.identity.generation,
                    error
                );
                report.recovery_failed += 1;
            }
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::runs::watchdog_ledger::{
        arm_watchdog, claim_watchdog_recovery, finish_watchdog_recovery, get_watchdog_lease,
        heartbeat_watchdog, NewWatchdogLease, WatchdogIdentity, WatchdogLeaseState, WatchdogPhase,
    };
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};

    struct CountingRecovery {
        calls: AtomicUsize,
        db: Arc<LocalDb>,
    }

    #[async_trait]
    impl WatchdogRecovery for CountingRecovery {
        async fn recover(&self, lease: &WatchdogLease) -> Result<RecoveryOutcome, String> {
            if !claim_watchdog_recovery(&self.db, &lease.identity, 300)
                .await
                .map_err(|error| error.to_string())?
            {
                return Ok(RecoveryOutcome::LostClaim);
            }
            self.calls.fetch_add(1, Ordering::SeqCst);
            finish_watchdog_recovery(
                &self.db,
                &lease.identity,
                300,
                true,
                "provider_watchdog_recovery",
                None,
                300,
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(RecoveryOutcome::Recovered {
                reason: "provider_watchdog_recovery".into(),
                successor: None,
            })
        }
    }

    async fn fixture(boot_id: &str) -> (Arc<LocalDb>, WatchdogIdentity) {
        let dir = tempfile::tempdir().unwrap().keep();
        let db = Arc::new(
            LocalDb::open(dir.join("watchdog-reconciler.db"))
                .await
                .unwrap(),
        );
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db.execute_batch(
            "PRAGMA foreign_keys = OFF;
             INSERT INTO sessions (id, chat_id, backend, status, sequence, created_at, updated_at)
               VALUES ('session', 'chat', 'codex', 'open', 1, 1, 1);
             INSERT INTO runs (id, status, session_id, created_at, updated_at, start_mode)
               VALUES ('run', 'live', 'session', 1, 1, 'new');
             PRAGMA foreign_keys = ON;",
        )
        .await
        .unwrap();
        let identity = WatchdogIdentity {
            run_id: "run".into(),
            session_id: "session".into(),
            provider_turn_id: "provider-turn".into(),
            generation: "generation".into(),
        };
        arm_watchdog(
            &db,
            NewWatchdogLease {
                identity: identity.clone(),
                runner_boot_id: boot_id.into(),
                phase: WatchdogPhase::PostToolContinuation,
                phase_deadline_at: 150,
                now: 100,
            },
            "armed".into(),
        )
        .await
        .unwrap();
        (db, identity)
    }

    #[tokio::test]
    async fn concurrent_reconcilers_produce_one_recovery_claim() {
        let (db, identity) = fixture("boot-a").await;
        let recovery = Arc::new(CountingRecovery {
            calls: AtomicUsize::new(0),
            db: db.clone(),
        });
        let first = reconcile_watchdog_leases(
            &db,
            ReconcileScope::Periodic,
            300,
            DEFAULT_HEARTBEAT_GRACE_SECS,
            recovery.as_ref(),
        );
        let second = reconcile_watchdog_leases(
            &db,
            ReconcileScope::Periodic,
            300,
            DEFAULT_HEARTBEAT_GRACE_SECS,
            recovery.as_ref(),
        );
        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap().claimed + second.unwrap().claimed, 1);
        assert_eq!(recovery.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            get_watchdog_lease(&db, "run", "session", "provider-turn")
                .await
                .unwrap()
                .unwrap()
                .state,
            WatchdogLeaseState::Terminalized
        );
        assert_eq!(identity.run_id, "run");
    }

    #[tokio::test]
    async fn startup_reconciles_only_predecessor_boot_leases() {
        let (db, _) = fixture("previous-boot").await;
        let recovery = CountingRecovery {
            calls: AtomicUsize::new(0),
            db: db.clone(),
        };
        let report = reconcile_watchdog_leases(
            &db,
            ReconcileScope::Startup {
                current_boot_id: "current-boot",
            },
            300,
            DEFAULT_HEARTBEAT_GRACE_SECS,
            &recovery,
        )
        .await
        .unwrap();
        assert_eq!(report.recovered, 1);

        let (current_db, _) = fixture("current-boot").await;
        let current = reconcile_watchdog_leases(
            &current_db,
            ReconcileScope::Startup {
                current_boot_id: "current-boot",
            },
            300,
            DEFAULT_HEARTBEAT_GRACE_SECS,
            &recovery,
        )
        .await
        .unwrap();
        assert_eq!(current.inspected, 0);
    }

    #[tokio::test]
    async fn stale_worker_is_recovered_after_deadline_and_grace() {
        let (db, _) = fixture("boot-a").await;
        let recovery = CountingRecovery {
            calls: AtomicUsize::new(0),
            db: db.clone(),
        };
        let report = reconcile_watchdog_leases(
            &db,
            ReconcileScope::Periodic,
            300,
            DEFAULT_HEARTBEAT_GRACE_SECS,
            &recovery,
        )
        .await
        .unwrap();
        assert_eq!(report.claimed, 1);
        assert_eq!(report.recovered, 1);
    }

    #[tokio::test]
    async fn current_worker_heartbeat_exempts_expired_deadline() {
        let (db, identity) = fixture("boot-a").await;
        assert!(heartbeat_watchdog(&db, &identity, 250).await.unwrap());
        let recovery = CountingRecovery {
            calls: AtomicUsize::new(0),
            db: db.clone(),
        };
        let report = reconcile_watchdog_leases(
            &db,
            ReconcileScope::Periodic,
            300,
            DEFAULT_HEARTBEAT_GRACE_SECS,
            &recovery,
        )
        .await
        .unwrap();
        assert_eq!(report.claimed, 0);
        assert_eq!(recovery.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn backward_clock_ambiguity_defers_destructive_action() {
        let (db, identity) = fixture("boot-a").await;
        assert!(heartbeat_watchdog(&db, &identity, 400).await.unwrap());
        let recovery = CountingRecovery {
            calls: AtomicUsize::new(0),
            db: db.clone(),
        };
        let report = reconcile_watchdog_leases(&db, ReconcileScope::Periodic, 300, 0, &recovery)
            .await
            .unwrap();
        assert_eq!(report.ambiguous_clock, 1);
        assert_eq!(report.claimed, 0);
    }
}
