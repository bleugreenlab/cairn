//! Per-team background sync loop: propagate routed writes between team members
//! via Turso Sync `push()` / `pull()`.
//!
//! Each open team replica runs TWO independent tasks (independence within a team,
//! not just across teams — a stuck push must never delay a pull):
//!
//! - The **push task** flushes local commits to the team's sync server. It does
//!   an unconditional initial push (to land frames a prior session may have
//!   crashed before pushing), then pushes whenever the replica's commit signal
//!   fires or a periodic backstop tick elapses, after a short debounce that
//!   coalesces a write burst into one push. This bounds commit→push latency under
//!   steady writes, coalesces frequency under a burst, and blocks (no busy-spin)
//!   when idle.
//! - The **pull task** periodically pulls remote frames. The self-hosted
//!   `tursodb --sync-server` does NOT honor long-poll (it returns immediately
//!   regardless of `long_poll_timeout_ms`), so a tight `pull()` loop would
//!   busy-spin — hence a fixed cadence is the staleness bound. On a pull that
//!   applied changes it emits a generic `db-change` so a running desktop
//!   re-queries the pulled data.
//!
//! Failure handling is best-effort for AVAILABILITY and fail-closed for
//! INTEGRITY: every `push()`/`pull()` is wrapped in a capped exponential backoff
//! that retries until success, never panicking and never blocking the write path.
//! No local "pushed" watermark is kept — `push()` is all-or-error and the sync
//! engine tracks the real frame watermark internally, so a failed push advances
//! nothing and its unpushed frames simply retry and land on recovery.
//!
//! Pulled frames replay as physical WAL pages, so receiver-side triggers do NOT
//! re-fire and derived state (the FTS/search outbox) is not rebuilt by a pull.
//! Rebuilding search over pulled rows is a deferred slice; this loop stays purely
//! push/pull plus the generic `db-change` emit.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde_json::json;

use crate::services::EventEmitter;
use crate::storage::LocalDb;

/// Re-derive a team's project routes after a pull applied remote frames, so a
/// teammate-created project that arrives via sync becomes routable without a
/// restart. Implemented by `DbState` (which owns the private DB and route
/// cache); injected into the pull task so `storage` stays ignorant of the
/// projects/routing layer. A `None` reconciler (every test, the static path)
/// makes the pull task pure push/pull as before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamSyncScope {
    pub(crate) team_id: String,
    pub(crate) project_ids: Vec<String>,
}

#[async_trait::async_trait]
pub trait RouteReconcile: Send + Sync {
    async fn reconcile(&self) -> Result<TeamSyncScope, String>;
}

/// Cadence knobs for one team's push and pull tasks. The defaults target prompt
/// propagation without busy-spinning; tests override them to converge in seconds.
#[derive(Debug, Clone)]
pub struct SyncCadence {
    /// Debounce after a push trigger, coalescing a write burst into one push.
    pub push_debounce: Duration,
    /// Periodic push backstop in case a commit signal is ever missed.
    pub push_backstop: Duration,
    /// Pull cadence — the remote-visibility staleness bound.
    pub pull_interval: Duration,
    /// Initial backoff after a push/pull error.
    pub backoff_base: Duration,
    /// Maximum backoff after repeated push/pull errors.
    pub backoff_cap: Duration,
}

impl Default for SyncCadence {
    fn default() -> Self {
        Self {
            push_debounce: Duration::from_millis(500),
            push_backstop: Duration::from_secs(30),
            pull_interval: Duration::from_secs(3),
            backoff_base: Duration::from_secs(1),
            backoff_cap: Duration::from_secs(60),
        }
    }
}

/// Capped exponential backoff with jitter. The shared availability helper behind
/// both tasks: on each failure it sleeps a growing, jittered delay; on success a
/// caller resets it to base. It never gives up — a permanently unreachable team
/// loops here in isolation, never touching another team's tasks or the write path.
struct Backoff {
    base: Duration,
    cap: Duration,
    current: Duration,
}

impl Backoff {
    fn new(base: Duration, cap: Duration) -> Self {
        Self {
            base,
            cap,
            current: base,
        }
    }

    fn reset(&mut self) {
        self.current = self.base;
    }

    async fn wait(&mut self) {
        let jitter = Duration::from_millis(rand::random::<u64>() % 250);
        tokio::time::sleep(self.current + jitter).await;
        self.current = (self.current * 2).min(self.cap);
    }
}

/// Push until it succeeds, backing off on each error. Cancel-safe: an abort while
/// awaiting `push()` or the backoff sleep simply drops the future, marking nothing
/// done (fail-closed integrity).
#[tracing::instrument(target = "profiler", name = "team_sync_push", skip_all)]
async fn push_until_ok(db: &LocalDb, backoff: &mut Backoff) {
    loop {
        match db.push().await {
            Ok(()) => {
                backoff.reset();
                return;
            }
            Err(error) => {
                log::warn!("team sync push failed: {error}; backing off and retrying");
                backoff.wait().await;
            }
        }
    }
}

/// Pull until it succeeds, backing off on each error; returns whether any remote
/// frames were applied. Cancel-safe for the same reason as [`push_until_ok`].
#[tracing::instrument(target = "profiler", name = "team_sync_pull", skip_all)]
async fn pull_until_ok(db: &LocalDb, backoff: &mut Backoff) -> bool {
    loop {
        match db.pull().await {
            Ok(applied) => {
                backoff.reset();
                return applied;
            }
            Err(error) => {
                log::warn!("team sync pull failed: {error}; backing off and retrying");
                backoff.wait().await;
            }
        }
    }
}

/// Drive one team replica's push side. Runs until aborted (its `JoinHandle` is
/// dropped when the team closes or `DbState` is dropped).
pub async fn run_push_task(db: Arc<LocalDb>, cadence: SyncCadence) {
    let signal = db.commit_signal();
    let mut backoff = Backoff::new(cadence.backoff_base, cadence.backoff_cap);

    // Unconditional initial push: flush any frames a prior session committed but
    // crashed before pushing.
    push_until_ok(&db, &mut backoff).await;

    // Silent-failure defense (CAIRN-2170). The turso push path counts
    // `rows_changed` from LOCAL changes and returns `Ok` even when the server
    // rejected the batch (it discards the per-step replay result), so a rejected
    // establishing push can advance the client while propagating nothing — the
    // failure mode that hid behind a benign-looking `no such table` server log.
    // We can't confirm per-step server application through today's public sync
    // API (a durable per-push server-revision ACK needs an upstream engine change
    // — tracked as a follow-up), but a confirming pull right after the
    // establishing push surfaces a server that is unreachable or erroring once we
    // believe we have pushed. The authoritative end-to-end guarantee lives in the
    // unfenced second-replica regression test; this is the cheap production hint.
    if let Err(error) = db.pull().await {
        log::error!(
            "team sync: the establishing push could not be confirmed by a follow-up pull \
             ({error}). The team server may be rejecting writes — team data may NOT be \
             propagating. See CAIRN-2170."
        );
    }

    let mut backstop = tokio::time::interval(cadence.push_backstop);
    // The first interval tick fires immediately; consume it — we just pushed.
    backstop.tick().await;

    loop {
        tokio::select! {
            // Permit-backed: a commit fired while we were elsewhere is preserved,
            // so no wakeup is lost and a burst collapses to one cycle.
            _ = signal.notified() => {}
            _ = backstop.tick() => {}
        }
        // Coalesce a burst of commits into a single push.
        tokio::time::sleep(cadence.push_debounce).await;
        push_until_ok(&db, &mut backoff).await;
    }
}

/// Drive one team replica's pull side. Runs until aborted. On a pull that applied
/// changes, reconciles project routes (so teammate-created projects that arrived
/// in this pull become routable) and emits a generic `db-change` so a running
/// desktop re-queries the pulled data (a headless host may inject a no-op
/// emitter). The reconcile runs BEFORE the emit so the route cache is current
/// when the frontend re-queries.
static PULL_APPLIED: OnceLock<tokio::sync::broadcast::Sender<String>> = OnceLock::new();

fn pull_applied_sender() -> &'static tokio::sync::broadcast::Sender<String> {
    PULL_APPLIED.get_or_init(|| tokio::sync::broadcast::channel(64).0)
}

/// Subscribe to team IDs whose pull applied frames and completed route
/// reconciliation. This process-local notification complements the periodic
/// owner sweep; physical WAL replay itself cannot fire receiver SQL triggers.
pub fn subscribe_team_pull_applied() -> tokio::sync::broadcast::Receiver<String> {
    pull_applied_sender().subscribe()
}

fn notify_team_pull_applied(scope: &Option<TeamSyncScope>) {
    if let Some(scope) = scope {
        let _ = pull_applied_sender().send(scope.team_id.clone());
    }
}

fn team_sync_change(scope: Option<TeamSyncScope>) -> serde_json::Value {
    match scope {
        Some(scope) => json!({
            "table": "team_sync",
            "action": "update",
            "teamId": scope.team_id,
            "projectIds": scope.project_ids,
        }),
        None => json!({ "table": "team_sync", "action": "update" }),
    }
}

pub async fn run_pull_task(
    db: Arc<LocalDb>,
    emitter: Arc<dyn EventEmitter>,
    cadence: SyncCadence,
    reconciler: Option<Arc<dyn RouteReconcile>>,
) {
    let mut backoff = Backoff::new(cadence.backoff_base, cadence.backoff_cap);
    loop {
        tokio::time::sleep(cadence.pull_interval).await;
        if pull_until_ok(&db, &mut backoff).await {
            let scope = match &reconciler {
                Some(reconciler) => match reconciler.reconcile().await {
                    Ok(scope) => Some(scope),
                    Err(error) => {
                        log::warn!(
                            "team sync route/scope reconciliation failed; emitting conservative unscoped refresh: {error}"
                        );
                        None
                    }
                },
                None => None,
            };
            // Physical WAL replay re-fires no triggers and carries no per-table
            // detail. Route reconciliation above is authoritative for which
            // projects belong to this replica; when it fails, omitting scope is
            // an intentional conservative fallback in the frontend.
            notify_team_pull_applied(&scope);
            let _ = emitter.emit("db-change", team_sync_change(scope));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn team_sync_change_carries_authoritative_replica_scope() {
        let payload = team_sync_change(Some(TeamSyncScope {
            team_id: "team-1".to_string(),
            project_ids: vec!["project-1".to_string(), "project-2".to_string()],
        }));

        assert_eq!(payload["table"], "team_sync");
        assert_eq!(payload["action"], "update");
        assert_eq!(payload["teamId"], "team-1");
        assert_eq!(payload["projectIds"], json!(["project-1", "project-2"]));
    }

    #[test]
    fn team_sync_change_omits_scope_for_conservative_fallback() {
        let payload = team_sync_change(None);

        assert_eq!(payload, json!({ "table": "team_sync", "action": "update" }));
    }

    #[tokio::test]
    async fn pull_applied_notification_targets_reconciled_team() {
        let mut receiver = subscribe_team_pull_applied();
        notify_team_pull_applied(&Some(TeamSyncScope {
            team_id: "team-target".to_string(),
            project_ids: vec!["project-1".to_string()],
        }));
        assert_eq!(receiver.recv().await.unwrap(), "team-target");

        notify_team_pull_applied(&None);
        assert!(receiver.try_recv().is_err());
    }
}
