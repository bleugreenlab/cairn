//! One renewal pass for every local admission lease this host holds.
//!
//! An admitted ordinary session or call keeps its admission row's lease alive
//! for as long as it runs. Renewing each lease from a task of its own puts one
//! pooled write per live lease in flight every minute, and when the engine
//! parks statements, every one of them stays parked: about 90 live sessions
//! kept about 90 renewal writes held at once on 2026-11-08. Here, every lease
//! is registered with one renewer that extends all of them in one write
//! transaction per tick, whatever the number of live sessions.
//!
//! The tick is awaited before the next is scheduled, so a renewal never starts
//! while the previous one is still parked. A renewal parked past a lease's
//! length lets `lease_expires_at` lapse, exactly as a parked per-lease
//! heartbeat did, and that is harmless on a live boot. A lane's occupancy
//! counts its `leased`/`starting`/`active` rows whatever their expiry, so a
//! lapse frees no unit. And nothing re-queues an expired row while the host
//! runs: `reap_expired_admissions` has no owner clause and re-queues every
//! expired live row, but its only production caller is startup call recovery,
//! when this boot's own leases are seconds old (and `AdmissionClient::reap`
//! has no production caller at all). Calling it periodically would break
//! this: a renewal parked past the lease length would re-queue every live
//! session, and the next pass would lease each of them a second time.
//! (The boot-scoped owner check is `requeue_orphaned_admissions`'.)

use crate::agent_process::process::AgentProcessState;
use crate::storage::{LeaseRenewal, LocalDb};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// How often every registered lease is renewed.
const RENEWAL_INTERVAL: Duration = Duration::from_secs(60);

/// One lease the renewer keeps alive.
#[derive(Debug, Clone)]
pub(crate) struct RenewedLease {
    pub request_id: String,
    pub owner: String,
    pub generation: i64,
    /// How far past each renewal the lease is extended.
    pub lease_seconds: i64,
    /// For an ordinary session, the run whose process admission claim must
    /// still name this request and generation; the lease is dropped without a
    /// write once it does not. A call has none: its row's own fence ends it.
    pub still_current: Option<String>,
}

/// The registry of leases to renew and the one task that renews them.
pub(crate) struct AdmissionRenewal {
    db: Arc<LocalDb>,
    process_state: Arc<AgentProcessState>,
    /// Keyed by request and generation, so a replacement generation of the
    /// same request is a separate lease.
    leases: Mutex<HashMap<(String, i64), RenewedLease>>,
    started: AtomicBool,
    /// [`RENEWAL_INTERVAL`] outside tests, which shorten it.
    interval: Duration,
}

impl AdmissionRenewal {
    pub(crate) fn new(db: Arc<LocalDb>, process_state: Arc<AgentProcessState>) -> Arc<Self> {
        Self::with_interval(db, process_state, RENEWAL_INTERVAL)
    }

    fn with_interval(
        db: Arc<LocalDb>,
        process_state: Arc<AgentProcessState>,
        interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            process_state,
            leases: Mutex::default(),
            started: AtomicBool::new(false),
            interval,
        })
    }

    fn leases(&self) -> std::sync::MutexGuard<'_, HashMap<(String, i64), RenewedLease>> {
        self.leases
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Start the renewal task on `handle`, once. The task holds the registry
    /// weakly and ends when the orchestrator that owns it is dropped.
    pub(crate) fn start(self: &Arc<Self>, handle: &tokio::runtime::Handle) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        handle.spawn(renew_every_interval(Arc::downgrade(self), self.interval));
    }

    /// Keep `lease` alive until its renewal misses or, for an ordinary
    /// session, its process claim moves on. Renewed from the next tick.
    pub(crate) fn register(self: &Arc<Self>, lease: RenewedLease) {
        self.leases()
            .insert((lease.request_id.clone(), lease.generation), lease);
        if !self.started.load(Ordering::SeqCst) {
            // An orchestrator built off any runtime starts its renewer with
            // the first lease registered on one.
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => self.start(&handle),
                Err(_) => {
                    log::warn!("admission lease renewal has no Tokio runtime to run on")
                }
            }
        }
    }

    /// Renew every registered lease that is still current in one write
    /// transaction, and forget the ones that are not. Returns how many rows
    /// renewed. A failed write keeps every lease for the next tick.
    pub(crate) async fn renew_once(&self, now: i64) -> usize {
        let batch: Vec<((String, i64), LeaseRenewal)> = {
            let mut leases = self.leases();
            leases.retain(|_, lease| {
                lease.still_current.as_deref().is_none_or(|run_id| {
                    self.process_state.admission_claim_is_current(
                        run_id,
                        &lease.request_id,
                        lease.generation,
                    )
                })
            });
            leases
                .iter()
                .map(|(key, lease)| {
                    (
                        key.clone(),
                        LeaseRenewal {
                            request_id: lease.request_id.clone(),
                            owner: lease.owner.clone(),
                            generation: lease.generation,
                            expires_at: now.saturating_add(lease.lease_seconds),
                        },
                    )
                })
                .collect()
        };
        if batch.is_empty() {
            return 0;
        }
        let renewals: Vec<LeaseRenewal> = batch.iter().map(|(_, lease)| lease.clone()).collect();
        match crate::storage::renew_admission_leases(&self.db, &renewals, now).await {
            Ok(renewed) => {
                let mut leases = self.leases();
                let mut count = 0;
                for ((key, _), renewed) in batch.into_iter().zip(renewed) {
                    if renewed {
                        count += 1;
                    } else {
                        // Released, or replaced by a newer generation: a stale
                        // renewal cannot revive it, so it is no longer ours.
                        leases.remove(&key);
                    }
                }
                count
            }
            Err(error) => {
                log::warn!(
                    "admission lease renewal of {} leases failed; retrying next tick: {error}",
                    renewals.len()
                );
                0
            }
        }
    }

    #[cfg(test)]
    fn registered(&self) -> usize {
        self.leases().len()
    }
}

async fn renew_every_interval(renewal: Weak<AdmissionRenewal>, interval: Duration) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // An interval's first tick completes at once; a lease is renewed one
    // interval after the renewer starts, not at registration.
    tick.tick().await;
    loop {
        tick.tick().await;
        let Some(renewal) = renewal.upgrade() else {
            return;
        };
        renewal.renew_once(chrono::Utc::now().timestamp()).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::process::ProcessAdmissionClaim;

    const OWNER: &str = "ordinary-session-promoter@1";

    async fn active_rows(name: &str, count: usize) -> Arc<LocalDb> {
        let db = crate::storage::migrated_test_db(name).await;
        let mut script = String::new();
        for i in 0..count {
            script.push_str(&format!(
                "INSERT INTO runtime_admission_requests(\
                    request_id,workspace_key,resource_lane,launch_key,claim_json,priority,\
                    enqueue_seq,state,lease_owner,lease_generation,lease_expires_at,created_at,updated_at\
                 ) VALUES(\
                    'request-{i}','private-runtime','codex','ordinary:run-{i}',\
                    '{{\"residentProcessUnits\":1,\"logicalStreamUnits\":1}}',0,\
                    {seq},'active','{OWNER}',7,300,0,0\
                 );",
                seq = i + 1,
            ));
        }
        db.execute_script(script).await.unwrap();
        Arc::new(db)
    }

    fn lease(i: usize, still_current: Option<String>) -> RenewedLease {
        RenewedLease {
            request_id: format!("request-{i}"),
            owner: OWNER.into(),
            generation: 7,
            lease_seconds: 300,
            still_current,
        }
    }

    fn claim(state: &AgentProcessState, i: usize, owner: &str, generation: i64) {
        state.prepare_admission_claim(
            format!("run-{i}"),
            ProcessAdmissionClaim {
                request_id: format!("request-{i}"),
                workspace_key: "private-runtime".into(),
                launch_key: format!("ordinary:run-{i}"),
                owner: owner.into(),
                generation,
                team_workspace_id: None,
            },
        );
    }

    async fn expiry(db: &LocalDb, i: usize) -> Option<i64> {
        crate::storage::get_admission_request(db, &format!("request-{i}"))
            .await
            .unwrap()
            .unwrap()
            .lease_expires_at
    }

    #[tokio::test]
    async fn fifty_leases_renew_in_one_write_and_outlive_their_original_lifetime() {
        let db = active_rows("renewal-fifty.db", 50).await;
        let renewal = AdmissionRenewal::new(db.clone(), Arc::default());
        for i in 0..50 {
            renewal.register(lease(i, None));
        }
        let writes = db.write_transaction_count();
        assert_eq!(renewal.renew_once(240).await, 50);
        assert_eq!(db.write_transaction_count() - writes, 1);
        assert_eq!(renewal.renew_once(480).await, 50);
        for i in 0..50 {
            assert_eq!(expiry(&db, i).await, Some(780));
        }
        assert_eq!(
            crate::storage::reap_expired_admissions(&db, 601)
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn a_replaced_generation_misses_once_and_leaves_the_registry() {
        let db = active_rows("renewal-replaced.db", 2).await;
        let renewal = AdmissionRenewal::new(db.clone(), Arc::default());
        renewal.register(lease(0, None));
        renewal.register(lease(1, None));
        db.execute(
            "UPDATE runtime_admission_requests SET lease_generation=8 WHERE request_id='request-1'",
            (),
        )
        .await
        .unwrap();
        assert_eq!(renewal.renew_once(240).await, 1);
        assert_eq!(renewal.registered(), 1);
        assert_eq!(expiry(&db, 0).await, Some(540));
        assert_eq!(
            expiry(&db, 1).await,
            Some(300),
            "the replacement is untouched"
        );
    }

    #[tokio::test]
    async fn a_private_lease_whose_claim_moved_on_is_dropped_without_a_write() {
        let db = active_rows("renewal-not-current.db", 2).await;
        let state = Arc::new(AgentProcessState::default());
        claim(&state, 0, OWNER, 7);
        // run-1's process now holds a replacement generation.
        claim(&state, 1, "replacement", 8);
        let renewal = AdmissionRenewal::new(db.clone(), state.clone());
        renewal.register(lease(0, Some("run-0".into())));
        renewal.register(lease(1, Some("run-1".into())));
        assert_eq!(renewal.renew_once(240).await, 1);
        assert_eq!(renewal.registered(), 1);
        assert_eq!(expiry(&db, 1).await, Some(300));

        claim(&state, 0, "replacement", 8);
        let writes = db.write_transaction_count();
        assert_eq!(renewal.renew_once(300).await, 0);
        assert_eq!(
            db.write_transaction_count(),
            writes,
            "nothing current, nothing written"
        );
        assert_eq!(renewal.registered(), 0);
    }

    /// A renewal parked in the database across many intervals stays the one
    /// renewal write: no tick starts another beside it, and once the slot
    /// frees it lands and renewal resumes on its interval.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_renewal_parked_across_many_ticks_is_the_only_renewal_in_flight() {
        use crate::execution::jobs::parked_writes::{
            hold_the_only_write_slot, until, waiting_from,
        };
        let db = active_rows("renewal-parked.db", 3).await;
        let (release, holder) = hold_the_only_write_slot(&db).await;
        let renewal = AdmissionRenewal::with_interval(db.clone(), Arc::default(), TEST_INTERVAL);
        renewal.start(&tokio::runtime::Handle::current());
        for i in 0..3 {
            renewal.register(lease(i, None));
        }
        until("the first renewal parks", || {
            waiting_from(&db, "runtime_admission.rs") == 1
        })
        .await;
        let requested = db.write_transaction_count();
        tokio::time::sleep(TEST_INTERVAL * 5).await;
        assert_eq!(
            waiting_from(&db, "runtime_admission.rs"),
            1,
            "five more intervals start no second renewal"
        );
        assert_eq!(db.write_transaction_count(), requested);

        release.add_permits(1);
        holder.await.unwrap();
        // A free slot only says the renewal took it; the moved expiry says
        // its transaction committed. One transaction renews all three.
        let mut committed = false;
        for _ in 0..2_000 {
            if expiry(&db, 0).await.unwrap() > 300 {
                committed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(committed, "the parked renewal never committed");
        let landed = db.write_transaction_count();
        for i in 0..3 {
            assert!(expiry(&db, i).await.unwrap() > 300, "lease {i} renewed");
        }
        // Waited for rather than slept on: a loaded runner can overrun any
        // fixed number of intervals before the next tick fires.
        until(
            "renewal resumes on its interval once the slot frees",
            || db.write_transaction_count() > landed,
        )
        .await;
    }

    /// The task renews on its interval, not at registration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_renewer_ticks_on_its_interval() {
        let db = active_rows("renewal-ticks.db", 1).await;
        let renewal = AdmissionRenewal::with_interval(db.clone(), Arc::default(), TEST_INTERVAL);
        let writes = db.write_transaction_count();
        renewal.start(&tokio::runtime::Handle::current());
        renewal.register(lease(0, None));
        tokio::time::sleep(TEST_INTERVAL / 2).await;
        assert_eq!(
            db.write_transaction_count(),
            writes,
            "not renewed at registration"
        );
        crate::execution::jobs::parked_writes::until("the first renewal", || {
            db.write_transaction_count() > writes
        })
        .await;
        crate::execution::jobs::parked_writes::until("the renewal lands", || {
            db.pooled_writes_waiting() == 0 && db.live_connections() == 0
        })
        .await;
        assert!(expiry(&db, 0).await.unwrap() > 300);
    }

    const TEST_INTERVAL: Duration = Duration::from_millis(200);
}
