//! Bounded, single-flight OS keychain reads, without a completed-value cache.
//!
//! Timed-out or invalidated reads retain their dedicated OS thread and admission
//! slot until the backend returns. Neither OS prompts nor durable health writes
//! occupy Tokio's blocking pool. The notification center reports degradation
//! from a coalescing, file-backed workspace hold.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::notifications::{ExpectedRelease, Hold, HoldSource};
use crate::services::EventEmitter;

type ReadResult = Result<Option<String>, String>;
type Key = (String, String);

const MAX_ACTIVE: usize = 4;
const WAIT_TIMEOUT: Duration = Duration::from_secs(3);
#[cfg(not(target_os = "linux"))]
const GUIDANCE: &str = "Open the Cairn desktop app, unlock your login keychain, and approve any pending keychain access prompt. Then retry.";
#[cfg(target_os = "linux")]
const GUIDANCE: &str = "Restore access to Secret Service, unlock its keyring, or approve any pending desktop access prompt, then retry. A headless Linux orchestrator without Secret Service cannot resolve MCP secrets from the environment; this is not supported yet (cairn/6176).";
const TIMEOUT_REASON: &str =
    "the keychain did not answer; it may be locked or waiting for approval on the desktop";
const SOURCE: &str = "keychain_read";

#[derive(Default)]
struct Flight {
    result: Mutex<Option<ReadResult>>,
    ready: Condvar,
    timed_out: AtomicBool,
    invalidated: AtomicBool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HealthRecord {
    id: String,
    opened_at: i64,
    released_at: Option<i64>,
    reason: String,
}

#[derive(Default)]
struct State {
    flights: HashMap<Key, Arc<Flight>>,
    // Never persisted. An unrelated healthy credential cannot clear a backend
    // error; that key must be successfully read again in this process.
    failed_keys: HashSet<Key>,
    persistence_failed: bool,
    health: Option<HealthRecord>,
    emitter: Option<Arc<dyn EventEmitter>>,
}

struct ReadService {
    state: Arc<Mutex<State>>,
    admission: Condvar,
    timeout: Duration,
    max_active: usize,
    health_updates: Mutex<Option<SyncSender<()>>>,
    health_path: Option<PathBuf>,
}

impl ReadService {
    fn new(max_active: usize, timeout: Duration, health_path: Option<PathBuf>) -> Arc<Self> {
        let health = health_path
            .as_ref()
            .and_then(|path| match std::fs::read(path) {
                Ok(bytes) => {
                    Some(serde_json::from_slice(&bytes).unwrap_or_else(|_| Self::restore_failure()))
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(_) => Some(Self::restore_failure()),
            });
        Arc::new(Self {
            state: Arc::new(Mutex::new(State {
                health,
                ..State::default()
            })),
            admission: Condvar::new(),
            timeout,
            max_active,
            health_updates: Mutex::new(None),
            health_path,
        })
    }

    fn queue_health(&self, state: &mut State) {
        let Some(path) = &self.health_path else {
            return;
        };
        let mut updates = self
            .health_updates
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(send) = updates.as_ref() {
            match send.try_send(()) {
                Ok(()) | Err(mpsc::TrySendError::Full(())) => return,
                Err(mpsc::TrySendError::Disconnected(())) => *updates = None,
            }
        }
        let (send, recv) = mpsc::sync_channel(1);
        let writer_state = self.state.clone();
        let path = path.clone();
        // At most one coalescing health writer, independent of backend reads.
        match std::thread::Builder::new()
            .name("keychain-read-health".into())
            .spawn(move || health_writer(path, writer_state, recv))
        {
            Ok(_) => {
                let _ = send.try_send(());
                *updates = Some(send);
            }
            Err(_) => {
                state.persistence_failed = true;
                state.health.get_or_insert_with(Self::restore_failure);
                log::warn!("Failed to start keychain read health writer; a later health operation will retry");
            }
        }
    }

    fn restore_failure() -> HealthRecord {
        HealthRecord {
            id: uuid::Uuid::new_v4().to_string(),
            opened_at: chrono::Utc::now().timestamp(),
            released_at: None,
            reason: format!("Keychain read health could not be restored. {GUIDANCE}"),
        }
    }

    fn unavailable(reason: &str) -> String {
        format!("Keychain unavailable: {reason}. {GUIDANCE}")
    }

    // Admission and health change together, so a racing timeout cannot publish
    // degradation after completion has already published recovery.
    fn health(&self, state: &mut State, failure: Option<&str>) {
        let before = state.health.clone();
        let persistence_before = state.persistence_failed;
        let now = chrono::Utc::now().timestamp();
        match failure {
            Some(reason) => {
                let existing = state
                    .health
                    .as_ref()
                    .filter(|record| record.released_at.is_none());
                let opened_at = existing.map_or(now, |record| record.opened_at);
                let id = existing.map_or_else(
                    || uuid::Uuid::new_v4().to_string(),
                    |record| record.id.clone(),
                );
                state.health = Some(HealthRecord {
                    id,
                    opened_at,
                    released_at: None,
                    reason: if reason.starts_with("Keychain unavailable:") {
                        reason.to_owned()
                    } else {
                        Self::unavailable(reason)
                    },
                });
            }
            None => {
                if let Some(record) = &mut state.health {
                    if record.released_at.is_none() {
                        record.released_at = Some(now);
                    }
                }
            }
        }
        // Retry startup even when the health record itself did not change.
        self.queue_health(state);
        if state.health == before && state.persistence_failed == persistence_before {
            return;
        }
        if let Some(emitter) = &state.emitter {
            let _ = emitter.emit(
                "db-change",
                serde_json::json!({
                    "table": crate::notifications::HOLDS_TABLE
                }),
            );
        }
    }

    fn read(self: &Arc<Self>, service: &str, account: &str) -> ReadResult {
        self.read_with_timeout(service, account, self.timeout)
    }

    fn read_with_timeout(
        self: &Arc<Self>,
        service: &str,
        account: &str,
        timeout: Duration,
    ) -> ReadResult {
        let deadline = Instant::now() + timeout;
        let key = (service.to_owned(), account.to_owned());
        let flight = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                // Join existing work before considering the global admission cap.
                if let Some(flight) = state.flights.get(&key) {
                    break flight.clone();
                }
                if state.flights.len() >= self.max_active {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        self.health(
                            &mut state,
                            Some("all read slots remained occupied until the deadline"),
                        );
                        return Err(Self::unavailable(
                            "all read slots remained occupied until the deadline",
                        ));
                    }
                    state = self
                        .admission
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|e| e.into_inner())
                        .0;
                    continue;
                }
                // Admission waiting and the OS read share one caller deadline.
                if Instant::now() >= deadline {
                    return Err(Self::unavailable(TIMEOUT_REASON));
                }
                let flight = Arc::new(Flight::default());
                state.flights.insert(key.clone(), flight.clone());
                let worker = self.clone();
                let worker_flight = flight.clone();
                let worker_key = key.clone();
                let spawn = std::thread::Builder::new()
                    .name("keychain-read".into())
                    .spawn(move || {
                        let result =
                            std::panic::catch_unwind(|| backend_read(&worker_key.0, &worker_key.1))
                                .unwrap_or_else(|_| {
                                    Err(Self::unavailable("the backend read panicked"))
                                });
                        let mut state = worker.state.lock().unwrap_or_else(|e| e.into_inner());
                        state.flights.remove(&worker_key);
                        worker.admission.notify_all();
                        if result.is_err() {
                            state.failed_keys.insert(worker_key);
                            worker.health(&mut state, result.as_ref().err().map(String::as_str));
                        } else {
                            state.failed_keys.remove(&worker_key);
                            if state.failed_keys.is_empty()
                                && !state.flights.values().any(|flight| {
                                    flight.timed_out.load(Ordering::Relaxed)
                                        || flight.invalidated.load(Ordering::Acquire)
                                })
                            {
                                worker.health(&mut state, None);
                            }
                        }
                        *worker_flight
                            .result
                            .lock()
                            .unwrap_or_else(|e| e.into_inner()) = Some(result);
                        worker_flight.ready.notify_all();
                    });
                if spawn.is_err() {
                    state.flights.remove(&key);
                    self.admission.notify_all();
                    self.health(&mut state, Some("a read worker could not start"));
                    return Err(Self::unavailable("a read worker could not start"));
                }
                break flight;
            }
        };
        let result = flight.result.lock().unwrap_or_else(|e| e.into_inner());
        let (result, _) = flight
            .ready
            .wait_timeout_while(
                result,
                deadline.saturating_duration_since(Instant::now()),
                |result| result.is_none() && !flight.invalidated.load(Ordering::Acquire),
            )
            .unwrap_or_else(|e| e.into_inner());
        if flight.invalidated.load(Ordering::Acquire) {
            return Err("Keychain credential changed during an active read; retry after the pending read completes.".into());
        }
        if let Some(result) = result.as_ref() {
            return result.clone();
        }
        drop(result);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state
            .flights
            .get(&key)
            .is_some_and(|active| Arc::ptr_eq(active, &flight))
        {
            flight.timed_out.store(true, Ordering::Relaxed);
            self.health(&mut state, Some(TIMEOUT_REASON));
        }
        Err(Self::unavailable(TIMEOUT_REASON))
    }

    fn invalidate_matching(&self, service: &str, matches: impl Fn(&str) -> bool) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut invalidated = false;
        for ((candidate_service, account), flight) in &state.flights {
            if candidate_service == service && matches(account) {
                // Synchronize the predicate and wake to prevent lost wakeups.
                let _result = flight.result.lock().unwrap_or_else(|e| e.into_inner());
                flight.invalidated.store(true, Ordering::Release);
                flight.ready.notify_all();
                invalidated = true;
            }
        }
        if invalidated {
            self.health(
                &mut state,
                Some("an active read is still pending after a credential change"),
            );
        }
    }
}

fn health_writer(path: PathBuf, state: Arc<Mutex<State>>, updates: mpsc::Receiver<()>) {
    let mut persisted = None;
    while updates.recv().is_ok() {
        let mut backoff = Duration::from_millis(100);
        loop {
            let record = state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .health
                .clone();
            let failed = record != persisted
                && record
                    .as_ref()
                    .is_some_and(|record| persist_health(&path, record).is_err());
            if !failed {
                persisted = record;
            }
            let emitter = {
                let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
                let changed = state.persistence_failed != failed;
                state.persistence_failed = failed;
                if changed {
                    state.emitter.clone()
                } else {
                    None
                }
            };
            if let Some(emitter) = emitter {
                let _ = emitter.emit(
                    "db-change",
                    serde_json::json!({
                        "table": crate::notifications::HOLDS_TABLE
                    }),
                );
            }
            if !failed {
                break;
            }
            // Keep the latest state dirty until durable. New transitions wake
            // retries immediately; closing the channel terminates the worker.
            match updates.recv_timeout(backoff) {
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}

fn persist_health(
    path: &std::path::Path,
    record: &HealthRecord,
) -> Result<(), Box<dyn std::error::Error>> {
    let parent = path.parent().ok_or("health path has no parent")?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(&mut file, record)?;
    file.flush()?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}

fn backend_read(service: &str, account: &str) -> ReadResult {
    let entry = keyring::Entry::new(service, account).map_err(backend_error)?;
    match entry.get_password() {
        Ok(value) => Ok(Some(value)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(backend_error(error)),
    }
}

fn backend_error(error: keyring::Error) -> String {
    let reason = match error {
        keyring::Error::NoStorageAccess(_) => {
            "keychain storage cannot be accessed; it may be locked or access may be denied"
        }
        keyring::Error::BadEncoding(_) => "the stored keychain value is not valid text",
        keyring::Error::PlatformFailure(_) => "the platform keychain operation failed",
        _ => "the keychain backend read failed",
    };
    // In particular, BadEncoding can contain secret bytes. Never format the
    // backend error itself into a response, log, or durable health record.
    ReadService::unavailable(reason)
}

fn global() -> &'static Arc<ReadService> {
    static SERVICE: OnceLock<Arc<ReadService>> = OnceLock::new();
    SERVICE.get_or_init(|| {
        ReadService::new(
            MAX_ACTIVE,
            WAIT_TIMEOUT,
            Some(cairn_common::paths::cairn_home().join("keychain-read-health.json")),
        )
    })
}

/// Read with at most four active backend requests process-wide. Admission and
/// backend waiting share a three-second deadline. Only an absent entry returns
/// `Ok(None)`; timeout neither cancels the OS call nor frees its slot.
pub fn read(service: &str, account: &str) -> ReadResult {
    global().read(service, account)
}

/// Prevent an active pre-mutation read from serving a stale value to new callers.
/// The invalidated OS read still occupies its slot until it finishes.
pub(crate) fn invalidate(service: &str, account: &str) {
    global().invalidate_matching(service, |candidate| candidate == account);
}

/// Invalidate active reads in an account namespace, without releasing their slots.
pub(crate) fn invalidate_prefix(service: &str, prefix: &str) {
    global().invalidate_matching(service, |candidate| candidate.starts_with(prefix));
}

struct HealthSource(Arc<ReadService>);

impl HoldSource for HealthSource {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn holds(&self, _project_id: &str) -> Vec<Hold> {
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(record) = &state.health else {
            return Vec::new();
        };
        if !state.persistence_failed
            && record.released_at.is_some_and(|at| {
                at < chrono::Utc::now().timestamp() - crate::notifications::RECENT_WINDOW_SECS
            })
        {
            return Vec::new();
        }
        vec![Hold {
            id: format!("keychain-read:{}", record.id),
            project_id: None,
            reason: if state.persistence_failed {
                format!("{} Keychain health cannot currently be persisted; Cairn will retry automatically.", record.reason)
            } else {
                record.reason.clone()
            },
            expected_release: Some(ExpectedRelease::Text(
                "After keychain access succeeds".into(),
            )),
            override_label: "Unlock keychain in desktop".into(),
            source_uri: None,
            opened_at: record.opened_at,
            released_at: if state.persistence_failed {
                None
            } else {
                record.released_at
            },
        }]
    }

    fn override_hold(&self, _hold_id: &str) -> Result<(), String> {
        Err(GUIDANCE.into())
    }
}

/// Register this source with the notification center. Health transitions emit
/// its existing `operator_holds` invalidation frame, without credential names.
pub fn health_source(emitter: Arc<dyn EventEmitter>) -> Arc<dyn HoldSource> {
    global()
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .emitter = Some(emitter);
    Arc::new(HealthSource(global().clone()))
}

#[cfg(test)]
mod tests {
    use super::super::secrets::mock_keychain;
    use super::*;

    const TEST_TIMEOUT: Duration = Duration::from_millis(40);
    const TEST_BOUND: Duration = Duration::from_secs(5);

    fn fixture() -> (Arc<ReadService>, String) {
        mock_keychain::install();
        (
            ReadService::new(4, TEST_BOUND, None),
            uuid::Uuid::new_v4().to_string(),
        )
    }

    fn until(mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + TEST_BOUND;
        while !predicate() {
            assert!(Instant::now() < deadline, "condition did not become true");
            std::thread::yield_now();
        }
    }

    fn finish(service: &ReadService) {
        until(|| service.state.lock().unwrap().flights.is_empty());
    }

    fn short_read(reader: &Arc<ReadService>, service: &str, account: &str) -> ReadResult {
        let start = Instant::now();
        let result = reader.read_with_timeout(service, account, TEST_TIMEOUT);
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "caller exceeded bounded wait"
        );
        result
    }

    fn durable(path: &std::path::Path, resolved: bool) {
        until(|| {
            std::fs::read(path)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<HealthRecord>(&bytes).ok())
                .is_some_and(|record| record.released_at.is_some() == resolved)
        });
    }

    #[test]
    fn backend_error_categories_never_include_backend_payloads() {
        let encoded = backend_error(keyring::Error::BadEncoding(b"private-secret".to_vec()));
        assert!(encoded.contains("not valid text"));
        assert!(!encoded.contains("private-secret"));
        let denied = backend_error(keyring::Error::NoStorageAccess(
            std::io::Error::other("private-secret").into(),
        ));
        assert!(denied.contains("denied"));
        assert!(!denied.contains("private-secret"));
        let platform = backend_error(keyring::Error::PlatformFailure(
            std::io::Error::other("private-secret").into(),
        ));
        assert!(platform.contains("platform keychain operation failed"));
        assert!(!platform.contains("private-secret"));
        #[cfg(target_os = "linux")]
        {
            assert!(platform.contains(
                "without Secret Service cannot resolve MCP secrets from the environment"
            ));
            assert!(platform.contains("cairn/6176"));
        }
    }

    #[test]
    fn absent_value_error_and_no_completed_cache() {
        let (reader, service) = fixture();
        assert_eq!(reader.read(&service, "account"), Ok(None));
        let entry = keyring::Entry::new(&service, "account").unwrap();
        entry.set_password("first").unwrap();
        assert_eq!(reader.read(&service, "account"), Ok(Some("first".into())));
        entry.set_password("second").unwrap();
        assert_eq!(reader.read(&service, "account"), Ok(Some("second".into())));
        entry.set_secret(&[255]).unwrap();
        assert!(reader.read(&service, "account").is_err());
        assert_eq!(mock_keychain::read_count(&service, "account"), 4);
        assert_eq!(reader.read(&service, "unrelated"), Ok(None));
        assert!(reader
            .state
            .lock()
            .unwrap()
            .health
            .as_ref()
            .unwrap()
            .released_at
            .is_none());
        entry.set_password("recovered").unwrap();
        assert_eq!(
            reader.read(&service, "account"),
            Ok(Some("recovered".into()))
        );
        assert!(reader
            .state
            .lock()
            .unwrap()
            .health
            .as_ref()
            .unwrap()
            .released_at
            .is_some());
    }

    #[test]
    fn concurrent_waiters_receive_one_shared_result() {
        let (reader, service) = fixture();
        keyring::Entry::new(&service, "account")
            .unwrap()
            .set_password("shared")
            .unwrap();
        let gate = mock_keychain::block_reads(&service, "account");
        let callers: Vec<_> = (0..8)
            .map(|_| {
                let reader = reader.clone();
                let service = service.clone();
                std::thread::spawn(move || reader.read(&service, "account"))
            })
            .collect();
        assert!(gate.wait_for_reads(1, TEST_BOUND));
        until(|| {
            reader
                .state
                .lock()
                .unwrap()
                .flights
                .values()
                .next()
                .is_some_and(|flight| Arc::strong_count(flight) == 10)
        });
        gate.release();
        for caller in callers {
            assert_eq!(caller.join().unwrap(), Ok(Some("shared".into())));
        }
        assert_eq!(mock_keychain::read_count(&service, "account"), 1);
    }

    #[test]
    fn concurrent_waiters_share_a_worker_even_after_timeout() {
        let (reader, service) = fixture();
        let gate = mock_keychain::block_reads(&service, "account");
        let callers: Vec<_> = (0..8)
            .map(|_| {
                let reader = reader.clone();
                let service = service.clone();
                std::thread::spawn(move || short_read(&reader, &service, "account"))
            })
            .collect();
        assert!(gate.wait_for_reads(1, TEST_BOUND));
        for caller in callers {
            assert!(caller.join().unwrap().is_err());
        }
        assert!(short_read(&reader, &service, "account").is_err());
        assert_eq!(mock_keychain::read_count(&service, "account"), 1);
        assert_eq!(reader.state.lock().unwrap().flights.len(), 1);
        drop(gate);
        finish(&reader);
        assert_eq!(reader.read(&service, "account"), Ok(None));
        assert_eq!(mock_keychain::read_count(&service, "account"), 2);
    }

    #[test]
    fn four_distinct_reads_hold_the_cap_until_backend_completion() {
        let (reader, service) = fixture();
        let gates: Vec<_> = (0..4)
            .map(|index| mock_keychain::block_reads(&service, &index.to_string()))
            .collect();
        let callers: Vec<_> = (0..4)
            .map(|index| {
                let reader = reader.clone();
                let service = service.clone();
                std::thread::spawn(move || short_read(&reader, &service, &index.to_string()))
            })
            .collect();
        for gate in &gates {
            assert!(gate.wait_for_reads(1, TEST_BOUND));
        }
        for caller in callers {
            assert!(caller.join().unwrap().is_err());
        }
        assert_eq!(reader.state.lock().unwrap().flights.len(), 4);
        assert!(short_read(&reader, &service, "overflow")
            .unwrap_err()
            .contains("slots"));
        assert_eq!(mock_keychain::read_count(&service, "overflow"), 0);
        assert!(short_read(&reader, &service, "0")
            .unwrap_err()
            .contains("did not answer"));
        assert_eq!(mock_keychain::read_count(&service, "0"), 1);
        // Admission waiters wake when one OS request finally answers.
        let admitted_reader = reader.clone();
        let admitted_service = service.clone();
        let waiter =
            std::thread::spawn(move || admitted_reader.read(&admitted_service, "overflow"));
        gates[0].release();
        assert_eq!(waiter.join().unwrap(), Ok(None));
        assert!(reader
            .state
            .lock()
            .unwrap()
            .health
            .as_ref()
            .unwrap()
            .released_at
            .is_none());
        drop(gates);
        finish(&reader);
        assert_eq!(reader.read(&service, "overflow"), Ok(None));
        assert!(reader
            .state
            .lock()
            .unwrap()
            .health
            .as_ref()
            .unwrap()
            .released_at
            .is_some());
    }

    #[test]
    fn service_is_part_of_the_single_flight_key() {
        let (reader, first) = fixture();
        let second = uuid::Uuid::new_v4().to_string();
        let first_gate = mock_keychain::block_reads(&first, "same-account");
        let second_gate = mock_keychain::block_reads(&second, "same-account");
        let callers: Vec<_> = [first.clone(), second.clone()]
            .into_iter()
            .map(|service| {
                let reader = reader.clone();
                std::thread::spawn(move || short_read(&reader, &service, "same-account"))
            })
            .collect();
        assert!(first_gate.wait_for_reads(1, TEST_BOUND));
        assert!(second_gate.wait_for_reads(1, TEST_BOUND));
        for caller in callers {
            assert!(caller.join().unwrap().is_err());
        }
        assert_eq!(mock_keychain::read_count(&first, "same-account"), 1);
        assert_eq!(mock_keychain::read_count(&second, "same-account"), 1);
        drop((first_gate, second_gate));
        finish(&reader);
    }

    #[test]
    fn invalidation_refuses_stale_flights_without_freeing_the_slot() {
        let (_, service) = fixture();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.json");
        let reader = ReadService::new(4, TEST_BOUND, Some(path.clone()));
        let gate = mock_keychain::block_reads(&service, "server/token");
        let caller_reader = reader.clone();
        let caller_service = service.clone();
        let caller =
            std::thread::spawn(move || caller_reader.read(&caller_service, "server/token"));
        assert!(gate.wait_for_reads(1, TEST_BOUND));
        reader.invalidate_matching(&service, |account| account == "different");
        assert!(!reader
            .state
            .lock()
            .unwrap()
            .flights
            .values()
            .next()
            .unwrap()
            .invalidated
            .load(Ordering::Acquire));
        reader.invalidate_matching(&service, |account| account.starts_with("server/"));
        assert!(caller.join().unwrap().unwrap_err().contains("changed"));
        durable(&path, false);
        assert_eq!(reader.read(&service, "unrelated"), Ok(None));
        assert!(HealthSource(reader.clone()).holds("project")[0]
            .released_at
            .is_none());
        assert!(reader
            .read(&service, "server/token")
            .unwrap_err()
            .contains("changed"));
        assert_eq!(reader.state.lock().unwrap().flights.len(), 1);
        assert_eq!(mock_keychain::read_count(&service, "server/token"), 1);
        drop(gate);
        finish(&reader);
        keyring::Entry::new(&service, "server/token")
            .unwrap()
            .set_password("new")
            .unwrap();
        assert_eq!(
            reader.read(&service, "server/token"),
            Ok(Some("new".into()))
        );
    }

    #[test]
    fn health_writer_retries_without_another_read_or_transition() {
        mock_keychain::install();
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("blocked-parent");
        std::fs::write(&parent, "not a directory").unwrap();
        let path = parent.join("health.json");
        let reader = ReadService::new(4, TEST_BOUND, Some(path.clone()));
        let service = uuid::Uuid::new_v4().to_string();
        let gate = mock_keychain::block_reads(&service, "account");
        assert!(short_read(&reader, &service, "account").is_err());
        until(|| reader.state.lock().unwrap().persistence_failed);
        assert!(HealthSource(reader.clone()).holds("project")[0]
            .reason
            .contains("cannot currently be persisted"));
        std::fs::remove_file(&parent).unwrap();
        std::fs::create_dir(&parent).unwrap();
        // No caller wakes the writer: its retained dirty state retries itself.
        durable(&path, false);
        until(|| !reader.state.lock().unwrap().persistence_failed);
        drop(gate);
        finish(&reader);
        durable(&path, true);
    }

    #[test]
    fn restored_degradation_is_display_only_and_a_real_read_recovers_it() {
        mock_keychain::install();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.json");
        let seed = HealthRecord {
            id: uuid::Uuid::new_v4().to_string(),
            opened_at: chrono::Utc::now().timestamp(),
            released_at: None,
            reason: ReadService::unavailable(TIMEOUT_REASON),
        };
        // Simulate a prior process with no live service or writer remaining.
        persist_health(&path, &seed).unwrap();
        let reader = ReadService::new(4, TEST_BOUND, Some(path.clone()));
        let source = HealthSource(reader.clone());
        assert!(source.holds("project")[0].released_at.is_none());
        let service = uuid::Uuid::new_v4().to_string();
        keyring::Entry::new(&service, "account")
            .unwrap()
            .set_password("present")
            .unwrap();
        assert_eq!(reader.read(&service, "account"), Ok(Some("present".into())));
        assert_eq!(mock_keychain::read_count(&service, "account"), 1);
        assert!(source.holds("project")[0].released_at.is_some());
        durable(&path, true);
        let restarted = ReadService::new(4, TEST_BOUND, Some(path));
        assert!(HealthSource(restarted).holds("project")[0]
            .released_at
            .is_some());
    }

    #[test]
    fn health_is_durable_redacted_and_resolves_after_recovery() {
        mock_keychain::install();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("health.json");
        let service = uuid::Uuid::new_v4().to_string();
        let reader = ReadService::new(4, TEST_BOUND, Some(path.clone()));
        let emitter = Arc::new(crate::services::testing::CapturingEmitter::new());
        reader.state.lock().unwrap().emitter = Some(emitter.clone());
        let gate = mock_keychain::block_reads(&service, "private-account");
        assert!(short_read(&reader, &service, "private-account").is_err());
        durable(&path, false);
        let persisted = std::fs::read_to_string(&path).unwrap();
        assert!(!persisted.contains("private-account"));
        assert!(!persisted.contains(&service));
        assert!(persisted.contains("desktop"));
        assert!(persisted.contains("approve"));
        let restored = ReadService::new(4, TEST_BOUND, Some(path.clone()));
        let source = HealthSource(restored.clone());
        assert_eq!(source.holds("any-project").len(), 1);
        assert!(source.holds("any-project")[0].released_at.is_none());
        assert!(source
            .override_hold("ignored")
            .unwrap_err()
            .contains("unlock"));
        drop(gate);
        finish(&reader);
        durable(&path, true);
        let events = emitter.events_named("db-change");
        assert!(
            events.len() >= 2,
            "degradation and recovery must invalidate the notification center"
        );
        assert!(events
            .iter()
            .all(|event| event["table"] == crate::notifications::HOLDS_TABLE));
        assert!(!serde_json::to_string(&events).unwrap().contains(&service));
        drop(reader);
        assert_eq!(restored.read(&service, "private-account"), Ok(None));
        durable(&path, true);
        assert!(source.holds("any-project")[0].released_at.is_some());
        let restored_again = ReadService::new(4, TEST_BOUND, Some(path));
        assert!(HealthSource(restored_again).holds("any-project")[0]
            .released_at
            .is_some());
    }
}
