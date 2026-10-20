//! The runner's own lifecycle facts, as rows.
//!
//! A log line is evidence that something was emitted; a row is what the runner
//! concluded. This module is the one sink for the second kind of fact about the
//! runner itself: its runtime stalling, executor generations attaching and
//! ending, colocated link resets, listener restages. Each is recorded once, at
//! the point the runner decided it, beside the log line that already says so.
//!
//! Three properties hold, and each is load-bearing:
//!
//! - **Recording never waits.** [`RunnerEvents::record`] is a `try_send` into a
//!   bounded channel. It is called from async supervisor code, from the
//!   watchdog's plain observer thread, and from `Fleet` paths holding a
//!   `std::sync::Mutex`, so it must never await, never block, and never hold a
//!   lock across I/O. When the channel is full the event is counted, not
//!   queued, and the count becomes a row.
//! - **The writer does not share the runner's runtime.** It is an OS thread with
//!   its own current-thread runtime, so the row for a runtime stall lands while
//!   the stall is still in progress. A writer spawned onto the stalled runtime
//!   would write the stall's row only once the stall had ended, which is too
//!   late to be the evidence it exists to be.
//! - **The table says what cleared it.** Retention prunes by age and by count;
//!   every prune and every dropped batch writes a `journal_gap` row naming how
//!   many rows and why, so a reader can always ask the table about its own
//!   holes.
//!
//! Generations restart at 1 with every runner process, so each process opens
//! with a `runner_started` row. That row is what keeps
//! [`LANES_LOST_TO_GENERATION_END_SQL`] from matching a lane to a
//! same-numbered generation of an earlier process.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{
    sync_channel, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::storage::{DbResult, LocalDb, RowExt};

/// Rows older than this are pruned.
pub const RETENTION_MS: u64 = 14 * 24 * 60 * 60 * 1_000;
/// At most this many rows are kept. During the 10-19 stall storm the runner
/// logged a stall every few seconds, roughly 30,000 rows a day, so this binds
/// at about a week in a storm and the age bound binds otherwise.
pub const RETENTION_ROWS: u64 = 200_000;

const CHANNEL_CAPACITY: usize = 4_096;
/// Events recorded before a journal is attached, which in production is the
/// moment between the fleet being built and the orchestrator attaching it, and
/// in tests is every event a `Fleet::default()` records.
const PRE_ATTACH_CAPACITY: usize = 256;
/// A batch is whatever is already queued, up to this many. The writer never
/// waits to fill one: rows held back to batch are rows an abrupt exit loses,
/// and under load batches form on their own while a write is in flight.
const BATCH_MAX: usize = 256;
const SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);
const ERROR_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// A stall's cause names every worker thread and what it was doing; bounded so
/// one pathological line cannot make a row the size of a log file.
const CAUSE_MAX_BYTES: usize = 4_096;

/// The vocabulary of `runner_events.kind`. The table carries no CHECK on it;
/// this enum is the one place the strings are defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RunnerEventKind {
    RunnerStarted,
    RunnerHandoff,
    RuntimeStall,
    RuntimeRecovered,
    ExecutorAttached,
    ExecutorAttachRefused,
    ExecutorProtocolIncompatible,
    ExecutorDetached,
    ExecutorLinkReset,
    ExecutorSubstrateFailed,
    ListenerRestaged,
    ListenerAckMismatch,
    TransportOutage,
    JournalGap,
}

impl RunnerEventKind {
    pub const ALL: [Self; 14] = [
        Self::RunnerStarted,
        Self::RunnerHandoff,
        Self::RuntimeStall,
        Self::RuntimeRecovered,
        Self::ExecutorAttached,
        Self::ExecutorAttachRefused,
        Self::ExecutorProtocolIncompatible,
        Self::ExecutorDetached,
        Self::ExecutorLinkReset,
        Self::ExecutorSubstrateFailed,
        Self::ListenerRestaged,
        Self::ListenerAckMismatch,
        Self::TransportOutage,
        Self::JournalGap,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::RunnerStarted => "runner_started",
            Self::RunnerHandoff => "runner_handoff",
            Self::RuntimeStall => "runtime_stall",
            Self::RuntimeRecovered => "runtime_recovered",
            Self::ExecutorAttached => "executor_attached",
            Self::ExecutorAttachRefused => "executor_attach_refused",
            Self::ExecutorProtocolIncompatible => "executor_protocol_incompatible",
            Self::ExecutorDetached => "executor_detached",
            Self::ExecutorLinkReset => "executor_link_reset",
            Self::ExecutorSubstrateFailed => "executor_substrate_failed",
            Self::ListenerRestaged => "listener_restaged",
            Self::ListenerAckMismatch => "listener_ack_mismatch",
            Self::TransportOutage => "transport_outage",
            Self::JournalGap => "journal_gap",
        }
    }
}

/// One fact, captured where it was decided.
#[derive(Debug, Clone, PartialEq)]
pub struct RunnerEvent {
    pub kind: RunnerEventKind,
    /// When the fact happened, stamped at the site rather than at write time:
    /// a stall's row may be written seconds later and must still say when.
    pub occurred_at_ms: u64,
    pub executor_id: Option<String>,
    pub generation: Option<u64>,
    /// The diagnostic sentence the matching log line carries.
    pub cause: Option<String>,
    pub detail: Value,
}

impl RunnerEvent {
    pub fn new(kind: RunnerEventKind) -> Self {
        Self {
            kind,
            occurred_at_ms: super::unix_time_ms(),
            executor_id: None,
            generation: None,
            cause: None,
            detail: json!({}),
        }
    }

    pub fn at(mut self, occurred_at_ms: u64) -> Self {
        self.occurred_at_ms = occurred_at_ms;
        self
    }

    pub fn executor(mut self, executor_id: impl Into<String>) -> Self {
        self.executor_id = Some(executor_id.into());
        self
    }

    pub fn generation(mut self, generation: u64) -> Self {
        self.generation = Some(generation);
        self
    }

    pub fn cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(cause.into());
        self
    }

    pub fn detail(mut self, detail: Value) -> Self {
        self.detail = detail;
        self
    }
}

fn journal_gap(reason: &str, count: u64, horizon_ms: Option<u64>) -> RunnerEvent {
    RunnerEvent::new(RunnerEventKind::JournalGap).detail(json!({
        "reason": reason,
        "count": count,
        "horizon_ms": horizon_ms,
    }))
}

fn bounded_cause(cause: &str) -> String {
    if cause.len() <= CAUSE_MAX_BYTES {
        return cause.to_string();
    }
    let mut end = CAUSE_MAX_BYTES;
    while !cause.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &cause[..end])
}

/// What the sink and its writer share. The writer holds only this, never the
/// sender, so dropping every [`RunnerEvents`] closes the channel and the
/// writer exits, releasing the database.
#[derive(Default)]
struct Shared {
    /// Events refused because the channel was full (or, before attach, pushed
    /// out of the ring), not yet accounted for by a `journal_gap` row.
    dropped: AtomicU64,
    /// Events whose write failed, not yet accounted for by a `journal_gap` row.
    write_failed: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl Shared {
    fn set_error(&self, error: String) {
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(error);
    }
}

enum Message {
    Event(RunnerEvent),
    /// Answered once every message sent before it has been written.
    Flush(SyncSender<()>),
}

enum Mode {
    Unattached(VecDeque<RunnerEvent>),
    Attached(SyncSender<Message>),
}

struct Inner {
    mode: Mutex<Mode>,
    shared: Arc<Shared>,
    /// When this sink, and so this runner incarnation, came into being. The
    /// `runner_started` row is stamped with it rather than with the attach
    /// time, so no event of this incarnation can predate its own start.
    created_at_ms: u64,
}

/// The one sink for runner lifecycle facts. Cheap to clone; every clone feeds
/// the same journal.
#[derive(Clone)]
pub struct RunnerEvents {
    inner: Arc<Inner>,
}

impl Default for RunnerEvents {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                mode: Mutex::new(Mode::Unattached(VecDeque::new())),
                shared: Arc::default(),
                created_at_ms: super::unix_time_ms(),
            }),
        }
    }
}

/// The writer's health, for the page footer.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JournalState {
    pub attached: bool,
    /// Events dropped and not yet recorded as a gap row.
    pub dropped: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Retention {
    max_age_ms: u64,
    max_rows: u64,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            max_age_ms: RETENTION_MS,
            max_rows: RETENTION_ROWS,
        }
    }
}

impl RunnerEvents {
    fn mode(&self) -> std::sync::MutexGuard<'_, Mode> {
        self.inner
            .mode
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record one fact. Never blocks, never awaits.
    pub fn record(&self, event: RunnerEvent) {
        let mut mode = self.mode();
        match &mut *mode {
            Mode::Unattached(ring) => {
                if ring.len() == PRE_ATTACH_CAPACITY {
                    ring.pop_front();
                    self.inner.shared.dropped.fetch_add(1, Ordering::Relaxed);
                }
                ring.push_back(event);
            }
            Mode::Attached(sender) => match sender.try_send(Message::Event(event)) {
                Ok(()) => {}
                Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                    self.inner.shared.dropped.fetch_add(1, Ordering::Relaxed);
                }
            },
        }
    }

    /// The events recorded so far on a sink with no journal, oldest first.
    /// Empty once a journal is attached, whose rows are the record from then.
    pub fn recent(&self) -> Vec<RunnerEvent> {
        match &*self.mode() {
            Mode::Unattached(ring) => ring.iter().cloned().collect(),
            Mode::Attached(_) => Vec::new(),
        }
    }

    /// Wait, for at most `timeout`, until everything recorded so far is
    /// written. Blocks the caller: only for a process about to end, which has
    /// nothing left to lose by waiting and its last row to lose by not.
    pub fn flush(&self, timeout: Duration) -> bool {
        let sender = match &*self.mode() {
            Mode::Unattached(_) => return true,
            Mode::Attached(sender) => sender.clone(),
        };
        let deadline = Instant::now() + timeout;
        let (ack_tx, ack_rx) = sync_channel(1);
        let mut message = Message::Flush(ack_tx);
        loop {
            match sender.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) => {
                    if Instant::now() >= deadline {
                        return false;
                    }
                    message = returned;
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(TrySendError::Disconnected(_)) => return false,
            }
        }
        ack_rx
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .is_ok()
    }

    pub fn journal_state(&self) -> JournalState {
        JournalState {
            attached: matches!(&*self.mode(), Mode::Attached(_)),
            dropped: self.inner.shared.dropped.load(Ordering::Relaxed),
            last_error: self
                .inner
                .shared
                .last_error
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
        }
    }

    /// Start the journal: write `runner_started`, then everything recorded
    /// before now, then every later event as it arrives.
    pub(crate) fn attach_journal(
        &self,
        db: Arc<LocalDb>,
        runner_incarnation: &str,
    ) -> Result<(), String> {
        self.attach_with(
            db,
            runner_incarnation,
            CHANNEL_CAPACITY,
            Retention::default(),
        )
    }

    fn attach_with(
        &self,
        db: Arc<LocalDb>,
        runner_incarnation: &str,
        capacity: usize,
        retention: Retention,
    ) -> Result<(), String> {
        let (sender, receiver) = sync_channel(capacity);
        let pending = self.install(sender)?;
        let mut initial = Vec::with_capacity(pending.len() + 1);
        initial.push(self.runner_started(runner_incarnation));
        initial.extend(pending);
        spawn_writer(
            receiver,
            db,
            runner_incarnation.to_string(),
            initial,
            self.inner.shared.clone(),
            retention,
        )
    }

    /// Switch to the channel, handing back what the ring held.
    fn install(&self, sender: SyncSender<Message>) -> Result<Vec<RunnerEvent>, String> {
        let mut mode = self.mode();
        match &mut *mode {
            Mode::Unattached(ring) => {
                let pending = ring.drain(..).collect();
                *mode = Mode::Attached(sender);
                Ok(pending)
            }
            Mode::Attached(_) => Err("the runner event journal is already attached".into()),
        }
    }

    fn runner_started(&self, runner_incarnation: &str) -> RunnerEvent {
        RunnerEvent::new(RunnerEventKind::RunnerStarted)
            .at(self.inner.created_at_ms)
            .detail(json!({
                "pid": std::process::id(),
                "build_id": cairn_checkenv::implementation_identity(),
                "runner_incarnation": runner_incarnation,
                "attached_at_ms": super::unix_time_ms(),
                "predecessor": serde_json::to_value(cairn_common::abnormal_exit::predecessor())
                    .unwrap_or(Value::Null),
            }))
    }
}

fn spawn_writer(
    receiver: Receiver<Message>,
    db: Arc<LocalDb>,
    runner_incarnation: String,
    initial: Vec<RunnerEvent>,
    shared: Arc<Shared>,
    retention: Retention,
) -> Result<(), String> {
    let on_failure = shared.clone();
    std::thread::Builder::new()
        .name("runner-events-journal".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let error = format!("runner event journal runtime could not start: {error}");
                    log::error!("{error}");
                    shared.set_error(error);
                    return;
                }
            };
            Writer {
                runtime,
                db,
                runner_incarnation,
                shared,
                retention,
                last_error_logged: None,
            }
            .run(receiver, initial);
        })
        .map(|_| ())
        .map_err(|error| {
            let error = format!("runner event journal thread could not start: {error}");
            on_failure.set_error(error.clone());
            error
        })
}

struct Writer {
    runtime: tokio::runtime::Runtime,
    db: Arc<LocalDb>,
    runner_incarnation: String,
    shared: Arc<Shared>,
    retention: Retention,
    last_error_logged: Option<Instant>,
}

impl Writer {
    fn run(mut self, receiver: Receiver<Message>, initial: Vec<RunnerEvent>) {
        self.write(initial, Vec::new());
        self.sweep();
        let mut last_sweep = Instant::now();
        loop {
            let first =
                match receiver.recv_timeout(SWEEP_INTERVAL.saturating_sub(last_sweep.elapsed())) {
                    Ok(message) => message,
                    Err(RecvTimeoutError::Timeout) => {
                        self.sweep();
                        last_sweep = Instant::now();
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                };
            let mut events = Vec::new();
            let mut acks = Vec::new();
            let mut disconnected = false;
            let mut next = Some(first);
            while let Some(message) = next.take() {
                match message {
                    Message::Event(event) => events.push(event),
                    Message::Flush(ack) => acks.push(ack),
                }
                // A flush is waited on: answer it with this batch. Everything
                // sent before it is already here.
                if !acks.is_empty() || events.len() >= BATCH_MAX {
                    break;
                }
                match receiver.try_recv() {
                    Ok(message) => next = Some(message),
                    Err(TryRecvError::Empty) => {}
                    Err(TryRecvError::Disconnected) => disconnected = true,
                }
            }
            self.write(events, acks);
            if last_sweep.elapsed() >= SWEEP_INTERVAL {
                self.sweep();
                last_sweep = Instant::now();
            }
            if disconnected {
                break;
            }
        }
    }

    /// Write one batch as one transaction, with a gap row for anything dropped
    /// or lost since the last write, then answer the batch's flushes.
    fn write(&mut self, mut events: Vec<RunnerEvent>, acks: Vec<SyncSender<()>>) {
        let recorded = events.len() as u64;
        let dropped = self.shared.dropped.swap(0, Ordering::Relaxed);
        if dropped > 0 {
            events.push(journal_gap("overflow", dropped, None));
        }
        let failed = self.shared.write_failed.load(Ordering::Relaxed);
        if failed > 0 {
            events.push(journal_gap("write_failed", failed, None));
        }
        if !events.is_empty() {
            let written =
                self.runtime
                    .block_on(insert_events(&self.db, &self.runner_incarnation, &events));
            match written {
                Ok(()) => {
                    self.shared
                        .write_failed
                        .fetch_sub(failed, Ordering::Relaxed);
                    *self
                        .shared
                        .last_error
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = None;
                }
                Err(error) => {
                    // Recorded, not thrown: the fleet keeps working, and what
                    // is lost is explanation, not correctness. The losses are
                    // counted so the next write that succeeds says so.
                    self.shared.dropped.fetch_add(dropped, Ordering::Relaxed);
                    self.shared
                        .write_failed
                        .fetch_add(recorded, Ordering::Relaxed);
                    self.note_error(format!("runner event journal write failed: {error}"));
                }
            }
        }
        for ack in acks {
            let _ = ack.try_send(());
        }
    }

    fn sweep(&mut self) {
        let swept = self.runtime.block_on(sweep_and_record(
            &self.db,
            &self.runner_incarnation,
            super::unix_time_ms(),
            self.retention,
        ));
        if let Err(error) = swept {
            self.note_error(format!(
                "runner event journal retention sweep failed: {error}"
            ));
        }
    }

    fn note_error(&mut self, error: String) {
        if self
            .last_error_logged
            .is_none_or(|at| at.elapsed() >= ERROR_LOG_INTERVAL)
        {
            log::warn!("{error}");
            self.last_error_logged = Some(Instant::now());
        }
        self.shared.set_error(error);
    }
}

#[derive(Clone)]
struct EventRow {
    id: String,
    kind: &'static str,
    occurred_at_ms: i64,
    executor_id: Option<String>,
    generation: Option<i64>,
    cause: Option<String>,
    detail_json: String,
}

async fn insert_events(
    db: &LocalDb,
    runner_incarnation: &str,
    events: &[RunnerEvent],
) -> DbResult<()> {
    // Ids are minted once, outside the transaction closure, so a retried
    // transaction writes the same rows rather than new ones.
    let rows: Vec<EventRow> = events
        .iter()
        .map(|event| EventRow {
            id: uuid::Uuid::now_v7().to_string(),
            kind: event.kind.as_str(),
            occurred_at_ms: event.occurred_at_ms as i64,
            executor_id: event.executor_id.clone(),
            generation: event.generation.map(|generation| generation as i64),
            cause: event.cause.as_deref().map(bounded_cause),
            detail_json: event.detail.to_string(),
        })
        .collect();
    let incarnation = runner_incarnation.to_string();
    db.write(move |conn| {
        let rows = rows.clone();
        let incarnation = incarnation.clone();
        Box::pin(async move {
            for row in &rows {
                conn.execute(
                    "INSERT INTO runner_events(
                         id, runner_incarnation, kind, occurred_at_ms,
                         executor_id, generation, cause, detail_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    (
                        row.id.as_str(),
                        incarnation.as_str(),
                        row.kind,
                        row.occurred_at_ms,
                        row.executor_id.as_deref(),
                        row.generation,
                        row.cause.as_deref(),
                        row.detail_json.as_str(),
                    ),
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
}

/// Prune by age, then by count, and record each prune that removed anything.
async fn sweep_and_record(
    db: &LocalDb,
    runner_incarnation: &str,
    now_ms: u64,
    retention: Retention,
) -> DbResult<()> {
    let mut gaps = Vec::new();
    let age_horizon = now_ms.saturating_sub(retention.max_age_ms) as i64;
    let aged = db
        .execute(
            "DELETE FROM runner_events WHERE occurred_at_ms < ?1",
            (age_horizon,),
        )
        .await?;
    if aged > 0 {
        gaps.push(journal_gap("pruned_age", aged, Some(age_horizon as u64)));
    }
    // The newest row beyond the cap; it and everything older go.
    let boundary = db
        .query_opt(
            "SELECT occurred_at_ms, id FROM runner_events
             ORDER BY occurred_at_ms DESC, id DESC LIMIT 1 OFFSET ?1",
            (retention.max_rows as i64,),
            |row| Ok((row.i64(0)?, row.text(1)?)),
        )
        .await?;
    if let Some((horizon, id)) = boundary {
        let capped = db
            .execute(
                "DELETE FROM runner_events
                 WHERE occurred_at_ms < ?1 OR (occurred_at_ms = ?1 AND id <= ?2)",
                (horizon, id),
            )
            .await?;
        if capped > 0 {
            gaps.push(journal_gap("pruned_cap", capped, Some(horizon as u64)));
        }
    }
    if gaps.is_empty() {
        return Ok(());
    }
    insert_events(db, runner_incarnation, &gaps).await
}

/// For each generation end at or after `?1` (unix ms), how many check
/// observations on that executor generation ended as infrastructure failures
/// because of it.
///
/// A generation ends once, but can leave two rows: a colocated reset is an
/// `executor_link_reset` stamped when the supervisor decided it, followed by
/// the `executor_detached` its route records once the socket notices. Each
/// lane is credited to the first end row of its generation in that runner
/// incarnation, so a death window adds up once, and to the row that precedes
/// the lanes it killed rather than one that may land after them.
///
/// An observation counts when it was written at or after that end, started at
/// or before it, and started in the same runner incarnation as the end: no
/// `runner_started` row lies between its start and the end. That clause is
/// what keeps a generation number reused by a later runner process from
/// matching. A lane whose machine vanished has no executor-side start (the
/// executor never reported one), so its start is `ran_at - duration_ms`, the
/// coordinator's own span for the lane. An observation with no generation is
/// never attributed. Both `ran_at` and the event times are unix milliseconds.
///
/// Quoted verbatim in the header of `0274_runner_events.sql`.
pub(crate) const LANES_LOST_TO_GENERATION_END_SQL: &str = "\
SELECT e.id, COUNT(o.id) AS lanes_lost
FROM check_result_observations o
JOIN runner_events e
  ON e.executor_id = o.executor_id
 AND e.generation = o.executor_connection_generation
WHERE o.failure_kind = 'infrastructure'
  AND o.ran_at >= ?1
  AND e.kind IN ('executor_detached', 'executor_link_reset')
  AND e.occurred_at_ms >= ?1
  AND o.ran_at >= e.occurred_at_ms
  AND COALESCE(o.executor_started_at_unix_ms, o.ran_at - o.duration_ms)
      <= e.occurred_at_ms
  AND NOT EXISTS (
      SELECT 1 FROM runner_events s
      WHERE s.kind = 'runner_started'
        AND s.occurred_at_ms > COALESCE(o.executor_started_at_unix_ms,
                                        o.ran_at - o.duration_ms)
        AND s.occurred_at_ms <= e.occurred_at_ms)
  AND NOT EXISTS (
      SELECT 1 FROM runner_events f
      WHERE f.kind IN ('executor_detached', 'executor_link_reset')
        AND f.runner_incarnation = e.runner_incarnation
        AND f.executor_id = e.executor_id
        AND f.generation = e.generation
        AND (f.occurred_at_ms < e.occurred_at_ms
             OR (f.occurred_at_ms = e.occurred_at_ms AND f.id < e.id)))
GROUP BY e.id";

/// One stored row, as the page reads it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StoredRunnerEvent {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) occurred_at_ms: u64,
    pub(crate) executor_id: Option<String>,
    pub(crate) generation: Option<u64>,
    pub(crate) cause: Option<String>,
    pub(crate) detail: Value,
}

/// Machines refused on one protocol mismatch within the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProtocolRefusals {
    pub(crate) expected: i64,
    pub(crate) received: i64,
    pub(crate) machines: u64,
}

/// What the executors page shows of the journal: one window of rows and the
/// facts that say what the table no longer holds.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct RunnerEventsWindow {
    pub(crate) since_ms: u64,
    /// Newest first, at most [`RECENT_EVENTS_SHOWN`].
    pub(crate) recent: Vec<StoredRunnerEvent>,
    /// Every row in the window, counted by kind, most frequent first.
    pub(crate) counts: Vec<(String, u64)>,
    pub(crate) protocol_refusals: Vec<ProtocolRefusals>,
    /// By event id: check lanes that ended on the generation that event ended.
    pub(crate) lanes_lost: HashMap<String, u64>,
    pub(crate) oldest_retained_ms: Option<u64>,
    pub(crate) last_gap: Option<StoredRunnerEvent>,
}

pub(crate) const RECENT_EVENTS_SHOWN: usize = 40;

const STORED_COLUMNS: &str =
    "id, kind, occurred_at_ms, executor_id, generation, cause, detail_json";

fn stored_event(row: &cairn_db::turso::Row) -> DbResult<StoredRunnerEvent> {
    Ok(StoredRunnerEvent {
        id: row.text(0)?,
        kind: row.text(1)?,
        occurred_at_ms: row.i64(2)?.max(0) as u64,
        executor_id: row.opt_text(3)?,
        generation: row.opt_i64(4)?.map(|generation| generation.max(0) as u64),
        cause: row.opt_text(5)?,
        detail: serde_json::from_str(&row.text(6)?).unwrap_or(Value::Null),
    })
}

pub(crate) async fn load_window(db: &LocalDb, since_ms: u64) -> DbResult<RunnerEventsWindow> {
    let since = since_ms as i64;
    let recent = db
        .query_all(
            format!(
                "SELECT {STORED_COLUMNS} FROM runner_events
                 WHERE occurred_at_ms >= ?1
                 ORDER BY occurred_at_ms DESC, id DESC LIMIT {RECENT_EVENTS_SHOWN}"
            ),
            (since,),
            stored_event,
        )
        .await?;
    let counts = db
        .query_all(
            "SELECT kind, COUNT(*) FROM runner_events
             WHERE occurred_at_ms >= ?1
             GROUP BY kind ORDER BY COUNT(*) DESC, kind",
            (since,),
            |row| Ok((row.text(0)?, row.i64(1)?.max(0) as u64)),
        )
        .await?;
    let protocol_refusals = db
        .query_all(
            "SELECT json_extract(detail_json, '$.expected'),
                    json_extract(detail_json, '$.received'),
                    COUNT(DISTINCT executor_id)
             FROM runner_events
             WHERE kind = 'executor_protocol_incompatible' AND occurred_at_ms >= ?1
             GROUP BY 1, 2 ORDER BY 3 DESC",
            (since,),
            |row| {
                Ok(ProtocolRefusals {
                    expected: row.opt_i64(0)?.unwrap_or_default(),
                    received: row.opt_i64(1)?.unwrap_or_default(),
                    machines: row.i64(2)?.max(0) as u64,
                })
            },
        )
        .await?;
    let lanes_lost = db
        .query_all(LANES_LOST_TO_GENERATION_END_SQL, (since,), |row| {
            Ok((row.text(0)?, row.i64(1)?.max(0) as u64))
        })
        .await?
        .into_iter()
        .collect();
    let oldest_retained_ms = db
        .query_opt("SELECT MIN(occurred_at_ms) FROM runner_events", (), |row| {
            row.opt_i64(0)
        })
        .await?
        .flatten()
        .map(|at| at.max(0) as u64);
    let last_gap = db
        .query_opt(
            format!(
                "SELECT {STORED_COLUMNS} FROM runner_events
                 WHERE kind = 'journal_gap'
                 ORDER BY occurred_at_ms DESC, id DESC LIMIT 1"
            ),
            (),
            stored_event,
        )
        .await?;
    Ok(RunnerEventsWindow {
        since_ms,
        recent,
        counts,
        protocol_refusals,
        lanes_lost,
        oldest_retained_ms,
        last_gap,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn test_db(runtime: &tokio::runtime::Runtime, name: &str) -> Arc<LocalDb> {
        Arc::new(runtime.block_on(crate::storage::migrated_test_db(name)))
    }

    /// `(kind, executor_id, generation, detail)` in the order rows were stored.
    fn stored(
        runtime: &tokio::runtime::Runtime,
        db: &LocalDb,
    ) -> Vec<(String, Option<String>, Option<i64>, Value)> {
        runtime
            .block_on(db.query_all(
                "SELECT kind, executor_id, generation, detail_json FROM runner_events ORDER BY rowid",
                (),
                |row| {
                    Ok((
                        row.text(0)?,
                        row.opt_text(1)?,
                        row.opt_i64(2)?,
                        serde_json::from_str(&row.text(3)?).unwrap(),
                    ))
                },
            ))
            .unwrap()
    }

    fn kinds(rows: &[(String, Option<String>, Option<i64>, Value)]) -> Vec<&str> {
        rows.iter().map(|row| row.0.as_str()).collect()
    }

    #[test]
    fn the_kind_vocabulary_is_pinned() {
        let strings: Vec<&str> = RunnerEventKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect();
        assert_eq!(
            strings,
            [
                "runner_started",
                "runner_handoff",
                "runtime_stall",
                "runtime_recovered",
                "executor_attached",
                "executor_attach_refused",
                "executor_protocol_incompatible",
                "executor_detached",
                "executor_link_reset",
                "executor_substrate_failed",
                "listener_restaged",
                "listener_ack_mismatch",
                "transport_outage",
                "journal_gap",
            ]
        );
        assert_eq!(strings.iter().collect::<HashSet<_>>().len(), strings.len());
    }

    #[test]
    fn events_recorded_before_attach_follow_runner_started_in_order() {
        let runtime = runtime();
        let db = test_db(&runtime, "runner_events_pre_attach.db");
        let events = RunnerEvents::default();
        events.record(RunnerEvent::new(RunnerEventKind::ListenerRestaged));
        events.record(
            RunnerEvent::new(RunnerEventKind::ExecutorAttached)
                .executor("colocated")
                .generation(1),
        );
        assert_eq!(events.recent().len(), 2);

        events.attach_journal(db.clone(), "runner-1").unwrap();
        events.record(
            RunnerEvent::new(RunnerEventKind::ExecutorDetached)
                .executor("colocated")
                .generation(1),
        );
        assert!(events.flush(Duration::from_secs(10)));

        let rows = stored(&runtime, &db);
        assert_eq!(
            kinds(&rows),
            [
                "runner_started",
                "listener_restaged",
                "executor_attached",
                "executor_detached"
            ]
        );
        assert_eq!(rows[0].3["runner_incarnation"], "runner-1");
        assert_eq!(rows[2].1.as_deref(), Some("colocated"));
        assert_eq!(rows[2].2, Some(1));
        // The incarnation's opening row predates everything it recorded, so
        // ordering by time keeps those events inside this incarnation.
        let times = runtime
            .block_on(db.query_all(
                "SELECT occurred_at_ms FROM runner_events ORDER BY rowid",
                (),
                |row| row.i64(0),
            ))
            .unwrap();
        assert!(times[1..].iter().all(|at| *at >= times[0]), "{times:?}");
        assert!(events.recent().is_empty());
        assert_eq!(events.journal_state().last_error, None);
        assert!(events.attach_journal(db, "runner-1").is_err());
    }

    /// The row for a stall must land while the stall is happening, so the
    /// writer cannot depend on the runtime the runner works on.
    #[test]
    fn the_writer_does_not_wait_for_a_stalled_runtime() {
        let stalled = Arc::new(runtime());
        let db = test_db(&stalled, "runner_events_stalled_runtime.db");
        let events = RunnerEvents::default();
        events.attach_journal(db.clone(), "runner-stall").unwrap();

        let stall_ended = Arc::new(AtomicBool::new(false));
        let (blocked_tx, blocked_rx) = std::sync::mpsc::channel();
        let blocker = {
            let stalled = stalled.clone();
            let stall_ended = stall_ended.clone();
            std::thread::spawn(move || {
                stalled.block_on(async {
                    blocked_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_secs(4));
                });
                stall_ended.store(true, Ordering::SeqCst);
            })
        };
        blocked_rx.recv().unwrap();
        events.record(RunnerEvent::new(RunnerEventKind::RuntimeStall).cause("Runner stalled"));

        let observer = runtime();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let found = observer
                .block_on(db.query_opt_text(
                    "SELECT cause FROM runner_events WHERE kind = 'runtime_stall'",
                    (),
                ))
                .unwrap();
            if found.as_deref() == Some("Runner stalled") {
                break;
            }
            assert!(Instant::now() < deadline, "the stall row never landed");
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !stall_ended.load(Ordering::SeqCst),
            "the stall row landed only after the stall ended"
        );
        blocker.join().unwrap();
    }

    #[test]
    fn events_dropped_on_a_full_channel_become_exactly_one_gap_row() {
        let runtime = runtime();
        let db = test_db(&runtime, "runner_events_overflow.db");
        let events = RunnerEvents::default();
        let (sender, receiver) = sync_channel(2);
        assert!(events.install(sender).unwrap().is_empty());
        for generation in 0..5 {
            events.record(
                RunnerEvent::new(RunnerEventKind::ExecutorDetached)
                    .executor("fedora")
                    .generation(generation),
            );
        }
        assert_eq!(events.journal_state().dropped, 3);
        spawn_writer(
            receiver,
            db.clone(),
            "runner-overflow".into(),
            Vec::new(),
            events.inner.shared.clone(),
            Retention::default(),
        )
        .unwrap();
        assert!(events.flush(Duration::from_secs(10)));

        let rows = stored(&runtime, &db);
        let gaps: Vec<&Value> = rows
            .iter()
            .filter(|row| row.0 == "journal_gap")
            .map(|row| &row.3)
            .collect();
        assert_eq!(gaps.len(), 1, "{rows:?}");
        assert_eq!(gaps[0]["reason"], "overflow");
        assert_eq!(gaps[0]["count"], 3);
        assert_eq!(
            rows.iter()
                .filter(|row| row.0 == "executor_detached")
                .count(),
            2
        );
        assert_eq!(events.journal_state().dropped, 0);
    }

    #[test]
    fn sweeps_prune_by_age_then_count_and_say_so() {
        let runtime = runtime();
        let db = test_db(&runtime, "runner_events_sweep.db");
        let retention = Retention {
            max_age_ms: 1_000,
            max_rows: 3,
        };
        let now = 10_000;
        let seed: Vec<RunnerEvent> = [100, 200, 9_100, 9_200, 9_300, 9_400, 9_500]
            .into_iter()
            .map(|at| RunnerEvent::new(RunnerEventKind::RuntimeStall).at(at))
            .collect();
        runtime
            .block_on(insert_events(&db, "runner-sweep", &seed))
            .unwrap();

        runtime
            .block_on(sweep_and_record(&db, "runner-sweep", now, retention))
            .unwrap();
        let rows = stored(&runtime, &db);
        let gaps: Vec<&Value> = rows
            .iter()
            .filter(|row| row.0 == "journal_gap")
            .map(|row| &row.3)
            .collect();
        assert_eq!(gaps.len(), 2, "{rows:?}");
        assert_eq!(gaps[0]["reason"], "pruned_age");
        assert_eq!(gaps[0]["count"], 2);
        assert_eq!(gaps[0]["horizon_ms"], 9_000);
        assert_eq!(gaps[1]["reason"], "pruned_cap");
        assert_eq!(gaps[1]["count"], 2);
        assert_eq!(gaps[1]["horizon_ms"], 9_200);
        let kept = runtime
            .block_on(db.query_all(
                "SELECT occurred_at_ms FROM runner_events WHERE kind = 'runtime_stall' ORDER BY occurred_at_ms",
                (),
                |row| row.i64(0),
            ))
            .unwrap();
        assert_eq!(kept, [9_300, 9_400, 9_500]);

        // A sweep with nothing to remove leaves no trace of itself.
        let before = stored(&runtime, &db).len();
        let roomy = Retention {
            max_age_ms: u64::MAX,
            max_rows: 100,
        };
        runtime
            .block_on(sweep_and_record(&db, "runner-sweep", now, roomy))
            .unwrap();
        assert_eq!(stored(&runtime, &db).len(), before);
    }

    fn seed_observation(
        runtime: &tokio::runtime::Runtime,
        db: &LocalDb,
        id: &str,
        failure_kind: Option<&str>,
        executor_started_at: Option<i64>,
        ran_at: i64,
        duration_ms: i64,
    ) {
        seed_observation_on(
            runtime,
            db,
            ("fedora", 5),
            id,
            failure_kind,
            executor_started_at,
            ran_at,
            duration_ms,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn seed_observation_on(
        runtime: &tokio::runtime::Runtime,
        db: &LocalDb,
        (executor_id, generation): (&str, i64),
        id: &str,
        failure_kind: Option<&str>,
        executor_started_at: Option<i64>,
        ran_at: i64,
        duration_ms: i64,
    ) {
        runtime
            .block_on(db.execute(
                "INSERT INTO check_result_observations (
                     id, project_id, commit_sha, tree_hash, check_name, input_hash,
                     environment_fingerprint, exit_code, verdict, failure_kind, complete,
                     reusable, parser_version, result_schema_version, ran_at, duration_ms,
                     cadence, executor_id, executor_connection_generation,
                     executor_started_at_unix_ms, output_tail)
                 VALUES (?1, 'project-a', 'commit', 'tree', 'rust-tests', 'input', 'env',
                         ?2, ?3, ?4, 1, 0, 1, 1, ?5, ?6, 'review', ?8, ?9, ?7, '')",
                (
                    id,
                    if failure_kind.is_some() { 1 } else { 0 },
                    if failure_kind.is_some() {
                        "failed"
                    } else {
                        "passed"
                    },
                    failure_kind,
                    ran_at,
                    duration_ms,
                    executor_started_at,
                    executor_id,
                    generation,
                ),
            ))
            .unwrap();
    }

    #[test]
    fn lanes_lost_count_only_this_incarnations_generation() {
        let runtime = runtime();
        let db = test_db(&runtime, "runner_events_lanes_lost.db");
        runtime
            .block_on(db.execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                 VALUES ('project-a', 'default', 'a', 'a', '/tmp/a', 1, 1, 0)",
                (),
            ))
            .unwrap();
        // A previous runner process, whose fedora also reached generation 5.
        // Its lane started then and was written late, after this process's
        // detach: only the incarnation boundary keeps it out.
        seed_observation(
            &runtime,
            &db,
            "previous-incarnation",
            Some("infrastructure"),
            Some(1_500),
            20_700,
            19_200,
        );
        // This process: one lane lost when generation 5 ended. Its machine
        // never reported a start, as a lost lane's never does.
        seed_observation(
            &runtime,
            &db,
            "lost",
            Some("infrastructure"),
            None,
            20_500,
            9_000,
        );
        seed_observation(&runtime, &db, "passed", None, Some(12_000), 20_600, 8_600);
        let events = [
            RunnerEvent::new(RunnerEventKind::RunnerStarted).at(1_000),
            RunnerEvent::new(RunnerEventKind::RunnerStarted).at(10_000),
            RunnerEvent::new(RunnerEventKind::ExecutorDetached)
                .at(20_000)
                .executor("fedora")
                .generation(5)
                .cause("Connection reset by peer"),
        ];
        runtime
            .block_on(insert_events(&db, "runner-lanes", &events))
            .unwrap();

        let window = runtime.block_on(load_window(&db, 15_000)).unwrap();
        assert_eq!(window.recent.len(), 1);
        let detached = &window.recent[0];
        assert_eq!(detached.kind, "executor_detached");
        assert_eq!(window.lanes_lost.get(&detached.id), Some(&1));
        assert_eq!(window.counts, [("executor_detached".to_string(), 1)]);
        assert_eq!(window.oldest_retained_ms, Some(1_000));
        assert_eq!(window.last_gap, None);

        // Without the boundary, the earlier process's lane would be blamed
        // on this one's generation 5.
        runtime
            .block_on(db.execute(
                "DELETE FROM runner_events WHERE kind = 'runner_started' AND occurred_at_ms = 10000",
                (),
            ))
            .unwrap();
        let window = runtime.block_on(load_window(&db, 15_000)).unwrap();
        assert_eq!(window.lanes_lost.get(&detached.id), Some(&2));
    }

    fn seed_project(runtime: &tokio::runtime::Runtime, db: &LocalDb) {
        runtime
            .block_on(db.execute(
                "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at, is_workspace)
                 VALUES ('project-a', 'default', 'a', 'a', '/tmp/a', 1, 1, 0)",
                (),
            ))
            .unwrap();
    }

    /// The colocated shape: the reset is decided, the lanes it abandoned are
    /// written while the child is retired, and only then does the route notice
    /// the dead socket. The lanes belong to the reset, once.
    #[test]
    fn a_colocated_reset_is_credited_once_with_the_lanes_written_after_it() {
        let runtime = runtime();
        let db = test_db(&runtime, "runner_events_colocated_reset.db");
        seed_project(&runtime, &db);
        seed_observation_on(
            &runtime,
            &db,
            ("colocated", 3),
            "abandoned",
            Some("infrastructure"),
            None,
            30_050,
            5_000,
        );
        let events = [
            RunnerEvent::new(RunnerEventKind::RunnerStarted).at(10_000),
            RunnerEvent::new(RunnerEventKind::ExecutorLinkReset)
                .at(30_000)
                .executor("colocated")
                .generation(3),
            RunnerEvent::new(RunnerEventKind::ExecutorDetached)
                .at(30_080)
                .executor("colocated")
                .generation(3),
        ];
        runtime
            .block_on(insert_events(&db, "runner-colocated", &events))
            .unwrap();

        let window = runtime.block_on(load_window(&db, 20_000)).unwrap();
        let id_of = |kind: &str| {
            window
                .recent
                .iter()
                .find(|event| event.kind == kind)
                .unwrap()
                .id
                .clone()
        };
        assert_eq!(
            window.lanes_lost.get(&id_of("executor_link_reset")),
            Some(&1)
        );
        assert_eq!(window.lanes_lost.get(&id_of("executor_detached")), None);
        assert_eq!(window.lanes_lost.values().sum::<u64>(), 1);
    }

    #[test]
    fn protocol_refusals_count_distinct_machines() {
        let runtime = runtime();
        let db = test_db(&runtime, "runner_events_protocol.db");
        let refusal = |machine: &str| {
            RunnerEvent::new(RunnerEventKind::ExecutorProtocolIncompatible)
                .executor(machine)
                .detail(json!({"expected": 151, "received": 150}))
        };
        let events = [
            refusal("bbs"),
            refusal("bbs"),
            refusal("fedora"),
            refusal("bgsrv-ub"),
            refusal("bginf-sp1"),
        ];
        runtime
            .block_on(insert_events(&db, "runner-protocol", &events))
            .unwrap();
        let window = runtime.block_on(load_window(&db, 0)).unwrap();
        assert_eq!(
            window.protocol_refusals,
            [ProtocolRefusals {
                expected: 151,
                received: 150,
                machines: 4
            }]
        );
    }

    #[test]
    fn an_overlong_cause_is_bounded_on_a_character_boundary() {
        let cause = "é".repeat(CAUSE_MAX_BYTES);
        let bounded = bounded_cause(&cause);
        assert!(bounded.len() <= CAUSE_MAX_BYTES + "…".len());
        assert!(bounded.ends_with('…'));
        assert_eq!(bounded_cause("short"), "short");
    }
}
