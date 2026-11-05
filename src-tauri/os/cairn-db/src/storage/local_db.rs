use std::collections::HashMap;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::panic::{AssertUnwindSafe, Location};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, MutexGuard, Once, OnceLock,
};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use cairn_platform::runtime_regions;
use cairn_platform::stack::off_frame;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::sync::{watch, Mutex as AsyncMutex, Notify};
use tokio::time::{error::Elapsed, sleep, timeout};
use turso::{params::IntoParams, Builder, Connection, Row};

use super::plan_audit::{self, PlanAudit};

const CONTENTION_ATTEMPT_THRESHOLD: usize = 1;
const CONTENTION_BACKOFF_THRESHOLD: Duration = Duration::from_millis(10);

#[derive(serde::Serialize)]
struct TransactionContentionEvent<'a> {
    event: &'static str,
    duration_us: u128,
    attempts: usize,
    backoff_us: u128,
    mode: &'a str,
}

fn transaction_contention_event(
    attempts: usize,
    backoff_slept: Duration,
    elapsed: Duration,
    begin_sql: &str,
) -> Option<TransactionContentionEvent<'_>> {
    if attempts <= CONTENTION_ATTEMPT_THRESHOLD && backoff_slept < CONTENTION_BACKOFF_THRESHOLD {
        return None;
    }

    Some(TransactionContentionEvent {
        event: "db_transaction_contention",
        duration_us: elapsed.as_micros(),
        attempts,
        backoff_us: backoff_slept.as_micros(),
        mode: if begin_sql == "BEGIN CONCURRENT" {
            "concurrent"
        } else {
            "exclusive"
        },
    })
}

fn emit_transaction_contention(
    attempts: usize,
    backoff_slept: Duration,
    elapsed: Duration,
    begin_sql: &str,
) {
    let Some(event) = transaction_contention_event(attempts, backoff_slept, elapsed, begin_sql)
    else {
        return;
    };
    match serde_json::to_string(&event) {
        Ok(encoded) => log::info!(target: "cairn.resume_timing", "{encoded}"),
        Err(error) => {
            log::warn!(target: "cairn.resume_timing", "timing event serialization failed: {error}")
        }
    }
}

use super::blocking::panic_message;
use super::content_store::{ContentStore, TeamReplicaContext};
use super::{DbError, DbResult, RowExt};
use crate::storage::TeamId;

/// Install a process-wide rustls [`CryptoProvider`](rustls::crypto::CryptoProvider)
/// exactly once, before any Turso Sync TLS client is built.
///
/// The dependency tree compiles rustls 0.23 with BOTH crypto providers:
/// `aws-lc-rs` (rustls' own default) and `ring` (pulled in by `jsonwebtoken` and
/// by rustls' `ring` feature). With two providers present, rustls cannot pick
/// one from crate features — the first TLS handshake panics in
/// `CryptoProvider::get_default_or_install_from_crate_features()`. In practice
/// that handshake is turso's sync IO building a hyper-rustls client via
/// `with_native_roots()`, which it does the INSTANT the `turso-sync-io` thread
/// spawns — at the top of the IO run loop, before it processes any queued IO,
/// not lazily on the first push/pull. Spawning that thread is a side effect of
/// `turso::sync::Builder::build()`, so the provider must already be installed by
/// the time any synced replica opens, or the sync thread races the install and
/// crashes the whole process (CAIRN-2176 / CAIRN-2196).
///
/// Installing a process default selects the provider deterministically
/// regardless of how many are compiled in, so it stays correct even if a future
/// dependency re-adds a second provider — the robust remedy rustls itself
/// recommends, rather than the fragile "keep exactly one provider in the tree".
/// We pick `aws-lc-rs` because it is rustls' modern default and turso's sync
/// client pins no provider (it resolves the process default via
/// `with_native_roots()`), so nothing requires `ring`.
///
/// Guarded by [`Once`] and idempotent: `install_default()` returns `Err` once a
/// default is already set, which we deliberately ignore. The PRIMARY install
/// site is the orchestrator constructor (`Orchestrator::build`): every host
/// binary (desktop app, dev instance, headless `cairn-server`) builds the
/// orchestrator synchronously at startup, before it starts team sync or opens
/// any replica, so the provider is in place before a `turso-sync-io` thread can
/// ever spawn. The synced-open paths below also call this as a
/// belt-and-suspenders guard for any caller that opens a [`LocalDb`] replica
/// directly without an orchestrator (e.g. tests).
pub fn install_crypto_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    });
}

/// How many idle connections one [`LocalDb`] retains for reuse.
///
/// This bounds retained file descriptors in a long-lived process, not
/// concurrency: [`LocalDb::checkout`] creates a connection rather than waiting
/// when the free-list is empty, and a connection released past the cap is simply
/// dropped. Warm connections are the point of the pool, so the cap only needs to
/// cover the realistic peak of *simultaneous* transactions.
const MAX_IDLE_CONNECTIONS: usize = 32;

/// Per-connection page cache ceiling, as a negative `PRAGMA cache_size` (KiB).
///
/// The engine's default is `-2000` — two megabytes, a few hundred pages. The MVCC
/// checkpoint walks the b-tree applying every row in the logical log while holding
/// a cursor per b-tree, and a cursor pins the pages it is parked on, so a schema
/// with a few hundred b-trees puts real pressure on a cache that small.
///
/// This began as a correctness floor. On the engine revision pinned through August
/// 2026 the checkpoint reached a page the cache could not admit, gave up, and
/// reported that as `Busy` — indistinguishable from contention, and reported
/// against a database nothing else was touching. Worse, the failure was not safe:
/// the checkpoint had already published a b-tree root page the rollback then took
/// back, so later reads of that table ran off the end of the file (CAIRN-3838).
///
/// Neither is true on the current pin. A cache that cannot admit a page spills and
/// then force-inserts past its nominal capacity instead of failing, and root-map
/// mutations are staged until the pager commit. What remains is a working-set
/// target: a checkpoint with room to work spills far less, and a checkpoint that
/// cannot finish promptly is a logical log that does not truncate. Measured against
/// a copy of a 9.2 GB database carrying a 136 MB logical log, 16 MB completed and
/// reset the log to 98 bytes where the 2 MB default did not; 64 MB is that with 4x
/// headroom.
///
/// It is a ceiling rather than a reservation, so a connection that never
/// checkpoints does not pay it, and the cost is bounded by the pool's
/// [`MAX_IDLE_CONNECTIONS`] plus whatever transactions are genuinely in flight.
const PAGE_CACHE_LIMIT: i32 = -65536;

/// `PRAGMA mvcc_checkpoint_threshold` value that disables the engine's
/// commit-path automatic checkpoint outright, leaving Cairn to checkpoint on a
/// cadence of its own.
///
/// The MVCC engine arms a checkpoint on the *committing* connection once the
/// logical log grows past a byte threshold (about 4.12 MB by default). That
/// checkpoint runs in TRUNCATE mode — the engine's passive mode is behind an
/// experimental builder flag Cairn deliberately does not set (see
/// `docs/database.md`) — and TRUNCATE takes the store's checkpoint lock in write
/// mode. Every MVCC transaction, read as well as write, explicit
/// `BEGIN CONCURRENT` as well as a lone autocommit statement, holds that same
/// lock in read mode for its whole lifetime. So the attempt can only win in an
/// instant where *zero* transactions are open anywhere in the process, and commit
/// time is the least likely such instant there is. Nor does it fail cheaply: the
/// engine honours the connection's `busy_timeout` by retrying internally, so a
/// losing attempt blocks for that entire timeout (see
/// [`CHECKPOINT_BUSY_TIMEOUT`]).
///
/// Worse, the arming is permanent rather than periodic: the threshold is a level,
/// not an edge, so once the log is over it every subsequent commit re-arms. In
/// production this failed roughly 130,000 times a day without succeeding once,
/// a third of all runner log volume, while the log grew to 537 MB (CAIRN-4146).
///
/// Disabling it also revives garbage collection. The engine reclaims invisible
/// row versions inline only on the commit branch where the checkpoint did *not*
/// fire, so a permanently-armed checkpoint starves the collector as well as the
/// log — a database in this state gets neither, and accumulates row versions in
/// memory without bound.
const CHECKPOINT_THRESHOLD_DISABLED: i64 = -1;

/// `busy_timeout` for the connection a checkpoint runs on.
///
/// Ordinary connections get five seconds ([`RetryConfig::default`]), and the
/// engine honours a busy timeout by retrying internally rather than returning
/// promptly — so a contended `PRAGMA wal_checkpoint(TRUNCATE)` blocked for the
/// full five seconds before reporting `database is locked`. That is what made a
/// hundred-attempt pass take ten to thirteen minutes against a five-minute
/// interval, so passes ran back to back with no gap at all (CAIRN-4167). The
/// design had assumed a losing attempt cost a compare-and-exchange.
///
/// A gated checkpoint has already established the precondition it needs before
/// it issues the statement, so waiting long inside the engine adds nothing:
/// either the drain emptied the gate and the attempt wins at once, or something
/// is still holding a transaction and no amount of internal retry will change
/// that within this pass.
const CHECKPOINT_BUSY_TIMEOUT: Duration = Duration::from_millis(250);

/// How long a pass's TRUNCATE attempts run, once the drain has finished,
/// before the pass logs that the fold is slow. The gate stays shut past it.
///
/// A fold that is merely slow must be waited out, not abandoned. On the pinned
/// engine a transaction that begins while a TRUNCATE is still running gets
/// `database is locked` rather than waiting, so reopening the gate onto a
/// running fold turns a pause every caller has already paid into a failure for
/// every caller that arrives next: on 11-03 a 236 MiB fold that ran 36.6 s
/// under a CPU storm failed 62 calls that way, in a boot that had no other
/// `database is locked` (cairn/5655). Production folds of 150 to 236 MiB took
/// 3.5 to 6 s when the host was healthy, so twenty seconds is a fold worth
/// naming, and the line that names it is the one a wedge watcher samples on.
pub const CHECKPOINT_SLOW_AFTER: Duration = Duration::from_secs(20);

/// How long a pass's TRUNCATE attempts may keep the connection gate shut, once
/// the drain has finished, before the pass gives them up as a suspected engine
/// wedge and reopens the gate.
///
/// The pinned Turso runs an MVCC `PRAGMA wal_checkpoint(TRUNCATE)` to the end
/// inside a single poll: `op_checkpoint` drives the checkpoint state machine in
/// a loop and waits on each I/O completion inline, so the statement never
/// yields to the runtime. An attempt that does not finish therefore blocks its
/// thread rather than parking a future, and a timeout wrapped around that
/// future would never be polled again. So each attempt runs on its own OS
/// thread, and the pass waits for it at most this long. Past the backstop the
/// pass gives the attempt up, the gate reopens, and the thread is left to
/// finish in the engine (see [`LocalDb::checkpoint`]).
///
/// Two minutes covers a 256 MiB fold at the worst production throughput yet
/// measured (6.5 MB/s) three times over, and stays far inside every caller's
/// own bound ([`READ_STATEMENT_BOUND`], the MCP socket's 795 s), so a real
/// wedge still reopens the gate before any caller gives up on it. Reaching it
/// is the specimen of a fold that blocks rather than runs slowly; the three
/// wedges of 10-30 (cairn/5644) are the only ones yet seen.
pub const CHECKPOINT_WEDGE_BACKSTOP: Duration = Duration::from_secs(120);

/// Logical-log size at which a running process folds: the periodic
/// checkpoint pass (`db_maintenance`) and the retention evictor
/// (`retention::EvictorBudget::PRODUCTION`) both act at it, so one number
/// decides when the log is big enough to fold.
///
/// The gate's pause is the fold, so the size is chosen for the fold's length:
/// at the 15 to 23 MB a minute production writes, 64 MiB comes round every
/// three to four minutes, folds in about a second on a healthy host, and in
/// about ten at the worst production throughput yet measured (6.5 MB/s).
pub const FOLD_AT_LOG_BYTES: u64 = 64 * 1024 * 1024;

const _: () = assert!(
    CHECKPOINT_SLOW_AFTER.as_millis() < CHECKPOINT_WEDGE_BACKSTOP.as_millis(),
    "a fold must be reported slow before it is abandoned"
);

/// The source line that asked for a database connection: the caller of a
/// public [`LocalDb`] entry point, captured with `#[track_caller]`.
pub type Caller = &'static Location<'static>;

/// How long one pooled READ statement may run before the engine interrupts it.
///
/// The engine checks this deadline before every VM instruction, which is what
/// makes it a bound at all: a statement stepping inside a single `poll` cannot
/// be dropped, timed out, or cancelled from Rust, and on 10-26 one such
/// statement held a pooled connection for 9.9 hours, blocking every checkpoint
/// while the logical log grew to 8.5 GB (tenet #1271). Ten minutes is far past
/// any read the orchestrator has recorded, so reaching it means a runaway
/// statement. Writes and `exclusive` are not bounded, and neither is `connect`,
/// which is for writers, migrations and probes only: every reader goes through
/// the pool, where this bound and [`PARKED_READ_SLACK`] apply.
pub const READ_STATEMENT_BOUND: Duration = Duration::from_secs(600);

/// How far past the bound a pooled read that PARKS between statements (and so
/// never trips the per-statement deadline) is dropped. Dropping the future
/// retires its connection, and the engine rolls the transaction back.
const PARKED_READ_SLACK: Duration = Duration::from_secs(5);

/// How far past the bound the gate sweep interrupts a pooled read that is
/// still held. The deadline and the outer timeout should both have ended it
/// long before, so this firing is a finding, and it says so in the log.
const READ_BACKSTOP_SLACK: Duration = Duration::from_secs(60);

/// How much of a statement's SQL a label carries.
const SQL_HEAD_CHARS: usize = 80;

/// The runtime region a database call's polls run inside.
const STATEMENT_REGION: &str = "db.statement";

/// Lets a returned `impl Future` borrow `'s` without requiring every type it
/// captures to outlive `'s`, which is what edition 2021's `+ 's` would demand
/// of the `sql` and `params` types [`TrackedConnection`]'s statement methods
/// are generic over.
pub trait Captures<T: ?Sized> {}
impl<T: ?Sized, U: ?Sized> Captures<T> for U {}

/// What a live connection is for, as the gate records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HolderKind {
    /// A pooled read: `read` and the `query_*` helpers. The only kind bounded.
    PooledRead,
    /// A pooled write: `write`, `exclusive`, `execute*`, `consume_query`.
    PooledWrite,
    /// An out-of-pool connection from `connect` (or the content store), held
    /// for as long as its caller keeps it.
    Connect,
}

impl std::fmt::Display for HolderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::PooledRead => "pooled read",
            Self::PooledWrite => "pooled write",
            Self::Connect => "connect",
        })
    }
}

/// `file:line`, without the column `Location`'s own `Display` adds.
pub(crate) struct Site(pub(crate) Caller);

impl std::fmt::Display for Site {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0.file(), self.0.line())
    }
}

/// `<file>:<line> <first 80 chars of sql, whitespace collapsed>`.
fn statement_label(at: Caller, sql: Option<&str>) -> Arc<str> {
    use std::fmt::Write as _;
    let mut label = String::with_capacity(at.file().len() + 8 + SQL_HEAD_CHARS);
    let _ = write!(label, "{}:{}", at.file(), at.line());
    if let Some(sql) = sql {
        label.push(' ');
        let mut taken = 0;
        let mut gap = false;
        for ch in sql.trim().chars() {
            if taken >= SQL_HEAD_CHARS {
                break;
            }
            if ch.is_whitespace() {
                gap = true;
                continue;
            }
            if gap {
                label.push(' ');
                taken += 1;
                gap = false;
            }
            label.push(ch);
            taken += 1;
        }
    }
    Arc::from(label)
}

/// `<file>:<line> (raw connection; sql not captured)`: the label for a
/// statement issued through [`TrackedConnection`]'s `Deref`, which sees where
/// the connection was borrowed but not what was run on it.
fn raw_statement_label(at: Caller) -> Arc<str> {
    Arc::from(format!(
        "{}:{} (raw connection; sql not captured)",
        at.file(),
        at.line()
    ))
}

/// What one database call is doing, shared by the call's future, its
/// connection, and the connection's gate record: the statement it last issued,
/// how many of its tracked statements are still running, and a handle on the
/// connection itself.
///
/// Per call rather than per gate, so recording a statement takes an
/// uncontended lock and never the gate's.
#[derive(Clone)]
pub(super) struct Holder(Arc<HolderShared>);

struct HolderShared {
    state: Mutex<HolderState>,
    /// Tracked statements still running: an `execute` not yet resolved, or a
    /// `query` whose [`TrackedRows`] are still alive. What tells a connection
    /// that is executing apart from one parked between statements.
    in_flight: AtomicUsize,
    /// Set by [`TrackedConnection::exempt_from_plan_audit`] for the rest of
    /// this call.
    plan_audit_exempt: AtomicBool,
}

struct HolderState {
    statement: Arc<str>,
    /// The raw-connection site `statement` names, when it names one, so a
    /// helper that borrows the connection repeatedly from one line relabels
    /// once rather than on every borrow.
    raw_site: Option<Caller>,
    /// A clone of the connection, for a pooled read (so the gate sweep can
    /// interrupt it) and for a `connect` holder (so the sweep can see whether
    /// it holds a transaction open). Taken out when the slot drops: a clone
    /// keeps the core connection alive, and the engine rolls a transaction back
    /// only when the last one drops, which must happen before the gate reports
    /// the slot released.
    handle: Option<Connection>,
}

/// What a holder's connection was doing when the gate sweep looked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HolderActivity {
    /// A tracked statement was running on it.
    pub executing: bool,
    /// Whether a transaction was open, or `None` when the gate holds no handle
    /// on the connection to ask (a pooled write).
    pub in_transaction: Option<bool>,
}

impl Holder {
    pub(super) fn new(caller: Caller, sql: Option<&str>) -> Self {
        Self(Arc::new(HolderShared {
            state: Mutex::new(HolderState {
                statement: statement_label(caller, sql),
                raw_site: None,
                handle: None,
            }),
            in_flight: AtomicUsize::new(0),
            plan_audit_exempt: AtomicBool::new(false),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, HolderState> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record the statement this call is about to run, and rename the polling
    /// thread's `db.statement` region to it. The region was entered with the
    /// label current when the poll began, and the binding yields only on I/O,
    /// so a statement over cached pages starts and runs inside that same poll.
    fn set_statement(&self, at: Caller, sql: &str) {
        let label = statement_label(at, Some(sql));
        {
            let mut state = self.lock();
            state.statement = label.clone();
            state.raw_site = None;
        }
        runtime_regions::set_innermost_detail(STATEMENT_REGION, label);
    }

    /// Record that the raw connection was borrowed at `at`, which is where
    /// every statement a `&Connection` helper runs is attributed: the helper's
    /// call site, rather than the last statement the wrapper saw.
    fn set_raw_site(&self, at: Caller) {
        let label = {
            let mut state = self.lock();
            if state.raw_site.is_some_and(|site| site == at) {
                return;
            }
            let label = raw_statement_label(at);
            state.raw_site = Some(at);
            state.statement = label.clone();
            label
        };
        runtime_regions::set_innermost_detail(STATEMENT_REGION, label);
        super::plan_audit::record_raw_site(at);
    }

    fn statement(&self) -> Arc<str> {
        self.lock().statement.clone()
    }

    fn plan_audit_exempt(&self) -> bool {
        self.0.plan_audit_exempt.load(Ordering::Relaxed)
    }

    fn start_statement(&self) -> InFlight {
        self.0.in_flight.fetch_add(1, Ordering::Relaxed);
        InFlight(self.clone())
    }

    fn activity(&self) -> HolderActivity {
        let executing = self.0.in_flight.load(Ordering::Relaxed) > 0;
        let in_transaction = self
            .lock()
            .handle
            .as_ref()
            .and_then(|conn| conn.is_autocommit().ok())
            .map(|autocommit| !autocommit);
        HolderActivity {
            executing,
            in_transaction,
        }
    }

    fn arm(&self, conn: Connection) {
        self.lock().handle = Some(conn);
    }

    fn disarm(&self) {
        let handle = self.lock().handle.take();
        drop(handle);
    }

    /// Ask the engine to interrupt whatever is running on the connection.
    /// `true` means the request was made, not that a statement was stopped:
    /// the engine treats an interrupt with nothing running as a no-op.
    fn interrupt(&self) -> bool {
        self.lock()
            .handle
            .as_ref()
            .is_some_and(|conn| conn.interrupt().is_ok())
    }
}

/// One tracked statement counted as running on its holder until dropped.
struct InFlight(Holder);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0 .0.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for Holder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Holder").field(&self.statement()).finish()
    }
}

/// One database call, as the caller awaiting it holds it: the holder naming
/// the call, and a pointer to the call's body.
///
/// Nearly every layer of a served path awaits several of these, so their size
/// is paid many times over. In an unoptimized build each awaited child is
/// built in its parent's frame and kept in its state (see
/// `cairn_platform::stack`). The body, which carries the checkout, the
/// transaction, the read bound and the caller's own closure, is therefore
/// built in [`off_frame`]'s frame and lives on the heap, so a call costs its
/// caller three words whatever it does inside.
/// `a_database_call_costs_its_caller_three_words` holds that.
///
/// Each poll runs inside a `db.statement` runtime region naming the statement
/// the holder last recorded. Per poll, because the region guard is `!Send` and
/// cannot be held across an await, and because a poll is exactly where a
/// runaway statement sits: a worker that has not parked for an hour is inside
/// one poll of one of these, and the runtime stall line then names the
/// statement instead of only the worker.
#[must_use = "a database call does nothing unless awaited"]
pub struct DbCall<'s, T> {
    holder: Holder,
    body: BoxFuture<'s, DbResult<T>>,
}

impl<'s, T> DbCall<'s, T> {
    fn new<Fut>(holder: Holder, make: impl FnOnce() -> Fut) -> Self
    where
        Fut: Future<Output = DbResult<T>> + Send + 's,
    {
        Self {
            holder,
            body: off_frame(make),
        }
    }
}

impl<T> Future for DbCall<'_, T> {
    type Output = DbResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _region = runtime_regions::enter_with(STATEMENT_REGION, self.holder.statement());
        self.body.as_mut().poll(cx)
    }
}

/// One holder that has kept its connection past the watch threshold.
#[derive(Debug, Clone)]
pub struct OverdueHolder {
    pub caller: Caller,
    pub kind: HolderKind,
    pub age: Duration,
    /// `<file>:<line> <sql head>` of the statement it last issued, or
    /// `<file>:<line> (raw connection; sql not captured)` where it last
    /// borrowed the raw connection.
    pub statement: Arc<str>,
    pub activity: HolderActivity,
}

impl OverdueHolder {
    /// What the holder is doing, for the gate watch's line.
    ///
    /// A `connect` holder is the one this has to spell out. It is never cut:
    /// the engine's interrupt is a no-op with no statement running, so one
    /// parked inside an open transaction lasts until its owner drops the
    /// connection, and the line says so rather than implying a backstop.
    pub fn describe(&self) -> String {
        if self.kind != HolderKind::Connect {
            return self.statement.to_string();
        }
        match self.activity {
            HolderActivity {
                executing: true, ..
            } => format!("executing {}", self.statement),
            HolderActivity {
                in_transaction: Some(true),
                ..
            } => format!(
                "parked in an open transaction since {}; a parked connect holder cannot be cut \
                 and ends only when its owner drops it",
                self.statement
            ),
            HolderActivity {
                in_transaction: Some(false),
                ..
            } => format!("no transaction open (last: {})", self.statement),
            HolderActivity {
                in_transaction: None,
                ..
            } => self.statement.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub busy_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 32,
            initial_backoff: Duration::from_millis(2),
            max_backoff: Duration::from_millis(250),
            busy_timeout: Duration::from_secs(5),
        }
    }
}

/// Backing database engine for a [`LocalDb`]. A local file database and a Turso
/// Sync replica expose an identical `turso::Connection` surface, so every query
/// helper on `LocalDb` routes through one `connect()` regardless of which engine
/// backs it. Only `push()`/`pull()` and the journaling pragma differ between
/// the two arms.
pub(super) enum DbHandle {
    /// A plain on-disk (or `:memory:`) database opened via `Builder::new_local`.
    Local(turso::Database),
    /// A Turso Sync replica opened via `turso::sync::Builder::new_remote`. Reads
    /// and writes are local; `push()`/`pull()` reconcile with the sync server.
    Synced(turso::sync::Database),
}

impl std::fmt::Debug for DbHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbHandle::Local(_) => f.write_str("DbHandle::Local"),
            DbHandle::Synced(_) => f.write_str("DbHandle::Synced"),
        }
    }
}

pub struct LocalDb {
    path: PathBuf,
    database: Arc<DbHandle>,
    retry: RetryConfig,
    /// Fired after every successful transaction on a SYNCED replica (never on a
    /// local database). The per-team push task waits on it to push promptly once
    /// writes settle; permit-backed, so a burst of commits collapses to a single
    /// pending wakeup and none is lost.
    commit_signal: Arc<Notify>,
    /// Monotonic admission-input signal. The orchestrator owns the receiver and
    /// cache; the database never retains a callback into its consumer.
    provider_admission_changes: watch::Sender<u64>,
    /// Process-unique identity for this OPEN of a database, never reused.
    ///
    /// [`Self::mutation_generation`] is per HANDLE and starts at zero, so it
    /// identifies a state of this handle and says nothing across handles. A
    /// consumer that caches by `(path, generation)` alone — the channel router's
    /// gate source is one — would hand a database reopened at the same path the
    /// previous handle's snapshot the moment its counter reached the same
    /// number. Pairing the two is what makes a cache key mean "this state of this
    /// database" rather than "some state of some database that lived here".
    instance: u64,
    /// Monotonic in-process generation advanced after every successful mutation.
    /// Readers use this to make expensive projections incremental without polling
    /// the database itself. Relaxed ordering is sufficient: the committed database
    /// transaction is the synchronization boundary; this counter only detects change.
    mutation_generation: AtomicU64,
    /// Set ONLY for a team replica: its intrinsic team id plus the per-team
    /// content store archival offloads to and reconstruction fetches from. The
    /// private DB carries `None`, so archival/reconstruct branch on
    /// `team_content_store()` and the local-run inline path is byte-for-byte
    /// unchanged.
    team: Option<Arc<TeamReplicaContext>>,
    /// Set with `team`. The private database's attachment bytes live in its
    /// own `cas_cache` table, reached through the pool like any other row.
    team_content_store: Option<Arc<dyn ContentStore>>,
    /// The private database's archival store (a file store beside the
    /// database), installed once at open through
    /// [`Self::set_private_content_store`] after the handle is shared.
    private_content_store: OnceLock<Arc<dyn ContentStore>>,
    /// Connections that hold no open transaction and are free to be reused.
    ///
    /// The first `BEGIN CONCURRENT` on a fresh turso connection pays a one-time
    /// MVCC transaction setup cost; later transactions on that connection are
    /// substantially cheaper. Opening a connection per call therefore repeated
    /// avoidable setup on every read and write, in the running app as much as in
    /// tests. Connections are
    /// checked out for the duration of one transaction and returned only when
    /// they are known to hold none.
    idle: Mutex<Vec<Connection>>,
    /// Every connection this handle has live, and the valve maintenance closes
    /// to empty that set so a checkpoint has an instant to work in. See
    /// [`ConnectionGate`].
    gate: Arc<ConnectionGate>,
    /// Serializes the full checkpoint pass, from opening its private connection
    /// through releasing the gate hold. Without this, one overlapping pass can
    /// drop its hold and reopen the boolean gate while another is still draining
    /// or running TRUNCATE.
    checkpoint_lock: AsyncMutex<()>,
    /// Set while a TRUNCATE attempt runs on its checkpoint thread, and cleared
    /// by that thread when the engine returns. A pass that finds it set follows
    /// one whose attempt outlived [`CHECKPOINT_WEDGE_BACKSTOP`], and must
    /// neither start a second TRUNCATE beside it nor close the gate for an
    /// attempt it cannot make.
    checkpoint_in_engine: Arc<AtomicBool>,
    /// Read transactions opened over this handle's life. Lets a test assert
    /// that an operation's transaction count is a constant rather than a
    /// function of how much data it covers — a load-independent guard against
    /// reintroducing per-row resolution. Exposed to `test-utils` consumers
    /// because the drains that most need that guard live in cairn-core.
    #[cfg(any(test, feature = "test-utils"))]
    read_transaction_count: AtomicUsize,
    /// Write transactions requested over this handle's life. Test-only evidence
    /// that read-only empty paths do not enter the contended writer pool.
    #[cfg(any(test, feature = "test-utils"))]
    write_transaction_count: AtomicUsize,
    /// Connections handed out by [`LocalDb::connect`] over this handle's life.
    /// Lets tests assert that a run of sequential operations reuses one
    /// connection rather than creating one per call — a load-independent guard
    /// against reintroducing per-call connect.
    #[cfg(test)]
    connections_created: AtomicUsize,
    /// [`READ_STATEMENT_BOUND`] outside tests, which shorten it.
    read_bound: Duration,
    /// Forces the history read seam's source for tests and the clone harness.
    /// Not compiled into a production build, so no production path can reach it.
    #[cfg(any(test, feature = "test-utils"))]
    history_source_override: Mutex<Option<crate::storage::events::history::HistorySource>>,
    /// Schema capability, established at open and after migrations, never on read.
    history_catalog_present: AtomicBool,
    /// Monotonic for this handle: eviction must raise this after committing.
    history_evicted: AtomicBool,
}

impl std::fmt::Debug for LocalDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDb")
            .field("path", &self.path)
            .field("database", &self.database)
            .field("retry", &self.retry)
            .field("team", &self.team)
            .field(
                "team_content_store",
                &self
                    .team_content_store
                    .as_ref()
                    .map(|_| "<dyn ContentStore>"),
            )
            .finish()
    }
}

/// Hands every open of a database a process-unique identity. Monotonic and
/// never reused, so no two live handles — and no handle and its own
/// predecessor at the same path — can be confused for one another.
fn next_instance() -> u64 {
    static INSTANCES: AtomicU64 = AtomicU64::new(0);
    INSTANCES.fetch_add(1, Ordering::Relaxed)
}

impl LocalDb {
    pub async fn open(path: impl AsRef<Path>) -> DbResult<Self> {
        Self::open_with_retry(path, RetryConfig::default()).await
    }

    pub async fn open_with_retry(path: impl AsRef<Path>, retry: RetryConfig) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        require_db_path(&path)?;
        let path_string = path.to_string_lossy().to_string();
        let database = Arc::new(DbHandle::Local(
            Builder::new_local(&path_string).build().await?,
        ));
        let gate = Arc::new(ConnectionGate::new());
        let db = Self {
            path,
            database,
            retry,
            commit_signal: Arc::new(Notify::new()),
            provider_admission_changes: watch::channel(0).0,
            instance: next_instance(),
            mutation_generation: AtomicU64::new(0),
            team: None,
            team_content_store: None,
            private_content_store: OnceLock::new(),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            checkpoint_in_engine: Arc::new(AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            write_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
            read_bound: READ_STATEMENT_BOUND,
            #[cfg(any(test, feature = "test-utils"))]
            history_source_override: Mutex::new(None),
            history_catalog_present: AtomicBool::new(false),
            history_evicted: AtomicBool::new(false),
        };
        db.configure().await?;
        db.initialize_history_capability().await?;
        Ok(db)
    }

    /// Open a Turso Sync replica at `path`, reconciling against the sync server
    /// at `remote_url`. `auth_token` is `None` for an unauthenticated local sync
    /// server (`tursodb --sync-server`) and `Some(token)` for a hosted endpoint.
    ///
    /// An empty replica bootstraps its schema and data from the server on open
    /// (`bootstrap_if_empty` defaults to `true`); a replica that already holds a
    /// schema opens as-is and converges on the next `pull()`.
    pub async fn open_synced(
        path: impl AsRef<Path>,
        remote_url: impl Into<String>,
        auth_token: Option<String>,
    ) -> DbResult<Self> {
        Self::open_synced_with_retry(path, remote_url, auth_token, RetryConfig::default()).await
    }

    pub async fn open_synced_with_retry(
        path: impl AsRef<Path>,
        remote_url: impl Into<String>,
        auth_token: Option<String>,
        retry: RetryConfig,
    ) -> DbResult<Self> {
        // Belt-and-suspenders: the orchestrator installs the rustls crypto
        // provider at startup, but guard direct-`LocalDb` callers (tests) here
        // too, before `build()` spawns the `turso-sync-io` thread and it builds
        // its TLS stack (see `install_crypto_provider`).
        install_crypto_provider();
        let path = path.as_ref().to_path_buf();
        let path_string = path.to_string_lossy().to_string();
        let mut builder =
            turso::sync::Builder::new_remote(&path_string).with_remote_url(remote_url.into());
        if let Some(token) = auth_token {
            builder = builder.with_auth_token(token);
        }
        let database = Arc::new(DbHandle::Synced(builder.build().await?));
        let gate = Arc::new(ConnectionGate::new());
        let db = Self {
            path,
            database,
            retry,
            commit_signal: Arc::new(Notify::new()),
            provider_admission_changes: watch::channel(0).0,
            instance: next_instance(),
            mutation_generation: AtomicU64::new(0),
            team: None,
            team_content_store: None,
            private_content_store: OnceLock::new(),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            checkpoint_in_engine: Arc::new(AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            write_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
            read_bound: READ_STATEMENT_BOUND,
            #[cfg(any(test, feature = "test-utils"))]
            history_source_override: Mutex::new(None),
            history_catalog_present: AtomicBool::new(false),
            history_evicted: AtomicBool::new(false),
        };
        db.configure().await?;
        db.initialize_history_capability().await?;
        Ok(db)
    }

    /// Open a Turso Sync replica whose auth token is produced on demand by
    /// `token_fn`, which the sync client invokes before every HTTP request. This
    /// is the ROTATING-token path: the closure can return a freshly minted token
    /// each call (e.g. via a rotating team-sync token minter),
    /// so a short-lived token is refreshed transparently without reopening the
    /// replica. A closure error fails the in-flight sync op (the caller's backoff
    /// retries it). The static-token / unauthenticated path is [`Self::open_synced`].
    pub async fn open_synced_with_token_fn<F, Fut>(
        path: impl AsRef<Path>,
        remote_url: impl Into<String>,
        token_fn: F,
    ) -> DbResult<Self>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = turso::Result<String>> + Send + 'static,
    {
        // Belt-and-suspenders: the orchestrator installs the rustls crypto
        // provider at startup, but guard direct-`LocalDb` callers (tests) here
        // too, before `build()` spawns the `turso-sync-io` thread and it builds
        // its TLS stack (see `install_crypto_provider`).
        install_crypto_provider();
        let path = path.as_ref().to_path_buf();
        let path_string = path.to_string_lossy().to_string();
        let database = Arc::new(DbHandle::Synced(
            turso::sync::Builder::new_remote(&path_string)
                .with_remote_url(remote_url.into())
                .with_auth_token_fn(token_fn)
                .build()
                .await?,
        ));
        let gate = Arc::new(ConnectionGate::new());
        let db = Self {
            path,
            database,
            retry: RetryConfig::default(),
            commit_signal: Arc::new(Notify::new()),
            provider_admission_changes: watch::channel(0).0,
            instance: next_instance(),
            mutation_generation: AtomicU64::new(0),
            team: None,
            team_content_store: None,
            private_content_store: OnceLock::new(),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            checkpoint_in_engine: Arc::new(AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            write_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
            read_bound: READ_STATEMENT_BOUND,
            #[cfg(any(test, feature = "test-utils"))]
            history_source_override: Mutex::new(None),
            history_catalog_present: AtomicBool::new(false),
            history_evicted: AtomicBool::new(false),
        };
        db.configure().await?;
        db.initialize_history_capability().await?;
        Ok(db)
    }

    /// Whether this handle is backed by a Turso Sync replica (vs a local file).
    pub fn is_synced(&self) -> bool {
        matches!(self.database.as_ref(), DbHandle::Synced(_))
    }

    /// The team id this handle belongs to, or `None` for the private DB. A team
    /// replica carries its own scope (set at open), so callers detect a team run
    /// from the resolved handle itself — independent of HOW it was resolved.
    pub fn team_id(&self) -> Option<&TeamId> {
        self.team.as_ref().map(|ctx| &ctx.team_id)
    }

    /// A team replica's brokered content store; `None` on the private database.
    /// Attachment bytes go through [`Self::put_content`] and
    /// [`Self::get_content`], which choose between this and `cas_cache`; the
    /// team-only paths (sealed-commit cloud coverage) hand it onward. Archival
    /// and reconstruction use [`Self::content_store`].
    pub fn team_content_store(&self) -> Option<&Arc<dyn ContentStore>> {
        self.team_content_store.as_ref()
    }

    /// The store this database's archived packs and segment blobs live in: the
    /// team store on a replica, the installed file store on the private
    /// database. `None` only on a private handle nothing installed a store on.
    pub fn content_store(&self) -> Option<&Arc<dyn ContentStore>> {
        self.team_content_store
            .as_ref()
            .or_else(|| self.private_content_store.get())
    }

    /// Install the private database's archival store. Set once: a second
    /// install is refused and returns the store it would have replaced.
    pub fn set_private_content_store(
        &self,
        store: Arc<dyn ContentStore>,
    ) -> Result<(), Arc<dyn ContentStore>> {
        self.private_content_store.set(store)
    }

    /// Whether any history has been evicted. Loaded at initialization, never
    /// queried by readers. False includes storeless team replicas.
    pub fn has_evicted_history(&self) -> bool {
        self.history_evicted.load(Ordering::Acquire)
    }

    pub(crate) fn has_history_catalog(&self) -> bool {
        self.history_catalog_present.load(Ordering::Acquire)
    }

    /// Raise the monotonic eviction fact before the first eviction transaction
    /// can commit. A failed transaction may leave a conservative true value.
    /// Eviction must also quiesce or revalidate in-flight cached-Hot readers
    /// across source selection and body fetch; this flag alone is not a lock.
    /// Clearing the fact requires reopening the database.
    pub fn mark_history_evicted(&self) {
        self.history_evicted.store(true, Ordering::Release);
    }

    /// Initialization only. A fresh database acquires its capability after
    /// migrations; an existing private database loads its eviction fact once at
    /// open. Content-store availability cannot identify a storeless team schema.
    pub(crate) async fn initialize_history_capability(&self) -> DbResult<()> {
        if self.has_history_catalog() {
            return Ok(());
        }
        let present = self.query_one(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'history_tier')",
            (),
            |row| Ok(row.get::<i64>(0)? != 0),
        ).await?;
        if present {
            let evicted = self
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM history_tier WHERE evicted_at IS NOT NULL)",
                    (),
                    |row| Ok(row.get::<i64>(0)? != 0),
                )
                .await?;
            if evicted {
                self.mark_history_evicted();
            }
            self.history_catalog_present.store(true, Ordering::Release);
        }
        Ok(())
    }

    /// Force the history read seam's source on this handle, or clear it.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_history_source_override(
        &self,
        source: Option<crate::storage::events::history::HistorySource>,
    ) {
        *self
            .history_source_override
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = source;
    }

    /// The source forced by [`Self::set_history_source_override`], if any.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn history_source_override(
        &self,
    ) -> Option<crate::storage::events::history::HistorySource> {
        *self
            .history_source_override
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Whether a checkpoint attempt is still running inside the engine. A
    /// maintenance pass that folds when due skips its fold while one is, rather
    /// than waiting on it.
    pub fn checkpoint_in_engine(&self) -> bool {
        self.checkpoint_in_engine.load(Ordering::SeqCst)
    }

    /// The private database that owns machine-local route metadata for this team
    /// replica, when available.
    pub fn private_route_db(&self) -> Option<&Arc<LocalDb>> {
        self.team.as_ref().and_then(|ctx| ctx.private_db.as_ref())
    }

    /// Attach a team replica's identity + content store. Called by `open_team`
    /// after construction (and by tests that inject a fake store) before the
    /// handle is shared behind an `Arc`.
    pub fn set_team_context(&mut self, ctx: TeamReplicaContext, store: Arc<dyn ContentStore>) {
        self.team = Some(Arc::new(ctx));
        self.team_content_store = Some(store);
    }

    /// The commit signal fired after each successful synced-replica transaction.
    /// The per-team push task in `storage::team_sync` waits on this to coalesce a
    /// write burst into one prompt push.
    /// Subscribe to admission-changing commits. Lease heartbeats deliberately
    /// do not advance this signal.
    pub fn subscribe_provider_admission_changes(&self) -> watch::Receiver<u64> {
        self.provider_admission_changes.subscribe()
    }

    pub fn provider_admission_changed(&self) {
        self.provider_admission_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    pub fn commit_signal(&self) -> Arc<Notify> {
        self.commit_signal.clone()
    }

    /// How many connections the gate currently counts as live. Tests use this to
    /// assert the invariant everything else rests on: that every way a
    /// connection's life can end settles the count back to zero. A registration
    /// leaked on any of those paths would wedge the drain shut permanently, and
    /// the symptom — checkpointing quietly stops working — is invisible until
    /// the log has grown for a day.
    ///
    /// Public so readers in other crates can assert the property the checkpoint
    /// depends on: that a long read holds no connection between its pages.
    pub fn live_connections(&self) -> usize {
        self.gate.lock().live.len()
    }

    /// Connections resting on the free-list, for the pool probes.
    #[cfg(test)]
    pub(crate) fn idle_len(&self) -> usize {
        self.idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// A handle whose pooled reads are bounded at `bound` instead of
    /// [`READ_STATEMENT_BOUND`], so tests can exercise the bound in
    /// milliseconds.
    #[cfg(test)]
    pub(crate) fn with_read_bound(mut self, bound: Duration) -> Self {
        self.read_bound = bound;
        self
    }

    /// Every connection held longer than `threshold`, each reported once and
    /// then again only every `rewarn` while it is still held.
    ///
    /// The same sweep is the last line of the pooled-read bound: a pooled read
    /// still held [`READ_BACKSTOP_SLACK`] past the bound has outlived both the
    /// engine deadline and the outer timeout, so it is interrupted here and
    /// the interrupt is logged, because reaching it means one of those failed.
    pub fn overdue_holders(&self, threshold: Duration, rewarn: Duration) -> Vec<OverdueHolder> {
        let now = Instant::now();
        let backstop = self.read_bound + READ_BACKSTOP_SLACK;
        let mut overdue = Vec::new();
        let mut runaway = Vec::new();
        {
            let mut state = self.gate.lock();
            for live in state.live.values_mut() {
                let age = now.saturating_duration_since(live.since);
                if age < threshold {
                    continue;
                }
                // Pooled reads only. A `connect` holder could be interrupted
                // only while a statement runs, and one parked in a transaction
                // is unaffected by an interrupt; it is named, not cut.
                if live.kind == HolderKind::PooledRead && age >= backstop && !live.interrupted {
                    live.interrupted = true;
                    runaway.push((live.caller, age, live.holder.clone()));
                }
                if live
                    .last_warned
                    .is_some_and(|at| now.saturating_duration_since(at) < rewarn)
                {
                    continue;
                }
                live.last_warned = Some(now);
                overdue.push(OverdueHolder {
                    caller: live.caller,
                    kind: live.kind,
                    age,
                    statement: live.holder.statement(),
                    activity: live.holder.activity(),
                });
            }
        }
        for (caller, age, holder) in runaway {
            let executing = holder.activity().executing;
            let outcome = if holder.interrupt() {
                format!(
                    "interrupt requested (tracked statement in flight: {})",
                    if executing { "yes" } else { "no" }
                )
            } else {
                "had no handle".to_owned()
            };
            log::warn!(
                "pooled read at {} outlived the {:?} read bound and its timeout ({:?} held); \
                 gate sweep {outcome}: {}",
                Site(caller),
                self.read_bound,
                age,
                holder.statement(),
            );
        }
        overdue
    }

    /// Current in-process mutation generation. Reading it never opens a database
    /// connection, so generation-driven consumers settle to atomic loads at idle.
    ///
    /// It is meaningful only alongside [`Self::instance`]: a handle reopened at
    /// the same path starts again at zero.
    pub fn mutation_generation(&self) -> u64 {
        self.mutation_generation.load(Ordering::Relaxed)
    }

    /// This open's process-unique identity. See the field's documentation for
    /// why a cache key needs it as well as the generation.
    pub fn instance(&self) -> u64 {
        self.instance
    }

    fn record_mutation(&self) {
        self.mutation_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// The BEGIN statement for concurrent read/write transactions. A local
    /// (MVCC) database uses `BEGIN CONCURRENT` for optimistic concurrency; the
    /// synced engine captures changes via CDC, which is incompatible with MVCC,
    /// so it uses a plain `BEGIN` (writers serialize and retry on Busy instead).
    pub fn concurrent_begin(&self) -> &'static str {
        match self.database.as_ref() {
            DbHandle::Local(_) => "BEGIN CONCURRENT",
            DbHandle::Synced(_) => "BEGIN",
        }
    }

    /// Push local changes to the sync server. Errors on a local (non-synced)
    /// database rather than silently no-opping, so a routing bug surfaces loudly.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Internal` when called on a local database, or a Turso
    /// error when the push fails.
    pub async fn push(&self) -> DbResult<()> {
        match self.database.as_ref() {
            DbHandle::Synced(db) => Ok(db.push().await?),
            DbHandle::Local(_) => Err(DbError::internal(
                "push() called on a local (non-synced) database",
            )),
        }
    }

    /// Pull remote changes from the sync server, returning `true` when any were
    /// applied. Errors on a local (non-synced) database rather than no-opping.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Internal` when called on a local database, or a Turso
    /// error when the pull fails.
    pub async fn pull(&self) -> DbResult<bool> {
        match self.database.as_ref() {
            DbHandle::Synced(db) => {
                let changed = db.pull().await?;
                if changed {
                    self.record_mutation();
                    // Pulled pages can carry another replica's migrations.
                    self.gate.plan_audit.schema_changed();
                }
                Ok(changed)
            }
            DbHandle::Local(_) => Err(DbError::internal(
                "pull() called on a local (non-synced) database",
            )),
        }
    }

    /// This database's plan audit, for the migration runner to say the schema
    /// moved.
    pub(crate) fn plan_audit(&self) -> &PlanAudit {
        &self.gate.plan_audit
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open a brand-new connection, outside the free-list.
    ///
    /// The escape hatch for writers that need a connection of their own for
    /// longer than one pooled transaction (`MigrationRunner::run_fk_off`, which
    /// toggles `PRAGMA foreign_keys` around a transaction and must not have that
    /// pragma outlive its own use), and for probes.
    /// Never for a read: a reader goes through [`Self::read`] or the `query_*`
    /// helpers, which bound the statement and cut a read that parks. Nothing
    /// can cut a `connect` holder parked in an open transaction: the engine's
    /// interrupt is a no-op with no statement running, so it lasts until its
    /// owner drops the connection.
    ///
    /// Out of the pool or not, the connection is registered with the
    /// [`ConnectionGate`] for as long as it lives, which is what makes the gate
    /// a statement about the whole process rather than about the pool. The
    /// gate also keeps a handle on it, so the watch can report whether a
    /// holder is executing or parked in a transaction.
    #[track_caller]
    pub fn connect(&self) -> DbCall<'_, TrackedConnection> {
        let caller = Location::caller();
        let holder = Holder::new(caller, None);
        DbCall::new(holder.clone(), move || async move {
            let slot = self
                .gate
                .admit(caller, HolderKind::Connect, holder.clone())
                .await;
            let conn = self.open_connection().await?;
            holder.arm(conn.clone());
            Ok(TrackedConnection::new(conn, slot))
        })
    }

    /// Open a connection without registering it with the gate.
    ///
    /// Two callers only: the gate-aware entry points above, and
    /// [`Self::checkpoint`], whose own connection must be invisible to the drain
    /// it is waiting on — a registered one would count itself and the drain
    /// could never reach zero.
    async fn open_connection(&self) -> DbResult<Connection> {
        #[cfg(test)]
        self.connections_created.fetch_add(1, Ordering::Relaxed);
        let conn = match self.database.as_ref() {
            DbHandle::Local(db) => db.connect()?,
            DbHandle::Synced(db) => db.connect().await?,
        };
        conn.busy_timeout(self.retry.busy_timeout)?;
        // Set once, at creation: a pooled connection keeps its pragmas across
        // checkouts, so re-issuing these per checkout would be pure round-trip.
        conn.execute("PRAGMA foreign_keys = ON", ()).await?;
        conn.execute(&format!("PRAGMA cache_size = {PAGE_CACHE_LIMIT}"), ())
            .await?;
        Ok(conn)
    }

    /// Take a connection to run one transaction on: an idle one when the
    /// free-list has any, a fresh one otherwise.
    ///
    /// Deliberately never waits on an empty free-list. Creating on demand is
    /// what makes the pool structurally deadlock-free: a call path that nested
    /// one database call inside another's transaction closure would get its own
    /// connection rather than block forever on one its own caller is holding.
    /// Concurrency is therefore unbounded exactly as it was before pooling;
    /// [`MAX_IDLE_CONNECTIONS`] bounds only what is *retained* when idle.
    ///
    /// It does wait at the [`ConnectionGate`], which is a different thing and
    /// does not reintroduce that hazard. The gate is shut only while maintenance
    /// folds the log, it reopens on every exit path including panic, and the
    /// drain behind it is bounded by a budget. A nested call arriving while the
    /// gate is shut costs its caller a bounded pause and denies maintenance the
    /// quiet instant it was after; it cannot wait on its own caller forever.
    /// The pause is the drain plus the pass's losing attempts, each bounded by
    /// [`CHECKPOINT_BUSY_TIMEOUT`], because the outer call's open transaction
    /// makes the engine report busy rather than fold.
    ///
    /// A pooled read's connection gets the read bound as its engine deadline
    /// and a write's gets none, set on every checkout because pooled
    /// connections alternate between the two. Both are atomic stores.
    async fn checkout(
        &self,
        caller: Caller,
        kind: HolderKind,
        holder: &Holder,
    ) -> DbResult<TrackedConnection> {
        let slot = self.gate.admit(caller, kind, holder.clone()).await;
        let pooled = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop();
        let conn = match pooled {
            Some(conn) => conn,
            None => self.open_connection().await?,
        };
        if kind == HolderKind::PooledRead {
            #[cfg(any(test, feature = "test-utils"))]
            self.gate
                .read_checkout_count
                .fetch_add(1, Ordering::Relaxed);
            conn.set_query_timeout(self.read_bound)?;
            holder.arm(conn.clone());
        } else {
            conn.set_query_timeout(Duration::ZERO)?;
        }
        Ok(TrackedConnection::new(conn, slot))
    }

    /// How long a whole pooled read may stay open before it is dropped.
    ///
    /// The engine deadline ends a statement that steps past the bound; this
    /// ends a read that parks between statements, which no per-statement
    /// deadline sees. Dropping the read on expiry drops its connection
    /// unreleased, and the engine rolls its transaction back.
    fn parked_read_limit(&self) -> Duration {
        self.read_bound + PARKED_READ_SLACK
    }

    /// A pooled read's result from its run under [`Self::parked_read_limit`],
    /// with either way of exceeding the bound turned into
    /// [`DbError::ReadBoundExceeded`] and logged.
    fn bounded_read_result<T>(
        &self,
        caller: Caller,
        holder: &Holder,
        started: Instant,
        outcome: Result<DbResult<T>, Elapsed>,
    ) -> DbResult<T> {
        match outcome {
            Ok(Err(error)) if error.is_interrupt() => {}
            Ok(result) => return result,
            Err(_elapsed) => {}
        }
        let error = DbError::ReadBoundExceeded {
            caller,
            bound: self.read_bound,
            elapsed: started.elapsed(),
            statement: holder.statement(),
        };
        log::warn!("{error}");
        Err(error)
    }

    /// Return a connection that provably holds no open transaction.
    ///
    /// Every caller passes only connections whose transaction committed or whose
    /// ROLLBACK succeeded; anything else is dropped instead, so a connection in
    /// unknown state can never be handed to the next caller's BEGIN.
    ///
    /// Taking the whole [`TrackedConnection`] rather than the bare connection is
    /// what keeps the gate's count honest on both exits. A count keyed on this
    /// method would leak on the retire path — the connection whose ROLLBACK
    /// failed never arrives here (see [`TxAttempt`]) — and one leaked
    /// registration wedges the gate's drain shut for the life of the process.
    /// Keyed on the slot's `Drop` instead, release and retire both settle it.
    fn release(&self, conn: TrackedConnection) {
        let conn = conn.into_connection();
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if idle.len() < MAX_IDLE_CONNECTIONS {
            idle.push(conn);
        }
    }

    /// Read transactions opened over this handle's life. See the field.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn read_transaction_count(&self) -> usize {
        self.read_transaction_count.load(Ordering::Relaxed)
    }

    /// Application statement submissions through tracked connections. Counts
    /// query, execute and prepare once, and execute_batch once per script.
    /// Excludes pool setup/transaction SQL and plan-audit EXPLAIN. Prepared
    /// executions through the returned engine Statement are not counted again.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn statement_count(&self) -> usize {
        self.gate.statement_count.load(Ordering::Relaxed)
    }

    /// All pooled read checkouts, including the autocommit query helpers.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn read_checkout_count(&self) -> usize {
        self.gate.read_checkout_count.load(Ordering::Relaxed)
    }

    /// Query/prepare submissions, with the same exclusions as statement_count.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn read_statement_count(&self) -> usize {
        self.gate.read_statement_count.load(Ordering::Relaxed)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn write_transaction_count(&self) -> usize {
        self.write_transaction_count.load(Ordering::Relaxed)
    }

    /// Run `f` in one read transaction on a pooled connection.
    ///
    /// Bounded: a statement that runs past [`READ_STATEMENT_BOUND`], or a read
    /// that is still open shortly after it, fails with
    /// [`DbError::ReadBoundExceeded`] and its connection is retired.
    #[track_caller]
    pub fn read<'s, T: Send + 's>(
        &'s self,
        f: impl for<'a> FnOnce(&'a TrackedConnection) -> BoxFuture<'a, DbResult<T>> + Send + 's,
    ) -> DbCall<'s, T> {
        let caller = Location::caller();
        let holder = Holder::new(caller, None);
        DbCall::new(holder.clone(), move || async move {
            #[cfg(any(test, feature = "test-utils"))]
            self.read_transaction_count.fetch_add(1, Ordering::Relaxed);
            let started = Instant::now();
            let outcome = timeout(self.parked_read_limit(), async {
                let conn = self
                    .checkout(caller, HolderKind::PooledRead, &holder)
                    .await?;
                let attempt = run_read_tx(&conn, self.concurrent_begin(), f).await;
                // An interrupted connection is retired even when its ROLLBACK
                // succeeded: what it was doing when the engine stopped it is
                // not something the next caller should inherit.
                let interrupted = attempt.result.as_ref().is_err_and(DbError::is_interrupt);
                if attempt.reusable && !interrupted {
                    self.release(conn);
                }
                attempt.result
            })
            .await;
            self.bounded_read_result(caller, &holder, started, outcome)
        })
    }

    #[track_caller]
    pub fn write<'s, T: Send + 's>(
        &'s self,
        f: impl for<'a> FnMut(&'a TrackedConnection) -> BoxFuture<'a, DbResult<T>> + Send + 's,
    ) -> DbCall<'s, T> {
        let caller = Location::caller();
        let holder = Holder::new(caller, None);
        DbCall::new(holder.clone(), move || async move {
            #[cfg(any(test, feature = "test-utils"))]
            self.write_transaction_count.fetch_add(1, Ordering::Relaxed);
            self.transaction_with_begin(caller, &holder, self.concurrent_begin(), f)
                .await
        })
    }

    #[track_caller]
    pub fn exclusive<'s, T: Send + 's>(
        &'s self,
        f: impl for<'a> FnMut(&'a TrackedConnection) -> BoxFuture<'a, DbResult<T>> + Send + 's,
    ) -> DbCall<'s, T> {
        let caller = Location::caller();
        let holder = Holder::new(caller, None);
        DbCall::new(holder.clone(), move || async move {
            self.transaction_with_begin(caller, &holder, "BEGIN", f)
                .await
        })
    }

    /// Runs one SELECT and collects every mapped row.
    ///
    /// A single SQL statement already observes one database snapshot. Avoiding an
    /// explicit BEGIN/COMMIT here removes two engine round-trips from the hottest
    /// read path. Call Self::read when several statements must share a snapshot.
    /// Bounded like [`Self::read`].
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching rows, or mapping each row.
    #[track_caller]
    pub fn query_all<T, F>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
        map: F,
    ) -> DbCall<'_, Vec<T>>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let caller = Location::caller();
        let sql = sql.into();
        let holder = Holder::new(caller, Some(&sql));
        DbCall::new(holder.clone(), move || async move {
            let started = Instant::now();
            let outcome = timeout(self.parked_read_limit(), async {
                let conn = self
                    .checkout(caller, HolderKind::PooledRead, &holder)
                    .await?;
                // `move`, so the body owns `map` rather than holding a borrow
                // of it across an await, which would demand `F: Sync`.
                let (conn_ref, sql) = (&conn, &sql);
                let result: DbResult<Vec<T>> = async move {
                    if let Some(audit) = conn_ref.plan_audit(caller, sql) {
                        audit.await;
                    }
                    #[cfg(any(test, feature = "test-utils"))]
                    {
                        self.gate.statement_count.fetch_add(1, Ordering::Relaxed);
                        self.gate
                            .read_statement_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let mut rows = conn_ref.conn.query(sql, params).await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push(map(&row)?);
                    }
                    Ok(out)
                }
                .await;
                // Returned only on success: a query abandoned part-way through
                // its rows leaves a statement whose state this layer has no way
                // to assert on.
                if result.is_ok() {
                    self.release(conn);
                }
                result
            })
            .await;
            self.bounded_read_result(caller, &holder, started, outcome)
        })
    }

    /// Runs one SELECT and maps the first row, if present.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or mapping the row.
    #[track_caller]
    pub fn query_opt<T, F>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
        map: F,
    ) -> DbCall<'_, Option<T>>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.select_first(Location::caller(), sql.into(), params, map, Ok)
    }

    /// The one SELECT behind [`Self::query_opt`] and the helpers built on it:
    /// map the first row, if there is one, then `finish` that.
    ///
    /// `finish` runs inside the call's own body, so a helper adds no future
    /// of its own around the call.
    fn select_first<T, U, F>(
        &self,
        caller: Caller,
        sql: String,
        params: impl IntoParams + Send + 'static,
        map: F,
        finish: fn(Option<T>) -> DbResult<U>,
    ) -> DbCall<'_, U>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
    {
        let holder = Holder::new(caller, Some(&sql));
        DbCall::new(holder.clone(), move || async move {
            let started = Instant::now();
            let outcome = timeout(self.parked_read_limit(), async {
                let conn = self
                    .checkout(caller, HolderKind::PooledRead, &holder)
                    .await?;
                // `move` for the same reason as in `query_all`.
                let (conn_ref, sql) = (&conn, &sql);
                let result: DbResult<Option<T>> = async move {
                    if let Some(audit) = conn_ref.plan_audit(caller, sql) {
                        audit.await;
                    }
                    #[cfg(any(test, feature = "test-utils"))]
                    {
                        self.gate.statement_count.fetch_add(1, Ordering::Relaxed);
                        self.gate
                            .read_statement_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let mut rows = conn_ref.conn.query(sql, params).await?;
                    rows.next().await?.map(|row| map(&row)).transpose()
                }
                .await;
                if result.is_ok() {
                    self.release(conn);
                }
                result
            })
            .await;
            self.bounded_read_result(caller, &holder, started, outcome)
                .and_then(finish)
        })
    }

    /// Runs one SELECT and returns the first column of the
    /// first row as optional text.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or reading column 0.
    #[track_caller]
    pub fn query_opt_text(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
    ) -> DbCall<'_, Option<String>> {
        self.select_first(
            Location::caller(),
            sql.into(),
            params,
            |row| row.opt_text(0),
            |value| Ok(value.flatten()),
        )
    }

    /// Runs one SELECT and returns the first column of the
    /// first row as optional integer.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or reading column 0.
    #[track_caller]
    pub fn query_opt_i64(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
    ) -> DbCall<'_, Option<i64>> {
        self.select_first(
            Location::caller(),
            sql.into(),
            params,
            |row| row.opt_i64(0),
            |value| Ok(value.flatten()),
        )
    }

    /// Runs one SELECT and returns the first column of the
    /// first row as text, or `None` when the query returns no row.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or reading column 0.
    #[track_caller]
    pub fn query_text(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
    ) -> DbCall<'_, Option<String>> {
        self.select_first(
            Location::caller(),
            sql.into(),
            params,
            |row| row.text(0),
            Ok,
        )
    }

    /// Runs one SELECT and requires one row.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or mapping the row. Returns `DbError::Row` when the
    /// query returns no rows.
    #[track_caller]
    pub fn query_one<T, F>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
        map: F,
    ) -> DbCall<'_, T>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.select_first(Location::caller(), sql.into(), params, map, |row| {
            row.ok_or_else(|| DbError::Row("query_one returned no rows".to_string()))
        })
    }

    /// Runs one statement through the retrying write transaction path.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the write transaction, executing the
    /// statement, committing the transaction, or exhausting retry attempts.
    #[track_caller]
    pub fn execute(&self, sql: impl Into<String>, params: impl IntoParams) -> DbCall<'_, u64> {
        let caller = Location::caller();
        let sql = sql.into();
        let holder = Holder::new(caller, Some(&sql));
        let params = params.into_params();
        DbCall::new(holder.clone(), move || async move {
            let params = params?;
            #[cfg(any(test, feature = "test-utils"))]
            self.write_transaction_count.fetch_add(1, Ordering::Relaxed);
            self.transaction_with_begin(caller, &holder, self.concurrent_begin(), move |conn| {
                let sql = sql.clone();
                let params = params.clone();
                Box::pin(async move {
                    if let Some(audit) = conn.plan_audit(caller, &sql) {
                        audit.await;
                    }
                    #[cfg(any(test, feature = "test-utils"))]
                    conn.slot
                        .gate
                        .statement_count
                        .fetch_add(1, Ordering::Relaxed);
                    Ok(conn.conn.execute(&sql, params).await?)
                })
            })
            .await
        })
    }

    /// Runs a semicolon-delimited SQL script through the retrying write
    /// transaction path.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the write transaction, executing the
    /// script, committing the transaction, or exhausting retry attempts.
    #[track_caller]
    pub fn execute_script(&self, sql: impl Into<String>) -> DbCall<'_, ()> {
        let caller = Location::caller();
        let sql = sql.into();
        let holder = Holder::new(caller, Some(&sql));
        DbCall::new(holder.clone(), move || async move {
            #[cfg(any(test, feature = "test-utils"))]
            self.write_transaction_count.fetch_add(1, Ordering::Relaxed);
            let schema_change = plan_audit::is_schema_change(&sql);
            self.transaction_with_begin(caller, &holder, self.concurrent_begin(), move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    #[cfg(any(test, feature = "test-utils"))]
                    conn.slot
                        .gate
                        .statement_count
                        .fetch_add(1, Ordering::Relaxed);
                    conn.conn.execute_batch(&sql).await?;
                    Ok(())
                })
            })
            .await?;
            if schema_change {
                self.gate.plan_audit.schema_changed();
            }
            Ok(())
        })
    }

    async fn transaction_with_begin<T>(
        &self,
        caller: Caller,
        holder: &Holder,
        begin_sql: &str,
        mut f: impl for<'a> FnMut(&'a TrackedConnection) -> BoxFuture<'a, DbResult<T>>,
    ) -> DbResult<T> {
        let started_at = Instant::now();
        let mut backoff = self.retry.initial_backoff;
        let mut last_retryable = None;
        let mut backoff_slept = Duration::ZERO;

        for attempt in 1..=self.retry.max_attempts {
            let conn = self
                .checkout(caller, HolderKind::PooledWrite, holder)
                .await?;
            let outcome = run_tx(&conn, begin_sql, &mut f).await;
            // Released between attempts as well as after the last one, so a
            // contended write that retries several times still reuses one warm
            // connection instead of creating one per attempt.
            if outcome.reusable {
                self.release(conn);
            }
            match outcome.result {
                Ok(value) => {
                    emit_transaction_contention(
                        attempt,
                        backoff_slept,
                        started_at.elapsed(),
                        begin_sql,
                    );
                    // Signal the push task that a synced replica committed. Gated
                    // on `is_synced()` so a local database stays zero-cost (the
                    // Notify is allocated but never fired). This is the ONLY fire
                    // site for `commit_signal`: `pull()` applies remote pages via
                    // physical WAL replay OUTSIDE `transaction_with_begin`, so an
                    // applied pull fires no commit signal — there is no
                    // push<->pull feedback loop.
                    self.record_mutation();
                    if self.is_synced() {
                        self.commit_signal.notify_one();
                    }
                    return Ok(value);
                }
                Err(error) if error.is_retryable() && attempt < self.retry.max_attempts => {
                    last_retryable = Some(error);
                    let jitter = Duration::from_millis(rand::random::<u64>() % 5);
                    let sleep_started = Instant::now();
                    sleep(backoff + jitter).await;
                    backoff_slept += sleep_started.elapsed();
                    backoff = (backoff * 2).min(self.retry.max_backoff);
                }
                Err(error) if error.is_retryable() => {
                    emit_transaction_contention(
                        attempt,
                        backoff_slept,
                        started_at.elapsed(),
                        begin_sql,
                    );
                    return Err(DbError::RetryExhausted {
                        attempts: attempt,
                        elapsed: started_at.elapsed(),
                        source: Box::new(error),
                    });
                }
                Err(error) => return Err(error),
            }
        }

        Err(DbError::RetryExhausted {
            attempts: self.retry.max_attempts,
            elapsed: started_at.elapsed(),
            source: Box::new(
                last_retryable.unwrap_or_else(|| DbError::internal("transaction retry exhausted")),
            ),
        })
    }

    #[track_caller]
    pub fn execute_batch<'s>(&'s self, sql: &'s str) -> DbCall<'s, ()> {
        let caller = Location::caller();
        let holder = Holder::new(caller, Some(sql));
        DbCall::new(holder.clone(), move || async move {
            let conn = self
                .checkout(caller, HolderKind::PooledWrite, &holder)
                .await?;
            #[cfg(any(test, feature = "test-utils"))]
            self.gate.statement_count.fetch_add(1, Ordering::Relaxed);
            let result = conn.conn.execute_batch(sql).await;
            if result.is_ok() {
                self.release(conn);
                self.record_mutation();
                if plan_audit::is_schema_change(sql) {
                    self.gate.plan_audit.schema_changed();
                }
            }
            Ok(result?)
        })
    }

    /// Run one statement on a pooled connection and drain its rows: PRAGMAs,
    /// some of which write, which is why it is admitted as a write and never
    /// bounded.
    #[track_caller]
    pub fn consume_query<'s>(&'s self, sql: &'s str) -> DbCall<'s, ()> {
        let caller = Location::caller();
        let holder = Holder::new(caller, Some(sql));
        DbCall::new(holder.clone(), move || async move {
            let conn = self
                .checkout(caller, HolderKind::PooledWrite, &holder)
                .await?;
            if let Some(audit) = conn.plan_audit(caller, sql) {
                audit.await;
            }
            let result = consume_on(&conn.conn, sql).await;
            if result.is_ok() {
                self.release(conn);
            }
            result
        })
    }

    /// Size in bytes of this database's logical log (`-log`) sidecar, or zero
    /// when it has none (an in-memory database, or a synced replica, which does
    /// not journal through MVCC).
    ///
    /// This is the quantity a checkpoint folds back into the main database, and
    /// therefore the one number that says whether checkpointing is keeping up.
    /// It is read from the filesystem rather than the engine because it is the
    /// same figure an operator sees in `~/.cairn`.
    pub fn logical_log_bytes(&self) -> u64 {
        std::fs::metadata(logical_log_path(&self.path))
            .map(|meta| meta.len())
            .unwrap_or(0)
    }

    /// Whether a TRUNCATE attempt is still running in the engine, past the
    /// pass that started it. Optional work that would add reads to a database
    /// in that state (the daily storage census) waits for it to clear.
    pub fn checkpoint_in_flight(&self) -> bool {
        self.checkpoint_in_engine.load(Ordering::SeqCst)
    }

    /// Fold the logical log back into the main database, creating the quiet
    /// instant it needs rather than waiting to be handed one.
    ///
    /// Cairn owns checkpointing because the engine's own commit-path attempt
    /// cannot win; see [`CHECKPOINT_THRESHOLD_DISABLED`]. A TRUNCATE checkpoint
    /// succeeds only in an instant when no MVCC transaction is open anywhere in
    /// the process, and on a machine serving a UI that polls it, no such instant
    /// occurs. That is measured, not assumed: ten consecutive production passes
    /// spent 6,651 seconds attempting and never once observed one (CAIRN-4167).
    /// Retrying does not help, however long the window — there is nothing to
    /// catch.
    ///
    /// So this closes the [`ConnectionGate`] first, holding new connections at
    /// the door while the ones already open finish, and only then attempts. The
    /// hold reopens the gate when it drops, on every path out of this function
    /// including a panic inside the engine. A caller waiting at the gate waits
    /// for the drain and then for the fold, however long the fold takes, up to
    /// [`CHECKPOINT_WEDGE_BACKSTOP`]: no caller is ever handed `database is
    /// locked` because a fold is running. How long the drain waits within
    /// `drain.ceiling` is [`DrainBudget::for_open`] of the number of connections
    /// open at the moment the gate closed. A pass whose drain could not finish
    /// meets an engine that reports busy, so it costs the drain plus its losing
    /// attempts, each bounded by [`CHECKPOINT_BUSY_TIMEOUT`].
    ///
    /// Dropping the hold is only enough if this function gets to return, and an
    /// engine call that never comes back would keep it from returning: the
    /// TRUNCATE runs synchronously inside one poll (see
    /// [`CHECKPOINT_WEDGE_BACKSTOP`]), so a stuck attempt on a runtime worker
    /// shuts every database call in the process out for good. Each attempt
    /// therefore runs on its own `cairn-checkpoint` thread with the private
    /// connection moved onto it. Past [`CHECKPOINT_SLOW_AFTER`] the pass warns
    /// once that the fold is still running and keeps waiting, with the gate
    /// shut. An attempt that has not returned by the backstop is abandoned as a
    /// suspected wedge: the pass reports `checkpoint attempt exceeded ...`,
    /// warns, and reopens the gate. The engine's own checkpoint lock still keeps
    /// correctness, since a transaction that begins while the abandoned attempt
    /// holds it gets `database is locked` rather than a torn read. Until that
    /// thread returns, later passes report the attempt still running and neither
    /// close the gate nor issue a second TRUNCATE.
    ///
    /// The attempt is made whether or not the drain completed. A drain that
    /// expires means a long-lived transaction is still open and the attempt will
    /// probably lose, but `db_maintenance`'s ceiling exists precisely for the
    /// machine that will not go quiet, and a losing attempt now costs
    /// [`CHECKPOINT_BUSY_TIMEOUT`] rather than the pooled five seconds. Always
    /// attempting is also what keeps `attempts` a real measurement of how hard
    /// checkpointing is on this machine.
    ///
    /// Only contention is retried. Any other failure ends the pass, because a
    /// checkpoint that failed *after* it began working has touched the pager
    /// (CAIRN-3838); its connection is private to this call and is dropped on the
    /// way out either way, so it can never reach another caller's BEGIN.
    ///
    /// Calls on the same handle are serialized across the whole pass. The gate
    /// has one boolean closed state, so overlapping holds would let the first
    /// completed call reopen admissions while the second still depended on a
    /// quiet database. Serialization also prevents concurrent TRUNCATE operations.
    ///
    /// Returns `Err` only when no connection could be obtained; a checkpoint that
    /// ran and lost is a successful call reporting an unsuccessful pass.
    pub async fn checkpoint(
        &self,
        max_attempts: usize,
        between_attempts: Duration,
        drain_budget: DrainBudget,
    ) -> DbResult<CheckpointReport> {
        self.checkpoint_bounded(
            max_attempts,
            between_attempts,
            drain_budget,
            AttemptBounds::PRODUCTION,
            Arc::new(truncate_attempt),
        )
        .await
    }

    /// [`Self::checkpoint`] with the attempts' bounds and the attempt itself
    /// supplied, so a test can stand in an engine call that is slow or never
    /// returns.
    async fn checkpoint_bounded(
        &self,
        max_attempts: usize,
        between_attempts: Duration,
        drain_budget: DrainBudget,
        bounds: AttemptBounds,
        attempt: CheckpointAttempt,
    ) -> DbResult<CheckpointReport> {
        // `checkpoint` is public and callers are not required to coordinate.
        // Keep this permit outside every other operation in the pass so a second
        // call cannot close the gate until the first call's GateHold has dropped.
        let _checkpoint_permit = self.checkpoint_lock.lock().await;
        let started = Instant::now();
        let log_bytes_before = self.logical_log_bytes();

        if self.checkpoint_in_engine.load(Ordering::SeqCst) {
            return Ok(CheckpointReport {
                attempts: 0,
                error: Some("an earlier checkpoint attempt is still running in the engine".into()),
                log_bytes_before,
                log_bytes_after: self.logical_log_bytes(),
                duration: started.elapsed(),
                drain: DrainReport::not_closed(),
                slow: false,
            });
        }

        // Opened before the gate closes and deliberately NOT registered with it:
        // a tracked connection here would be counted among the transactions this
        // pass is waiting to drain, and the drain could never reach zero.
        let conn = self.open_connection().await?;
        conn.busy_timeout(CHECKPOINT_BUSY_TIMEOUT)?;
        let mut conn = Some(conn);

        let (hold, drain) = self.gate.close(drain_budget).await;
        let attempts_started = Instant::now();
        let slow_at = attempts_started + bounds.slow_after;
        let deadline = attempts_started + bounds.backstop;
        let mut attempts = 0;
        let mut error = None;
        let mut abandoned = false;
        let mut slow = false;

        for attempt_number in 1..=max_attempts.max(1) {
            if attempt_number > 1 && !between_attempts.is_zero() {
                sleep(between_attempts).await;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            let Some(attempt_conn) = conn.take().filter(|_| !remaining.is_zero()) else {
                break;
            };
            attempts = attempt_number;
            let until_slow = (!slow).then(|| slow_at.saturating_duration_since(Instant::now()));
            let (outcome, became_slow) = run_checkpoint_attempt(
                attempt_conn,
                attempt.clone(),
                until_slow,
                remaining,
                &self.checkpoint_in_engine,
                logical_log_path(&self.path),
                || {
                    log::warn!(
                        "Checkpoint attempt still running after {:?}; the gate stays shut until \
                         it returns: {drain} log_mib_before={:.1}",
                        bounds.slow_after,
                        log_bytes_before as f64 / (1024.0 * 1024.0),
                    );
                },
            )
            .await;
            slow |= became_slow;
            match outcome {
                AttemptOutcome::Returned(_, Ok(())) => {
                    error = None;
                    break;
                }
                AttemptOutcome::Returned(returned, Err(failure)) => {
                    conn = Some(returned);
                    let contended = failure.is_retryable();
                    error = Some(failure.to_string());
                    if !contended {
                        break;
                    }
                }
                AttemptOutcome::Exceeded => {
                    error = Some(format!("checkpoint attempt exceeded {:?}", bounds.backstop));
                    abandoned = true;
                    break;
                }
                AttemptOutcome::Lost(reason) => {
                    error = Some(reason);
                    break;
                }
            }
        }
        drop(hold);

        let report = CheckpointReport {
            attempts,
            error,
            log_bytes_before,
            log_bytes_after: self.logical_log_bytes(),
            duration: started.elapsed(),
            drain,
            slow,
        };
        if abandoned {
            log::warn!(
                "Checkpoint pass abandoned its TRUNCATE attempt at the {:?} wedge backstop and \
                 reopened the connection gate. This is the suspected engine wedge cairn/5644 \
                 guards against: the attempt's `cairn-checkpoint` thread is still inside the \
                 engine, and its stack names what it waits on: {report}",
                bounds.backstop,
            );
        }
        Ok(report)
    }

    async fn configure(&self) -> DbResult<()> {
        // `journal_mode = mvcc` enables BEGIN CONCURRENT (optimistic concurrency)
        // on a local database. The synced engine cannot use MVCC: it captures
        // changes via CDC for push, and "CDC is not supported in MVCC mode". A
        // synced handle therefore keeps the sync engine's own journaling and
        // uses a plain BEGIN for transactions (see `concurrent_begin`). Foreign
        // keys are enforced on every connection regardless of backend.
        if matches!(self.database.as_ref(), DbHandle::Local(_)) {
            self.consume_query("PRAGMA journal_mode = 'mvcc'").await?;
            // Hand checkpointing to Cairn's own maintenance cadence. Unlike
            // `cache_size`, which is per-connection and therefore set in
            // `connect`, this threshold writes through to the store shared by
            // every connection, so setting it once here covers the whole handle.
            self.consume_query(&format!(
                "PRAGMA mvcc_checkpoint_threshold = {CHECKPOINT_THRESHOLD_DISABLED}"
            ))
            .await?;
        }
        self.consume_query("PRAGMA foreign_keys = ON").await?;
        Ok(())
    }
}

/// The set of live connections that might hold an MVCC transaction, and the
/// valve maintenance closes to empty it.
///
/// # Why this gates connections rather than pool checkouts
///
/// A TRUNCATE checkpoint needs an instant in which no MVCC transaction is open
/// anywhere in the process. Production never offers one, so Cairn makes one:
/// close the gate, let the transactions already open finish, checkpoint, reopen.
///
/// That only works if the gate sees every transaction, and gating
/// [`LocalDb::checkout`] would not: [`LocalDb::connect`] hands out connections
/// outside the pool, and a writer or migration on one can hold a transaction
/// as long as it likes. When this gate was built, the resource layer also read
/// on such connections, holding a `BEGIN CONCURRENT` across a whole render. A
/// pool-only gate would have drained to zero, reported a quiet instant, and
/// lost the lock anyway.
///
/// Every MVCC transaction runs on a `turso::Connection`, so registering the
/// CONNECTION is a superset of registering the transaction. That is the
/// direction to err in: an idle registered connection can cost a skipped pass,
/// but it can never manufacture a quiet instant that does not exist.
///
/// # Why a caller waiting here cannot hang
///
/// [`Self::close`] returns a [`GateHold`] that reopens the gate in its `Drop`,
/// so the gate reopens on every exit path — early return, error, panic — and the
/// drain itself is bounded by a budget. A caller waiting at the gate therefore
/// waits for that budget and then for the fold, up to
/// [`CHECKPOINT_WEDGE_BACKSTOP`]. That property is what makes gating the whole
/// process tractable: a call path this design failed to anticipate costs a
/// bounded pause and a wasted maintenance pass, not a wedged application, and a
/// fold that is merely slow costs a longer pause, never a failed call.
#[derive(Debug)]
pub(super) struct ConnectionGate {
    /// What this database's statements have been explained as. On the gate
    /// because the gate is the one object every connection to the database
    /// reaches.
    plan_audit: PlanAudit,
    #[cfg(any(test, feature = "test-utils"))]
    statement_count: AtomicUsize,
    #[cfg(any(test, feature = "test-utils"))]
    read_statement_count: AtomicUsize,
    #[cfg(any(test, feature = "test-utils"))]
    read_checkout_count: AtomicUsize,
    state: Mutex<GateState>,
    /// Woken when the gate reopens, releasing everyone waiting to be admitted.
    reopened: Notify,
    /// Woken when the last live connection is released, so a drain settles the
    /// instant it can rather than at the end of a polling interval.
    drained: Notify,
}

#[derive(Debug, Default)]
struct GateState {
    closed: bool,
    next_id: u64,
    live: HashMap<u64, LiveConnection>,
}

/// One live connection, recorded to explain a drain that did not finish and a
/// holder that has kept its connection too long: who asked for it, for what,
/// since when, and what it is running now.
#[derive(Debug)]
struct LiveConnection {
    caller: Caller,
    kind: HolderKind,
    since: Instant,
    holder: Holder,
    /// When the gate watch last reported this holder.
    last_warned: Option<Instant>,
    /// Whether the backstop has already interrupted it.
    interrupted: bool,
}

impl ConnectionGate {
    fn new() -> Self {
        Self {
            plan_audit: PlanAudit::new(),
            #[cfg(any(test, feature = "test-utils"))]
            statement_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            read_statement_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            read_checkout_count: AtomicUsize::new(0),
            state: Mutex::new(GateState::default()),
            reopened: Notify::new(),
            drained: Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Register one connection, waiting first if maintenance holds the gate
    /// shut.
    ///
    /// Interest in the reopen signal is registered BEFORE `closed` is observed,
    /// which is what `enable()` does. Checking first and waiting second would
    /// lose a reopen landing in between and park the caller until the next pass
    /// closed and reopened the gate — a stall of one whole interval, and one
    /// that would only show up under load.
    pub(super) async fn admit(
        self: &Arc<Self>,
        caller: Caller,
        kind: HolderKind,
        holder: Holder,
    ) -> ConnectionSlot {
        loop {
            let reopened = self.reopened.notified();
            tokio::pin!(reopened);
            reopened.as_mut().enable();
            {
                let mut state = self.lock();
                if !state.closed {
                    let id = state.next_id;
                    state.next_id = state.next_id.wrapping_add(1);
                    state.live.insert(
                        id,
                        LiveConnection {
                            caller,
                            kind,
                            since: Instant::now(),
                            holder: holder.clone(),
                            last_warned: None,
                            interrupted: false,
                        },
                    );
                    return ConnectionSlot {
                        gate: self.clone(),
                        id,
                        holder,
                    };
                }
            }
            reopened.await;
        }
    }

    fn release(&self, id: u64) {
        let (removed, emptied) = {
            let mut state = self.lock();
            let removed = state.live.remove(&id);
            (removed, state.live.is_empty())
        };
        drop(removed);
        if emptied {
            self.drained.notify_waiters();
        }
    }

    /// Close the gate and wait for the connections already open to be released,
    /// for as long as `drain` allows given how many were open at the close.
    ///
    /// The count is sampled under the same lock that shuts the gate, so it is
    /// exactly the set this drain has to wait out: nothing can join it after.
    ///
    /// Returns the hold whether or not the drain finished, so the caller decides
    /// what a partial drain is worth and the gate reopens when that hold drops
    /// either way.
    async fn close(self: &Arc<Self>, drain: DrainBudget) -> (GateHold, DrainReport) {
        let started = Instant::now();
        let open_at_close = {
            let mut state = self.lock();
            state.closed = true;
            state.live.len()
        };
        let hold = GateHold { gate: self.clone() };
        let budget = drain.for_open(open_at_close);
        let deadline = started + budget;

        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();

            let (still_open, oldest) = {
                let state = self.lock();
                let oldest = state
                    .live
                    .values()
                    .min_by_key(|live| live.since)
                    .map(|live| (live.caller, live.since.elapsed()));
                (state.live.len(), oldest)
            };
            if still_open == 0 {
                return (
                    hold,
                    DrainReport {
                        drained: true,
                        open_at_close,
                        budget,
                        still_open: 0,
                        oldest: None,
                        waited: started.elapsed(),
                    },
                );
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return (
                    hold,
                    DrainReport {
                        drained: false,
                        open_at_close,
                        budget,
                        still_open,
                        oldest,
                        waited: started.elapsed(),
                    },
                );
            }
            let _ = tokio::time::timeout(remaining, drained).await;
        }
    }

    fn open(&self) {
        self.lock().closed = false;
        self.reopened.notify_waiters();
    }
}

/// Keeps the gate shut for as long as it lives, and reopens it when dropped.
///
/// The reopen lives in `Drop` rather than at the end of the checkpoint so that
/// it happens on every exit path, including a panic raised inside the database
/// engine — which this checkpoint path has produced before (CAIRN-3838). A
/// checkpoint that dies holding the gate open-coded would lock every database
/// call in the process out permanently.
struct GateHold {
    gate: Arc<ConnectionGate>,
}

impl Drop for GateHold {
    fn drop(&mut self) {
        self.gate.open();
    }
}

/// One live connection's registration with the gate.
///
/// Dropping it deregisters, which is the whole mechanism: every way a
/// connection's life can end — released to the free-list, retired after a failed
/// ROLLBACK, dropped on an error path, dropped while unwinding — settles the
/// count without any of those paths knowing the gate exists.
pub(super) struct ConnectionSlot {
    gate: Arc<ConnectionGate>,
    id: u64,
    holder: Holder,
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        // The interrupt handle first: it is a clone of the connection, and the
        // engine rolls a dropped connection's transaction back only when the
        // last clone goes, which has to happen before the drain hears this
        // connection is gone.
        self.holder.disarm();
        self.gate.release(self.id);
    }
}

/// A `turso::Connection` that counts toward the quiet instant maintenance waits
/// for.
///
/// Derefs to the connection, so a helper typed `&turso::Connection` takes one
/// unchanged. The wrapper's first purpose is its `Drop`.
///
/// Its own `query`, `execute`, `prepare` and `execute_batch` shadow the
/// connection's to record each statement's call site and SQL head on the
/// connection's gate record before running it, and to count the statement as
/// in flight until it finishes (for a query, until its [`TrackedRows`] drop).
/// That label is what the gate watch, a read-bound error, and the runtime
/// stall line name. `query`, `execute` and `prepare` also hand the statement to
/// the plan audit (`storage::plan_audit`), which explains it the first time
/// its call site issues it; `execute_batch` is labelled but not explained,
/// since a script is many statements. Everything else reaches the connection
/// through `Deref`, which is `#[track_caller]` and records the borrowing line
/// as `(raw connection; sql not captured)`: a helper called with `&conn` is
/// named by its call site, is not counted as in flight, and is invisible to
/// the plan audit. Retyping such a helper to take `&TrackedConnection` is what
/// brings its statements into view.
pub struct TrackedConnection {
    conn: Connection,
    slot: ConnectionSlot,
}

impl TrackedConnection {
    pub(super) fn new(conn: Connection, slot: ConnectionSlot) -> Self {
        Self { conn, slot }
    }

    /// [`Connection::query`], recording the statement first. The statement
    /// counts as running until the returned rows drop.
    #[track_caller]
    pub fn query(
        &self,
        sql: impl AsRef<str>,
        params: impl IntoParams,
    ) -> impl Future<Output = turso::Result<TrackedRows>> + Captures<&'_ ()> {
        #[cfg(any(test, feature = "test-utils"))]
        self.slot
            .gate
            .statement_count
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(any(test, feature = "test-utils"))]
        self.slot
            .gate
            .read_statement_count
            .fetch_add(1, Ordering::Relaxed);
        let at = Location::caller();
        self.slot.holder.set_statement(at, sql.as_ref());
        let audit = self.plan_audit(at, sql.as_ref());
        let in_flight = self.slot.holder.start_statement();
        let running = self.conn.query(sql, params);
        async move {
            if let Some(audit) = audit {
                audit.await;
            }
            let rows = running.await?;
            Ok(TrackedRows {
                rows,
                _in_flight: in_flight,
            })
        }
    }

    /// [`Connection::execute`], recording the statement first. The statement
    /// counts as running until it resolves.
    #[track_caller]
    pub fn execute(
        &self,
        sql: impl AsRef<str>,
        params: impl IntoParams,
    ) -> impl Future<Output = turso::Result<u64>> + Captures<&'_ ()> {
        #[cfg(any(test, feature = "test-utils"))]
        self.slot
            .gate
            .statement_count
            .fetch_add(1, Ordering::Relaxed);
        let at = Location::caller();
        self.slot.holder.set_statement(at, sql.as_ref());
        let audit = self.plan_audit(at, sql.as_ref());
        let in_flight = self.slot.holder.start_statement();
        let running = self.conn.execute(sql, params);
        async move {
            if let Some(audit) = audit {
                audit.await;
            }
            let changed = running.await;
            drop(in_flight);
            changed
        }
    }

    /// [`Connection::prepare`], recording the statement and handing it to the
    /// plan audit first. The prepared statement is not counted as in flight:
    /// its runs happen through the statement, out of this wrapper's sight.
    #[track_caller]
    pub fn prepare(
        &self,
        sql: impl AsRef<str>,
    ) -> impl Future<Output = turso::Result<turso::Statement>> + Captures<&'_ ()> {
        #[cfg(any(test, feature = "test-utils"))]
        self.slot
            .gate
            .statement_count
            .fetch_add(1, Ordering::Relaxed);
        #[cfg(any(test, feature = "test-utils"))]
        self.slot
            .gate
            .read_statement_count
            .fetch_add(1, Ordering::Relaxed);
        let at = Location::caller();
        self.slot.holder.set_statement(at, sql.as_ref());
        let audit = self.plan_audit(at, sql.as_ref());
        let preparing = self.conn.prepare(sql);
        async move {
            if let Some(audit) = audit {
                audit.await;
            }
            preparing.await
        }
    }

    /// [`Connection::execute_batch`], recording the script first and counting
    /// it as running until it resolves. Not explained: `EXPLAIN` takes one
    /// statement. A script that may change the schema tells the plan audit,
    /// which reads the schema again at its next sighting.
    #[track_caller]
    pub fn execute_batch(
        &self,
        sql: impl AsRef<str>,
    ) -> impl Future<Output = turso::Result<()>> + Captures<&'_ ()> {
        #[cfg(any(test, feature = "test-utils"))]
        self.slot
            .gate
            .statement_count
            .fetch_add(1, Ordering::Relaxed);
        self.slot
            .holder
            .set_statement(Location::caller(), sql.as_ref());
        let schema_change = plan_audit::is_schema_change(sql.as_ref());
        let in_flight = self.slot.holder.start_statement();
        let running = self.conn.execute_batch(sql);
        async move {
            let ran = running.await;
            drop(in_flight);
            if schema_change && ran.is_ok() {
                self.slot.gate.plan_audit.schema_changed();
            }
            ran
        }
    }

    /// Leave every statement for the rest of this database call out of the
    /// plan audit. For SQL an agent wrote (`cairn://db`): an ad hoc scan is
    /// the reader's choice, not a defect in the codebase.
    pub fn exempt_from_plan_audit(&self) {
        self.slot
            .holder
            .0
            .plan_audit_exempt
            .store(true, Ordering::Relaxed);
    }

    /// The plan audit's work for `sql` issued at `at`: an `EXPLAIN` the first
    /// time this call site issues this SQL here, nothing after. Boxed, so the
    /// statement future carries a pointer rather than the explain's state.
    fn plan_audit(&self, at: Caller, sql: &str) -> Option<BoxFuture<'_, ()>> {
        if self.slot.holder.plan_audit_exempt() {
            return None;
        }
        let sighting = self.slot.gate.plan_audit.first_sighting(at, sql)?;
        Some(Box::pin(
            self.slot.gate.plan_audit.run(&self.conn, sighting),
        ))
    }

    /// Take the bare connection back, ending its registration.
    ///
    /// Only [`LocalDb::release`] does this: a connection resting on the
    /// free-list holds no transaction, so counting it would hold the drain above
    /// zero forever.
    fn into_connection(self) -> Connection {
        let Self { conn, slot } = self;
        drop(slot);
        conn
    }
}

impl Deref for TrackedConnection {
    type Target = Connection;

    #[track_caller]
    fn deref(&self) -> &Connection {
        self.slot.holder.set_raw_site(Location::caller());
        &self.conn
    }
}

/// [`turso::Rows`] from a [`TrackedConnection::query`], counting as a running
/// statement on its connection until dropped. That mirrors the engine, which
/// keeps a query's root statement active until its rows are reset, so a
/// caller holding rows between `next` calls is still mid-statement.
pub struct TrackedRows {
    rows: turso::Rows,
    _in_flight: InFlight,
}

impl Deref for TrackedRows {
    type Target = turso::Rows;

    fn deref(&self) -> &turso::Rows {
        &self.rows
    }
}

impl DerefMut for TrackedRows {
    fn deref_mut(&mut self) -> &mut turso::Rows {
        &mut self.rows
    }
}

impl std::fmt::Debug for TrackedRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TrackedRows")
    }
}

impl std::fmt::Debug for TrackedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TrackedConnection")
    }
}

/// How long a checkpoint pass may hold the gate shut waiting for the
/// connections already open, as a function of how many there were.
///
/// A fixed budget fails exactly when it matters. Each open connection is a
/// transaction that has to finish before the fold can run, and production has
/// had ten to forty-five of them open at once; half a second drains ten short
/// ones and none of forty-five. Scaling with the count lets a crowded pass wait
/// long enough to win, while the ceiling keeps the stall every database call
/// pays during the drain bounded no matter how crowded the machine is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainBudget {
    /// The wait with nothing open, and the least any pass waits.
    pub floor: Duration,
    /// Added for each connection open when the gate closes.
    pub per_open: Duration,
    /// The most any pass waits, however many were open.
    pub ceiling: Duration,
}

impl DrainBudget {
    /// The drain the production checkpoint pass runs: half a second for a
    /// quiet machine, fifty milliseconds more per open connection, and never
    /// more than two and a half seconds of stall. Defined here, beside the
    /// type, so `db_maintenance` and the production-copy probes cannot drift
    /// apart.
    pub const PRODUCTION: Self = Self {
        floor: Duration::from_millis(500),
        per_open: Duration::from_millis(50),
        ceiling: Duration::from_millis(2_500),
    };

    /// A budget that ignores the count. For tests and for the startup fold,
    /// where nothing else is running.
    pub const fn fixed(budget: Duration) -> Self {
        Self {
            floor: budget,
            per_open: Duration::ZERO,
            ceiling: budget,
        }
    }

    /// `floor + per_open × open`, clamped to `[floor, ceiling]`.
    pub fn for_open(&self, open: usize) -> Duration {
        let open = u32::try_from(open).unwrap_or(u32::MAX);
        let ceiling = self.ceiling.max(self.floor);
        self.floor
            .saturating_add(self.per_open.saturating_mul(open))
            .clamp(self.floor, ceiling)
    }
}

/// What one attempt to empty the gate achieved.
///
/// The failure fields are the point of this type. A drain that expires means
/// some transaction outlived the budget, and the question that decides whether
/// this whole design holds — is it one pathological reader, or genuinely ten
/// short ones that never align? — is answerable only if the pass says which.
/// Production has `read_batch` reaching 109.7 s and
/// `get_thread_status_indicators` 18.6 s, so naming the oldest holder turns the
/// follow-up into reading one log line rather than reopening the investigation.
#[derive(Debug, Clone)]
pub struct DrainReport {
    /// Whether every connection open when the gate closed had been released
    /// before the budget expired.
    pub drained: bool,
    /// Connections live at the instant the gate closed: the set this drain had
    /// to wait out, and what `budget` was derived from.
    pub open_at_close: usize,
    /// How long this drain was allowed to wait.
    pub budget: Duration,
    /// Connections still live at the deadline.
    pub still_open: usize,
    /// The source line that asked for the longest-held live connection, and how
    /// long it had been out.
    pub oldest: Option<(Caller, Duration)>,
    /// How long the drain waited — the stall every database call in the process
    /// paid for this pass.
    pub waited: Duration,
}

impl DrainReport {
    /// The report of a pass that never closed the gate.
    fn not_closed() -> Self {
        Self {
            drained: false,
            open_at_close: 0,
            budget: Duration::ZERO,
            still_open: 0,
            oldest: None,
            waited: Duration::ZERO,
        }
    }
}

impl std::fmt::Display for DrainReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drain_ms={} budget_ms={} open_at_close={} drained={}",
            self.waited.as_millis(),
            self.budget.as_millis(),
            self.open_at_close,
            self.drained
        )?;
        if !self.drained {
            write!(f, " still_open={}", self.still_open)?;
            if let Some((caller, age)) = &self.oldest {
                write!(f, " oldest={}/{}ms", Site(caller), age.as_millis())?;
            }
        }
        Ok(())
    }
}

/// Run `sql` on an already-checked-out connection, draining its result rows.
///
/// Split out from [`LocalDb::consume_query`] so a caller that must issue several
/// statements on ONE connection — [`LocalDb::checkpoint`] retrying a contended
/// checkpoint — can do so without returning the connection to the pool and
/// drawing a different one between attempts.
async fn consume_on(conn: &Connection, sql: &str) -> DbResult<()> {
    let mut rows = conn.query(sql, ()).await?;
    while rows.next().await?.is_some() {}
    Ok(())
}

/// One checkpoint attempt, run to completion on the thread that calls it.
type CheckpointAttempt = Arc<dyn Fn(&Connection) -> DbResult<()> + Send + Sync>;

/// The production attempt: the TRUNCATE statement, driven on a runtime of its
/// own so that nothing it blocks belongs to the application's runtime.
fn truncate_attempt(conn: &Connection) -> DbResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .map_err(|error| DbError::internal(format!("checkpoint thread runtime: {error}")))?;
    runtime.block_on(consume_on(conn, "PRAGMA wal_checkpoint(TRUNCATE)"))
}

/// When a pass's attempts are reported slow, and when they are given up.
#[derive(Debug, Clone, Copy)]
struct AttemptBounds {
    slow_after: Duration,
    backstop: Duration,
}

impl AttemptBounds {
    const PRODUCTION: Self = Self {
        slow_after: CHECKPOINT_SLOW_AFTER,
        backstop: CHECKPOINT_WEDGE_BACKSTOP,
    };
}

/// How one bounded attempt ended.
enum AttemptOutcome {
    /// The engine returned, handing the connection back for a retry.
    Returned(Connection, DbResult<()>),
    /// The bound passed first; the attempt is still running on its thread.
    Exceeded,
    /// The attempt ended without a result: its thread could not start, or it
    /// panicked.
    Lost(String),
}

/// Marks a checkpoint attempt as running in the engine for as long as it lives.
struct InEngine(Arc<AtomicBool>);

impl InEngine {
    fn enter(flag: Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::SeqCst);
        Self(flag)
    }
}

impl Drop for InEngine {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Run `attempt` on `conn` on a dedicated `cairn-checkpoint` thread and wait
/// for it at most `bound`, calling `on_slow` once if it is still running after
/// `until_slow`. Returns how the attempt ended and whether it was slow.
///
/// The thread runs at the QoS the runtime's own workers run at. A spawned
/// thread does not inherit its parent's QoS on macOS, so it would otherwise
/// come up at default, a class below the workers, and the workers are what
/// load the host when a fold meets a storm. On the clone that difference was
/// most of the fold's throughput: a 64 MiB fold under twelve busy
/// user-initiated threads ran at 15 to 18 MB/s at default and 146 to 239
/// MB/s raised (cairn/5655).
///
/// The marker is cleared before the result is sent, so a pass that has its
/// connection back never reads its own attempt as still running. A panic
/// clears it by unwinding.
///
/// The pass drops the receiving end when it abandons the attempt, so a send
/// that fails is exactly an attempt nobody waited for. That attempt reports
/// its own end: how long it was inside the engine, how it came out, and the
/// logical log (`log_path`) it left. The line says whether the condition that
/// held it released once the gate reopened, which a wedge sample alone cannot.
async fn run_checkpoint_attempt(
    conn: Connection,
    attempt: CheckpointAttempt,
    until_slow: Option<Duration>,
    bound: Duration,
    in_engine: &Arc<AtomicBool>,
    log_path: PathBuf,
    on_slow: impl FnOnce(),
) -> (AttemptOutcome, bool) {
    let (sender, mut receiver) = tokio::sync::oneshot::channel();
    let running = InEngine::enter(in_engine.clone());
    let spawned = std::thread::Builder::new()
        .name("cairn-checkpoint".to_string())
        .spawn(move || {
            cairn_platform::priority::raise_current_thread_for_orchestration();
            let started = Instant::now();
            let result = attempt(&conn);
            drop(running);
            if let Err((_, result)) = sender.send((conn, result)) {
                let log_bytes = std::fs::metadata(&log_path).map_or(0, |meta| meta.len());
                let outcome = match &result {
                    Ok(()) => "checkpointed".to_string(),
                    Err(error) => format!("failed error={error}"),
                };
                log::warn!(
                    "Abandoned checkpoint attempt left the engine after {}ms: outcome={outcome} \
                     log_mib_after={:.1}",
                    started.elapsed().as_millis(),
                    log_bytes as f64 / (1024.0 * 1024.0),
                );
            }
        });
    if let Err(error) = spawned {
        return (
            AttemptOutcome::Lost(format!("could not start the checkpoint thread: {error}")),
            false,
        );
    }
    let started = Instant::now();
    let mut slow = false;
    let received = match until_slow.filter(|until| *until < bound) {
        Some(until) => match timeout(until, &mut receiver).await {
            Ok(received) => Some(received),
            Err(_) => {
                slow = true;
                on_slow();
                None
            }
        },
        None => None,
    };
    let received = match received {
        Some(received) => Ok(received),
        None => timeout(bound.saturating_sub(started.elapsed()), receiver).await,
    };
    let outcome = match received {
        Ok(Ok((conn, result))) => AttemptOutcome::Returned(conn, result),
        Ok(Err(_)) => AttemptOutcome::Lost("the checkpoint attempt's thread panicked".to_string()),
        Err(_) => AttemptOutcome::Exceeded,
    };
    (outcome, slow)
}

/// What one [`LocalDb::checkpoint`] pass did: whether it won, what it cost, and
/// how much logical log it folded away.
#[derive(Debug, Clone)]
pub struct CheckpointReport {
    /// TRUNCATE attempts this pass issued.
    ///
    /// With the connection gate drained, the first attempt should win. More than
    /// one attempt means the gate did not establish the quiet window completely;
    /// repeated failures with `drain.drained == true` mean a checkpoint-lock holder
    /// exists outside the connections this process tracks.
    pub attempts: usize,
    /// The final attempt's failure, or `None` when the pass checkpointed.
    pub error: Option<String>,
    pub log_bytes_before: u64,
    pub log_bytes_after: u64,
    pub duration: Duration,
    /// How emptying the gate went before the attempts began. A failed pass whose
    /// drain also failed is a pass that never had its precondition; a failed pass
    /// whose drain SUCCEEDED is a different and much more alarming thing, and
    /// only this field tells them apart.
    pub drain: DrainReport,
    /// Whether the attempts ran past [`CHECKPOINT_SLOW_AFTER`] with the gate
    /// shut. Read beside [`Self::fold_mib_per_s`]: a slow fold at a healthy
    /// rate is a big log, a slow fold at a collapsed rate is a starved host.
    pub slow: bool,
}

impl CheckpointReport {
    pub fn succeeded(&self) -> bool {
        self.error.is_none()
    }

    /// The fold's throughput: the log it started with over the time the
    /// attempts took, the drain excluded. `None` unless the pass folded.
    pub fn fold_mib_per_s(&self) -> Option<f64> {
        let work = self.duration.saturating_sub(self.drain.waited);
        (self.succeeded() && !work.is_zero())
            .then(|| self.log_bytes_before as f64 / (1024.0 * 1024.0) / work.as_secs_f64())
    }
}

impl std::fmt::Display for CheckpointReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mib = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
        write!(
            f,
            "attempts={} duration_ms={} log_mib_before={:.1} log_mib_after={:.1} {}",
            self.attempts,
            self.duration.as_millis(),
            mib(self.log_bytes_before),
            mib(self.log_bytes_after),
            self.drain,
        )?;
        if let Some(rate) = self.fold_mib_per_s() {
            write!(f, " fold_mib_per_s={rate:.1}")?;
        }
        if self.slow {
            write!(f, " slow=true")?;
        }
        match &self.error {
            Some(error) => write!(f, " outcome=failed error={error}"),
            None => write!(f, " outcome=checkpointed"),
        }
    }
}

/// The three files comprising one MVCC database set: the main `.db` plus its
/// `-wal` and `-log` sidecars. Committed data lives in the sidecars, so any
/// move, copy, backup, or snapshot of the database must treat all three as one
/// unit (see docs/database.md). A private database's archive directory is part
/// of it too: after the archive offload, a copy of the set without the archive
/// reads archived content as stubs. Returned in the order `[main, -wal, -log]`.
///
/// Each sidecar is named the way the pinned Turso names it, so the set is
/// the files the engine actually wrote even for a path [`LocalDb::open`] would
/// refuse. For a `.db` path that is `{path, path-wal, path-log}`.
pub fn db_set_paths(base: &Path) -> [PathBuf; 3] {
    [base.to_path_buf(), wal_path(base), logical_log_path(base)]
}

/// Turso's WAL: `-wal` appended to the whole path.
fn wal_path(base: &Path) -> PathBuf {
    let mut name = base.as_os_str().to_owned();
    name.push("-wal");
    PathBuf::from(name)
}

/// Turso's MVCC logical log: the path with its extension replaced by
/// `db-log` (turso_core's `journal_mode.rs`), not `-log` appended. The two
/// agree only for a `.db` path; `cairn.db.probe` logs to `cairn.db.db-log`.
fn logical_log_path(base: &Path) -> PathBuf {
    base.with_extension("db-log")
}

/// Admit only a path whose file set belongs to it alone: a `.db` file, or
/// Turso's file-less `:memory:`.
///
/// Because Turso derives the logical log by replacing the extension, any two
/// paths that differ only in their last extension share one log:
/// `cairn.db.vacuum-staged` and `cairn.db.vacuum-backup` both write
/// `cairn.db.db-log`, and an extensionless `db` writes `db.db-log`, the log of
/// `db.db`. Moving, sizing, or deleting either set then acts on the other's
/// committed data. A `.db` path's log is `{path}-log`, which no other path
/// produces.
pub(super) fn require_db_path(path: &Path) -> DbResult<()> {
    if path.as_os_str() == ":memory:" || path.extension().is_some_and(|ext| ext == "db") {
        return Ok(());
    }
    Err(DbError::internal(format!(
        "database path {} must end in .db: Turso names the logical log from the \
         extension, so under any other name it can share its log with another database",
        path.display()
    )))
}

/// Total size in bytes of every member of `base`'s three-file set that exists on
/// disk. Absent sidecars contribute zero, so the figure is meaningful whether or
/// not the set's log has been folded away.
pub fn db_set_size(base: &Path) -> u64 {
    db_set_paths(base)
        .iter()
        .filter_map(|p| std::fs::metadata(p).ok())
        .map(|m| m.len())
        .sum()
}

/// Move an MVCC database set from `from_base` to `to_base`, relocating every
/// member of the set that exists and skipping any absent sidecar. Refuses to
/// clobber: if any destination member already exists, nothing is moved and an
/// `AlreadyExists` error is returned.
pub fn move_db_set(from_base: &Path, to_base: &Path) -> std::io::Result<()> {
    let sources = db_set_paths(from_base);
    let dests = db_set_paths(to_base);
    for dest in &dests {
        if dest.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("destination already exists: {}", dest.display()),
            ));
        }
    }
    for (src, dest) in sources.iter().zip(dests.iter()) {
        if src.exists() {
            std::fs::rename(src, dest)?;
        }
    }
    Ok(())
}

/// One transaction attempt's result, plus whether its connection may go back on
/// the free-list.
///
/// A connection is reusable only when it is known to hold no open transaction:
/// the transaction committed, or the ROLLBACK that unwound it succeeded. When a
/// ROLLBACK itself fails the connection's transaction state is unknown, and
/// returning it would make some later, unrelated caller fail its BEGIN on a
/// connection it never touched. Retiring it costs one connection; reusing it
/// costs a bug that reads as random.
struct TxAttempt<T> {
    result: DbResult<T>,
    reusable: bool,
}

/// Run the statement that ends a transaction, containing a panic raised inside
/// the database engine.
///
/// Turso checks pager invariants with assertions rather than errors, and
/// `ROLLBACK` is where they are checked: `rollback_tx` asserts that a
/// non-writing transaction left no dirty pages behind. A connection can fail
/// that assertion through no fault of the statement running on it. In the
/// episode that motivated this, the assertion fired on connections doing
/// ordinary reads, in a process where every MVCC auto-checkpoint had been
/// failing for days (CAIRN-3838); the route from that failure to this assertion
/// has not been pinned down, and the reproduction on that issue reaches
/// different bounds checks first. What the assertion establishes either way is
/// local and sufficient: this connection's pager is not in the state the engine
/// expects, so it is unfit to hand to the next caller.
///
/// Letting that panic unwind spends a user's action on a pooled connection's
/// private corruption — the failure the caller sees has nothing to do with what
/// it asked for. Containing it here turns the panic into what it actually is:
/// evidence that this connection's transaction state is unknown, which is
/// already the pool's condition for retiring one ([`TxAttempt::reusable`]). The
/// connection is dropped either way; only the caller's fate differs.
async fn end_tx(conn: &Connection, sql: &'static str) -> DbResult<()> {
    match AssertUnwindSafe(conn.execute(sql, ())).catch_unwind().await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(error.into()),
        Err(payload) => {
            let message = panic_message(&*payload);
            log::error!(
                "{sql} panicked inside the database engine ({message}); retiring this connection"
            );
            Err(DbError::internal(format!(
                "{sql} panicked inside the database engine: {message}"
            )))
        }
    }
}

async fn run_tx<T>(
    conn: &TrackedConnection,
    begin_sql: &str,
    f: &mut impl for<'a> FnMut(&'a TrackedConnection) -> BoxFuture<'a, DbResult<T>>,
) -> TxAttempt<T> {
    if let Err(error) = conn.conn.execute(begin_sql, ()).await {
        // A BEGIN that fails opened nothing, but it also means this connection
        // was not in the state the pool guarantees. Retire it rather than reason
        // about why.
        return TxAttempt {
            result: Err(error.into()),
            reusable: false,
        };
    }

    match f(conn).await {
        Ok(value) => match end_tx(&conn.conn, "COMMIT").await {
            Ok(()) => TxAttempt {
                result: Ok(value),
                reusable: true,
            },
            Err(error) => {
                let reusable = end_tx(&conn.conn, "ROLLBACK").await.is_ok();
                TxAttempt {
                    result: Err(error),
                    reusable,
                }
            }
        },
        Err(error) => {
            let reusable = end_tx(&conn.conn, "ROLLBACK").await.is_ok();
            TxAttempt {
                result: Err(error),
                reusable,
            }
        }
    }
}

async fn run_read_tx<T>(
    conn: &TrackedConnection,
    begin_sql: &str,
    f: impl for<'a> FnOnce(&'a TrackedConnection) -> BoxFuture<'a, DbResult<T>>,
) -> TxAttempt<T> {
    if let Err(error) = conn.conn.execute(begin_sql, ()).await {
        return TxAttempt {
            result: Err(error.into()),
            reusable: false,
        };
    }

    match f(conn).await {
        Ok(value) => {
            // A read transaction's teardown cannot invalidate what it already
            // read: the rows came from one consistent snapshot, and rolling a
            // read transaction back discards nothing. So a `ROLLBACK` that
            // fails — or that panics on an engine assertion (see [`end_tx`]) —
            // condemns the connection and nothing else, and the value it
            // produced is returned rather than thrown away with it. Reporting
            // the teardown instead would fail a read that had already
            // succeeded, on grounds the caller can neither see nor act on.
            let outcome = end_tx(&conn.conn, "ROLLBACK").await;
            if let Err(error) = &outcome {
                log::warn!(
                    "read transaction completed but its ROLLBACK failed ({error}); returning the value and retiring this connection"
                );
            }
            TxAttempt {
                result: Ok(value),
                reusable: outcome.is_ok(),
            }
        }
        Err(error) => {
            let reusable = end_tx(&conn.conn, "ROLLBACK").await.is_ok();
            TxAttempt {
                result: Err(error),
                reusable,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn contention_timing_reports_retries_and_total_backoff() {
        assert!(transaction_contention_event(
            1,
            Duration::from_millis(9),
            Duration::from_millis(20),
            "BEGIN CONCURRENT",
        )
        .is_none());

        let event = transaction_contention_event(
            2,
            Duration::from_millis(7),
            Duration::from_millis(25),
            "BEGIN CONCURRENT",
        )
        .expect("a retried transaction is contention");
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["event"], "db_transaction_contention");
        assert_eq!(value["attempts"], 2);
        assert_eq!(value["backoff_us"], 7_000);
        assert_eq!(value["duration_us"], 25_000);
        assert_eq!(value["mode"], "concurrent");
    }
    use crate::storage::{Migration, MigrationRunner, RowExt};

    /// The drain budget these tests hold the gate open for.
    ///
    /// Generous next to the 500 ms `db_maintenance` uses in production, because
    /// a test asserting that a checkpoint WINS must not be able to fail on a
    /// loaded CI machine for want of a few milliseconds. Tests that assert the
    /// budget EXPIRES pass their own short one.
    const DRAIN_BUDGET: DrainBudget = DrainBudget::fixed(Duration::from_secs(5));

    /// Attempts a test pass makes. More than production's three, so that a test
    /// asserting `attempts == 1` is asserting the gate did its job rather than
    /// being propped up by having nowhere else to go.
    const ATTEMPTS_FOR_TESTS: usize = 4;

    #[tokio::test]
    async fn single_select_helpers_avoid_transaction_round_trip() {
        let db = test_db().await.unwrap();
        let before = db.read_transaction_count.load(Ordering::Relaxed);
        let value = db
            .query_one("SELECT 1", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(value, 1);
        assert_eq!(
            db.read_transaction_count.load(Ordering::Relaxed),
            before,
            "single-statement helpers must not open an explicit read transaction"
        );

        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query("SELECT 1", ()).await?;
                rows.next().await?.unwrap().i64(0)
            })
        })
        .await
        .unwrap();
        assert_eq!(
            db.read_transaction_count.load(Ordering::Relaxed),
            before + 1,
            "multi-statement read API must retain explicit snapshot transactions"
        );
    }

    /// The load-independent guard against reintroducing per-call connect.
    ///
    /// Before the free-list, `LocalDb` opened a fresh connection for every
    /// `read`, `write`, `execute`, and single-statement helper. A fresh
    /// connection repeats transaction setup whose absolute duration varies with
    /// machine load. Asserting on connection COUNT rather than elapsed time
    /// states the reuse invariant directly and cannot go quiet under load.
    #[tokio::test]
    async fn sequential_operations_reuse_one_pooled_connection() {
        let db = test_db().await.unwrap();
        let after_open = db.connections_created.load(Ordering::Relaxed);
        assert_eq!(
            after_open, 1,
            "opening and migrating a database should need exactly one connection"
        );

        for i in 0..20 {
            db.execute(
                "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                (format!("pooled-{i}"), i64::from(i)),
            )
            .await
            .unwrap();
            db.query_one("SELECT COUNT(*) FROM counters", (), |row| row.i64(0))
                .await
                .unwrap();
            db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM counters", ()).await?;
                    rows.next()
                        .await?
                        .ok_or_else(|| DbError::Row("missing count row".to_string()))?
                        .i64(0)
                })
            })
            .await
            .unwrap();
        }

        assert_eq!(
            db.connections_created.load(Ordering::Relaxed),
            after_open,
            "60 sequential operations must reuse the pooled connection, not open one apiece"
        );
    }

    #[tokio::test]
    async fn a_rolled_back_transaction_returns_a_clean_connection_to_the_pool() {
        let db = test_db().await.unwrap();
        let before = db.connections_created.load(Ordering::Relaxed);

        let error = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO counters(id, value) VALUES ('rolled-back', 1)",
                        (),
                    )
                    .await?;
                    Err::<(), DbError>(DbError::internal("force rollback"))
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::Internal(_)));

        // Its ROLLBACK succeeded, so the connection went back on the free-list
        // and the next operation reuses it rather than opening another...
        db.execute("INSERT INTO counters(id, value) VALUES ('after', 1)", ())
            .await
            .unwrap();
        assert_eq!(
            db.connections_created.load(Ordering::Relaxed),
            before,
            "a cleanly rolled-back connection must be reused, not retired"
        );

        // ...carrying no residue from the transaction that was unwound on it.
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM counters WHERE id = 'rolled-back'"
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM counters WHERE id = 'after'")
                .await
                .unwrap(),
            1
        );
    }

    /// Every connection must carry the page cache ceiling, not just the one
    /// that happened to configure the database: any connection can be the one
    /// that commits, and committing is what runs the checkpoint whose working
    /// set the default 2 MB cannot hold. A pooled connection and a fresh one
    /// both have to be covered, so this asserts on both.
    #[tokio::test]
    async fn every_connection_carries_the_page_cache_ceiling() {
        let db = test_db().await.unwrap();

        assert_eq!(
            query_i64(&db, "PRAGMA cache_size").await.unwrap(),
            PAGE_CACHE_LIMIT as i64,
            "a pooled connection must raise the cache ceiling above the engine default"
        );

        // The raw out-of-pool escape hatch takes the same configuration.
        let direct = db.connect().await.unwrap();
        let mut rows = direct.query("PRAGMA cache_size", ()).await.unwrap();
        let value = rows.next().await.unwrap().unwrap().i64(0).unwrap();
        assert_eq!(
            value, PAGE_CACHE_LIMIT as i64,
            "a connection opened outside the pool must be configured too"
        );
    }

    /// A read that finished must not be failed by its own teardown. The engine
    /// can refuse or blow up on the terminal `ROLLBACK` for reasons private to
    /// the connection — page state wrecked by a failed auto-checkpoint is the one
    /// that reached users, surfacing as "Database task panicked" on every session
    /// resume (CAIRN-3838). Rolling a read transaction back discards nothing, so
    /// the only thing that failure establishes is that the connection is unfit to
    /// reuse.
    ///
    /// The closure ends its own transaction, so the outer `ROLLBACK` arrives
    /// with nothing to roll back and fails — the same teardown-failed shape,
    /// reachable without a corrupt pager.
    #[tokio::test]
    async fn a_completed_read_survives_a_failing_teardown_and_retires_its_connection() {
        let db = test_db().await.unwrap();
        db.execute("INSERT INTO counters(id, value) VALUES ('read', 5)", ())
            .await
            .unwrap();
        let before = db.connections_created.load(Ordering::Relaxed);

        let value = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query("SELECT value FROM counters WHERE id = 'read'", ())
                        .await?;
                    let value = rows
                        .next()
                        .await?
                        .ok_or_else(|| DbError::Row("missing value row".to_string()))?
                        .i64(0)?;
                    drop(rows);
                    conn.execute("ROLLBACK", ()).await?;
                    Ok(value)
                })
            })
            .await
            .expect("a completed read must return its value even when teardown fails");
        assert_eq!(value, 5);

        // The connection's transaction state is unknown, so it is retired
        // rather than handed to the next caller's BEGIN.
        db.execute("INSERT INTO counters(id, value) VALUES ('after', 1)", ())
            .await
            .unwrap();
        assert!(
            db.connections_created.load(Ordering::Relaxed) > before,
            "a connection whose teardown failed must be retired, not reused"
        );
    }

    /// Pooled connections move between tokio tasks on a multi-threaded runtime,
    /// and concurrent MVCC transactions over them must each commit exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_pooled_transactions_each_commit_exactly_once() {
        let db = Arc::new(test_db().await.unwrap());

        let mut tasks = Vec::new();
        for task_id in 0..4 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..25 {
                    db.execute(
                        "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                        (format!("t{task_id}-{i}"), 1_i64),
                    )
                    .await
                    .unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM counters")
                .await
                .unwrap(),
            100,
            "every concurrent insert must land exactly once"
        );
    }

    /// `connect()` sets `PRAGMA foreign_keys = ON` once, at creation, rather than
    /// on every checkout — so this pins that a recycled connection still carries
    /// it. If the pragma were ever reset by reuse, enforcement would silently
    /// lapse after the first transaction and orphan rows would start committing.
    #[tokio::test]
    async fn foreign_key_enforcement_survives_connection_recycling() {
        let db = test_db().await.unwrap();
        db.execute("INSERT INTO counters(id, value) VALUES ('parent', 1)", ())
            .await
            .unwrap();

        for i in 0..10 {
            db.execute(
                "INSERT INTO counter_notes(id, counter_id) VALUES (?1, 'parent')",
                (format!("note-{i}"),),
            )
            .await
            .unwrap();
        }

        let error = db
            .execute(
                "INSERT INTO counter_notes(id, counter_id) VALUES ('orphan', 'missing')",
                (),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().to_lowercase().contains("foreign key"),
            "expected a foreign-key violation on a recycled connection, got: {error}"
        );
        assert_eq!(query_i64(&db, "PRAGMA foreign_keys").await.unwrap(), 1);
    }

    const TEST_SCHEMA: &[Migration] = &[Migration::new(
        "0001",
        "storage_kernel",
        "
            CREATE TABLE counters (
                id TEXT PRIMARY KEY NOT NULL,
                value INTEGER NOT NULL
            );

            CREATE TABLE unrelated_writes (
                id TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );

            CREATE TABLE counter_notes (
                id TEXT PRIMARY KEY NOT NULL,
                counter_id TEXT NOT NULL REFERENCES counters(id)
            );

            CREATE TABLE issues (
                id TEXT PRIMARY KEY NOT NULL,
                project_id TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE search_outbox (
                id TEXT PRIMARY KEY NOT NULL,
                source_table TEXT NOT NULL,
                source_id TEXT NOT NULL,
                content_type TEXT NOT NULL,
                op TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
                status TEXT NOT NULL CHECK (status IN ('pending', 'applied')),
                created_at INTEGER NOT NULL
            );

            CREATE INDEX idx_search_outbox_status_created
                ON search_outbox(status, created_at);

            CREATE TRIGGER search_issues_insert AFTER INSERT ON issues BEGIN
                INSERT INTO search_outbox(
                    id, source_table, source_id, content_type, op, status, created_at
                )
                VALUES (
                    'search:' || NEW.id || ':' || NEW.updated_at,
                    'issues',
                    NEW.id,
                    'issue',
                    'upsert',
                    'pending',
                    NEW.updated_at
                );
            END;
        ",
    )];

    /// What one trivial statement costs through the two hot entry points, in
    /// nanoseconds per call. `cargo test --release -p cairn-db --lib --
    /// --ignored --nocapture statement_overhead_probe`. Kept as the measurement
    /// the per-statement naming and bound were held to: a budget of about two
    /// microseconds per statement on top of what the engine itself costs.
    ///
    /// The `audited` line repeats the measurement on a database at a lineage
    /// head, where every statement takes the plan audit's first-sighting check
    /// (a hash and a set lookup) rather than stopping at its eligibility load.
    /// Its difference from the first line is the audit's per-statement cost,
    /// held to 0.3 us (cairn/5900).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "measurement probe; run explicitly in release"]
    async fn statement_overhead_probe() {
        let db = test_db().await.unwrap();
        let (one, read) = overhead_of(&db).await;
        println!("statement_overhead_probe: query_one={one}ns/call read={read}ns/call");
        let temp = tempdir().unwrap();
        let audited = LocalDb::open_migrated_for_test(temp.path().join("audited.db"))
            .await
            .unwrap();
        let (one, read) = overhead_of(&audited).await;
        println!("statement_overhead_probe: audited query_one={one}ns/call read={read}ns/call");
    }

    /// Best-of-five nanoseconds per `SELECT 1` through `query_one` and `read`.
    async fn overhead_of(db: &LocalDb) -> (u128, u128) {
        const CALLS: u32 = 20_000;
        for _ in 0..1_000 {
            db.query_one("SELECT 1", (), |row| row.i64(0))
                .await
                .unwrap();
        }
        let mut best_one = u128::MAX;
        let mut best_read = u128::MAX;
        for _round in 0..5 {
            let started = Instant::now();
            for _ in 0..CALLS {
                db.query_one("SELECT 1", (), |row| row.i64(0))
                    .await
                    .unwrap();
            }
            best_one = best_one.min(started.elapsed().as_nanos() / u128::from(CALLS));
            let started = Instant::now();
            for _ in 0..CALLS {
                query_i64(db, "SELECT 1").await.unwrap();
            }
            best_read = best_read.min(started.elapsed().as_nanos() / u128::from(CALLS));
        }
        (best_one, best_read)
    }

    /// One resource read's statements, as the hot resource readers issue them:
    /// a node coordinate resolved to its job, then the node's tasks and
    /// artifacts, the project's recent issues, and its active threads.
    #[derive(Clone)]
    struct ProbeNode {
        project_id: String,
        project_key: String,
        number: i64,
        exec_seq: i64,
        segment: String,
        job_id: String,
    }

    async fn probe_mix(conn: &Connection, node: &ProbeNode) -> DbResult<()> {
        let statements: [(&str, Vec<turso::Value>); 7] = [
            (
                "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2 LIMIT 1",
                vec![node.project_id.clone().into(), node.number.into()],
            ),
            (
                "SELECT j.id FROM jobs j JOIN issues i ON j.issue_id = i.id \
                 JOIN projects p ON i.project_id = p.id JOIN executions e ON j.execution_id = e.id \
                 WHERE p.key = ?1 AND i.number = ?2 AND e.seq = ?3 AND j.parent_job_id IS NULL \
                 AND j.uri_segment = ?4 LIMIT 1",
                vec![
                    node.project_key.clone().into(),
                    node.number.into(),
                    node.exec_seq.into(),
                    node.segment.clone().into(),
                ],
            ),
            (
                "SELECT id, parent_job_id, status, completed_at, started_at, uri_segment \
                 FROM jobs WHERE id = ?1 LIMIT 1",
                vec![node.job_id.clone().into()],
            ),
            (
                "SELECT id, uri_segment, agent_config_id, task_description, status FROM jobs \
                 WHERE parent_job_id = ?1 ORDER BY task_index ASC, created_at ASC",
                vec![node.job_id.clone().into()],
            ),
            (
                "SELECT output_name FROM artifacts WHERE job_id = ?1",
                vec![node.job_id.clone().into()],
            ),
            (
                "SELECT id, number, title, status, updated_at FROM issues WHERE project_id = ?1 \
                 ORDER BY updated_at DESC LIMIT 50",
                vec![node.project_id.clone().into()],
            ),
            (
                "SELECT name, jurisdiction, status, attention, updated_at FROM threads \
                 WHERE project_id = ?1 AND status = 'active' ORDER BY name",
                vec![node.project_id.clone().into()],
            ),
        ];
        for (sql, params) in statements {
            let mut rows = conn.query(sql, params).await?;
            while rows.next().await?.is_some() {}
        }
        Ok(())
    }

    fn percentile(sorted: &[Duration], p: f64) -> Duration {
        if sorted.is_empty() {
            return Duration::ZERO;
        }
        sorted[((sorted.len() - 1) as f64 * p).round() as usize]
    }

    /// Today's resource reads against the pool they move into, under load.
    ///
    /// Mode A is the retired path: `connect()`, `BEGIN CONCURRENT`, the
    /// statements, drop. Mode B is `read`. Same statement mix, same nodes, N
    /// concurrent readers for D seconds each. Reports per-call p50/p99,
    /// connections opened, the peak live count (sampled every 10 ms), and what
    /// the free-list retains afterwards. Run against an APFS clone of a real
    /// database, never the live file:
    ///
    /// ```text
    /// cp -c ~/.cairn/cairn.db* /tmp/probe/
    /// CAIRN_PROBE_DB=/tmp/probe/cairn.db cargo test --release -p cairn-db --lib \
    ///   -- --ignored --nocapture pool_read_load_probe
    /// ```
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    #[ignore = "needs a real database copy via CAIRN_PROBE_DB; run in release"]
    async fn pool_read_load_probe() {
        let path = std::env::var("CAIRN_PROBE_DB").expect("set CAIRN_PROBE_DB to an APFS clone");
        let secs: u64 = std::env::var("CAIRN_PROBE_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(20);
        let db = Arc::new(LocalDb::open(&path).await.unwrap());
        let nodes: Arc<Vec<ProbeNode>> = Arc::new(
            db.query_all(
                "SELECT p.id, p.key, i.number, e.seq, j.uri_segment, j.id FROM jobs j \
                 JOIN issues i ON j.issue_id = i.id JOIN projects p ON i.project_id = p.id \
                 JOIN executions e ON j.execution_id = e.id \
                 WHERE j.parent_job_id IS NULL AND j.uri_segment IS NOT NULL \
                 ORDER BY j.created_at DESC LIMIT 64",
                (),
                |row| {
                    Ok(ProbeNode {
                        project_id: row.text(0)?,
                        project_key: row.text(1)?,
                        number: row.i64(2)?,
                        exec_seq: row.i64(3)?,
                        segment: row.text(4)?,
                        job_id: row.text(5)?,
                    })
                },
            )
            .await
            .unwrap(),
        );
        assert!(
            !nodes.is_empty(),
            "the database copy has no addressable nodes"
        );
        println!(
            "pool_read_load_probe: {} nodes, {secs}s per run, idle cap {MAX_IDLE_CONNECTIONS}",
            nodes.len()
        );
        println!("mode n calls p50_us p99_us connections_created peak_live idle_after");

        for n in [8usize, 32, 64] {
            for pooled in [false, true] {
                let created_before = db.connections_created.load(Ordering::Relaxed);
                let deadline = Instant::now() + Duration::from_secs(secs);
                let peak = Arc::new(AtomicUsize::new(0));
                let sampler = {
                    let (db, peak) = (db.clone(), peak.clone());
                    tokio::spawn(async move {
                        while Instant::now() < deadline {
                            peak.fetch_max(db.live_connections(), Ordering::Relaxed);
                            sleep(Duration::from_millis(10)).await;
                        }
                    })
                };
                let mut readers = Vec::new();
                for reader in 0..n {
                    let (db, nodes) = (db.clone(), nodes.clone());
                    readers.push(tokio::spawn(async move {
                        let mut latencies = Vec::new();
                        let mut index = reader;
                        while Instant::now() < deadline {
                            let node = nodes[index % nodes.len()].clone();
                            index += n;
                            let started = Instant::now();
                            if pooled {
                                db.read(move |conn| {
                                    Box::pin(async move { probe_mix(conn, &node).await })
                                })
                                .await
                                .unwrap();
                            } else {
                                let conn = db.connect().await.unwrap();
                                conn.execute(db.concurrent_begin(), ()).await.unwrap();
                                probe_mix(&conn, &node).await.unwrap();
                                drop(conn);
                            }
                            latencies.push(started.elapsed());
                        }
                        latencies
                    }));
                }
                let mut latencies = Vec::new();
                for reader in readers {
                    latencies.extend(reader.await.unwrap());
                }
                sampler.await.unwrap();
                latencies.sort_unstable();
                println!(
                    "{} {n} {} {} {} {} {} {}",
                    if pooled { "B(read)" } else { "A(connect)" },
                    latencies.len(),
                    percentile(&latencies, 0.50).as_micros(),
                    percentile(&latencies, 0.99).as_micros(),
                    db.connections_created.load(Ordering::Relaxed) - created_before,
                    peak.load(Ordering::Relaxed),
                    db.idle_len(),
                );
                assert!(db.idle_len() <= MAX_IDLE_CONNECTIONS);
                assert_eq!(db.live_connections(), 0);
            }
        }
    }

    /// Awaiting a database call costs the caller's frame and state the call's
    /// holder and a pointer to its body, however much the body grows. A served
    /// path awaits these at nearly every layer, and in an unoptimized build
    /// each layer pays about twice the size of what it awaits, so a body built
    /// by value here is what cairn-core's served-path stack guard measures.
    #[tokio::test]
    async fn a_database_call_costs_its_caller_three_words() {
        const BUDGET: usize = 3 * std::mem::size_of::<usize>();
        let db = test_db().await.unwrap();
        let sql = String::from("PRAGMA user_version");
        let sizes = [
            ("connect", size_of_val(&db.connect())),
            (
                "read",
                size_of_val(&db.read(|_| Box::pin(async { Ok(()) }))),
            ),
            (
                "write",
                size_of_val(&db.write(|_| Box::pin(async { Ok(()) }))),
            ),
            (
                "exclusive",
                size_of_val(&db.exclusive(|_| Box::pin(async { Ok(()) }))),
            ),
            (
                "query_all",
                size_of_val(&db.query_all("SELECT 1", (), |row| row.i64(0))),
            ),
            (
                "query_opt",
                size_of_val(&db.query_opt("SELECT 1", (), |row| row.i64(0))),
            ),
            (
                "query_one",
                size_of_val(&db.query_one("SELECT 1", (), |row| row.i64(0))),
            ),
            ("query_text", size_of_val(&db.query_text("SELECT 'a'", ()))),
            (
                "query_opt_text",
                size_of_val(&db.query_opt_text("SELECT 'a'", ())),
            ),
            (
                "query_opt_i64",
                size_of_val(&db.query_opt_i64("SELECT 1", ())),
            ),
            ("execute", size_of_val(&db.execute("SELECT 1", ()))),
            (
                "execute_script",
                size_of_val(&db.execute_script("SELECT 1")),
            ),
            ("execute_batch", size_of_val(&db.execute_batch(&sql))),
            ("consume_query", size_of_val(&db.consume_query(&sql))),
        ];
        for (entry, size) in sizes {
            assert!(
                size <= BUDGET,
                "awaiting `{entry}` costs its caller {size} bytes, over {BUDGET}"
            );
        }
    }

    async fn test_db() -> DbResult<LocalDb> {
        let temp = tempdir()?;
        let path = temp.keep().join("cairn-turso-test.db");
        let db = LocalDb::open(path).await?;
        MigrationRunner::new(TEST_SCHEMA.to_vec()).run(&db).await?;
        Ok(db)
    }

    async fn query_i64(db: &LocalDb, sql: &'static str) -> DbResult<i64> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, ()).await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing integer row".to_string()))?;
                row.i64(0)
            })
        })
        .await
    }

    async fn query_text(db: &LocalDb, sql: &'static str) -> DbResult<String> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn.query(sql, ()).await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("missing text row".to_string()))?;
                row.text(0)
            })
        })
        .await
    }

    #[tokio::test]
    async fn query_helpers_map_rows_and_missing_rows() {
        let db = test_db().await.unwrap();
        db.execute(
            "INSERT INTO counters(id, value) VALUES (?1, ?2), (?3, ?4)",
            ("a", 1_i64, "b", 2_i64),
        )
        .await
        .unwrap();

        let values = db
            .query_all(
                "SELECT value FROM counters WHERE value > ?1 ORDER BY value ASC",
                (0_i64,),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(values, vec![1, 2]);

        let empty = db
            .query_all(
                "SELECT value FROM counters WHERE value > ?1",
                (10_i64,),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert!(empty.is_empty());

        let found = db
            .query_opt("SELECT value FROM counters WHERE id = ?1", ("a",), |row| {
                row.i64(0)
            })
            .await
            .unwrap();
        assert_eq!(found, Some(1));

        let missing = db
            .query_opt(
                "SELECT value FROM counters WHERE id = ?1",
                ("missing",),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(missing, None);

        let found_text = db
            .query_opt_text("SELECT id FROM counters WHERE id = ?1", ("a",))
            .await
            .unwrap();
        assert_eq!(found_text, Some("a".to_string()));

        let missing_text = db
            .query_opt_text("SELECT id FROM counters WHERE id = ?1", ("missing",))
            .await
            .unwrap();
        assert_eq!(missing_text, None);

        let found_integer = db
            .query_opt_i64("SELECT value FROM counters WHERE id = ?1", ("a",))
            .await
            .unwrap();
        assert_eq!(found_integer, Some(1));

        let missing_integer = db
            .query_opt_i64("SELECT value FROM counters WHERE id = ?1", ("missing",))
            .await
            .unwrap();
        assert_eq!(missing_integer, None);

        let required_text = db
            .query_text("SELECT id FROM counters WHERE id = ?1", ("a",))
            .await
            .unwrap();
        assert_eq!(required_text, Some("a".to_string()));

        let one = db
            .query_one("SELECT value FROM counters WHERE id = ?1", ("b",), |row| {
                row.i64(0)
            })
            .await
            .unwrap();
        assert_eq!(one, 2);

        let err = db
            .query_one(
                "SELECT value FROM counters WHERE id = ?1",
                ("missing",),
                |row| row.i64(0),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, DbError::Row(message) if message == "query_one returned no rows"));
    }

    #[tokio::test]
    async fn execute_script_runs_multiple_statements_in_write_transaction() {
        let db = test_db().await.unwrap();
        db.execute_script(
            "
            INSERT INTO counters(id, value) VALUES ('a', 1);
            INSERT INTO counters(id, value) VALUES ('b', 2);
            UPDATE counters SET value = value + 10 WHERE id = 'a';
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(&db, "SELECT SUM(value) FROM counters")
                .await
                .unwrap(),
            13
        );
    }

    #[tokio::test]
    async fn execute_returns_rows_affected_and_updates_rows() {
        let db = test_db().await.unwrap();
        let inserted = db
            .execute(
                "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                ("exec", 1_i64),
            )
            .await
            .unwrap();
        assert_eq!(inserted, 1);

        let updated = db
            .execute(
                "UPDATE counters SET value = ?1 WHERE id = ?2",
                (5_i64, "exec"),
            )
            .await
            .unwrap();
        assert_eq!(updated, 1);
        assert_eq!(
            query_i64(&db, "SELECT value FROM counters WHERE id = 'exec'")
                .await
                .unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn execute_retries_conflicting_commits() {
        let db = Arc::new(test_db().await.unwrap());
        db.execute(
            "INSERT INTO counters(id, value) VALUES ('shared-exec', 0)",
            (),
        )
        .await
        .unwrap();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                db.execute(
                    "UPDATE counters SET value = value + 1 WHERE id = 'shared-exec'",
                    (),
                )
                .await
            }));
        }

        for task in tasks {
            assert_eq!(task.await.unwrap().unwrap(), 1);
        }
        assert_eq!(
            query_i64(&db, "SELECT value FROM counters WHERE id = 'shared-exec'")
                .await
                .unwrap(),
            16
        );
    }

    #[tokio::test]
    async fn local_db_enables_mvcc_and_foreign_keys() {
        let db = test_db().await.unwrap();

        assert_eq!(
            query_text(&db, "PRAGMA journal_mode").await.unwrap(),
            "mvcc"
        );
        assert_eq!(query_i64(&db, "PRAGMA foreign_keys").await.unwrap(), 1);
    }

    async fn synced_memory_db() -> DbResult<LocalDb> {
        // A synced replica with bootstrapping disabled and no remote is purely
        // local-engine-backed, so it proves the `DbHandle::Synced` arm is
        // transparent to every query helper without needing a sync server. The
        // synced engine runs CDC (incompatible with MVCC), so it uses a plain
        // BEGIN rather than BEGIN CONCURRENT -- the test below pins that fact.
        let database = Arc::new(DbHandle::Synced(
            turso::sync::Builder::new_remote(":memory:")
                .bootstrap_if_empty(false)
                .build()
                .await?,
        ));
        let gate = Arc::new(ConnectionGate::new());
        let db = LocalDb {
            path: PathBuf::from(":memory:"),
            database,
            retry: RetryConfig::default(),
            commit_signal: Arc::new(Notify::new()),
            provider_admission_changes: watch::channel(0).0,
            instance: next_instance(),
            mutation_generation: AtomicU64::new(0),
            team: None,
            team_content_store: None,
            private_content_store: OnceLock::new(),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            checkpoint_in_engine: Arc::new(AtomicBool::new(false)),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(any(test, feature = "test-utils"))]
            write_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
            read_bound: READ_STATEMENT_BOUND,
            #[cfg(any(test, feature = "test-utils"))]
            history_source_override: Mutex::new(None),
            history_catalog_present: AtomicBool::new(false),
            history_evicted: AtomicBool::new(false),
        };
        db.configure().await?;
        MigrationRunner::new(TEST_SCHEMA.to_vec()).run(&db).await?;
        Ok(db)
    }

    #[tokio::test]
    async fn synced_handle_is_transparent_to_query_helpers() {
        let db = synced_memory_db().await.unwrap();
        assert!(db.is_synced());

        // The synced engine cannot run MVCC (CDC is incompatible), so its
        // journal mode is NOT mvcc; writes use a plain BEGIN instead.
        let journal = db
            .query_one("PRAGMA journal_mode", (), |row| row.text(0))
            .await
            .unwrap();
        assert_ne!(journal, "mvcc");

        // Writes route through the same write() helper as a local handle, but
        // under a plain BEGIN here (not BEGIN CONCURRENT, which needs MVCC).
        db.execute(
            "INSERT INTO counters(id, value) VALUES (?1, ?2), (?3, ?4)",
            ("a", 1_i64, "b", 2_i64),
        )
        .await
        .unwrap();

        let total = db
            .query_one("SELECT SUM(value) FROM counters", (), |row| row.i64(0))
            .await
            .unwrap();
        assert_eq!(total, 3);

        let one = db
            .query_one("SELECT value FROM counters WHERE id = ?1", ("b",), |row| {
                row.i64(0)
            })
            .await
            .unwrap();
        assert_eq!(one, 2);
    }

    #[test]
    fn crypto_provider_installs_and_client_config_builds() {
        // Regression guard for the `turso-sync-io` panic: with both `aws-lc-rs`
        // and `ring` compiled into the rustls tree, rustls' feature-based
        // provider auto-detection is ambiguous and panics when a TLS client is
        // built without a process default installed. `ensure_crypto_provider`
        // installs one; after it, building a `ClientConfig` — the same provider
        // resolution path turso's sync client takes via hyper-rustls
        // `with_native_roots()` — must succeed without panicking.
        install_crypto_provider();

        // A default provider is now installed process-wide.
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());

        // Building a ClientConfig exercises provider resolution; it would panic
        // in the ambiguous dual-provider tree if no default were installed.
        let _config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();

        // Idempotent: a second call is a no-op guarded by `Once` and never panics.
        install_crypto_provider();
    }

    #[tokio::test]
    async fn push_pull_error_on_local_database() {
        let db = test_db().await.unwrap();
        assert!(!db.is_synced());
        assert!(matches!(db.push().await.unwrap_err(), DbError::Internal(_)));
        assert!(matches!(db.pull().await.unwrap_err(), DbError::Internal(_)));
    }

    #[tokio::test]
    async fn migration_runner_applies_each_migration_once() {
        let db = test_db().await.unwrap();
        let runner = MigrationRunner::new(TEST_SCHEMA.to_vec());

        assert!(runner.run(&db).await.unwrap().is_empty());
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM cairn_schema_migrations")
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn concurrent_writes_retry_conflicting_commits() {
        let db = Arc::new(test_db().await.unwrap());
        db.execute("INSERT INTO counters(id, value) VALUES ('shared', 0)", ())
            .await
            .unwrap();

        let attempts = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let db = db.clone();
            let attempts = attempts.clone();
            tasks.push(tokio::spawn(async move {
                db.write(|conn| {
                    let attempts = attempts.clone();
                    Box::pin(async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        let mut rows = conn
                            .query("SELECT value FROM counters WHERE id = 'shared'", ())
                            .await?;
                        let row = rows
                            .next()
                            .await?
                            .ok_or_else(|| DbError::Row("missing counter row".to_string()))?;
                        let value = row.i64(0)?;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        conn.execute(
                            "UPDATE counters SET value = ?1 WHERE id = 'shared'",
                            (value + 1,),
                        )
                        .await?;
                        Ok(())
                    })
                })
                .await
            }));
        }

        for task in tasks {
            task.await.unwrap().unwrap();
        }

        assert_eq!(
            query_i64(&db, "SELECT value FROM counters WHERE id = 'shared'")
                .await
                .unwrap(),
            16
        );
        assert!(
            attempts.load(Ordering::SeqCst) > 16,
            "expected at least one optimistic retry under shared-row contention"
        );
    }

    #[tokio::test]
    async fn long_reader_does_not_block_unrelated_writer() {
        let db = test_db().await.unwrap();
        let reader = db.connect().await.unwrap();
        reader.execute("BEGIN CONCURRENT", ()).await.unwrap();
        let mut rows = reader
            .query("SELECT COUNT(*) FROM counters", ())
            .await
            .unwrap();
        assert!(rows.next().await.unwrap().is_some());
        drop(rows);

        db.execute(
            "INSERT INTO unrelated_writes(id, value) VALUES ('writer-1', 'ok')",
            (),
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM unrelated_writes")
                .await
                .unwrap(),
            1
        );
        reader.execute("ROLLBACK", ()).await.unwrap();
    }

    #[tokio::test]
    async fn triggers_populate_search_outbox_only_for_committed_writes() {
        let db = test_db().await.unwrap();

        db.execute(
            "INSERT INTO issues(id, project_id, title, body, created_at, updated_at)
             VALUES ('issue-1', 'project-1', 'Turso search', 'Committed issue', 1, 1)",
            (),
        )
        .await
        .unwrap();

        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM search_outbox WHERE status = 'pending'"
            )
            .await
            .unwrap(),
            1
        );

        let error = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO issues(id, project_id, title, body, created_at, updated_at)
                         VALUES ('rolled-back', 'project-1', 'Rollback', 'Should not index', 2, 2)",
                        (),
                    )
                    .await?;
                    Err::<(), DbError>(DbError::internal("force rollback"))
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(error, DbError::Internal(_)));

        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM issues WHERE id = 'rolled-back'")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM search_outbox")
                .await
                .unwrap(),
            1
        );
    }

    /// CAIRN-1133 Phase 0 (in-process arm): two independent `LocalDb` instances
    /// (separate `turso::Database` handles) pointed at the same file must
    /// coordinate writes through busy_timeout + optimistic retry without losing
    /// updates. The cross-process arm lives in `examples/concurrent_db_probe.rs`.
    #[tokio::test]
    async fn two_local_db_instances_share_one_file_without_lost_updates() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("shared-handles.turso.db");

        // Instance A seeds the schema + shared row.
        let db_a = LocalDb::open(&path).await.unwrap();
        MigrationRunner::new(TEST_SCHEMA.to_vec())
            .run(&db_a)
            .await
            .unwrap();
        db_a.execute("INSERT INTO counters(id, value) VALUES ('shared', 0)", ())
            .await
            .unwrap();

        // Instance B opens the *same file* via a fresh Database handle.
        let db_b = LocalDb::open(&path).await.unwrap();

        let db_a = Arc::new(db_a);
        let db_b = Arc::new(db_b);
        let per_handle = 25;
        let mut tasks = Vec::new();
        for handle in [db_a.clone(), db_b.clone()] {
            for _ in 0..per_handle {
                let handle = handle.clone();
                tasks.push(tokio::spawn(async move {
                    handle
                        .write(|conn| {
                            Box::pin(async move {
                                let mut rows = conn
                                    .query("SELECT value FROM counters WHERE id = 'shared'", ())
                                    .await?;
                                let row = rows.next().await?.ok_or_else(|| {
                                    DbError::Row("missing counter row".to_string())
                                })?;
                                let value = row.i64(0)?;
                                conn.execute(
                                    "UPDATE counters SET value = ?1 WHERE id = 'shared'",
                                    (value + 1,),
                                )
                                .await?;
                                Ok(())
                            })
                        })
                        .await
                }));
            }
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }

        // Read back through the *other* handle to confirm cross-handle visibility.
        let total = query_i64(&db_b, "SELECT value FROM counters WHERE id = 'shared'")
            .await
            .unwrap();
        assert_eq!(
            total,
            (per_handle * 2) as i64,
            "updates lost across handles"
        );
    }

    #[tokio::test]
    async fn db_set_paths_names_the_log_turso_writes_whatever_the_path() {
        // Opened through the engine directly, because `LocalDb::open` refuses
        // every name here but the first.
        let dir = tempdir().unwrap();
        for name in ["set.db", "cairn.db.probe", "extensionless"] {
            let path = dir.path().join(name);
            let database = Builder::new_local(&path.to_string_lossy())
                .build()
                .await
                .unwrap();
            let conn = database.connect().unwrap();
            let mut rows = conn
                .query("PRAGMA journal_mode = 'mvcc'", ())
                .await
                .unwrap();
            while rows.next().await.unwrap().is_some() {}
            drop(rows);
            conn.execute("CREATE TABLE t (x INTEGER)", ())
                .await
                .unwrap();
            conn.execute("INSERT INTO t VALUES (1)", ()).await.unwrap();

            // Nothing has folded the log, so the commit lives only there.
            let [_, _, log] = db_set_paths(&path);
            let bytes = std::fs::metadata(&log).map(|meta| meta.len()).unwrap_or(0);
            assert!(
                bytes > 0,
                "{name}: the committed log is not at {}",
                log.display()
            );
        }
    }

    #[tokio::test]
    async fn open_refuses_a_path_that_does_not_end_in_db() {
        let dir = tempdir().unwrap();
        for name in ["cairn.db.vacuum-staged", "db", "cairn.DB"] {
            let error = LocalDb::open(dir.path().join(name))
                .await
                .err()
                .unwrap_or_else(|| panic!("{name} opened"));
            assert!(error.to_string().contains("must end in .db"), "{error}");
        }
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            0,
            "a refused open left files behind"
        );
        LocalDb::open(":memory:").await.unwrap();
    }

    #[tokio::test]
    async fn a_local_db_reports_the_log_its_commits_land_in() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.turso.db");
        let db = LocalDb::open(&path).await.unwrap();
        db.execute_batch("CREATE TABLE t (x INTEGER); INSERT INTO t VALUES (1)")
            .await
            .unwrap();

        let [_, _, log] = db_set_paths(&path);
        assert_eq!(log, dir.path().join("live.turso.db-log"));
        assert!(db.logical_log_bytes() > 0);
        assert_eq!(
            db.logical_log_bytes(),
            std::fs::metadata(&log).unwrap().len()
        );
    }

    #[test]
    fn move_db_set_relocates_every_present_member_and_leaves_backup_intact() {
        let dir = tempdir().unwrap();
        let live = dir.path().join("live.turso.db");
        let staged = dir.path().join("staged.turso.db");
        let backup = dir.path().join("live.turso.vacuum-backup.db");

        // A full live set; a staged set missing its -log sidecar.
        std::fs::write(&live, b"live-db").unwrap();
        std::fs::write(dir.path().join("live.turso.db-wal"), b"live-wal").unwrap();
        std::fs::write(dir.path().join("live.turso.db-log"), b"live-log").unwrap();
        std::fs::write(&staged, b"staged-db").unwrap();
        std::fs::write(dir.path().join("staged.turso.db-wal"), b"staged-wal").unwrap();

        // live -> backup moves all three present members.
        move_db_set(&live, &backup).unwrap();
        assert!(!live.exists());
        assert_eq!(std::fs::read(&backup).unwrap(), b"live-db");
        assert_eq!(
            std::fs::read(dir.path().join("live.turso.vacuum-backup.db-wal")).unwrap(),
            b"live-wal"
        );
        assert_eq!(
            std::fs::read(dir.path().join("live.turso.vacuum-backup.db-log")).unwrap(),
            b"live-log"
        );

        // staged -> live moves only the two present members; no -log appears.
        move_db_set(&staged, &live).unwrap();
        assert_eq!(std::fs::read(&live).unwrap(), b"staged-db");
        assert_eq!(
            std::fs::read(dir.path().join("live.turso.db-wal")).unwrap(),
            b"staged-wal"
        );
        assert!(!dir.path().join("live.turso.db-log").exists());

        // The backup set is untouched throughout.
        assert!(backup.exists());
    }

    #[test]
    fn move_db_set_refuses_to_clobber_existing_destination() {
        let dir = tempdir().unwrap();
        let from = dir.path().join("from.turso.db");
        let to = dir.path().join("to.turso.db");
        std::fs::write(&from, b"from").unwrap();
        std::fs::write(&to, b"to").unwrap();

        let err = move_db_set(&from, &to).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        // Nothing moved: source intact, destination unchanged.
        assert_eq!(std::fs::read(&from).unwrap(), b"from");
        assert_eq!(std::fs::read(&to).unwrap(), b"to");
    }

    /// `mvcc_checkpoint_threshold` writes through to the store every connection
    /// shares, so `configure()` setting it once covers the whole handle. That is
    /// the whole reason it lives there rather than in `connect` alongside
    /// `cache_size`, which is genuinely per-connection — a distinction this file
    /// has had to relearn before. Asserting it from a connection created AFTER
    /// `configure()` ran is what states the scope directly.
    #[tokio::test]
    async fn disabling_auto_checkpoint_applies_to_every_connection() {
        let db = test_db().await.unwrap();

        let fresh = db.connect().await.unwrap();
        let mut rows = fresh
            .query("PRAGMA mvcc_checkpoint_threshold", ())
            .await
            .unwrap();
        let threshold = rows.next().await.unwrap().unwrap().i64(0).unwrap();

        assert_eq!(
            threshold, CHECKPOINT_THRESHOLD_DISABLED,
            "a connection opened after configure() must still see auto-checkpoint disabled; \
             if this reads back the engine default, the threshold has become per-connection \
             and every non-configuring connection is re-arming the commit-path checkpoint"
        );
    }

    /// The engine's own default `mvcc_checkpoint_threshold`, in bytes.
    ///
    /// Named here only so the test below can write past it. Nothing in Cairn
    /// depends on the value; if a future engine pin changes it, this test still
    /// states the same thing as long as the constant follows.
    const ENGINE_DEFAULT_CHECKPOINT_THRESHOLD: u64 = 4_120_000;

    /// The behavioural half of `disabling_auto_checkpoint_applies_to_every_connection`:
    /// the pragma is set, and committing past the engine's threshold genuinely
    /// does not checkpoint.
    ///
    /// This is the assertion that would catch the pragma being dropped even if
    /// the reading test were also changed. Nothing else holds a transaction open
    /// here, so an enabled auto-checkpoint would not merely *attempt* at these
    /// sizes — it would succeed, and truncate the log out from under the final
    /// assertion. A log that keeps growing past the threshold is proof the commit
    /// path is no longer checkpointing.
    #[tokio::test]
    async fn commits_past_the_engine_threshold_no_longer_checkpoint() {
        let db = test_db().await.unwrap();
        let payload = "x".repeat(64 * 1024);

        let mut written = 0;
        while db.logical_log_bytes() <= ENGINE_DEFAULT_CHECKPOINT_THRESHOLD {
            db.execute(
                "INSERT INTO unrelated_writes(id, value) VALUES (?1, ?2)",
                (format!("grow-{written}"), payload.clone()),
            )
            .await
            .unwrap();
            written += 1;
            assert!(
                written < 4_096,
                "wrote {written} rows without the logical log passing {ENGINE_DEFAULT_CHECKPOINT_THRESHOLD} bytes"
            );
        }

        // Cairn's own checkpoint still folds it, so the log is bounded by
        // maintenance rather than by nothing at all.
        let report = db
            .checkpoint(4, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();
        assert!(report.succeeded(), "{report}");
        assert!(
            db.logical_log_bytes() < ENGINE_DEFAULT_CHECKPOINT_THRESHOLD,
            "an owned checkpoint must still fold a log the engine no longer touches"
        );
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM unrelated_writes")
                .await
                .unwrap(),
            i64::from(written)
        );
    }

    #[tokio::test]
    async fn checkpoint_folds_the_log_and_reports_what_it_cost() {
        let db = test_db().await.unwrap();
        for i in 0..200 {
            db.execute(
                "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                (format!("fold-{i}"), i64::from(i)),
            )
            .await
            .unwrap();
        }
        let before = db.logical_log_bytes();
        assert!(before > 0, "writes should have grown the logical log");

        let report = db
            .checkpoint(4, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();

        assert!(
            report.succeeded(),
            "uncontended checkpoint failed: {report}"
        );
        assert_eq!(
            report.attempts, 1,
            "a checkpoint with no transaction open anywhere should win first try: {report}"
        );
        assert_eq!(report.log_bytes_before, before);
        assert!(
            report.log_bytes_after < before,
            "checkpoint reported success without folding the log: {report}"
        );
        assert_eq!(
            db.logical_log_bytes(),
            report.log_bytes_after,
            "the report's after-size must be the size on disk"
        );

        // The rows survive the fold, and the database is still coherent.
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM counters WHERE id LIKE 'fold-%'")
                .await
                .unwrap(),
            200
        );
        assert_eq!(
            query_text(&db, "PRAGMA integrity_check").await.unwrap(),
            "ok"
        );
    }

    /// Measure a checkpoint against a copy of a real, large database.
    ///
    /// Ignored by default because it needs a database this repository cannot
    /// carry. Point it at a COPY of the whole three-file set — `{db, -wal, -log}`,
    /// never the `.db` alone — and run it to learn what a backlog fold actually
    /// costs before trusting one to run at startup:
    ///
    /// ```text
    /// CAIRN_CHECKPOINT_PROBE_DB=/tmp/copy/probe.db \
    ///   cargo test -p cairn-db checkpoint_probe -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "needs a real database copy via CAIRN_CHECKPOINT_PROBE_DB"]
    async fn checkpoint_probe_against_a_real_database_copy() {
        let path = std::env::var("CAIRN_CHECKPOINT_PROBE_DB")
            .expect("set CAIRN_CHECKPOINT_PROBE_DB to a copy of a real database set");
        let opened = Instant::now();
        let db = LocalDb::open(&path).await.unwrap();
        println!("opened in {} ms", opened.elapsed().as_millis());

        let report = db
            .checkpoint(1, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();
        println!("checkpoint: {report}");

        let checked = Instant::now();
        let integrity = query_text(&db, "PRAGMA integrity_check").await.unwrap();
        println!(
            "integrity_check: {integrity} ({} ms)",
            checked.elapsed().as_millis()
        );
        assert!(report.succeeded(), "{report}");
        assert_eq!(integrity, "ok");
    }

    #[test]
    fn a_drain_budget_grows_with_what_is_open_and_stays_inside_its_bounds() {
        let drain = DrainBudget {
            floor: Duration::from_millis(500),
            per_open: Duration::from_millis(50),
            ceiling: Duration::from_millis(2_500),
        };
        assert_eq!(drain.for_open(0), Duration::from_millis(500));
        assert_eq!(drain.for_open(10), Duration::from_millis(1_000));
        assert_eq!(drain.for_open(40), Duration::from_millis(2_500));
        assert_eq!(drain.for_open(usize::MAX), Duration::from_millis(2_500));
        let mut previous = Duration::ZERO;
        for open in 0..100 {
            let budget = drain.for_open(open);
            assert!(budget >= previous, "for_open must be monotone");
            assert!(budget >= drain.floor && budget <= drain.ceiling);
            previous = budget;
        }

        let fixed = DrainBudget::fixed(Duration::from_millis(300));
        assert_eq!(fixed.for_open(0), Duration::from_millis(300));
        assert_eq!(fixed.for_open(1_000), Duration::from_millis(300));

        // An inverted ceiling cannot panic `clamp`; the floor wins.
        let inverted = DrainBudget {
            floor: Duration::from_millis(500),
            per_open: Duration::from_millis(50),
            ceiling: Duration::from_millis(100),
        };
        assert_eq!(inverted.for_open(10), Duration::from_millis(500));
    }

    /// The production shape behind tenet 1260: ten transactions open, none of
    /// them pathological, all finishing inside a second. A fixed half-second
    /// drain loses to them every time; a budget that scales with the count
    /// waits them out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_drain_that_scales_with_still_open_outlasts_ten_short_holders() {
        async fn hold_ten_for(db: &LocalDb, hold: Duration) -> tokio::task::JoinHandle<()> {
            let mut holders = Vec::new();
            for _ in 0..10 {
                let conn = db.connect().await.unwrap();
                conn.execute(db.concurrent_begin(), ()).await.unwrap();
                holders.push(conn);
            }
            tokio::spawn(async move {
                sleep(hold).await;
                for conn in holders.drain(..) {
                    conn.execute("ROLLBACK", ()).await.unwrap();
                }
            })
        }

        let db = test_db().await.unwrap();
        db.execute("INSERT INTO counters(id, value) VALUES ('a', 1)", ())
            .await
            .unwrap();

        let releasing = hold_ten_for(&db, Duration::from_millis(800)).await;
        let fixed = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(500)),
            )
            .await
            .unwrap();
        releasing.await.unwrap();
        assert!(!fixed.drain.drained, "{fixed}");
        assert_eq!(fixed.drain.open_at_close, 10, "{fixed}");
        assert_eq!(fixed.drain.budget, Duration::from_millis(500), "{fixed}");
        assert_eq!(db.live_connections(), 0);

        let releasing = hold_ten_for(&db, Duration::from_millis(800)).await;
        let scaled = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget {
                    floor: Duration::from_millis(500),
                    per_open: Duration::from_millis(100),
                    ceiling: Duration::from_secs(3),
                },
            )
            .await
            .unwrap();
        releasing.await.unwrap();
        assert!(scaled.drain.drained, "{scaled}");
        assert_eq!(scaled.drain.open_at_close, 10, "{scaled}");
        assert_eq!(
            scaled.drain.budget,
            Duration::from_millis(1_500),
            "{scaled}"
        );
        assert!(scaled.succeeded(), "{scaled}");
        let line = scaled.to_string();
        assert!(
            line.contains("budget_ms=1500") && line.contains("open_at_close=10"),
            "{line}"
        );
        assert_eq!(db.live_connections(), 0);
    }

    // ========================================================================
    // The connection gate
    //
    // These encode facts that each took real work to establish and that the
    // design is wrong without. Several are the CAIRN-4167 investigation probes
    // kept as tests: they answer in seconds a question that otherwise costs a
    // session.
    // ========================================================================

    /// The whole design in one test: a checkpoint wins on its FIRST attempt
    /// despite transactions being open when the pass begins.
    ///
    /// This is the production condition. Ten consecutive real passes made 1,000
    /// attempts over 6,651 seconds and never once found the process
    /// transaction-free — with enough short overlapping transactions the union is
    /// never empty, so waiting for a quiet instant cannot work at any retry
    /// count. The pass here begins with three transactions open and more work
    /// arriving, and wins anyway, because it stops new work and waits the open
    /// ones out instead of sampling for a gap that is not there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_checkpoint_wins_by_waiting_out_the_transactions_already_open() {
        let db = Arc::new(test_db().await.unwrap());
        for i in 0..200 {
            db.execute(
                "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                (format!("gated-{i}"), i64::from(i)),
            )
            .await
            .unwrap();
        }

        // Three transactions open before the pass starts. Under the old design
        // this alone lost every attempt for as long as they lived.
        let mut holders = Vec::new();
        for _ in 0..3 {
            let conn = db.connect().await.unwrap();
            conn.execute(db.concurrent_begin(), ()).await.unwrap();
            holders.push(conn);
        }
        assert_eq!(db.live_connections(), 3);

        // They end shortly after the pass begins, as ordinary short transactions
        // do.
        let finishing = tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            for conn in holders.drain(..) {
                conn.execute("ROLLBACK", ()).await.unwrap();
            }
        });

        // Work that ARRIVES mid-pass, finds the gate shut, and must still be
        // served rather than failed.
        let arriving = {
            let db = db.clone();
            tokio::spawn(async move {
                sleep(Duration::from_millis(50)).await;
                db.query_one("SELECT COUNT(*) FROM counters", (), |row| row.i64(0))
                    .await
                    .unwrap()
            })
        };

        let report = db
            .checkpoint(ATTEMPTS_FOR_TESTS, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();

        finishing.await.unwrap();
        let counted = tokio::time::timeout(Duration::from_secs(10), arriving)
            .await
            .expect("a caller that arrived while the gate was shut was never served")
            .unwrap();
        assert_eq!(counted, 200);

        assert!(report.drain.drained, "the gate did not empty: {report}");
        assert!(
            report.drain.waited >= Duration::from_millis(50),
            "the pass should have WAITED for the open transactions rather than \
             finding the database already quiet: {report}"
        );
        assert!(report.succeeded(), "{report}");
        assert_eq!(
            report.attempts, 1,
            "behind a drained gate the first attempt must win; retrying is no \
             longer how this finds a quiet instant: {report}"
        );
        assert!(
            report.log_bytes_after < report.log_bytes_before,
            "reported success without folding the log: {report}"
        );
        assert_eq!(db.live_connections(), 0);
    }

    #[tokio::test]
    async fn checkpoint_does_not_lose_committed_rows() {
        let db = test_db().await.unwrap();
        db.execute(
            "INSERT INTO counters(id, value) VALUES ('checkpoint', 7)",
            (),
        )
        .await
        .unwrap();

        db.consume_query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .unwrap();

        assert_eq!(
            query_i64(&db, "SELECT value FROM counters WHERE id = 'checkpoint'")
                .await
                .unwrap(),
            7
        );
    }

    /// The regression test for the mistake this change was nearly built on.
    ///
    /// The plan for CAIRN-4167 held that every transaction path funnels through
    /// `checkout()`, so gating the pool would be enough. It is not: `connect()`
    /// hands out OUT-OF-POOL connections, and a transaction opened on one can
    /// be held as long as its owner likes (resource reads did exactly that
    /// until cairn/5502 moved them into the pool). A pool-only gate would have
    /// drained to zero, declared a quiet instant, and lost the lock.
    ///
    /// So: a transaction on an out-of-pool connection must be visible to the
    /// drain, and must be NAMED when it outlasts the budget.
    #[tokio::test]
    async fn an_out_of_pool_read_transaction_is_visible_to_the_drain() {
        let db = test_db().await.unwrap();
        db.execute("INSERT INTO counters(id, value) VALUES ('a', 1)", ())
            .await
            .unwrap();

        // Connect outside the pool, BEGIN, and hold it.
        let connect_line = line!() + 1;
        let reader = db.connect().await.unwrap();
        reader.execute(db.concurrent_begin(), ()).await.unwrap();
        let mut rows = reader
            .query("SELECT COUNT(*) FROM counters", ())
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap();
        drop(rows);

        assert_eq!(
            db.live_connections(),
            1,
            "an out-of-pool read transaction must count toward the drain"
        );

        let report = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(50)),
            )
            .await
            .unwrap();

        assert!(
            !report.drain.drained,
            "the drain claimed to empty while a read transaction was open: {report}"
        );
        assert_eq!(report.drain.still_open, 1, "{report}");
        let (caller, _age) = report
            .drain
            .oldest
            .expect("a drain that did not finish must name what it was waiting on");
        assert_eq!(
            (caller.file(), caller.line()),
            (file!(), connect_line),
            "the report must say WHERE the blocking transaction came from, or the \
             follow-up is another investigation instead of one log line"
        );
        assert!(
            report
                .to_string()
                .contains(&format!("oldest={}:{connect_line}/", file!())),
            "{report}"
        );
        assert!(!report.succeeded(), "{report}");

        // And the gate reopened regardless, so ordinary work continues.
        reader.execute("ROLLBACK", ()).await.unwrap();
        drop(reader);
        assert_eq!(db.live_connections(), 0);
        assert_eq!(
            query_i64(&db, "SELECT COUNT(*) FROM counters")
                .await
                .unwrap(),
            1
        );
    }

    /// A statement that never ends on its own: a recursive count with no bound.
    /// It steps inside the engine without yielding, which is the 10-26 shape.
    const RUNAWAY_COUNT: &str =
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) SELECT count(*) FROM c";

    /// The pooled-read bound the tests below run under.
    const TEST_READ_BOUND: Duration = Duration::from_millis(200);

    fn read_bound_exceeded(error: &DbError) -> (Caller, Duration, Arc<str>) {
        match error {
            DbError::ReadBoundExceeded {
                caller,
                elapsed,
                statement,
                ..
            } => (*caller, *elapsed, statement.clone()),
            other => panic!("expected ReadBoundExceeded, got {other:?}"),
        }
    }

    async fn assert_checkpoint_wins(db: &LocalDb) {
        let report = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(200)),
            )
            .await
            .unwrap();
        assert!(report.drain.drained && report.succeeded(), "{report}");
    }

    /// Dropping a read part-way releases its gate slot AND ends its MVCC
    /// transaction, so cancelling a read can never leak what a checkpoint waits
    /// on. A checkpoint winning is the proof of the second half: a slot alone
    /// could be released while the engine still held the transaction.
    #[tokio::test]
    async fn a_dropped_read_future_releases_its_slot_and_its_transaction() {
        let db = test_db().await.unwrap();
        db.execute("INSERT INTO counters(id, value) VALUES ('a', 1)", ())
            .await
            .unwrap();
        {
            let read = db.read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM counters", ()).await?;
                    rows.next().await?;
                    drop(rows);
                    std::future::pending::<()>().await;
                    Ok(())
                })
            });
            let mut read = std::pin::pin!(read);
            assert!(futures_util::poll!(read.as_mut()).is_pending());
            assert_eq!(db.live_connections(), 1);
        }
        assert_eq!(db.live_connections(), 0);
        assert_checkpoint_wins(&db).await;
    }

    /// The tenet's case: a pooled read statement stepping past the bound is
    /// interrupted by the engine, fails naming its caller and its SQL, and
    /// leaves nothing behind for the checkpoint to wait on. Multi-threaded
    /// because the statement never yields the worker it runs on.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_runaway_pooled_read_is_interrupted_and_named() {
        let db = test_db().await.unwrap().with_read_bound(TEST_READ_BOUND);

        let line = line!() + 1;
        let error = db.query_one(RUNAWAY_COUNT, (), |row| row.i64(0)).await;
        let (caller, elapsed, statement) = read_bound_exceeded(&error.unwrap_err());
        assert_eq!((caller.file(), caller.line()), (file!(), line));
        assert!(elapsed < Duration::from_secs(10), "{elapsed:?}");
        assert!(
            statement.contains("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL"),
            "{statement}"
        );
        assert_eq!(db.live_connections(), 0);
        assert_checkpoint_wins(&db).await;

        // Through a transaction closure, the label is the closure's statement.
        let error = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(RUNAWAY_COUNT, ()).await?;
                    rows.next().await?;
                    Ok(())
                })
            })
            .await;
        let (caller, _, statement) = read_bound_exceeded(&error.unwrap_err());
        assert_eq!(caller.file(), file!());
        assert!(
            statement.starts_with(file!()) && statement.contains("WITH RECURSIVE"),
            "{statement}"
        );
        assert_eq!(db.live_connections(), 0);
        assert_checkpoint_wins(&db).await;
    }

    /// A read that parks between statements never trips the per-statement
    /// deadline, so the outer timeout ends it, and dropping it rolls back.
    #[tokio::test]
    async fn a_parked_pooled_read_is_retired_at_the_bound() {
        let db = test_db().await.unwrap().with_read_bound(TEST_READ_BOUND);
        let started = Instant::now();
        let error = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query("SELECT COUNT(*) FROM counters", ()).await?;
                    rows.next().await?;
                    drop(rows);
                    std::future::pending::<()>().await;
                    Ok(())
                })
            })
            .await;
        let (_, elapsed, statement) = read_bound_exceeded(&error.unwrap_err());
        let deadline = TEST_READ_BOUND + PARKED_READ_SLACK;
        assert!(elapsed >= deadline, "{elapsed:?}");
        assert!(started.elapsed() < deadline + Duration::from_secs(5));
        assert!(
            statement.contains("SELECT COUNT(*) FROM counters"),
            "{statement}"
        );
        assert_eq!(db.live_connections(), 0);
        assert_checkpoint_wins(&db).await;
    }

    /// What the engine stopped is not handed to the next caller.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_interrupted_connection_is_not_returned_to_the_pool() {
        let db = test_db().await.unwrap().with_read_bound(TEST_READ_BOUND);
        db.query_one("SELECT 1", (), |row| row.i64(0))
            .await
            .unwrap();
        let before = db.connections_created.load(Ordering::Relaxed);

        let error = db.query_one(RUNAWAY_COUNT, (), |row| row.i64(0)).await;
        read_bound_exceeded(&error.unwrap_err());
        db.query_one("SELECT 1", (), |row| row.i64(0))
            .await
            .unwrap();

        assert_eq!(
            db.connections_created.load(Ordering::Relaxed),
            before + 1,
            "the interrupted connection must be retired, so the next checkout opens a fresh one"
        );
    }

    /// The bound is for pooled READS. A write, or a `connect` connection, on
    /// which a statement runs well past it still succeeds, including a write on
    /// the very connection a bounded read just used. The statement is sized
    /// against this machine first so the test cannot pass by being fast.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_and_connect_are_not_bounded() {
        let db = test_db().await.unwrap().with_read_bound(TEST_READ_BOUND);
        let count_to = |n: u64| {
            format!(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {n}) \
                 SELECT count(*) FROM c"
            )
        };

        let unbounded = db.connect().await.unwrap();
        let mut n = 50_000u64;
        let slow = loop {
            let sql = count_to(n);
            let started = Instant::now();
            let mut rows = unbounded.query(&sql, ()).await.unwrap();
            rows.next().await.unwrap().unwrap();
            let took = started.elapsed();
            if took > TEST_READ_BOUND * 3 || n >= 1 << 28 {
                break (n, took);
            }
            n *= 2;
        };
        assert!(
            slow.1 > TEST_READ_BOUND * 3,
            "no statement ran past the bound on a connect connection: {slow:?}"
        );
        drop(unbounded);

        db.query_one("SELECT 1", (), |row| row.i64(0))
            .await
            .unwrap();
        let pooled = db.connections_created.load(Ordering::Relaxed);
        let sql = count_to(slow.0);
        let started = Instant::now();
        let counted = db
            .write(|conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let mut rows = conn.query(&sql, ()).await?;
                    let row = rows.next().await?.expect("count row");
                    row.i64(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(counted, i64::try_from(slow.0).unwrap());
        assert!(
            started.elapsed() > TEST_READ_BOUND,
            "{:?}",
            started.elapsed()
        );
        assert_eq!(
            db.connections_created.load(Ordering::Relaxed),
            pooled,
            "the write must have reused the connection the bounded read used"
        );
    }

    /// The gate watch's source: each overdue holder once, then only again
    /// after the re-warn interval, with who holds it and what it ran last.
    #[tokio::test]
    async fn overdue_holders_report_once_then_after_the_rewarn_interval() {
        let db = test_db().await.unwrap();
        let connect_line = line!() + 1;
        let reader = db.connect().await.unwrap();
        reader.execute(db.concurrent_begin(), ()).await.unwrap();
        let threshold = Duration::from_millis(10);
        let rewarn = Duration::from_millis(300);
        assert!(db
            .overdue_holders(Duration::from_secs(60), rewarn)
            .is_empty());

        sleep(Duration::from_millis(30)).await;
        let first = db.overdue_holders(threshold, rewarn);
        assert_eq!(first.len(), 1, "{first:?}");
        assert_eq!(first[0].kind, HolderKind::Connect);
        assert_eq!(
            (first[0].caller.file(), first[0].caller.line()),
            (file!(), connect_line)
        );
        assert!(first[0].statement.contains("BEGIN"), "{:?}", first[0]);
        assert!(db.overdue_holders(threshold, rewarn).is_empty());

        sleep(rewarn + Duration::from_millis(50)).await;
        assert_eq!(db.overdue_holders(threshold, rewarn).len(), 1);

        drop(reader);
        assert!(db.overdue_holders(threshold, Duration::ZERO).is_empty());
    }

    #[tokio::test]
    async fn generic_query_rows_preserve_the_caller_and_in_flight_guard() {
        use crate::storage::QueryConn;

        #[track_caller]
        fn forward<'a, C: QueryConn + ?Sized>(
            conn: &'a C,
            sql: &'a str,
        ) -> impl Future<Output = DbResult<crate::storage::QueryRows>> + Send + 'a {
            conn.query_rows(sql, ())
        }

        let db = test_db().await.unwrap();
        let conn = db.connect().await.unwrap();
        let line = line!() + 1;
        let pending = forward(&conn, "SELECT 7 AS value UNION ALL SELECT 8");
        assert_eq!(
            &*conn.slot.holder.statement(),
            format!("{}:{line} SELECT 7 AS value UNION ALL SELECT 8", file!())
        );
        assert!(conn.slot.holder.activity().executing);
        let mut rows = pending.await.unwrap();
        assert_eq!(rows.column_names(), vec!["value".to_string()]);
        assert_eq!(rows.next().await.unwrap().unwrap().i64(0).unwrap(), 7);
        assert!(conn.slot.holder.activity().executing);
        drop(rows);
        assert!(!conn.slot.holder.activity().executing);

        let line = line!() + 1;
        let pending = crate::storage::query_opt_i64_conn(&conn, "SELECT 9", ());
        assert_eq!(
            &*conn.slot.holder.statement(),
            format!("{}:{line} SELECT 9", file!())
        );
        drop(pending);
        assert!(!conn.slot.holder.activity().executing);
    }

    /// A deliberately raw helper for the Deref regression tests.
    async fn count_counters(conn: &Connection) -> i64 {
        let mut rows = conn
            .query("SELECT COUNT(*) FROM counters", ())
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap().i64(0).unwrap()
    }

    /// `Deref` is `#[track_caller]`, and that reaches through the coercions
    /// the compiler inserts: passing `&conn` to a `&Connection` helper, and
    /// calling a `Connection` method on a `TrackedConnection`, both name the
    /// line that did it rather than this file's `deref`.
    #[tokio::test]
    async fn deref_records_the_coercion_site() {
        let db = test_db().await.unwrap();
        let conn = db.connect().await.unwrap();

        let line = line!() + 1;
        count_counters(&conn).await;
        assert_eq!(
            &*conn.slot.holder.statement(),
            format!("{}:{line} (raw connection; sql not captured)", file!())
        );

        let line = line!() + 1;
        let _ = conn.is_autocommit();
        assert_eq!(
            &*conn.slot.holder.statement(),
            format!("{}:{line} (raw connection; sql not captured)", file!())
        );

        // A tracked statement takes the label back, with its SQL.
        let line = line!() + 1;
        conn.execute("SELECT 1", ()).await.unwrap();
        assert_eq!(
            &*conn.slot.holder.statement(),
            format!("{}:{line} SELECT 1", file!())
        );
    }

    /// The 10-29 shape: a `connect` holder that began a transaction, ran its
    /// statements through a `&Connection` helper, and then waited on something
    /// else. The watch names the helper's line rather than the BEGIN, says the
    /// holder is parked in a transaction, and does not touch it, because
    /// nothing in the engine can.
    #[tokio::test]
    async fn a_parked_connect_holder_is_reported_parked_and_not_cut() {
        let db = test_db().await.unwrap().with_read_bound(TEST_READ_BOUND);
        db.execute("INSERT INTO counters(id, value) VALUES ('a', 1)", ())
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute(db.concurrent_begin(), ()).await.unwrap();
        let line = line!() + 1;
        assert_eq!(count_counters(&conn).await, 1);

        sleep(TEST_READ_BOUND * 2).await;
        let held = db.overdue_holders(Duration::ZERO, Duration::ZERO);
        assert_eq!(held.len(), 1, "{held:?}");
        assert_eq!(held[0].kind, HolderKind::Connect);
        assert_eq!(
            held[0].activity,
            HolderActivity {
                executing: false,
                in_transaction: Some(true),
            }
        );
        let site = format!("{}:{line} (raw connection", file!());
        assert!(held[0].statement.starts_with(&site), "{:?}", held[0]);
        let described = held[0].describe();
        assert!(
            described.starts_with(&format!("parked in an open transaction since {site}")),
            "{described}"
        );
        assert!(described.contains("cannot be cut"), "{described}");

        assert_eq!(count_counters(&conn).await, 1, "the holder was cut");
        conn.execute("ROLLBACK", ()).await.unwrap();
        let held = db.overdue_holders(Duration::ZERO, Duration::ZERO);
        assert_eq!(held[0].activity.in_transaction, Some(false), "{held:?}");
        assert!(held[0].describe().starts_with("no transaction open"));

        drop(conn);
        assert_eq!(db.live_connections(), 0);
        assert_checkpoint_wins(&db).await;
    }

    /// A `connect` holder whose statement is still stepping is reported as
    /// executing it. Multi-threaded because the statement never yields.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_executing_connect_holder_is_reported_executing() {
        let db = Arc::new(test_db().await.unwrap());
        let conn = db.connect().await.unwrap();
        // The engine deadline is what ends the runaway; a connect connection
        // has none by default, and a statement that never yields cannot be
        // stopped any other way the test can rely on.
        conn.conn.set_query_timeout(Duration::from_secs(2)).unwrap();
        let line = line!() + 3;
        let runner = tokio::spawn(async move {
            let outcome = async {
                let mut rows = conn.query(RUNAWAY_COUNT, ()).await?;
                rows.next().await?;
                turso::Result::Ok(())
            }
            .await;
            (conn, outcome)
        });

        let started = Instant::now();
        let held = loop {
            let held = db.overdue_holders(Duration::ZERO, Duration::ZERO);
            if held.first().is_some_and(|holder| holder.activity.executing) {
                break held;
            }
            assert!(started.elapsed() < Duration::from_secs(10), "{held:?}");
            sleep(Duration::from_millis(5)).await;
        };
        let described = held[0].describe();
        assert!(
            described.starts_with(&format!("executing {}:{line} WITH RECURSIVE", file!())),
            "{described}"
        );

        let (conn, outcome) = runner.await.unwrap();
        assert!(outcome.is_err(), "the runaway count ended on its own");
        let held = db.overdue_holders(Duration::ZERO, Duration::ZERO);
        assert!(!held[0].activity.executing, "{held:?}");
        drop(conn);
        assert_eq!(db.live_connections(), 0);
    }

    /// A pooled read parked after a `&Connection` helper is retired at the
    /// bound naming the helper's line, not the BEGIN.
    #[tokio::test]
    async fn a_parked_pooled_read_names_its_last_raw_borrow() {
        let db = test_db().await.unwrap().with_read_bound(TEST_READ_BOUND);
        let borrowed_at = Arc::new(AtomicUsize::new(0));
        let seen = borrowed_at.clone();
        let error = db
            .read(move |conn| {
                Box::pin(async move {
                    seen.store(line!() as usize + 1, Ordering::Relaxed);
                    count_counters(conn).await;
                    std::future::pending::<()>().await;
                    Ok(())
                })
            })
            .await;
        let (_, _, statement) = read_bound_exceeded(&error.unwrap_err());
        let line = borrowed_at.load(Ordering::Relaxed);
        assert_eq!(
            &*statement,
            format!("{}:{line} (raw connection; sql not captured)", file!())
        );
        assert_eq!(db.live_connections(), 0);
        assert_checkpoint_wins(&db).await;
    }

    /// A statement a closure starts mid-poll renames the region that poll
    /// entered, before anything yields. This is the 10-26 shape: an idle pooled
    /// connection lets checkout, BEGIN, and the statement all happen in the
    /// first poll, and the statement then steps inside it.
    #[test]
    fn a_statement_started_mid_poll_renames_the_running_region() {
        const RUNTIME: &str = "cairn-db-midpoll-region-test";
        let region = std::thread::spawn(|| {
            runtime_regions::register_runtime_thread(RUNTIME);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let db = test_db().await.unwrap();
                db.read(|conn| {
                    Box::pin(async move {
                        let rows = conn.query("SELECT 42", ());
                        let region = runtime_regions::runtime_threads(RUNTIME)
                            .into_iter()
                            .find_map(|report| report.region);
                        drop(rows);
                        Ok(region)
                    })
                })
                .await
                .unwrap()
            })
        })
        .join()
        .unwrap()
        .expect("the polling thread must report a region");
        assert_eq!(region.label, STATEMENT_REGION);
        let detail = region
            .detail
            .expect("a db.statement region carries its statement");
        assert!(
            detail.starts_with(file!()) && detail.ends_with(" SELECT 42"),
            "{detail}"
        );
    }

    /// What the runtime stall line reads: a thread polling a database call is
    /// inside a `db.statement` region naming the call site and the SQL head,
    /// whitespace collapsed.
    #[test]
    fn a_stepping_statement_is_named_in_its_runtime_region() {
        const RUNTIME: &str = "cairn-db-region-test";
        let line = line!() + 1;
        let holder = Holder::new(Location::caller(), Some("SELECT  42\n\t FROM   t"));
        let region = std::thread::spawn(move || {
            runtime_regions::register_runtime_thread(RUNTIME);
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            runtime
                .block_on(DbCall::new(holder, || async {
                    Ok(runtime_regions::runtime_threads(RUNTIME)
                        .into_iter()
                        .find_map(|report| report.region))
                }))
                .unwrap()
        })
        .join()
        .unwrap()
        .expect("the polling thread must report a region");
        assert_eq!(region.label, "db.statement");
        assert_eq!(
            region.detail.as_deref(),
            Some(format!("{}:{line} SELECT 42 FROM t", file!()).as_str())
        );
    }

    #[test]
    fn a_statement_label_keeps_eighty_characters_of_sql() {
        let sql = format!("SELECT {}", "x".repeat(200));
        let label = statement_label(Location::caller(), Some(&sql));
        let head = label.split_once(' ').unwrap().1;
        assert_eq!(head.chars().count(), SQL_HEAD_CHARS);
        assert!(head.starts_with("SELECT xxx"));
    }

    /// A query that abandons most of its rows leaves no transaction behind.
    ///
    /// One of the CAIRN-4167 probes, kept because the question it answers looks
    /// exactly like a bug and is not one. `query_opt` steps a single row of a
    /// possibly-many-row result and returns the connection to the pool without
    /// draining the statement, which reads as a permanently-open reader on every
    /// pooled connection — the cheap explanation for "no quiet instant ever", and
    /// the first thing to suspect. It is wrong: `Statement::drop` resets the
    /// statement, so the transaction is gone and a checkpoint wins immediately.
    ///
    /// Worth defending because the pool would be free to stop dropping the
    /// statement, and nothing else would notice until checkpointing quietly died.
    #[tokio::test]
    async fn an_abandoned_query_leaves_no_transaction_open() {
        let db = test_db().await.unwrap();
        for i in 0..200 {
            db.execute(
                "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                (format!("abandon-{i}"), i64::from(i)),
            )
            .await
            .unwrap();
        }

        // Steps one row of two hundred and returns the connection to the pool.
        let first = db
            .query_opt("SELECT value FROM counters ORDER BY id", (), |row| {
                row.i64(0)
            })
            .await
            .unwrap();
        assert!(first.is_some());

        // Same for the other hot-path helpers, so the whole set is covered.
        db.query_all("SELECT value FROM counters LIMIT 5", (), |row| row.i64(0))
            .await
            .unwrap();
        db.query_opt_i64("SELECT value FROM counters LIMIT 1", ())
            .await
            .unwrap();
        db.query_opt_text("SELECT id FROM counters LIMIT 1", ())
            .await
            .unwrap();
        query_i64(&db, "SELECT COUNT(*) FROM counters")
            .await
            .unwrap();

        assert_eq!(db.live_connections(), 0);
        let report = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(50)),
            )
            .await
            .unwrap();
        assert!(
            report.succeeded() && report.drain.drained,
            "mixed hot-path traffic left something holding a transaction: {report}"
        );
        assert_eq!(report.attempts, 1, "{report}");
    }

    /// A result set still being iterated DOES hold a transaction open.
    ///
    /// The other half of the probe above, and the reason that one is worth
    /// stating: the distinction is not "turso does not hold transactions for
    /// reads", it is "the statement's `Drop` is what ends it". A live `Rows`
    /// blocks a checkpoint; the same query one line after the rows are dropped
    /// does not.
    #[tokio::test]
    async fn a_live_result_set_holds_a_transaction_open() {
        let db = test_db().await.unwrap();
        for i in 0..200 {
            db.execute(
                "INSERT INTO counters(id, value) VALUES (?1, ?2)",
                (format!("live-{i}"), i64::from(i)),
            )
            .await
            .unwrap();
        }

        let conn = db.connect().await.unwrap();
        let mut rows = conn
            .query("SELECT value FROM counters ORDER BY id", ())
            .await
            .unwrap();
        rows.next().await.unwrap().unwrap();

        let blocked = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(50)),
            )
            .await
            .unwrap();
        assert!(
            !blocked.succeeded(),
            "a checkpoint should not win against a live result set: {blocked}"
        );

        drop(rows);
        drop(conn);
        assert_eq!(db.live_connections(), 0);

        let unblocked = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(50)),
            )
            .await
            .unwrap();
        assert!(
            unblocked.succeeded() && unblocked.drain.drained,
            "dropping the result set should have freed the checkpoint: {unblocked}"
        );
    }

    /// Migrations must not deadlock against maintenance.
    ///
    /// `MigrationRunner::run_fk_off` takes an out-of-pool connection and holds it
    /// across a whole migration. That is registered with the gate like anything
    /// else, so this states the consequence: a long-held connection costs
    /// maintenance its pass, and never the other way round.
    #[tokio::test]
    async fn a_long_held_connection_costs_a_pass_and_nothing_else() {
        let db = Arc::new(test_db().await.unwrap());
        let held = db.connect().await.unwrap();
        held.execute(db.concurrent_begin(), ()).await.unwrap();

        let report = db
            .checkpoint(
                1,
                Duration::ZERO,
                DrainBudget::fixed(Duration::from_millis(50)),
            )
            .await
            .unwrap();
        assert!(!report.drain.drained, "{report}");

        // The held connection is unharmed and can still finish its work.
        held.execute("INSERT INTO counters(id, value) VALUES ('migrated', 1)", ())
            .await
            .unwrap();
        held.execute("COMMIT", ()).await.unwrap();
        drop(held);

        assert_eq!(db.live_connections(), 0);
        let after = db
            .checkpoint(1, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();
        assert!(
            after.succeeded() && after.drain.drained,
            "the next pass should win once the long holder is gone: {after}"
        );
    }

    /// A database call nested inside another's transaction closure completes
    /// while a maintenance pass is pending.
    ///
    /// This is the deadlock the whole idea was feared for, and the reason
    /// `checkout()` has never waited on anything. The nested call CANNOT be
    /// served while the gate is shut, and the drain CANNOT finish while its
    /// caller holds a connection, so the two would wait on each other forever if
    /// the gate had no budget. It has one, and the hold reopens on drop, so this
    /// resolves into a bounded pause and a wasted pass.
    ///
    /// The timeout is load-bearing: without it a regression here would hang the
    /// test suite rather than fail it, which is how a deadlock test becomes a
    /// passing no-op.
    #[tokio::test]
    async fn a_nested_call_completes_while_a_maintenance_pass_is_pending() {
        let db = Arc::new(test_db().await.unwrap());
        db.execute("INSERT INTO counters(id, value) VALUES ('nested', 5)", ())
            .await
            .unwrap();

        let checkpointing = {
            let db = db.clone();
            tokio::spawn(async move {
                // Long enough that the outer read is certainly still open when
                // the gate closes, short enough that the suite does not crawl.
                sleep(Duration::from_millis(50)).await;
                db.checkpoint(
                    1,
                    Duration::ZERO,
                    DrainBudget::fixed(Duration::from_millis(200)),
                )
                .await
                .unwrap()
            })
        };

        let inner = db.clone();
        let nested = tokio::time::timeout(
            Duration::from_secs(10),
            db.read(|conn| {
                let inner = inner.clone();
                Box::pin(async move {
                    let mut rows = conn.query("SELECT value FROM counters", ()).await?;
                    rows.next().await?;
                    drop(rows);
                    // The pass closes the gate somewhere in here, while this
                    // task holds the outer connection.
                    sleep(Duration::from_millis(150)).await;
                    inner
                        .query_one(
                            "SELECT value FROM counters WHERE id = 'nested'",
                            (),
                            |row| row.i64(0),
                        )
                        .await
                })
            }),
        )
        .await
        .expect(
            "a nested database call deadlocked against a maintenance pass: the gate did not \
             reopen when its drain could not finish",
        )
        .unwrap();

        assert_eq!(nested, 5);
        let report = checkpointing.await.unwrap();
        assert!(
            !report.drain.drained,
            "this test is only meaningful if the pass actually failed to drain \
             behind the held connection: {report}"
        );
        assert_eq!(db.live_connections(), 0);
    }

    /// Every way a transaction can end settles the gate's count.
    ///
    /// A registration leaked on any of these paths wedges the drain shut for the
    /// life of the process, and the symptom — checkpointing silently stops
    /// working — does not surface until the log has grown for a day. The
    /// rolled-back arm is the one worth having: a connection whose transaction
    /// failed is retired rather than released, so a count keyed on the pool's
    /// `release` would leak exactly here.
    #[tokio::test]
    async fn every_transaction_ending_settles_the_gate() {
        let db = test_db().await.unwrap();
        assert_eq!(db.live_connections(), 0, "a freshly opened handle is idle");

        db.execute("INSERT INTO counters(id, value) VALUES ('ok', 1)", ())
            .await
            .unwrap();
        assert_eq!(db.live_connections(), 0, "after a committed write");

        query_i64(&db, "SELECT COUNT(*) FROM counters")
            .await
            .unwrap();
        assert_eq!(db.live_connections(), 0, "after a read transaction");

        let rolled_back = db
            .write(|conn| {
                Box::pin(async move {
                    conn.execute("INSERT INTO counters(id, value) VALUES ('gone', 2)", ())
                        .await?;
                    Err::<(), DbError>(DbError::internal("force rollback"))
                })
            })
            .await;
        assert!(rolled_back.is_err());
        assert_eq!(db.live_connections(), 0, "after a rolled-back write");

        let failed = db
            .query_all("SELECT nonexistent FROM counters", (), |row| row.i64(0))
            .await;
        assert!(failed.is_err());
        assert_eq!(
            db.live_connections(),
            0,
            "after a query that failed and whose connection was retired rather \
             than released"
        );
    }

    /// Public checkpoint calls need no external coordination. A second pass must
    /// wait before it opens its private connection or closes the gate; otherwise
    /// the first pass to finish would reopen admissions underneath the other.
    #[tokio::test]
    async fn overlapping_checkpoints_are_serialized_before_the_gate() {
        let db = Arc::new(test_db().await.unwrap());
        let first_pass = db.checkpoint_lock.lock().await;

        let mut second_pass = {
            let db = db.clone();
            tokio::spawn(async move {
                db.checkpoint(1, Duration::ZERO, DRAIN_BUDGET)
                    .await
                    .unwrap()
            })
        };

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut second_pass)
                .await
                .is_err(),
            "an overlapping checkpoint entered while the first pass still held the permit"
        );

        // Waiting for the checkpoint permit must not close the connection gate.
        // A normal caller can still enter until the active pass itself closes it.
        let conn = tokio::time::timeout(Duration::from_secs(1), db.connect())
            .await
            .expect("a queued checkpoint closed the gate before acquiring its permit")
            .unwrap();
        drop(conn);

        drop(first_pass);
        let report = tokio::time::timeout(Duration::from_secs(10), second_pass)
            .await
            .expect("the queued checkpoint did not proceed after the first pass released")
            .unwrap();
        assert!(report.drain.drained, "{report}");
    }

    /// A drain that cannot finish must reopen the gate anyway, promptly.
    ///
    /// This is the property that turns "one missed path deadlocks the
    /// application" into "one missed path costs a bounded pause", and it is the
    /// only reason gating every connection in the process is safe to ship.
    #[tokio::test]
    async fn the_gate_reopens_when_the_drain_budget_expires() {
        let db = Arc::new(test_db().await.unwrap());

        // A transaction that will still be open when the budget expires.
        let holder = db.connect().await.unwrap();
        holder.execute(db.concurrent_begin(), ()).await.unwrap();

        let checkpointing = {
            let db = db.clone();
            tokio::spawn(async move {
                db.checkpoint(
                    1,
                    Duration::ZERO,
                    DrainBudget::fixed(Duration::from_millis(100)),
                )
                .await
                .unwrap()
            })
        };

        // Arrives while the gate is shut and must still be served.
        let served = tokio::time::timeout(
            Duration::from_secs(10),
            db.query_one("SELECT 1", (), |row| row.i64(0)),
        )
        .await
        .expect("the gate never reopened: a caller blocked behind a drain that could not finish")
        .unwrap();
        assert_eq!(served, 1);

        let report = checkpointing.await.unwrap();
        assert!(!report.drain.drained, "{report}");

        holder.execute("ROLLBACK", ()).await.unwrap();
        drop(holder);
        assert_eq!(db.live_connections(), 0);
    }

    /// A fold that outlives the slow mark but returns before the backstop keeps
    /// the gate shut the whole time, so a caller arriving mid-fold waits it out
    /// and is served, rather than being admitted onto a running TRUNCATE and
    /// handed `database is locked`.
    #[tokio::test]
    async fn a_slow_checkpoint_keeps_the_gate_shut_until_it_returns() {
        let db = Arc::new(test_db().await.unwrap());
        let entered = Arc::new(Notify::new());
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Arc::new(Mutex::new(released));
        let slow: CheckpointAttempt = {
            let entered = entered.clone();
            Arc::new(move |_conn| {
                entered.notify_one();
                let _ = released.lock().unwrap().recv();
                Ok(())
            })
        };

        let checkpointing = {
            let db = db.clone();
            tokio::spawn(async move {
                db.checkpoint_bounded(
                    3,
                    Duration::ZERO,
                    DrainBudget::fixed(Duration::from_millis(50)),
                    AttemptBounds {
                        slow_after: Duration::from_millis(100),
                        backstop: Duration::from_secs(5),
                    },
                    slow,
                )
                .await
                .unwrap()
            })
        };
        entered.notified().await;
        sleep(Duration::from_millis(50)).await;

        let mut caller = {
            let db = db.clone();
            tokio::spawn(async move {
                let result = db.query_one("SELECT 1", (), |row| row.i64(0)).await;
                (result, Instant::now())
            })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(350), &mut caller)
                .await
                .is_err(),
            "a caller was admitted while the fold was still running"
        );
        let released_at = Instant::now();
        release.send(()).unwrap();

        let (served, served_at) = tokio::time::timeout(Duration::from_secs(10), caller)
            .await
            .expect("the gate never reopened after the fold returned")
            .unwrap();
        assert_eq!(served.unwrap(), 1);
        assert!(served_at >= released_at);

        let report = checkpointing.await.unwrap();
        assert!(report.succeeded(), "{report}");
        assert!(report.slow, "{report}");
        assert_eq!(report.attempts, 1, "{report}");
        assert!(report.to_string().contains("slow=true"), "{report}");
        assert_eq!(db.live_connections(), 0);
    }

    /// The checkpoint thread runs at the runtime workers' QoS. A spawned thread
    /// does not inherit its parent's on macOS, so without the explicit raise it
    /// comes up at default, a class below the workers, and folds an order of
    /// magnitude slower when they load the host.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn the_checkpoint_thread_runs_at_user_initiated_qos() {
        extern "C" {
            fn qos_class_self() -> u32;
        }
        const QOS_CLASS_USER_INITIATED: u32 = 0x19;
        let db = test_db().await.unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let reading: CheckpointAttempt = {
            let observed = observed.clone();
            Arc::new(move |_conn| {
                // SAFETY: reads the calling thread's own QoS class.
                let qos = unsafe { qos_class_self() };
                observed.store(qos as usize, Ordering::SeqCst);
                Ok(())
            })
        };
        let report = db
            .checkpoint_bounded(
                1,
                Duration::ZERO,
                DRAIN_BUDGET,
                AttemptBounds::PRODUCTION,
                reading,
            )
            .await
            .unwrap();
        assert!(report.succeeded(), "{report}");
        assert_eq!(
            observed.load(Ordering::SeqCst),
            QOS_CLASS_USER_INITIATED as usize
        );
    }

    /// The longer hold is for a fold that runs. A pass whose drain cannot
    /// finish, because a caller holds a transaction open while it awaits a
    /// nested call, meets an engine that reports busy, and must reopen as
    /// promptly as before: the drain, plus three attempts each bounded by
    /// [`CHECKPOINT_BUSY_TIMEOUT`], plus slack.
    #[tokio::test]
    async fn a_busy_attempt_behind_a_nested_holder_still_reopens_promptly() {
        let db = Arc::new(test_db().await.unwrap());
        db.execute("INSERT INTO counters(id, value) VALUES ('nested', 7)", ())
            .await
            .unwrap();
        let holder = db.connect().await.unwrap();
        holder.execute(db.concurrent_begin(), ()).await.unwrap();
        let mut rows = holder
            .query("SELECT value FROM counters", ())
            .await
            .unwrap();
        rows.next().await.unwrap();
        drop(rows);

        let drain = Duration::from_millis(100);
        let checkpointing = {
            let db = db.clone();
            tokio::spawn(async move {
                db.checkpoint(3, Duration::from_millis(50), DrainBudget::fixed(drain))
                    .await
                    .unwrap()
            })
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        while !db.gate.lock().closed {
            assert!(Instant::now() < deadline, "the pass never closed the gate");
            tokio::task::yield_now().await;
        }

        let arrived = Instant::now();
        let nested = tokio::time::timeout(
            Duration::from_secs(10),
            db.query_one(
                "SELECT value FROM counters WHERE id = 'nested'",
                (),
                |row| row.i64(0),
            ),
        )
        .await
        .expect("a nested call never got past a pass that could not fold")
        .unwrap();
        let waited = arrived.elapsed();
        assert_eq!(nested, 7);
        let bound = drain + 3 * CHECKPOINT_BUSY_TIMEOUT + 2 * Duration::from_millis(50);
        assert!(
            waited < bound + Duration::from_secs(1),
            "the nested call waited {waited:?}, past {bound:?} plus slack"
        );

        let report = checkpointing.await.unwrap();
        assert!(!report.drain.drained, "{report}");
        assert!(!report.slow, "{report}");
        holder.execute("ROLLBACK", ()).await.unwrap();
        drop(holder);
        assert_eq!(db.live_connections(), 0);
    }

    /// An engine attempt that never returns must not keep the gate shut.
    ///
    /// The gate's other safety properties all rest on the checkpoint function
    /// returning, which drops the hold. This pins the one path where it would
    /// not: the attempt is stuck inside the engine past the wedge backstop, and
    /// a read that arrives while it is stuck is still served once the backstop
    /// passes. Until the stuck attempt returns, a later pass must not close the
    /// gate or start a second TRUNCATE; once it returns, checkpointing resumes.
    #[tokio::test]
    async fn the_gate_reopens_when_a_checkpoint_attempt_never_returns() {
        let db = Arc::new(test_db().await.unwrap());
        let entered = Arc::new(Notify::new());
        let (release, released) = std::sync::mpsc::channel::<()>();
        let released = Arc::new(Mutex::new(released));
        let stuck: CheckpointAttempt = {
            let entered = entered.clone();
            Arc::new(move |_conn| {
                entered.notify_one();
                let _ = released.lock().unwrap().recv();
                Ok(())
            })
        };

        let checkpointing = {
            let db = db.clone();
            tokio::spawn(async move {
                db.checkpoint_bounded(
                    3,
                    Duration::ZERO,
                    DrainBudget::fixed(Duration::from_millis(50)),
                    AttemptBounds {
                        slow_after: Duration::from_millis(100),
                        backstop: Duration::from_millis(300),
                    },
                    stuck,
                )
                .await
                .unwrap()
            })
        };
        entered.notified().await;
        let arrived = Instant::now();

        // Arrives with the gate shut and the attempt stuck, and must be served
        // once the backstop gives the attempt up.
        let served = tokio::time::timeout(
            Duration::from_secs(10),
            db.query_one("SELECT 1", (), |row| row.i64(0)),
        )
        .await
        .expect("the gate never reopened: a caller blocked behind a stuck checkpoint attempt")
        .unwrap();
        assert_eq!(served, 1);
        assert!(
            arrived.elapsed() >= Duration::from_millis(200),
            "the gate reopened before the backstop, at {:?}",
            arrived.elapsed()
        );

        let report = checkpointing.await.unwrap();
        assert_eq!(
            report.attempts, 1,
            "an abandoned attempt is not retried: {report}"
        );
        assert!(report.slow, "{report}");
        assert_eq!(
            report.error.as_deref(),
            Some("checkpoint attempt exceeded 300ms"),
            "{report}"
        );

        let refused = db
            .checkpoint(1, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();
        assert_eq!(refused.attempts, 0, "{refused}");
        assert_eq!(
            refused.drain.open_at_close, 0,
            "a pass behind a stuck attempt must not close the gate: {refused}"
        );
        assert!(!refused.succeeded(), "{refused}");

        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while db.checkpoint_in_engine.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "the released attempt never left the engine"
            );
            sleep(Duration::from_millis(10)).await;
        }
        let resumed = db
            .checkpoint(1, Duration::ZERO, DRAIN_BUDGET)
            .await
            .unwrap();
        assert!(resumed.succeeded(), "{resumed}");
        assert_eq!(db.live_connections(), 0);
    }

    #[tokio::test]
    async fn checkpoint_preserves_migrated_schema_after_delete_heavy_writes() {
        let db = crate::storage::migrated_test_db("checkpoint-delete-heavy.turso.db").await;
        let path = db.path().to_path_buf();

        for i in 0..60 {
            db.execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
                (format!("checkpoint-w{i}"), format!("checkpoint-name-{i}")),
            )
            .await
            .unwrap();
        }

        db.execute(
            "UPDATE workspaces SET updated_at = 2 WHERE id IN (
                SELECT id FROM workspaces WHERE id LIKE 'checkpoint-w%' ORDER BY id LIMIT 30
            )",
            (),
        )
        .await
        .unwrap();

        for i in (0..60).step_by(3) {
            db.execute(
                "DELETE FROM workspaces WHERE id = ?1",
                (format!("checkpoint-w{i}"),),
            )
            .await
            .unwrap();
        }

        db.consume_query("PRAGMA wal_checkpoint(TRUNCATE)")
            .await
            .unwrap();

        assert_eq!(
            query_text(&db, "PRAGMA integrity_check").await.unwrap(),
            "ok"
        );
        assert_eq!(
            query_i64(
                &db,
                "SELECT COUNT(*) FROM workspaces WHERE id LIKE 'checkpoint-w%'"
            )
            .await
            .unwrap(),
            40
        );

        let reopened = LocalDb::open(path).await.unwrap();
        assert_eq!(
            query_text(&reopened, "PRAGMA integrity_check")
                .await
                .unwrap(),
            "ok"
        );
        assert_eq!(
            query_i64(
                &reopened,
                "SELECT COUNT(*) FROM workspaces WHERE id LIKE 'checkpoint-w%'"
            )
            .await
            .unwrap(),
            40
        );
    }
}
