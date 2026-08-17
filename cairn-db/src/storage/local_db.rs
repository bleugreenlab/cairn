use std::collections::HashMap;
use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
#[cfg(any(test, feature = "test-utils"))]
use std::sync::atomic::AtomicUsize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, MutexGuard, Once,
};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::sleep;
use turso::{params::IntoParams, Builder, Connection, Row};

use super::blocking::panic_message;
use super::content_store::{ContentStore, PrivateContentStore, TeamReplicaContext};
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
    /// Monotonic in-process generation advanced after every successful mutation.
    /// Readers use this to make expensive projections incremental without polling
    /// the database itself. Relaxed ordering is sufficient: the committed database
    /// transaction is the synchronization boundary; this counter only detects change.
    mutation_generation: AtomicU64,
    /// Set ONLY for a team replica: its intrinsic team id plus the per-team
    /// content store archival offloads to and reconstruction fetches from. The
    /// private DB carries `None`, so archival/reconstruct branch on
    /// `content_store()` and the local-run inline path is byte-for-byte unchanged.
    team: Option<Arc<TeamReplicaContext>>,
    content_store: Arc<dyn ContentStore>,
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
    /// Read transactions opened over this handle's life. Lets a test assert
    /// that an operation's transaction count is a constant rather than a
    /// function of how much data it covers — a load-independent guard against
    /// reintroducing per-row resolution. Exposed to `test-utils` consumers
    /// because the drains that most need that guard live in cairn-core.
    #[cfg(any(test, feature = "test-utils"))]
    read_transaction_count: AtomicUsize,
    /// Connections handed out by [`LocalDb::connect`] over this handle's life.
    /// Lets tests assert that a run of sequential operations reuses one
    /// connection rather than creating one per call — a load-independent guard
    /// against reintroducing per-call connect.
    #[cfg(test)]
    connections_created: AtomicUsize,
}

impl std::fmt::Debug for LocalDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalDb")
            .field("path", &self.path)
            .field("database", &self.database)
            .field("retry", &self.retry)
            .field("team", &self.team)
            .field("content_store", &"<dyn ContentStore>")
            .finish()
    }
}

impl LocalDb {
    pub async fn open(path: impl AsRef<Path>) -> DbResult<Self> {
        Self::open_with_retry(path, RetryConfig::default()).await
    }

    pub async fn open_with_retry(path: impl AsRef<Path>, retry: RetryConfig) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let path_string = path.to_string_lossy().to_string();
        let database = Arc::new(DbHandle::Local(
            Builder::new_local(&path_string).build().await?,
        ));
        let gate = Arc::new(ConnectionGate::new());
        let db = Self {
            path,
            database: database.clone(),
            retry,
            commit_signal: Arc::new(Notify::new()),
            mutation_generation: AtomicU64::new(0),
            team: None,
            content_store: Arc::new(PrivateContentStore::new(database, gate.clone())),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
        };
        db.configure().await?;
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
            database: database.clone(),
            retry,
            commit_signal: Arc::new(Notify::new()),
            mutation_generation: AtomicU64::new(0),
            team: None,
            content_store: Arc::new(PrivateContentStore::new(database, gate.clone())),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
        };
        db.configure().await?;
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
            database: database.clone(),
            retry: RetryConfig::default(),
            commit_signal: Arc::new(Notify::new()),
            mutation_generation: AtomicU64::new(0),
            team: None,
            content_store: Arc::new(PrivateContentStore::new(database, gate.clone())),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
        };
        db.configure().await?;
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

    /// The content store owned by this database. Private databases use
    /// `cas_cache`; team replicas use their brokered team store.
    pub fn content_store(&self) -> &Arc<dyn ContentStore> {
        &self.content_store
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
        self.content_store = store;
    }

    /// The commit signal fired after each successful synced-replica transaction.
    /// The per-team push task in `storage::team_sync` waits on this to coalesce a
    /// write burst into one prompt push.
    pub fn commit_signal(&self) -> Arc<Notify> {
        self.commit_signal.clone()
    }

    /// How many connections the gate currently counts as live. Tests use this to
    /// assert the invariant everything else rests on: that every way a
    /// connection's life can end settles the count back to zero. A registration
    /// leaked on any of those paths would wedge the drain shut permanently, and
    /// the symptom — checkpointing quietly stops working — is invisible until
    /// the log has grown for a day.
    #[cfg(test)]
    fn live_connections(&self) -> usize {
        self.gate.lock().live.len()
    }

    /// Current in-process mutation generation. Reading it never opens a database
    /// connection, so generation-driven consumers settle to atomic loads at idle.
    pub fn mutation_generation(&self) -> u64 {
        self.mutation_generation.load(Ordering::Relaxed)
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
                }
                Ok(changed)
            }
            DbHandle::Local(_) => Err(DbError::internal(
                "pull() called on a local (non-synced) database",
            )),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Open a brand-new connection, outside the free-list.
    ///
    /// This is the raw escape hatch for the caller that needs a connection of
    /// its own for longer than one pooled transaction: `MigrationRunner::run_fk_off`,
    /// which toggles `PRAGMA foreign_keys` around a transaction and must not
    /// have that pragma outlive its own use, and the resource layer's
    /// `connect_for_read`, which opens one read transaction and holds it across
    /// a whole resource render. Ordinary data access goes through
    /// [`Self::checkout`] instead so it reuses a warm connection.
    ///
    /// Out of the pool or not, the connection is registered with the
    /// [`ConnectionGate`] for as long as it lives. That registration is what
    /// makes the gate a statement about the whole process rather than about the
    /// pool, and it is not optional: the resource readers on this path are the
    /// bulk of the transactions a checkpoint has to wait out.
    pub async fn connect(&self) -> DbResult<TrackedConnection> {
        let slot = self.gate.admit("connect").await;
        Ok(TrackedConnection::new(self.open_connection().await?, slot))
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
    async fn checkout(&self) -> DbResult<TrackedConnection> {
        let slot = self.gate.admit("pool").await;
        let pooled = self
            .idle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pop();
        let conn = match pooled {
            Some(conn) => conn,
            None => self.open_connection().await?,
        };
        Ok(TrackedConnection::new(conn, slot))
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

    pub async fn read<T>(
        &self,
        f: impl for<'a> FnOnce(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
    ) -> DbResult<T> {
        #[cfg(any(test, feature = "test-utils"))]
        self.read_transaction_count.fetch_add(1, Ordering::Relaxed);
        let conn = self.checkout().await?;
        let attempt = run_read_tx(&conn, self.concurrent_begin(), f).await;
        if attempt.reusable {
            self.release(conn);
        }
        attempt.result
    }

    pub async fn write<T>(
        &self,
        mut f: impl for<'a> FnMut(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
    ) -> DbResult<T> {
        self.transaction_with_begin(self.concurrent_begin(), &mut f)
            .await
    }

    pub async fn exclusive<T>(
        &self,
        mut f: impl for<'a> FnMut(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
    ) -> DbResult<T> {
        self.transaction_with_begin("BEGIN", &mut f).await
    }

    /// Runs one SELECT and collects every mapped row.
    ///
    /// A single SQL statement already observes one database snapshot. Avoiding an
    /// explicit BEGIN/COMMIT here removes two engine round-trips from the hottest
    /// read path. Call Self::read when several statements must share a snapshot.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching rows, or mapping each row.
    pub async fn query_all<T, F>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
        map: F,
    ) -> DbResult<Vec<T>>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let sql = sql.into();
        let conn = self.checkout().await?;
        let result: DbResult<Vec<T>> = async {
            let mut rows = conn.query(&sql, params).await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(map(&row)?);
            }
            Ok(out)
        }
        .await;
        // Returned only on success: a query abandoned part-way through its rows
        // leaves a statement whose state this layer has no way to assert on.
        if result.is_ok() {
            self.release(conn);
        }
        result
    }

    /// Runs one SELECT and maps the first row, if present.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or mapping the row.
    pub async fn query_opt<T, F>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
        map: F,
    ) -> DbResult<Option<T>>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let sql = sql.into();
        let conn = self.checkout().await?;
        let result: DbResult<Option<T>> = async {
            let mut rows = conn.query(&sql, params).await?;
            rows.next().await?.map(|row| map(&row)).transpose()
        }
        .await;
        if result.is_ok() {
            self.release(conn);
        }
        result
    }

    /// Runs one SELECT and returns the first column of the
    /// first row as optional text.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or reading column 0.
    pub async fn query_opt_text(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
    ) -> DbResult<Option<String>> {
        self.query_opt(sql, params, |row| row.opt_text(0))
            .await
            .map(Option::flatten)
    }

    /// Runs one SELECT and returns the first column of the
    /// first row as optional integer.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or reading column 0.
    pub async fn query_opt_i64(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
    ) -> DbResult<Option<i64>> {
        self.query_opt(sql, params, |row| row.opt_i64(0))
            .await
            .map(Option::flatten)
    }

    /// Runs one SELECT and returns the first column of the
    /// first row as text, or `None` when the query returns no row.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or reading column 0.
    pub async fn query_text(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
    ) -> DbResult<Option<String>> {
        self.query_opt(sql, params, |row| row.text(0)).await
    }

    /// Runs one SELECT and requires one row.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the connection, running the query,
    /// fetching the row, or mapping the row. Returns `DbError::Row` when the
    /// query returns no rows.
    pub async fn query_one<T, F>(
        &self,
        sql: impl Into<String>,
        params: impl IntoParams + Send + 'static,
        map: F,
    ) -> DbResult<T>
    where
        F: Fn(&Row) -> DbResult<T> + Send + 'static,
        T: Send + 'static,
    {
        self.query_opt(sql, params, map)
            .await?
            .ok_or_else(|| DbError::Row("query_one returned no rows".to_string()))
    }

    /// Runs one statement through the retrying write transaction path.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the write transaction, executing the
    /// statement, committing the transaction, or exhausting retry attempts.
    pub async fn execute(&self, sql: impl Into<String>, params: impl IntoParams) -> DbResult<u64> {
        let sql = sql.into();
        let params = params.into_params()?;
        self.write(move |conn| {
            let sql = sql.clone();
            let params = params.clone();
            Box::pin(async move { Ok(conn.execute(&sql, params).await?) })
        })
        .await
    }

    /// Runs a semicolon-delimited SQL script through the retrying write
    /// transaction path.
    ///
    /// # Errors
    ///
    /// Returns database errors from opening the write transaction, executing the
    /// script, committing the transaction, or exhausting retry attempts.
    pub async fn execute_script(&self, sql: impl Into<String>) -> DbResult<()> {
        let sql = sql.into();
        self.write(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                conn.execute_batch(&sql).await?;
                Ok(())
            })
        })
        .await
    }

    async fn transaction_with_begin<T>(
        &self,
        begin_sql: &str,
        f: &mut impl for<'a> FnMut(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
    ) -> DbResult<T> {
        let started_at = Instant::now();
        let mut backoff = self.retry.initial_backoff;
        let mut last_retryable = None;

        for attempt in 1..=self.retry.max_attempts {
            let conn = self.checkout().await?;
            let outcome = run_tx(&conn, begin_sql, f).await;
            // Released between attempts as well as after the last one, so a
            // contended write that retries several times still reuses one warm
            // connection instead of creating one per attempt.
            if outcome.reusable {
                self.release(conn);
            }
            match outcome.result {
                Ok(value) => {
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
                    sleep(backoff + jitter).await;
                    backoff = (backoff * 2).min(self.retry.max_backoff);
                }
                Err(error) if error.is_retryable() => {
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

    pub async fn execute_batch(&self, sql: &str) -> DbResult<()> {
        let conn = self.checkout().await?;
        let result = conn.execute_batch(sql).await;
        if result.is_ok() {
            self.release(conn);
            self.record_mutation();
        }
        Ok(result?)
    }

    pub async fn consume_query(&self, sql: &str) -> DbResult<()> {
        let conn = self.checkout().await?;
        let result = consume_on(&conn, sql).await;
        if result.is_ok() {
            self.release(conn);
        }
        result
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
        std::fs::metadata(sidecar_path(&self.path, "-log"))
            .map(|meta| meta.len())
            .unwrap_or(0)
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
    /// including a panic inside the engine, so a caller waiting at the gate waits
    /// at most `drain_budget` plus this checkpoint.
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
        drain_budget: Duration,
    ) -> DbResult<CheckpointReport> {
        // `checkpoint` is public and callers are not required to coordinate.
        // Keep this permit outside every other operation in the pass so a second
        // call cannot close the gate until the first call's GateHold has dropped.
        let _checkpoint_permit = self.checkpoint_lock.lock().await;
        let started = Instant::now();
        let log_bytes_before = self.logical_log_bytes();

        // Opened before the gate closes and deliberately NOT registered with it:
        // a tracked connection here would be counted among the transactions this
        // pass is waiting to drain, and the drain could never reach zero.
        let conn = self.open_connection().await?;
        conn.busy_timeout(CHECKPOINT_BUSY_TIMEOUT)?;

        let (_hold, drain) = self.gate.close(drain_budget).await;
        let mut attempts = 0;
        let mut error = None;

        for attempt in 1..=max_attempts.max(1) {
            if attempt > 1 && !between_attempts.is_zero() {
                sleep(between_attempts).await;
            }
            attempts = attempt;
            match consume_on(&conn, "PRAGMA wal_checkpoint(TRUNCATE)").await {
                Ok(()) => {
                    error = None;
                    break;
                }
                Err(failure) => {
                    let contended = failure.is_retryable();
                    error = Some(failure.to_string());
                    if !contended {
                        break;
                    }
                }
            }
        }

        Ok(CheckpointReport {
            attempts,
            error,
            log_bytes_before,
            log_bytes_after: self.logical_log_bytes(),
            duration: started.elapsed(),
            drain,
        })
    }

    /// Reclaim freelist space by writing a self-contained, compacted image of
    /// this database to `dest` via `VACUUM INTO`.
    ///
    /// Unlike an in-place `VACUUM`, this writes a separate image that can be
    /// validated before any offline swap. Older Turso revisions also had an MVCC
    /// TRUNCATE-checkpoint corruption path on migrated schemas with deleted rows;
    /// keep checkpoint-heavy changes covered by the regression tests described in
    /// docs/database.md. `dest` is therefore itself
    /// an MVCC three-file set (`{dest, dest-wal, dest-log}`) with committed data
    /// living in the sidecars; move and validate it as a whole set, never the
    /// `.db` file alone.
    ///
    /// Refuses to run if any member of `dest`'s file set already exists. `VACUUM`
    /// cannot run inside a `BEGIN..COMMIT` transaction, so this issues the
    /// statement on a raw connection (via `consume_query`) rather than the
    /// transaction-wrapped `execute`/`write` path.
    pub async fn vacuum_into(&self, dest: &Path) -> DbResult<()> {
        for member in db_set_paths(dest) {
            if member.exists() {
                return Err(DbError::internal(format!(
                    "vacuum_into destination already exists: {}",
                    member.display()
                )));
            }
        }
        let target = dest.to_string_lossy().replace('\'', "''");
        self.consume_query(&format!("VACUUM INTO '{target}'")).await
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
/// [`LocalDb::checkout`] would not. The resource layer's `connect_for_read`
/// opens a `BEGIN CONCURRENT` on an OUT-OF-POOL connection and holds it for a
/// whole resource render, at some thirty call sites across ten readers, and
/// those reads are what the desktop UI's status polling drives. A pool-only gate
/// would have drained to zero, reported a quiet instant, and lost the lock
/// anyway.
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
/// waits at most that budget plus one checkpoint. That property is what makes
/// gating the whole process tractable: a call path this design failed to
/// anticipate costs a bounded pause and a wasted maintenance pass, not a wedged
/// application.
#[derive(Debug)]
pub(super) struct ConnectionGate {
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

/// One live connection, recorded for the single purpose of explaining a drain
/// that did not finish.
#[derive(Debug, Clone, Copy)]
struct LiveConnection {
    origin: &'static str,
    since: Instant,
}

impl ConnectionGate {
    fn new() -> Self {
        Self {
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
    pub(super) async fn admit(self: &Arc<Self>, origin: &'static str) -> ConnectionSlot {
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
                            origin,
                            since: Instant::now(),
                        },
                    );
                    return ConnectionSlot {
                        gate: self.clone(),
                        id,
                    };
                }
            }
            reopened.await;
        }
    }

    fn release(&self, id: u64) {
        let emptied = {
            let mut state = self.lock();
            state.live.remove(&id);
            state.live.is_empty()
        };
        if emptied {
            self.drained.notify_waiters();
        }
    }

    /// Close the gate and wait up to `budget` for the connections already open
    /// to be released.
    ///
    /// Returns the hold whether or not the drain finished, so the caller decides
    /// what a partial drain is worth and the gate reopens when that hold drops
    /// either way.
    async fn close(self: &Arc<Self>, budget: Duration) -> (GateHold, DrainReport) {
        let started = Instant::now();
        self.lock().closed = true;
        let hold = GateHold { gate: self.clone() };
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
                    .map(|live| (live.origin, live.since.elapsed()));
                (state.live.len(), oldest)
            };
            if still_open == 0 {
                return (hold, DrainReport::drained(started.elapsed()));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return (
                    hold,
                    DrainReport {
                        drained: false,
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
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.gate.release(self.id);
    }
}

/// A `turso::Connection` that counts toward the quiet instant maintenance waits
/// for.
///
/// Derefs to the connection, so callers use it exactly as they used a bare one
/// and the resource layer's ~30 `connect_for_read` sites needed no edit at all.
/// The wrapper's entire purpose is its `Drop`.
pub struct TrackedConnection {
    conn: Connection,
    _slot: ConnectionSlot,
}

impl TrackedConnection {
    pub(super) fn new(conn: Connection, slot: ConnectionSlot) -> Self {
        Self { conn, _slot: slot }
    }

    /// Take the bare connection back, ending its registration.
    ///
    /// Only [`LocalDb::release`] does this: a connection resting on the
    /// free-list holds no transaction, so counting it would hold the drain above
    /// zero forever.
    fn into_connection(self) -> Connection {
        self.conn
    }
}

impl Deref for TrackedConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        &self.conn
    }
}

impl std::fmt::Debug for TrackedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TrackedConnection")
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
    /// Connections still live at the deadline.
    pub still_open: usize,
    /// Where the longest-held live connection came from, and how long it had
    /// been out.
    pub oldest: Option<(&'static str, Duration)>,
    /// How long the drain waited — the stall every database call in the process
    /// paid for this pass.
    pub waited: Duration,
}

impl DrainReport {
    fn drained(waited: Duration) -> Self {
        Self {
            drained: true,
            still_open: 0,
            oldest: None,
            waited,
        }
    }
}

impl std::fmt::Display for DrainReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drain_ms={} drained={}",
            self.waited.as_millis(),
            self.drained
        )?;
        if !self.drained {
            write!(f, " still_open={}", self.still_open)?;
            if let Some((origin, age)) = &self.oldest {
                write!(f, " oldest={origin}/{}ms", age.as_millis())?;
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
}

impl CheckpointReport {
    pub fn succeeded(&self) -> bool {
        self.error.is_none()
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
        match &self.error {
            Some(error) => write!(f, " outcome=failed error={error}"),
            None => write!(f, " outcome=checkpointed"),
        }
    }
}

/// The three files comprising one MVCC database set: the main `.db` plus its
/// `-wal` and `-log` sidecars. Committed data lives in the sidecars, so any
/// move, copy, backup, or snapshot of the database must treat all three as one
/// unit (see docs/database.md). Returned in the order `[main, -wal, -log]`.
pub fn db_set_paths(base: &Path) -> [PathBuf; 3] {
    [
        base.to_path_buf(),
        sidecar_path(base, "-wal"),
        sidecar_path(base, "-log"),
    ]
}

fn sidecar_path(base: &Path, suffix: &str) -> PathBuf {
    let mut name = base.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

/// Total size in bytes of every member of `base`'s three-file set that exists on
/// disk. Absent sidecars contribute zero, so the figure is meaningful both
/// before and after a `VACUUM INTO` regardless of how many sidecars are present.
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
    conn: &Connection,
    begin_sql: &str,
    f: &mut impl for<'a> FnMut(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
) -> TxAttempt<T> {
    if let Err(error) = conn.execute(begin_sql, ()).await {
        // A BEGIN that fails opened nothing, but it also means this connection
        // was not in the state the pool guarantees. Retire it rather than reason
        // about why.
        return TxAttempt {
            result: Err(error.into()),
            reusable: false,
        };
    }

    match f(conn).await {
        Ok(value) => match end_tx(conn, "COMMIT").await {
            Ok(()) => TxAttempt {
                result: Ok(value),
                reusable: true,
            },
            Err(error) => {
                let reusable = end_tx(conn, "ROLLBACK").await.is_ok();
                TxAttempt {
                    result: Err(error),
                    reusable,
                }
            }
        },
        Err(error) => {
            let reusable = end_tx(conn, "ROLLBACK").await.is_ok();
            TxAttempt {
                result: Err(error),
                reusable,
            }
        }
    }
}

async fn run_read_tx<T>(
    conn: &Connection,
    begin_sql: &str,
    f: impl for<'a> FnOnce(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
) -> TxAttempt<T> {
    if let Err(error) = conn.execute(begin_sql, ()).await {
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
            let outcome = end_tx(conn, "ROLLBACK").await;
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
            let reusable = end_tx(conn, "ROLLBACK").await.is_ok();
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
    use crate::storage::{Migration, MigrationRunner, RowExt};

    /// The drain budget these tests hold the gate open for.
    ///
    /// Generous next to the 500 ms `db_maintenance` uses in production, because
    /// a test asserting that a checkpoint WINS must not be able to fail on a
    /// loaded CI machine for want of a few milliseconds. Tests that assert the
    /// budget EXPIRES pass their own short one.
    const DRAIN_BUDGET: Duration = Duration::from_secs(5);

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
            database: database.clone(),
            retry: RetryConfig::default(),
            commit_signal: Arc::new(Notify::new()),
            mutation_generation: AtomicU64::new(0),
            team: None,
            content_store: Arc::new(PrivateContentStore::new(database, gate.clone())),
            idle: Mutex::new(Vec::new()),
            gate,
            checkpoint_lock: AsyncMutex::new(()),
            #[cfg(any(test, feature = "test-utils"))]
            read_transaction_count: AtomicUsize::new(0),
            #[cfg(test)]
            connections_created: AtomicUsize::new(0),
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
    async fn vacuum_into_produces_valid_compacted_image_with_all_rows() {
        let db = crate::storage::migrated_test_db("vacuum-src.turso.db").await;
        // Seed rows but never checkpoint, so these committed bytes live only in
        // the source -wal/-log sidecars — exercising three-file handling end to
        // end through VACUUM INTO.
        for i in 0..50 {
            db.execute(
                "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES (?1, ?2, 1, 1)",
                (format!("w{i}"), format!("name-{i}")),
            )
            .await
            .unwrap();
        }

        let dir = tempdir().unwrap();
        let staged = dir.path().join("vacuum-staged.turso.db");
        db.vacuum_into(&staged).await.unwrap();

        // The staged image is a valid, self-contained database with every row.
        let staged_db = LocalDb::open(&staged).await.unwrap();
        assert_eq!(
            query_text(&staged_db, "PRAGMA integrity_check")
                .await
                .unwrap(),
            "ok"
        );
        assert_eq!(
            query_i64(
                &staged_db,
                "SELECT COUNT(*) FROM workspaces WHERE id LIKE 'w%'"
            )
            .await
            .unwrap(),
            50
        );
        assert_eq!(
            query_text(&staged_db, "SELECT name FROM workspaces WHERE id = 'w7'")
                .await
                .unwrap(),
            "name-7"
        );
    }

    #[tokio::test]
    async fn vacuum_into_refuses_existing_destination() {
        let db = crate::storage::migrated_test_db("vacuum-refuse.turso.db").await;
        let dir = tempdir().unwrap();

        // An existing main .db blocks it...
        let dest = dir.path().join("occupied.turso.db");
        std::fs::write(&dest, b"occupied").unwrap();
        assert!(matches!(
            db.vacuum_into(&dest).await.unwrap_err(),
            DbError::Internal(_)
        ));

        // ...and so does an existing sidecar with no main .db file.
        let sidecar_only = dir.path().join("sidecar-only.turso.db");
        std::fs::write(dir.path().join("sidecar-only.turso.db-wal"), b"x").unwrap();
        assert!(matches!(
            db.vacuum_into(&sidecar_only).await.unwrap_err(),
            DbError::Internal(_)
        ));
    }

    #[test]
    fn move_db_set_relocates_every_present_member_and_leaves_backup_intact() {
        let dir = tempdir().unwrap();
        let live = dir.path().join("live.turso.db");
        let staged = dir.path().join("staged.turso.db");
        let backup = dir.path().join("live.turso.db.vacuum-backup");

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
            std::fs::read(dir.path().join("live.turso.db.vacuum-backup-wal")).unwrap(),
            b"live-wal"
        );
        assert_eq!(
            std::fs::read(dir.path().join("live.turso.db.vacuum-backup-log")).unwrap(),
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
    /// `checkout()`, so gating the pool would be enough. It is not: the resource
    /// layer's `connect_for_read` opens `BEGIN CONCURRENT` on an OUT-OF-POOL
    /// connection and holds it for a whole render, at ~30 call sites, and those
    /// reads are what the desktop UI's status polling drives. A pool-only gate
    /// would have drained to zero, declared a quiet instant, and lost the lock.
    ///
    /// So: a transaction shaped exactly like a resource read must be visible to
    /// the drain, and must be NAMED when it outlasts the budget.
    #[tokio::test]
    async fn an_out_of_pool_read_transaction_is_visible_to_the_drain() {
        let db = test_db().await.unwrap();
        db.execute("INSERT INTO counters(id, value) VALUES ('a', 1)", ())
            .await
            .unwrap();

        // Precisely what `connect_for_read` does: connect outside the pool, BEGIN,
        // and hold it for the life of the read.
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
            .checkpoint(1, Duration::ZERO, Duration::from_millis(50))
            .await
            .unwrap();

        assert!(
            !report.drain.drained,
            "the drain claimed to empty while a read transaction was open: {report}"
        );
        assert_eq!(report.drain.still_open, 1, "{report}");
        let (origin, _age) = report
            .drain
            .oldest
            .expect("a drain that did not finish must name what it was waiting on");
        assert_eq!(
            origin, "connect",
            "the report must say WHERE the blocking transaction came from, or the \
             follow-up is another investigation instead of one log line"
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
            .checkpoint(1, Duration::ZERO, Duration::from_millis(50))
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
            .checkpoint(1, Duration::ZERO, Duration::from_millis(50))
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
            .checkpoint(1, Duration::ZERO, Duration::from_millis(50))
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
            .checkpoint(1, Duration::ZERO, Duration::from_millis(50))
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
                db.checkpoint(1, Duration::ZERO, Duration::from_millis(200))
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
                db.checkpoint(1, Duration::ZERO, Duration::from_millis(100))
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
