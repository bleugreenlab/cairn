use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use tokio::sync::Notify;
use tokio::time::sleep;
use turso::{params::IntoParams, Builder, Connection, Row};

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
enum DbHandle {
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

#[derive(Debug)]
pub struct LocalDb {
    path: PathBuf,
    database: DbHandle,
    retry: RetryConfig,
    /// Fired after every successful transaction on a SYNCED replica (never on a
    /// local database). The per-team push task waits on it to push promptly once
    /// writes settle; permit-backed, so a burst of commits collapses to a single
    /// pending wakeup and none is lost.
    commit_signal: Arc<Notify>,
    /// Set ONLY for a team replica: its intrinsic team id plus the per-team
    /// content store archival offloads to and reconstruction fetches from. The
    /// private DB carries `None`, so archival/reconstruct branch on
    /// `content_store()` and the local-run inline path is byte-for-byte unchanged.
    team: Option<Arc<TeamReplicaContext>>,
    #[cfg(test)]
    read_transaction_count: AtomicUsize,
}

impl LocalDb {
    pub async fn open(path: impl AsRef<Path>) -> DbResult<Self> {
        Self::open_with_retry(path, RetryConfig::default()).await
    }

    pub async fn open_with_retry(path: impl AsRef<Path>, retry: RetryConfig) -> DbResult<Self> {
        let path = path.as_ref().to_path_buf();
        let path_string = path.to_string_lossy().to_string();
        let database = Builder::new_local(&path_string).build().await?;
        let db = Self {
            path,
            database: DbHandle::Local(database),
            retry,
            commit_signal: Arc::new(Notify::new()),
            team: None,
            #[cfg(test)]
            read_transaction_count: AtomicUsize::new(0),
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
        let database = builder.build().await?;
        let db = Self {
            path,
            database: DbHandle::Synced(database),
            retry,
            commit_signal: Arc::new(Notify::new()),
            team: None,
            #[cfg(test)]
            read_transaction_count: AtomicUsize::new(0),
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
        let database = turso::sync::Builder::new_remote(&path_string)
            .with_remote_url(remote_url.into())
            .with_auth_token_fn(token_fn)
            .build()
            .await?;
        let db = Self {
            path,
            database: DbHandle::Synced(database),
            retry: RetryConfig::default(),
            commit_signal: Arc::new(Notify::new()),
            team: None,
            #[cfg(test)]
            read_transaction_count: AtomicUsize::new(0),
        };
        db.configure().await?;
        Ok(db)
    }

    /// Whether this handle is backed by a Turso Sync replica (vs a local file).
    pub fn is_synced(&self) -> bool {
        matches!(self.database, DbHandle::Synced(_))
    }

    /// The team id this handle belongs to, or `None` for the private DB. A team
    /// replica carries its own scope (set at open), so callers detect a team run
    /// from the resolved handle itself — independent of HOW it was resolved.
    pub fn team_id(&self) -> Option<&TeamId> {
        self.team.as_ref().map(|ctx| &ctx.team_id)
    }

    /// The per-team content store for a team replica, or `None` for the private
    /// DB. `Some` is the signal to offload archival bytes (and fetch them back)
    /// by hash; `None` keeps the local-run inline path.
    pub fn content_store(&self) -> Option<&Arc<dyn ContentStore>> {
        self.team.as_ref().map(|ctx| &ctx.store)
    }

    /// The private database that owns machine-local route metadata for this team
    /// replica, when available.
    pub fn private_route_db(&self) -> Option<&Arc<LocalDb>> {
        self.team.as_ref().and_then(|ctx| ctx.private_db.as_ref())
    }

    /// Attach a team replica's identity + content store. Called by `open_team`
    /// after construction (and by tests that inject a fake store) before the
    /// handle is shared behind an `Arc`.
    pub fn set_team_context(&mut self, ctx: TeamReplicaContext) {
        self.team = Some(Arc::new(ctx));
    }

    /// The commit signal fired after each successful synced-replica transaction.
    /// The per-team push task in `storage::team_sync` waits on this to coalesce a
    /// write burst into one prompt push.
    pub fn commit_signal(&self) -> Arc<Notify> {
        self.commit_signal.clone()
    }

    /// The BEGIN statement for concurrent read/write transactions. A local
    /// (MVCC) database uses `BEGIN CONCURRENT` for optimistic concurrency; the
    /// synced engine captures changes via CDC, which is incompatible with MVCC,
    /// so it uses a plain `BEGIN` (writers serialize and retry on Busy instead).
    pub fn concurrent_begin(&self) -> &'static str {
        match self.database {
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
        match &self.database {
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
        match &self.database {
            DbHandle::Synced(db) => Ok(db.pull().await?),
            DbHandle::Local(_) => Err(DbError::internal(
                "pull() called on a local (non-synced) database",
            )),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub async fn connect(&self) -> DbResult<Connection> {
        let conn = match &self.database {
            DbHandle::Local(db) => db.connect()?,
            DbHandle::Synced(db) => db.connect().await?,
        };
        conn.busy_timeout(self.retry.busy_timeout)?;
        conn.execute("PRAGMA foreign_keys = ON", ()).await?;
        Ok(conn)
    }

    pub async fn read<T>(
        &self,
        f: impl for<'a> FnOnce(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
    ) -> DbResult<T> {
        #[cfg(test)]
        self.read_transaction_count.fetch_add(1, Ordering::Relaxed);
        let conn = self.connect().await?;
        run_read_tx(&conn, self.concurrent_begin(), f).await
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
        let conn = self.connect().await?;
        let mut rows = conn.query(&sql, params).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(map(&row)?);
        }
        Ok(out)
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
        let conn = self.connect().await?;
        let mut rows = conn.query(&sql, params).await?;
        rows.next().await?.map(|row| map(&row)).transpose()
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
            let conn = self.connect().await?;
            match run_tx(&conn, begin_sql, f).await {
                Ok(value) => {
                    // Signal the push task that a synced replica committed. Gated
                    // on `is_synced()` so a local database stays zero-cost (the
                    // Notify is allocated but never fired). This is the ONLY fire
                    // site for `commit_signal`: `pull()` applies remote pages via
                    // physical WAL replay OUTSIDE `transaction_with_begin`, so an
                    // applied pull fires no commit signal — there is no
                    // push<->pull feedback loop.
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
        let conn = self.connect().await?;
        conn.execute_batch(sql).await?;
        Ok(())
    }

    pub async fn consume_query(&self, sql: &str) -> DbResult<()> {
        let conn = self.connect().await?;
        let mut rows = conn.query(sql, ()).await?;
        while rows.next().await?.is_some() {}
        Ok(())
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
        if matches!(self.database, DbHandle::Local(_)) {
            self.consume_query("PRAGMA journal_mode = 'mvcc'").await?;
        }
        self.consume_query("PRAGMA foreign_keys = ON").await?;
        Ok(())
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

async fn run_tx<T>(
    conn: &Connection,
    begin_sql: &str,
    f: &mut impl for<'a> FnMut(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
) -> DbResult<T> {
    conn.execute(begin_sql, ()).await?;

    let result = f(conn).await;
    match result {
        Ok(value) => match conn.execute("COMMIT", ()).await {
            Ok(_) => Ok(value),
            Err(error) => {
                let _ = conn.execute("ROLLBACK", ()).await;
                Err(error.into())
            }
        },
        Err(error) => {
            let _ = conn.execute("ROLLBACK", ()).await;
            Err(error)
        }
    }
}

async fn run_read_tx<T>(
    conn: &Connection,
    begin_sql: &str,
    f: impl for<'a> FnOnce(&'a Connection) -> BoxFuture<'a, DbResult<T>>,
) -> DbResult<T> {
    conn.execute(begin_sql, ()).await?;

    let result = f(conn).await;
    match result {
        Ok(value) => {
            conn.execute("ROLLBACK", ()).await?;
            Ok(value)
        }
        Err(error) => {
            let _ = conn.execute("ROLLBACK", ()).await;
            Err(error)
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
                Ok(rows.next().await?.unwrap().i64(0)?)
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
        let database = turso::sync::Builder::new_remote(":memory:")
            .bootstrap_if_empty(false)
            .build()
            .await?;
        let db = LocalDb {
            path: PathBuf::from(":memory:"),
            database: DbHandle::Synced(database),
            retry: RetryConfig::default(),
            commit_signal: Arc::new(Notify::new()),
            team: None,
            #[cfg(test)]
            read_transaction_count: AtomicUsize::new(0),
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
