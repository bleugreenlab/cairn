use std::panic::Location;
use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;

pub type DbResult<T> = Result<T, DbError>;

#[derive(Debug, Error)]
pub enum DbError {
    #[error(transparent)]
    Turso(#[from] turso::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Tantivy(#[from] tantivy::TantivyError),

    #[error("database row conversion failed: {0}")]
    Row(String),

    #[error("database migration failed: {0}")]
    Migration(String),

    #[error("database search failed: {0}")]
    Search(String),

    #[error("database transaction failed after {attempts} attempts over {elapsed:?}: {source}")]
    RetryExhausted {
        attempts: usize,
        elapsed: Duration,
        source: Box<DbError>,
    },

    /// A pooled read ran past the read bound and was interrupted or dropped;
    /// see `local_db::READ_STATEMENT_BOUND`.
    #[error(
        "pooled read at {}:{} exceeded the {bound:?} read bound after {elapsed:.1?}: {statement}",
        .caller.file(),
        .caller.line()
    )]
    ReadBoundExceeded {
        caller: &'static Location<'static>,
        bound: Duration,
        elapsed: Duration,
        /// `<file>:<line> <sql head>` of the statement the read last issued.
        statement: Arc<str>,
    },

    /// The store's migration ledger records migrations the build opening it does
    /// not register, so a newer build has migrated it. Raised by
    /// [`LocalDb::open_lineage`](super::LocalDb::open_lineage) and
    /// [`LocalDb::refuse_newer_store`](super::LocalDb::refuse_newer_store)
    /// before Cairn configures, queries, or migrates the store.
    #[error(
        "{path} records {} that this Cairn build does not have (its migrations end at {build_latest}), so a newer build has migrated it. An older build would serve tables and rows a newer one changed or retired, so this build refuses to open the store and changed nothing in it. Upgrade Cairn to a build that includes every migration the store records, or point this build at a store of its own.",
        summarize_migrations(.unknown)
    )]
    StoreNewerThanBuild {
        path: String,
        /// `version_name` of every recorded migration the build does not
        /// register, in version order.
        unknown: Vec<String>,
        /// `version_name` of the build's last registered migration in apply
        /// order, or `none`.
        build_latest: String,
    },

    #[error("{0}")]
    Internal(String),
}

/// How many of a newer store's unknown migrations the refusal names before it
/// summarizes the rest; a store many releases ahead would otherwise print its
/// whole history.
const NAMED_MIGRATIONS: usize = 5;

fn summarize_migrations(unknown: &[String]) -> String {
    let named = unknown
        .iter()
        .take(NAMED_MIGRATIONS)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    match unknown.len() {
        1 => format!("migration {named}"),
        count if count <= NAMED_MIGRATIONS => format!("migrations {named}"),
        count => format!("migrations {named} and {} more", count - NAMED_MIGRATIONS),
    }
}

/// What `COMMIT` reports when the engine has already ended the transaction
/// under us. MVCC rolls a `BEGIN CONCURRENT` transaction back on a constraint
/// violation, clearing `auto_commit`, so the driver's `COMMIT` arrives with no
/// transaction left to commit and the failure surfaces here rather than at the
/// offending statement.
///
/// It means the same thing as a write-write conflict: this attempt wrote
/// nothing and lost a race. Re-running the closure re-reads current state and
/// succeeds. Two concurrent writers allocating a transcript event's `sequence`
/// for one run are the case that made this reachable (CAIRN-3290) — without the
/// retry, the loser's event is silently dropped.
const ABORTED_TRANSACTION: &str = "cannot commit - no transaction is active";

impl DbError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// Whether the engine interrupted the statement: the per-statement
    /// deadline expired or someone called `Connection::interrupt`.
    pub(crate) fn is_interrupt(&self) -> bool {
        matches!(self, Self::Turso(turso::Error::Interrupt(_)))
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Turso(turso::Error::Busy(_)) | Self::Turso(turso::Error::BusySnapshot(_)) => true,
            Self::Turso(turso::Error::Error(message))
                if message.contains("Write-write conflict")
                    || message.contains(ABORTED_TRANSACTION) =>
            {
                true
            }
            Self::RetryExhausted { source, .. } => source.is_retryable(),
            _ => false,
        }
    }
}
