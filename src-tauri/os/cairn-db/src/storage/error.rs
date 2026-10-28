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

    #[error("{0}")]
    Internal(String),
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
