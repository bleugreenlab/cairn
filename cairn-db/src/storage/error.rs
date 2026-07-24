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

    #[error("{0}")]
    Internal(String),
}

impl DbError {
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    pub(crate) fn is_retryable(&self) -> bool {
        match self {
            Self::Turso(turso::Error::Busy(_)) | Self::Turso(turso::Error::BusySnapshot(_)) => true,
            Self::Turso(turso::Error::Error(message))
                if message.contains("Write-write conflict") =>
            {
                true
            }
            Self::RetryExhausted { source, .. } => source.is_retryable(),
            _ => false,
        }
    }
}
