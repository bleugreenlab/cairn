use thiserror::Error;

use crate::storage::DbError;

#[derive(Debug, Error)]
pub enum CairnError {
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },

    #[error(transparent)]
    Storage(#[from] DbError),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Internal(String),
}

/// Tauri commands and other host code return Result<T, String>.
/// This impl lets `?` propagate CairnError through those boundaries.
impl From<CairnError> for String {
    fn from(e: CairnError) -> String {
        e.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_display() {
        let err = CairnError::NotFound {
            entity: "issue",
            id: "abc-123".to_string(),
        };
        assert_eq!(err.to_string(), "issue not found: abc-123");
    }

    #[test]
    fn internal_display() {
        let err = CairnError::Internal("something broke".to_string());
        assert_eq!(err.to_string(), "something broke");
    }

    #[test]
    fn from_serde_json_error() {
        let json_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err: CairnError = json_err.into();
        assert!(matches!(err, CairnError::Json(_)));
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file gone");
        let err: CairnError = io_err.into();
        assert!(matches!(err, CairnError::Io(_)));
    }

    #[test]
    fn cairn_error_to_string_conversion() {
        let err = CairnError::NotFound {
            entity: "project",
            id: "P1".to_string(),
        };
        let s: String = err.into();
        assert_eq!(s, "project not found: P1");
    }
}
