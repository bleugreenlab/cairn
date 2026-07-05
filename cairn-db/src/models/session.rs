//! Session model — durable backend conversation identity.
//!
//! A Session represents the backend conversation (e.g. Claude CLI's `--session-id`).
//! Its status describes whether the conversation is **resumable**, not whether
//! a process is currently running.
//!
//! ## Lifecycle
//!
//! ```text
//! Open → Open     (runs start/exit/crash — no status change)
//! Open → Closed   (intentional: user closes issue, ends chat)
//! Open → Failed   (positive evidence: backend rejected session token)
//! ```

use std::fmt;
use std::str::FromStr;

/// Session status — conversation identity health, not runtime state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStatus {
    /// Conversation identity is valid and resumable.
    /// Clean process exit leaves session Open.
    Open,
    /// Intentionally ended — not resumable.
    /// Only set by user action (close issue, end chat).
    Closed,
    /// Conversation identity is known bad — backend rejected session token.
    /// Not resumable.
    Failed,
}

impl fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionStatus::Open => write!(f, "open"),
            SessionStatus::Closed => write!(f, "closed"),
            SessionStatus::Failed => write!(f, "failed"),
        }
    }
}

impl FromStr for SessionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "open" => Ok(SessionStatus::Open),
            "closed" => Ok(SessionStatus::Closed),
            "failed" => Ok(SessionStatus::Failed),
            other => Err(format!("unknown session status: {}", other)),
        }
    }
}

/// Durable backend conversation identity.
#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub job_id: Option<String>,
    pub chat_id: Option<String>,
    pub backend: String,
    pub status: SessionStatus,
    pub parent_session_id: Option<String>,
    pub replaced_by_id: Option<String>,
    pub terminal_reason: Option<String>,
    pub sequence: i32,
    pub created_at: i64,
    pub closed_at: Option<i64>,
    pub updated_at: i64,
    /// Backend conversation ID — Claude session ID or Codex thread ID.
    /// Set after the backend starts (Claude: prescribed as session.id, Codex: from thread/start response).
    /// Used for resume; never used to index events.
    pub backend_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_status_display() {
        assert_eq!(SessionStatus::Open.to_string(), "open");
        assert_eq!(SessionStatus::Closed.to_string(), "closed");
        assert_eq!(SessionStatus::Failed.to_string(), "failed");
    }

    #[test]
    fn test_session_status_from_str() {
        assert_eq!(
            "open".parse::<SessionStatus>().unwrap(),
            SessionStatus::Open
        );
        assert_eq!(
            "closed".parse::<SessionStatus>().unwrap(),
            SessionStatus::Closed
        );
        assert_eq!(
            "failed".parse::<SessionStatus>().unwrap(),
            SessionStatus::Failed
        );
    }

    #[test]
    fn test_session_status_from_str_unknown() {
        let result = "expired".parse::<SessionStatus>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown session status"));
    }

    #[test]
    fn test_session_status_roundtrip() {
        for status in [
            SessionStatus::Open,
            SessionStatus::Closed,
            SessionStatus::Failed,
        ] {
            let s = status.to_string();
            let parsed: SessionStatus = s.parse().unwrap();
            assert_eq!(parsed, status);
        }
    }
}
