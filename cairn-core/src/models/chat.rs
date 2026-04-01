//! Chat domain models for project-level conversations.

use serde::{Deserialize, Serialize};

use crate::diesel_models::DbChat;

/// Status of a chat session
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChatStatus {
    #[default]
    Running,
    Complete,
    Failed,
}

impl std::fmt::Display for ChatStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChatStatus::Running => write!(f, "running"),
            ChatStatus::Complete => write!(f, "complete"),
            ChatStatus::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ChatStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "running" => Ok(ChatStatus::Running),
            "complete" => Ok(ChatStatus::Complete),
            "failed" => Ok(ChatStatus::Failed),
            _ => Err(format!("Invalid chat status: {}", s)),
        }
    }
}

/// A project-level chat session
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Chat {
    pub id: String,
    pub project_id: String,
    pub current_session_id: Option<String>,
    pub status: ChatStatus,
    pub created_at: i64,
    pub updated_at: i64,
}

impl From<DbChat> for Chat {
    fn from(db: DbChat) -> Self {
        Chat {
            id: db.id,
            project_id: db.project_id,
            current_session_id: db.current_session_id,
            status: db.status.parse().unwrap_or_default(),
            created_at: db.created_at as i64,
            updated_at: db.updated_at as i64,
        }
    }
}
