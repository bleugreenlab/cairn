//! Message types for cross-agent communication.

use serde::{Deserialize, Serialize};

/// Channel type determines delivery behavior.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ChannelType {
    Project,
    Issue,
    Direct,
}

impl std::fmt::Display for ChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelType::Project => write!(f, "project"),
            ChannelType::Issue => write!(f, "issue"),
            ChannelType::Direct => write!(f, "direct"),
        }
    }
}

impl std::str::FromStr for ChannelType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "project" => Ok(ChannelType::Project),
            "issue" => Ok(ChannelType::Issue),
            "direct" => Ok(ChannelType::Direct),
            _ => Err(format!("Unknown channel type: {}", s)),
        }
    }
}

/// A message in the agent chatroom.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub channel_type: ChannelType,
    pub channel_id: Option<String>,
    pub sender_run_id: Option<String>,
    pub sender_name: String,
    pub recipient_run_id: Option<String>,
    pub recipient_manager_id: Option<String>,
    pub content: String,
    pub created_at: i64,
}
