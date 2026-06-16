//! Message types for cross-agent communication.

use serde::{Deserialize, Serialize};

use crate::messages::queued::DeliveryUrgency;

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
///
/// `delivered_at` is per-message delivery state for direct messages (CAIRN-1196):
/// `None` means "queued, not yet shown to the recipient"; `Some(ts)` records the
/// unix timestamp at which the queued direct was claimed by an injection path
/// (Claude hook additionalContext or Cairn tool-result augmentation) or the
/// stdin push to a warm recipient succeeded. Channel messages don't use this
/// field — channel delivery is tracked by the per-process in-memory cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub id: String,
    pub channel_type: ChannelType,
    pub channel_id: Option<String>,
    pub sender_run_id: Option<String>,
    pub sender_name: String,
    pub recipient_run_id: Option<String>,
    pub content: String,
    pub created_at: i64,
    pub delivered_at: Option<i64>,
    pub urgency: Option<DeliveryUrgency>,
}
