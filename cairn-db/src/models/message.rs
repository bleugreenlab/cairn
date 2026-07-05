//! Message types for cross-agent communication.

use serde::{Deserialize, Serialize};

/// Canonical delivery urgency for inbound job-bound content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryUrgency {
    /// Do not wake; ride along with the next agent-bound payload.
    Passive,
    /// Wake idle recipients; active recipients receive this at turn boundary.
    Queue,
    /// Wake idle recipients; active recipients receive this at tool or turn boundary.
    Steer,
    /// Wake idle recipients; active recipients are interrupted then resumed.
    Interrupt,
}

impl DeliveryUrgency {
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryUrgency::Passive => "passive",
            DeliveryUrgency::Queue => "queue",
            DeliveryUrgency::Steer => "steer",
            DeliveryUrgency::Interrupt => "interrupt",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "passive" => Ok(DeliveryUrgency::Passive),
            "queue" => Ok(DeliveryUrgency::Queue),
            "steer" => Ok(DeliveryUrgency::Steer),
            "interrupt" => Ok(DeliveryUrgency::Interrupt),
            other => Err(format!("invalid delivery urgency: {other}")),
        }
    }

    pub fn wakes_idle(self) -> bool {
        self >= DeliveryUrgency::Queue
    }

    pub fn delivered_at_tool_boundary(self) -> bool {
        matches!(
            self,
            DeliveryUrgency::Passive | DeliveryUrgency::Steer | DeliveryUrgency::Interrupt
        )
    }
}

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
    pub urgency: Option<DeliveryUrgency>,
}
