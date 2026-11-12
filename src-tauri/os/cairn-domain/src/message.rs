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
}

/// Channel type determines delivery behavior.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ChannelType {
    Project,
    Issue,
    Thread,
    Direct,
}

impl std::fmt::Display for ChannelType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelType::Project => write!(f, "project"),
            ChannelType::Issue => write!(f, "issue"),
            ChannelType::Thread => write!(f, "thread"),
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
            "thread" => Ok(ChannelType::Thread),
            "direct" => Ok(ChannelType::Direct),
            _ => Err(format!("Unknown channel type: {}", s)),
        }
    }
}

/// Wake level: where an attention push sits on the wake axis. Ordered by
/// loudness, so merging two routes to one recipient keeps the louder with `max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Wake {
    /// Never wakes an idle agent. On an active agent it still rides along at
    /// its boundary like any other push — wake level governs idle-waking, not
    /// whether a running agent sees the push.
    Passive,
    /// Wakes an idle agent; on an active agent lands at its boundary.
    Wake,
    /// Breaks the running turn now.
    Interrupt,
}

impl Wake {
    pub fn as_str(self) -> &'static str {
        match self {
            Wake::Passive => "passive",
            Wake::Wake => "wake",
            Wake::Interrupt => "interrupt",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "passive" => Some(Wake::Passive),
            "wake" => Some(Wake::Wake),
            "interrupt" => Some(Wake::Interrupt),
            _ => None,
        }
    }

    /// Whether this wake level wakes an *idle* agent — so a push creator should
    /// nudge the recipient. `Passive` rides along on the next run and never
    /// wakes; `Wake` and `Interrupt` do. A muted source downgraded to `Passive`
    /// reports `false` here, which is how a creator skips nudging a muted
    /// recipient.
    pub fn wakes_idle(self) -> bool {
        matches!(self, Wake::Wake | Wake::Interrupt)
    }
}

/// One addressee of a direct message, in the order the sender named them
/// (the append target first, then each `cc` entry).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageRecipient {
    /// The addressed session, node or task job. `None` only for history whose
    /// run row was gone when recipients moved off `messages`.
    pub job_id: Option<String>,
    /// The recipient's canonical home URI, derived from `job_id` when the
    /// message is loaded. `None` when the job has no canonical address.
    pub address: Option<String>,
    /// The recipient's latest run when the message was sent: provenance only.
    pub run_id_at_send: Option<String>,
    /// What this recipient's own mute made of the sender's urgency. `None` for
    /// history written before it was recorded.
    pub wake: Option<Wake>,
}

impl MessageRecipient {
    /// How this recipient is named to a reader: its address, else the run it
    /// was sent to.
    pub fn label(&self) -> String {
        match (&self.address, &self.run_id_at_send) {
            (Some(address), _) => address.clone(),
            (None, Some(run)) => format!("run {run} (no longer resolvable)"),
            (None, None) => "an unknown recipient".to_string(),
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
    /// A direct message's addressees in position order; empty for a channel
    /// message.
    #[serde(default)]
    pub recipients: Vec<MessageRecipient>,
    pub content: String,
    pub created_at: i64,
    pub urgency: Option<DeliveryUrgency>,
}
