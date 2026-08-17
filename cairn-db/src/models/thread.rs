//! First-class thread entity types.

use serde::{Deserialize, Serialize};

/// A thread's lifecycle state, and the only thing that varies about it.
///
/// This is attention routing, not a resolution: `Closed` makes a thread dormant
/// — gone from active listings, unable to be prompted, ineligible for wake
/// delivery — while its transcript, jobs, subscriptions, and children stay
/// exactly where they were. `Active` restores every one of those without
/// reconstructing anything, which is why the pair is a reversible state rather
/// than a terminal one.
///
/// The two spellings here are the two the `threads.status` CHECK constraint
/// admits, so parsing at this boundary is the one place the vocabulary is
/// stated.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ThreadStatus {
    #[default]
    Active,
    Closed,
}

impl ThreadStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ThreadStatus::Active => "active",
            ThreadStatus::Closed => "closed",
        }
    }

    pub fn is_active(self) -> bool {
        self == ThreadStatus::Active
    }

    /// Parse a stored or caller-supplied status, naming the whole vocabulary on
    /// failure so a rejected payload says what it should have said.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "active" => Ok(ThreadStatus::Active),
            "closed" => Ok(ThreadStatus::Closed),
            other => Err(format!(
                "thread status must be active or closed, not '{other}'"
            )),
        }
    }
}

impl std::fmt::Display for ThreadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Thread {
    pub id: String,
    pub project_id: String,
    /// A thread's one identifier: its address, its label, and its display name.
    pub name: String,
    pub jurisdiction: Option<String>,
    pub status: ThreadStatus,
    pub attention: String,
    /// Serialized ThreadDefinition. NULL selects the system default.
    pub definition: Option<String>,
    pub migrated_from_number: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateThread {
    pub project_id: String,
    /// The thread's address, or absence asking the server to allocate one.
    ///
    /// A name is a judgment about what a topic IS, and at creation nobody has
    /// made it yet: the person has typed one message and the Thread agent has
    /// not read it. So the boundary stays name-free and the server allocates a
    /// `thread-<n>` placeholder the agent replaces from inside its first
    /// session. An explicit name is still honoured verbatim — a migration, a
    /// channel surface, or an agent creating a thread it can already name knows
    /// what it wants — and only absence invokes allocation.
    #[serde(default)]
    pub name: Option<String>,
    pub jurisdiction: Option<String>,
    pub definition: Option<String>,
    pub migrated_from_number: Option<i64>,
    pub model: Option<crate::models::ModelSelection>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateThread {
    pub id: String,
    pub name: Option<String>,
    pub jurisdiction: Option<Option<String>>,
    pub definition: Option<Option<String>>,
    /// Close the thread (dormant, reversible) or reopen it. Absent leaves the
    /// current state alone, so every other field can be edited from either side
    /// of the lifecycle.
    #[serde(default)]
    pub status: Option<ThreadStatus>,
    /// Re-point the thread's session at another model. Stored on the session
    /// job exactly as [`CreateThread::model`] stores it, so a thread has one
    /// model story whether it was chosen at creation or changed later; the
    /// runtime picks it up on the next turn.
    #[serde(default)]
    pub model: Option<crate::models::ModelSelection>,
}
