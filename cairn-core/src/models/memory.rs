//! Memory types for the agent learning system.

use serde::{Deserialize, Serialize};

/// A learned memory that can be surfaced to agents during work.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Memory {
    pub id: String,
    pub project_id: Option<String>,
    pub content: String,
    pub confidence: MemoryConfidence,
    pub source_issue: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub surfaced_count: i32,
    pub last_surfaced_at: Option<i64>,
    pub active: bool,
    pub triggers: Vec<MemoryTrigger>,
    pub scope: String,
    pub keywords: Vec<String>,
    pub source_run_id: Option<String>,
}

/// A trigger condition for surfacing a memory.
/// Conditions with the same trigger_index are ANDed together.
/// Different trigger_index values are ORed across the memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryTrigger {
    pub id: i32,
    pub memory_id: String,
    pub trigger_index: i32,
    pub json_path: String,
    pub pattern: String,
}

/// Confidence level for a memory.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MemoryConfidence {
    Tentative,
    Established,
}

impl std::fmt::Display for MemoryConfidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryConfidence::Tentative => write!(f, "tentative"),
            MemoryConfidence::Established => write!(f, "established"),
        }
    }
}

impl std::str::FromStr for MemoryConfidence {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tentative" => Ok(MemoryConfidence::Tentative),
            "established" => Ok(MemoryConfidence::Established),
            _ => Err(format!("Invalid confidence: {}", s)),
        }
    }
}

/// Input for creating a new memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMemory {
    pub content: String,
    pub project_id: Option<String>,
    pub confidence: Option<MemoryConfidence>,
    pub source_issue: Option<String>,
    pub triggers: Vec<CreateMemoryTrigger>,
    pub scope: Option<String>,
    pub keywords: Option<Vec<String>>,
    pub source_run_id: Option<String>,
}

/// Input for creating a trigger condition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateMemoryTrigger {
    pub trigger_index: i32,
    pub json_path: String,
    pub pattern: String,
}

/// Input for updating an existing memory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateMemory {
    pub id: String,
    pub content: Option<String>,
    pub confidence: Option<MemoryConfidence>,
    pub active: Option<bool>,
    pub triggers: Option<Vec<CreateMemoryTrigger>>,
    pub scope: Option<String>,
    pub keywords: Option<Vec<String>>,
}
