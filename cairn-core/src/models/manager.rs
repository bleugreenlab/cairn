//! Manager types.

use serde::{Deserialize, Serialize};

use super::common::Model;

/// A manager - a long-lived agent session that owns a branch and manages issues.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manager {
    pub id: String,
    pub project_id: String,
    pub home_project_id: Option<String>,
    pub scope_kind: ManagerScopeKind,
    pub name: String,
    pub description: String,
    pub branch: String,
    pub job_id: Option<String>,
    pub status: ManagerStatus,
    pub current_session_id: Option<String>,
    pub current_turn_id: Option<String>,
    pub last_wake_at: Option<i64>,
    pub last_turn_completed_at: Option<i64>,
    pub last_error: Option<String>,
    pub agent_config_id: Option<String>,
    #[serde(alias = "model")]
    pub tier: Option<Model>,
    pub parent_manager_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub execution_id: Option<String>,
}

/// Manager lifecycle status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ManagerStatus {
    #[default]
    Active,
    Paused,
    Draining,
    Completed,
    Errored,
}

/// Actor scope for a manager.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ManagerScopeKind {
    #[default]
    Branch,
    Project,
    Workspace,
}

impl std::fmt::Display for ManagerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerStatus::Active => write!(f, "active"),
            ManagerStatus::Paused => write!(f, "paused"),
            ManagerStatus::Draining => write!(f, "draining"),
            ManagerStatus::Completed => write!(f, "completed"),
            ManagerStatus::Errored => write!(f, "errored"),
        }
    }
}

impl std::fmt::Display for ManagerScopeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManagerScopeKind::Branch => write!(f, "branch"),
            ManagerScopeKind::Project => write!(f, "project"),
            ManagerScopeKind::Workspace => write!(f, "workspace"),
        }
    }
}

impl std::str::FromStr for ManagerStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(ManagerStatus::Active),
            "paused" => Ok(ManagerStatus::Paused),
            "draining" => Ok(ManagerStatus::Draining),
            "completed" => Ok(ManagerStatus::Completed),
            "errored" => Ok(ManagerStatus::Errored),
            _ => Err(format!("Unknown manager status: {}", s)),
        }
    }
}

impl std::str::FromStr for ManagerScopeKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "branch" => Ok(ManagerScopeKind::Branch),
            "project" => Ok(ManagerScopeKind::Project),
            "workspace" => Ok(ManagerScopeKind::Workspace),
            _ => Err(format!("Unknown manager scope kind: {}", s)),
        }
    }
}

/// Input for creating a manager
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateManager {
    pub project_id: String,
    #[serde(default)]
    pub home_project_id: Option<String>,
    #[serde(default)]
    pub scope_kind: Option<ManagerScopeKind>,
    pub name: String,
    pub branch: String,
    pub description: Option<String>,
    pub agent_config_id: Option<String>,
    #[serde(alias = "model")]
    pub tier: Option<Model>,
    pub parent_manager_id: Option<String>,
}

/// Input for updating a manager
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateManager {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub status: Option<ManagerStatus>,
    #[serde(alias = "model")]
    pub tier: Option<Option<Model>>,
    pub agent_config_id: Option<Option<String>>,
}
