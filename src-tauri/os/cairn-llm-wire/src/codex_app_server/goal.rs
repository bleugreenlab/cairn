//! Codex native goals: the `thread/goal/*` responses and notifications.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum GoalStatus {
    Active,
    Paused,
    Blocked,
    UsageLimited,
    BudgetLimited,
    Complete,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Goal {
    pub thread_id: String,
    pub objective: String,
    pub status: GoalStatus,
    pub token_budget: Option<u64>,
    pub tokens_used: u64,
    pub time_used_seconds: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GoalResponse {
    pub goal: Option<Goal>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct GoalClearResponse {
    pub cleared: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GoalUpdatedNotification {
    pub thread_id: String,
    #[serde(default)]
    pub turn_id: Option<String>,
    pub goal: Goal,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GoalClearedNotification {
    pub thread_id: String,
}

pub fn parse_response<T: for<'de> Deserialize<'de>>(
    method: &str,
    value: serde_json::Value,
) -> Result<T, String> {
    serde_json::from_value(value)
        .map_err(|error| format!("Invalid Codex {method} response: {error}"))
}
