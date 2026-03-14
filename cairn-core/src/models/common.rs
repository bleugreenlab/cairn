//! Common types used across the application.

use serde::{Deserialize, Serialize};

/// Claude model selection
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum Model {
    #[default]
    Sonnet,
    Opus,
    Haiku,
}

impl std::fmt::Display for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Model::Sonnet => write!(f, "sonnet"),
            Model::Opus => write!(f, "opus"),
            Model::Haiku => write!(f, "haiku"),
        }
    }
}

impl std::str::FromStr for Model {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sonnet" => Ok(Model::Sonnet),
            "opus" => Ok(Model::Opus),
            "haiku" => Ok(Model::Haiku),
            _ => Err(format!("Unknown model: {}", s)),
        }
    }
}

/// Merge type for pull requests
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MergeType {
    Merge,
    #[default]
    Squash,
    Rebase,
}

impl std::fmt::Display for MergeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeType::Merge => write!(f, "merge"),
            MergeType::Squash => write!(f, "squash"),
            MergeType::Rebase => write!(f, "rebase"),
        }
    }
}

impl std::str::FromStr for MergeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "merge" => Ok(MergeType::Merge),
            "squash" => Ok(MergeType::Squash),
            "rebase" => Ok(MergeType::Rebase),
            _ => Err(format!("Unknown merge type: {}", s)),
        }
    }
}
