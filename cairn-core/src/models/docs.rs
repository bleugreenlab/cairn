//! Documentation types.

use serde::{Deserialize, Serialize};

/// Represents a documentation file or directory in the repo
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocFile {
    pub path: String,
    pub name: String,
    pub is_directory: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<DocFile>>,
}

/// Link between a documentation file and an issue
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocReference {
    pub id: String,
    pub issue_id: String,
    pub doc_path: String,
    pub created_at: i64,
}

/// Content of a documentation file
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocContent {
    pub path: String,
    pub content: String,
}
