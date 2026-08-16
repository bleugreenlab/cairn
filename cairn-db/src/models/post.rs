use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Post {
    pub id: i64,
    pub project_id: Option<String>,
    pub title: Option<String>,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePost {
    pub project_id: Option<String>,
    pub title: Option<String>,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PostComment {
    pub id: i64,
    pub post_id: i64,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePostComment {
    pub post_id: i64,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
}
