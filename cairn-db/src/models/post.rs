use cairn_common::identity::display::PrincipalDisplay;
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
    /// How `author` reads to a person. Resolved on the way to a surface, never
    /// stored; see [`cairn_common::identity::display`].
    #[serde(default)]
    pub author_display: Option<PrincipalDisplay>,
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
    /// How `author` reads to a person. Resolved on the way to a surface, never
    /// stored; see [`cairn_common::identity::display`].
    #[serde(default)]
    pub author_display: Option<PrincipalDisplay>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct CreatePostComment {
    pub post_id: i64,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
}
