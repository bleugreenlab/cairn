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
    /// The camp this post is addressed to, by its immutable id. Always within
    /// `project_id`'s project.
    #[serde(default)]
    pub camp_id: Option<String>,
    /// How `camp_id` reads to a person. Resolved on the way to a surface from
    /// the camp's own database, never stored.
    #[serde(default)]
    pub camp: Option<PostCamp>,
    pub created_at: i64,
}

/// A camp post's camp as a surface names it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PostCamp {
    pub slug: String,
    pub title: String,
}

#[derive(Debug, Clone)]
pub struct CreatePost {
    pub project_id: Option<String>,
    /// Set only by the one publish path, after checking the camp belongs to
    /// `project_id` and is live.
    pub camp_id: Option<String>,
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
