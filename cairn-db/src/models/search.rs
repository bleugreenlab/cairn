//! Full-text search types.

use serde::{Deserialize, Serialize};

/// Content type for search results
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SearchContentType {
    Issue,
    Comment,
    Artifact,
    Event,
    Message,
}

impl std::fmt::Display for SearchContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchContentType::Issue => write!(f, "issue"),
            SearchContentType::Comment => write!(f, "comment"),
            SearchContentType::Artifact => write!(f, "artifact"),
            SearchContentType::Event => write!(f, "event"),
            SearchContentType::Message => write!(f, "message"),
        }
    }
}

impl std::str::FromStr for SearchContentType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "issue" => Ok(SearchContentType::Issue),
            "comment" => Ok(SearchContentType::Comment),
            "artifact" => Ok(SearchContentType::Artifact),
            "event" => Ok(SearchContentType::Event),
            "message" => Ok(SearchContentType::Message),
            _ => Err(format!("Unknown content type: {}", s)),
        }
    }
}

/// A single search result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchResult {
    /// ID in the source table
    pub id: String,
    /// Type of content: 'issue', 'comment', 'artifact', 'event'
    pub content_type: SearchContentType,
    /// Project ID
    pub project_id: String,
    /// Issue ID (None for project-level chats)
    pub issue_id: Option<String>,
    /// Job ID (None for issues/comments)
    pub job_id: Option<String>,
    /// Title or label for the result
    pub title: String,
    /// Highlighted snippet with match context
    pub snippet: String,
    /// Combined relevance + recency score
    pub rank: f64,
    /// Creation timestamp
    pub created_at: i64,
    /// URI for direct navigation (e.g., cairn://PROJECT/123)
    pub uri: String,
    /// Issue number for context (None for issues themselves)
    pub issue_number: Option<i32>,
    /// Issue title for context (None for issues themselves)
    pub issue_title: Option<String>,
    /// Node name for navigation (from jobs.node_name)
    pub node_name: Option<String>,
    /// Execution sequence for navigation (from executions.seq)
    pub exec_seq: Option<i32>,
}

/// Search filters for narrowing results
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SearchFilters {
    /// Filter to specific project
    pub project_id: Option<String>,
    /// Filter to specific issue
    pub issue_id: Option<String>,
    /// Filter to specific content types
    pub content_types: Option<Vec<String>>,
    /// Filter to an author-role facet: `assistant`/`user`/`tool` for events,
    /// `user`/`agent` for comments. Empty for issues/artifacts/messages.
    pub role: Option<String>,
    /// Match the query against the title field only (the `in=title` axis).
    #[serde(default)]
    pub title_only: bool,
    /// Only include results after this timestamp
    pub since: Option<i64>,
    /// Maximum results to return (default: 50, max: 100)
    pub limit: Option<usize>,
}
