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
    Post,
    #[serde(rename = "post_comment")]
    PostComment,
}

#[cfg(test)]
mod tests {
    use super::SearchContentType;

    #[test]
    fn post_comment_content_type_uses_the_filter_vocabulary() {
        let encoded = serde_json::to_string(&SearchContentType::PostComment).unwrap();
        assert_eq!(encoded, "\"post_comment\"");
        assert_eq!(
            serde_json::from_str::<SearchContentType>(&encoded).unwrap(),
            SearchContentType::PostComment
        );
        assert_eq!(
            encoded.trim_matches('"').parse(),
            Ok(SearchContentType::PostComment)
        );
    }
}

impl std::fmt::Display for SearchContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchContentType::Issue => write!(f, "issue"),
            SearchContentType::Comment => write!(f, "comment"),
            SearchContentType::Artifact => write!(f, "artifact"),
            SearchContentType::Event => write!(f, "event"),
            SearchContentType::Message => write!(f, "message"),
            SearchContentType::Post => write!(f, "post"),
            SearchContentType::PostComment => write!(f, "post_comment"),
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
            "post" => Ok(SearchContentType::Post),
            "post_comment" => Ok(SearchContentType::PostComment),
            _ => Err(format!("Unknown content type: {}", s)),
        }
    }
}

/// A single search result.
///
/// Transcript (event) results are HOTSPOTS, not individual events: adjacent
/// matches within one node's conversation are merged into a span whose row
/// carries the peak hit's snippet, the number of matches merged (`hit_count`),
/// and the turn range the stretch covers. Every other content type produces one
/// row per document, with `hit_count` 1 and no turn range.
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
    /// Relevance score, ordered descending — but NOT on a stable scale.
    ///
    /// When only the text index answers, this is its combined relevance and
    /// recency score (BM25-derived, single digits). When the semantic lane also
    /// answers, both lanes are fused by reciprocal rank and this becomes the
    /// fused score (around 0.016), because the lanes' own units are not
    /// comparable. So the magnitude depends on whether a lane happened to
    /// answer: order by it, never threshold on it.
    pub rank: f64,
    /// Creation timestamp
    pub created_at: i64,
    /// URI for direct navigation (e.g., cairn://PROJECT/123)
    pub uri: String,
    /// Issue number for context (None for issues themselves)
    pub issue_number: Option<i32>,
    /// Issue title for context (None for issues themselves)
    pub issue_title: Option<String>,
    /// Addressable node segment for navigation (`jobs.uri_segment` of the node
    /// job; the PARENT's segment when the hit belongs to a sub-agent task).
    /// This is the segment URIs and app routes address, not the display
    /// `jobs.node_name` — `builder`, never `Builder`.
    pub node_segment: Option<String>,
    /// Task segment when the hit belongs to a sub-agent task job
    /// (`jobs.uri_segment` of the task), else None.
    pub task_segment: Option<String>,
    /// Execution sequence for navigation (from executions.seq)
    pub exec_seq: Option<i32>,
    /// Matches merged into this row: >1 only for a transcript span.
    ///
    /// For a text span this counts matching EVENTS. For a span only the
    /// semantic lane found it counts matching TURNS, since that lane retrieves
    /// turns — so a semantic-only row reports how much of the conversation was
    /// on topic, not how many times the query's words appear (which is zero, or
    /// the text lane would have found it too).
    pub hit_count: usize,
    /// First turn of a transcript span (`turns.sequence` in the node's primary
    /// session). None when the hit is not an addressable transcript span.
    pub turn_start: Option<i32>,
    /// Last turn of a transcript span; equals `turn_start` for a single turn.
    pub turn_end: Option<i32>,
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
