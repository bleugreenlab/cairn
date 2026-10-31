//! The `page_calls` audit trail: what a page's open frames did on this install.

use cairn_db::turso::params;
use serde_json::Value;

use crate::storage::{LocalDb, RowExt};

/// Rows kept per page; older ones are pruned as each new one lands.
pub(crate) const RETAINED_PER_PAGE: usize = 2_000;

/// The longest `detail` stored, in bytes.
pub(crate) const DETAIL_MAX_BYTES: usize = 8 * 1024;

/// What a logged call was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCallVerb {
    /// A one-shot bridged read.
    Read,
    /// Opening a subscription.
    Subscribe,
    /// A subscription's re-reads, counted by the host that ran them: one row
    /// per frame per minute that had any, naming each target, how often it
    /// re-read, and which table's change triggered it.
    Refresh,
    /// An uncaught error or `console.error` the frame reported.
    Diag,
    /// A navigation the host stopped.
    Navigation,
    /// A write attempt (refused until page writes land).
    Write,
    /// A call to a project service through the bridge.
    Service,
}

impl PageCallVerb {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Subscribe => "subscribe",
            Self::Refresh => "refresh",
            Self::Diag => "diag",
            Self::Navigation => "navigation",
            Self::Write => "write",
            Self::Service => "service",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "read" => Self::Read,
            "subscribe" => Self::Subscribe,
            "refresh" => Self::Refresh,
            "diag" => Self::Diag,
            "navigation" => Self::Navigation,
            "write" => Self::Write,
            "service" => Self::Service,
            _ => return None,
        })
    }
}

/// How a logged call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageCallOutcome {
    Ok,
    Refused,
    Error,
}

impl PageCallOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Refused => "refused",
            Self::Error => "error",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "ok" => Self::Ok,
            "refused" => Self::Refused,
            "error" => Self::Error,
            _ => return None,
        })
    }
}

/// One call to record.
pub(crate) struct PageCall<'a> {
    pub project_id: &'a str,
    pub page_artifact_id: &'a str,
    pub page_uri: &'a str,
    pub page_version: i32,
    pub actor: &'a Value,
    pub verb: PageCallVerb,
    pub targets: &'a [String],
    pub outcome: PageCallOutcome,
    pub detail: &'a str,
}

/// `detail` cut to [`DETAIL_MAX_BYTES`] on a character boundary.
pub(crate) fn truncate_detail(detail: &str) -> String {
    if detail.len() <= DETAIL_MAX_BYTES {
        return detail.to_string();
    }
    const MARK: &str = "\n[truncated]";
    let mut end = DETAIL_MAX_BYTES - MARK.len();
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{MARK}", &detail[..end])
}

/// Record one call, then prune the page's log to its newest rows.
pub(crate) async fn record(local: &LocalDb, call: &PageCall<'_>) -> Result<(), String> {
    record_retaining(local, call, RETAINED_PER_PAGE).await
}

pub(crate) async fn record_retaining(
    local: &LocalDb,
    call: &PageCall<'_>,
    retain: usize,
) -> Result<(), String> {
    let id = uuid::Uuid::new_v4().to_string();
    let project_id = call.project_id.to_string();
    let artifact_id = call.page_artifact_id.to_string();
    let page_uri = call.page_uri.to_string();
    let version = call.page_version;
    let actor = call.actor.to_string();
    let verb = call.verb.as_str();
    let targets = serde_json::to_string(call.targets).unwrap_or_else(|_| "[]".to_string());
    let outcome = call.outcome.as_str();
    let detail = truncate_detail(call.detail);
    let created_at = chrono::Utc::now().timestamp_millis();
    local
        .write(move |conn| {
            let (id, project_id, artifact_id, page_uri) = (
                id.clone(),
                project_id.clone(),
                artifact_id.clone(),
                page_uri.clone(),
            );
            let (actor, targets, detail) = (actor.clone(), targets.clone(), detail.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO page_calls (
                         id, project_id, page_artifact_id, page_uri, page_version, actor_json,
                         verb, targets_json, outcome, detail, created_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        id.as_str(),
                        project_id.as_str(),
                        artifact_id.as_str(),
                        page_uri.as_str(),
                        version,
                        actor.as_str(),
                        verb,
                        targets.as_str(),
                        outcome,
                        detail.as_str(),
                        created_at
                    ],
                )
                .await?;
                // The oldest row still kept; everything older goes.
                let mut cutoff = conn
                    .query(
                        "SELECT created_at, rowid FROM page_calls WHERE page_uri = ?1
                         ORDER BY created_at DESC, rowid DESC LIMIT 1 OFFSET ?2",
                        params![page_uri.as_str(), (retain as i64).saturating_sub(1)],
                    )
                    .await?;
                let boundary = match cutoff.next().await? {
                    Some(row) => Some((row.i64(0)?, row.i64(1)?)),
                    None => None,
                };
                drop(cutoff);
                if let Some((at, rowid)) = boundary {
                    conn.execute(
                        "DELETE FROM page_calls WHERE page_uri = ?1
                           AND (created_at < ?2 OR (created_at = ?2 AND rowid < ?3))",
                        params![page_uri.as_str(), at, rowid],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|error| format!("Failed to record page call: {error}"))
}

/// One recorded call.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PageCallRow {
    pub page_version: i32,
    pub actor: Value,
    pub verb: String,
    pub targets: Vec<String>,
    pub outcome: String,
    pub detail: String,
    /// Unix milliseconds.
    pub created_at: i64,
}

/// A page's calls, newest first.
pub(crate) async fn list(
    local: &LocalDb,
    page_uri: &str,
    limit: usize,
    verb: Option<PageCallVerb>,
    outcome: Option<PageCallOutcome>,
) -> Result<Vec<PageCallRow>, String> {
    local
        .query_all(
            "SELECT page_version, actor_json, verb, targets_json, outcome, detail, created_at
             FROM page_calls
             WHERE page_uri = ?1
               AND (?2 IS NULL OR verb = ?2)
               AND (?3 IS NULL OR outcome = ?3)
             ORDER BY created_at DESC, rowid DESC
             LIMIT ?4",
            params![
                page_uri,
                verb.map(PageCallVerb::as_str),
                outcome.map(PageCallOutcome::as_str),
                limit as i64
            ],
            |row| {
                Ok(PageCallRow {
                    page_version: row.i64(0)? as i32,
                    actor: serde_json::from_str(&row.text(1)?).unwrap_or(Value::Null),
                    verb: row.text(2)?,
                    targets: serde_json::from_str(&row.text(3)?).unwrap_or_default(),
                    outcome: row.text(4)?,
                    detail: row.text(5)?,
                    created_at: row.i64(6)?,
                })
            },
        )
        .await
        .map_err(|error| format!("Failed to load the page's call log: {error}"))
}
