//! The `service_log` table: what a service did, as a person reads it at
//! `cairn://p/<project>/services/<id>/log` (or, for a thread's service,
//! `cairn://p/<project>/<thread>/services/<name>/log`). Rows are keyed by
//! project, owner, and service, so a thread's service and a project service of
//! the same name keep separate logs.
//!
//! Three kinds of row share it. `lifecycle` rows are the supervisor's
//! transitions and refusals, each naming its actor. `stderr` rows are the
//! service's own log lines. `call` rows record one call each: who, which
//! method, the outcome, and how long it took, never the params or the value.
//!
//! Everything a service process wrote is untrusted and model-visible once
//! read, so text and detail pass through the structural sanitizer before they
//! are stored.

use crate::security::{RedactionPolicy, Sanitizer};
use cairn_db::turso::params;
use serde_json::Value;

use crate::storage::{LocalDb, RowExt};

/// Rows kept per service; older ones are pruned as each new one lands.
pub(crate) const RETAINED_PER_SERVICE: usize = 5_000;
/// The longest `text` stored, in bytes.
pub(crate) const TEXT_MAX_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogKind {
    Lifecycle,
    Stderr,
    Call,
}

impl LogKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            LogKind::Lifecycle => "lifecycle",
            LogKind::Stderr => "stderr",
            LogKind::Call => "call",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "lifecycle" => Some(LogKind::Lifecycle),
            "stderr" => Some(LogKind::Stderr),
            "call" => Some(LogKind::Call),
            _ => None,
        }
    }
}

/// One row to record.
#[derive(Debug, Clone)]
pub(crate) struct LogEntry<'a> {
    /// Empty for a machine service, which belongs to no project and is stored
    /// with a NULL `project_id`.
    pub project_id: &'a str,
    /// [`super::ServiceOwner::key`]: the owning thread's id, or empty.
    pub owner_thread: &'a str,
    pub service: &'a str,
    pub generation: u64,
    pub kind: LogKind,
    pub text: &'a str,
    pub detail: Value,
}

/// One stored row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LogRow {
    pub generation: i64,
    pub kind: String,
    pub text: String,
    pub detail: Value,
    pub created_at: i64,
}

/// The `project_id` a row is stored under: NULL for a machine service.
fn stored_project(project_id: &str) -> Option<String> {
    (!project_id.is_empty()).then(|| project_id.to_string())
}

fn clip(text: &mut String, max: usize) {
    const MARK: &str = "… (truncated)";
    if text.len() <= max {
        return;
    }
    let mut end = max.saturating_sub(MARK.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(MARK);
}

/// Sanitize, cap, and return what would be stored for `entry`.
pub(crate) fn prepare(entry: &LogEntry<'_>, policy: &RedactionPolicy) -> (String, String) {
    let mut sanitizer = Sanitizer::structural(policy);
    let mut text = entry.text.to_string();
    sanitizer.text_in_place(&mut text);
    clip(&mut text, TEXT_MAX_BYTES);
    let mut detail = entry.detail.clone();
    sanitizer.json(&mut detail);
    let mut detail = serde_json::to_string(&detail).unwrap_or_else(|_| "{}".to_string());
    if detail.len() > TEXT_MAX_BYTES {
        detail = serde_json::json!({ "truncated": true }).to_string();
    }
    (text, detail)
}

/// Record one row, then prune the service's log to its newest rows.
pub(crate) async fn record(
    db: &LocalDb,
    policy: &RedactionPolicy,
    entry: &LogEntry<'_>,
) -> Result<(), String> {
    record_retaining(db, policy, entry, RETAINED_PER_SERVICE).await
}

pub(crate) async fn record_retaining(
    db: &LocalDb,
    policy: &RedactionPolicy,
    entry: &LogEntry<'_>,
    retain: usize,
) -> Result<(), String> {
    let (text, detail) = prepare(entry, policy);
    let project_id = stored_project(entry.project_id);
    let owner = entry.owner_thread.to_string();
    let service = entry.service.to_string();
    let generation = entry.generation as i64;
    let kind = entry.kind.as_str();
    let created_at = chrono::Utc::now().timestamp_millis();
    db.write(move |conn| {
        let (project_id, owner, service, text, detail) = (
            project_id.clone(),
            owner.clone(),
            service.clone(),
            text.clone(),
            detail.clone(),
        );
        Box::pin(async move {
            conn.execute(
                "INSERT INTO service_log (project_id, owner_thread, service, generation, kind, text, detail_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    project_id.clone(),
                    owner.as_str(),
                    service.as_str(),
                    generation,
                    kind,
                    text.as_str(),
                    detail.as_str(),
                    created_at
                ],
            )
            .await?;
            let mut cutoff = conn
                .query(
                    "SELECT id FROM service_log
                     WHERE project_id IS ?1 AND service = ?2 AND owner_thread = ?3
                     ORDER BY id DESC LIMIT 1 OFFSET ?4",
                    params![
                        project_id.clone(),
                        service.as_str(),
                        owner.as_str(),
                        (retain as i64).saturating_sub(1)
                    ],
                )
                .await?;
            let boundary = match cutoff.next().await? {
                Some(row) => Some(row.i64(0)?),
                None => None,
            };
            drop(cutoff);
            if let Some(id) = boundary {
                conn.execute(
                    "DELETE FROM service_log
                     WHERE project_id IS ?1 AND service = ?2 AND owner_thread = ?3 AND id < ?4",
                    params![project_id.clone(), service.as_str(), owner.as_str(), id],
                )
                .await?;
            }
            Ok(())
        })
    })
    .await
    .map_err(|error| format!("Failed to record service log row: {error}"))
}

/// A service's log rows, oldest first: the newest `limit` that match.
pub(crate) async fn list(
    db: &LocalDb,
    project_id: &str,
    owner_thread: &str,
    service: &str,
    since_ms: Option<i64>,
    kind: Option<LogKind>,
    limit: usize,
) -> Result<Vec<LogRow>, String> {
    let project_id = stored_project(project_id);
    let owner = owner_thread.to_string();
    let service = service.to_string();
    let since = since_ms.unwrap_or(i64::MIN);
    let kind = kind.map(LogKind::as_str).unwrap_or("");
    let limit = limit.max(1) as i64;
    let mut rows = db
        .read(move |conn| {
            let (project_id, owner, service) = (project_id.clone(), owner.clone(), service.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT generation, kind, text, detail_json, created_at FROM service_log
                         WHERE project_id IS ?1 AND service = ?2 AND owner_thread = ?3
                           AND created_at >= ?4 AND (?5 = '' OR kind = ?5)
                         ORDER BY id DESC LIMIT ?6",
                        params![
                            project_id.clone(),
                            service.as_str(),
                            owner.as_str(),
                            since,
                            kind,
                            limit
                        ],
                    )
                    .await?;
                let mut out = Vec::new();
                while let Some(row) = rows.next().await? {
                    out.push(LogRow {
                        generation: row.i64(0)?,
                        kind: row.text(1)?,
                        text: row.text(2)?,
                        detail: serde_json::from_str(&row.text(3)?).unwrap_or(Value::Null),
                        created_at: row.i64(4)?,
                    });
                }
                Ok(out)
            })
        })
        .await
        .map_err(|error| format!("Failed to read service log: {error}"))?;
    rows.reverse();
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn text_is_capped_on_a_char_boundary() {
        let mut text = "é".repeat(TEXT_MAX_BYTES);
        clip(&mut text, TEXT_MAX_BYTES);
        assert!(text.len() <= TEXT_MAX_BYTES);
        assert!(text.ends_with("(truncated)"));
    }

    #[test]
    fn credentials_in_text_and_detail_are_redacted() {
        let policy = RedactionPolicy::default();
        let entry = LogEntry {
            project_id: "p",
            owner_thread: "",
            service: "echo",
            generation: 1,
            kind: LogKind::Stderr,
            text: "calling with Authorization: Bearer sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            detail: json!({"authorization": "Bearer sk-ant-api03-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}),
        };
        let (text, detail) = prepare(&entry, &policy);
        assert!(!text.contains("sk-ant-api03-AAAA"), "{text}");
        assert!(!detail.contains("sk-ant-api03-AAAA"), "{detail}");
    }

    async fn db() -> (tempfile::TempDir, LocalDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = LocalDb::open_migrated_for_test(dir.path().join("cairn.turso.db"))
            .await
            .unwrap();
        crate::project_services::testing::seed_project(&db).await;
        (dir, db)
    }

    #[tokio::test]
    async fn rows_list_oldest_first_filter_by_kind_and_prune() {
        let (_dir, db) = db().await;
        let policy = RedactionPolicy::default();
        for (index, kind) in [
            LogKind::Lifecycle,
            LogKind::Stderr,
            LogKind::Call,
            LogKind::Stderr,
        ]
        .into_iter()
        .enumerate()
        {
            let text = format!("row {index}");
            record_retaining(
                &db,
                &policy,
                &LogEntry {
                    project_id: "p",
                    owner_thread: "",
                    service: "echo",
                    generation: 1,
                    kind,
                    text: &text,
                    detail: json!({}),
                },
                3,
            )
            .await
            .unwrap();
        }
        let rows = list(&db, "p", "", "echo", None, None, 100).await.unwrap();
        let texts: Vec<&str> = rows.iter().map(|row| row.text.as_str()).collect();
        assert_eq!(texts, vec!["row 1", "row 2", "row 3"]);
        let stderr = list(&db, "p", "", "echo", None, Some(LogKind::Stderr), 100)
            .await
            .unwrap();
        assert_eq!(stderr.len(), 2);
        let newest = list(&db, "p", "", "echo", None, None, 1).await.unwrap();
        assert_eq!(newest[0].text, "row 3");
        assert!(list(&db, "p", "", "other", None, None, 10)
            .await
            .unwrap()
            .is_empty());
    }

    /// A thread's service keeps its own log beside a project service of the
    /// same name, and pruning one never touches the other.
    #[tokio::test]
    async fn a_thread_service_and_a_project_service_of_one_name_keep_separate_logs() {
        let (_dir, db) = db().await;
        let policy = RedactionPolicy::default();
        for (owner, text) in [
            ("", "project"),
            ("thread-1", "thread a"),
            ("thread-1", "thread b"),
        ] {
            record_retaining(
                &db,
                &policy,
                &LogEntry {
                    project_id: "p",
                    owner_thread: owner,
                    service: "echo",
                    generation: 1,
                    kind: LogKind::Lifecycle,
                    text,
                    detail: json!({}),
                },
                1,
            )
            .await
            .unwrap();
        }
        let project = list(&db, "p", "", "echo", None, None, 10).await.unwrap();
        assert_eq!(project.len(), 1);
        assert_eq!(project[0].text, "project");
        let thread = list(&db, "p", "thread-1", "echo", None, None, 10)
            .await
            .unwrap();
        assert_eq!(thread.len(), 1, "pruned to one row");
        assert_eq!(thread[0].text, "thread b");
    }
}
