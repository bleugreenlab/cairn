//! CAIRN-2487: durable journal for the workflow harness's `agent()` calls.
//!
//! A workflow node's script drives ephemeral `agent()` calls straight-line, each
//! assigned a monotonic dispatch `ordinal` by the harness. This module memoizes
//! each call's validated result in the `workflow_journal` table keyed by
//! `(run_id, ordinal)`, so a host-restart replay (which re-runs the script from
//! the top) short-circuits already-resolved calls: at each ordinal a matching
//! [`hash_prompt`] returns the journaled result with no new call job, while a
//! mismatch is a cache miss that runs the call live and overwrites the row.
//!
//! The journal is per-machine runner-transient state (Private in `TABLE_SCOPES`),
//! so losing it degrades gracefully to cache-miss re-runs, never to corruption.
//! The store/get functions here are the substrate; the calls spawn path wires
//! the memo check and the finalize-time write in a later stage.
#![allow(dead_code)]

use cairn_db::turso::params;
use sha2::{Digest, Sha256};

use crate::storage::{LocalDb, RowExt};

/// Whether a journaled `agent()` call resolved to a validated result or failed
/// (a call error or a schema-violating result). A failure journals
/// `result_json = NULL` and replays as `null` without re-spawning the call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalStatus {
    Success,
    Failure,
}

impl JournalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            JournalStatus::Success => "success",
            JournalStatus::Failure => "failure",
        }
    }
}

/// A memoized `agent()` result for a workflow run at a given dispatch ordinal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEntry {
    /// Guard hash of the prompt that produced this result. A replay whose prompt
    /// hashes differently at the same ordinal is a cache miss.
    pub prompt_hash: String,
    /// The validated result JSON, or `None` for a journaled failure.
    pub result_json: Option<String>,
    pub status: JournalStatus,
}

/// Stable guard hash of an `agent()` prompt. Single-sourced here so the write
/// path and the memo-check path can never disagree on the key.
pub fn hash_prompt(prompt: &str) -> String {
    format!("{:x}", Sha256::digest(prompt.as_bytes()))
}

/// Fetch the journal entry for a workflow run at a dispatch ordinal, if present.
pub async fn get_entry(
    db: &LocalDb,
    run_id: &str,
    ordinal: i64,
) -> Result<Option<JournalEntry>, String> {
    let run_id = run_id.to_string();
    db.read(|conn| {
        let run_id = run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT prompt_hash, result_json, status FROM workflow_journal \
                     WHERE run_id = ?1 AND ordinal = ?2",
                    params![run_id.as_str(), ordinal],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                // The CHECK constraint guarantees status is 'success' | 'failure'.
                let status = if row.text(2)? == "success" {
                    JournalStatus::Success
                } else {
                    JournalStatus::Failure
                };
                Ok(Some(JournalEntry {
                    prompt_hash: row.text(0)?,
                    result_json: row.opt_text(1)?,
                    status,
                }))
            } else {
                Ok(None)
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Persist (or overwrite) the journal entry for a workflow run at a dispatch
/// ordinal. The id is deterministic in `(run_id, ordinal)`, so a cache-miss
/// overwrite at the same ordinal replaces the prior row via `INSERT OR REPLACE`.
pub async fn store_entry(
    db: &LocalDb,
    run_id: &str,
    ordinal: i64,
    prompt_hash: &str,
    result_json: Option<&str>,
    status: JournalStatus,
) -> Result<(), String> {
    let id = format!("{run_id}#{ordinal}");
    let run_id = run_id.to_string();
    let prompt_hash = prompt_hash.to_string();
    let result_json = result_json.map(str::to_string);
    let status_str = status.as_str();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let id = id.clone();
        let run_id = run_id.clone();
        let prompt_hash = prompt_hash.clone();
        let result_json = result_json.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR REPLACE INTO workflow_journal \
                 (id, run_id, ordinal, prompt_hash, result_json, status, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    id.as_str(),
                    run_id.as_str(),
                    ordinal,
                    prompt_hash.as_str(),
                    result_json.as_deref(),
                    status_str,
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The journal key an in-flight ephemeral call must write when it finalizes:
/// the parent workflow run and the harness dispatch ordinal, plus the
/// prompt_hash guard. Recorded in `workflow_call` when a workflow-parented call
/// is spawned live (a journal cache miss), then read once and deleted at the
/// call's completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallLink {
    pub workflow_run_id: String,
    pub ordinal: i64,
    pub prompt_hash: String,
}

/// Record the journal key for a live workflow-parented call, keyed by the call's
/// own run id. Best-effort: a failure never fails the call spawn.
pub async fn store_call_link(
    db: &LocalDb,
    call_run_id: &str,
    link: &CallLink,
) -> Result<(), String> {
    let call_run_id = call_run_id.to_string();
    let workflow_run_id = link.workflow_run_id.clone();
    let ordinal = link.ordinal;
    let prompt_hash = link.prompt_hash.clone();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let call_run_id = call_run_id.clone();
        let workflow_run_id = workflow_run_id.clone();
        let prompt_hash = prompt_hash.clone();
        Box::pin(async move {
            conn.execute(
                "INSERT OR REPLACE INTO workflow_call \
                 (run_id, workflow_run_id, ordinal, prompt_hash, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    call_run_id.as_str(),
                    workflow_run_id.as_str(),
                    ordinal,
                    prompt_hash.as_str(),
                    now
                ],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Fetch the journal key for a call run, if it was a live workflow-parented call.
pub async fn load_call_link(db: &LocalDb, call_run_id: &str) -> Result<Option<CallLink>, String> {
    let call_run_id = call_run_id.to_string();
    db.read(|conn| {
        let call_run_id = call_run_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT workflow_run_id, ordinal, prompt_hash FROM workflow_call \
                     WHERE run_id = ?1",
                    params![call_run_id.as_str()],
                )
                .await?;
            if let Some(row) = rows.next().await? {
                Ok(Some(CallLink {
                    workflow_run_id: row.text(0)?,
                    ordinal: row.i64(1)?,
                    prompt_hash: row.text(2)?,
                }))
            } else {
                Ok(None)
            }
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Delete a consumed call link. Called after the completion path stores the
/// journal entry, so a re-entrant finalize does not re-store.
pub async fn delete_call_link(db: &LocalDb, call_run_id: &str) -> Result<(), String> {
    let call_run_id = call_run_id.to_string();
    db.write(|conn| {
        let call_run_id = call_run_id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM workflow_call WHERE run_id = ?1",
                params![call_run_id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("workflow-journal.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn missing_entry_is_none() {
        let db = test_db().await;
        assert_eq!(get_entry(&db, "run-1", 0).await.unwrap(), None);
    }

    #[tokio::test]
    async fn store_and_get_roundtrips_a_success() {
        let db = test_db().await;
        let hash = hash_prompt("do the thing");
        store_entry(
            &db,
            "run-1",
            3,
            &hash,
            Some("{\"score\":42}"),
            JournalStatus::Success,
        )
        .await
        .unwrap();

        let entry = get_entry(&db, "run-1", 3).await.unwrap().unwrap();
        assert_eq!(entry.prompt_hash, hash);
        assert_eq!(entry.result_json.as_deref(), Some("{\"score\":42}"));
        assert_eq!(entry.status, JournalStatus::Success);
        // A different ordinal is a distinct key.
        assert_eq!(get_entry(&db, "run-1", 4).await.unwrap(), None);
    }

    #[tokio::test]
    async fn failure_journals_null_result() {
        let db = test_db().await;
        store_entry(
            &db,
            "run-1",
            0,
            &hash_prompt("p"),
            None,
            JournalStatus::Failure,
        )
        .await
        .unwrap();
        let entry = get_entry(&db, "run-1", 0).await.unwrap().unwrap();
        assert_eq!(entry.result_json, None);
        assert_eq!(entry.status, JournalStatus::Failure);
    }

    #[tokio::test]
    async fn call_link_roundtrips_and_deletes() {
        let db = test_db().await;
        assert_eq!(load_call_link(&db, "call-run").await.unwrap(), None);
        let link = CallLink {
            workflow_run_id: "wf-run".to_string(),
            ordinal: 5,
            prompt_hash: hash_prompt("analyze the corpus"),
        };
        store_call_link(&db, "call-run", &link).await.unwrap();
        assert_eq!(load_call_link(&db, "call-run").await.unwrap(), Some(link));
        // The link is consumed once at completion, so deletion makes a re-entrant
        // finalize a no-op.
        delete_call_link(&db, "call-run").await.unwrap();
        assert_eq!(load_call_link(&db, "call-run").await.unwrap(), None);
    }

    #[tokio::test]
    async fn overwrite_at_same_ordinal_replaces_the_row() {
        let db = test_db().await;
        let first = hash_prompt("first prompt");
        store_entry(
            &db,
            "run-1",
            0,
            &first,
            Some("\"a\""),
            JournalStatus::Success,
        )
        .await
        .unwrap();
        // A cache-miss replay at the same ordinal with a different prompt hash
        // overwrites in place rather than inserting a duplicate.
        let second = hash_prompt("changed prompt");
        store_entry(
            &db,
            "run-1",
            0,
            &second,
            Some("\"b\""),
            JournalStatus::Success,
        )
        .await
        .unwrap();
        let entry = get_entry(&db, "run-1", 0).await.unwrap().unwrap();
        assert_eq!(entry.prompt_hash, second);
        assert_eq!(entry.result_json.as_deref(), Some("\"b\""));
    }
}
