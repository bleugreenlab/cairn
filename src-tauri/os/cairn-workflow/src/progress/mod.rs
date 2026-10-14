//! CAIRN-2499: durable phase/log progress timeline for a workflow node.
//!
//! A workflow node's script drives ephemeral `agent()` calls and, between them,
//! signals its progress with the harness `phase(name)` / `log(msg)` verbs. Those
//! verbs append typed entries to the `workflow_progress` table keyed by the
//! workflow node's `job_id` with a monotonic per-job `seq`. The workflow
//! monitoring panel reads them back as an ordered phase spine (the `phase`
//! entries) plus a log stream (the `log` entries).
//!
//! Progress is durable typed execution state, not transcript events: keeping it
//! out of the `events` stream keeps chat reconstruction and the analytics
//! token/tool extraction (which both scan `events`) clean. `workflow_progress`
//! is a project-scoped SHARED table living alongside the jobs/runs it is joined
//! against, so it resolves through the same DB routing (`for_project` on write,
//! `owning_db_for_job` on read) and a team workflow's progress syncs to
//! teammates rather than being stranded on the runner's private database.
//!
//! Every write is best-effort from the script's side (a failed progress write
//! must never fail the workflow), so these functions surface errors to the
//! dispatch layer, which swallows them for the caller.

use cairn_db::turso::params;

use crate::storage::{LocalDb, RowExt};

pub mod monitor;

/// One appended progress entry: a `phase` boundary (its `text` is the phase
/// name) or a `log` line (its `text` is the message).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressEntry {
    /// Monotonic per-job append order.
    pub seq: i64,
    /// `"phase"` or `"log"` (guarded by the table CHECK constraint).
    pub kind: String,
    /// The phase name (kind=`phase`) or the log message (kind=`log`).
    pub text: String,
    pub created_at: i64,
}

/// Append a progress entry for a workflow node's job, allocating the next
/// monotonic `seq`. Returns the inserted row.
pub async fn append_entry(
    db: &LocalDb,
    job_id: &str,
    kind: &str,
    text: &str,
) -> Result<ProgressEntry, String> {
    let job_id = job_id.to_string();
    let kind = kind.to_string();
    let text = text.to_string();
    let now = chrono::Utc::now().timestamp();
    db.write(|conn| {
        let job_id = job_id.clone();
        let kind = kind.clone();
        let text = text.clone();
        Box::pin(async move {
            // Allocate the next per-job seq. Workflow progress writes originate
            // from one straight-line script, so there is no concurrent-writer
            // race; the UNIQUE(job_id, seq) constraint is the backstop.
            let mut rows = conn
                .query(
                    "SELECT COALESCE(MAX(seq), -1) FROM workflow_progress WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let next_seq = match rows.next().await? {
                Some(row) => row.i64(0)? + 1,
                None => 0,
            };
            let id = format!("{job_id}:{next_seq}");
            conn.execute(
                "INSERT INTO workflow_progress \
                 (id, job_id, seq, kind, text, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id.as_str(),
                    job_id.as_str(),
                    next_seq,
                    kind.as_str(),
                    text.as_str(),
                    now
                ],
            )
            .await?;
            Ok(ProgressEntry {
                seq: next_seq,
                kind,
                text,
                created_at: now,
            })
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// List a workflow node job's progress entries in append order (oldest first).
pub async fn list_entries(db: &LocalDb, job_id: &str) -> Result<Vec<ProgressEntry>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT seq, kind, text, created_at FROM workflow_progress \
                     WHERE job_id = ?1 ORDER BY seq ASC",
                    params![job_id.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(ProgressEntry {
                    seq: row.i64(0)?,
                    kind: row.text(1)?,
                    text: row.text(2)?,
                    created_at: row.i64(3)?,
                });
            }
            Ok(out)
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
        let db = LocalDb::open(temp.path().join("workflow-progress.db"))
            .await
            .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn empty_job_lists_nothing() {
        let db = test_db().await;
        assert_eq!(list_entries(&db, "job-1").await.unwrap(), vec![]);
    }

    #[tokio::test]
    async fn append_allocates_monotonic_seq_and_roundtrips_in_order() {
        let db = test_db().await;
        let a = append_entry(&db, "job-1", "phase", "scope").await.unwrap();
        let b = append_entry(&db, "job-1", "log", "3 angles").await.unwrap();
        let c = append_entry(&db, "job-1", "phase", "search").await.unwrap();
        assert_eq!((a.seq, b.seq, c.seq), (0, 1, 2));

        let entries = list_entries(&db, "job-1").await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].kind, "phase");
        assert_eq!(entries[0].text, "scope");
        assert_eq!(entries[1].kind, "log");
        assert_eq!(entries[1].text, "3 angles");
        assert_eq!(entries[2].text, "search");
    }

    #[tokio::test]
    async fn seq_is_per_job() {
        let db = test_db().await;
        append_entry(&db, "job-1", "phase", "scope").await.unwrap();
        let other = append_entry(&db, "job-2", "phase", "scope").await.unwrap();
        // A distinct job starts its own seq at 0.
        assert_eq!(other.seq, 0);
        assert_eq!(list_entries(&db, "job-1").await.unwrap().len(), 1);
        assert_eq!(list_entries(&db, "job-2").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn kind_check_constraint_rejects_unknown_kind() {
        let db = test_db().await;
        assert!(append_entry(&db, "job-1", "bogus", "x").await.is_err());
    }
}
