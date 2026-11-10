//! Cheap candidate discovery for workflow dependency-failure repair.
//!
//! Candidates are not proof of impossibility. The workflow owner must load and
//! migrate each candidate's snapshot and recheck eligibility in its transaction.

use super::{DbResult, LocalDb, RowExt, TrackedConnection};
use crate::turso::params;

const CANDIDATES_SQL: &str = "
    SELECT DISTINCT pending.execution_id
    FROM jobs pending INDEXED BY idx_jobs_status_execution
    WHERE pending.status = 'pending'
      AND pending.execution_id IS NOT NULL
      AND pending.execution_id > ?1
      AND EXISTS (
          SELECT 1 FROM jobs failed INDEXED BY idx_jobs_status_execution
          WHERE failed.status = 'failed'
            AND failed.execution_id IS NOT NULL
            AND failed.execution_id = pending.execution_id
      )
    ORDER BY pending.execution_id
    LIMIT ?2";

/// A keyset page of executions containing both a pending job and a failed job.
/// Reads jobs alone, without touching execution rows or their snapshots.
/// Historical failed attempts can be false positives; this never settles work.
pub async fn failed_dependency_candidates_conn(
    conn: &TrackedConnection,
    after_execution_id: &str,
    limit: u32,
) -> DbResult<Vec<String>> {
    let mut rows = conn
        .query(
            CANDIDATES_SQL,
            params![after_execution_id, i64::from(limit)],
        )
        .await?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next().await? {
        ids.push(row.text(0)?);
    }
    Ok(ids)
}

/// Database wrapper for [`failed_dependency_candidates_conn`].
pub async fn failed_dependency_candidates(
    db: &LocalDb,
    after_execution_id: &str,
    limit: u32,
) -> DbResult<Vec<String>> {
    let after = after_execution_id.to_owned();
    db.read(move |conn| {
        Box::pin(async move { failed_dependency_candidates_conn(conn, &after, limit).await })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{migrated_test_db, query_plan};

    #[tokio::test]
    async fn candidate_discovery_is_jobs_only_and_covering() {
        let db = migrated_test_db("dependency-candidate-plan").await;
        let plan = query_plan(&db, CANDIDATES_SQL, vec!["".into(), 32_i64.into()]).await;
        assert!(
            plan.contains("COVERING INDEX idx_jobs_status_execution"),
            "candidate discovery must read narrow job facts, not job rows: {plan}"
        );
        assert!(!plan.contains("executions"), "{plan}");
        assert!(!plan.contains("SORTER"), "{plan}");
        assert!(!plan.contains("ephemeral"), "{plan}");
    }

    #[tokio::test]
    async fn candidates_are_distinct_keyset_pages_not_impossibility_verdicts() {
        let db = migrated_test_db("dependency-candidates").await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
             INSERT INTO executions(id, recipe_id, project_id, status, started_at, seq, snapshot)
             VALUES('a', 'r', 'p', 'running', 1, 1, 'not valid json'),
                   ('b', 'r', 'p', 'running', 1, 2, 'not valid json'),
                   ('c', 'r', 'p', 'running', 1, 3, 'not valid json'),
                   ('d', 'r', 'p', 'running', 1, 4, 'not valid json');
             INSERT INTO jobs(id, execution_id, project_id, status, created_at, updated_at)
             VALUES('a-failed', 'a', 'p', 'failed', 1, 1),
                   ('a-pending', 'a', 'p', 'pending', 1, 1),
                   ('a-pending2', 'a', 'p', 'pending', 1, 1),
                   ('b-failed', 'b', 'p', 'failed', 1, 1),
                   ('b-pending', 'b', 'p', 'pending', 1, 1),
                   ('b-complete-retry', 'b', 'p', 'complete', 2, 2),
                   ('c-pending', 'c', 'p', 'pending', 1, 1),
                   ('d-failed', 'd', 'p', 'failed', 1, 1),
                   ('d-blocked', 'd', 'p', 'blocked', 1, 1),
                   ('orphan-pending', NULL, 'p', 'pending', 1, 1),
                   ('orphan-failed', NULL, 'p', 'failed', 1, 1);",
        )
        .await
        .unwrap();

        assert_eq!(
            failed_dependency_candidates(&db, "", 10).await.unwrap(),
            ["a", "b"]
        );
        assert_eq!(
            failed_dependency_candidates(&db, "", 1).await.unwrap(),
            ["a"]
        );
        assert_eq!(
            failed_dependency_candidates(&db, "a", 1).await.unwrap(),
            ["b"]
        );
        assert!(failed_dependency_candidates(&db, "b", 1)
            .await
            .unwrap()
            .is_empty());
        assert!(failed_dependency_candidates(&db, "", 0)
            .await
            .unwrap()
            .is_empty());

        db.write(|conn| Box::pin(async move {
            conn.execute("UPDATE jobs SET status = 'failed' WHERE execution_id = 'a' AND status = 'pending'", ()).await?;
            assert_eq!(failed_dependency_candidates_conn(conn, "", 10).await?, ["b"]);
            Ok(())
        })).await.unwrap();
    }
}
