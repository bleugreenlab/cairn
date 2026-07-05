//! Artifact query operations.

use cairn_db::turso::params;
use serde_json::Value;

use crate::models::Artifact;
use crate::storage::{DbError, LocalDb, RowExt};

pub(crate) const ARTIFACT_COLUMNS: &str =
    "id, job_id, artifact_type, schema_version, data, version,
    parent_version_id, output_name, created_at, updated_at, seen_at, confirmed";

fn db_error(context: &str, error: DbError) -> String {
    format!("{context}: {error}")
}

pub(crate) fn artifact_from_row(row: &cairn_db::turso::Row) -> Result<Artifact, DbError> {
    let data: Value = serde_json::from_str(&row.text(4)?)
        .map_err(|e| DbError::internal(format!("Invalid artifact data JSON: {e}")))?;

    Ok(Artifact {
        id: row.text(0)?,
        job_id: row.opt_text(1)?,
        artifact_type: row.text(2)?,
        schema_version: row.i64(3)? as i32,
        data,
        version: row.i64(5)? as i32,
        parent_version_id: row.opt_text(6)?,
        output_name: row.opt_text(7)?,
        created_at: row.i64(8)?,
        updated_at: row.i64(9)?,
        seen_at: row.opt_i64(10)?,
        confirmed: row.i64(11)? != 0,
    })
}

pub async fn get(db: &LocalDb, artifact_id: &str) -> Result<Artifact, String> {
    let artifact_id = artifact_id.to_string();
    db.query_opt(
        format!("SELECT {ARTIFACT_COLUMNS} FROM artifacts WHERE id = ?1"),
        params![artifact_id.as_str()],
        artifact_from_row,
    )
    .await
    .and_then(|artifact| {
        artifact.ok_or_else(|| DbError::internal(format!("Artifact not found: {artifact_id}")))
    })
    .map_err(|e| db_error("Failed to get artifact", e))
}

pub async fn list(db: &LocalDb, job_id: &str) -> Result<Vec<Artifact>, String> {
    let job_id = job_id.to_string();
    db.query_all(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM artifacts
             WHERE job_id = ?1
             ORDER BY version DESC"
        ),
        params![job_id.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to list artifacts", e))
}

/// The job's most recently written artifact, across all output names. `version`
/// is scoped per `(job_id, output_name)`, so ordering by version would compare
/// version numbers across unrelated names (a ctx-self living doc patched to v3
/// would outrank a freshly created terminal output at v1). Order by write
/// recency instead: `created_at` (seconds) with `rowid` as the monotonic
/// insertion-order tiebreaker for same-second writes.
pub async fn get_latest(db: &LocalDb, job_id: &str) -> Result<Option<Artifact>, String> {
    let job_id = job_id.to_string();
    db.query_opt(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM artifacts
             WHERE job_id = ?1
             ORDER BY created_at DESC, rowid DESC
             LIMIT 1"
        ),
        params![job_id.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to get latest artifact", e))
}

/// The latest version within a single name's chain. Unlike [`get_latest`]
/// (which orders across names by write recency), `version` is scoped per
/// `(job_id, output_name)`, so within one name the highest version is the
/// newest — order by `version DESC`. This mirrors the resource-layer
/// `get_named_artifact_for_job` and lets the frontend address a named artifact
/// (`.../{node}/board`, `/plan`) even when it is not the node's latest output.
pub async fn get_named(
    db: &LocalDb,
    job_id: &str,
    output_name: &str,
) -> Result<Option<Artifact>, String> {
    let job_id = job_id.to_string();
    let output_name = output_name.to_string();
    db.query_opt(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM artifacts
             WHERE job_id = ?1 AND output_name = ?2
             ORDER BY version DESC
             LIMIT 1"
        ),
        params![job_id.as_str(), output_name.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to get named artifact", e))
}

pub async fn mark_seen(db: &LocalDb, artifact_id: &str) -> Result<(), String> {
    let seen_at = chrono::Utc::now().timestamp();
    mark_seen_at(db, artifact_id, seen_at).await
}

pub async fn mark_seen_at(db: &LocalDb, artifact_id: &str, seen_at: i64) -> Result<(), String> {
    let artifact_id = artifact_id.to_string();
    db.execute(
        "UPDATE artifacts SET seen_at = ?1 WHERE id = ?2",
        params![seen_at, artifact_id.as_str()],
    )
    .await
    .map(|_| ())
    .map_err(|e| db_error("Failed to mark artifact seen", e))
}

pub async fn list_for_issue(db: &LocalDb, issue_id: &str) -> Result<Vec<Artifact>, String> {
    let issue_id = issue_id.to_string();
    db.query_all(
        format!(
            "SELECT {ARTIFACT_COLUMNS}
             FROM (
                SELECT
                    a.id, a.job_id, a.artifact_type, a.schema_version, a.data,
                    a.version, a.parent_version_id, a.output_name, a.created_at,
                    a.updated_at, a.seen_at, a.confirmed,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.job_id, a.output_name
                        ORDER BY a.version DESC
                    ) AS artifact_rank
                FROM artifacts a
                INNER JOIN jobs j ON j.id = a.job_id
                WHERE j.issue_id = ?1
             ) ranked_artifacts
             WHERE artifact_rank = 1"
        ),
        params![issue_id.as_str()],
        artifact_from_row,
    )
    .await
    .map_err(|e| db_error("Failed to list artifacts for issue", e))
}

#[cfg(test)]
mod latest_recency_tests {
    use super::*;

    async fn seed_job(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues (id,project_id,number,title,status,attention,created_at,updated_at) VALUES ('i','p',1,'T','active','none',1,1);
             INSERT INTO jobs (id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at) VALUES ('j','i','p','running','b','b',1,1);",
        )
        .await
        .unwrap();
    }

    async fn insert_artifact(
        db: &LocalDb,
        id: &str,
        output_name: &str,
        version: i64,
        created_at: i64,
    ) {
        db.execute(
            "INSERT INTO artifacts (id,job_id,artifact_type,schema_version,data,version,output_name,created_at,updated_at,confirmed)
             VALUES (?1,'j',?2,1,'{}',?3,?2,?4,?4,1)",
            params![id, output_name, version, created_at],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn latest_is_most_recent_name_not_highest_version() {
        // A coordinator-shaped job: a ctx-self `board` patched to v2, then a
        // terminal `create-pr` written later at v1. `version` is per-name, so the
        // board outranks create-pr by version number; recency ordering must still
        // return the newly written create-pr.
        let db = crate::storage::migrated_test_db("latest-recency-name.db").await;
        seed_job(&db).await;
        insert_artifact(&db, "board-1", "board", 1, 10).await;
        insert_artifact(&db, "board-2", "board", 2, 20).await;
        insert_artifact(&db, "pr-1", "create-pr", 1, 30).await;

        let latest = get_latest(&db, "j").await.unwrap().expect("an artifact");
        assert_eq!(latest.output_name.as_deref(), Some("create-pr"));
        assert_eq!(latest.version, 1);
    }

    #[tokio::test]
    async fn rowid_breaks_same_timestamp_ties_by_insertion_order() {
        // Same-second writes: created_at ties, so the monotonic `rowid` tiebreaker
        // must pick the last-inserted row (the create-pr), not the highest version.
        let db = crate::storage::migrated_test_db("latest-recency-tie.db").await;
        seed_job(&db).await;
        insert_artifact(&db, "board-1", "board", 1, 5).await;
        insert_artifact(&db, "board-2", "board", 2, 5).await;
        insert_artifact(&db, "pr-1", "create-pr", 1, 5).await;

        let latest = get_latest(&db, "j").await.unwrap().expect("an artifact");
        assert_eq!(latest.output_name.as_deref(), Some("create-pr"));
    }

    #[tokio::test]
    async fn get_named_returns_requested_name_not_recency_winner() {
        // Two names present: a `board` patched to v2, then a `create-pr` written
        // later. `get_latest` returns create-pr by recency; `get_named` must
        // return the requested name's latest version regardless of recency.
        let db = crate::storage::migrated_test_db("get-named.db").await;
        seed_job(&db).await;
        insert_artifact(&db, "board-1", "board", 1, 10).await;
        insert_artifact(&db, "board-2", "board", 2, 20).await;
        insert_artifact(&db, "pr-1", "create-pr", 1, 30).await;

        let board = get_named(&db, "j", "board")
            .await
            .unwrap()
            .expect("board artifact");
        assert_eq!(board.output_name.as_deref(), Some("board"));
        assert_eq!(board.version, 2);

        let pr = get_named(&db, "j", "create-pr")
            .await
            .unwrap()
            .expect("create-pr artifact");
        assert_eq!(pr.output_name.as_deref(), Some("create-pr"));
        assert_eq!(pr.version, 1);

        assert!(get_named(&db, "j", "missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn single_name_chain_returns_highest_version() {
        // The common single-artifact node is unaffected: recency ordering returns
        // the latest version of the only name, exactly as version-ordering did.
        let db = crate::storage::migrated_test_db("latest-recency-single.db").await;
        seed_job(&db).await;
        insert_artifact(&db, "plan-1", "plan", 1, 10).await;
        insert_artifact(&db, "plan-2", "plan", 2, 20).await;
        insert_artifact(&db, "plan-3", "plan", 3, 30).await;

        let latest = get_latest(&db, "j").await.unwrap().expect("an artifact");
        assert_eq!(latest.output_name.as_deref(), Some("plan"));
        assert_eq!(latest.version, 3);
    }
}
