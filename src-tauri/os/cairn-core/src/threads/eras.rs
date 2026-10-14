//! Persisted era fetch for the `cairn:~/eras` resource family (CAIRN-4302).
//!
//! An era is minted and summarized at thread-compaction FOLD time and
//! persisted once, generation-independently -- never recomputed here. This
//! module is read-only: it fetches exactly what fold-time minting already
//! wrote. Minting, folding, and the background summary upgrade live with
//! thread compaction, not here.
//!
//! `range_uri` is deliberately absent from [`ThreadEra`]: a thread renames
//! (CAIRN-3759), so a URI baked at mint time would go stale exactly the way
//! CAIRN-3916 eliminated for baked home snapshots. `start_block`/`end_block`
//! fully identify the span; callers derive the live `.../chat?offset=&limit=`
//! address from the thread's current coordinates at render time.

use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

/// A persisted, generation-independent era: a contiguous span of a thread's
/// turn blocks, folded once. Identity and span are immutable; only
/// `summary`/`summary_status`/`summary_model`/`summarized_at` may be upgraded
/// once, from a mechanical fallback to a generated paragraph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadEra {
    pub seq: i32,
    pub start_block: i64,
    pub end_block: i64,
    pub started_at: i64,
    pub ended_at: i64,
    pub summary: String,
    pub summary_status: String,
    pub summary_model: Option<String>,
    pub source_tokens: i64,
    pub minted_generation: i64,
    pub created_at: i64,
    pub summarized_at: Option<i64>,
}

/// A child issue an era's folded span resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EraChild {
    pub child_issue_id: String,
    pub child_issue_uri: String,
    pub child_title: String,
    pub final_status: String,
}

const ERA_SELECT: &str = "SELECT seq, start_block, end_block, started_at, ended_at, summary, \
     summary_status, summary_model, source_tokens, minted_generation, created_at, summarized_at \
     FROM thread_eras";

/// Every persisted era for a job, newest sequence first.
pub async fn list_eras(
    db: &LocalDb,
    job_id: &str,
    limit: i64,
    offset: i64,
) -> DbResult<Vec<ThreadEra>> {
    db.query_all(
        format!("{ERA_SELECT} WHERE job_id = ?1 ORDER BY seq DESC LIMIT ?2 OFFSET ?3"),
        params![job_id.to_string(), limit, offset],
        |row| {
            Ok(ThreadEra {
                seq: row.i64(0)? as i32,
                start_block: row.i64(1)?,
                end_block: row.i64(2)?,
                started_at: row.i64(3)?,
                ended_at: row.i64(4)?,
                summary: row.text(5)?,
                summary_status: row.text(6)?,
                summary_model: row.opt_text(7)?,
                source_tokens: row.i64(8)?,
                minted_generation: row.i64(9)?,
                created_at: row.i64(10)?,
                summarized_at: row.opt_i64(11)?,
            })
        },
    )
    .await
}

/// Total era count for a job, for pagination framing.
pub async fn count_eras(db: &LocalDb, job_id: &str) -> DbResult<i64> {
    Ok(db
        .query_opt_i64(
            "SELECT COUNT(*) FROM thread_eras WHERE job_id = ?1",
            params![job_id.to_string()],
        )
        .await?
        .unwrap_or(0))
}

/// One era by its 1-based sequence within a job.
pub async fn get_era(db: &LocalDb, job_id: &str, seq: i32) -> DbResult<Option<ThreadEra>> {
    db.query_opt(
        format!("{ERA_SELECT} WHERE job_id = ?1 AND seq = ?2"),
        params![job_id.to_string(), seq],
        |row| {
            Ok(ThreadEra {
                seq: row.i64(0)? as i32,
                start_block: row.i64(1)?,
                end_block: row.i64(2)?,
                started_at: row.i64(3)?,
                ended_at: row.i64(4)?,
                summary: row.text(5)?,
                summary_status: row.text(6)?,
                summary_model: row.opt_text(7)?,
                source_tokens: row.i64(8)?,
                minted_generation: row.i64(9)?,
                created_at: row.i64(10)?,
                summarized_at: row.opt_i64(11)?,
            })
        },
    )
    .await
}

/// The children an era's folded span resolved, in stable order.
pub async fn era_children(db: &LocalDb, job_id: &str, seq: i32) -> DbResult<Vec<EraChild>> {
    db.query_all(
        "SELECT child_issue_id, child_issue_uri, child_title, final_status
         FROM thread_era_children
         WHERE job_id = ?1 AND era_seq = ?2
         ORDER BY child_issue_id ASC",
        params![job_id.to_string(), seq],
        |row| {
            Ok(EraChild {
                child_issue_id: row.text(0)?,
                child_issue_uri: row.text(1)?,
                child_title: row.text(2)?,
                final_status: row.text(3)?,
            })
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed_job(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','prj','/tmp/p',1,1);
             INSERT INTO issues (id,project_id,number,title,status,attention,created_at,updated_at) VALUES ('i','p',1,'T','active','none',1,1);
             INSERT INTO jobs (id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at) VALUES ('j','i','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();
    }

    async fn insert_era(db: &LocalDb, seq: i32, status: &str) {
        db.execute(
            "INSERT INTO thread_eras (
                 job_id, seq, start_block, end_block, started_at, ended_at, summary,
                 summary_status, summary_model, source_tokens, minted_generation, created_at
             ) VALUES ('j', ?1, ?2, ?3, 100, 200, ?4, ?5, NULL, 900, 1, 300)",
            params![
                seq,
                (seq as i64 - 1) * 10,
                (seq as i64) * 10 - 1,
                format!("era {seq} summary"),
                status.to_string()
            ],
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn eras_persist_and_fetch_newest_first() {
        let db = crate::storage::migrated_test_db("thread-eras.db").await;
        seed_job(&db).await;
        insert_era(&db, 1, "generated").await;
        insert_era(&db, 2, "fallback").await;

        assert_eq!(count_eras(&db, "j").await.unwrap(), 2);
        let list = list_eras(&db, "j", 50, 0).await.unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].seq, 2, "newest sequence first");
        assert_eq!(list[1].seq, 1);
        assert_eq!(list[0].summary_status, "fallback");

        let era = get_era(&db, "j", 1).await.unwrap().expect("era 1 exists");
        assert_eq!(era.summary, "era 1 summary");
        assert_eq!(era.summary_status, "generated");
        assert!(get_era(&db, "j", 99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn era_children_resolve_in_stable_order() {
        let db = crate::storage::migrated_test_db("thread-era-children.db").await;
        seed_job(&db).await;
        insert_era(&db, 1, "fallback").await;
        db.execute_script(
            "INSERT INTO thread_era_children (job_id, era_seq, child_issue_id, child_issue_uri, child_title, final_status)
             VALUES ('j', 1, 'c2', 'cairn://p/prj/2', 'Second child', 'merged');
             INSERT INTO thread_era_children (job_id, era_seq, child_issue_id, child_issue_uri, child_title, final_status)
             VALUES ('j', 1, 'c1', 'cairn://p/prj/1', 'First child', 'closed');",
        )
        .await
        .unwrap();

        let children = era_children(&db, "j", 1).await.unwrap();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].child_issue_id, "c1");
        assert_eq!(children[1].child_issue_id, "c2");
    }
}
