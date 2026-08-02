//! Durable state for rolling thread-session compaction (CAIRN-3388).
//!
//! Three mechanical records, and nothing authored:
//!
//! - a **mark** is eligibility. A child issue reaching a terminal status means
//!   the parent turns that discussed it may be replaced by a table-of-contents
//!   line; it never rewrites anything by itself.
//! - a **generation** is one composed compaction, with the two byte counts its
//!   trigger compared so the threshold can be calibrated from what happened. It
//!   counts as *applied* only once the job has left the session it was composed
//!   from, because rotation is the only thing that moves that pointer — so a
//!   rotation that never landed leaves the next attempt exactly as armed as the
//!   first, with its marks still eligible.
//! - an **entry** is one chapter of the generated table of contents, carried
//!   forward across generations so a chapter keeps one stable overview and
//!   address for the life of the thread.
//!
//! The thread's authored arc lives in its own artifact and is never copied here.
//! Generation and authorship stay separable on purpose: two stores of "where
//! things stand" is the drift the design exists to avoid.

use cairn_db::turso::params;

use crate::storage::{DbResult, LocalDb, RowExt};

/// What made a compaction fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionTrigger {
    /// The prompt cache expired, so the next turn pays a write regardless and
    /// the session is being rebuilt either way.
    Expiry,
    /// The live session filled enough of the model's context window, while the
    /// cache was still warm, that it has to give something up.
    Capacity,
    /// An operator forced a digest resume.
    Manual,
}

impl CompactionTrigger {
    pub fn as_db(self) -> &'static str {
        match self {
            CompactionTrigger::Expiry => "expiry",
            CompactionTrigger::Capacity => "capacity",
            CompactionTrigger::Manual => "manual",
        }
    }
}

/// A child issue whose terminal transition made its parent turns compactable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildMark {
    pub child_issue_id: String,
    pub child_issue_uri: String,
    pub child_title: String,
    pub final_status: String,
    pub marked_at: i64,
}

/// Where a table-of-contents entry came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntrySource {
    /// A child issue: it has a beginning, an end, a subject, and a URI.
    Child,
    /// Conversation between children, bounded at user-turn boundaries.
    Interstitial,
}

impl EntrySource {
    pub fn as_db(self) -> &'static str {
        match self {
            EntrySource::Child => "child",
            EntrySource::Interstitial => "interstitial",
        }
    }

    fn from_db(value: &str) -> Option<Self> {
        match value {
            "child" => Some(EntrySource::Child),
            "interstitial" => Some(EntrySource::Interstitial),
            _ => None,
        }
    }
}

/// One chapter of the generated table of contents.
///
/// `start_block`/`end_block` are indices into the job's chronological turn-block
/// sequence, which is what both the transcript renderer and the chapter's
/// re-read address use. Turn ids ride along for forensics but cannot be the key:
/// `turns.sequence` restarts at 1 in every rotated session, and a thread
/// accumulates many.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TocEntry {
    pub source: EntrySource,
    pub overview: String,
    pub content_uri: String,
    pub child_issue_id: Option<String>,
    pub start_block: i64,
    pub end_block: i64,
    pub start_turn_id: Option<String>,
    pub end_turn_id: Option<String>,
}

impl TocEntry {
    /// The identity used to carry an entry forward without duplicating it: the
    /// address it re-reads plus the range it stands in for.
    pub fn dedupe_key(&self) -> (&str, i64, i64) {
        (&self.content_uri, self.start_block, self.end_block)
    }
}

/// Everything one applied compaction records.
#[derive(Debug, Clone)]
pub struct AppliedCompaction {
    pub trigger: CompactionTrigger,
    /// The session that was live when this seed was composed. The generation is
    /// pending until the job leaves it.
    pub source_session_id: String,
    pub entries: Vec<TocEntry>,
    /// What the rebuilt prompt weighs. The rotation's cache write is paid on
    /// this, so it is the number a later calibration prices against; the two
    /// below both describe the dropped range and cannot answer that alone.
    pub seed_bytes: i64,
    pub source_bytes: i64,
    pub candidate_bytes: i64,
    pub compacted_through_block: Option<i64>,
    pub recency_start_block: i64,
    /// The marks this generation consumed, by child issue id.
    pub consumed_child_issue_ids: Vec<String>,
}

/// Record that `mark`'s child reached a terminal status under `job_id`.
///
/// Idempotent by (job, child): the terminal fact is delivered to every watcher
/// and can be re-delivered after a restart, and a mark already consumed by a
/// generation must never be resurrected by a duplicate.
pub async fn mark_child_terminal(db: &LocalDb, job_id: &str, mark: &ChildMark) -> DbResult<()> {
    db.execute(
        "INSERT INTO thread_compaction_marks (
             job_id, child_issue_id, child_issue_uri, child_title, final_status, marked_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (job_id, child_issue_id) DO NOTHING",
        params![
            job_id,
            mark.child_issue_id.as_str(),
            mark.child_issue_uri.as_str(),
            mark.child_title.as_str(),
            mark.final_status.as_str(),
            mark.marked_at
        ],
    )
    .await
    .map(|_| ())
}

/// The marks no *applied* generation has consumed, oldest first.
///
/// A mark consumed by a generation that is still pending — composed against the
/// session the job is still on, so its rotation never landed — counts as
/// unconsumed. Without that, a failed rotation would silently disarm the size
/// trigger and leave the thread carrying its whole live prefix until expiry.
pub async fn unconsumed_marks(
    db: &LocalDb,
    job_id: &str,
    current_session_id: &str,
) -> DbResult<Vec<ChildMark>> {
    db.query_all(
        "SELECT m.child_issue_id, m.child_issue_uri, m.child_title, m.final_status, m.marked_at
         FROM thread_compaction_marks m
         LEFT JOIN thread_compactions c
           ON c.job_id = m.job_id AND c.generation = m.consumed_generation
         WHERE m.job_id = ?1
           AND (m.consumed_generation IS NULL OR c.source_session_id = ?2)
         ORDER BY m.marked_at ASC, m.child_issue_id ASC",
        params![job_id.to_string(), current_session_id.to_string()],
        |row| {
            Ok(ChildMark {
                child_issue_id: row.text(0)?,
                child_issue_uri: row.text(1)?,
                child_title: row.text(2)?,
                final_status: row.text(3)?,
                marked_at: row.i64(4)?,
            })
        },
    )
    .await
}

/// The newest *applied* generation number for a job, or `None` when it has none.
pub async fn latest_applied_generation(
    db: &LocalDb,
    job_id: &str,
    current_session_id: &str,
) -> DbResult<Option<i64>> {
    db.query_opt_i64(
        "SELECT MAX(generation) FROM thread_compactions
         WHERE job_id = ?1 AND source_session_id <> ?2",
        params![job_id.to_string(), current_session_id.to_string()],
    )
    .await
}

/// The table of contents as of the newest applied generation, in position order.
///
/// A pending generation contributes nothing: its seed never reached an agent, so
/// treating its chapters as already compacted would drop those turns from the
/// next seed without anything ever having summarized them.
pub async fn applied_entries(
    db: &LocalDb,
    job_id: &str,
    current_session_id: &str,
) -> DbResult<Vec<TocEntry>> {
    db.query_all(
        "SELECT source_kind, overview, content_uri, child_issue_id,
                start_block, end_block, start_turn_id, end_turn_id
         FROM thread_compaction_entries
         WHERE job_id = ?1
           AND generation = (
               SELECT MAX(generation) FROM thread_compactions
               WHERE job_id = ?1 AND source_session_id <> ?2
           )
         ORDER BY position ASC",
        params![job_id.to_string(), current_session_id.to_string()],
        |row| {
            let kind = row.text(0)?;
            Ok(TocEntry {
                source: EntrySource::from_db(&kind).unwrap_or(EntrySource::Interstitial),
                overview: row.text(1)?,
                content_uri: row.text(2)?,
                child_issue_id: row.opt_text(3)?,
                start_block: row.i64(4)?,
                end_block: row.i64(5)?,
                start_turn_id: row.opt_text(6)?,
                end_turn_id: row.opt_text(7)?,
            })
        },
    )
    .await
}

/// Persist one composed compaction: its generation row, its whole table of
/// contents, and the consumption of the marks it folded in — in one transaction.
///
/// Returns the generation number. Called *before* the session rotates, because
/// the seed must be recorded by the time it can reach an agent. A rotation that
/// then fails leaves this generation pending, and a retry from the same session
/// supersedes it in place: the pending row and its entries are dropped and its
/// marks released before the new generation is written, so retrying converges on
/// one generation per rotation instead of accumulating an abandoned one each
/// time.
pub async fn persist_generation(
    db: &LocalDb,
    job_id: &str,
    applied: &AppliedCompaction,
    now: i64,
) -> DbResult<i64> {
    let job_id = job_id.to_string();
    let applied = applied.clone();
    db.write(move |conn| {
        let job_id = job_id.clone();
        let applied = applied.clone();
        Box::pin(async move {
            // Supersede any pending generation composed from this same session:
            // release its marks first, then drop it and its entries.
            conn.execute(
                "UPDATE thread_compaction_marks SET consumed_generation = NULL
                 WHERE job_id = ?1 AND consumed_generation IN (
                     SELECT generation FROM thread_compactions
                     WHERE job_id = ?1 AND source_session_id = ?2
                 )",
                params![job_id.as_str(), applied.source_session_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM thread_compaction_entries
                 WHERE job_id = ?1 AND generation IN (
                     SELECT generation FROM thread_compactions
                     WHERE job_id = ?1 AND source_session_id = ?2
                 )",
                params![job_id.as_str(), applied.source_session_id.as_str()],
            )
            .await?;
            conn.execute(
                "DELETE FROM thread_compactions WHERE job_id = ?1 AND source_session_id = ?2",
                params![job_id.as_str(), applied.source_session_id.as_str()],
            )
            .await?;

            let mut rows = conn
                .query(
                    "SELECT COALESCE(MAX(generation), 0) + 1 FROM thread_compactions WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
            let generation = match rows.next().await? {
                Some(row) => row.i64(0)?,
                None => 1,
            };

            conn.execute(
                "INSERT INTO thread_compactions (
                     job_id, generation, source_session_id, compacted_through_block,
                     recency_start_block, seed_bytes, source_bytes, candidate_bytes,
                     trigger, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    job_id.as_str(),
                    generation,
                    applied.source_session_id.as_str(),
                    applied.compacted_through_block,
                    applied.recency_start_block,
                    applied.seed_bytes,
                    applied.source_bytes,
                    applied.candidate_bytes,
                    applied.trigger.as_db(),
                    now
                ],
            )
            .await?;

            for (position, entry) in applied.entries.iter().enumerate() {
                conn.execute(
                    "INSERT INTO thread_compaction_entries (
                         job_id, generation, position, source_kind, overview, content_uri,
                         child_issue_id, start_block, end_block, start_turn_id, end_turn_id
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        job_id.as_str(),
                        generation,
                        position as i64,
                        entry.source.as_db(),
                        entry.overview.as_str(),
                        entry.content_uri.as_str(),
                        entry.child_issue_id.as_deref(),
                        entry.start_block,
                        entry.end_block,
                        entry.start_turn_id.as_deref(),
                        entry.end_turn_id.as_deref()
                    ],
                )
                .await?;
            }

            for child_issue_id in applied.consumed_child_issue_ids.iter() {
                conn.execute(
                    "UPDATE thread_compaction_marks
                     SET consumed_generation = ?1
                     WHERE job_id = ?2 AND child_issue_id = ?3 AND consumed_generation IS NULL",
                    params![generation, job_id.as_str(), child_issue_id.as_str()],
                )
                .await?;
            }

            Ok(generation)
        })
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed_job(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO workspaces (id,name,created_at,updated_at) VALUES ('w','W',1,1);
             INSERT INTO projects (id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','w','P','PRJ','/tmp/p',1,1);
             INSERT INTO issues (id,project_id,number,title,status,attention,created_at,updated_at) VALUES ('i','p',1,'T','active','none',1,1);
             INSERT INTO jobs (id,issue_id,project_id,status,uri_segment,node_name,created_at,updated_at) VALUES ('j','i','p','running','thread','thread',1,1);",
        )
        .await
        .unwrap();
    }

    fn mark(id: &str, at: i64) -> ChildMark {
        ChildMark {
            child_issue_id: id.to_string(),
            child_issue_uri: format!("cairn://p/PRJ/{id}"),
            child_title: format!("child {id}"),
            final_status: "merged".to_string(),
            marked_at: at,
        }
    }

    fn entry(uri: &str, start: i64, end: i64) -> TocEntry {
        TocEntry {
            source: EntrySource::Child,
            overview: format!("work on {uri}"),
            content_uri: uri.to_string(),
            child_issue_id: Some("c1".to_string()),
            start_block: start,
            end_block: end,
            start_turn_id: None,
            end_turn_id: None,
        }
    }

    /// The session a compaction is composed from. Rotation is what makes the
    /// generation applied, so "before" and "after" are just which session id a
    /// read is made against.
    const BEFORE: &str = "sess-1";
    const AFTER: &str = "sess-2";

    fn applied(entries: Vec<TocEntry>, consumed: Vec<String>) -> AppliedCompaction {
        AppliedCompaction {
            trigger: CompactionTrigger::Expiry,
            source_session_id: BEFORE.to_string(),
            entries,
            seed_bytes: 60_000,
            source_bytes: 9_000,
            candidate_bytes: 300,
            compacted_through_block: Some(4),
            recency_start_block: 5,
            consumed_child_issue_ids: consumed,
        }
    }

    #[tokio::test]
    async fn re_marking_a_child_is_a_no_op() {
        // The terminal fact reaches every watcher and survives a restart, so the
        // same child can be finalized against the same thread repeatedly. The
        // second delivery must not duplicate the mark or rewrite its first
        // observation.
        let db = crate::storage::migrated_test_db("thread-mark-idempotent.db").await;
        seed_job(&db).await;

        mark_child_terminal(&db, "j", &mark("c1", 100))
            .await
            .unwrap();
        let mut later = mark("c1", 200);
        later.final_status = "closed".to_string();
        mark_child_terminal(&db, "j", &later).await.unwrap();

        let marks = unconsumed_marks(&db, "j", BEFORE).await.unwrap();
        assert_eq!(
            marks.len(),
            1,
            "duplicate terminal delivery created a second mark"
        );
        assert_eq!(marks[0].marked_at, 100);
        assert_eq!(marks[0].final_status, "merged");
    }

    #[tokio::test]
    async fn a_rotated_generation_consumes_its_marks_and_leaves_the_rest() {
        let db = crate::storage::migrated_test_db("thread-mark-consumption.db").await;
        seed_job(&db).await;
        mark_child_terminal(&db, "j", &mark("c1", 100))
            .await
            .unwrap();
        mark_child_terminal(&db, "j", &mark("c2", 200))
            .await
            .unwrap();

        let generation = persist_generation(
            &db,
            "j",
            &applied(
                vec![entry("cairn://p/PRJ/c1", 0, 3)],
                vec!["c1".to_string()],
            ),
            1_000,
        )
        .await
        .unwrap();
        assert_eq!(generation, 1);

        // Read as of the successor session: rotation landed, so the generation
        // is applied and its mark is spent.
        let remaining = unconsumed_marks(&db, "j", AFTER).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].child_issue_id, "c2");
    }

    #[tokio::test]
    async fn re_marking_a_consumed_child_does_not_resurrect_it() {
        // A child that is finalized twice — once before a compaction and once
        // after — must not re-enter the queue, or the same range would be
        // compacted again in the next generation.
        let db = crate::storage::migrated_test_db("thread-mark-no-resurrect.db").await;
        seed_job(&db).await;
        mark_child_terminal(&db, "j", &mark("c1", 100))
            .await
            .unwrap();
        persist_generation(
            &db,
            "j",
            &applied(
                vec![entry("cairn://p/PRJ/c1", 0, 3)],
                vec!["c1".to_string()],
            ),
            1_000,
        )
        .await
        .unwrap();

        mark_child_terminal(&db, "j", &mark("c1", 300))
            .await
            .unwrap();

        assert!(
            unconsumed_marks(&db, "j", AFTER).await.unwrap().is_empty(),
            "a consumed mark came back"
        );
    }

    #[tokio::test]
    async fn generations_advance_and_the_latest_table_of_contents_wins() {
        let db = crate::storage::migrated_test_db("thread-generations.db").await;
        seed_job(&db).await;

        persist_generation(
            &db,
            "j",
            &applied(vec![entry("cairn://p/PRJ/c1", 0, 3)], vec![]),
            1_000,
        )
        .await
        .unwrap();
        // The second compaction is composed from the session the first rotated
        // into, and rotates again.
        let mut second_compaction = applied(
            vec![
                entry("cairn://p/PRJ/c1", 0, 3),
                entry("cairn://p/PRJ/c2", 4, 9),
            ],
            vec![],
        );
        second_compaction.source_session_id = AFTER.to_string();
        let second = persist_generation(&db, "j", &second_compaction, 2_000)
            .await
            .unwrap();

        assert_eq!(second, 2);
        assert_eq!(
            latest_applied_generation(&db, "j", "sess-3").await.unwrap(),
            Some(2)
        );

        let entries = applied_entries(&db, "j", "sess-3").await.unwrap();
        assert_eq!(
            entries.len(),
            2,
            "the newest generation carries the whole table of contents"
        );
        assert_eq!(entries[0].content_uri, "cairn://p/PRJ/c1");
        assert_eq!(entries[1].content_uri, "cairn://p/PRJ/c2");
        assert_eq!(entries[1].start_block, 4);
    }

    #[tokio::test]
    async fn a_generation_whose_rotation_never_landed_leaves_the_retry_fully_armed() {
        // Persistence happens before rotation, so a rotation failure leaves a
        // generation composed from a session the job is STILL on. Nothing about
        // the next decision may have moved: the marks must still be eligible
        // (or the size trigger can never select again, and the thread carries
        // its whole live prefix until the one-hour expiry), and the chapters
        // must not count as compacted (or those turns would vanish from the
        // next seed without anything having summarized them).
        let db = crate::storage::migrated_test_db("thread-pending-generation.db").await;
        seed_job(&db).await;
        mark_child_terminal(&db, "j", &mark("c1", 100))
            .await
            .unwrap();
        persist_generation(
            &db,
            "j",
            &applied(
                vec![entry("cairn://p/PRJ/c1", 0, 3)],
                vec!["c1".to_string()],
            ),
            1_000,
        )
        .await
        .unwrap();

        // Still on the same session: the compaction never took effect.
        assert_eq!(
            unconsumed_marks(&db, "j", BEFORE).await.unwrap().len(),
            1,
            "a failed rotation disarmed the size trigger"
        );
        assert!(
            applied_entries(&db, "j", BEFORE).await.unwrap().is_empty(),
            "a seed that never reached an agent claimed its turns anyway"
        );
        assert_eq!(
            latest_applied_generation(&db, "j", BEFORE).await.unwrap(),
            None
        );

        // The retry supersedes the pending generation in place rather than
        // stacking another one behind it.
        persist_generation(
            &db,
            "j",
            &applied(
                vec![entry("cairn://p/PRJ/c1", 0, 3)],
                vec!["c1".to_string()],
            ),
            2_000,
        )
        .await
        .unwrap();
        let generations: Vec<i64> = db
            .query_all(
                "SELECT generation FROM thread_compactions WHERE job_id = 'j' ORDER BY generation",
                (),
                |row| row.i64(0),
            )
            .await
            .unwrap();
        assert_eq!(
            generations.len(),
            1,
            "an abandoned generation was left behind"
        );

        // Once the retry's rotation lands, it applies exactly as a first-time
        // compaction would have.
        assert!(unconsumed_marks(&db, "j", AFTER).await.unwrap().is_empty());
        assert_eq!(applied_entries(&db, "j", AFTER).await.unwrap().len(), 1);
    }
}
