use super::common::{connect_for_read, lookup_project_by_key};
use crate::storage::{LocalDb, RowExt};
use cairn_common::query::QueryParam;
use serde::Serialize;
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 200;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HomeEntry {
    pub at: i64,
    pub id: String,
    pub kind: String,
    pub author: String,
    pub target: Option<String>,
    pub body: String,
    pub source: String,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HomePayload {
    pub project: String,
    pub entries: Vec<HomeEntry>,
    pub next_cursor: Option<String>,
}

pub(crate) async fn read_project_home(db: &LocalDb, key: &str, params: &[QueryParam]) -> String {
    let query = match parse_params(params) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let conn = match connect_for_read(db).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let project = match lookup_project_by_key(&conn, key).await {
        Ok(v) => v,
        Err(e) => return e,
    };
    let payload = match query_project_home(
        &conn,
        &project.project_id,
        key,
        query.limit,
        query.before,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return format!("Failed to read project home: {e}"),
    };
    if query.json {
        serde_json::to_string(&payload)
            .unwrap_or_else(|e| format!("Failed to render project home: {e}"))
    } else {
        render(&payload)
    }
}
/// One parsed `?limit=&before=&format=` request for a page of the timeline.
struct HomeQuery {
    limit: usize,
    /// The `EPOCH:ID` cursor to resume below, or `None` for the newest page.
    before: Option<(i64, String)>,
    json: bool,
}

fn parse_params(params: &[QueryParam]) -> Result<HomeQuery, String> {
    let (mut limit, mut before, mut json) = (DEFAULT_LIMIT, None, false);
    for p in params {
        match p.key.as_str() {
            "limit" => {
                limit = p
                    .value
                    .parse()
                    .map_err(|_| "Home limit must be an integer.")?;
                if !(1..=MAX_LIMIT).contains(&limit) {
                    return Err(format!("Home limit must be between 1 and {MAX_LIMIT}."));
                }
            }
            "before" => {
                let (at, id) = p
                    .value
                    .split_once(':')
                    .ok_or("Home cursor must be EPOCH:ID.")?;
                before = Some((
                    at.parse()
                        .map_err(|_| "Home cursor epoch must be an integer.")?,
                    id.into(),
                ));
            }
            "format" if p.value == "json" => json = true,
            "format" => return Err("Home format must be json.".into()),
            key => return Err(format!("Unsupported project home query parameter: {key}")),
        }
    }
    Ok(HomeQuery {
        limit,
        before,
        json,
    })
}
pub async fn query_project_home(
    conn: &cairn_db::turso::Connection,
    project_id: &str,
    key: &str,
    limit: usize,
    before: Option<(i64, String)>,
) -> cairn_db::storage::DbResult<HomePayload> {
    let (at, id) = before
        .map(|(a, i)| (Some(a), Some(i)))
        .unwrap_or((None, None));
    let mut rows = conn
        .query(
            &home_sql(),
            cairn_db::turso::params![project_id, key, at, id, limit as i64],
        )
        .await?;
    let mut entries = Vec::new();
    while let Some(r) = rows.next().await? {
        let Some(body) = home_entry_body(&r) else {
            continue;
        };
        entries.push(HomeEntry {
            at: r.i64(0)?,
            id: r.text(1)?,
            kind: r.text(2)?,
            author: r.text(3)?,
            target: r.opt_text(4)?,
            body,
            source: r.text(6)?,
        });
    }
    let next_cursor = (entries.len() == limit)
        .then(|| entries.last().map(|e| format!("{}:{}", e.at, e.id)))
        .flatten();
    Ok(HomePayload {
        project: key.into(),
        entries,
        next_cursor,
    })
}

fn home_entry_body(row: &cairn_db::turso::Row) -> Option<String> {
    let inline = row.text(5).ok()?;
    let body = match (
        row.opt_blob(7).ok().flatten(),
        row.opt_text(8).ok().flatten(),
    ) {
        (Some(blob), Some(codec)) => crate::storage::decompress(&codec, &blob)
            .ok()
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or(inline),
        _ => inline,
    };
    serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("content")
                .and_then(|content| content.as_str())
                .map(str::to_owned)
        })
        .or(Some(body))
        .filter(|body| !body.trim().is_empty())
}
/// The shape of an event whose stored body Project Home can render as prose.
///
/// This is spelled twice on purpose: here, and as the predicate of
/// `idx_events_home_prose` in migration `0225_home_prose_index`. Turso
/// substitutes a partial index only while the query's WHERE terms imply the
/// index's, so the two must stay identical apart from this query's `e.` alias.
/// A rewording on either side keeps every entry correct while silently dropping
/// Home back to a full scan of `events` that deserializes each row's JSON only
/// to discover the row was never a candidate.
/// `home_prose_shape_matches_its_index` holds the two spellings together.
///
/// An archived (`zstd`) row qualifies on its envelope alone, because its prose
/// lives in a compressed blob SQL cannot open. [`home_entry_body`] decompresses
/// and drops the empties afterwards, so this is generous rather than wrong --
/// the same bargain the query struck before the index existed.
///
/// The `CASE` is load-bearing and cannot be flattened into an `AND` chain.
/// Turso's JSON functions raise `malformed JSON` instead of returning NULL, and
/// its `AND` evaluates both sides rather than short-circuiting, so a guard
/// written as `json_valid(data) AND json_type(data, ...)` still reaches
/// `json_type` with the body that cannot be parsed. `CASE` is the one form that
/// does short-circuit. Because an index predicate runs on the WRITE path, the
/// difference is not a failed read but an event the database refuses to store.
const HOME_PROSE_SHAPE: &str = "((CASE WHEN json_valid(e.data) AND (e.storage_mode IS NULL OR e.storage_mode='full') THEN json_type(e.data,'$.content')='text' AND trim(json_extract(e.data,'$.content'))<>'' ELSE 0 END) OR (e.storage_mode='zstd' AND e.data_blob IS NOT NULL AND e.codec IS NOT NULL))";

/// The merged transcript, newest first, as one page.
///
/// Every branch is shaped the same way, and the shape is what keeps Home's cost
/// off the size of the corpus:
///
/// - **The candidate walk selects keys, never bodies.** Each branch picks its
///   own newest `?5` rows as `(created_at, id)` and joins back to the row only
///   for the ones that survive. Carrying `e.data` through the sort instead --
///   which is what selecting the body inline does -- materializes every
///   candidate body in the sorter to return fifty of them, and event bodies are
///   the largest column in the database.
/// - **Renderability is answered by `idx_events_home_prose`, not by parsing.**
///   [`HOME_PROSE_SHAPE`] is the index's predicate, so the walk visits only rows
///   that already qualify.
/// - **Every filter that can drop a row sits above that branch's `LIMIT`.** The
///   turn join, the turn's `start_reason`, and the attention-push exclusion all
///   run inside the candidate walk. Below the `LIMIT` they would thin a page
///   after it was counted, and a short page reads as "no more entries" to
///   [`query_project_home`], truncating pagination early.
/// - **Each branch carries the pagination cursor.** Without it the outer `LIMIT`
///   could only be applied after the union had materialized the project's whole
///   history.
///
/// The union is only correct because taking each branch's top-N still yields the
/// global top-N; `pagination_merges_branches_in_global_order` walks a cursor
/// across a page boundary to prove it.
fn home_sql() -> String {
    let prose = HOME_PROSE_SHAPE;
    format!(
        r#"
WITH thread_runs AS (
 SELECT r.id run_id,t.name thread_name FROM threads t
 JOIN jobs j ON j.thread_id=t.id AND j.parent_job_id IS NULL AND j.uri_segment='thread'
 JOIN runs r ON r.job_id=j.id WHERE t.project_id=?1
), home_entries AS (
 SELECT * FROM (
  SELECT pick.at at,pick.id id,'say' kind,'cairn://p/'||?2||'/'||tr.thread_name author,NULL target,
  e.data body,'cairn://p/'||?2||'/'||tr.thread_name||'/chat/turn/'||pick.seq source,e.data_blob data_blob,e.codec codec
  FROM (
   SELECT e.created_at at,e.id id,e.run_id run_id,tu.sequence seq
   FROM thread_runs tr
   CROSS JOIN events e INDEXED BY idx_events_home_prose ON e.run_id=tr.run_id AND e.event_type='assistant'
   JOIN turns tu ON tu.id=e.turn_id
   WHERE {prose}
   AND (?3 IS NULL OR e.created_at<?3 OR (e.created_at=?3 AND e.id<?4))
   ORDER BY e.created_at DESC,e.id DESC LIMIT ?5
  ) pick
  JOIN events e ON e.id=pick.id
  JOIN thread_runs tr ON tr.run_id=pick.run_id
  ORDER BY pick.at DESC,pick.id DESC LIMIT ?5
 )
 UNION ALL
 SELECT * FROM (
  SELECT pick.at,pick.id,'say','operator',NULL,
  e.data,'cairn://p/'||?2||'/'||tr.thread_name||'/chat/turn/'||pick.seq,e.data_blob,e.codec
  FROM (
   SELECT e.created_at at,e.id id,e.run_id run_id,tu.sequence seq
   FROM thread_runs tr
   CROSS JOIN events e INDEXED BY idx_events_home_prose ON e.run_id=tr.run_id AND e.event_type='user'
   JOIN turns tu ON tu.id=e.turn_id AND tu.start_reason IN ('initial','follow_up')
   WHERE {prose}
   AND NOT EXISTS(SELECT 1 FROM attention_pushes ap WHERE ap.delivered_event_id=e.id)
   AND (?3 IS NULL OR e.created_at<?3 OR (e.created_at=?3 AND e.id<?4))
   ORDER BY e.created_at DESC,e.id DESC LIMIT ?5
  ) pick
  JOIN events e ON e.id=pick.id
  JOIN thread_runs tr ON tr.run_id=pick.run_id
  ORDER BY pick.at DESC,pick.id DESC LIMIT ?5
 )
 UNION ALL
 SELECT * FROM (
  SELECT pick.at,pick.id,'message',m.sender_name,
  CASE WHEN recipient.thread_name IS NULL THEN NULL ELSE 'cairn://p/'||?2||'/'||recipient.thread_name END,m.content,
  CASE WHEN sender.thread_name IS NOT NULL THEN 'cairn://p/'||?2||'/'||sender.thread_name||'/messages' ELSE 'cairn://p/'||?2||'/'||recipient.thread_name||'/messages' END,NULL,NULL
  FROM (
   SELECT m.created_at at,m.id id FROM messages m
   LEFT JOIN thread_runs sender ON sender.run_id=m.sender_run_id
   LEFT JOIN thread_runs recipient ON recipient.run_id=m.recipient_run_id
   WHERE m.channel_type='direct' AND (sender.run_id IS NOT NULL OR recipient.run_id IS NOT NULL)
   AND (?3 IS NULL OR m.created_at<?3 OR (m.created_at=?3 AND m.id<?4))
   ORDER BY m.created_at DESC,m.id DESC LIMIT ?5
  ) pick
  JOIN messages m ON m.id=pick.id
  LEFT JOIN thread_runs sender ON sender.run_id=m.sender_run_id
  LEFT JOIN thread_runs recipient ON recipient.run_id=m.recipient_run_id
  ORDER BY pick.at DESC,pick.id DESC LIMIT ?5
 )
 UNION ALL
 SELECT * FROM (
  SELECT pick.at,pick.id,'message',m.sender_name,'cairn://p/'||?2||'/'||t.name,m.content,'cairn://p/'||?2||'/'||t.name||'/messages',NULL,NULL
  FROM (
   SELECT m.created_at at,m.id id FROM messages m
   JOIN threads t ON t.id=m.channel_id AND t.project_id=?1
   WHERE m.channel_type='thread'
   AND (?3 IS NULL OR m.created_at<?3 OR (m.created_at=?3 AND m.id<?4))
   ORDER BY m.created_at DESC,m.id DESC LIMIT ?5
  ) pick
  JOIN messages m ON m.id=pick.id
  JOIN threads t ON t.id=m.channel_id
  ORDER BY pick.at DESC,pick.id DESC LIMIT ?5
 )
)
SELECT at,id,kind,author,target,body,source,data_blob,codec FROM home_entries
ORDER BY at DESC,id DESC LIMIT ?5"#
    )
}
fn render(p: &HomePayload) -> String {
    let mut out = format!("# {} home\n\n", p.project);
    for e in p.entries.iter().rev() {
        let t = e
            .target
            .as_ref()
            .map(|v| format!(" → {v}"))
            .unwrap_or_default();
        out.push_str(&format!(
            "## {}{} · {}\n\n{}\n\nSource: {}\n\n",
            e.author, t, e.at, e.body, e.source
        ));
    }
    out
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::migrated_test_db;
    #[tokio::test]
    async fn includes_live_and_archived_transcript_prose_once() {
        let db = migrated_test_db("home-archived-transcript.db").await;
        let archived = serde_json::json!({"content":"archived assistant"}).to_string();
        let blob = crate::storage::compress(archived.as_bytes()).unwrap();
        db.write(move |conn| {
            let blob = blob.clone();
            Box::pin(async move {
            for sql in [
                "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','default','Project','proj','/tmp/proj',1,1)",
                "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES ('t','p','general',1,1)",
                "INSERT INTO jobs(id,project_id,thread_id,status,uri_segment,created_at,updated_at) VALUES ('j','p','t','complete','thread',1,1)",
                "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('r','j','exited',1,1)",
                "INSERT INTO turns(id,session_id,run_id,sequence,start_reason,created_at,updated_at) VALUES ('ta','s','r',1,'initial',1,1),('tu','s','r',2,'follow_up',2,2)",
                "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id) VALUES ('live','r',2,2,'user','{\"content\":\"live operator\"}',2,'tu')",
            ] {
                conn.execute(sql, ()).await?;
            }
            conn.execute(
                "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id,storage_mode,data_blob,codec) VALUES ('archived','r',1,1,'assistant','{\"archived\":\"zstd\"}',1,'ta','zstd',?1,'zstd_v1')",
                (blob,),
            ).await?;
            Ok(())
        })}).await.unwrap();

        let payload = db
            .read(|conn| {
                Box::pin(async move { query_project_home(conn, "p", "proj", 20, None).await })
            })
            .await
            .unwrap();

        assert_eq!(
            payload
                .entries
                .iter()
                .filter(|e| e.body == "archived assistant")
                .count(),
            1
        );
        assert_eq!(
            payload
                .entries
                .iter()
                .filter(|e| e.body == "live operator")
                .count(),
            1
        );
    }

    /// The index predicate and the query predicate are one fact spelled twice.
    ///
    /// Turso substitutes a partial index only while the query's WHERE terms
    /// imply the index's, and it reports nothing when they stop matching: Home
    /// keeps returning exactly the same entries, having gone back to reading the
    /// whole corpus to produce them. Nothing but this equality catches a
    /// reworded predicate on either side.
    #[test]
    fn home_prose_shape_matches_its_index() {
        let migration = include_str!("../../../../turso_migrations/0225_home_prose_index.sql");
        let unaliased = HOME_PROSE_SHAPE.replace("e.", "");
        assert!(
            migration.contains(&unaliased),
            "idx_events_home_prose must carry the query's prose predicate \
             verbatim, or Turso silently stops substituting it.\n\
             query wants: {unaliased}\nmigration has: {migration}"
        );
    }

    /// The prose predicate must answer "not prose" for a body that is not JSON,
    /// rather than raising.
    ///
    /// A partial index's predicate is evaluated on the WRITE path, once per
    /// event inserted, so a form that raises would not fail a read -- it would
    /// make the row unstorable. Nothing else in the system would notice if this
    /// stopped holding: `events.data` is always valid JSON today, because the
    /// `search_events_*` triggers already reject anything else and all 1.7M rows
    /// of the reported workspace parse. But that invariant belongs to the search
    /// index, and Home should not quietly acquire a second, differently-worded
    /// reason to refuse an agent's transcript if it ever moves.
    ///
    /// The `CASE` in [`HOME_PROSE_SHAPE`] is what makes this hold. Turso's `AND`
    /// evaluates both sides instead of short-circuiting, so the more obvious
    /// `json_valid(data) AND json_type(data, ...)` spelling still reaches
    /// `json_type` with the body that cannot be parsed.
    #[tokio::test]
    async fn the_prose_predicate_tolerates_a_body_that_is_not_json() {
        let db = migrated_test_db("home-prose-nonjson.db").await;
        let sql = format!(
            "SELECT {HOME_PROSE_SHAPE} FROM (SELECT 'this is not json' AS data, \
             NULL AS storage_mode, NULL AS data_blob, NULL AS codec) e"
        );
        let verdict = db
            .read(move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let mut rows = conn.query(&sql, ()).await?;
                    let row = rows.next().await?.expect("one row");
                    row.i64(0)
                })
            })
            .await
            .expect("the prose predicate must not raise on a body that is not JSON");
        assert_eq!(verdict, 0, "a body that is not JSON is not prose");
    }

    /// The plan the whole cost argument rests on.
    ///
    /// Both properties below are ones Turso abandons SILENTLY, and a plan is the
    /// only witness: every entry Home returns stays correct either way, so no
    /// behavioural test can fail. Turso also keeps no table statistics, so it
    /// has nothing to tell it the thread runs are the small side of the join --
    /// which is precisely how the original query came to walk the entire corpus.
    ///
    /// - **`events` must never be SCANned.** A scan means the join inverted
    ///   again: instead of visiting the project's thread runs and seeking each
    ///   one's newest entries, the branch walks every event in the database and
    ///   asks each one whether it belongs to a thread.
    /// - **The walk must reach events through `idx_events_home_prose`.** Any
    ///   other index answers `run_id` but not renderability, so each candidate
    ///   has to be fetched from the table and JSON-parsed to be rejected.
    #[tokio::test]
    async fn home_plan_never_scans_the_event_corpus() {
        let db = migrated_test_db("home-plan.db").await;
        let sql = format!("EXPLAIN QUERY PLAN {}", home_sql());
        let plan = db
            .read(move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            &sql,
                            cairn_db::turso::params![
                                "p",
                                "proj",
                                None::<i64>,
                                None::<String>,
                                50i64
                            ],
                        )
                        .await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps)
                })
            })
            .await
            .unwrap();
        let plan_text = plan.join("\n");

        assert!(
            !plan_text.contains("SCAN events"),
            "a scan of `events` means Home is reading the whole corpus to find \
             one page of thread prose:\n{plan_text}"
        );
        assert!(
            plan_text.contains("idx_events_home_prose"),
            "the candidate walk must reach events through the prose index; any \
             other path has to open and parse each row to reject it:\n{plan_text}"
        );
    }

    /// What Home costs must track the transcript it shows, not the database it
    /// lives in.
    ///
    /// This is the regression that the plan assertion above cannot state and
    /// that no correctness test can reach: the original query returned exactly
    /// these entries, and took time proportional to every unrelated event in the
    /// workspace to do it. Live, that was >30s per fetch against a 1.7M-row
    /// corpus, fifteen of them concurrently.
    ///
    /// So the assertion is on observed work, and the corpus is built so that
    /// work is the only variable: both databases hold the SAME thread
    /// transcript and return the SAME page. They differ only in how much the
    /// query is entitled to ignore -- events in runs that are not threads, and
    /// events in the thread whose bodies are not prose. A ratio rather than a
    /// wall-clock ceiling, and a generous one: the defect makes this scale with
    /// the noise multiplier, so it fails by an order of magnitude while leaving
    /// room for a loaded CI machine.
    #[tokio::test]
    async fn home_cost_does_not_track_irrelevant_corpus_size() {
        const NOISE: usize = 20_000;

        let quiet = seed_home_cost_corpus("home-cost-quiet.db", 0).await;
        let noisy = seed_home_cost_corpus("home-cost-noisy.db", NOISE).await;

        let page = |db: &'static LocalDb| async move {
            db.read(|conn| {
                Box::pin(async move { query_project_home(conn, "p", "proj", 20, None).await })
            })
            .await
            .unwrap()
        };
        let quiet: &'static LocalDb = Box::leak(Box::new(quiet));
        let noisy: &'static LocalDb = Box::leak(Box::new(noisy));

        assert_eq!(
            page(quiet).await.entries.len(),
            page(noisy).await.entries.len(),
            "the noise must not change the answer, or this measures two \
             different questions"
        );

        // The best of several passes, not the mean: a minimum is what the
        // machine can do when nothing else interferes, and is far steadier under
        // a loaded test runner than an average that any one stall can dominate.
        let best = |db: &'static LocalDb| async move {
            let mut best = std::time::Duration::MAX;
            for _ in 0..5 {
                let started = std::time::Instant::now();
                page(db).await;
                best = best.min(started.elapsed());
            }
            best
        };
        let quiet_cost = best(quiet).await;
        let noisy_cost = best(noisy).await;

        assert!(
            noisy_cost < quiet_cost * 8 + std::time::Duration::from_millis(5),
            "Home read {NOISE} irrelevant events to return the same page: \
             {quiet_cost:?} without them, {noisy_cost:?} with them"
        );
    }

    /// Direct messages reach Home through the thread run on either end.
    ///
    /// This branch resolves its `target` and `source` from two independent
    /// LEFT JOINs, and the row is now selected by key and joined back to
    /// afterwards -- so the second pair of joins has to land on the same rows the
    /// first pair chose. A message addressed TO a thread and one sent FROM a
    /// thread take opposite arms of both `CASE`s, which is what makes them worth
    /// asserting together.
    #[tokio::test]
    async fn direct_messages_resolve_their_thread_end() {
        let db = migrated_test_db("home-direct-messages.db").await;
        seed_project_with_thread(&db).await;
        db.write(|conn| {
            Box::pin(async move {
                for sql in [
                    // A second thread, so a thread-to-thread message has a real
                    // run on both ends rather than a NULL standing in.
                    "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES ('t2','p','testing',1,1)",
                    "INSERT INTO jobs(id,project_id,thread_id,status,uri_segment,created_at,updated_at) VALUES ('j3','p','t2','complete','thread',1,1)",
                    "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('r3','j3','exited',1,1)",
                    // Sent by a thread to something that is not a thread.
                    "INSERT INTO messages(id,channel_type,sender_name,sender_run_id,recipient_run_id,content,created_at) VALUES ('from','direct','general','r','outsider','sent from a thread',30)",
                    // Sent to a thread by something that is not a thread.
                    "INSERT INTO messages(id,channel_type,sender_name,sender_run_id,recipient_run_id,content,created_at) VALUES ('to','direct','someone','outsider','r','sent to a thread',20)",
                    // Between two threads.
                    "INSERT INTO messages(id,channel_type,sender_name,sender_run_id,recipient_run_id,content,created_at) VALUES ('both','direct','general','r','r3','thread to thread',10)",
                    // Between two runs that are not threads: not Home's business.
                    "INSERT INTO messages(id,channel_type,sender_name,sender_run_id,recipient_run_id,content,created_at) VALUES ('neither','direct','someone','outsider','other','not a thread at either end',40)",
                ] {
                    conn.execute(sql, ()).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();

        let payload = db
            .read(|conn| {
                Box::pin(async move { query_project_home(conn, "p", "proj", 20, None).await })
            })
            .await
            .unwrap();
        let seen: Vec<_> = payload
            .entries
            .iter()
            .map(|e| (e.id.as_str(), e.target.as_deref(), e.source.as_str()))
            .collect();

        assert_eq!(
            seen,
            vec![
                ("from", None, "cairn://p/proj/general/messages"),
                (
                    "to",
                    Some("cairn://p/proj/general"),
                    "cairn://p/proj/general/messages"
                ),
                (
                    "both",
                    Some("cairn://p/proj/testing"),
                    "cairn://p/proj/general/messages"
                ),
            ],
            "a direct message belongs to Home exactly when a thread run sits on \
             one of its ends, and is attributed to that end"
        );
    }

    /// An entry already delivered as an attention push must not appear again.
    ///
    /// The exclusion has to sit INSIDE the branch's candidate walk, above its
    /// `LIMIT`. Below it, a page would be counted before the pushes were removed
    /// from it, and [`query_project_home`] reads a short page as "nothing older
    /// remains" -- so a run of pushed entries would not merely thin one page, it
    /// would truncate the reader's scrollback at that point.
    #[tokio::test]
    async fn delivered_attention_pushes_stay_out_of_home() {
        let db = migrated_test_db("home-attention-pushes.db").await;
        seed_project_with_thread(&db).await;
        db.write(|conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id)
                     VALUES ('spoken','r',1,1,'user','{\"content\":\"a person said this\"}',10,'tu'),
                            ('pushed','r',2,2,'user','{\"content\":\"a push delivered this\"}',20,'tu')",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO attention_pushes(id,recipient,content_ref,wake,boundary,\"key\",delivered_event_id,created_at)
                     VALUES ('ap','j','cairn://p/proj/1','wake','turn','k','pushed',20)",
                    (),
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

        let payload = db
            .read(|conn| {
                Box::pin(async move { query_project_home(conn, "p", "proj", 20, None).await })
            })
            .await
            .unwrap();
        assert_eq!(
            payload
                .entries
                .iter()
                .map(|e| e.id.as_str())
                .collect::<Vec<_>>(),
            vec!["spoken"],
            "an entry the operator was already pushed must not be restated here"
        );
    }

    /// What Home costs at the shape of the reported workspace, printed rather
    /// than asserted.
    ///
    /// Ignored by default: it loads ~1.7M events, and a wall-clock threshold in
    /// CI would be a flake. Run it when changing the query or its indexes:
    ///
    /// ```text
    /// cargo test -p cairn-core --lib home_cost_at_reported_scale -- --ignored --nocapture
    /// ```
    ///
    /// The row counts are the ones measured on the workspace in CAIRN-4621, so
    /// the numbers are comparable to the live `EXPLAIN QUERY PLAN` taken there:
    /// 776 thread runs, 19,316 assistant and 1,409 user entries inside them,
    /// 31,700 direct and 318 thread messages, 36,076 attention pushes, and a
    /// corpus of 1.7M events overall. It also times building
    /// `idx_events_home_prose` over that corpus, which is the one-time cost the
    /// migration adds to a first launch.
    #[tokio::test]
    #[ignore = "measurement, not an assertion; run with --ignored --nocapture"]
    async fn home_cost_at_reported_scale() {
        let db = seed_reported_scale_corpus("home-reported-scale.db").await;

        // The migration's one-time cost, measured over a loaded corpus rather
        // than an empty one.
        db.execute_batch("DROP INDEX IF EXISTS idx_events_home_prose")
            .await
            .unwrap();
        let started = std::time::Instant::now();
        db.execute_batch(include_str!(
            "../../../../turso_migrations/0225_home_prose_index.sql"
        ))
        .await
        .unwrap();
        println!("index build over 1.7M events: {:?}", started.elapsed());

        let page = |before: Option<(i64, String)>| {
            let db = &db;
            async move {
                db.read(move |conn| {
                    let before = before.clone();
                    Box::pin(async move { query_project_home(conn, "p", "proj", 50, before).await })
                })
                .await
                .unwrap()
            }
        };

        let first = page(None).await;
        let mut best = std::time::Duration::MAX;
        for _ in 0..5 {
            let started = std::time::Instant::now();
            page(None).await;
            best = best.min(started.elapsed());
        }
        println!("newest page ({} entries): {best:?}", first.entries.len());

        // A page reached by walking the cursor, which is where a per-branch
        // bound that only held for the newest page would show up.
        if let Some(cursor) = first.next_cursor.clone() {
            let (at, id) = cursor.split_once(':').unwrap();
            let deep = Some((at.parse().unwrap(), id.to_string()));
            let mut best = std::time::Duration::MAX;
            for _ in 0..5 {
                let started = std::time::Instant::now();
                page(deep.clone()).await;
                best = best.min(started.elapsed());
            }
            println!("second page: {best:?}");
        }
    }

    /// A corpus shaped like the workspace CAIRN-4621 was reported against.
    async fn seed_reported_scale_corpus(name: &str) -> LocalDb {
        const THREADS: usize = 30;
        const RUNS: usize = 776;
        const THREAD_ASSISTANT: usize = 19_316;
        const THREAD_USER: usize = 1_409;
        const TOTAL_EVENTS: usize = 1_700_000;
        const DIRECT_MESSAGES: usize = 31_700;
        const THREAD_MESSAGES: usize = 318;
        const PUSHES: usize = 36_076;

        let db = migrated_test_db(name).await;
        db.write(move |conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','default','P','proj','/tmp/p',1,1)",
                    (),
                )
                .await?;
                // Threads, each with a root thread job, and runs spread across them.
                for thread in 0..THREADS {
                    conn.execute(
                        &format!("INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES ('t{thread}','p','thread{thread}',1,1)"),
                        (),
                    )
                    .await?;
                    conn.execute(
                        &format!("INSERT INTO jobs(id,project_id,thread_id,status,uri_segment,created_at,updated_at) VALUES ('j{thread}','p','t{thread}','complete','thread',1,1)"),
                        (),
                    )
                    .await?;
                }
                for run in 0..RUNS {
                    let thread = run % THREADS;
                    conn.execute(
                        &format!("INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('r{run}','j{thread}','exited',1,1)"),
                        (),
                    )
                    .await?;
                    conn.execute(
                        &format!("INSERT INTO turns(id,session_id,run_id,sequence,start_reason,created_at,updated_at) VALUES ('tu{run}','s{run}','r{run}',1,'initial',1,1)"),
                        (),
                    )
                    .await?;
                }
                // A job and run outside the threads, holding the bulk of the corpus.
                for sql in [
                    "INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES ('jx','p','complete','builder',1,1)",
                    "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('rx','jx','exited',1,1)",
                    "INSERT INTO turns(id,session_id,run_id,sequence,start_reason,created_at,updated_at) VALUES ('tux','sx','rx',1,'initial',1,1)",
                ] {
                    conn.execute(sql, ()).await?;
                }

                // Roughly half the thread assistant entries are prose; the rest
                // carry tool-call content blocks, as measured live.
                let prose = "{\"content\":\"a paragraph of assistant prose\"}";
                let blocks = "{\"content\":[{\"type\":\"tool_use\",\"name\":\"Read\"}]}";
                let mut at = 0i64;
                let mut batch: Vec<String> = Vec::with_capacity(500);
                let mut flush = |batch: &mut Vec<String>| {
                    let sql = format!(
                        "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id) VALUES {}",
                        batch.join(",")
                    );
                    batch.clear();
                    sql
                };
                for i in 0..THREAD_ASSISTANT {
                    at += 1;
                    let run = i % RUNS;
                    let body = if i % 2 == 0 { prose } else { blocks };
                    batch.push(format!(
                        "('a{i}','r{run}',{at},{at},'assistant','{body}',{at},'tu{run}')"
                    ));
                    if batch.len() == 500 {
                        conn.execute(&flush(&mut batch), ()).await?;
                    }
                }
                if !batch.is_empty() {
                    conn.execute(&flush(&mut batch), ()).await?;
                }
                for i in 0..THREAD_USER {
                    at += 1;
                    let run = i % RUNS;
                    batch.push(format!(
                        "('u{i}','r{run}',{at},{at},'user','{prose}',{at},'tu{run}')"
                    ));
                    if batch.len() == 500 {
                        conn.execute(&flush(&mut batch), ()).await?;
                    }
                }
                if !batch.is_empty() {
                    conn.execute(&flush(&mut batch), ()).await?;
                }
                // Everything else: events in a run that is not a thread run.
                for i in 0..(TOTAL_EVENTS - THREAD_ASSISTANT - THREAD_USER) {
                    at += 1;
                    batch.push(format!(
                        "('x{i}','rx',{at},{at},'assistant','{blocks}',{at},'tux')"
                    ));
                    if batch.len() == 500 {
                        conn.execute(&flush(&mut batch), ()).await?;
                    }
                }
                if !batch.is_empty() {
                    conn.execute(&flush(&mut batch), ()).await?;
                }

                // Messages: mostly direct, only a small share touching a thread run.
                let mut batch: Vec<String> = Vec::with_capacity(500);
                let mut flush_messages = |batch: &mut Vec<String>| {
                    let sql = format!(
                        "INSERT INTO messages(id,channel_type,channel_id,sender_name,sender_run_id,recipient_run_id,content,created_at) VALUES {}",
                        batch.join(",")
                    );
                    batch.clear();
                    sql
                };
                for i in 0..DIRECT_MESSAGES {
                    at += 1;
                    // Roughly 7% sit on a thread run, as measured live.
                    let sender = if i % 14 == 0 {
                        format!("'r{}'", i % RUNS)
                    } else {
                        "'rx'".into()
                    };
                    batch.push(format!(
                        "('dm{i}','direct',NULL,'someone',{sender},'rx','a direct message',{at})"
                    ));
                    if batch.len() == 500 {
                        conn.execute(&flush_messages(&mut batch), ()).await?;
                    }
                }
                if !batch.is_empty() {
                    conn.execute(&flush_messages(&mut batch), ()).await?;
                }
                for i in 0..THREAD_MESSAGES {
                    at += 1;
                    let thread = i % THREADS;
                    batch.push(format!(
                        "('tm{i}','thread','t{thread}','someone',NULL,NULL,'a thread message',{at})"
                    ));
                    if batch.len() == 500 {
                        conn.execute(&flush_messages(&mut batch), ()).await?;
                    }
                }
                if !batch.is_empty() {
                    conn.execute(&flush_messages(&mut batch), ()).await?;
                }

                // Attention pushes, each retiring one user entry from Home.
                let mut batch: Vec<String> = Vec::with_capacity(500);
                for i in 0..PUSHES {
                    let thread = i % THREADS;
                    let delivered = if i < THREAD_USER {
                        format!("'u{i}'")
                    } else {
                        "NULL".into()
                    };
                    batch.push(format!(
                        "('ap{i}','j{thread}','cairn://p/proj/1','wake','turn','k{i}',{delivered},1)"
                    ));
                    if batch.len() == 500 {
                        let sql = format!(
                            "INSERT INTO attention_pushes(id,recipient,content_ref,wake,boundary,\"key\",delivered_event_id,created_at) VALUES {}",
                            batch.join(",")
                        );
                        batch.clear();
                        conn.execute(&sql, ()).await?;
                    }
                }
                if !batch.is_empty() {
                    let sql = format!(
                        "INSERT INTO attention_pushes(id,recipient,content_ref,wake,boundary,\"key\",delivered_event_id,created_at) VALUES {}",
                        batch.join(",")
                    );
                    conn.execute(&sql, ()).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    async fn seed_project_with_thread(db: &LocalDb) {
        db.write(|conn| {
            Box::pin(async move {
                for sql in [
                    "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','default','Project','proj','/tmp/proj',1,1)",
                    "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES ('t','p','general',1,1)",
                    "INSERT INTO jobs(id,project_id,thread_id,status,uri_segment,created_at,updated_at) VALUES ('j','p','t','complete','thread',1,1)",
                    "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('r','j','exited',1,1)",
                    "INSERT INTO turns(id,session_id,run_id,sequence,start_reason,created_at,updated_at) VALUES ('tu','s','r',1,'initial',1,1)",
                ] {
                    conn.execute(sql, ()).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// One thread holding a fixed, small transcript, buried under `noise`
    /// events Home is entitled to ignore.
    ///
    /// The noise is split between the two ways an event can be irrelevant, so
    /// neither exclusion can regress unnoticed: half sit in a run that is not a
    /// thread run, and half sit in the thread itself but carry tool-call content
    /// blocks rather than prose. Both kinds are newer than the transcript, so a
    /// query that walks the corpus in time order meets all of them first.
    async fn seed_home_cost_corpus(name: &str, noise: usize) -> LocalDb {
        let db = migrated_test_db(name).await;
        seed_project_with_thread(&db).await;
        db.write(move |conn| {
            Box::pin(async move {
                // A job and run that belong to an issue rather than a thread.
                for sql in [
                    "INSERT INTO jobs(id,project_id,status,node_name,created_at,updated_at) VALUES ('j2','p','complete','builder',1,1)",
                    "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('r2','j2','exited',1,1)",
                    "INSERT INTO turns(id,session_id,run_id,sequence,start_reason,created_at,updated_at) VALUES ('tu2','s2','r2',1,'initial',1,1)",
                ] {
                    conn.execute(sql, ()).await?;
                }
                // The transcript under test: 40 prose events at times 1..=40.
                let mut values = Vec::new();
                for seq in 1..=40 {
                    values.push(format!(
                        "('prose{seq}','r',{seq},{seq},'assistant','{{\"content\":\"entry {seq}\"}}',{seq},'tu')"
                    ));
                }
                conn.execute(
                    &format!(
                        "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id) VALUES {}",
                        values.join(",")
                    ),
                    (),
                )
                .await?;
                // Everything Home must skip, all of it newer than the prose.
                let block = "{\"content\":[{\"type\":\"tool_use\",\"name\":\"Read\",\"input\":{\"path\":\"src/lib.rs\"}}]}";
                for chunk in (0..noise).collect::<Vec<_>>().chunks(500) {
                    let mut values = Vec::new();
                    for i in chunk {
                        let at = 100 + i;
                        let (run, turn) = if i % 2 == 0 { ("r2", "tu2") } else { ("r", "tu") };
                        values.push(format!(
                            "('noise{i}','{run}',{at},{at},'assistant','{block}',{at},'{turn}')"
                        ));
                    }
                    conn.execute(
                        &format!(
                            "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id) VALUES {}",
                            values.join(",")
                        ),
                        (),
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
        db
    }

    /// The union is only correct if taking the top-N of each branch still yields
    /// the global top-N. Interleave two sources across a page boundary and walk
    /// the cursor to prove ordering and completeness survive the pushdown.
    #[tokio::test]
    async fn pagination_merges_branches_in_global_order() {
        let db = migrated_test_db("home-branch-pagination.db").await;
        db.write(move |conn| {
            Box::pin(async move {
                for sql in [
                    "INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES ('p','default','Project','proj','/tmp/proj',1,1)",
                    "INSERT INTO threads(id,project_id,name,created_at,updated_at) VALUES ('t','p','general',1,1)",
                    "INSERT INTO jobs(id,project_id,thread_id,status,uri_segment,created_at,updated_at) VALUES ('j','p','t','complete','thread',1,1)",
                    "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES ('r','j','exited',1,1)",
                    "INSERT INTO turns(id,session_id,run_id,sequence,start_reason,created_at,updated_at) VALUES ('tu','s','r',1,'initial',1,1)",
                    // events at 10 and 30, messages at 20 and 40: the branches interleave.
                    "INSERT INTO events(id,run_id,sequence,timestamp,event_type,data,created_at,turn_id) VALUES ('e10','r',1,10,'user','{\"content\":\"e10\"}',10,'tu'),('e30','r',2,30,'user','{\"content\":\"e30\"}',30,'tu')",
                    "INSERT INTO messages(id,channel_type,channel_id,sender_name,content,created_at) VALUES ('m20','thread','t','someone','m20',20),('m40','thread','t','someone','m40',40)",
                ] {
                    conn.execute(sql, ()).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();

        let page = |before: Option<(i64, String)>| {
            let db = &db;
            async move {
                db.read(move |conn| {
                    let before = before.clone();
                    Box::pin(async move { query_project_home(conn, "p", "proj", 2, before).await })
                })
                .await
                .unwrap()
            }
        };

        let first = page(None).await;
        assert_eq!(
            first.entries.iter().map(|e| e.at).collect::<Vec<_>>(),
            vec![40, 30],
            "newest page must merge both branches in global order"
        );

        let cursor = first.next_cursor.clone().expect("more entries remain");
        let (at, id) = cursor.split_once(':').unwrap();
        let second = page(Some((at.parse().unwrap(), id.to_string()))).await;
        assert_eq!(
            second.entries.iter().map(|e| e.at).collect::<Vec<_>>(),
            vec![20, 10],
            "the cursor must resume across branch boundaries without gaps"
        );
    }
}
