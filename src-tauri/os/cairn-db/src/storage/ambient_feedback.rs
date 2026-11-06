//! The ambient routing label set (`ambient_routing_feedback`): one row per
//! outcome of the ambient Discord route, keyed by the fact log row the message
//! minted. Rows are written by the channels sweep from the route's firings, by
//! a thread's feedback write, and stamped once by the 48-hour engagement pass.
//! Every timestamp is epoch seconds.

use super::{DbResult, LocalDb, RowExt};
use crate::turso::params;

/// Where a row's recipient and features came from.
pub mod addresses_from {
    /// The route's surviving firings for the event.
    pub const FIRINGS: &str = "firings";
    /// Reserved for the durable decision record (cairn/6317); nothing writes it yet.
    pub const DECISION_RECORD: &str = "decision_record";
    /// The firings were gone when the sweep reached the event.
    pub const NONE: &str = "none";
    /// A feedback write created the row before the sweep reached its event.
    pub const LABEL: &str = "label";
}

/// The outcome recorded when an event's firings were evicted before the sweep.
pub const OUTCOME_UNRECORDED: &str = "unrecorded";

/// One outcome row as the sweep (or a feedback write) first records it.
#[derive(Debug, Clone, PartialEq)]
pub struct NewAmbientFeedback {
    pub project_key: String,
    pub event_id: i64,
    /// The delivered thread's URI; `None` for the event's refused-side row.
    pub recipient_thread: Option<String>,
    pub outcome: String,
    pub pick: Option<String>,
    pub probability: Option<f64>,
    pub measured_by: Option<String>,
    pub revision: Option<String>,
    pub picks_json: Option<String>,
    pub addresses_from: String,
    pub event_at: i64,
    pub swept_at: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AmbientFeedbackRow {
    pub id: i64,
    pub project_key: String,
    pub event_id: i64,
    pub recipient_thread: Option<String>,
    pub outcome: String,
    pub pick: Option<String>,
    pub probability: Option<f64>,
    pub measured_by: Option<String>,
    pub revision: Option<String>,
    pub picks_json: Option<String>,
    pub addresses_from: String,
    pub verdict: Option<String>,
    pub reroute_to: Option<String>,
    pub note: Option<String>,
    pub labelled_by: Option<String>,
    pub first_labelled_at: Option<i64>,
    pub labelled_at: Option<i64>,
    pub engaged: Option<bool>,
    pub engaged_by: Option<String>,
    pub engagement_checked_at: Option<i64>,
    pub event_at: i64,
    pub swept_at: i64,
}

const COLUMNS: &str = "id,project_key,event_id,recipient_thread,outcome,pick,probability,measured_by,revision,picks_json,addresses_from,verdict,reroute_to,note,labelled_by,first_labelled_at,labelled_at,engaged,engaged_by,engagement_checked_at,event_at,swept_at";

fn from_row(row: &crate::turso::Row) -> DbResult<AmbientFeedbackRow> {
    Ok(AmbientFeedbackRow {
        id: row.i64(0)?,
        project_key: row.text(1)?,
        event_id: row.i64(2)?,
        recipient_thread: row.opt_text(3)?,
        outcome: row.text(4)?,
        pick: row.opt_text(5)?,
        probability: row.opt_f64(6)?,
        measured_by: row.opt_text(7)?,
        revision: row.opt_text(8)?,
        picks_json: row.opt_text(9)?,
        addresses_from: row.text(10)?,
        verdict: row.opt_text(11)?,
        reroute_to: row.opt_text(12)?,
        note: row.opt_text(13)?,
        labelled_by: row.opt_text(14)?,
        first_labelled_at: row.opt_i64(15)?,
        labelled_at: row.opt_i64(16)?,
        engaged: row.opt_i64(17)?.map(|value| value != 0),
        engaged_by: row.opt_text(18)?,
        engagement_checked_at: row.opt_i64(19)?,
        event_at: row.i64(20)?,
        swept_at: row.i64(21)?,
    })
}

/// What [`record_swept`] did with one batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweptCounts {
    pub inserted: u64,
    /// Rows a feedback write created first, now given the sweep's features.
    pub filled: u64,
}

async fn existing_source(
    conn: &crate::storage::TrackedConnection,
    row: &NewAmbientFeedback,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT addresses_from FROM ambient_routing_feedback WHERE project_key=?1 AND event_id=?2 AND recipient_thread IS ?3",
            params![row.project_key.clone(), row.event_id, row.recipient_thread.clone()],
        )
        .await?;
    match rows.next().await? {
        Some(found) => Ok(Some(found.text(0)?)),
        None => Ok(None),
    }
}

async fn insert_conn(
    conn: &crate::storage::TrackedConnection,
    row: &NewAmbientFeedback,
) -> DbResult<()> {
    conn.execute(
        "INSERT INTO ambient_routing_feedback (project_key,event_id,recipient_thread,outcome,pick,probability,measured_by,revision,picks_json,addresses_from,event_at,swept_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
        params![
            row.project_key.clone(),
            row.event_id,
            row.recipient_thread.clone(),
            row.outcome.clone(),
            row.pick.clone(),
            row.probability,
            row.measured_by.clone(),
            row.revision.clone(),
            row.picks_json.clone(),
            row.addresses_from.clone(),
            row.event_at,
            row.swept_at
        ],
    )
    .await?;
    Ok(())
}

/// Record one sweep batch and advance the cursor to `cursor` in the same
/// transaction. A row that already exists is left alone, except one a feedback
/// write created first (`addresses_from = 'label'`), which takes the sweep's
/// features and source while keeping its label. Existence is checked before
/// inserting: the sweep re-reaches nothing, but a write may have raced it.
pub async fn record_swept(
    db: &LocalDb,
    rows: Vec<NewAmbientFeedback>,
    cursor: i64,
) -> DbResult<SweptCounts> {
    db.write(move |conn| {
        let rows = rows.clone();
        Box::pin(async move {
            let mut counts = SweptCounts::default();
            for row in &rows {
                match existing_source(conn, row).await?.as_deref() {
                    None => {
                        insert_conn(conn, row).await?;
                        counts.inserted += 1;
                    }
                    Some(addresses_from::LABEL) => {
                        conn.execute(
                            "UPDATE ambient_routing_feedback SET outcome=?4,pick=?5,probability=?6,measured_by=?7,revision=?8,picks_json=?9,addresses_from=?10,swept_at=?11 WHERE project_key=?1 AND event_id=?2 AND recipient_thread IS ?3 AND addresses_from='label'",
                            params![
                                row.project_key.clone(),
                                row.event_id,
                                row.recipient_thread.clone(),
                                row.outcome.clone(),
                                row.pick.clone(),
                                row.probability,
                                row.measured_by.clone(),
                                row.revision.clone(),
                                row.picks_json.clone(),
                                row.addresses_from.clone(),
                                row.swept_at
                            ],
                        )
                        .await?;
                        counts.filled += 1;
                    }
                    Some(_) => {}
                }
            }
            conn.execute(
                "INSERT INTO ambient_routing_feedback_cursor (id, fact_log_id) VALUES (1, ?1) ON CONFLICT(id) DO UPDATE SET fact_log_id=excluded.fact_log_id WHERE excluded.fact_log_id > ambient_routing_feedback_cursor.fact_log_id",
                (cursor,),
            )
            .await?;
            Ok(counts)
        })
    })
    .await
}

/// The highest fact log id the sweep has settled; 0 before its first pass.
pub async fn sweep_cursor(db: &LocalDb) -> DbResult<i64> {
    Ok(db
        .query_opt_i64(
            "SELECT fact_log_id FROM ambient_routing_feedback_cursor WHERE id=1",
            (),
        )
        .await?
        .unwrap_or(0))
}

/// One event's row for `recipient` (`None`: its refused-side row).
pub async fn find(
    db: &LocalDb,
    project_key: &str,
    event_id: i64,
    recipient: Option<&str>,
) -> DbResult<Option<AmbientFeedbackRow>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM ambient_routing_feedback WHERE project_key=?1 AND event_id=?2 AND recipient_thread IS ?3"),
        params![project_key.to_string(), event_id, recipient.map(str::to_owned)],
        from_row,
    )
    .await
}

/// Insert `row` unless its key already holds one, and return what is stored.
pub async fn ensure(db: &LocalDb, row: NewAmbientFeedback) -> DbResult<AmbientFeedbackRow> {
    let key = (
        row.project_key.clone(),
        row.event_id,
        row.recipient_thread.clone(),
    );
    db.write(move |conn| {
        let row = row.clone();
        Box::pin(async move {
            if existing_source(conn, &row).await?.is_none() {
                insert_conn(conn, &row).await?;
            }
            Ok(())
        })
    })
    .await?;
    find(db, &key.0, key.1, key.2.as_deref())
        .await?
        .ok_or_else(|| super::DbError::internal("ensured ambient feedback row disappeared"))
}

/// A label as one feedback write sets it.
#[derive(Debug, Clone, PartialEq)]
pub struct AmbientLabel {
    pub verdict: String,
    pub reroute_to: Option<String>,
    pub note: Option<String>,
    pub labelled_by: String,
    pub labelled_at: i64,
}

/// Replace row `id`'s label. The first label's time survives every later one.
pub async fn set_label(
    db: &LocalDb,
    id: i64,
    label: AmbientLabel,
) -> DbResult<Option<AmbientFeedbackRow>> {
    db.execute(
        "UPDATE ambient_routing_feedback SET verdict=?2, reroute_to=?3, note=?4, labelled_by=?5, labelled_at=?6, first_labelled_at=COALESCE(first_labelled_at, ?6) WHERE id=?1",
        params![
            id,
            label.verdict,
            label.reroute_to,
            label.note,
            label.labelled_by,
            label.labelled_at
        ],
    )
    .await?;
    get(db, id).await
}

pub async fn get(db: &LocalDb, id: i64) -> DbResult<Option<AmbientFeedbackRow>> {
    db.query_opt(
        format!("SELECT {COLUMNS} FROM ambient_routing_feedback WHERE id=?1"),
        (id,),
        from_row,
    )
    .await
}

/// The engagement filter a read selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engagement {
    Engaged,
    NotEngaged,
    Unchecked,
}

/// A read over the label set. Every field narrows; `None` does not.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AmbientFeedbackFilter {
    /// Rows whose event is at or after this epoch second.
    pub since: Option<i64>,
    pub verdict: Option<String>,
    pub outcome: Option<String>,
    /// Rows delivered to, or re-routed to, this thread URI.
    pub thread: Option<String>,
    pub engaged: Option<Engagement>,
}

fn filter_sql(filter: &AmbientFeedbackFilter) -> (String, Vec<crate::turso::Value>) {
    use crate::turso::Value;
    let mut clauses = Vec::new();
    let mut binds = Vec::new();
    let mut bind = |clause: &str, value: Value, binds: &mut Vec<Value>| {
        binds.push(value);
        clauses.push(clause.replace('?', &format!("?{}", binds.len())));
    };
    if let Some(since) = filter.since {
        bind("event_at >= ?", Value::Integer(since), &mut binds);
    }
    if let Some(verdict) = &filter.verdict {
        bind("verdict = ?", Value::Text(verdict.clone()), &mut binds);
    }
    if let Some(outcome) = &filter.outcome {
        bind("outcome = ?", Value::Text(outcome.clone()), &mut binds);
    }
    if let Some(thread) = &filter.thread {
        binds.push(Value::Text(thread.clone()));
        let n = binds.len();
        clauses.push(format!("(recipient_thread = ?{n} OR reroute_to = ?{n})"));
    }
    match filter.engaged {
        Some(Engagement::Engaged) => clauses.push("engaged = 1".into()),
        Some(Engagement::NotEngaged) => clauses.push("engaged = 0".into()),
        Some(Engagement::Unchecked) => clauses.push("engagement_checked_at IS NULL".into()),
        None => {}
    }
    let sql = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    (sql, binds)
}

/// The filtered rows, newest event first.
pub async fn list(
    db: &LocalDb,
    filter: &AmbientFeedbackFilter,
    limit: i64,
) -> DbResult<Vec<AmbientFeedbackRow>> {
    let (clause, mut binds) = filter_sql(filter);
    binds.push(crate::turso::Value::Integer(limit));
    let n = binds.len();
    db.query_all(
        format!("SELECT {COLUMNS} FROM ambient_routing_feedback{clause} ORDER BY event_id DESC, recipient_thread LIMIT ?{n}"),
        binds,
        from_row,
    )
    .await
}

/// `(rows, rows with outcome unrecorded)` under `filter`: how many events the
/// sweep reached only after route_firings had evicted their firings.
pub async fn counts(db: &LocalDb, filter: &AmbientFeedbackFilter) -> DbResult<(i64, i64)> {
    let (clause, binds) = filter_sql(filter);
    let sql = format!(
        "SELECT COUNT(*), COALESCE(SUM(CASE WHEN outcome='{OUTCOME_UNRECORDED}' THEN 1 ELSE 0 END), 0) FROM ambient_routing_feedback{clause}"
    );
    Ok(db
        .query_opt(sql, binds, |row| Ok((row.i64(0)?, row.i64(1)?)))
        .await?
        .unwrap_or((0, 0)))
}

/// Rows whose engagement is unchecked and whose event is at or before
/// `due_by` (now less the horizon), oldest first.
pub async fn pending_engagement(
    db: &LocalDb,
    due_by: i64,
    limit: i64,
) -> DbResult<Vec<AmbientFeedbackRow>> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM ambient_routing_feedback WHERE engagement_checked_at IS NULL AND event_at <= ?1 ORDER BY event_at, id LIMIT ?2"),
        (due_by, limit),
        from_row,
    )
    .await
}

/// Settle one row's engagement, once.
pub async fn stamp_engagement(
    db: &LocalDb,
    id: i64,
    engaged: bool,
    engaged_by: Option<String>,
    checked_at: i64,
) -> DbResult<()> {
    db.execute(
        "UPDATE ambient_routing_feedback SET engaged=?2, engaged_by=?3, engagement_checked_at=?4 WHERE id=?1 AND engagement_checked_at IS NULL",
        params![id, i64::from(engaged), engaged_by, checked_at],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn swept(event_id: i64, recipient: Option<&str>, source: &str) -> NewAmbientFeedback {
        NewAmbientFeedback {
            project_key: "cairn".into(),
            event_id,
            recipient_thread: recipient.map(str::to_owned),
            outcome: "admitted".into(),
            pick: Some("devops".into()),
            probability: Some(0.9),
            measured_by: Some("native".into()),
            revision: Some("abc".into()),
            picks_json: Some("[\"devops\"]".into()),
            addresses_from: source.into(),
            event_at: 100,
            swept_at: 200,
        }
    }

    #[tokio::test]
    async fn a_repeated_sweep_inserts_nothing_and_the_cursor_only_advances() {
        let db = crate::storage::migrated_test_db("ambient-feedback-idempotent.db").await;
        let devops = Some("cairn://p/cairn/devops");
        let batch = vec![
            swept(1, devops, addresses_from::FIRINGS),
            swept(1, Some("cairn://p/cairn/docs"), addresses_from::FIRINGS),
            swept(2, None, addresses_from::NONE),
        ];
        let first = record_swept(&db, batch.clone(), 5).await.unwrap();
        assert_eq!(first.inserted, 3);
        let again = record_swept(&db, batch, 3).await.unwrap();
        assert_eq!(again, SweptCounts::default());
        assert_eq!(sweep_cursor(&db).await.unwrap(), 5, "never moves back");
        let (total, unrecorded) = counts(&db, &AmbientFeedbackFilter::default())
            .await
            .unwrap();
        assert_eq!((total, unrecorded), (3, 0));
    }

    #[tokio::test]
    async fn the_refused_side_holds_one_row_per_event() {
        let db = crate::storage::migrated_test_db("ambient-feedback-refused.db").await;
        record_swept(&db, vec![swept(7, None, addresses_from::NONE)], 7)
            .await
            .unwrap();
        let duplicate = db
            .execute(
                "INSERT INTO ambient_routing_feedback (project_key,event_id,outcome,addresses_from,event_at,swept_at) VALUES ('cairn',7,'refused','none',1,1)",
                (),
            )
            .await;
        assert!(duplicate.is_err(), "the partial unique index holds");
    }

    #[tokio::test]
    async fn a_label_row_takes_the_sweeps_features_and_keeps_its_label() {
        let db = crate::storage::migrated_test_db("ambient-feedback-fill.db").await;
        let mut early = swept(3, None, addresses_from::LABEL);
        early.outcome = OUTCOME_UNRECORDED.into();
        early.pick = None;
        let row = ensure(&db, early).await.unwrap();
        set_label(
            &db,
            row.id,
            AmbientLabel {
                verdict: "reroute".into(),
                reroute_to: Some("cairn://p/cairn/docs".into()),
                note: None,
                labelled_by: "cairn://p/cairn/devops".into(),
                labelled_at: 150,
            },
        )
        .await
        .unwrap();
        let mut later = swept(3, None, addresses_from::FIRINGS);
        later.outcome = "below_threshold".into();
        let counts = record_swept(&db, vec![later], 3).await.unwrap();
        assert_eq!(counts.filled, 1);
        let row = find(&db, "cairn", 3, None).await.unwrap().unwrap();
        assert_eq!(
            (
                row.outcome.as_str(),
                row.addresses_from.as_str(),
                row.pick.as_deref(),
                row.verdict.as_deref(),
                row.first_labelled_at,
            ),
            (
                "below_threshold",
                "firings",
                Some("devops"),
                Some("reroute"),
                Some(150)
            )
        );
    }

    #[tokio::test]
    async fn a_second_label_keeps_the_first_labelled_time() {
        let db = crate::storage::migrated_test_db("ambient-feedback-relabel.db").await;
        let row = ensure(&db, swept(4, Some("cairn://p/cairn/devops"), "firings"))
            .await
            .unwrap();
        let label = |verdict: &str, at: i64| AmbientLabel {
            verdict: verdict.into(),
            reroute_to: None,
            note: None,
            labelled_by: "cairn://p/cairn/devops".into(),
            labelled_at: at,
        };
        set_label(&db, row.id, label("not_mine", 10)).await.unwrap();
        let row = set_label(&db, row.id, label("reroute", 20))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                row.verdict.as_deref(),
                row.first_labelled_at,
                row.labelled_at
            ),
            (Some("reroute"), Some(10), Some(20))
        );
    }

    #[tokio::test]
    async fn engagement_is_due_after_the_horizon_and_stamped_once() {
        let db = crate::storage::migrated_test_db("ambient-feedback-engagement.db").await;
        record_swept(
            &db,
            vec![swept(5, Some("cairn://p/cairn/devops"), "firings")],
            5,
        )
        .await
        .unwrap();
        assert!(pending_engagement(&db, 99, 10).await.unwrap().is_empty());
        let [due] = pending_engagement(&db, 100, 10)
            .await
            .unwrap()
            .try_into()
            .unwrap();
        stamp_engagement(&db, due.id, true, None, 300)
            .await
            .unwrap();
        stamp_engagement(&db, due.id, false, None, 400)
            .await
            .unwrap();
        let row = get(&db, due.id).await.unwrap().unwrap();
        assert_eq!(
            (row.engaged, row.engagement_checked_at),
            (Some(true), Some(300))
        );
        let filter = AmbientFeedbackFilter {
            engaged: Some(Engagement::Engaged),
            thread: Some("cairn://p/cairn/devops".into()),
            ..Default::default()
        };
        assert_eq!(list(&db, &filter, 10).await.unwrap().len(), 1);
        let filter = AmbientFeedbackFilter {
            engaged: Some(Engagement::Unchecked),
            ..Default::default()
        };
        assert!(list(&db, &filter, 10).await.unwrap().is_empty());
    }
}
