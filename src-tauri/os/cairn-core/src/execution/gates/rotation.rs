//! The execution queue's ONE order: lanes in rotation (cairn/5943).
//!
//! Every reader of the queue (the sweep's release walk, the fleet cap's
//! ahead-count, the fleet places in line, the queue page and the operator's
//! move) orders the cap-held executions through [`release_order`], so the
//! page's numbering is the sequence the sweeps release in.
//!
//! ## Lanes
//!
//! A lane is the parent that filed the work (`cairn_db::queue_lane_sql!`):
//! `thread:<id>` for a thread's child, `issue:<id>` for a child of a
//! coordinator or other parent issue, and one `shared` lane for top-level
//! issues and issue-less executions. A burst comes from one parent filing
//! many children, so each filer gets one turn per round. Parentless work
//! shares a lane: were each top-level filing its own lane, every one would be
//! a fresh, never-served lane and would jump ahead of every thread.
//!
//! ## Turns
//!
//! Every release (a sweep's, Start now, an evaluation, a gate patch's) and
//! every capped filing that starts at once records its lane's TURN in
//! `queue_lane_turns`, in the same transaction as the start itself, through
//! [`record_turn_conn`]. A turn stamp is minted like a queue key ([`mint_turn`]):
//! its second scaled, or one past the newest stamp in any database, read under
//! the admission lock, so stamps are unique and increasing across databases
//! even within one second. The rotation therefore survives a restart, and a
//! release that loses its guarded update records nothing.
//!
//! A lane's turn is written in the database that owns the released execution,
//! and a thread's or parent issue's children live in that parent's database,
//! so a `thread:` or `issue:` lane's stamp is read from the databases its
//! held executions live in. Only `shared` spans databases; it is read from
//! every one, and the newest stamp wins.
//!
//! ## The order
//!
//! First an ordered PREFIX: the pinned executions (an operator's move, see
//! `reorder`), by key. Each consumes a turn for its lane, as its release will.
//! Then the lanes take turns round-robin over a fixed rank: never served
//! first, then least recently served, ties broken by each lane's head. Within
//! a lane, executions keep their key order (`fleet_order_key_sql!`, then
//! database, then id), which is where a thread's own `order` acts.
//!
//! The entry set is the page's set (`capacity::cap_held_sql!`). Each cap
//! filters it for its own count; nobody orders a subset, because round-robin
//! over a subset is a different interleave.

use std::collections::{BTreeMap, HashMap};

use cairn_db::turso::{params, Connection};
use cairn_workflow::gates::store::QUEUE_KEY_SCALE;

use crate::storage::DbResult;

/// The lane of top-level issues and issue-less executions.
pub(crate) const SHARED_LANE: &str = "shared";

/// One issue's lane, by primary key.
pub(crate) const LANE_OF_ISSUE_SQL: &str = concat!(
    "SELECT ",
    cairn_db::queue_lane_sql!(),
    " FROM issues i WHERE i.id = ?1"
);

/// One lane's stamp, by primary key.
pub(crate) const LANE_TURN_SQL: &str = "SELECT turn FROM queue_lane_turns WHERE lane = ?1";

/// The newest stamp in this database. Ordered down to it rather than read as
/// `MAX(turn)`, which Turso answers by walking the whole index.
pub(crate) const NEWEST_TURN_SQL: &str =
    "SELECT turn FROM queue_lane_turns ORDER BY turn DESC LIMIT 1";

/// [`record_turn_conn`]'s upsert: an update by primary key, then an insert
/// when no row changed.
pub(crate) const UPDATE_TURN_SQL: &str =
    "UPDATE queue_lane_turns SET turn = ?2, execution_id = ?3, served_at = ?4 WHERE lane = ?1";
pub(crate) const INSERT_TURN_SQL: &str =
    "INSERT INTO queue_lane_turns (lane, turn, execution_id, served_at) VALUES (?1, ?2, ?3, ?4)";

/// What the order knows about one lane. A struct rather than a bare stamp so a
/// per-lane weight can join it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LaneTurn {
    /// Its newest turn stamp, `None` when it has never started work.
    pub served: Option<i64>,
}

/// The lane turns a reader has seen, the newest stamp per lane across
/// databases.
pub(crate) type LaneTurns = HashMap<String, LaneTurn>;

/// Record that `lane` was served at `turn`, keeping the newer stamp.
pub(crate) fn note_turn(turns: &mut LaneTurns, lane: &str, turn: i64) {
    let entry = turns.entry(lane.to_string()).or_default();
    entry.served = Some(entry.served.map_or(turn, |served| served.max(turn)));
}

/// The stamp a start at `now` takes: its second's, or one past `newest`.
pub(crate) fn mint_turn(now: i64, newest: i64) -> i64 {
    now.saturating_mul(QUEUE_KEY_SCALE)
        .max(newest.saturating_add(1))
}

/// One cap-held execution as the order sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Entry<'a> {
    pub execution_id: &'a str,
    pub db_index: usize,
    /// Its place within its lane: `fleet_order_key_sql!`.
    pub key: i64,
    pub lane: &'a str,
    /// In the ordered prefix ahead of the rotation.
    pub pinned: bool,
}

impl Entry<'_> {
    fn head(&self) -> (i64, usize, &str) {
        (self.key, self.db_index, self.execution_id)
    }
}

/// The queue's release order over `entries`, as indices into it: the pinned
/// prefix, then round-robin over the lanes (see the module doc). `turns` needs
/// only the lanes present; a lane it does not name has never been served.
pub(crate) fn release_order(entries: &[Entry<'_>], turns: &LaneTurns) -> Vec<usize> {
    let mut served: HashMap<&str, i64> = HashMap::new();
    for entry in entries {
        if let Some(stamp) = turns.get(entry.lane).and_then(|turn| turn.served) {
            served.insert(entry.lane, stamp);
        }
    }
    let mut order = Vec::with_capacity(entries.len());

    // The prefix: each pin consumes its lane's turn with a stamp past every
    // real one, in prefix order, exactly as its release will stamp it.
    let mut pinned: Vec<usize> = (0..entries.len())
        .filter(|index| entries[*index].pinned)
        .collect();
    pinned.sort_by_key(|index| entries[*index].head());
    let mut virtual_stamp = served.values().max().map_or(0, |newest| newest + 1);
    for index in pinned {
        served.insert(entries[index].lane, virtual_stamp);
        virtual_stamp = virtual_stamp.saturating_add(1);
        order.push(index);
    }

    let mut lanes: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, entry) in entries.iter().enumerate() {
        if !entry.pinned {
            lanes.entry(entry.lane).or_default().push(index);
        }
    }
    let mut ranked: Vec<Vec<usize>> = lanes
        .into_values()
        .map(|mut rows| {
            rows.sort_by_key(|index| entries[*index].head());
            rows
        })
        .collect();
    // Never served first, then least recently served, then by head.
    ranked.sort_by_key(|rows| {
        let head = &entries[rows[0]];
        let stamp = served.get(head.lane).copied();
        (stamp.is_some(), stamp.unwrap_or(0), head.head())
    });
    let rounds = ranked.iter().map(Vec::len).max().unwrap_or(0);
    for round in 0..rounds {
        order.extend(ranked.iter().filter_map(|rows| rows.get(round).copied()));
    }
    order
}

/// Issue `issue_id`'s lane, or [`SHARED_LANE`] for an issue-less execution.
pub(crate) async fn lane_of_issue_conn(
    conn: &Connection,
    issue_id: Option<&str>,
) -> DbResult<String> {
    let Some(issue_id) = issue_id else {
        return Ok(SHARED_LANE.to_string());
    };
    Ok(
        crate::storage::query_opt_text_conn(conn, LANE_OF_ISSUE_SQL, params![issue_id])
            .await?
            .unwrap_or_else(|| SHARED_LANE.to_string()),
    )
}

/// Record that execution `execution_id`, on issue `issue_id`, started at
/// `served_at` and so spent its lane's turn, stamped `turn`. Inside the
/// start's own transaction, after the start is certain: a release that lost
/// its guarded update must record nothing.
pub(crate) async fn record_turn_conn(
    conn: &Connection,
    issue_id: Option<&str>,
    execution_id: &str,
    turn: i64,
    served_at: i64,
) -> DbResult<()> {
    let lane = lane_of_issue_conn(conn, issue_id).await?;
    let updated = conn
        .execute(
            UPDATE_TURN_SQL,
            params![lane.as_str(), turn, execution_id, served_at],
        )
        .await?;
    if updated == 0 {
        conn.execute(
            INSERT_TURN_SQL,
            params![lane.as_str(), turn, execution_id, served_at],
        )
        .await?;
    }
    Ok(())
}

/// The stamps this database holds for `lanes`, one primary-key search each,
/// merged into `turns`.
pub(crate) async fn read_lane_turns_conn<'a>(
    conn: &Connection,
    lanes: impl IntoIterator<Item = &'a str>,
    turns: &mut LaneTurns,
) -> DbResult<()> {
    for lane in lanes {
        if let Some(turn) =
            crate::storage::query_opt_i64_conn(conn, LANE_TURN_SQL, params![lane]).await?
        {
            note_turn(turns, lane, turn);
        }
    }
    Ok(())
}

/// The newest stamp this database holds.
pub(crate) async fn newest_turn_conn(conn: &Connection) -> DbResult<Option<i64>> {
    crate::storage::query_opt_i64_conn(conn, NEWEST_TURN_SQL, ()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RowExt;

    fn entry<'a>(id: &'a str, lane: &'a str, key: i64) -> Entry<'a> {
        Entry {
            execution_id: id,
            db_index: 0,
            key,
            lane,
            pinned: false,
        }
    }

    fn ids<'a>(entries: &[Entry<'a>], turns: &LaneTurns) -> Vec<&'a str> {
        release_order(entries, turns)
            .into_iter()
            .map(|index| entries[index].execution_id)
            .collect()
    }

    fn served(pairs: &[(&str, i64)]) -> LaneTurns {
        let mut turns = LaneTurns::new();
        for (lane, stamp) in pairs {
            note_turn(&mut turns, lane, *stamp);
        }
        turns
    }

    /// Lanes of ten, three and one, none served, heads A < B < C.
    fn ten_three_one() -> Vec<(String, &'static str, i64)> {
        let mut rows = Vec::new();
        for n in 1..=10 {
            rows.push((format!("A{n}"), "A", n));
        }
        for n in 1..=3 {
            rows.push((format!("B{n}"), "B", 100 + n));
        }
        rows.push(("C1".to_string(), "C", 200));
        rows
    }

    #[test]
    fn ten_three_and_one_interleave_one_turn_per_lane_per_round() {
        let rows = ten_three_one();
        let entries: Vec<Entry> = rows
            .iter()
            .map(|(id, lane, key)| entry(id, lane, *key))
            .collect();
        assert_eq!(
            ids(&entries, &LaneTurns::new()),
            vec![
                "A1", "B1", "C1", "A2", "B2", "A3", "B3", "A4", "A5", "A6", "A7", "A8", "A9", "A10"
            ]
        );
    }

    #[test]
    fn the_least_recently_served_lane_goes_first() {
        let entries = [entry("a", "A", 1), entry("b", "B", 2), entry("c", "C", 3)];
        // C never served; B served before A.
        let turns = served(&[("A", 50), ("B", 40)]);
        assert_eq!(ids(&entries, &turns), vec!["c", "b", "a"]);
    }

    #[test]
    fn lanes_never_served_tie_by_their_head() {
        let entries = [
            entry("b2", "B", 5),
            entry("a1", "A", 3),
            entry("b1", "B", 2),
            entry("a2", "A", 9),
        ];
        assert_eq!(
            ids(&entries, &LaneTurns::new()),
            vec!["b1", "a1", "b2", "a2"]
        );
    }

    #[test]
    fn a_pin_leads_and_consumes_its_lanes_turn() {
        let mut entries = vec![
            entry("a1", "A", 1),
            entry("a2", "A", 2),
            entry("b1", "B", 3),
            entry("c1", "C", 4),
            entry("c2", "C", 5),
        ];
        // c2 pinned: it leads, and C has spent its turn, so A and B (never
        // served) go before C's next.
        entries[4].pinned = true;
        assert_eq!(
            ids(&entries, &LaneTurns::new()),
            vec!["c2", "a1", "b1", "c1", "a2"]
        );
        // Pins take stamps past every real one, in prefix order.
        entries[0].pinned = true;
        let turns = served(&[("A", 10), ("B", 20), ("C", 30)]);
        assert_eq!(ids(&entries, &turns), vec!["a1", "c2", "b1", "a2", "c1"]);
    }

    #[test]
    fn databases_break_key_ties_then_ids() {
        let entries = [
            Entry {
                db_index: 1,
                ..entry("x", "A", 7)
            },
            Entry {
                db_index: 0,
                ..entry("z", "A", 7)
            },
            Entry {
                db_index: 0,
                ..entry("y", "A", 7)
            },
            Entry {
                db_index: 1,
                ..entry("w", "B", 7)
            },
        ];
        // Lane A's head is (7, db 0, y), ahead of B's (7, db 1, w).
        assert_eq!(ids(&entries, &LaneTurns::new()), vec!["y", "w", "z", "x"]);
    }

    #[test]
    fn a_turn_is_its_second_or_one_past_the_newest() {
        assert_eq!(mint_turn(5, 0), 5 * QUEUE_KEY_SCALE);
        assert_eq!(mint_turn(5, 5 * QUEUE_KEY_SCALE), 5 * QUEUE_KEY_SCALE + 1);
        assert_eq!(mint_turn(6, 5 * QUEUE_KEY_SCALE + 9), 6 * QUEUE_KEY_SCALE);
    }

    async fn plan(db: &crate::storage::LocalDb, sql: &str, bound: usize) -> Vec<String> {
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        db.read(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                let values = vec![cairn_db::turso::Value::Integer(1); bound];
                let mut rows = conn.query(&sql, values).await?;
                let mut plan = Vec::new();
                while let Some(row) = rows.next().await? {
                    plan.push(row.text(3)?);
                }
                Ok(plan)
            })
        })
        .await
        .unwrap()
    }

    /// The lane-turn reads and the upsert touch `queue_lane_turns` (and the
    /// lane read `issues`) by key, and never `executions`.
    #[tokio::test]
    async fn the_turn_statements_seek_and_never_name_executions() {
        let db = crate::storage::migrated_test_db("rotation-turn-plans.db").await;
        let lane = plan(&db, LANE_TURN_SQL, 1).await;
        assert!(
            lane.iter().any(
                |line| line.starts_with("SEARCH queue_lane_turns") && line.contains("(lane=?)")
            ),
            "a primary-key search: {lane:#?}"
        );
        let newest = plan(&db, NEWEST_TURN_SQL, 0).await;
        assert!(
            newest
                .iter()
                .any(|line| line.contains("idx_queue_lane_turns_turn")),
            "through the turn index: {newest:#?}"
        );
        let issue = plan(&db, LANE_OF_ISSUE_SQL, 1).await;
        assert!(
            issue
                .iter()
                .any(|line| line.starts_with("SEARCH i ") && line.contains("=?")),
            "the issue by key: {issue:#?}"
        );
        let update = plan(&db, UPDATE_TURN_SQL, 4).await;
        for (label, steps) in [
            ("lane turn", &lane),
            ("newest turn", &newest),
            ("lane of issue", &issue),
            ("upsert", &update),
        ] {
            for forbidden in ["executions", "SORTER", "SCAN i"] {
                assert!(
                    !steps.iter().any(|line| line.contains(forbidden)),
                    "{label} names `{forbidden}`: {steps:#?}"
                );
            }
            // The newest stamp walks the turn index down and stops at its
            // first entry; nothing reads the table itself.
            assert!(
                !steps
                    .iter()
                    .any(|line| line.starts_with("SCAN queue_lane_turns")
                        && !line.contains("USING COVERING INDEX idx_queue_lane_turns_turn")),
                "{label} scans the table: {steps:#?}"
            );
        }
        for sql in [
            LANE_TURN_SQL,
            NEWEST_TURN_SQL,
            UPDATE_TURN_SQL,
            INSERT_TURN_SQL,
            LANE_OF_ISSUE_SQL,
        ] {
            assert!(!sql.contains("executions"), "{sql}");
        }
    }

    /// The upsert keeps one row per lane, carrying the newest stamp.
    #[tokio::test]
    async fn recording_a_turn_upserts_the_lanes_one_row() {
        let db = crate::storage::migrated_test_db("rotation-upsert.db").await;
        db.write(|conn| {
            Box::pin(async move {
                record_turn_conn(conn, None, "e1", 10, 1).await?;
                record_turn_conn(conn, None, "e2", 11, 2).await?;
                let mut turns = LaneTurns::new();
                read_lane_turns_conn(conn, [SHARED_LANE, "thread:none"], &mut turns).await?;
                assert_eq!(turns.len(), 1);
                assert_eq!(turns[SHARED_LANE].served, Some(11));
                assert_eq!(newest_turn_conn(conn).await?, Some(11));
                Ok(())
            })
        })
        .await
        .unwrap();
    }
}
