//! A thread's own order for its cap-held work (cairn/5848).
//!
//! A thread may say in which order its held children start. It never moves
//! ahead of another thread's work: the executions it lists take the places its
//! cap-held set ALREADY occupies, in the fleet order and in the thread order,
//! by permuting the `queue_key`s of those rows among themselves. Every other
//! row keeps its key, so every other thread's place is unchanged. No cap is
//! skipped either, because a cap is a count and keys change no count: only the
//! operator's Start now skips the queue.
//!
//! Each cap's rows are rotated in two classes, never across them. BLOCKING
//! rows (`no_evidence`, unlatched) are the places the set occupies now.
//! DORMANT rows are ones whose cap cleared while another cap still holds the
//! execution (`satisfied`, unlatched): no one counts them ahead, but each
//! wakes on its own key the next time its cap blocks, so they are rotated
//! among themselves too, or a thread sitting at its focus cap while the fleet
//! drains would lose its order the moment the fleet fills again. Handing a
//! dormant key to a blocking row (or the reverse) would change who is counted
//! ahead of other threads, so the classes never mix.
//!
//! The fleet and thread orders are permuted independently, so an execution
//! held on one cap and one held only on the other compare by whichever key
//! each has; their caps decide them separately, so no release depends on it.
//!
//! ## No admission lock
//!
//! A reorder does not take `Orchestrator::admission_lock`. Caps are counts the
//! keys cannot change, so a reorder can never let a start exceed a cap, and
//! exactly-once release still rests on `release_conn`'s guarded `released_at`,
//! which this never touches. The one interleaving is a reorder committing
//! between a release's decision and its write: that release then goes out in
//! the order from just before the reorder, which is the same as the release
//! having happened just before it. A filing that asks for the head of the line
//! rotates inside its own insert transaction, which already holds the lock;
//! the rotation adds statements to that transaction, not an acquisition.

use std::collections::{HashMap, HashSet};

use cairn_db::turso::{params, Connection};
use cairn_workflow::gates::store::{set_gate_detail_conn, set_gate_queue_key_conn};

use super::capacity::{capacity_fact, decode_hold, encode_hold, CAPACITY_FACTS};
use crate::models::CapacityHold;
use crate::storage::{DbResult, RowExt};

/// One unlatched capacity row of a queued execution: a place it occupies
/// (blocking) or one it will wake on (dormant).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueuedHold {
    pub fact: &'static str,
    pub gate_id: String,
    pub queue_key: i64,
    /// The row's facts, `None` when the queue has not written them yet.
    pub hold: Option<CapacityHold>,
}

/// One cap-held execution of a thread's children, as its queue lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThreadQueued {
    pub execution_id: String,
    pub issue_number: i64,
    pub issue_title: String,
    /// The age held, for display. Never rewritten by a reorder.
    pub held_since: i64,
    /// Its blocking capacity rows, the fleet's first.
    pub holds: Vec<QueuedHold>,
    /// Its dormant capacity rows: caps that cleared while another still holds
    /// it.
    pub dormant: Vec<QueuedHold>,
}

impl ThreadQueued {
    pub fn hold(&self, fact: &str) -> Option<&QueuedHold> {
        self.holds.iter().find(|row| row.fact == fact)
    }

    /// Its place on `fact`'s cap: `None` when that cap does not hold it,
    /// `Some(None)` when it holds it but the queue has not numbered it yet.
    pub fn place(&self, fact: &str) -> Option<Option<usize>> {
        self.hold(fact)
            .map(|row| row.hold.as_ref().and_then(CapacityHold::place))
    }

    /// The thread whose reorder moved it ahead, if one did.
    pub fn placed_by(&self) -> Option<&str> {
        self.holds
            .iter()
            .find_map(|row| row.hold.as_ref().and_then(CapacityHold::placed_by))
    }
}

/// The thread's cap-held children's executions, in queue order: exactly the
/// set the queue page lists, narrowed to this thread.
const THREAD_QUEUE_SQL: &str = concat!(
    "SELECT e.id, i.number, i.title, ",
    cairn_workflow::held_since_sql!(),
    ", ",
    cairn_workflow::queue_key_sql!(),
    "
       FROM executions e JOIN issues i ON i.id = e.issue_id
      WHERE i.parent_thread_id = ?1 AND ",
    super::capacity::cap_held_sql!(),
    " ORDER BY 5, e.id"
);

/// Every unlatched capacity row of the thread's children, blocking or dormant.
const THREAD_HOLDS_SQL: &str = concat!(
    "SELECT g.execution_id, g.id, g.predicate_json,
            COALESCE(g.queue_key, g.created_at * 1000000), g.detail,
            g.last_outcome = 'no_evidence'
       FROM execution_gates g
       JOIN executions e ON e.id = g.execution_id
       JOIN issues i ON i.id = e.issue_id
      WHERE i.parent_thread_id = ?1
        AND g.satisfied_at IS NULL AND ",
    cairn_db::capacity_gate_sql!(),
    " ORDER BY g.seq, g.id"
);

/// The thread's cap-held executions in queue order, each with its blocking
/// and dormant capacity rows.
pub(crate) async fn thread_queue_conn(
    conn: &Connection,
    thread_id: &str,
) -> DbResult<Vec<ThreadQueued>> {
    let mut rows = conn.query(THREAD_QUEUE_SQL, params![thread_id]).await?;
    let mut queue = Vec::new();
    while let Some(row) = rows.next().await? {
        queue.push(ThreadQueued {
            execution_id: row.text(0)?,
            issue_number: row.i64(1)?,
            issue_title: row.text(2)?,
            held_since: row.opt_i64(3)?.unwrap_or(0),
            holds: Vec::new(),
            dormant: Vec::new(),
        });
    }
    drop(rows);
    let mut holds: HashMap<String, Vec<QueuedHold>> = HashMap::new();
    let mut dormant: HashMap<String, Vec<QueuedHold>> = HashMap::new();
    let mut rows = conn.query(THREAD_HOLDS_SQL, params![thread_id]).await?;
    while let Some(row) = rows.next().await? {
        let predicate = row.text(2)?;
        let Some(fact) = capacity_fact(&predicate) else {
            continue;
        };
        let detail = row.opt_text(4)?;
        let class = if row.opt_i64(5)?.unwrap_or(0) != 0 {
            &mut holds
        } else {
            &mut dormant
        };
        class.entry(row.text(0)?).or_default().push(QueuedHold {
            fact,
            gate_id: row.text(1)?,
            queue_key: row.i64(3)?,
            hold: detail
                .as_deref()
                .and_then(|detail| decode_hold(fact, detail)),
        });
    }
    for queued in &mut queue {
        queued.holds = holds.remove(&queued.execution_id).unwrap_or_default();
        queued.dormant = dormant.remove(&queued.execution_id).unwrap_or_default();
    }
    Ok(queue)
}

/// What a rotation did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Rotation {
    /// Executions whose rows it rewrote, each owing a db-change.
    pub moved: Vec<String>,
    /// The thread's queue as it reads afterwards.
    pub queue: Vec<ThreadQueued>,
}

/// Put `front` (execution ids of the thread's cap-held set, already
/// validated) at the head of the thread's set, in that order, the rest behind
/// them in their current order. On each cap separately, and within each class
/// (blocking, dormant) separately, the i-th execution of that order takes the
/// i-th smallest key the set's rows of that class hold; a blocking row also
/// takes, while numbered places remain, the i-th smallest place. A row whose
/// key went down records `placed_by`; one whose key went up forgets it. A
/// blocking row left without a place is numbered by the next sweep. Runs
/// inside the caller's write transaction.
pub(crate) async fn rotate_to_front_conn(
    conn: &Connection,
    thread_id: &str,
    front: &[String],
    placed_by: &str,
) -> DbResult<Rotation> {
    let set = thread_queue_conn(conn, thread_id).await?;
    let mut desired: Vec<&ThreadQueued> = front
        .iter()
        .filter_map(|id| set.iter().find(|queued| &queued.execution_id == id))
        .collect();
    desired.extend(
        set.iter()
            .filter(|queued| !front.contains(&queued.execution_id)),
    );

    let mut moved = HashSet::new();
    for (fact, blocking) in CAPACITY_FACTS
        .iter()
        .flat_map(|fact| [(*fact, true), (*fact, false)])
    {
        let members: Vec<(&str, &QueuedHold)> = desired
            .iter()
            .filter_map(|queued| {
                let class = if blocking {
                    &queued.holds
                } else {
                    &queued.dormant
                };
                class
                    .iter()
                    .find(|row| row.fact == fact)
                    .map(|row| (queued.execution_id.as_str(), row))
            })
            .collect();
        let mut keys: Vec<i64> = members.iter().map(|(_, row)| row.queue_key).collect();
        keys.sort_unstable();
        let mut places: Vec<usize> = members
            .iter()
            .filter_map(|(_, row)| row.hold.as_ref().and_then(CapacityHold::place))
            .collect();
        places.sort_unstable();
        for (index, (execution_id, row)) in members.iter().enumerate() {
            let key = keys[index];
            if key != row.queue_key {
                set_gate_queue_key_conn(conn, &row.gate_id, key).await?;
                moved.insert(execution_id.to_string());
            }
            let Some(hold) = &row.hold else {
                continue;
            };
            let mut next = hold.clone();
            // A dormant row's place is renumbered when its cap blocks again.
            if blocking {
                next.set_place(places.get(index).copied());
            }
            match key.cmp(&row.queue_key) {
                std::cmp::Ordering::Less => next.set_placed_by(Some(placed_by.to_string())),
                std::cmp::Ordering::Greater => next.set_placed_by(None),
                std::cmp::Ordering::Equal => {}
            }
            if &next != hold {
                set_gate_detail_conn(conn, &row.gate_id, &encode_hold(&next)).await?;
                moved.insert(execution_id.to_string());
            }
        }
    }
    let mut moved: Vec<String> = moved.into_iter().collect();
    moved.sort();
    Ok(Rotation {
        moved,
        queue: thread_queue_conn(conn, thread_id).await?,
    })
}

/// Why an issue a thread listed has no cap-held execution to order.
const LATEST_EXECUTION_SQL: &str =
    "SELECT status FROM executions WHERE issue_id = ?1 ORDER BY seq DESC LIMIT 1";

const AWAITS_AUTHORED_SQL: &str = concat!(
    "SELECT 1 FROM executions e WHERE e.issue_id = ?1 AND ",
    cairn_db::awaiting_release_sql!(),
    " AND ",
    cairn_db::awaits_authored_gate_sql!(),
    " LIMIT 1"
);

async fn not_queued_reason(conn: &Connection, issue_id: &str) -> DbResult<String> {
    let waits = crate::storage::query_opt_i64_conn(conn, AWAITS_AUTHORED_SQL, params![issue_id])
        .await?
        .is_some();
    if waits {
        return Ok("waits on its after: gates".to_string());
    }
    Ok(
        match crate::storage::query_opt_text_conn(conn, LATEST_EXECUTION_SQL, params![issue_id])
            .await?
            .as_deref()
        {
            None => "no execution".to_string(),
            Some("running") => "running".to_string(),
            Some(status) => format!("its latest execution is {status}"),
        },
    )
}

/// The execution ids an `order` list puts at the head of a thread's queue, in
/// order, or one line per offending entry. Each number expands to every
/// cap-held execution of that child, in their current relative order.
pub(crate) async fn resolve_order_conn(
    conn: &Connection,
    thread_id: &str,
    thread_name: &str,
    order: &[serde_json::Value],
    set: &[ThreadQueued],
) -> DbResult<Result<Vec<String>, Vec<String>>> {
    let mut refusals = Vec::new();
    if order.is_empty() {
        refusals.push(format!(
            "`order` is empty: list {thread_name}'s children by issue number, first to start first"
        ));
    }
    let project_id = crate::storage::query_opt_text_conn(
        conn,
        "SELECT project_id FROM threads WHERE id = ?1",
        params![thread_id],
    )
    .await?
    .unwrap_or_default();
    let mut seen = HashSet::new();
    let mut front = Vec::new();
    for entry in order {
        let Some(number) = entry.as_i64() else {
            refusals.push(format!("{entry} is not an issue number"));
            continue;
        };
        if !seen.insert(number) {
            refusals.push(format!("{number} is listed twice"));
            continue;
        }
        let issue_id = crate::storage::query_opt_text_conn(
            conn,
            "SELECT id FROM issues WHERE project_id = ?1 AND number = ?2 AND parent_thread_id = ?3",
            params![project_id.as_str(), number, thread_id],
        )
        .await?;
        let Some(issue_id) = issue_id else {
            refusals.push(format!("{number} is not a child of {thread_name}"));
            continue;
        };
        let queued: Vec<&ThreadQueued> = set
            .iter()
            .filter(|queued| queued.issue_number == number)
            .collect();
        if queued.is_empty() {
            let why = not_queued_reason(conn, &issue_id).await?;
            refusals.push(format!("{number} has no cap-held execution ({why})"));
            continue;
        }
        front.extend(queued.iter().map(|queued| queued.execution_id.clone()));
    }
    Ok(if refusals.is_empty() {
        Ok(front)
    } else {
        Err(refusals)
    })
}

/// A thread's `order` patch, validated and applied in the caller's ONE write
/// transaction: every offending entry, with nothing written, or the rotation.
pub(crate) async fn order_thread_queue_conn(
    conn: &Connection,
    thread_id: &str,
    thread_name: &str,
    order: &[serde_json::Value],
    placed_by: &str,
) -> DbResult<Result<Rotation, Vec<String>>> {
    let set = thread_queue_conn(conn, thread_id).await?;
    let front = match resolve_order_conn(conn, thread_id, thread_name, order, &set).await? {
        Ok(front) => front,
        Err(refusals) => return Ok(Err(refusals)),
    };
    Ok(Ok(
        rotate_to_front_conn(conn, thread_id, &front, placed_by).await?
    ))
}

/// `fleet 3rd · thread 1st`: an execution's place on each cap holding it.
pub(crate) fn places_phrase(queued: &ThreadQueued, separator: &str) -> String {
    let mut parts = Vec::new();
    for (fact, label) in [
        (super::capacity::FLEET_SLOT_FACT, "fleet"),
        (super::capacity::THREAD_FOCUS_FACT, "thread"),
    ] {
        if let Some(place) = queued.place(fact) {
            parts.push(format!(
                "{label} {}",
                place
                    .map(super::census::ordinal)
                    .unwrap_or_else(|| "unnumbered".to_string())
            ));
        }
    }
    parts.join(separator)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::execution::gates::capacity::tests::{
        add_issue, add_started, add_thread, caps, detail, full_fleet, is_held,
    };
    use crate::execution::gates::capacity::{queue_view, FLEET_SLOT_FACT, THREAD_FOCUS_FACT};
    use crate::execution::gates::sweep::tests::{count, env, execution, file, now, Env};
    use crate::execution::gates::sweep::{evaluate_execution, sweep, sweep_at};
    use crate::mcp::types::{ChangeItem, ChangeMode, McpCallbackRequest};
    use crate::storage::LocalDb;
    use serde_json::{json, Value};

    const FLEET: &str = FLEET_SLOT_FACT;
    const THREAD: &str = THREAD_FOCUS_FACT;

    /// Every capacity row: (execution, fact) -> (gate id, queue key, detail,
    /// created_at).
    type Rows = BTreeMap<(String, String), (String, i64, Option<String>, i64)>;

    async fn rows(env: &Env) -> Rows {
        env.db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(
                            "SELECT execution_id, json_extract(predicate_json, '$.fact'), id,
                                    queue_key, detail, created_at
                               FROM execution_gates
                              WHERE json_extract(predicate_json, '$.fact')
                                    IN ('fleet_slot', 'thread_focus')",
                            (),
                        )
                        .await?;
                    let mut out = BTreeMap::new();
                    while let Some(row) = rows.next().await? {
                        out.insert(
                            (row.text(0)?, row.text(1)?),
                            (row.text(2)?, row.i64(3)?, row.opt_text(4)?, row.i64(5)?),
                        );
                    }
                    Ok(out)
                })
            })
            .await
            .unwrap()
    }

    fn row<'a>(rows: &'a Rows, id: &str, fact: &str) -> &'a (String, i64, Option<String>, i64) {
        &rows[&(id.to_string(), fact.to_string())]
    }

    fn key(rows: &Rows, id: &str, fact: &str) -> i64 {
        row(rows, id, fact).1
    }

    fn hold(rows: &Rows, id: &str, fact: &str) -> CapacityHold {
        decode_hold(fact, row(rows, id, fact).2.as_deref().unwrap()).unwrap()
    }

    fn place(rows: &Rows, id: &str, fact: &str) -> Option<usize> {
        hold(rows, id, fact).place()
    }

    fn placed_by(rows: &Rows, id: &str, fact: &str) -> Option<String> {
        hold(rows, id, fact).placed_by().map(str::to_string)
    }

    async fn queue_ids(env: &Env) -> Vec<String> {
        env.db
            .read(|conn| Box::pin(async move { thread_queue_conn(conn, "ta").await }))
            .await
            .unwrap()
            .into_iter()
            .map(|queued| queued.execution_id)
            .collect()
    }

    /// `order` applied straight through the one write transaction.
    async fn order(env: &Env, entries: Value, by: &str) -> Result<Rotation, Vec<String>> {
        let entries = entries.as_array().unwrap().clone();
        let by = by.to_string();
        env.db
            .write(move |conn| {
                let (entries, by) = (entries.clone(), by.clone());
                Box::pin(async move {
                    order_thread_queue_conn(conn, "ta", "alpha", &entries, &by).await
                })
            })
            .await
            .unwrap()
    }

    struct World {
        env: Env,
        a1: String,
        a2: String,
        a3: String,
        b1: String,
    }

    /// A full fleet. Thread alpha holds a1 < a2 < a3 (issues 201-203), and
    /// beta's b1 (issue 301) is held on the fleet between a1 and a2. With
    /// `thread_capped`, alpha's own focus cap (one open child, 150) holds its
    /// three as well. The sweep has numbered every place.
    async fn world(name: &str, thread_capped: bool) -> World {
        let env = env(name).await;
        caps(&env, 6, if thread_capped { 1 } else { 50 });
        full_fleet(&env).await;
        add_thread(&env, "ta", "alpha").await;
        add_thread(&env, "tb", "beta").await;
        if thread_capped {
            add_started(&env, 150, Some("ta"), "running").await;
        }
        for number in [201, 202, 203, 204] {
            add_issue(&env, number, Some("ta")).await;
        }
        add_issue(&env, 301, Some("tb")).await;
        for number in [201, 301, 202, 203] {
            file(&env, number, json!([])).await.unwrap();
            assert!(is_held(&env, number).await, "{number} holds");
        }
        sweep_at(&env.orch, now()).await.unwrap();
        let a1 = execution(&env, 201).await.0;
        let a2 = execution(&env, 202).await.0;
        let a3 = execution(&env, 203).await.0;
        let b1 = execution(&env, 301).await.0;
        World {
            env,
            a1,
            a2,
            a3,
            b1,
        }
    }

    /// A thread job (or, with `parent`, a task under one) and its live run.
    async fn thread_run(env: &Env, run: &str, thread: Option<&str>, parent: Option<&str>) {
        let job = format!("job-{run}");
        let thread = thread
            .map(|thread| format!("'{thread}'"))
            .unwrap_or_else(|| "NULL".to_string());
        // A thread's session job is the one with no parent and segment
        // `thread`; a task hangs beneath it.
        let segment = if parent.is_some() { "task-1" } else { "thread" };
        let parent = parent
            .map(|parent| format!("'{parent}'"))
            .unwrap_or_else(|| "NULL".to_string());
        env.db
            .execute_script(format!(
                "INSERT INTO jobs(id, project_id, node_name, status, thread_id, parent_job_id,
                                  uri_segment, created_at, updated_at)
                   VALUES('{job}', 'p', 'session', 'running', {thread}, {parent}, '{segment}',
                          1, 1);
                 INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at)
                   VALUES('{run}', 'p', '{job}', 'live', 1, 1);"
            ))
            .await
            .unwrap();
    }

    fn request(run: Option<&str>) -> McpCallbackRequest {
        McpCallbackRequest {
            answer_bound_ms: None,
            cwd: "/".to_string(),
            run_id: run.map(str::to_string),
            tool: "write".to_string(),
            payload: Value::Null,
            tool_use_id: None,
            thread_id: None,
            origin: None,
        }
    }

    async fn write(
        env: &Env,
        run: Option<&str>,
        target: &str,
        mode: ChangeMode,
        payload: Value,
    ) -> Result<String, String> {
        let item = ChangeItem {
            target: target.to_string(),
            mode,
            payload: Some(payload),
        };
        crate::resources::mutations::dispatch_resource_change(
            &env.orch,
            &request(run),
            0,
            &item,
            false,
        )
        .await
        .map(|change| change.summary)
        .map_err(|failure| failure.error)
    }

    async fn patch_queue(env: &Env, run: Option<&str>, entries: Value) -> Result<String, String> {
        write(
            env,
            run,
            "cairn://p/prj/alpha/queue",
            ChangeMode::Patch,
            json!({ "order": entries }),
        )
        .await
    }

    async fn file_at_head(
        env: &Env,
        run: Option<&str>,
        number: i32,
        extra: Value,
    ) -> Result<String, String> {
        let mut payload = json!({ "recipe": "memory-triage", "place": "head" });
        if let (Some(payload), Some(extra)) = (payload.as_object_mut(), extra.as_object()) {
            payload.extend(extra.clone());
        }
        write(
            env,
            run,
            &format!("cairn://p/prj/{number}/executions"),
            ChangeMode::Append,
            payload,
        )
        .await
    }

    // -- the rotation ---------------------------------------------------------

    /// The acceptance case: alpha holds a1 < a2 < a3 with beta's b1 between
    /// them on the fleet. `order:[a3, a1]` hands a3 a1's places, a1 a2's and
    /// a2 a3's, in both orders; b1 keeps its key and its place, before and
    /// after the next sweep renumbers; and no age held moves.
    #[tokio::test]
    async fn an_order_takes_the_threads_own_places_and_moves_no_one_else() {
        let w = world("reorder-interleaved.db", true).await;
        let before = rows(&w.env).await;
        assert_eq!(
            [&w.a1, &w.b1, &w.a2, &w.a3].map(|id| place(&before, id, FLEET)),
            [Some(1), Some(2), Some(3), Some(4)]
        );
        assert_eq!(
            [&w.a1, &w.a2, &w.a3].map(|id| place(&before, id, THREAD)),
            [Some(1), Some(2), Some(3)]
        );
        let ages_before: Vec<(String, i64)> = queue_view(&w.env.orch)
            .await
            .unwrap()
            .held
            .into_iter()
            .map(|held| (held.execution_id, held.held_since))
            .collect();

        let rotation = order(&w.env, json!([203, 201]), "alpha").await.unwrap();
        assert_eq!(
            rotation
                .queue
                .iter()
                .map(|queued| queued.issue_number)
                .collect::<Vec<_>>(),
            vec![203, 201, 202]
        );
        let after = rows(&w.env).await;
        for fact in [FLEET, THREAD] {
            assert_eq!(key(&after, &w.a3, fact), key(&before, &w.a1, fact));
            assert_eq!(key(&after, &w.a1, fact), key(&before, &w.a2, fact));
            assert_eq!(key(&after, &w.a2, fact), key(&before, &w.a3, fact));
            assert_eq!(place(&after, &w.a3, fact), place(&before, &w.a1, fact));
            assert_eq!(place(&after, &w.a1, fact), place(&before, &w.a2, fact));
            assert_eq!(place(&after, &w.a2, fact), place(&before, &w.a3, fact));
            assert_eq!(placed_by(&after, &w.a3, fact).as_deref(), Some("alpha"));
            assert_eq!(placed_by(&after, &w.a1, fact), None, "pushed back");
            assert_eq!(placed_by(&after, &w.a2, fact), None, "pushed back");
        }
        assert_eq!(
            row(&after, &w.b1, FLEET),
            row(&before, &w.b1, FLEET),
            "beta's row is untouched"
        );
        for (id, (_, _, _, created_at)) in &before {
            assert_eq!(after[id].3, *created_at, "the age held of {id:?} moved");
        }

        sweep_at(&w.env.orch, now()).await.unwrap();
        let swept = rows(&w.env).await;
        assert_eq!(place(&swept, &w.b1, FLEET), Some(2), "beta is still 2nd");
        for id in [&w.a1, &w.a2, &w.a3] {
            for fact in [FLEET, THREAD] {
                assert_eq!(
                    place(&swept, id, fact),
                    place(&after, id, fact),
                    "the sweep numbers the places the rotation wrote"
                );
            }
        }
        let ages_after: Vec<(String, i64)> = queue_view(&w.env.orch)
            .await
            .unwrap()
            .held
            .into_iter()
            .map(|held| (held.execution_id, held.held_since))
            .collect();
        let mut sorted_before = ages_before.clone();
        let mut sorted_after = ages_after.clone();
        sorted_before.sort();
        sorted_after.sort();
        assert_eq!(sorted_after, sorted_before, "no age held moves");
        assert_eq!(
            ages_after
                .iter()
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>(),
            vec![w.a3.clone(), w.b1.clone(), w.a1.clone(), w.a2.clone()],
            "the queue page shows the new order"
        );

        assert!(detail(&w.env, &w.a3)
            .await
            .ends_with("placed ahead by alpha"));
        assert!(!detail(&w.env, &w.a1).await.contains("placed ahead"));
    }

    #[tokio::test]
    async fn unlisted_children_keep_their_relative_order_behind_the_listed() {
        let w = world("reorder-unlisted.db", false).await;
        file(&w.env, 204, json!([])).await.unwrap();
        let a4 = execution(&w.env, 204).await.0;
        order(&w.env, json!([203]), "alpha").await.unwrap();
        assert_eq!(
            queue_ids(&w.env).await,
            vec![w.a3.clone(), w.a1.clone(), w.a2.clone(), a4]
        );
    }

    /// Every offending entry is named, and a refused order writes nothing.
    #[tokio::test]
    async fn every_refusal_names_each_offending_entry_and_changes_nothing() {
        let w = world("reorder-refusals.db", true).await;
        add_issue(&w.env, 205, Some("ta")).await;
        file(
            &w.env,
            205,
            json!([{"fact": "issue_resolution", "issue": "cairn://p/prj/1", "resolution": "merged"}]),
        )
        .await
        .unwrap();
        add_issue(&w.env, 206, Some("ta")).await;
        let before = rows(&w.env).await;

        let cases: Vec<(Value, Vec<&str>)> = vec![
            (json!([999]), vec!["999 is not a child of alpha"]),
            (json!([301]), vec!["301 is not a child of alpha"]),
            (
                json!([205]),
                vec!["205 has no cap-held execution (waits on its after: gates)"],
            ),
            (
                json!([150]),
                vec!["150 has no cap-held execution (running)"],
            ),
            (
                json!([206]),
                vec!["206 has no cap-held execution (no execution)"],
            ),
            (json!([201, 201]), vec!["201 is listed twice"]),
            (json!(["x"]), vec!["\"x\" is not an issue number"]),
            (
                json!([]),
                vec![
                    "`order` is empty: list alpha's children by issue number, first to start first",
                ],
            ),
            (
                json!([203, 999, 301, 205, 150, 206, 201, 203, "x"]),
                vec![
                    "999 is not a child of alpha",
                    "301 is not a child of alpha",
                    "205 has no cap-held execution (waits on its after: gates)",
                    "150 has no cap-held execution (running)",
                    "206 has no cap-held execution (no execution)",
                    "203 is listed twice",
                    "\"x\" is not an issue number",
                ],
            ),
        ];
        for (entries, expected) in cases {
            let refused = order(&w.env, entries.clone(), "alpha").await.unwrap_err();
            assert_eq!(refused, expected, "{entries}");
            assert_eq!(rows(&w.env).await, before, "{entries} changed something");
        }

        // Through the resource: one bullet per entry, then Nothing changed.
        let refused = patch_queue(&w.env, None, json!([203, 999, 201, 201]))
            .await
            .unwrap_err();
        assert!(
            refused.contains(
                "alpha's queue was not ordered:\n- 999 is not a child of alpha\n- 201 is listed \
                 twice\nNothing changed."
            ),
            "{refused}"
        );
        assert_eq!(rows(&w.env).await, before);
    }

    // -- who may order --------------------------------------------------------

    #[tokio::test]
    async fn only_the_thread_its_tasks_or_the_operator_order_its_queue() {
        let w = world("reorder-authority.db", true).await;
        thread_run(&w.env, "run-alpha", Some("ta"), None).await;
        thread_run(&w.env, "run-alpha-task", None, Some("job-run-alpha")).await;
        thread_run(&w.env, "run-beta", Some("tb"), None).await;
        let before = rows(&w.env).await;

        let refused = patch_queue(&w.env, Some("run-beta"), json!([203]))
            .await
            .unwrap_err();
        assert!(
            refused.contains(
                "Only alpha (its sessions and their tasks) or the operator orders alpha's queue; \
                 this run acts for beta. Nothing changed."
            ),
            "{refused}"
        );
        assert_eq!(rows(&w.env).await, before);

        let ordered = patch_queue(&w.env, Some("run-alpha-task"), json!([203]))
            .await
            .unwrap();
        assert_eq!(
            ordered,
            "Ordered alpha's queue: 203 (fleet 1st, thread 1st); 2 more behind them in their \
             current order."
        );
        assert_eq!(
            placed_by(&rows(&w.env).await, &w.a3, FLEET).as_deref(),
            Some("alpha")
        );

        let ordered = patch_queue(&w.env, None, json!([202])).await.unwrap();
        assert!(
            ordered.starts_with("Ordered alpha's queue: 202 (fleet 1st, thread 1st)"),
            "{ordered}"
        );
        assert_eq!(
            placed_by(&rows(&w.env).await, &w.a2, FLEET).as_deref(),
            Some("the operator")
        );

        let read =
            crate::resources::read_thread_queue(&w.env.db, "prj", "alpha", now() + 720).await;
        let lines: Vec<&str> = read
            .lines()
            .filter(|line| line.starts_with(char::is_numeric))
            .collect();
        assert_eq!(lines.len(), 3, "{read}");
        assert!(
            lines[0].starts_with(
                "1. [202](cairn://p/prj/202) Issue 202 · fleet 1st · thread 1st · held "
            ) && lines[0].ends_with(" · placed ahead by the operator"),
            "{read}"
        );
        assert!(
            lines[1].starts_with("2. [203](cairn://p/prj/203) Issue 203 · fleet 3rd · thread 2nd"),
            "{read}"
        );
        assert!(
            lines[2].starts_with("3. [201](cairn://p/prj/201) Issue 201 · fleet 4th · thread 3rd"),
            "{read}"
        );

        let spec = cairn_common::contract::mutation_spec(
            cairn_common::contract::ResourceKind::ThreadQueue,
            ChangeMode::Patch,
        )
        .expect("the queue advertises its order patch");
        assert_eq!(
            spec.required.iter().map(|key| key.key).collect::<Vec<_>>(),
            vec!["order"]
        );
    }

    // -- place:"head" at filing ----------------------------------------------

    #[tokio::test]
    async fn a_head_filing_takes_the_threads_first_places_and_moves_no_one_else() {
        let w = world("reorder-head-filing.db", true).await;
        thread_run(&w.env, "run-alpha", Some("ta"), None).await;
        let before = rows(&w.env).await;

        let filed = file_at_head(&w.env, Some("run-alpha"), 204, json!({}))
            .await
            .unwrap();
        assert!(filed.contains(": Queued · 1st · "), "{filed}");
        let a4 = execution(&w.env, 204).await.0;
        assert_eq!(
            queue_ids(&w.env).await,
            vec![a4.clone(), w.a1.clone(), w.a2.clone(), w.a3.clone()]
        );
        let after = rows(&w.env).await;
        for fact in [FLEET, THREAD] {
            assert_eq!(key(&after, &a4, fact), key(&before, &w.a1, fact));
            assert_eq!(placed_by(&after, &a4, fact).as_deref(), Some("alpha"));
        }
        assert_eq!(row(&after, &w.b1, FLEET), row(&before, &w.b1, FLEET));

        // Issue create files straight to the head too, under its thread. The
        // fixture's issues were inserted by number, past the counter.
        w.env
            .db
            .execute(
                "UPDATE projects SET next_issue_number = 1000 WHERE id = 'p'",
                (),
            )
            .await
            .unwrap();
        let created = write(
            &w.env,
            Some("run-alpha"),
            "cairn://p/prj/issues",
            ChangeMode::Append,
            json!({
                "title": "Red on main",
                "parent": "cairn://p/prj/alpha",
                "execution": {"recipe": "memory-triage", "place": "head"},
            }),
        )
        .await
        .unwrap();
        assert!(created.contains(": Queued · 1st · "), "{created}");
        let red = env_newest_execution(&w.env).await;
        assert_eq!(queue_ids(&w.env).await[0], red);
    }

    async fn env_newest_execution(env: &Env) -> String {
        env.db
            .query_opt_text(
                "SELECT e.id FROM executions e JOIN issues i ON i.id = e.issue_id
                  WHERE i.title = 'Red on main'",
                (),
            )
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn a_head_filing_is_refused_before_anything_is_written() {
        let w = world("reorder-head-refusals.db", true).await;
        thread_run(&w.env, "run-alpha", Some("ta"), None).await;
        thread_run(&w.env, "run-beta", Some("tb"), None).await;
        let executions = || count(&w.env, "SELECT COUNT(*) FROM executions");
        let filed = executions().await;

        let with_after = file_at_head(
            &w.env,
            Some("run-alpha"),
            204,
            json!({"after": [{"fact": "issue_resolution", "issue": "cairn://p/prj/1", "resolution": "merged"}]}),
        )
        .await
        .unwrap_err();
        assert!(
            with_after.contains("place:\"head\" orders cap-held work; an after:-gated execution has no place until its gates clear"),
            "{with_after}"
        );

        let other_thread = file_at_head(&w.env, Some("run-beta"), 204, json!({}))
            .await
            .unwrap_err();
        assert!(
            other_thread.contains("Only alpha (its sessions and their tasks) or the operator"),
            "{other_thread}"
        );

        // Issue 2 is a child of an issue, not of a thread.
        let no_thread = file_at_head(&w.env, None, 2, json!({})).await.unwrap_err();
        assert!(no_thread.contains("has no parent thread"), "{no_thread}");

        let bad_place = write(
            &w.env,
            None,
            "cairn://p/prj/204/executions",
            ChangeMode::Append,
            json!({"recipe": "memory-triage", "place": "tail"}),
        )
        .await
        .unwrap_err();
        assert!(
            bad_place.contains("payload.place takes only \"head\""),
            "{bad_place}"
        );

        let issues = count(&w.env, "SELECT COUNT(*) FROM issues").await;
        let create_from_beta = write(
            &w.env,
            Some("run-beta"),
            "cairn://p/prj/issues",
            ChangeMode::Append,
            json!({
                "title": "Not beta's to place",
                "parent": "cairn://p/prj/alpha",
                "execution": {"recipe": "memory-triage", "place": "head"},
            }),
        )
        .await
        .unwrap_err();
        assert!(
            create_from_beta.contains("Only alpha"),
            "{create_from_beta}"
        );
        assert_eq!(
            count(&w.env, "SELECT COUNT(*) FROM issues").await,
            issues,
            "the refused create made no issue"
        );
        assert_eq!(executions().await, filed, "nothing was filed");
    }

    #[tokio::test]
    async fn a_head_filing_that_starts_at_once_has_nothing_to_place() {
        let env = env("reorder-head-starts.db").await;
        caps(&env, 50, 50);
        add_thread(&env, "ta", "alpha").await;
        add_issue(&env, 201, Some("ta")).await;
        let filed = file_at_head(&env, None, 201, json!({})).await.unwrap();
        assert!(filed.starts_with("Started execution"), "{filed}");
        assert!(
            filed.contains("It started now; nothing to place."),
            "{filed}"
        );
    }

    /// One write batch: a plain filing then a head filing, both held at a full
    /// fleet, must complete with the second at the head. The admission lock
    /// the first filing takes must be let go before the second asks for it
    /// (cairn/5844's batch deadlock).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn one_batch_of_a_filing_and_a_head_filing_completes_with_both_held() {
        let env = env("reorder-head-batch.db").await;
        full_fleet(&env).await;
        add_thread(&env, "ta", "alpha").await;
        add_issue(&env, 201, Some("ta")).await;
        add_issue(&env, 202, Some("ta")).await;
        thread_run(&env, "run-alpha", Some("ta"), None).await;
        let mut batch = request(Some("run-alpha"));
        batch.payload = json!({"changes": [
            {"target": "cairn://p/prj/201/executions", "mode": "append",
             "payload": {"recipe": "memory-triage"}},
            {"target": "cairn://p/prj/202/executions", "mode": "append",
             "payload": {"recipe": "memory-triage", "place": "head"}},
        ]});
        let report = tokio::time::timeout(
            std::time::Duration::from_secs(60),
            crate::mcp::handlers::write::handle_write(&env.orch, &batch),
        )
        .await
        .expect("the batch completes");
        assert!(
            is_held(&env, 201).await && is_held(&env, 202).await,
            "{report}"
        );
        assert_eq!(
            queue_ids(&env).await,
            vec![execution(&env, 202).await.0, execution(&env, 201).await.0],
            "{report}"
        );
    }

    // -- the one order ---------------------------------------------------------

    /// The sweep releases in the keys' order, exactly once.
    #[tokio::test]
    async fn release_follows_the_new_order_once() {
        let w = world("reorder-release.db", false).await;
        order(&w.env, json!([203]), "alpha").await.unwrap();
        w.env
            .db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        let report = sweep_at(&w.env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![w.a3.clone()], "a3, not a1");
        assert!(sweep_at(&w.env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        assert!(is_held(&w.env, 201).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reordered_release_racing_sweeps_happens_once() {
        let w = world("reorder-release-race.db", false).await;
        order(&w.env, json!([203]), "alpha").await.unwrap();
        w.env
            .db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        let mut handles = Vec::new();
        for _ in 0..4 {
            let orch = w.env.orch.clone();
            handles.push(tokio::spawn(
                async move { sweep(&orch).await.unwrap().released },
            ));
        }
        for _ in 0..2 {
            let (orch, id) = (w.env.orch.clone(), w.a3.clone());
            handles.push(tokio::spawn(async move {
                if evaluate_execution(&orch, &id).await.unwrap().released {
                    vec![id]
                } else {
                    Vec::new()
                }
            }));
        }
        let mut released = Vec::new();
        for handle in handles {
            released.extend(handle.await.unwrap());
        }
        assert_eq!(released, vec![w.a3.clone()], "exactly one release, of a3");
        assert_eq!(
            count(
                &w.env,
                &format!(
                    "SELECT COUNT(*) - COUNT(DISTINCT recipe_node_id) FROM jobs
                      WHERE execution_id = '{}'",
                    w.a3
                )
            )
            .await,
            0,
            "one job set"
        );
    }

    /// A thread held at its focus cap while the fleet drains: its fleet rows go
    /// dormant (cleared, not latched). An order must carry them too, or the
    /// readers list the old order now, and when the fleet fills and the focus
    /// cap frees, the dormant fleet rows wake on their old keys and release
    /// a1 ahead of the a3 the thread put first.
    #[tokio::test]
    async fn an_order_holds_across_a_cap_that_cleared_and_blocks_again() {
        use crate::execution::gates::capacity::fleet_load;
        use crate::execution::gates::sweep::tests::resolve_issue;
        let w = world("reorder-dormant.db", true).await;
        for number in 100..105 {
            w.env
                .db
                .execute(
                    format!("UPDATE jobs SET status = 'complete' WHERE id = 'sj{number}'"),
                    (),
                )
                .await
                .unwrap();
        }
        sweep_at(&w.env.orch, now()).await.unwrap();
        let drained = rows(&w.env).await;
        for (number, id) in [(201, &w.a1), (202, &w.a2), (203, &w.a3)] {
            assert!(is_held(&w.env, number).await, "{number} is still held");
            let fleet = w
                .env
                .db
                .query_opt_text(
                    "SELECT last_outcome FROM execution_gates WHERE id = ?1 AND satisfied_at IS NULL",
                    (row(&drained, id, FLEET).0.clone(),),
                )
                .await
                .unwrap();
            assert_eq!(
                fleet.as_deref(),
                Some("satisfied"),
                "its fleet row is dormant"
            );
        }

        order(&w.env, json!([203]), "alpha").await.unwrap();
        let after = rows(&w.env).await;
        assert_eq!(
            key(&after, &w.a3, FLEET),
            key(&drained, &w.a1, FLEET),
            "dormant keys rotate"
        );
        assert_eq!(key(&after, &w.a3, THREAD), key(&drained, &w.a1, THREAD));
        assert_eq!(queue_ids(&w.env).await[0], w.a3, "the thread's queue");
        let view: Vec<String> = queue_view(&w.env.orch)
            .await
            .unwrap()
            .held
            .into_iter()
            .map(|held| held.execution_id)
            .collect();
        assert_eq!(view[0], w.a3, "the queue page");
        let read = crate::resources::read_thread_queue(&w.env.db, "prj", "alpha", now()).await;
        assert!(read.contains("\n1. [203](cairn://p/prj/203)"), "{read}");

        // The fleet fills again and the focus cap frees: the dormant fleet
        // rows wake on their rotated keys.
        w.env
            .db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj150'", ())
            .await
            .unwrap();
        resolve_issue(&w.env, 150, "merged").await;
        let mut next = 400;
        while fleet_load(&w.env.orch).await.unwrap().in_progress < 6 {
            add_started(&w.env, next, None, "running").await;
            next += 1;
        }
        assert!(sweep_at(&w.env.orch, now())
            .await
            .unwrap()
            .released
            .is_empty());
        w.env
            .db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj400'", ())
            .await
            .unwrap();
        let report = sweep_at(&w.env.orch, now()).await.unwrap();
        assert_eq!(
            report.released,
            vec![w.a3.clone()],
            "a3, as the thread ordered"
        );
        assert!(is_held(&w.env, 201).await);
    }

    /// Filings in one second: the queue page, the places in line and the
    /// release order are one order, the order they were filed in.
    #[tokio::test]
    async fn ties_in_one_second_read_one_order_everywhere() {
        let env = env("reorder-ties.db").await;
        full_fleet(&env).await;
        for number in [3, 1, 2] {
            file(&env, number, json!([])).await.unwrap();
        }
        let (e3, e1, e2) = (
            execution(&env, 3).await.0,
            execution(&env, 1).await.0,
            execution(&env, 2).await.0,
        );
        sweep_at(&env.orch, now()).await.unwrap();
        let view: Vec<String> = queue_view(&env.orch)
            .await
            .unwrap()
            .held
            .into_iter()
            .map(|held| held.execution_id)
            .collect();
        assert_eq!(view, vec![e3.clone(), e1.clone(), e2.clone()]);
        let numbered = rows(&env).await;
        assert_eq!(
            [&e3, &e1, &e2].map(|id| place(&numbered, id, FLEET)),
            [Some(1), Some(2), Some(3)]
        );
        env.db
            .execute("UPDATE jobs SET status = 'complete' WHERE id = 'sj100'", ())
            .await
            .unwrap();
        assert_eq!(sweep_at(&env.orch, now()).await.unwrap().released, vec![e3]);
    }

    // -- the migration ----------------------------------------------------------

    /// The backfill gives every capacity row a distinct key on its second's
    /// scale, breaking ties as the sweep did: issue number, none last.
    #[tokio::test]
    async fn the_backfill_orders_ties_by_issue_number() {
        use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
        let temp = tempfile::tempdir().unwrap();
        let db = Arc::new(
            LocalDb::open(temp.path().join("backfill.db"))
                .await
                .unwrap(),
        );
        let before = TURSO_MIGRATIONS
            .iter()
            .filter(|migration| migration.name() != "execution_queue_order_key")
            .copied()
            .collect::<Vec<_>>();
        assert_eq!(before.len() + 1, TURSO_MIGRATIONS.len());
        MigrationRunner::new(before).run(&db).await.unwrap();
        db.execute_script(
            r#"INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
                 VALUES('p', 'default', 'P', 'prj', '/tmp/p', 1, 1);
               INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES('i3', 'p', 3, 'Three', 'backlog', 'backlog', 'none', 1, 1),
                       ('i1', 'p', 1, 'One', 'backlog', 'backlog', 'none', 1, 1),
                       ('i2', 'p', 2, 'Two', 'backlog', 'backlog', 'none', 1, 1);
               INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                 VALUES('e3', 'r', 'i3', 'p', 'held', 1, 1),
                       ('e1', 'r', 'i1', 'p', 'held', 1, 1),
                       ('e2', 'r', 'i2', 'p', 'held', 1, 1),
                       ('ex', 'r', NULL, 'p', 'held', 1, 1);
               INSERT INTO execution_gates(id, execution_id, seq, predicate_json, last_outcome, created_at)
                 VALUES('g3', 'e3', 1000, '{"fact":"fleet_slot"}', 'no_evidence', 100),
                       ('gx', 'ex', 1000, '{"fact":"fleet_slot"}', 'no_evidence', 100),
                       ('g1', 'e1', 1000, '{"fact":"fleet_slot"}', 'no_evidence', 100),
                       ('g2', 'e2', 1000, '{"fact":"fleet_slot"}', 'no_evidence', 100),
                       ('gt', 'e3', 1001, '{"fact":"thread_focus"}', 'no_evidence', 99),
                       ('ga', 'e2', 0, '{"fact":"issue_resolution"}', NULL, 100);"#,
        )
        .await
        .unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        let mut keys = Vec::new();
        for gate in ["g1", "g2", "g3", "gx", "gt", "ga"] {
            keys.push(
                db.query_opt_i64(
                    "SELECT queue_key FROM execution_gates WHERE id = ?1",
                    (gate.to_string(),),
                )
                .await
                .unwrap(),
            );
        }
        assert_eq!(
            keys,
            vec![
                Some(100_000_000),
                Some(100_000_001),
                Some(100_000_002),
                Some(100_000_003),
                Some(99_000_000),
                None,
            ]
        );
    }
}
