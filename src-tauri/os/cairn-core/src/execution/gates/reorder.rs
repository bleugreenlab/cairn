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
//! A row the operator PINNED (its execution was moved by
//! [`move_held_execution`]) sits out of every rotation: the thread's order
//! applies within the places its other children hold, and the operator's
//! place survives. An `order` that lists a pinned child is refused by name. A
//! pin ends when its execution is released and its rows latch.
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
//!
//! ## The operator's fleet move
//!
//! The queue page lists every cap-held execution in the fleet's order
//! (`capacity::fleet_order`), and the operator may move one of them to any
//! position, across threads ([`move_held_execution`]). The move gives each of
//! that execution's capacity rows a key FREE in its database. Its fleet row
//! takes a key strictly between its new neighbours' on the page, ties across
//! databases broken by database as every reader breaks them. Its thread row
//! takes a key ahead of every sibling now below it and behind the nearest
//! sibling above it. Nobody else's key changes, unless no free key is left
//! between the neighbours (keys are dense inside a burst). Then every key at
//! or before the lower neighbour, in every database, shifts down by one
//! second's scale, which keeps every relative order and opens a gap of a
//! million. The moved execution's rows are pinned (above) and record
//! `placed_by: the operator` where its key went down.
//!
//! Two edges the move does not cover. A pin covers the rows that exist when
//! the move runs: an execution moved while only its thread cap holds it gets
//! a fresh, unpinned fleet key at the back if the fleet fills later. And a move
//! to the back takes a key about a second past its neighbour, so a filing in
//! ANOTHER database within that second mints an earlier key and sorts ahead of
//! it; a filing in its own database still lands after it.
//!
//! Unlike a rotation, a move takes the admission lock
//! (`AdmissionSite::OperatorMove`): a renumber spans databases, and no
//! admission decision may read a line half shifted. The lock is taken before
//! any write and let go before the places are renumbered. A database failing
//! between two shifts can leave the cross-database order skewed by one shift;
//! database writes fail here only when the database itself is failing, and
//! the error reaches the operator. Caps are counts the keys cannot change, so
//! a move, like a rotation, never lets a start exceed a cap.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use cairn_db::turso::{params, Connection};
use cairn_workflow::gates::store::{
    set_gate_detail_conn, set_gate_queue_key_conn, shift_queue_keys_conn, QUEUE_KEY_SCALE,
};

use super::capacity::{
    capacity_fact, decode_hold, encode_hold, fleet_order, AdmissionSite, FleetOrdered,
    CAPACITY_FACTS, FLEET_SLOT_FACT, THREAD_FOCUS_FACT,
};
use crate::models::CapacityHold;
use crate::orchestrator::Orchestrator;
use crate::storage::{DbResult, LocalDb, RowExt};

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

    /// Whether the operator gave it its place, so its thread's order does not
    /// move it.
    pub fn pinned(&self) -> bool {
        self.holds
            .iter()
            .chain(&self.dormant)
            .any(QueuedHold::pinned)
    }
}

impl QueuedHold {
    fn pinned(&self) -> bool {
        self.hold.as_ref().is_some_and(CapacityHold::pinned)
    }
}

/// The thread's cap-held children's executions, in queue order: exactly the
/// set the queue page lists, narrowed to this thread. The predicate's gate
/// list drives it, so `issues` (which has no `parent_thread_id` index) is only
/// probed by id.
pub(super) const THREAD_QUEUE_SQL: &str = concat!(
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
pub(super) const THREAD_HOLDS_SQL: &str = concat!(
    "SELECT g.execution_id, g.id, g.predicate_json,
            COALESCE(g.queue_key, g.created_at * 1000000), g.detail,
            g.last_outcome = 'no_evidence'
       FROM execution_gates g
       JOIN executions e INDEXED BY idx_executions_hold_facts ON e.id = g.execution_id
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
/// takes, while numbered places remain, the i-th smallest place. A row the
/// operator pinned is left out, keeping its key and place. A row whose key
/// went down records `placed_by`; one whose key went up forgets it. A
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
                    .find(|row| row.fact == fact && !row.pinned())
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

/// Why an issue a thread listed has no cap-held execution to order. It walks
/// `idx_executions_issue_seq` down to the newest execution and reads that one
/// row: `status` is in no index keyed by issue. Only a refused `order` entry
/// runs it.
pub(super) const LATEST_EXECUTION_SQL: &str =
    "SELECT status FROM executions WHERE issue_id = ?1 ORDER BY seq DESC LIMIT 1";

/// Whether the issue's execution waits on an authored gate. The unsatisfied
/// gates drive it, which that gate implies; the unary `+` keeps
/// `idx_executions_issue_seq`, which would read every execution on the issue
/// from its row, from serving the issue instead.
pub(super) const AWAITS_AUTHORED_SQL: &str = concat!(
    "SELECT 1 FROM executions e WHERE ",
    cairn_db::unsatisfied_gate_source_sql!(),
    " AND +e.issue_id = ?1 AND ",
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
        if queued.iter().any(|queued| queued.pinned()) {
            refusals.push(format!("{number}'s place is the operator's"));
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

/// A thread's queue as an `order` addresses it, by project key and thread
/// name: the project's database, the project's id, and the thread.
pub(crate) async fn queue_thread(
    orch: &Orchestrator,
    project: &str,
    name: &str,
) -> Result<(Arc<LocalDb>, String, cairn_db::models::Thread), String> {
    let db = orch.db.for_project(project).await;
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(&db, project).await?;
    let thread = crate::threads::crud::by_name(&db, &project_id, name).await?;
    Ok((db, project_id, thread))
}

/// Order `thread`'s queue, the one path both the `<thread>/queue {order}`
/// write and the operator's thread queue popover take: validated and applied
/// in ONE write transaction ([`order_thread_queue_conn`]), then a db-change
/// for every execution it moved. Returns what it did, or every offending
/// entry with nothing changed. The caller has established `placed_by`'s
/// authority.
pub(crate) async fn order_thread_queue(
    orch: &Orchestrator,
    db: &LocalDb,
    thread: &cairn_db::models::Thread,
    order: &[serde_json::Value],
    placed_by: &str,
) -> Result<String, String> {
    let (thread_id, thread_name) = (thread.id.clone(), thread.name.clone());
    let (entries, by) = (order.to_vec(), placed_by.to_string());
    let outcome = db
        .write(move |conn| {
            let (thread_id, thread_name) = (thread_id.clone(), thread_name.clone());
            let (entries, by) = (entries.clone(), by.clone());
            Box::pin(async move {
                order_thread_queue_conn(conn, &thread_id, &thread_name, &entries, &by).await
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    let rotation = outcome.map_err(|refusals| {
        let lines: Vec<String> = refusals
            .iter()
            .map(|refusal| format!("- {refusal}"))
            .collect();
        format!(
            "{}'s queue was not ordered:\n{}\nNothing changed.",
            thread.name,
            lines.join("\n")
        )
    })?;
    for execution_id in &rotation.moved {
        let change =
            crate::notify::execution_gates_db_change_for_execution(db, execution_id, "update")
                .await;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    let listed: Vec<i64> = order.iter().filter_map(serde_json::Value::as_i64).collect();
    Ok(ordered_summary(&thread.name, &listed, &rotation.queue))
}

/// The operator's order of a thread's queue, from the sidebar's thread queue
/// popover: the same order a thread gives its own queue, recorded as placed
/// by the operator. The caller has proven the operator.
pub async fn operator_order_thread_queue(
    orch: &Orchestrator,
    project: &str,
    thread: &str,
    order: &[serde_json::Value],
) -> Result<String, String> {
    let (db, _, target) = queue_thread(orch, project, thread).await?;
    order_thread_queue(orch, &db, &target, order, OPERATOR).await
}

/// `Ordered harness's queue: 5841 (fleet 3rd, thread 1st), 5829 (thread 2nd)`.
fn ordered_summary(thread: &str, listed: &[i64], queue: &[ThreadQueued]) -> String {
    let (front, rest): (Vec<_>, Vec<_>) = queue
        .iter()
        .partition(|queued| listed.contains(&queued.issue_number));
    let named: Vec<String> = front
        .iter()
        .map(|queued| format!("{} ({})", queued.issue_number, places_phrase(queued, ", ")))
        .collect();
    let mut summary = format!("Ordered {thread}'s queue: {}", named.join(", "));
    if !rest.is_empty() {
        summary.push_str(&format!(
            "; {} more behind them in their current order",
            rest.len()
        ));
    }
    summary.push('.');
    summary
}

// ---------------------------------------------------------------------------
// The operator's fleet move
// ---------------------------------------------------------------------------

/// Who an operator move records as having placed a hold ahead.
const OPERATOR: &str = "the operator";

const NOT_QUEUED: &str = "That execution is not queued: it started, or it waits on its own gates.";

/// How many times a move re-reads the queue after renumbering to make room.
/// One shift opens a gap of a million, so a second is already unexpected.
const MOVE_ATTEMPTS: usize = 3;

/// What the operator's move did.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FleetMove {
    /// Whether anything was written: false when it already stood there.
    pub moved: bool,
    /// Its 1-based place in the fleet's order afterwards.
    pub position: usize,
}

/// Every unlatched capacity row in one database, blocking or dormant. Read
/// from `execution_gates` alone: the move takes each execution's database and
/// thread from the queue rows, so this never opens an `executions` row.
pub(crate) const CAPACITY_HOLDS_SQL: &str = concat!(
    "SELECT g.execution_id, g.id, g.predicate_json,
            COALESCE(g.queue_key, g.created_at * 1000000), g.detail
       FROM execution_gates g
      WHERE g.satisfied_at IS NULL AND ",
    cairn_db::capacity_gate_sql!(),
    " ORDER BY g.seq, g.id"
);

/// Every key in use in one database within `[?1, ?2]`, latched rows included.
pub(crate) const KEYS_IN_RANGE_SQL: &str =
    "SELECT queue_key FROM execution_gates WHERE queue_key >= ?1 AND queue_key <= ?2";

/// A place in the fleet's order: a key, and the database that breaks ties.
type Spot = (i64, usize);

/// Whether `key` in database `db` sorts strictly after `lo` and before `hi`
/// (an open end bounds nothing).
fn fits(key: i64, db: usize, lo: Option<Spot>, hi: Option<Spot>) -> bool {
    lo.is_none_or(|lo| (key, db) > lo) && hi.is_none_or(|hi| (key, db) < hi)
}

/// The keys a row in database `db` may take between `lo` and `hi`, as an
/// inclusive range: `(key, db)` sorts after `lo` and before `hi`. An open end
/// reaches two seconds' scale past the other, so a move to either end lands
/// one second's scale beyond its neighbour. `None` with both ends open.
fn key_range(lo: Option<Spot>, hi: Option<Spot>, db: usize) -> Option<(i64, i64)> {
    let first = lo.map(|(key, at)| if db > at { key } else { key.saturating_add(1) });
    let last = hi.map(|(key, at)| if db < at { key } else { key.saturating_sub(1) });
    let span = 2 * QUEUE_KEY_SCALE;
    match (first, last) {
        (Some(first), Some(last)) => Some((first, last)),
        (Some(first), None) => Some((first, first.saturating_add(span))),
        (None, Some(last)) => Some((last.saturating_sub(span), last)),
        (None, None) => None,
    }
}

/// The unused key in `[first, last]` nearest its middle, or `None` when every
/// key there is taken.
fn free_key(first: i64, last: i64, used: &BTreeSet<i64>) -> Option<i64> {
    if first > last {
        return None;
    }
    let middle = first + (last - first) / 2;
    let mut distance = 0i64;
    loop {
        let (below, above) = (middle - distance, middle.saturating_add(distance));
        if below < first && above > last {
            return None;
        }
        if below >= first && !used.contains(&below) {
            return Some(below);
        }
        if above <= last && !used.contains(&above) {
            return Some(above);
        }
        distance += 1;
    }
}

/// The later of two lower bounds, an open one being no bound.
fn later(a: Option<Spot>, b: Option<Spot>) -> Option<Spot> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

/// The earlier of two upper bounds, an open one being no bound.
fn earlier(a: Option<Spot>, b: Option<Spot>) -> Option<Spot> {
    match (a, b) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, None) => a,
        (None, b) => b,
    }
}

/// Every unlatched capacity row of the listed executions, by execution.
async fn capacity_holds(
    order: &[FleetOrdered],
) -> Result<HashMap<String, Vec<QueuedHold>>, String> {
    let listed: HashSet<&str> = order
        .iter()
        .map(|entry| entry.queued.execution_id.as_str())
        .collect();
    let mut read = HashSet::new();
    let mut holds: HashMap<String, Vec<QueuedHold>> = HashMap::new();
    for entry in order {
        if !read.insert(entry.db_index) {
            continue;
        }
        let rows = entry
            .db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn.query(CAPACITY_HOLDS_SQL, ()).await?;
                    let mut out = Vec::new();
                    while let Some(row) = rows.next().await? {
                        out.push((
                            row.text(0)?,
                            row.text(1)?,
                            row.text(2)?,
                            row.i64(3)?,
                            row.opt_text(4)?,
                        ));
                    }
                    Ok(out)
                })
            })
            .await
            .map_err(|error| format!("failed to read the queue: {error}"))?;
        for (execution_id, gate_id, predicate, queue_key, detail) in rows {
            let Some(fact) = capacity_fact(&predicate) else {
                continue;
            };
            if !listed.contains(execution_id.as_str()) {
                continue;
            }
            holds.entry(execution_id).or_default().push(QueuedHold {
                fact,
                gate_id,
                queue_key,
                hold: detail
                    .as_deref()
                    .and_then(|detail| decode_hold(fact, detail)),
            });
        }
    }
    Ok(holds)
}

async fn keys_in_range(db: &LocalDb, first: i64, last: i64) -> Result<BTreeSet<i64>, String> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn.query(KEYS_IN_RANGE_SQL, params![first, last]).await?;
            let mut keys = BTreeSet::new();
            while let Some(row) = rows.next().await? {
                keys.insert(row.i64(0)?);
            }
            Ok(keys)
        })
    })
    .await
    .map_err(|error| format!("failed to read the queue: {error}"))
}

/// Move held execution `execution_id` to `position` (1-based) in the fleet's
/// order, as the operator's drag on the queue page does: across threads, or
/// within one. The position is resolved against the queue as it stands now,
/// so a move to the front or the back is exact even from a stale page. A
/// refusal changes nothing. The caller has proven the operator; see the
/// module doc for what the move writes and why it takes the admission lock.
pub async fn move_held_execution(
    orch: &Orchestrator,
    execution_id: &str,
    position: usize,
) -> Result<FleetMove, String> {
    let guard = orch
        .admission_lock
        .acquire(AdmissionSite::OperatorMove, execution_id)
        .await
        .map_err(|busy| busy.to_string())?;
    let moved = move_under_lock(orch, execution_id, position).await;
    drop(guard);
    let (outcome, touched) = moved?;
    if outcome.moved {
        super::capacity::refresh_places(orch, None).await?;
    }
    for (db, execution_id) in touched {
        let change =
            crate::notify::execution_gates_db_change_for_execution(&db, &execution_id, "update")
                .await;
        let _ = orch.services.emitter.emit("db-change", change);
    }
    Ok(outcome)
}

/// The move's reads and writes, holding the admission lock. Returns what it
/// did and every execution whose rows it wrote or shifted.
async fn move_under_lock(
    orch: &Orchestrator,
    execution_id: &str,
    position: usize,
) -> Result<(FleetMove, Vec<(Arc<LocalDb>, String)>), String> {
    let mut touched: Vec<(Arc<LocalDb>, String)> = Vec::new();
    for _ in 0..MOVE_ATTEMPTS {
        let order = fleet_order(orch).await?;
        let Some(from) = order
            .iter()
            .position(|entry| entry.queued.execution_id == execution_id)
        else {
            return Err(NOT_QUEUED.to_string());
        };
        if position == 0 || position > order.len() {
            return Err(format!(
                "Position {position} is outside the queue ({} queued).",
                order.len()
            ));
        }
        if position == from + 1 {
            return Ok((
                FleetMove {
                    moved: false,
                    position,
                },
                touched,
            ));
        }
        let holds = capacity_holds(&order).await?;
        let mut line: Vec<&FleetOrdered> = order.iter().collect();
        let x = line.remove(from);
        let at = position - 1;
        line.insert(at, x);
        let spot = |entry: &FleetOrdered| (entry.key, entry.db_index);
        let page_lo = at.checked_sub(1).map(|index| spot(line[index]));
        let page_hi = line.get(at + 1).map(|entry| spot(entry));

        // Its thread's order: ahead of every sibling now below it, behind the
        // nearest sibling above it that still sorts before those.
        let thread_key = |entry: &&FleetOrdered| -> Option<i64> {
            let same_thread = entry.db_index == x.db_index
                && entry.queued.thread_id.is_some()
                && entry.queued.thread_id == x.queued.thread_id
                && entry.queued.execution_id != execution_id;
            if !same_thread {
                return None;
            }
            holds
                .get(&entry.queued.execution_id)?
                .iter()
                .find(|row| row.fact == THREAD_FOCUS_FACT)
                .map(|row| row.queue_key)
        };
        let thread_hi = line[at + 1..].iter().filter_map(thread_key).min();
        let thread_lo = line[..at]
            .iter()
            .filter_map(thread_key)
            .filter(|key| thread_hi.is_none_or(|hi| *key < hi))
            .max();
        let thread_gap = (
            thread_lo.map(|key| (key, x.db_index)),
            thread_hi.map(|key| (key, x.db_index)),
        );

        let rows = holds.get(execution_id).cloned().unwrap_or_default();
        let has_fleet_row = rows.iter().any(|row| row.fact == FLEET_SLOT_FACT);
        let mut picked = BTreeSet::new();
        let mut keys = Vec::new();
        let mut renumber_at = None;
        for row in &rows {
            let (lo, hi) = if row.fact == FLEET_SLOT_FACT {
                (page_lo, page_hi)
            } else if has_fleet_row {
                thread_gap
            } else {
                // With no fleet row its thread key is its place on the page
                // too: inside both gaps when they meet, else its thread's.
                let (lo, hi) = (later(thread_gap.0, page_lo), earlier(thread_gap.1, page_hi));
                match key_range(lo, hi, x.db_index) {
                    Some((first, last)) if first > last => thread_gap,
                    _ => (lo, hi),
                }
            };
            if fits(row.queue_key, x.db_index, lo, hi) {
                keys.push(row.queue_key);
                continue;
            }
            let Some((first, last)) = key_range(lo, hi, x.db_index) else {
                keys.push(row.queue_key);
                continue;
            };
            let mut used = keys_in_range(&x.db, first, last).await?;
            used.extend(picked.iter().copied());
            match free_key(first, last, &used) {
                Some(key) => {
                    picked.insert(key);
                    keys.push(key);
                }
                None => {
                    renumber_at = Some(lo);
                    break;
                }
            }
        }

        if let Some(lo) = renumber_at {
            let Some((key, db_at)) = lo else {
                return Err("The queue has no room ahead of its first place.".to_string());
            };
            for (index, db) in orch.db.all_dbs().await.into_iter().enumerate() {
                // Keep cross-database ties: a later database's row at the same
                // key sorts after the neighbour, so it stays.
                let at_most = if index <= db_at { key } else { key - 1 };
                db.write(move |conn| {
                    Box::pin(
                        async move { shift_queue_keys_conn(conn, at_most, -QUEUE_KEY_SCALE).await },
                    )
                })
                .await
                .map_err(|error| format!("failed to renumber the queue: {error}"))?;
                for entry in order.iter().filter(|entry| entry.db_index == index) {
                    let shifted = holds
                        .get(&entry.queued.execution_id)
                        .is_some_and(|rows| rows.iter().any(|row| row.queue_key <= at_most));
                    if shifted {
                        touched.push((entry.db.clone(), entry.queued.execution_id.clone()));
                    }
                }
            }
            continue;
        }

        let mut writes = Vec::new();
        for (row, key) in rows.iter().zip(&keys) {
            let detail = row.hold.as_ref().and_then(|hold| {
                let mut next = hold.clone();
                next.set_pinned(true);
                match key.cmp(&row.queue_key) {
                    std::cmp::Ordering::Less => next.set_placed_by(Some(OPERATOR.to_string())),
                    std::cmp::Ordering::Greater => next.set_placed_by(None),
                    std::cmp::Ordering::Equal => {}
                }
                (&next != hold).then(|| encode_hold(&next))
            });
            let key = (*key != row.queue_key).then_some(*key);
            if key.is_some() || detail.is_some() {
                writes.push((row.gate_id.clone(), key, detail));
            }
        }
        let moved = !writes.is_empty();
        if moved {
            x.db.write(|conn| {
                let writes = writes.clone();
                Box::pin(async move {
                    for (gate_id, key, detail) in &writes {
                        if let Some(key) = key {
                            set_gate_queue_key_conn(conn, gate_id, *key).await?;
                        }
                        if let Some(detail) = detail {
                            set_gate_detail_conn(conn, gate_id, detail).await?;
                        }
                    }
                    Ok(())
                })
            })
            .await
            .map_err(|error| format!("failed to move it: {error}"))?;
            touched.push((x.db.clone(), execution_id.to_string()));
        }

        // Where it landed: its new place key among everyone else's.
        let fleet_key = rows
            .iter()
            .zip(&keys)
            .find(|(row, _)| row.fact == FLEET_SLOT_FACT)
            .map(|(_, key)| *key);
        let landed_key = fleet_key
            .or_else(|| keys.iter().copied().min())
            .unwrap_or(x.key);
        let landed = 1 + order
            .iter()
            .filter(|entry| entry.queued.execution_id != execution_id)
            .filter(|entry| spot(entry) < (landed_key, x.db_index))
            .count();
        return Ok((
            FleetMove {
                moved,
                position: landed,
            },
            touched,
        ));
    }
    Err("The queue could not make room to move it; its order is unchanged.".to_string())
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
    use crate::execution::gates::capacity::queue_view;
    use crate::execution::gates::capacity::tests::{
        add_issue, add_started, add_thread, caps, detail, full_fleet, is_held,
    };
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
        assert!(
            filed.starts_with("Started execution 1 for prj/201 \"Issue 201\" (recipe '"),
            "{filed}"
        );
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

    // -- the operator's fleet move ----------------------------------------------

    /// The queue page's order: every cap-held execution, the fleet's order.
    async fn view_ids(env: &Env) -> Vec<String> {
        queue_view(&env.orch)
            .await
            .unwrap()
            .held
            .into_iter()
            .map(|held| held.execution_id)
            .collect()
    }

    fn ids(list: &[&String]) -> Vec<String> {
        list.iter().map(|id| id.to_string()).collect()
    }

    async fn complete(env: &Env, job: &str) {
        env.db
            .execute(
                format!("UPDATE jobs SET status = 'complete' WHERE id = '{job}'"),
                (),
            )
            .await
            .unwrap();
    }

    /// The acceptance case: five held across alpha and beta, the 5th moved to
    /// 1st. Nobody else's key moves, and the next release is the moved one,
    /// exactly once under racing sweeps and evaluations.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_operator_moves_the_fifth_held_execution_to_first_across_threads() {
        let w = world("move-fifth-first.db", false).await;
        file(&w.env, 204, json!([])).await.unwrap();
        let a4 = execution(&w.env, 204).await.0;
        sweep_at(&w.env.orch, now()).await.unwrap();
        assert_eq!(
            view_ids(&w.env).await,
            ids(&[&w.a1, &w.b1, &w.a2, &w.a3, &a4])
        );
        let before = rows(&w.env).await;

        let moved = move_held_execution(&w.env.orch, &a4, 1).await.unwrap();
        assert_eq!(
            moved,
            FleetMove {
                moved: true,
                position: 1
            }
        );
        assert_eq!(
            view_ids(&w.env).await,
            ids(&[&a4, &w.a1, &w.b1, &w.a2, &w.a3])
        );
        let after = rows(&w.env).await;
        assert!(key(&after, &a4, FLEET) < key(&before, &w.a1, FLEET));
        assert_eq!(
            placed_by(&after, &a4, FLEET).as_deref(),
            Some("the operator")
        );
        assert!(hold(&after, &a4, FLEET).pinned());
        assert_eq!(place(&after, &a4, FLEET), Some(1), "renumbered at once");
        for (id, was) in [(&w.a1, 1), (&w.b1, 2), (&w.a2, 3), (&w.a3, 4)] {
            assert_eq!(
                key(&after, id, FLEET),
                key(&before, id, FLEET),
                "{id} kept its key"
            );
            assert_eq!(place(&after, id, FLEET), Some(was + 1));
            assert!(!hold(&after, id, FLEET).pinned());
        }
        assert!(detail(&w.env, &a4)
            .await
            .ends_with("placed ahead by the operator"));
        assert_eq!(
            w.env
                .orch
                .admission_lock
                .acquisitions(AdmissionSite::OperatorMove),
            1,
            "the move took the lock under its own site"
        );

        complete(&w.env, "sj100").await;
        let mut handles = Vec::new();
        for _ in 0..4 {
            let orch = w.env.orch.clone();
            handles.push(tokio::spawn(
                async move { sweep(&orch).await.unwrap().released },
            ));
        }
        for _ in 0..2 {
            let (orch, id) = (w.env.orch.clone(), a4.clone());
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
        assert_eq!(
            released,
            vec![a4.clone()],
            "exactly one release, of the moved"
        );
        assert_eq!(
            count(
                &w.env,
                &format!(
                    "SELECT COUNT(*) - COUNT(DISTINCT recipe_node_id) FROM jobs
                      WHERE execution_id = '{a4}'"
                )
            )
            .await,
            0,
            "one job set"
        );
        for number in [201, 202, 203, 301] {
            assert!(is_held(&w.env, number).await, "{number} is still held");
        }
    }

    /// The operator moved a3 to the front; alpha then orders its own queue.
    /// a3 keeps its keys and its place, alpha's order permutes a1 and a2
    /// among their own keys, and an order naming a3 is refused.
    #[tokio::test]
    async fn an_operator_placement_survives_a_later_thread_reorder() {
        let w = world("move-survives-order.db", true).await;
        move_held_execution(&w.env.orch, &w.a3, 1).await.unwrap();
        let placed = rows(&w.env).await;
        assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.a1, &w.b1, &w.a2]));

        let rotation = order(&w.env, json!([202, 201]), "alpha").await.unwrap();
        assert_eq!(
            rotation
                .queue
                .iter()
                .map(|queued| queued.issue_number)
                .collect::<Vec<_>>(),
            vec![203, 202, 201]
        );
        let after = rows(&w.env).await;
        for fact in [FLEET, THREAD] {
            assert_eq!(key(&after, &w.a3, fact), key(&placed, &w.a3, fact));
            assert_eq!(key(&after, &w.a2, fact), key(&placed, &w.a1, fact));
            assert_eq!(key(&after, &w.a1, fact), key(&placed, &w.a2, fact));
            assert!(hold(&after, &w.a3, fact).pinned());
            assert_eq!(
                placed_by(&after, &w.a3, fact).as_deref(),
                Some("the operator")
            );
        }
        assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.a2, &w.b1, &w.a1]));

        let refused = order(&w.env, json!([203]), "alpha").await.unwrap_err();
        assert_eq!(refused, vec!["203's place is the operator's"]);
        assert_eq!(rows(&w.env).await, after, "a refused order changes nothing");
    }

    /// The sidebar's thread queue reads the queue view filtered by thread, so
    /// each row carries its thread. The operator's order from it is the same
    /// order a `<thread>/queue` write gives: alpha's own places, b1 untouched,
    /// recorded as the operator's; a refusal names the entry and changes
    /// nothing.
    #[tokio::test]
    async fn the_operators_thread_order_takes_the_threads_own_places() {
        let w = world("operator-thread-order.db", false).await;
        let threads: Vec<(String, Option<String>)> = queue_view(&w.env.orch)
            .await
            .unwrap()
            .held
            .into_iter()
            .map(|held| (held.execution_id, held.thread_id))
            .collect();
        let alpha = Some("ta".to_string());
        assert_eq!(
            threads,
            vec![
                (w.a1.clone(), alpha.clone()),
                (w.b1.clone(), Some("tb".to_string())),
                (w.a2.clone(), alpha.clone()),
                (w.a3.clone(), alpha),
            ]
        );

        let summary = operator_order_thread_queue(&w.env.orch, "prj", "alpha", &[json!(203)])
            .await
            .unwrap();
        assert!(
            summary.starts_with("Ordered alpha's queue: 203 ("),
            "{summary}"
        );
        assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.b1, &w.a1, &w.a2]));
        let after = rows(&w.env).await;
        assert_eq!(
            placed_by(&after, &w.a3, FLEET).as_deref(),
            Some("the operator")
        );
        assert!(
            !hold(&after, &w.a3, FLEET).pinned(),
            "an order pins nothing"
        );

        let refused = operator_order_thread_queue(&w.env.orch, "prj", "alpha", &[json!(301)])
            .await
            .unwrap_err();
        assert_eq!(
            refused,
            "alpha's queue was not ordered:\n- 301 is not a child of alpha\nNothing changed."
        );
        assert_eq!(rows(&w.env).await, after, "a refused order changes nothing");
    }

    /// Within one thread: a3 above a1 on both the fleet and the thread, and
    /// the next release, once both caps free a place, is a3.
    #[tokio::test]
    async fn the_operator_moves_within_one_thread() {
        use crate::execution::gates::sweep::tests::resolve_issue;
        let w = world("move-within-thread.db", true).await;
        let before = rows(&w.env).await;
        move_held_execution(&w.env.orch, &w.a3, 1).await.unwrap();
        let after = rows(&w.env).await;
        assert_eq!(view_ids(&w.env).await, ids(&[&w.a3, &w.a1, &w.b1, &w.a2]));
        assert_eq!(queue_ids(&w.env).await, ids(&[&w.a3, &w.a1, &w.a2]));
        assert!(key(&after, &w.a3, THREAD) < key(&before, &w.a1, THREAD));
        assert_eq!(
            [&w.a3, &w.a1, &w.a2].map(|id| place(&after, id, THREAD)),
            [Some(1), Some(2), Some(3)]
        );
        for id in [&w.a1, &w.a2, &w.b1] {
            assert_eq!(row(&after, id, FLEET).1, row(&before, id, FLEET).1);
        }

        complete(&w.env, "sj100").await;
        complete(&w.env, "sj150").await;
        resolve_issue(&w.env, 150, "merged").await;
        let report = sweep_at(&w.env.orch, now()).await.unwrap();
        assert_eq!(report.released, vec![w.a3.clone()]);
        assert!(is_held(&w.env, 201).await);
    }

    /// Consecutive keys leave no room between a1 and b1: the move shifts
    /// everything up to a1 down a second's scale, lands exactly where asked,
    /// moves no one else's order, and a fresh filing still lands last.
    #[tokio::test]
    async fn a_move_into_a_one_wide_gap_renumbers_without_reordering() {
        let w = world("move-renumber.db", false).await;
        file(&w.env, 204, json!([])).await.unwrap();
        let a4 = execution(&w.env, 204).await.0;
        sweep_at(&w.env.orch, now()).await.unwrap();
        let base = key(&rows(&w.env).await, &w.a1, FLEET);
        for (offset, id) in [&w.a1, &w.b1, &w.a2, &w.a3, &a4].into_iter().enumerate() {
            w.env
                .db
                .execute(
                    format!(
                        "UPDATE execution_gates SET queue_key = {} WHERE execution_id = '{id}'
                            AND json_extract(predicate_json, '$.fact') = 'fleet_slot'",
                        base + offset as i64
                    ),
                    (),
                )
                .await
                .unwrap();
        }
        let before = rows(&w.env).await;

        let moved = move_held_execution(&w.env.orch, &a4, 2).await.unwrap();
        assert_eq!(moved.position, 2);
        assert_eq!(
            view_ids(&w.env).await,
            ids(&[&w.a1, &a4, &w.b1, &w.a2, &w.a3])
        );
        let after = rows(&w.env).await;
        assert_eq!(
            key(&after, &w.a1, FLEET),
            base - cairn_workflow::gates::store::QUEUE_KEY_SCALE,
            "the shift ran"
        );
        for id in [&w.b1, &w.a2, &w.a3] {
            assert_eq!(key(&after, id, FLEET), key(&before, id, FLEET));
        }
        let mut keys: Vec<i64> = after.values().map(|(_, key, ..)| *key).collect();
        let all = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(keys.len(), all, "every key stays distinct");

        file(&w.env, 1, json!([])).await.unwrap();
        let fresh = execution(&w.env, 1).await.0;
        assert_eq!(
            view_ids(&w.env).await.last(),
            Some(&fresh),
            "a fresh filing lands last"
        );
    }

    /// Not queued, out of range, already there: each refused or a no-op, and
    /// no row changes.
    #[tokio::test]
    async fn a_move_that_cannot_apply_changes_nothing() {
        let w = world("move-refusals.db", false).await;
        let before = rows(&w.env).await;
        let refused = move_held_execution(&w.env.orch, "se100", 1)
            .await
            .unwrap_err();
        assert_eq!(
            refused,
            "That execution is not queued: it started, or it waits on its own gates."
        );
        for position in [0, 5, 99] {
            let refused = move_held_execution(&w.env.orch, &w.a2, position)
                .await
                .unwrap_err();
            assert_eq!(
                refused,
                format!("Position {position} is outside the queue (4 queued).")
            );
        }
        assert_eq!(
            move_held_execution(&w.env.orch, &w.a2, 3).await.unwrap(),
            FleetMove {
                moved: false,
                position: 3
            },
            "a2 is already 3rd"
        );
        assert_eq!(rows(&w.env).await, before);
    }

    #[test]
    fn a_free_key_sits_strictly_between_its_neighbours() {
        let used = BTreeSet::from([10, 11, 12]);
        assert_eq!(key_range(Some((10, 0)), Some((12, 0)), 0), Some((11, 11)));
        assert_eq!(free_key(11, 11, &used), None, "a one-wide gap is full");
        // A later database's row may share the lower neighbour's key; an
        // earlier one's the upper's.
        assert_eq!(key_range(Some((10, 0)), Some((12, 0)), 1), Some((10, 11)));
        assert_eq!(key_range(Some((10, 1)), Some((12, 1)), 0), Some((11, 12)));
        assert_eq!(free_key(10, 20, &used), Some(15));
        assert_eq!(free_key(10, 13, &used), Some(13));
        let scale = QUEUE_KEY_SCALE;
        assert_eq!(
            key_range(None, Some((100, 0)), 0),
            Some((99 - 2 * scale, 99))
        );
        assert_eq!(
            key_range(Some((100, 0)), None, 0),
            Some((101, 101 + 2 * scale))
        );
        assert_eq!(key_range(None, None, 0), None);
        assert!(fits(11, 0, Some((10, 0)), Some((12, 0))));
        assert!(!fits(10, 0, Some((10, 0)), None));
        assert!(fits(10, 1, Some((10, 0)), None));
    }

    async fn plan(env: &Env, sql: &str) -> String {
        let sql = format!(
            "EXPLAIN QUERY PLAN {}",
            sql.replace("?1", "0").replace("?2", "0")
        );
        env.db
            .read(move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let mut rows = conn.query(&sql, ()).await?;
                    let mut steps = Vec::new();
                    while let Some(row) = rows.next().await? {
                        steps.push(row.text(3)?);
                    }
                    Ok(steps.join("\n"))
                })
            })
            .await
            .unwrap()
    }

    /// The move's own statements never open an `executions` row (whose
    /// snapshot Turso would assemble, cairn/5894): they read and shift
    /// `execution_gates` alone, the keys through their index. The queue rows
    /// the move shares with the page are pinned by cairn/5894.
    #[tokio::test]
    async fn the_moves_statements_never_touch_executions() {
        let env = env("move-plans.db").await;
        for (name, sql, index) in [
            ("capacity holds", CAPACITY_HOLDS_SQL, None),
            (
                "keys in range",
                KEYS_IN_RANGE_SQL,
                Some("idx_execution_gates_queue_key"),
            ),
            (
                "the shift",
                cairn_workflow::gates::store::SHIFT_QUEUE_KEYS_SQL,
                Some("idx_execution_gates_queue_key"),
            ),
        ] {
            let plan = plan(&env, sql).await;
            assert!(
                !plan.contains("executions"),
                "{name} reads executions:\n{plan}"
            );
            if let Some(index) = index {
                assert!(plan.contains(index), "{name} does not use {index}:\n{plan}");
            }
        }
    }

    /// cairn/5894's cost fixture: 2,000 executions with 150 KB snapshots, 20
    /// of them cap-held. One move holds the admission lock for every read and
    /// write it makes, and every filing and sweep waits on it, so it must stay
    /// short. Run with `--ignored` for the number.
    #[tokio::test]
    #[ignore = "builds a 300 MB database; run with --ignored for the timing"]
    async fn one_move_over_two_thousand_snapshots_is_bounded() {
        let env = env("move-cost.db").await;
        full_fleet(&env).await;
        let snapshot = "x".repeat(150_000);
        for batch in 0..40 {
            let mut script = String::new();
            for index in 0..50 {
                let id = batch * 50 + index;
                script.push_str(&format!(
                    "INSERT INTO executions(id, recipe_id, project_id, status, started_at, seq, snapshot)
                       VALUES('history-{id}', 'r', 'p', 'complete', 1, 1, '{snapshot}');"
                ));
            }
            env.db.execute_script(script).await.unwrap();
        }
        add_thread(&env, "ta", "alpha").await;
        let mut held = Vec::new();
        for number in 600..620 {
            add_issue(&env, number, Some("ta")).await;
            file(&env, number, json!([])).await.unwrap();
            held.push(execution(&env, number).await.0);
        }
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(view_ids(&env).await.len(), 20);

        let started = std::time::Instant::now();
        move_held_execution(&env.orch, &held[19], 1).await.unwrap();
        let took = started.elapsed();
        println!("one move over 2,000 snapshots took {took:?}");
        assert_eq!(view_ids(&env).await[0], held[19]);
        assert!(took < std::time::Duration::from_secs(5), "{took:?}");
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
