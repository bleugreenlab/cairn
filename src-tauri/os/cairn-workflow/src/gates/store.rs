//! Durable storage for execution `after:` gates.
//!
//! A gate is one predicate an execution waits on. While an execution has gates
//! and `executions.released_at` is NULL it is **held**: it mints no jobs, no
//! admission request, and consumes no slot. That pair of facts is the whole
//! definition, which is why the status projection derives `held` rather than
//! anyone writing it (see [`crate::transitions`]).
//!
//! Two properties this layer exists to guarantee:
//!
//! - **Satisfaction latches.** Predicates are level-resolvable, so a check that
//!   goes green and later red would otherwise flip a gate back to unmet. Once
//!   `satisfied_at` is set it is never cleared, so release stays a one-way door.
//! - **Release happens exactly once.** [`release_execution_conn`] is guarded on
//!   `released_at IS NULL` and returns rows-affected, so a caller that gets 0
//!   knows another sweep already released this execution and must not mint a
//!   second set of jobs for it.
//!
//! Everything here is connection-level so a caller can compose it into one write
//! transaction. Release in particular must land in the same transaction as the
//! job creation that follows it: the window between the two halves is exactly
//! when the execution has `released_at` set and no jobs, which the projection
//! reads as `complete`.
//!
//! `last_outcome` is a plain string at this layer. The typed outcome vocabulary
//! and the resolvers that produce it live in `cairn-core`'s
//! `execution::gates::vocabulary`.

use cairn_db::sql::{placeholders, IN_LIST_CHUNK};
use cairn_db::turso::{params, Connection, Value};

use crate::models::ExecutionGate;
use crate::storage::{DbResult, RowExt};

const GATE_COLUMNS: &str = "id, execution_id, seq, predicate_json, satisfied_at, \
                            last_outcome, foreclosed_at, foreclosed_reason, created_at, detail";

fn gate_from_row(row: &cairn_db::turso::Row) -> DbResult<ExecutionGate> {
    Ok(ExecutionGate {
        id: row.text(0)?,
        execution_id: row.text(1)?,
        seq: row.i64(2)?,
        predicate_json: row.text(3)?,
        satisfied_at: row.opt_i64(4)?,
        last_outcome: row.opt_text(5)?,
        foreclosed_at: row.opt_i64(6)?,
        foreclosed_reason: row.opt_text(7)?,
        created_at: row.i64(8)?,
        detail: row.opt_text(9)?,
    })
}

/// File `predicates` as this execution's gates, in the order given, and return
/// the minted gate ids.
///
/// `seq` is the index in `predicates`, so authored order survives into every
/// later listing regardless of how rows come back off disk.
pub async fn insert_gates_conn(
    conn: &Connection,
    execution_id: &str,
    predicates: &[String],
    now: i64,
) -> DbResult<Vec<String>> {
    let mut ids = Vec::with_capacity(predicates.len());
    for (seq, predicate_json) in predicates.iter().enumerate() {
        let id = cairn_common::ids::mint_child(execution_id);
        conn.execute(
            "INSERT INTO execution_gates (
                 id, execution_id, seq, predicate_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id.as_str(),
                execution_id,
                seq as i64,
                predicate_json.as_str(),
                now
            ],
        )
        .await?;
        ids.push(id);
    }
    Ok(ids)
}

/// This execution's gates in authored order.
pub async fn list_gates_conn(
    conn: &Connection,
    execution_id: &str,
) -> DbResult<Vec<ExecutionGate>> {
    let sql = format!(
        "SELECT {GATE_COLUMNS} FROM execution_gates WHERE execution_id = ?1 ORDER BY seq, id"
    );
    let mut rows = conn.query(&sql, params![execution_id]).await?;
    let mut gates = Vec::new();
    while let Some(row) = rows.next().await? {
        gates.push(gate_from_row(&row)?);
    }
    Ok(gates)
}

/// The gates of `n` executions, by execution then authored order.
fn gates_for_executions_sql(n: usize) -> String {
    format!(
        "SELECT {GATE_COLUMNS} FROM execution_gates
          WHERE execution_id IN ({})
          ORDER BY execution_id, seq, id",
        placeholders(n)
    )
}

/// The gates of every execution in `execution_ids`, grouped by execution and
/// in authored order within each. One query per [`IN_LIST_CHUNK`] executions;
/// an execution with no gates is simply absent from the map.
pub async fn list_gates_for_executions_conn(
    conn: &Connection,
    execution_ids: &[String],
) -> DbResult<std::collections::HashMap<String, Vec<ExecutionGate>>> {
    let mut grouped = std::collections::HashMap::<String, Vec<ExecutionGate>>::new();
    // A chunk boundary never splits one execution's gates: each id lands in
    // exactly one chunk, so the per-execution order survives the split.
    for chunk in execution_ids.chunks(IN_LIST_CHUNK) {
        let bound: Vec<Value> = chunk.iter().map(|id| Value::Text(id.clone())).collect();
        let mut rows = conn
            .query(&gates_for_executions_sql(chunk.len()), bound)
            .await?;
        while let Some(row) = rows.next().await? {
            let gate = gate_from_row(&row)?;
            grouped
                .entry(gate.execution_id.clone())
                .or_default()
                .push(gate);
        }
    }
    Ok(grouped)
}

/// Record what this gate's predicate most recently resolved to, latching
/// satisfaction when it is satisfied.
///
/// `satisfied_at` is `Some(now)` only for a satisfying resolution. The
/// `COALESCE` is the latch: a second satisfying observation leaves the original
/// timestamp alone, and a later refutation passes `None`, which cannot clear an
/// already-latched gate. `last_outcome` always reflects the newest resolution,
/// because that is what a census renders.
pub async fn record_outcome_conn(
    conn: &Connection,
    gate_id: &str,
    outcome: &str,
    satisfied_at: Option<i64>,
) -> DbResult<()> {
    conn.execute(
        "UPDATE execution_gates
            SET last_outcome = ?2,
                satisfied_at = COALESCE(satisfied_at, ?3)
          WHERE id = ?1",
        params![gate_id, outcome, satisfied_at],
    )
    .await?;
    Ok(())
}

/// Latch this gate as permanently unsatisfiable, returning whether this call was
/// the one that latched it.
///
/// Guarded on `foreclosed_at IS NULL`, so a repeating sweep gets `true` exactly
/// once — which is what lets the foreclosure wake be sent from the same place
/// without a second dedupe mechanism.
pub async fn latch_foreclosure_conn(
    conn: &Connection,
    gate_id: &str,
    foreclosed_at: i64,
    reason: &str,
) -> DbResult<bool> {
    let affected = conn
        .execute(
            "UPDATE execution_gates
                SET foreclosed_at = ?2, foreclosed_reason = ?3
              WHERE id = ?1 AND foreclosed_at IS NULL",
            params![gate_id, foreclosed_at, reason],
        )
        .await?;
    Ok(affected > 0)
}

/// Release a held execution, returning the number of rows affected.
///
/// This is the exactly-once release primitive. The `released_at IS NULL` guard
/// means a caller that gets 0 lost the race and must return without minting
/// jobs; a caller that gets 1 owns the transition and is the one that starts the
/// execution. Same shape as the wake cursor guard in `wakes::conditions`.
///
/// `override_json` is `Some` only for a manual early release, recording the
/// acting principal. Its presence is what distinguishes "the predicates came
/// true" from "someone decided not to wait". A `None` keeps an override already
/// recorded while the execution was still held: an early release the execution
/// queue's caps outlasted is still why its authored gates stopped holding it.
///
/// A SUPERSEDED execution is never released: a later launch on its issue took
/// the attempt while it was still waiting, so starting it now would put two
/// attempts on one issue. Nor is one whose issue has already been merged or
/// closed: that work was resolved without it.
pub async fn release_execution_conn(
    conn: &Connection,
    execution_id: &str,
    released_at: i64,
    override_json: Option<&str>,
) -> DbResult<u64> {
    // Not `awaiting_release_sql!`: removing the last gate releases in the same
    // transaction, after the gate rows are gone, so the gate count is the
    // caller's to authorize. Liveness is the shared half.
    Ok(conn
        .execute(
            concat!(
                "UPDATE executions
                    SET released_at = ?2,
                        release_override_json = COALESCE(?3, release_override_json)
                  WHERE id = ?1 AND released_at IS NULL
                    AND EXISTS (SELECT 1 FROM executions e WHERE e.id = ?1 AND ",
                cairn_db::live_execution_sql!(),
                ")"
            ),
            params![execution_id, released_at, override_json],
        )
        .await?)
}

/// `(authored gates, unsatisfied authored gates)` for one execution as stored
/// right now, read in the caller's transaction. This is what authorizes an
/// automatic release: the sweep's own view of the gates is a snapshot from
/// before it resolved them, and a gate patch may have landed since.
///
/// Capacity gates are not counted. They are level conditions re-decided inside
/// the release itself, never latched in advance, so they cannot authorize or
/// refuse a release from here.
pub async fn gate_counts_conn(conn: &Connection, execution_id: &str) -> DbResult<(i64, i64)> {
    let mut rows = conn
        .query(
            concat!(
                "SELECT COUNT(*), COALESCE(SUM(g.satisfied_at IS NULL), 0)
                   FROM execution_gates g WHERE g.execution_id = ?1 AND NOT (",
                cairn_db::capacity_gate_sql!(),
                ")"
            ),
            params![execution_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok((row.i64(0)?, row.i64(1)?)),
        None => Ok((0, 0)),
    }
}

/// Remove one gate. Only meaningful while its execution is held; the caller
/// enforces that, in the same transaction, because removing a gate from a
/// released execution would rewrite why it started.
pub async fn delete_gate_conn(conn: &Connection, gate_id: &str) -> DbResult<()> {
    conn.execute(
        "DELETE FROM execution_gates WHERE id = ?1",
        params![gate_id],
    )
    .await?;
    Ok(())
}

/// Rewrite a surviving gate's authored position after a gate patch, so `seq`
/// stays the order the author's latest list gave.
pub async fn set_gate_seq_conn(conn: &Connection, gate_id: &str, seq: i64) -> DbResult<()> {
    conn.execute(
        "UPDATE execution_gates SET seq = ?2 WHERE id = ?1",
        params![gate_id, seq],
    )
    .await?;
    Ok(())
}

/// Where capacity gates sit in an execution's gate order: after every authored
/// gate, so a census lists what the filer wrote first.
pub const CAPACITY_GATE_SEQ_BASE: i64 = 1_000;

/// A queue key is its second scaled by this, so keys minted in one second stay
/// distinct and still sort with the second they were minted in. Spelled as a
/// literal in [`queue_key_sql!`] and in the migration that backfilled the keys.
pub const QUEUE_KEY_SCALE: i64 = 1_000_000;

/// The newest queue key in this database. Ordered down to it rather than read
/// as `MAX(queue_key)`, which Turso answers by walking the whole index; NULL
/// keys (authored gates) sort last descending.
const LAST_QUEUE_KEY_SQL: &str =
    "SELECT queue_key FROM execution_gates ORDER BY queue_key DESC LIMIT 1";

/// The key a capacity row filed at `now` takes: its second's, or one past the
/// newest key in this database when that is later, so every key is unique.
async fn mint_queue_key_conn(conn: &Connection, now: i64) -> DbResult<i64> {
    let last = match conn.query(LAST_QUEUE_KEY_SQL, ()).await?.next().await? {
        Some(row) => row.opt_i64(0)?,
        None => None,
    };
    let scaled = now.saturating_mul(QUEUE_KEY_SCALE);
    Ok(last.map_or(scaled, |last| scaled.max(last.saturating_add(1))))
}

/// Record that a capacity cap holds this execution now: its row exists, reads
/// `no_evidence`, and carries `detail` as its facts (`CapacityHold` JSON).
/// Inserted on first block, with a freshly minted `queue_key` (its place in
/// line) and `created_at` (its age held). A later block rewrites the same row's
/// facts and touches neither, so a re-decided hold keeps its place. Returns
/// whether anything changed.
pub async fn hold_capacity_gate_conn(
    conn: &Connection,
    execution_id: &str,
    predicate_json: &str,
    seq: i64,
    detail: &str,
    now: i64,
) -> DbResult<bool> {
    let updated = conn
        .execute(
            "UPDATE execution_gates
                SET last_outcome = 'no_evidence', detail = ?3
              WHERE execution_id = ?1 AND predicate_json = ?2 AND satisfied_at IS NULL
                AND (last_outcome IS NOT 'no_evidence' OR detail IS NOT ?3)",
            params![execution_id, predicate_json, detail],
        )
        .await?;
    if updated > 0 {
        return Ok(true);
    }
    let exists = conn
        .query(
            "SELECT 1 FROM execution_gates WHERE execution_id = ?1 AND predicate_json = ?2",
            params![execution_id, predicate_json],
        )
        .await?
        .next()
        .await?
        .is_some();
    if exists {
        return Ok(false);
    }
    let id = cairn_common::ids::mint_child(execution_id);
    let queue_key = mint_queue_key_conn(conn, now).await?;
    conn.execute(
        "INSERT INTO execution_gates (
             id, execution_id, seq, predicate_json, last_outcome, detail, created_at, queue_key
         ) VALUES (?1, ?2, ?3, ?4, 'no_evidence', ?5, ?6, ?7)",
        params![
            id.as_str(),
            execution_id,
            seq,
            predicate_json,
            detail,
            now,
            queue_key
        ],
    )
    .await?;
    Ok(true)
}

/// Move a capacity gate to another place in its queue: a thread's own reorder
/// permutes keys its set already holds, and the operator's fleet move writes
/// one it chose free in this database, so keys stay unique either way.
pub async fn set_gate_queue_key_conn(
    conn: &Connection,
    gate_id: &str,
    queue_key: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE execution_gates SET queue_key = ?2 WHERE id = ?1",
        params![gate_id, queue_key],
    )
    .await?;
    Ok(())
}

/// Shift every gate whose key is at most `at_most` by `by`, in one statement:
/// the operator's fleet move opening a gap where consecutive keys left none.
/// A uniform shift of a prefix of the key order keeps every relative order
/// (fleet, thread, release), and every key in this database stays distinct
/// because rows above `at_most` are untouched and a downward shift cannot
/// reach them. Latched rows shift with the rest, so uniqueness holds over the
/// whole column; a fresh mint still lands past the newest key. Touches only
/// `execution_gates`, through `idx_execution_gates_queue_key`. Returns how
/// many rows moved.
pub async fn shift_queue_keys_conn(conn: &Connection, at_most: i64, by: i64) -> DbResult<u64> {
    Ok(conn
        .execute(SHIFT_QUEUE_KEYS_SQL, params![at_most, by])
        .await?)
}

/// The statement [`shift_queue_keys_conn`] runs.
pub const SHIFT_QUEUE_KEYS_SQL: &str =
    "UPDATE execution_gates SET queue_key = queue_key + ?2 WHERE queue_key <= ?1";

/// Record that a capacity cap no longer blocks this execution, WITHOUT
/// latching it: a free slot can be taken again before the release, so the row
/// stays (keeping the execution's place in line) and reads `satisfied` until
/// the release latches it or a later decision blocks it again. Its last facts
/// stay, so a census still knows the row for the cap it is. Returns whether
/// anything changed.
pub async fn clear_capacity_gate_conn(
    conn: &Connection,
    execution_id: &str,
    predicate_json: &str,
) -> DbResult<bool> {
    Ok(conn
        .execute(
            "UPDATE execution_gates SET last_outcome = 'satisfied'
              WHERE execution_id = ?1 AND predicate_json = ?2 AND satisfied_at IS NULL
                AND last_outcome IS NOT 'satisfied'",
            params![execution_id, predicate_json],
        )
        .await?
        > 0)
}

/// Latch every capacity gate of an execution being released, in the release's
/// own transaction: from here on its caps are history, not holds.
pub async fn latch_capacity_gates_conn(
    conn: &Connection,
    execution_id: &str,
    now: i64,
) -> DbResult<()> {
    conn.execute(
        concat!(
            "UPDATE execution_gates
                SET satisfied_at = COALESCE(satisfied_at, ?2), last_outcome = 'satisfied'
              WHERE id IN (SELECT g.id FROM execution_gates g WHERE g.execution_id = ?1 AND ",
            cairn_db::capacity_gate_sql!(),
            ")"
        ),
        params![execution_id, now],
    )
    .await?;
    Ok(())
}

/// Rewrite a capacity gate's facts, as its place in line moves.
pub async fn set_gate_detail_conn(conn: &Connection, gate_id: &str, detail: &str) -> DbResult<()> {
    conn.execute(
        "UPDATE execution_gates SET detail = ?2 WHERE id = ?1",
        params![gate_id, detail],
    )
    .await?;
    Ok(())
}

/// One capacity gate that is holding a still-releasable execution right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockingCapacityGate {
    pub gate_id: String,
    pub execution_id: String,
    pub issue_id: Option<String>,
    pub predicate_json: String,
    pub detail: Option<String>,
    /// Its place in line on this cap: the queue's one order key.
    pub queue_key: i64,
}

/// [`list_blocking_capacity_gates_conn`]'s statement. It starts from the
/// unsatisfied gates and reaches each execution by an equality, which the
/// forced `idx_executions_hold_facts` serves without reading the row.
pub const BLOCKING_CAPACITY_GATES_SQL: &str = concat!(
    "SELECT g.id, g.execution_id, e.issue_id, g.predicate_json, g.detail,
            COALESCE(g.queue_key, g.created_at * 1000000)
       FROM execution_gates g
       JOIN executions e INDEXED BY idx_executions_hold_facts ON e.id = g.execution_id
      WHERE g.satisfied_at IS NULL AND g.last_outcome = 'no_evidence' AND ",
    cairn_db::capacity_gate_sql!(),
    " AND ",
    cairn_db::awaiting_release_sql!(),
    " AND NOT ",
    cairn_db::awaits_authored_gate_sql!(),
    " ORDER BY 6, g.id"
);

/// Every capacity gate currently blocking an execution that can still start
/// and is not waiting on an authored gate of its own (such an execution has no
/// place in line until its own gates clear).
pub async fn list_blocking_capacity_gates_conn(
    conn: &Connection,
) -> DbResult<Vec<BlockingCapacityGate>> {
    let mut rows = conn.query(BLOCKING_CAPACITY_GATES_SQL, ()).await?;
    let mut gates = Vec::new();
    while let Some(row) = rows.next().await? {
        gates.push(BlockingCapacityGate {
            gate_id: row.text(0)?,
            execution_id: row.text(1)?,
            issue_id: row.opt_text(2)?,
            predicate_json: row.text(3)?,
            detail: row.opt_text(4)?,
            queue_key: row.i64(5)?,
        });
    }
    Ok(gates)
}

/// How long execution `e` has been held (see [`HeldExecution::held_since`]):
/// the oldest capacity row's `created_at`, else the oldest gate's. An age for
/// display; the queue's order is [`queue_key_sql!`].
#[macro_export]
macro_rules! held_since_sql {
    () => {
        concat!(
            "COALESCE((SELECT MIN(g.created_at) FROM execution_gates g WHERE g.execution_id = e.id AND ",
            cairn_db::capacity_gate_sql!(),
            "), (SELECT MIN(first_gate.created_at) FROM execution_gates first_gate WHERE first_gate.execution_id = e.id))"
        )
    };
}

/// Execution `e`'s place in the queue (see [`HeldExecution::queue_key`]): its
/// earliest BLOCKING capacity row's `queue_key` (the place a decision counts),
/// else its earliest capacity row's (one whose cap cleared, which it wakes on
/// when that cap blocks again), else its oldest gate's `created_at` on the same
/// scale. The literal is [`QUEUE_KEY_SCALE`].
#[macro_export]
macro_rules! queue_key_sql {
    () => {
        concat!(
            "COALESCE((SELECT MIN(g.queue_key) FROM execution_gates g WHERE g.execution_id = e.id AND g.satisfied_at IS NULL AND g.last_outcome = 'no_evidence' AND ",
            cairn_db::capacity_gate_sql!(),
            "), (SELECT MIN(g.queue_key) FROM execution_gates g WHERE g.execution_id = e.id AND ",
            cairn_db::capacity_gate_sql!(),
            "), (SELECT MIN(first_gate.created_at) * 1000000 FROM execution_gates first_gate WHERE first_gate.execution_id = e.id))"
        )
    };
}

/// Execution `e`'s place on the queue PAGE, which lists the fleet's order:
/// the key of its unlatched `fleet_slot` row, blocking or dormant, else
/// [`queue_key_sql!`]. It differs from [`queue_key_sql!`] (the earliest
/// blocking key on EITHER cap) when an execution's thread key is older than
/// its fleet key: a thread-held child that got its thread key at t=100 and its
/// fleet row at t=500 is released on the fleet after a fleet-only hold from
/// t=300, so a page numbered by the earliest key would show it ahead of that
/// hold and the number would be false. The page and the operator's move
/// resolve positions against this one key.
#[macro_export]
macro_rules! fleet_order_key_sql {
    () => {
        concat!(
            "COALESCE((SELECT MIN(fleet_row.queue_key) FROM execution_gates fleet_row WHERE fleet_row.execution_id = e.id AND fleet_row.satisfied_at IS NULL AND json_extract(fleet_row.predicate_json, '$.fact') = 'fleet_slot'), ",
            $crate::queue_key_sql!(),
            ")"
        )
    };
}

/// One execution that is held right now: it has gates and has not been
/// released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldExecution {
    pub execution_id: String,
    pub issue_id: Option<String>,
    pub issue_number: Option<i64>,
    pub seq: Option<i64>,
    /// How long it has been held: when it first held on a CAPACITY cap, or,
    /// with no capacity row yet, its oldest gate's `created_at`. The age a
    /// surface shows; never the order.
    pub held_since: i64,
    /// Its place in the queue: its earliest capacity row's `queue_key`, unique
    /// within its database, or, with no capacity row yet, its oldest gate's
    /// `created_at` on the key's scale. The sweep releases in this order across
    /// every database, ties broken by database, then issue number (none last),
    /// then seq: only executions with no capacity row can tie, as a burst
    /// filed in one second does. An execution that waited on its `after:`
    /// gates takes a capacity key only when those clear, so this agrees with
    /// the place in line a capacity row renders.
    pub queue_key: i64,
}

/// [`list_held_executions_conn`]'s statement. Its row source is EVERY gated
/// execution ([`cairn_db::gated_execution_source_sql!`]), not only those with
/// an unsatisfied gate: one whose gates are all satisfied but which is still
/// unreleased (a crash between satisfying and releasing) is still held, and
/// the sweep must see it.
pub const HELD_EXECUTIONS_SQL: &str = concat!(
    "SELECT e.id, e.issue_id, i.number, e.seq, ",
    held_since_sql!(),
    ", ",
    queue_key_sql!(),
    "
       FROM executions e
       LEFT JOIN issues i ON i.id = e.issue_id
      WHERE ",
    cairn_db::gated_execution_source_sql!(),
    " AND ",
    cairn_db::awaiting_release_sql!(),
    " ORDER BY 6, i.number IS NULL, i.number, e.seq, e.id"
);

/// Every held execution that can still be released, in queue order (see
/// [`HeldExecution::queue_key`]). The set is
/// [`cairn_db::awaiting_release_sql!`] — the same predicate the gate census
/// lists as held — so a superseded execution, or one whose issue is already
/// resolved, is excluded here AND rendered as `won't start` there.
/// [`release_execution_conn`] would refuse it anyway.
///
/// The order is load-bearing. Admission ranks by `queued_at`, so the order a
/// burst is released in becomes the order it is admitted in; a nondeterministic
/// order would turn a plan's implicit priority into a coin flip.
pub async fn list_held_executions_conn(conn: &Connection) -> DbResult<Vec<HeldExecution>> {
    let mut rows = conn.query(HELD_EXECUTIONS_SQL, ()).await?;
    let mut held = Vec::new();
    while let Some(row) = rows.next().await? {
        held.push(HeldExecution {
            execution_id: row.text(0)?,
            issue_id: row.opt_text(1)?,
            issue_number: row.opt_i64(2)?,
            seq: row.opt_i64(3)?,
            held_since: row.opt_i64(4)?.unwrap_or(0),
            queue_key: row.opt_i64(5)?.unwrap_or(0),
        });
    }
    Ok(held)
}

/// [`list_waiting_gate_predicates_conn`]'s statement. It starts from the
/// unsatisfied gates already, so it needs no row source; each execution is
/// reached by an equality the forced `idx_executions_hold_facts` serves.
pub const WAITING_GATE_PREDICATES_SQL: &str = concat!(
    "SELECT e.issue_id, g.predicate_json
       FROM execution_gates g
       JOIN executions e INDEXED BY idx_executions_hold_facts ON e.id = g.execution_id
      WHERE g.satisfied_at IS NULL
        AND e.issue_id IS NOT NULL
        AND e.id IS NOT ?1
        AND ",
    cairn_db::awaiting_release_sql!()
);

/// The unsatisfied gates of every held execution except `excluding`, each with
/// the id of the issue its execution belongs to. This is the whole edge set
/// "issue X waits on ..." that cycle refusal walks; a satisfied gate can no
/// longer hold anything, so it contributes no edge, and neither does an
/// execution that will never start.
pub async fn list_waiting_gate_predicates_conn(
    conn: &Connection,
    excluding: Option<&str>,
) -> DbResult<Vec<(String, String)>> {
    let mut rows = conn
        .query(WAITING_GATE_PREDICATES_SQL, params![excluding])
        .await?;
    let mut edges = Vec::new();
    while let Some(row) = rows.next().await? {
        edges.push((row.text(0)?, row.text(1)?));
    }
    Ok(edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    async fn seeded_db() -> LocalDb {
        let db = crate::storage::migrated_test_db("execution-gates-store.db").await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at)
               VALUES('w', 'W', 1, 1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p', 'w', 'Project', 'proj', '/tmp/repo', 1, 1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('i', 'p', 1, 'Issue', 'backlog', 'backlog', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('e', 'recipe', 'i', 'p', 'held', 1, 1);",
        )
        .await
        .unwrap();
        db
    }

    /// Every surface that lists executions renders their gates through this
    /// statement. Bound through `json_each` it could not seek; it must reach
    /// `execution_gates` by execution id, never by scanning the table
    /// (cairn/5206).
    #[tokio::test]
    async fn gates_for_executions_seek_by_execution() {
        let db = seeded_db().await;
        let sql = format!("EXPLAIN QUERY PLAN {}", gates_for_executions_sql(3));
        let plan: Vec<String> = db
            .read(move |conn| {
                let sql = sql.clone();
                Box::pin(async move {
                    let bound = vec![Value::Text("e".into()); 3];
                    let mut rows = conn.query(&sql, bound).await?;
                    let mut plan = Vec::new();
                    while let Some(row) = rows.next().await? {
                        plan.push(row.text(3)?);
                    }
                    Ok(plan)
                })
            })
            .await
            .unwrap();
        assert!(
            plan.iter()
                .any(|line| line.contains("idx_execution_gates_execution")),
            "the gate read seeks idx_execution_gates_execution: {plan:?}"
        );
        assert!(
            !plan.iter().any(|line| line.starts_with("SCAN")),
            "the gate read must not scan: {plan:?}"
        );
    }

    #[tokio::test]
    async fn gates_for_many_executions_group_across_chunks() {
        let db = seeded_db().await;
        insert(&db, vec!["{\"a\":1}".into(), "{\"b\":2}".into()]).await;
        // More ids than one bound list carries, with the gated one in the
        // second chunk, so the read must span chunks and still group.
        let mut ids: Vec<String> = (0..IN_LIST_CHUNK).map(|n| format!("absent-{n}")).collect();
        ids.push("e".into());
        let grouped = db
            .read(move |conn| {
                let ids = ids.clone();
                Box::pin(async move { list_gates_for_executions_conn(conn, &ids).await })
            })
            .await
            .unwrap();
        assert_eq!(grouped.len(), 1);
        let seqs: Vec<i64> = grouped["e"].iter().map(|gate| gate.seq).collect();
        assert_eq!(seqs, vec![0, 1]);
    }

    async fn gates(db: &LocalDb) -> Vec<ExecutionGate> {
        db.read(|conn| Box::pin(async move { list_gates_conn(conn, "e").await }))
            .await
            .unwrap()
    }

    async fn insert(db: &LocalDb, predicates: Vec<String>) -> Vec<String> {
        db.write(move |conn| {
            let predicates = predicates.clone();
            Box::pin(async move { insert_gates_conn(conn, "e", &predicates, 10).await })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn gates_round_trip_in_authored_order() {
        let db = seeded_db().await;
        let ids = insert(
            &db,
            vec![
                "{\"fact\":\"issue_resolution\"}".to_string(),
                "{\"fact\":\"clock\"}".to_string(),
            ],
        )
        .await;

        let gates = gates(&db).await;
        assert_eq!(gates.len(), 2);
        assert_eq!(
            gates.iter().map(|g| g.seq).collect::<Vec<_>>(),
            vec![0, 1],
            "authored order is durable, not insertion-race order"
        );
        assert_eq!(gates[0].id, ids[0]);
        assert_eq!(gates[0].predicate_json, "{\"fact\":\"issue_resolution\"}");
        assert!(gates
            .iter()
            .all(|g| !g.is_satisfied() && !g.is_foreclosed()));
    }

    /// The one-way door. A gate observed satisfied and then refuted keeps its
    /// original `satisfied_at`, so a check that goes green and later red cannot
    /// un-start an execution.
    #[tokio::test]
    async fn satisfaction_latches_and_a_later_refutation_cannot_clear_it() {
        let db = seeded_db().await;
        let ids = insert(&db, vec!["{\"fact\":\"check_verdict\"}".to_string()]).await;
        let gate_id = ids[0].clone();

        for (outcome, at) in [
            ("satisfied", Some(50_i64)),
            ("satisfied", Some(60)),
            ("refuted", None),
        ] {
            let (gate_id, outcome) = (gate_id.clone(), outcome.to_string());
            db.write(move |conn| {
                let (gate_id, outcome) = (gate_id.clone(), outcome.clone());
                Box::pin(async move { record_outcome_conn(conn, &gate_id, &outcome, at).await })
            })
            .await
            .unwrap();
        }

        let gates = gates(&db).await;
        assert_eq!(
            gates[0].satisfied_at,
            Some(50),
            "the FIRST satisfying observation is the latch"
        );
        assert_eq!(
            gates[0].last_outcome.as_deref(),
            Some("refuted"),
            "the newest resolution is still what the census renders"
        );
    }

    /// Foreclosure latches once, so a sweep that runs every tick wakes the
    /// filing thread exactly once.
    #[tokio::test]
    async fn foreclosure_latches_exactly_once() {
        let db = seeded_db().await;
        let ids = insert(&db, vec!["{\"fact\":\"issue_resolution\"}".to_string()]).await;
        let gate_id = ids[0].clone();

        let mut latched = Vec::new();
        for reason in ["closed without merging", "closed without merging"] {
            let (gate_id, reason) = (gate_id.clone(), reason.to_string());
            latched.push(
                db.write(move |conn| {
                    let (gate_id, reason) = (gate_id.clone(), reason.clone());
                    Box::pin(
                        async move { latch_foreclosure_conn(conn, &gate_id, 70, &reason).await },
                    )
                })
                .await
                .unwrap(),
            );
        }

        assert_eq!(latched, vec![true, false]);
        let gates = gates(&db).await;
        assert_eq!(gates[0].foreclosed_at, Some(70));
        assert_eq!(
            gates[0].foreclosed_reason.as_deref(),
            Some("closed without merging")
        );
        assert!(
            !gates[0].is_satisfied(),
            "foreclosure holds; it does not release"
        );
    }

    /// The exactly-once release primitive: the second caller gets 0 rows and so
    /// knows not to mint a second set of jobs.
    #[tokio::test]
    async fn release_is_guarded_and_affects_one_row_exactly_once() {
        let db = seeded_db().await;
        insert(&db, vec!["{\"fact\":\"clock\"}".to_string()]).await;

        let first = db
            .write(|conn| {
                Box::pin(async move { release_execution_conn(conn, "e", 80, None).await })
            })
            .await
            .unwrap();
        let second = db
            .write(|conn| {
                Box::pin(async move {
                    release_execution_conn(conn, "e", 90, Some("{\"principal\":\"op\"}")).await
                })
            })
            .await
            .unwrap();

        assert_eq!((first, second), (1, 0));
        assert_eq!(
            db.query_opt_i64("SELECT released_at FROM executions WHERE id = 'e'", ())
                .await
                .unwrap(),
            Some(80),
            "the losing caller must not overwrite the winner's release"
        );
        assert_eq!(
            db.query_opt_text(
                "SELECT release_override_json FROM executions WHERE id = 'e'",
                ()
            )
            .await
            .unwrap(),
            None,
            "a lost race must not retroactively brand a release as an override"
        );
    }

    async fn queue_keys(db: &LocalDb) -> Vec<(String, String, i64)> {
        db.read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT execution_id, json_extract(predicate_json, '$.fact'), queue_key
                           FROM execution_gates ORDER BY queue_key",
                        (),
                    )
                    .await?;
                let mut keys = Vec::new();
                while let Some(row) = rows.next().await? {
                    keys.push((row.text(0)?, row.text(1)?, row.i64(2)?));
                }
                Ok(keys)
            })
        })
        .await
        .unwrap()
    }

    async fn hold(
        db: &LocalDb,
        execution_id: &'static str,
        fact: &'static str,
        detail: &'static str,
        now: i64,
    ) {
        db.write(move |conn| {
            Box::pin(async move {
                let predicate = format!("{{\"fact\":\"{fact}\"}}");
                hold_capacity_gate_conn(conn, execution_id, &predicate, 1000, detail, now).await
            })
        })
        .await
        .unwrap();
    }

    /// cairn/5848: a burst filed in one second still gets one distinct key per
    /// hold, in filing order; a re-decided hold keeps its key; a later second
    /// starts at its own scale.
    #[tokio::test]
    async fn queue_keys_are_unique_within_a_second_and_kept_across_decisions() {
        let db = seeded_db().await;
        db.execute_script(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('i2', 'p', 2, 'Second', 'backlog', 'backlog', 'none', 1, 1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('e2', 'recipe', 'i2', 'p', 'held', 1, 1);",
        )
        .await
        .unwrap();
        hold(&db, "e", "fleet_slot", "{}", 50).await;
        hold(&db, "e", "thread_focus", "{}", 50).await;
        hold(&db, "e2", "fleet_slot", "{}", 50).await;
        // Re-decided with new facts, a second later: the same key.
        hold(&db, "e", "fleet_slot", "{\"place\":1}", 51).await;
        hold(&db, "e2", "thread_focus", "{}", 60).await;
        assert_eq!(
            queue_keys(&db).await,
            vec![
                (
                    "e".to_string(),
                    "fleet_slot".to_string(),
                    50 * QUEUE_KEY_SCALE
                ),
                (
                    "e".to_string(),
                    "thread_focus".to_string(),
                    50 * QUEUE_KEY_SCALE + 1
                ),
                (
                    "e2".to_string(),
                    "fleet_slot".to_string(),
                    50 * QUEUE_KEY_SCALE + 2
                ),
                (
                    "e2".to_string(),
                    "thread_focus".to_string(),
                    60 * QUEUE_KEY_SCALE
                ),
            ]
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT created_at FROM execution_gates WHERE execution_id = 'e' AND seq = 1000
                  AND json_extract(predicate_json, '$.fact') = 'fleet_slot'",
                ()
            )
            .await
            .unwrap(),
            Some(50),
            "the age held is never rewritten"
        );
    }

    /// Minting reads the newest key on every capacity filing, so it must
    /// reach it through the index, never by sorting the table.
    #[tokio::test]
    async fn the_newest_queue_key_is_read_through_its_index() {
        let db = seeded_db().await;
        let plan: Vec<String> = db
            .read(|conn| {
                Box::pin(async move {
                    let mut rows = conn
                        .query(&format!("EXPLAIN QUERY PLAN {LAST_QUEUE_KEY_SQL}"), ())
                        .await?;
                    let mut plan = Vec::new();
                    while let Some(row) = rows.next().await? {
                        plan.push(row.text(3)?);
                    }
                    Ok(plan)
                })
            })
            .await
            .unwrap();
        assert!(
            plan.iter()
                .any(|line| line.contains("idx_execution_gates_queue_key")),
            "the newest key is read through idx_execution_gates_queue_key: {plan:?}"
        );
        assert!(
            !plan
                .iter()
                .any(|line| line.to_ascii_uppercase().contains("SORT")
                    || line.to_ascii_uppercase().contains("B-TREE")),
            "the newest key is not found by sorting: {plan:?}"
        );
    }

    #[test]
    fn the_queue_key_macro_spells_the_scale() {
        assert!(queue_key_sql!().contains(&format!("* {QUEUE_KEY_SCALE} ")));
    }

    /// Gates belong to their execution: deleting it takes them with it.
    #[tokio::test]
    async fn gates_cascade_with_their_execution() {
        let db = seeded_db().await;
        insert(&db, vec!["{\"fact\":\"clock\"}".to_string()]).await;
        db.execute("DELETE FROM executions WHERE id = 'e'", ())
            .await
            .unwrap();
        assert_eq!(
            db.query_opt_i64("SELECT COUNT(*) FROM execution_gates", ())
                .await
                .unwrap(),
            Some(0)
        );
    }
}
