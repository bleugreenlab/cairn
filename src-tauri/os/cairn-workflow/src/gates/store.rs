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

use cairn_db::turso::{params, Connection};

use crate::models::ExecutionGate;
use crate::storage::{DbResult, RowExt};

const GATE_COLUMNS: &str = "id, execution_id, seq, predicate_json, satisfied_at, \
                            last_outcome, foreclosed_at, foreclosed_reason, created_at";

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

/// The gates of every execution in `execution_ids`, grouped by execution and
/// in authored order within each. One query however many executions a list
/// surface shows; an execution with no gates is simply absent from the map.
pub async fn list_gates_for_executions_conn(
    conn: &Connection,
    execution_ids: &[String],
) -> DbResult<std::collections::HashMap<String, Vec<ExecutionGate>>> {
    let mut grouped = std::collections::HashMap::<String, Vec<ExecutionGate>>::new();
    if execution_ids.is_empty() {
        return Ok(grouped);
    }
    let ids_json = serde_json::to_string(execution_ids).map_err(|error| {
        crate::storage::DbError::internal(format!("failed to serialize execution ids: {error}"))
    })?;
    let sql = format!(
        "SELECT {GATE_COLUMNS} FROM execution_gates
          WHERE execution_id IN (SELECT value FROM json_each(?1))
          ORDER BY execution_id, seq, id"
    );
    let mut rows = conn.query(&sql, params![ids_json]).await?;
    while let Some(row) = rows.next().await? {
        let gate = gate_from_row(&row)?;
        grouped
            .entry(gate.execution_id.clone())
            .or_default()
            .push(gate);
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
/// true" from "someone decided not to wait".
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
                    SET released_at = ?2, release_override_json = ?3
                  WHERE id = ?1 AND released_at IS NULL
                    AND EXISTS (SELECT 1 FROM executions e WHERE e.id = ?1 AND ",
                cairn_db::live_execution_sql!(),
                ")"
            ),
            params![execution_id, released_at, override_json],
        )
        .await?)
}

/// `(gates, unsatisfied gates)` for one execution as stored right now, read in
/// the caller's transaction. This is what authorizes an automatic release: the
/// sweep's own view of the gates is a snapshot from before it resolved them,
/// and a gate patch may have landed since.
pub async fn gate_counts_conn(conn: &Connection, execution_id: &str) -> DbResult<(i64, i64)> {
    let mut rows = conn
        .query(
            "SELECT COUNT(*), COALESCE(SUM(satisfied_at IS NULL), 0)
               FROM execution_gates WHERE execution_id = ?1",
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

/// One execution that is held right now: it has gates and has not been
/// released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldExecution {
    pub execution_id: String,
    pub issue_id: Option<String>,
    pub issue_number: Option<i64>,
    pub seq: Option<i64>,
}

/// Every held execution that can still be released, in RELEASE order:
/// ascending issue number, then execution seq. The set is
/// [`cairn_db::awaiting_release_sql!`] — the same predicate the gate census
/// lists as held — so a superseded execution, or one whose issue is already
/// resolved, is excluded here AND rendered as `won't start` there.
/// [`release_execution_conn`] would refuse it anyway.
///
/// The order is load-bearing. Admission ranks by `queued_at`, so the order a
/// burst is released in becomes the order it is admitted in; a nondeterministic
/// order would turn a plan's implicit priority into a coin flip.
pub async fn list_held_executions_conn(conn: &Connection) -> DbResult<Vec<HeldExecution>> {
    let mut rows = conn
        .query(
            concat!(
                "SELECT e.id, e.issue_id, i.number, e.seq
                   FROM executions e
                   LEFT JOIN issues i ON i.id = e.issue_id
                  WHERE ",
                cairn_db::awaiting_release_sql!(),
                " ORDER BY i.number IS NULL, i.number, e.seq, e.id"
            ),
            (),
        )
        .await?;
    let mut held = Vec::new();
    while let Some(row) = rows.next().await? {
        held.push(HeldExecution {
            execution_id: row.text(0)?,
            issue_id: row.opt_text(1)?,
            issue_number: row.opt_i64(2)?,
            seq: row.opt_i64(3)?,
        });
    }
    Ok(held)
}

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
        .query(
            concat!(
                "SELECT e.issue_id, g.predicate_json
                   FROM execution_gates g
                   JOIN executions e ON e.id = g.execution_id
                  WHERE g.satisfied_at IS NULL
                    AND e.issue_id IS NOT NULL
                    AND e.id IS NOT ?1
                    AND ",
                cairn_db::awaiting_release_sql!()
            ),
            params![excluding],
        )
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
