//! The delegated-packet store: one row of `delegated_packets` per packet.
//!
//! Packets used to live inline in `executions.snapshot`, and an UPDATE writes
//! the whole new row into the MVCC logical log, so every packet's creation and
//! every status change rewrote the snapshot with all of its execution's
//! packets. On a thread host with 1,154 packets one status change wrote 107 MB
//! of log (cairn/5597). Here a status change rewrites one row of a few KB.
//!
//! **Read-through.** Executions written by earlier builds still hold packets
//! in the snapshot's `delegatedPackets` array until the backfill
//! ([`move_inline_packets`]) moves them. Every read here merges the table with
//! that array, and a table row wins over an inline packet with the same id, so
//! a packet is never missing while the backfill is partway: it is read from
//! wherever it is.
//!
//! **Copy-on-write.** Nothing writes the inline array any more. A change to a
//! packet that is only inline first copies it into the table at its array
//! index, then changes the row, in one transaction. The inline copy is then
//! stale but shadowed, and the backfill strips it.
//!
//! **The prompt is held only while pending.** Every status change away from
//! `pending` clears `problem_statement`: expansion has copied it into the
//! context node, and a call's prompt went to its own launch. A retried spawn
//! matches by [`prompt_digest`] instead.

use std::collections::HashSet;

use cairn_db::storage::TrackedConnection;
use cairn_db::turso::params;
use sha2::{Digest, Sha256};

use crate::models::{DelegatedStatus, DelegatedWorkPacket};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

const SELECT: &str = "SELECT seq, id, parent_job_id, parent_turn_id, result_artifact_job_id, \
     status, materialized_node_ids, problem_statement, spec, created_at FROM delegated_packets";

/// Packet keys stored in their own columns, so never in `spec`.
const COLUMN_KEYS: &[&str] = &[
    "id",
    "parentJobId",
    "parentTurnId",
    "resultArtifactJobId",
    "status",
    "materializedNodeIds",
    "problemStatement",
    "createdAt",
];

/// SHA-256 of a prompt, lowercase hex.
pub fn prompt_digest(prompt: &str) -> String {
    Sha256::digest(prompt.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn status_str(status: &DelegatedStatus) -> &'static str {
    match status {
        DelegatedStatus::Pending => "pending",
        DelegatedStatus::Materialized => "materialized",
        DelegatedStatus::Running => "running",
        DelegatedStatus::Completed => "completed",
        DelegatedStatus::Failed => "failed",
        DelegatedStatus::Cancelled => "cancelled",
    }
}

fn internal(error: impl std::fmt::Display) -> DbError {
    DbError::internal(error.to_string())
}

/// A packet as its row stores it.
struct Encoded {
    id: String,
    parent_job_id: String,
    parent_turn_id: Option<String>,
    result_artifact_job_id: Option<String>,
    status: &'static str,
    materialized_node_ids: String,
    problem_statement: Option<String>,
    spec: String,
    created_at: i64,
}

fn encode(packet: &DelegatedWorkPacket) -> DbResult<Encoded> {
    let mut value = serde_json::to_value(packet).map_err(internal)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| internal("a delegated packet serializes to an object"))?;
    for key in COLUMN_KEYS {
        object.remove(*key);
    }
    // A packet copied from a snapshot predates the digest; give it one while
    // its prompt is still at hand, so a retried spawn still matches it.
    if !object.contains_key("promptDigest") {
        if let Some(prompt) = &packet.problem_statement {
            object.insert("promptDigest".into(), prompt_digest(prompt).into());
        }
    }
    let pending = packet.status == DelegatedStatus::Pending;
    Ok(Encoded {
        id: packet.id.clone(),
        parent_job_id: packet.parent_job_id.clone(),
        parent_turn_id: packet.parent_turn_id.clone(),
        result_artifact_job_id: packet.result_artifact_job_id.clone(),
        status: status_str(&packet.status),
        materialized_node_ids: serde_json::to_string(&packet.materialized_node_ids)
            .map_err(internal)?,
        problem_statement: packet.problem_statement.clone().filter(|_| pending),
        spec: serde_json::to_string(&value).map_err(internal)?,
        created_at: packet.created_at,
    })
}

fn decode(row: &cairn_db::turso::Row) -> DbResult<(i64, DelegatedWorkPacket)> {
    let seq = row.i64(0)?;
    let mut spec: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&row.text(8)?).map_err(internal)?;
    spec.insert("id".into(), row.text(1)?.into());
    spec.insert("parentJobId".into(), row.text(2)?.into());
    if let Some(turn) = row.opt_text(3)? {
        spec.insert("parentTurnId".into(), turn.into());
    }
    if let Some(job) = row.opt_text(4)? {
        spec.insert("resultArtifactJobId".into(), job.into());
    }
    spec.insert("status".into(), row.text(5)?.into());
    spec.insert(
        "materializedNodeIds".into(),
        serde_json::from_str(&row.text(6)?).map_err(internal)?,
    );
    if let Some(prompt) = row.opt_text(7)? {
        spec.insert("problemStatement".into(), prompt.into());
    }
    spec.insert("createdAt".into(), row.i64(9)?.into());
    let packet = serde_json::from_value(serde_json::Value::Object(spec)).map_err(internal)?;
    Ok((seq, packet))
}

async fn insert_row(
    conn: &TrackedConnection,
    execution_id: &str,
    seq: i64,
    packet: &DelegatedWorkPacket,
    or_ignore: bool,
) -> DbResult<u64> {
    let row = encode(packet)?;
    let verb = if or_ignore {
        "INSERT OR IGNORE"
    } else {
        "INSERT"
    };
    let sql = format!(
        "{verb} INTO delegated_packets (execution_id, id, seq, parent_job_id, parent_turn_id, \
         result_artifact_job_id, status, materialized_node_ids, problem_statement, spec, \
         created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
    );
    Ok(conn
        .execute(
            &sql,
            params![
                execution_id,
                row.id,
                seq,
                row.parent_job_id,
                row.parent_turn_id,
                row.result_artifact_job_id,
                row.status,
                row.materialized_node_ids,
                row.problem_statement,
                row.spec,
                row.created_at
            ],
        )
        .await?)
}

#[track_caller]
fn query_rows<'a, C, P>(
    conn: &'a C,
    sql: &'a str,
    params: P,
) -> impl std::future::Future<Output = DbResult<Vec<(i64, DelegatedWorkPacket)>>> + Send + 'a
where
    C: cairn_db::storage::QueryConn + ?Sized,
    P: cairn_db::turso::IntoParams + Send + 'a,
{
    let rows = conn.query_rows(sql, params);
    async move {
        let mut rows = rows.await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(decode(&row)?);
        }
        Ok(out)
    }
}

/// The packets still inline in `execution_id`'s snapshot, in array order.
///
/// The same on-read migration a snapshot load applies is applied to each.
pub async fn inline_packets(
    conn: &TrackedConnection,
    execution_id: &str,
) -> DbResult<Vec<DelegatedWorkPacket>> {
    let mut rows = conn
        .query(
            "SELECT json_extract(snapshot, '$.delegatedPackets') FROM executions WHERE id = ?1",
            (execution_id,),
        )
        .await?;
    let Some(json) = rows.next().await?.map(|row| row.opt_text(0)).transpose()? else {
        return Ok(Vec::new());
    };
    let Some(json) = json else {
        return Ok(Vec::new());
    };
    let mut packets: Vec<DelegatedWorkPacket> =
        serde_json::from_str(&json).map_err(|e| DbError::Row(format!("inline packets: {e}")))?;
    for packet in &mut packets {
        crate::snapshot_migrate::migrate_packet_on_read(packet);
    }
    Ok(packets)
}

/// Every packet of `execution_id`, in creation order, table rows shadowing
/// inline packets with the same id.
pub async fn list(
    conn: &TrackedConnection,
    execution_id: &str,
) -> DbResult<Vec<DelegatedWorkPacket>> {
    let mut packets = query_rows(
        conn,
        &format!("{SELECT} WHERE execution_id = ?1"),
        (execution_id,),
    )
    .await?;
    let stored: HashSet<String> = packets.iter().map(|(_, p)| p.id.clone()).collect();
    for (index, packet) in inline_packets(conn, execution_id)
        .await?
        .into_iter()
        .enumerate()
    {
        if !stored.contains(&packet.id) {
            packets.push((index as i64, packet));
        }
    }
    packets.sort_by_key(|(seq, _)| *seq);
    Ok(packets.into_iter().map(|(_, packet)| packet).collect())
}

/// [`list`] in its own read.
pub async fn list_db(db: &LocalDb, execution_id: &str) -> DbResult<Vec<DelegatedWorkPacket>> {
    let execution_id = execution_id.to_string();
    db.read(|conn| Box::pin(async move { list(conn, &execution_id).await }))
        .await
}

async fn stored(
    conn: &TrackedConnection,
    execution_id: &str,
    packet_id: &str,
) -> DbResult<Option<DelegatedWorkPacket>> {
    Ok(query_rows(
        conn,
        &format!("{SELECT} WHERE execution_id = ?1 AND id = ?2"),
        (execution_id, packet_id),
    )
    .await?
    .into_iter()
    .next()
    .map(|(_, packet)| packet))
}

/// One packet by id, from the table or, failing that, the snapshot.
pub async fn get(
    conn: &TrackedConnection,
    execution_id: &str,
    packet_id: &str,
) -> DbResult<Option<DelegatedWorkPacket>> {
    if let Some(packet) = stored(conn, execution_id, packet_id).await? {
        return Ok(Some(packet));
    }
    Ok(inline_packets(conn, execution_id)
        .await?
        .into_iter()
        .find(|packet| packet.id == packet_id))
}

/// [`get`] in its own read.
pub async fn get_db(
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
) -> DbResult<Option<DelegatedWorkPacket>> {
    let (execution_id, packet_id) = (execution_id.to_string(), packet_id.to_string());
    db.read(|conn| Box::pin(async move { get(conn, &execution_id, &packet_id).await }))
        .await
}

/// The packet whose child is `job_id`: what a child's completion resumes its
/// parent through.
pub async fn find_by_result_job(
    conn: &TrackedConnection,
    execution_id: &str,
    job_id: &str,
) -> DbResult<Option<DelegatedWorkPacket>> {
    // The predicate restates the index's, or Turso plans around it.
    let found = query_rows(
        conn,
        &format!(
            "{SELECT} INDEXED BY idx_delegated_packets_result_job \
             WHERE result_artifact_job_id = ?2 AND result_artifact_job_id IS NOT NULL \
               AND execution_id = ?1"
        ),
        (execution_id, job_id),
    )
    .await?;
    if let Some((_, packet)) = found.into_iter().next() {
        return Ok(Some(packet));
    }
    // A child's job id is set once, so an inline packet naming it has no
    // table row naming another; `get` still prefers the row if one exists.
    let Some(inline) = inline_packets(conn, execution_id)
        .await?
        .into_iter()
        .find(|packet| packet.result_artifact_job_id.as_deref() == Some(job_id))
    else {
        return Ok(None);
    };
    get(conn, execution_id, &inline.id).await
}

/// [`find_by_result_job`] in its own read.
pub async fn find_by_result_job_db(
    db: &LocalDb,
    execution_id: &str,
    job_id: &str,
) -> DbResult<Option<DelegatedWorkPacket>> {
    let (execution_id, job_id) = (execution_id.to_string(), job_id.to_string());
    db.read(|conn| Box::pin(async move { find_by_result_job(conn, &execution_id, &job_id).await }))
        .await
}

/// The packet that materialized recipe node `node_id`, if a delegation did.
pub async fn find_by_node(
    conn: &TrackedConnection,
    execution_id: &str,
    node_id: &str,
) -> DbResult<Option<DelegatedWorkPacket>> {
    let quoted = serde_json::to_string(node_id).map_err(internal)?;
    let names =
        |packet: &DelegatedWorkPacket| packet.materialized_node_ids.iter().any(|id| id == node_id);
    let found = query_rows(
        conn,
        &format!("{SELECT} WHERE execution_id = ?1 AND instr(materialized_node_ids, ?2) > 0"),
        (execution_id, quoted.as_str()),
    )
    .await?;
    if let Some((_, packet)) = found.into_iter().find(|(_, packet)| names(packet)) {
        return Ok(Some(packet));
    }
    let Some(inline) = inline_packets(conn, execution_id)
        .await?
        .into_iter()
        .find(|packet| names(packet))
    else {
        return Ok(None);
    };
    Ok(get(conn, execution_id, &inline.id)
        .await?
        .filter(|packet| names(packet)))
}

/// Append a new packet after every packet `execution_id` already has.
pub async fn insert(
    conn: &TrackedConnection,
    execution_id: &str,
    packet: &DelegatedWorkPacket,
) -> DbResult<()> {
    let mut rows = conn
        .query(
            "SELECT (SELECT COALESCE(MAX(seq) + 1, 0) FROM delegated_packets WHERE execution_id = ?1), \
                    (SELECT COALESCE(json_array_length(snapshot, '$.delegatedPackets'), 0) \
                       FROM executions WHERE id = ?1)",
            (execution_id,),
        )
        .await?;
    let (after_rows, after_inline) = match rows.next().await? {
        Some(row) => (row.i64(0)?, row.opt_i64(1)?.unwrap_or(0)),
        None => (0, 0),
    };
    drop(rows);
    insert_row(
        conn,
        execution_id,
        after_rows.max(after_inline),
        packet,
        false,
    )
    .await?;
    Ok(())
}

/// [`insert`] in its own write.
pub async fn insert_db(
    db: &LocalDb,
    execution_id: &str,
    packet: &DelegatedWorkPacket,
) -> DbResult<()> {
    let execution_id = execution_id.to_string();
    let packet = packet.clone();
    db.write(|conn| {
        let (execution_id, packet) = (execution_id.clone(), packet.clone());
        Box::pin(async move { insert(conn, &execution_id, &packet).await })
    })
    .await
}

/// The fields of a packet that change after it is created.
#[derive(Debug, Clone, Default)]
pub struct PacketChange {
    pub status: Option<DelegatedStatus>,
    pub materialized_node_ids: Option<Vec<String>>,
    /// Set only where it is unset: a packet's child is named once.
    pub result_artifact_job_id: Option<String>,
}

/// Copy a packet that is only inline into the table, at its array index.
/// `false` when the packet is in neither.
async fn ensure_stored(
    conn: &TrackedConnection,
    execution_id: &str,
    packet_id: &str,
) -> DbResult<bool> {
    if stored(conn, execution_id, packet_id).await?.is_some() {
        return Ok(true);
    }
    let inline = inline_packets(conn, execution_id).await?;
    let Some(index) = inline.iter().position(|packet| packet.id == packet_id) else {
        return Ok(false);
    };
    insert_row(conn, execution_id, index as i64, &inline[index], true).await?;
    Ok(true)
}

/// Apply `change` to one packet, wherever it is stored. `false` when the
/// execution has no such packet.
///
/// Field by field, so concurrent writers of different fields cannot undo one
/// another, which is what the inline `json_set` patch existed to guarantee.
pub async fn update(
    conn: &TrackedConnection,
    execution_id: &str,
    packet_id: &str,
    change: &PacketChange,
) -> DbResult<bool> {
    if !ensure_stored(conn, execution_id, packet_id).await? {
        return Ok(false);
    }
    if let Some(status) = &change.status {
        conn.execute(
            "UPDATE delegated_packets SET status = ?3, \
                 problem_statement = CASE WHEN ?3 = 'pending' THEN problem_statement END \
             WHERE execution_id = ?1 AND id = ?2",
            (execution_id, packet_id, status_str(status)),
        )
        .await?;
    }
    if let Some(nodes) = &change.materialized_node_ids {
        conn.execute(
            "UPDATE delegated_packets SET materialized_node_ids = ?3 \
             WHERE execution_id = ?1 AND id = ?2",
            (
                execution_id,
                packet_id,
                serde_json::to_string(nodes).map_err(internal)?,
            ),
        )
        .await?;
    }
    if let Some(job_id) = &change.result_artifact_job_id {
        conn.execute(
            "UPDATE delegated_packets SET result_artifact_job_id = ?3 \
             WHERE execution_id = ?1 AND id = ?2 AND result_artifact_job_id IS NULL",
            (execution_id, packet_id, job_id.as_str()),
        )
        .await?;
    }
    Ok(true)
}

/// [`update`] in its own write.
pub async fn update_db(
    db: &LocalDb,
    execution_id: &str,
    packet_id: &str,
    change: &PacketChange,
) -> DbResult<bool> {
    let (execution_id, packet_id) = (execution_id.to_string(), packet_id.to_string());
    let change = change.clone();
    db.write(|conn| {
        let (execution_id, packet_id, change) =
            (execution_id.clone(), packet_id.clone(), change.clone());
        Box::pin(async move { update(conn, &execution_id, &packet_id, &change).await })
    })
    .await
}

/// What [`move_inline_packets`] did to one execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineMove {
    /// The snapshot holds no inline packets.
    Nothing,
    /// This many packets are in the table and the snapshot no longer holds them.
    Moved(usize),
    /// The snapshot changed between the read and the write; nothing was stripped.
    Contended,
}

/// Move `execution_id`'s inline packets into the table and strip them from its
/// snapshot, in one short write.
///
/// The snapshot is parsed outside the write. The write copies every inline
/// packet a row does not already shadow, then replaces the snapshot only if it
/// is still the text that was parsed, so a writer that changed it in between
/// loses nothing: the move reports [`InlineMove::Contended`] and is retried.
pub async fn move_inline_packets(db: &LocalDb, execution_id: &str) -> DbResult<InlineMove> {
    let id = execution_id.to_string();
    let Some(original) = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT snapshot FROM executions WHERE id = ?1",
                        (id.as_str(),),
                    )
                    .await?;
                rows.next()
                    .await?
                    .map(|row| row.opt_text(0))
                    .transpose()
                    .map(Option::flatten)
            })
        })
        .await?
    else {
        return Ok(InlineMove::Nothing);
    };
    let mut value: serde_json::Value = serde_json::from_str(&original).map_err(internal)?;
    let Some(object) = value.as_object_mut() else {
        return Ok(InlineMove::Nothing);
    };
    let inline = match object.get("delegatedPackets") {
        Some(serde_json::Value::Array(packets)) if !packets.is_empty() => packets.clone(),
        _ => return Ok(InlineMove::Nothing),
    };
    let mut packets: Vec<DelegatedWorkPacket> =
        serde_json::from_value(serde_json::Value::Array(inline)).map_err(internal)?;
    for packet in &mut packets {
        crate::snapshot_migrate::migrate_packet_on_read(packet);
    }
    object.remove("delegatedPackets");
    let stripped = serde_json::to_string(&value).map_err(internal)?;
    let count = packets.len();
    let execution_id = execution_id.to_string();
    let replaced = db
        .write(|conn| {
            let (execution_id, packets) = (execution_id.clone(), packets.clone());
            let (original, stripped) = (original.clone(), stripped.clone());
            Box::pin(async move {
                for (index, packet) in packets.iter().enumerate() {
                    insert_row(conn, &execution_id, index as i64, packet, true).await?;
                }
                Ok(conn
                    .execute(
                        "UPDATE executions SET snapshot = ?2 WHERE id = ?1 AND snapshot = ?3",
                        (execution_id.as_str(), stripped.as_str(), original.as_str()),
                    )
                    .await?)
            })
        })
        .await?;
    Ok(if replaced == 1 {
        InlineMove::Moved(count)
    } else {
        InlineMove::Contended
    })
}

/// Log size past which the backfill folds before it moves another execution.
const BACKFILL_FOLD_AT_LOG_BYTES: u64 = 64 * 1024 * 1024;

/// Moves of one execution the backfill tries while its snapshot keeps changing
/// underneath, before it stops until the next start.
const BACKFILL_MOVE_ATTEMPTS: u32 = 5;

/// What one [`run_backfill`] call did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BackfillReport {
    /// Executions whose packets moved in this call.
    pub executions: usize,
    pub packets: usize,
    pub folds: usize,
    /// The walk has reached the end, in this call or an earlier one.
    pub completed: bool,
}

/// Move every execution's inline packets into the table, one execution per
/// write ([`move_inline_packets`]).
///
/// `delegated_packet_backfill_state` holds the walk's rowid cursor, so a
/// restart resumes rather than rescans, and its `completed_at` makes every call
/// after the walk reaches the end a single read. Between executions the log is
/// folded once it passes [`BACKFILL_FOLD_AT_LOG_BYTES`]: moving a thread host's
/// packets rewrites its snapshot once more, and a backlog of them must not pile
/// up unfolded. A failed fold, or a snapshot that keeps changing under the
/// move, ends the call without advancing past that execution, and the next
/// start retries it.
///
/// Nothing waits on this: every read in this module reads inline packets
/// through until they move.
pub async fn run_backfill(db: &LocalDb) -> DbResult<BackfillReport> {
    let mut report = BackfillReport::default();
    let (mut cursor, completed_at) = db
        .query_one(
            "SELECT cursor_rowid, completed_at FROM delegated_packet_backfill_state WHERE id = 1",
            (),
            |row| Ok((row.i64(0)?, row.opt_i64(1)?)),
        )
        .await?;
    if completed_at.is_some() {
        report.completed = true;
        return Ok(report);
    }
    while let Some((rowid, execution_id)) = next_inline_execution(db, cursor).await? {
        let mut outcome = move_inline_packets(db, &execution_id).await?;
        let mut attempt = 1;
        while outcome == InlineMove::Contended && attempt < BACKFILL_MOVE_ATTEMPTS {
            tokio::time::sleep(std::time::Duration::from_millis(50 << attempt)).await;
            outcome = move_inline_packets(db, &execution_id).await?;
            attempt += 1;
        }
        let moved = match outcome {
            InlineMove::Moved(count) => count,
            InlineMove::Nothing => 0,
            InlineMove::Contended => {
                log::warn!(
                    "delegated packet backfill: the snapshot of {execution_id} kept changing; \
                     resuming at it next start"
                );
                return Ok(report);
            }
        };
        db.execute(
            "UPDATE delegated_packet_backfill_state \
             SET cursor_rowid = ?1, moved = moved + ?2 WHERE id = 1",
            (rowid, moved as i64),
        )
        .await?;
        cursor = rowid;
        if moved > 0 {
            report.executions += 1;
            report.packets += moved;
        }
        if db.logical_log_bytes() >= BACKFILL_FOLD_AT_LOG_BYTES {
            let fold = db
                .checkpoint(
                    3,
                    std::time::Duration::from_millis(50),
                    crate::storage::DrainBudget::PRODUCTION,
                )
                .await?;
            report.folds += 1;
            if !fold.succeeded() {
                log::warn!("delegated packet backfill: fold failed ({fold}); resuming next start");
                return Ok(report);
            }
        }
    }
    db.execute(
        "UPDATE delegated_packet_backfill_state SET completed_at = unixepoch() WHERE id = 1",
        (),
    )
    .await?;
    report.completed = true;
    Ok(report)
}

/// The first execution past rowid `after` whose snapshot may hold inline
/// packets. The match is textual, so it can name an execution with none, which
/// [`move_inline_packets`] then reports as [`InlineMove::Nothing`].
async fn next_inline_execution(db: &LocalDb, after: i64) -> DbResult<Option<(i64, String)>> {
    db.read(move |conn| {
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT rowid, id FROM executions \
                     WHERE rowid > ?1 AND instr(snapshot, '\"delegatedPackets\"') > 0 \
                     ORDER BY rowid LIMIT 1",
                    (after,),
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some((row.i64(0)?, row.text(1)?))),
                None => Ok(None),
            }
        })
    })
    .await
}

#[cfg(test)]
mod tests;
