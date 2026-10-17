//! The contradiction wake: when the decayed weight of a tenet's contradictions
//! that no wake has counted yet crosses [`WAKE_THRESHOLD`], its proposing
//! thread is woken once to supersede it or correct course.
//!
//! Eligibility is per row, never a timestamp edge. A wake consumes exactly the
//! rows it counted and listed, stamping each one's `consumed_by_wake` with the
//! wake's message id in the same write that queues the message. A
//! contradiction that replicates in late, from a machine that was offline or
//! whose push was delayed, arrives unconsumed and counts toward the next wake,
//! however old its `evaluated_at`. Settling is not a delivery watermark, so no
//! clock comparison can stand in for "no wake has counted this".
//!
//! Only the machine owning the newest unconsumed contradiction fires, and only
//! once that row is [`WAKE_SETTLE_SECS`] old, by which time every online
//! replica has pulled the same rows and agrees on which is newest, so two
//! online machines never fire over the same rows. Consumption marks land on
//! the evidence rows' own primary keys, so two machines consuming different
//! rows never overwrite each other under last-writer-wins. The message id
//! derives from the consumed ids: a wake over the same rows is the same
//! message row, and finding it already sent re-stamps those rows rather than
//! sending it twice.
//!
//! `tenets.contradiction_alerted_at` records when the last wake fired, for
//! display only. A recipient that cannot be reached yet (a closed thread, a
//! thread with no run) leaves every row unconsumed, so the next sweep retries.

use crate::messages::queued::DeliveryUrgency;
use cairn_db::turso::{params, Connection};

use super::worker::EvidenceDeps;
use super::{stable_id, utc_date, HALF_LIFE_DAYS, WAKE_SETTLE_SECS, WAKE_THRESHOLD};
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// One contradiction counted toward a wake.
#[derive(Debug, Clone, PartialEq)]
pub struct Contradiction {
    pub id: String,
    pub source_uri: String,
    pub confidence: f64,
    pub owner_device_id: String,
    pub evaluated_at: i64,
}

/// A contradiction's weight at `now`: its confidence, halved every
/// [`HALF_LIFE_DAYS`].
pub fn decayed(confidence: f64, evaluated_at: i64, now: i64) -> f64 {
    let age_days = (now - evaluated_at).max(0) as f64 / 86_400.0;
    confidence * 0.5f64.powf(age_days / HALF_LIFE_DAYS)
}

/// The summed decayed weight of `rows` at `now`.
pub fn weight(rows: &[Contradiction], now: i64) -> f64 {
    rows.iter()
        .map(|row| decayed(row.confidence, row.evaluated_at, now))
        .sum()
}

/// The newest row, by `evaluated_at` then `id`: the row whose owner fires.
fn newest(rows: &[Contradiction]) -> Option<&Contradiction> {
    rows.iter()
        .max_by(|a, b| (a.evaluated_at, &a.id).cmp(&(b.evaluated_at, &b.id)))
}

/// Whether `device` may fire a wake over `rows` at `now`.
fn due(rows: &[Contradiction], device: &str, now: i64) -> bool {
    newest(rows).is_some_and(|row| {
        row.owner_device_id == device && row.evaluated_at <= now - WAKE_SETTLE_SECS
    }) && weight(rows, now) >= WAKE_THRESHOLD
}

/// The message id of a wake over `rows`: derived from the set of rows it
/// consumes, independent of order, so the same wake is always the same row.
fn wake_id(tenet_id: &str, rows: &[Contradiction]) -> String {
    let mut ids: Vec<&str> = rows.iter().map(|row| row.id.as_str()).collect();
    ids.sort_unstable();
    stable_id(
        "tenet-contradiction",
        &format!("{tenet_id}:{}", ids.join(",")),
    )
}

/// The tenet's contradictions no wake has consumed yet, oldest first.
async fn unconsumed_conn(conn: &Connection, tenet_id: &str) -> DbResult<Vec<Contradiction>> {
    let mut rows = conn
        .query(
            "SELECT id, source_uri, confidence, owner_device_id, evaluated_at
             FROM tenet_evidence
             WHERE tenet_id = ?1 AND sign = 'contradicts' AND consumed_by_wake IS NULL
             ORDER BY evaluated_at, id",
            params![tenet_id],
        )
        .await?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().await? {
        out.push(Contradiction {
            id: row.text(0)?,
            source_uri: row.text(1)?,
            confidence: row.f64(2)?,
            owner_device_id: row.text(3)?,
            evaluated_at: row.i64(4)?,
        });
    }
    Ok(out)
}

/// Stamp exactly `rows` as consumed by `wake`. Every row must still be
/// unconsumed; otherwise the write fails, rolling back with it, rather than
/// let one message claim rows another wake counted.
async fn consume_conn(
    conn: &Connection,
    rows: &[Contradiction],
    wake: &str,
    now: i64,
) -> DbResult<()> {
    for row in rows {
        let stamped = conn
            .execute(
                "UPDATE tenet_evidence SET consumed_by_wake = ?1, consumed_at = ?2
                 WHERE id = ?3 AND consumed_by_wake IS NULL",
                params![wake, now, row.id.as_str()],
            )
            .await?;
        if stamped != 1 {
            return Err(DbError::internal(format!(
                "tenet evidence {} was consumed while wake {wake} was firing",
                row.id
            )));
        }
    }
    Ok(())
}

/// Fire every wake this machine owes, in every database. Also the retry path
/// for a wake whose recipient could not be reached before. Returns how many
/// fired.
pub async fn recheck_contradictions(deps: &dyn EvidenceDeps) -> usize {
    let device = deps.this_device();
    let mut fired = 0;
    for db in deps.dbs().await {
        let now = deps.now();
        let candidates = match db
            .query_all(
                "SELECT DISTINCT t.id
                 FROM tenets t JOIN tenet_evidence e ON e.tenet_id = t.id
                 WHERE t.status = 'approved' AND e.sign = 'contradicts'
                   AND e.consumed_by_wake IS NULL",
                (),
                |row| row.text(0),
            )
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                log::warn!("tenet contradiction sweep could not read candidates: {error}");
                continue;
            }
        };
        for tenet_id in candidates {
            // A read first, so a machine that does not own the wake never
            // opens a write for it.
            let rows = {
                let tenet_id = tenet_id.clone();
                db.read(move |conn| {
                    let tenet_id = tenet_id.clone();
                    Box::pin(async move { unconsumed_conn(conn, &tenet_id).await })
                })
                .await
            };
            match rows {
                Ok(rows) if due(&rows, &device, now) => {}
                Ok(_) => continue,
                Err(error) => {
                    log::warn!("tenet contradiction sweep could not read {tenet_id}: {error}");
                    continue;
                }
            }
            match fire(deps, &db, &tenet_id, &device, now).await {
                Ok(true) => fired += 1,
                Ok(false) => {}
                Err(error) => log::warn!("tenet contradiction wake for {tenet_id} failed: {error}"),
            }
        }
    }
    fired
}

/// Fire one tenet's wake if it is still owed, re-checking everything inside
/// one write. Reports whether a message was queued.
pub async fn fire(
    deps: &dyn EvidenceDeps,
    db: &LocalDb,
    tenet_id: &str,
    device: &str,
    now: i64,
) -> Result<bool, String> {
    let tenet_id = tenet_id.to_string();
    let device = device.to_string();
    let recipient = db
        .write(move |conn| {
            let tenet_id = tenet_id.clone();
            let device = device.clone();
            Box::pin(async move { fire_conn(conn, &tenet_id, &device, now).await })
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some(job_id) = recipient else {
        return Ok(false);
    };
    deps.nudge(&job_id);
    crate::tenets::emit_changed(deps.emitter(), "update");
    Ok(true)
}

async fn fire_conn(
    conn: &Connection,
    tenet_id: &str,
    device: &str,
    now: i64,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            &crate::tenets::read::select_sql("t.id = ?1"),
            params![tenet_id],
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let tenet = crate::tenets::read::decode(&row)?;
    drop(rows);
    if tenet.status != crate::tenets::TenetStatus::Approved {
        return Ok(None);
    }
    let contradictions = unconsumed_conn(conn, tenet_id).await?;
    if !due(&contradictions, device, now) {
        return Ok(None);
    }

    let message_id = wake_id(tenet_id, &contradictions);
    if crate::storage::query_opt_text_conn(
        conn,
        "SELECT id FROM messages WHERE id = ?1",
        params![message_id.as_str()],
    )
    .await?
    .is_some()
    {
        // This wake was already sent and listed exactly these rows; only its
        // consumption stamps are missing here. Restore them instead of
        // sending it again.
        consume_conn(conn, &contradictions, &message_id, now).await?;
        return Ok(None);
    }

    let thread_status = crate::storage::query_opt_text_conn(
        conn,
        "SELECT status FROM threads WHERE id = ?1",
        params![tenet.proposing_thread_id.as_str()],
    )
    .await?;
    let active = thread_status
        .and_then(|status| crate::models::ThreadStatus::parse(&status).ok())
        .is_some_and(|status| status.is_active());
    if !active {
        return Ok(None);
    }
    let job_id =
        crate::threads::ensure_thread_session_conn(conn, &tenet.proposing_thread_id, None).await?;
    let Some(run_id) = crate::storage::query_opt_text_conn(
        conn,
        "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC, id DESC LIMIT 1",
        params![job_id.as_str()],
    )
    .await?
    else {
        return Ok(None);
    };

    consume_conn(conn, &contradictions, &message_id, now).await?;
    conn.execute(
        "UPDATE tenets SET contradiction_alerted_at = ?1 WHERE id = ?2",
        params![now, tenet_id],
    )
    .await?;

    let project_key = crate::storage::query_opt_text_conn(
        conn,
        "SELECT key FROM projects WHERE id = ?1",
        params![tenet.project_id.as_str()],
    )
    .await?
    .unwrap_or_default();
    let content = message(
        &tenet,
        &project_key,
        &contradictions,
        tenet.contradiction_alerted_at,
        now,
    );
    cairn_attention::direct::insert_system_direct_push_with_message_id_conn(
        conn,
        &job_id,
        &run_id,
        &content,
        DeliveryUrgency::Steer,
        Some(&format!("tenet-contradiction:{tenet_id}")),
        &message_id,
    )
    .await?;
    Ok(Some(job_id))
}

/// The wake's text. It lists exactly the rows the wake consumes: on a later
/// wake that is everything the last alert did not count, which can include a
/// contradiction dated before that alert if it replicated in after it.
fn message(
    tenet: &crate::tenets::TenetRow,
    project_key: &str,
    contradictions: &[Contradiction],
    last_alerted: Option<i64>,
    now: i64,
) -> String {
    let number = tenet.number;
    let text = tenet.text().unwrap_or(&tenet.question);
    let mut out = format!(
        "Tenet #{number} is contradicted by concluded work (weight {:.2}, threshold {WAKE_THRESHOLD}).\n\n#{number} \u{2014} {text}\n",
        weight(contradictions, now)
    );
    let rejected = tenet.rejected();
    if !rejected.is_empty() {
        out.push_str(&format!("  rejected: {}\n", rejected.join("; ")));
    }
    match last_alerted {
        None => out.push_str("Contradicting evidence since approval:\n"),
        Some(at) => out.push_str(&format!(
            "Contradicting evidence not counted by the last alert ({}):\n",
            utc_date(at, "%Y-%m-%d")
        )),
    }
    for row in contradictions {
        out.push_str(&format!(
            "- contradicts {} ({:.2}, {})\n",
            row.source_uri,
            row.confidence,
            utc_date(row.evaluated_at, "%Y-%m-%d")
        ));
    }
    out.push_str(&format!(
        "Full record: {}\n\njudge: supersede (propose a successor with supersedes:{number}) or correct course",
        crate::tenets::tenet_uri(project_key, number)
    ));
    out
}
