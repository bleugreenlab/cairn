//! Durable REPL persistence: the `job_repls` identity row and the
//! `repl_exchanges` transcript.
//!
//! A REPL has two halves, and the old design conflated them. The **namespace** is
//! a live interpreter heap — it lives in the child process and there is no
//! persisting it. The **logical REPL** is an entity with a lifecycle, a language,
//! a dependency set, and a history, and that half is a row.
//!
//! So `cairn:~/repl/<slug>` is the durable identity and the interpreter process is
//! a *generation* within it. A REPL that dies stays visible with its fate
//! recorded; creating it again starts the next generation and continues the same
//! transcript. Because REPL state is finally table-backed, every REPL surface in
//! the GUI invalidates through the one app-wide `db-change` path that every other
//! entity already uses, instead of a pane-scoped listener that dies with the pane.

use uuid::Uuid;

use super::{ReplBinding, ReplExchange, ReplExchangeStatus, ReplLang, ReplOrigin};
use crate::storage::{DbResult, LocalDb, RowExt};

/// Whether a REPL's current generation still has a live interpreter process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplRowStatus {
    Running,
    Exited,
}

impl ReplRowStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Exited => "exited",
        }
    }

    fn parse(raw: &str) -> Self {
        match raw {
            "running" => Self::Running,
            _ => Self::Exited,
        }
    }
}

/// Why a REPL's interpreter process is no longer running. Recorded on the row so
/// death — the most significant event in a REPL's life, because it means the
/// accumulated namespace is gone — reads as a fact rather than an absence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplExitReason {
    /// Stopped deliberately: an agent `delete`, the UI's Stop, or node teardown.
    Closed,
    /// The child exited on its own (or its pipes closed) mid-send.
    Died,
    /// No response within the send timeout; the session was killed.
    Timeout,
    /// A framed line that did not parse as the protocol.
    Protocol,
    /// The runner restarted. No REPL survives that, so any row still marked
    /// running is a lie and is reaped at startup.
    HostRestart,
}

impl ReplExitReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Died => "died",
            Self::Timeout => "timeout",
            Self::Protocol => "protocol",
            Self::HostRestart => "host_restart",
        }
    }

    fn parse(raw: &str) -> Option<Self> {
        Some(match raw {
            "closed" => Self::Closed,
            "died" => Self::Died,
            "timeout" => Self::Timeout,
            "protocol" => Self::Protocol,
            "host_restart" => Self::HostRestart,
            _ => return None,
        })
    }

    /// The agent-facing explanation of what this fate means for the namespace.
    pub fn describe(self) -> &'static str {
        match self {
            Self::Closed => "stopped",
            Self::Died => "the interpreter died",
            Self::Timeout => "a send timed out and the interpreter was killed",
            Self::Protocol => "the interpreter desynchronized and was killed",
            Self::HostRestart => "the runner restarted",
        }
    }
}

/// The durable `job_repls` row: one logical REPL, across every process that has
/// served it.
#[derive(Debug, Clone)]
pub struct ReplRecord {
    pub id: String,
    pub job_id: String,
    pub project_id: Option<String>,
    pub slug: String,
    pub interpreter: ReplLang,
    /// Python-only preloaded packages, inherited by a resume when the create
    /// payload omits them.
    pub deps: Vec<String>,
    /// Which process incarnation is current; a resume bumps it.
    pub generation: i64,
    pub status: ReplRowStatus,
    pub exit_reason: Option<ReplExitReason>,
    /// Namespace snapshot as of the last settled send (python only). A namespace
    /// can only change as the result of a send, so this is the live namespace
    /// while the process runs and the last known one after it dies.
    pub bindings: Vec<ReplBinding>,
    pub created_at: i64,
    pub exited_at: Option<i64>,
    /// Load-time projection: how many exchanges the transcript holds.
    pub exchange_count: i64,
}

/// Listing projection for the facet strip and tab list: the row plus the newest
/// exchange's status, which is what colors a REPL's facet icon.
#[derive(Debug, Clone)]
pub struct ReplListing {
    pub job_id: String,
    pub slug: String,
    pub interpreter: ReplLang,
    pub created_at: i64,
    pub generation: i64,
    pub status: ReplRowStatus,
    pub exit_reason: Option<ReplExitReason>,
    pub last_status: Option<ReplExchangeStatus>,
}

const RECORD_COLUMNS: &str = "id, job_id, project_id, slug, interpreter, deps, generation, status, \
                              exit_reason, bindings, created_at, exited_at, \
                              (SELECT COUNT(*) FROM repl_exchanges e WHERE e.repl_id = job_repls.id)";

fn map_record(row: &cairn_db::turso::Row) -> DbResult<ReplRecord> {
    Ok(ReplRecord {
        id: row.text(0)?,
        job_id: row.text(1)?,
        project_id: row.opt_text(2)?,
        slug: row.text(3)?,
        interpreter: ReplLang::parse(&row.text(4)?).unwrap_or(ReplLang::Python),
        deps: row
            .opt_text(5)?
            .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
            .unwrap_or_default(),
        generation: row.i64(6)?,
        status: ReplRowStatus::parse(&row.text(7)?),
        exit_reason: row.opt_text(8)?.as_deref().and_then(ReplExitReason::parse),
        bindings: row
            .opt_text(9)?
            .and_then(|raw| serde_json::from_str::<Vec<ReplBinding>>(&raw).ok())
            .unwrap_or_default(),
        created_at: row.i64(10)?,
        exited_at: row.opt_i64(11)?,
        exchange_count: row.i64(12)?,
    })
}

fn map_listing(row: &cairn_db::turso::Row) -> DbResult<ReplListing> {
    Ok(ReplListing {
        job_id: row.text(0)?,
        slug: row.text(1)?,
        interpreter: ReplLang::parse(&row.text(2)?).unwrap_or(ReplLang::Python),
        created_at: row.i64(3)?,
        generation: row.i64(4)?,
        status: ReplRowStatus::parse(&row.text(5)?),
        exit_reason: row.opt_text(6)?.as_deref().and_then(ReplExitReason::parse),
        last_status: row
            .opt_text(7)?
            .as_deref()
            .and_then(ReplExchangeStatus::parse),
    })
}

const LISTING_SQL: &str = "SELECT job_id, slug, interpreter, created_at, generation, status, exit_reason, \
     (SELECT e.status FROM repl_exchanges e WHERE e.repl_id = job_repls.id ORDER BY e.seq DESC LIMIT 1) \
     FROM job_repls";

/// The durable row for `(job, slug)`, running or exited.
pub async fn load(db: &LocalDb, job_id: &str, slug: &str) -> DbResult<Option<ReplRecord>> {
    db.query_opt(
        format!("SELECT {RECORD_COLUMNS} FROM job_repls WHERE job_id = ?1 AND slug = ?2 LIMIT 1"),
        (job_id.to_string(), slug.to_string()),
        map_record,
    )
    .await
}

/// Every REPL belonging to one job, newest last.
pub async fn list_for_job(db: &LocalDb, job_id: &str) -> DbResult<Vec<ReplListing>> {
    db.query_all(
        format!("{LISTING_SQL} WHERE job_id = ?1 ORDER BY created_at"),
        (job_id.to_string(),),
        map_listing,
    )
    .await
}

/// Every REPL on this host (the global facet source).
pub async fn list_all(db: &LocalDb) -> DbResult<Vec<ReplListing>> {
    db.query_all(
        format!("{LISTING_SQL} ORDER BY created_at"),
        (),
        map_listing,
    )
    .await
}

/// Open a generation for `(job, slug)`: insert the row on first create, or bump
/// an existing row back to `running` on a resume. Returns the row as it now
/// stands. The caller has already resolved `interpreter`/`deps` (inheriting them
/// from an exited row when the payload omitted them), so this writes what it is
/// given rather than deciding.
pub async fn begin(
    db: &LocalDb,
    job_id: &str,
    project_id: Option<&str>,
    slug: &str,
    interpreter: ReplLang,
    deps: &[String],
) -> DbResult<ReplRecord> {
    let now = chrono::Utc::now().timestamp_millis();
    let deps_json = serde_json::to_string(deps).unwrap_or_else(|_| "[]".to_string());
    let updated = db
        .execute(
            "UPDATE job_repls
                SET generation = generation + 1,
                    status = 'running',
                    exit_reason = NULL,
                    exited_at = NULL,
                    bindings = NULL,
                    interpreter = ?3,
                    deps = ?4
              WHERE job_id = ?1 AND slug = ?2",
            (job_id, slug, interpreter.label(), deps_json.as_str()),
        )
        .await?;
    if updated == 0 {
        db.execute(
            "INSERT INTO job_repls
                 (id, job_id, project_id, slug, interpreter, deps, generation, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, 'running', ?7)",
            (
                Uuid::new_v4().to_string(),
                job_id.to_string(),
                project_id.map(str::to_string),
                slug.to_string(),
                interpreter.label().to_string(),
                deps_json,
                now,
            ),
        )
        .await?;
    }
    load(db, job_id, slug)
        .await?
        .ok_or_else(|| crate::storage::DbError::Row(format!("REPL row vanished for '{slug}'")))
}

/// Record that a *specific generation's* process is gone.
///
/// The `generation` guard is what makes this safe to call after an `await`. A
/// caller that owned the session when it started tearing it down may no longer
/// own the REPL by the time this lands: stopping a child yields, and a resume can
/// install the next generation in that window. Keyed only by `id`, a stale
/// teardown would then mark the *live* replacement exited. Conditioning on the
/// generation the caller actually held makes such an update match zero rows.
///
/// Idempotent within a generation: an already-exited row keeps its first recorded
/// fate, because the first one is the one that explains where the namespace went.
/// Returns the number of rows updated, so a caller can tell whether its own
/// generation was still the current one.
pub async fn mark_exited(
    db: &LocalDb,
    repl_id: &str,
    generation: i64,
    reason: ReplExitReason,
) -> DbResult<u64> {
    db.execute(
        "UPDATE job_repls SET status = 'exited', exit_reason = ?3, exited_at = ?4
          WHERE id = ?1 AND generation = ?2 AND status = 'running'",
        (
            repl_id,
            generation,
            reason.as_str(),
            chrono::Utc::now().timestamp_millis(),
        ),
    )
    .await
}

/// Discard an exited REPL outright, transcript included. Turso enforces foreign
/// keys on INSERT but not on DELETE, so the child rows are swept explicitly.
pub async fn remove(db: &LocalDb, repl_id: &str) -> DbResult<()> {
    db.execute("DELETE FROM repl_exchanges WHERE repl_id = ?1", (repl_id,))
        .await?;
    db.execute("DELETE FROM job_repls WHERE id = ?1", (repl_id,))
        .await?;
    Ok(())
}

/// Persist the namespace snapshot that rode back on a generation's eval response.
///
/// Generation-fenced for the same reason as [`mark_exited`]: a send can receive
/// its response and then be delayed before this write, and a close-plus-resume in
/// that window installs the next generation with its bindings cleared. Keyed only
/// by `id`, the late write would paint the dead generation's namespace onto the
/// live one, so a read would confidently list variables that do not exist in the
/// fresh interpreter — worse than reporting nothing.
pub async fn set_bindings(
    db: &LocalDb,
    repl_id: &str,
    generation: i64,
    bindings: &[ReplBinding],
) -> DbResult<u64> {
    let blob = serde_json::to_string(bindings).unwrap_or_else(|_| "[]".to_string());
    db.execute(
        "UPDATE job_repls SET bindings = ?3 WHERE id = ?1 AND generation = ?2",
        (repl_id, generation, blob.as_str()),
    )
    .await
}

/// The next exchange sequence for a REPL. Monotonic across generations, so the
/// transcript reads as one continuous history.
pub async fn next_seq(db: &LocalDb, repl_id: &str) -> DbResult<u64> {
    let max = db
        .query_opt_i64(
            "SELECT MAX(seq) FROM repl_exchanges WHERE repl_id = ?1",
            (repl_id.to_string(),),
        )
        .await?;
    Ok(max.map(|seq| seq as u64 + 1).unwrap_or(0))
}

/// Insert the submitted (pending) exchange, so a send in flight is visible
/// rather than invisible until it lands.
pub async fn insert_pending(
    db: &LocalDb,
    repl_id: &str,
    project_id: Option<&str>,
    exchange: &ReplExchange,
) -> DbResult<u64> {
    db.execute(
        "INSERT INTO repl_exchanges
             (id, repl_id, project_id, generation, seq, origin, code, status, truncated, started_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', 0, ?8)",
        (
            Uuid::new_v4().to_string(),
            repl_id.to_string(),
            project_id.map(str::to_string),
            exchange.generation,
            exchange.seq as i64,
            exchange.origin.as_str().to_string(),
            exchange.code.clone(),
            exchange.started_at,
        ),
    )
    .await
}

/// Replace the pending row with its settled form, in place — one row per send,
/// not two.
pub async fn settle(db: &LocalDb, repl_id: &str, exchange: &ReplExchange) -> DbResult<u64> {
    db.execute(
        "UPDATE repl_exchanges
            SET status = ?3, value = ?4, stdout = ?5, stderr = ?6, error = ?7, note = ?8,
                duration_ms = ?9, truncated = ?10
          WHERE repl_id = ?1 AND seq = ?2",
        (
            repl_id.to_string(),
            exchange.seq as i64,
            exchange.status.as_str().to_string(),
            exchange.value.clone(),
            exchange.stdout.clone(),
            exchange.stderr.clone(),
            exchange.error.clone(),
            exchange.note.clone(),
            exchange.duration_ms.map(|ms| ms as i64),
            i64::from(exchange.truncated),
        ),
    )
    .await
}

/// The transcript for one REPL, oldest first, spanning every generation.
pub async fn history(db: &LocalDb, repl_id: &str) -> DbResult<Vec<ReplExchange>> {
    db.query_all(
        "SELECT seq, origin, code, started_at, duration_ms, status, value, stdout, stderr, error, \
         note, truncated, generation FROM repl_exchanges WHERE repl_id = ?1 ORDER BY seq",
        (repl_id.to_string(),),
        |row| {
            Ok(ReplExchange {
                seq: row.i64(0)? as u64,
                generation: row.i64(12)?,
                origin: ReplOrigin::parse(&row.text(1)?).unwrap_or(ReplOrigin::Agent),
                code: row.text(2)?,
                started_at: row.i64(3)?,
                duration_ms: row.opt_i64(4)?.map(|ms| ms as u64),
                status: ReplExchangeStatus::parse(&row.text(5)?)
                    .unwrap_or(ReplExchangeStatus::Protocol),
                value: row.opt_text(6)?,
                stdout: row.opt_text(7)?,
                stderr: row.opt_text(8)?,
                error: row.opt_text(9)?,
                note: row.opt_text(10)?,
                truncated: row.i64(11)? != 0,
            })
        },
    )
    .await
}

/// Startup reap. No REPL survives a runner restart, so the registry is empty by
/// definition and any `running` row is a lie. This marks rather than reconciles
/// (the deliberate divergence from terminal recovery): a recovered REPL process
/// would have an empty namespace and is worth nothing, so there is nothing to
/// reconcile — only to record. Returns how many rows were corrected.
pub async fn reap_orphans(db: &LocalDb) -> DbResult<u64> {
    let now = chrono::Utc::now().timestamp_millis();
    let repls = db
        .execute(
            "UPDATE job_repls SET status = 'exited', exit_reason = 'host_restart', exited_at = ?1
              WHERE status = 'running'",
            (now,),
        )
        .await?;
    db.execute(
        "UPDATE repl_exchanges SET status = 'died', error = ?1 WHERE status = 'pending'",
        ("The runner restarted while this send was in flight; the REPL's namespace is gone.",),
    )
    .await?;
    Ok(repls)
}
