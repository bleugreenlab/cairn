//! The one hold for a node that cannot run for want of supply.
//!
//! A hold has a [`HoldKind`]: a provider limit that landed mid-turn yields its
//! head turn and the hold owns that head (`yield`); a cold launch refused
//! before any session ran holds the pending node (`launch`); a continuation
//! refused before spawn holds the head it rests on (`resume`). Each row owns its
//! clock: the declared reset, or a bounded re-check when none is declared. A
//! change to the credential store releases holds early through the same path
//! (see [`release_on_supply_change`]). A held node is never failed: it holds no
//! fleet slot and is not placeable, and its execution stays live.

use cairn_common::ids;
use cairn_db::turso::{params, Row};
use cairn_models::supply::CapacityRefusal;
use serde::{Deserialize, Serialize};

use crate::execution::jobs::{continue_job_impl, ResumeContext};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// How a hold met its want of supply, and so what releasing it does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldKind {
    /// A provider limit landed mid-turn. The yielded head owns the wait, and
    /// release mints its successor and continues.
    Yield,
    /// A cold launch was refused before any session ran. The node waits
    /// pending and unplaceable; release makes it placeable, and its launch
    /// selects again against supply as it then stands.
    Launch,
    /// A continuation was refused before spawn. The node rests on `turn_id`;
    /// release continues it, redelivering what the refusal returned.
    Resume,
}

impl HoldKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yield => "yield",
            Self::Launch => "launch",
            Self::Resume => "resume",
        }
    }

    fn parse(raw: &str) -> DbResult<Self> {
        match raw {
            "yield" => Ok(Self::Yield),
            "launch" => Ok(Self::Launch),
            "resume" => Ok(Self::Resume),
            other => Err(DbError::internal(format!("unknown hold kind {other}"))),
        }
    }

    /// Whether this hold was armed by a refused launch rather than a limit a
    /// running session met.
    pub fn is_supply(self) -> bool {
        matches!(self, Self::Launch | Self::Resume)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderLimitHold {
    pub id: String,
    pub job_id: String,
    /// The run the hold's refusal or limit is recorded on. `None` only for a
    /// launch hold whose refusal no run has recorded yet.
    pub run_id: Option<String>,
    /// The head the hold owns: the yielded turn, or the refused one a resume
    /// rests on. `None` for a launch hold, which owns no turn.
    pub turn_id: Option<String>,
    pub kind: HoldKind,
    pub backend: String,
    pub account_id: Option<String>,
    pub retry_not_before: Option<i64>,
    pub boundary_source: String,
    pub observed_text: Option<String>,
    pub observed_count: i64,
    pub launch_attempts: i64,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub resolved_at: Option<i64>,
    /// Why automatic resolution is withheld whatever the boundary says.
    pub resume_gate: Option<String>,
    /// The account whose reset the boundary is, when not this hold's own.
    pub boundary_account_id: Option<String>,
    /// What a launch or resume hold waits for supply of: a `provider/model`,
    /// or an allotment as `[sm, md, lg]`.
    pub subject: Option<String>,
}

/// The limit landed while a tool call had no recorded result. Resuming could run
/// that call a second time, so only an explicit continue resumes the work.
pub const RESUME_GATE_TOOL_IN_FLIGHT: &str = "tool_in_flight";

/// Every automatic launch of the held work failed for a reason supply does
/// not explain. Re-checking would only fail it again, so only an explicit
/// continue resumes the work.
pub const RESUME_GATE_LAUNCH_FAILED: &str = "launch_failed";

/// How long a hold with no declared reset waits before asking again whether
/// anything can serve it. A change to the accounts asks sooner.
pub const SUPPLY_RECHECK_SECS: i64 = 900;

/// How soon a hold whose work supply can serve again, but whose fleet is
/// full, asks for a slot again. A supply change asks sooner.
pub const FLEET_SLOT_RECHECK_SECS: i64 = 60;

impl ProviderLimitHold {
    /// When this hold next asks whether its work can run, or `None` when it
    /// never does by itself (resolved, or gated on the operator).
    ///
    /// A declared reset is jittered by a stable FNV-1a hash, independent of
    /// process hash seeds and restarts, which spreads jobs released by one
    /// shared reset; a switch to another account has no shared instant to
    /// spread, so it is due as declared. With no declared reset the hold
    /// re-checks [`SUPPLY_RECHECK_SECS`] after it last learned anything.
    pub fn next_due_at(&self) -> Option<i64> {
        if self.state != "active" || self.resume_gate.is_some() {
            return None;
        }
        // A switch to another account, or a wait for a fleet slot, has no
        // shared instant to spread, and a slot goes to the oldest hold first.
        if matches!(
            self.boundary_source.as_str(),
            "account_switch" | "fleet_slot"
        ) {
            return self.retry_not_before;
        }
        let Some(at) = self.retry_not_before else {
            return self.updated_at.checked_add(SUPPLY_RECHECK_SECS);
        };
        let hash = self.id.bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
        });
        at.checked_add((hash % 121) as i64)
    }

    pub fn due_at(&self, now: i64) -> Option<i64> {
        self.next_due_at().filter(|due| *due <= now)
    }

    /// Whether a change to the accounts may release this hold now.
    pub fn releasable(&self) -> bool {
        self.state == "active" && self.resume_gate.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct ProviderLimitObservation {
    pub job_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub backend: String,
    pub account_id: Option<String>,
    pub retry_not_before: Option<i64>,
    pub boundary_source: String,
    pub observed_text: Option<String>,
    /// [`RESUME_GATE_TOOL_IN_FLIGHT`] when a tool call had no recorded result.
    pub resume_gate: Option<&'static str>,
    /// The account whose reset `retry_not_before` is, when it is not this
    /// hold's own: the earliest any account the policy may select can serve.
    /// `boundary_source` then still says how this hold's own limit was seen.
    pub boundary_account_id: Option<String>,
    pub now: i64,
}

const COLUMNS: &str = "id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source, observed_text, observed_count, launch_attempts, state, created_at, updated_at, resolved_at, resume_gate, boundary_account_id, kind, subject";

fn from_row(row: &Row) -> DbResult<ProviderLimitHold> {
    Ok(ProviderLimitHold {
        id: row.text(0)?,
        job_id: row.text(1)?,
        run_id: row.opt_text(2)?,
        turn_id: row.opt_text(3)?,
        backend: row.text(4)?,
        account_id: row.opt_text(5)?,
        retry_not_before: row.opt_i64(6)?,
        boundary_source: row.text(7)?,
        observed_text: row.opt_text(8)?,
        observed_count: row.i64(9)?,
        launch_attempts: row.i64(10)?,
        state: row.text(11)?,
        created_at: row.i64(12)?,
        updated_at: row.i64(13)?,
        resolved_at: row.opt_i64(14)?,
        resume_gate: row.opt_text(15)?,
        boundary_account_id: row.opt_text(16)?,
        kind: HoldKind::parse(&row.text(17)?)?,
        subject: row.opt_text(18)?,
    })
}

async fn load_conn(
    conn: &crate::storage::TrackedConnection,
    id: &str,
) -> DbResult<Option<ProviderLimitHold>> {
    let mut rows = conn
        .query(
            &format!("SELECT {COLUMNS} FROM provider_limit_holds WHERE id=?1"),
            (id,),
        )
        .await?;
    rows.next().await?.as_ref().map(from_row).transpose()
}

/// Yield the provider-refused head and record the hold that owns it in one
/// owning-database transaction. Both land or neither does, so a provider-yielded
/// head never exists without an active hold to resolve it. A repeated observation
/// of a head that is already provider-yielded coalesces into its hold; a stale
/// observation never acquires ownership of a newer operator turn.
pub async fn yield_and_arm_provider_limit_hold(
    db: &LocalDb,
    observation: ProviderLimitObservation,
) -> Result<ProviderLimitHold, String> {
    let timerless = matches!(
        observation.boundary_source.as_str(),
        "unknown" | "credits_depleted"
    );
    if !matches!(
        observation.boundary_source.as_str(),
        "rate_limit_event" | "result_text" | "account_switch" | "credits_depleted" | "unknown"
    ) || (observation.boundary_account_id.is_none()
        && observation.retry_not_before.is_none() != timerless)
        || (observation.boundary_account_id.is_some() && observation.retry_not_before.is_none())
    {
        return Err("provider limit boundary and source disagree".into());
    }
    if observation
        .resume_gate
        .is_some_and(|gate| gate != RESUME_GATE_TOOL_IN_FLIGHT)
    {
        return Err("unknown provider limit resume gate".into());
    }
    db.write(move |conn| {
        let observation = observation.clone();
        Box::pin(async move {
            let o = observation;
            crate::transitions::turn::yield_running_turn_conn(
                conn,
                &o.turn_id,
                crate::models::TurnYieldReason::ProviderLimit,
                o.now,
            )
            .await?;
            // Whether this observation yielded the turn or an earlier one did, the
            // head must now be exactly this provider-yielded turn.
            let mut rows = conn.query(
                "SELECT 1 FROM jobs j JOIN turns t ON t.id=j.current_turn_id
                 JOIN runs r ON r.id=?3 AND r.job_id=j.id
                 WHERE j.id=?1 AND t.id=?2 AND t.job_id=j.id
                   AND t.state='yielded' AND t.yield_reason='provider_limit'",
                params![o.job_id.as_str(), o.turn_id.as_str(), o.run_id.as_str()],
            ).await?;
            if rows.next().await?.is_none() {
                return Err(DbError::internal("provider limit no longer owns the yielded job head".to_string()));
            }
            drop(rows);
            // Any other active hold owns something this head has replaced.
            conn.execute(
                "UPDATE provider_limit_holds SET state='superseded', updated_at=?1
                 WHERE job_id=?2 AND state='active'
                   AND (kind<>'yield' OR turn_id IS NULL OR turn_id<>?3)",
                params![o.now, o.job_id.as_str(), o.turn_id.as_str()],
            ).await?;
            let mut rows = conn.query(
                "SELECT id FROM provider_limit_holds WHERE job_id=?1 AND state='active'",
                (o.job_id.as_str(),),
            ).await?;
            let existing = rows.next().await?.map(|row| row.text(0)).transpose()?;
            drop(rows);
            let id = if let Some(id) = existing {
                // NULL observations do not erase a previously declared boundary,
                // and no later observation lifts a resume gate.
                let later = "?1 IS NOT NULL AND (retry_not_before IS NULL OR ?1 > retry_not_before)";
                conn.execute(
                    &format!("UPDATE provider_limit_holds SET
                       boundary_source=CASE WHEN {later} THEN ?2 ELSE boundary_source END,
                       boundary_account_id=CASE WHEN {later} THEN ?7 ELSE boundary_account_id END,
                       retry_not_before=CASE WHEN retry_not_before IS NULL THEN ?1 WHEN ?1 IS NULL THEN retry_not_before ELSE MAX(retry_not_before, ?1) END,
                       resume_gate=COALESCE(resume_gate, ?6),
                       observed_text=?3, observed_count=observed_count+1, updated_at=?4
                     WHERE id=?5 AND state='active'"),
                    params![o.retry_not_before, o.boundary_source, o.observed_text, o.now, id.as_str(), o.resume_gate, o.boundary_account_id],
                ).await?;
                id
            } else {
                let id = ids::mint_child(&o.job_id);
                conn.execute(
                    "INSERT INTO provider_limit_holds
                     (id,job_id,run_id,turn_id,backend,account_id,retry_not_before,boundary_source,observed_text,created_at,updated_at,resume_gate,boundary_account_id,kind)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10,?11,?12,'yield')",
                    params![id.as_str(), o.job_id, o.run_id, o.turn_id, o.backend, o.account_id,
                        o.retry_not_before, o.boundary_source, o.observed_text, o.now, o.resume_gate,
                        o.boundary_account_id],
                ).await?;
                id
            };
            load_conn(conn, &id).await?.ok_or_else(|| DbError::internal("armed hold disappeared".to_string()))
        })
    }).await.map_err(|error| error.to_string())
}

/// A launch or a continuation refused for want of supply, as a hold arms it.
#[derive(Clone, Debug)]
pub struct SupplyRefusal {
    pub job_id: String,
    /// [`HoldKind::Launch`] or [`HoldKind::Resume`].
    pub kind: HoldKind,
    /// The provider the refused launch would have run on, which the hold asks
    /// again when it comes due. A launch hold asks selection instead.
    pub backend: String,
    /// The run the launch had created, when it got that far.
    pub run_id: Option<String>,
    /// The head a resume rests on. Required for a resume.
    pub turn_id: Option<String>,
    pub refusal: CapacityRefusal,
    pub now: i64,
}

/// Hold a refused launch or continuation until something can serve it.
///
/// In one transaction: the job's active hold (any earlier one this refusal
/// supersedes) records the refusal, its text, and its soonest declared reset,
/// or the bounded re-check when none is declared. A launch hold also returns
/// its job to `pending` and clears the stored selection, so the launch that
/// release allows selects again against supply as it then stands; a model the
/// job was pinned to stays on `jobs.model`, so the pin is chosen again. A
/// resume keeps its selection, because its conversation lives on the provider
/// it already chose.
///
/// The refusal's caller still returns its error. Whoever records a failed
/// start reads the active hold and records the run without failing the job
/// (see [`settle_supply_held_start`]).
pub async fn arm_supply_hold(
    db: &LocalDb,
    refused: SupplyRefusal,
) -> Result<ProviderLimitHold, String> {
    if !refused.kind.is_supply() {
        return Err("a supply hold is a launch or a resume".into());
    }
    if refused.kind == HoldKind::Resume && refused.turn_id.is_none() {
        return Err("a resume hold owns the head it rests on".into());
    }
    db.write(move |conn| {
        let r = refused.clone();
        Box::pin(async move {
            let source = if r.refusal.retry_at.is_some() { "supply_reset" } else { "recheck" };
            let mut rows = conn
                .query(
                    "SELECT id, kind FROM provider_limit_holds WHERE job_id=?1 AND state='active'",
                    (r.job_id.as_str(),),
                )
                .await?;
            let existing = match rows.next().await? {
                Some(row) => Some((row.text(0)?, row.text(1)?)),
                None => None,
            };
            drop(rows);
            let same = existing
                .as_ref()
                .filter(|(_, kind)| kind == r.kind.as_str())
                .map(|(id, _)| id.clone());
            if let (Some((id, _)), None) = (&existing, &same) {
                set_state_conn(conn, id, "superseded", r.now).await?;
            }
            let id = match same {
                Some(id) => {
                    conn.execute(
                        "UPDATE provider_limit_holds SET
                           run_id=COALESCE(?2, run_id), turn_id=COALESCE(?3, turn_id), backend=?4,
                           retry_not_before=?5, boundary_source=?6, boundary_account_id=?7,
                           observed_text=?8, observed_count=observed_count+1, updated_at=?9,
                           subject=?10
                         WHERE id=?1 AND state='active'",
                        params![id.as_str(), r.run_id.as_deref(), r.turn_id.as_deref(), r.backend.as_str(),
                            r.refusal.retry_at, source, r.refusal.retry_account.as_deref(),
                            r.refusal.reason.as_str(), r.now, r.refusal.subject.as_str()],
                    )
                    .await?;
                    id
                }
                None => {
                    let id = ids::mint_child(&r.job_id);
                    conn.execute(
                        "INSERT INTO provider_limit_holds
                         (id,job_id,run_id,turn_id,backend,retry_not_before,boundary_source,boundary_account_id,observed_text,created_at,updated_at,kind,subject)
                         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10,?11,?12)",
                        params![id.as_str(), r.job_id.as_str(), r.run_id.as_deref(), r.turn_id.as_deref(),
                            r.backend.as_str(), r.refusal.retry_at, source, r.refusal.retry_account.as_deref(),
                            r.refusal.reason.as_str(), r.now, r.kind.as_str(), r.refusal.subject.as_str()],
                    )
                    .await?;
                    id
                }
            };
            if r.kind == HoldKind::Launch {
                // SET reads the row as it stood, so `model` is cleared only
                // where a stored selection chose it rather than a pin.
                conn.execute(
                    "UPDATE jobs SET
                       model = CASE WHEN selection IS NOT NULL
                                     AND COALESCE(json_extract(selection, '$.requirement.source.kind'), '') <> 'pinned'
                                    THEN NULL ELSE model END,
                       selection = NULL, status = 'pending', updated_at = ?2
                     WHERE id = ?1 AND status IN ('pending', 'running')",
                    params![r.job_id.as_str(), r.now],
                )
                .await?;
            }
            load_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal("armed hold disappeared".to_string()))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// The job's launch or resume hold, while it still owns the job (see
/// `cairn_db::supply_hold_owns_job_sql!`).
pub async fn active_supply_hold(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<ProviderLimitHold>, String> {
    let job_id = job_id.to_string();
    db.read(move |conn| {
        let job_id = job_id.clone();
        Box::pin(async move { active_supply_hold_conn(conn, &job_id).await })
    })
    .await
    .map_err(|error| error.to_string())
}

pub(crate) async fn active_supply_hold_conn(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<Option<ProviderLimitHold>> {
    let columns = COLUMNS
        .split(", ")
        .map(|column| format!("supply_hold.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut rows = conn
        .query(
            &format!(
                "SELECT {columns} FROM jobs j
                 JOIN provider_limit_holds supply_hold ON supply_hold.job_id = j.id
                 WHERE j.id = ?1 AND {}",
                cairn_db::supply_hold_owns_job_sql!()
            ),
            (job_id,),
        )
        .await?;
    rows.next().await?.as_ref().map(from_row).transpose()
}

/// Exit reason of a run whose launch was held for want of supply. Not a
/// verdict on the work: the job projection never reads it as failed.
pub const SUPPLY_HELD_EXIT_REASON: &str = "supply_held";

/// Record a refused start on a job the refusal held, in place of failing it.
/// Returns `false`, changing nothing, when the job holds no supply hold.
///
/// Every seam that would fail a start calls this first, so a held job is never
/// failed whichever path tried to launch it. The latest run (one is inserted
/// when the refusal came before any) ends `crashed` with
/// [`SUPPLY_HELD_EXIT_REASON`] and keeps the refusal's text; a turn the launch
/// had prepared and nothing received is cancelled as `launch_refused`, which the
/// projection reads as an audit row, not work. The job then re-derives:
/// `pending` for a launch hold, its resting status for a resume.
pub async fn settle_supply_held_start(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    error: &str,
) -> Result<bool, String> {
    let Some(hold) = active_supply_hold(db, job_id).await? else {
        return Ok(false);
    };
    let now = chrono::Utc::now().timestamp();
    let job = job_id.to_string();
    let error = error.to_string();
    let hold_id = hold.id.clone();
    let run_id = db
        .write(move |conn| {
            let (job, error, hold_id) = (job.clone(), error.clone(), hold_id.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT id FROM runs WHERE job_id=?1 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                        (job.as_str(),),
                    )
                    .await?;
                let latest = rows.next().await?.map(|row| row.text(0)).transpose()?;
                drop(rows);
                let run_id = match latest {
                    Some(run_id) => {
                        conn.execute(
                            "UPDATE runs SET status='crashed', exit_reason=?2, error_message=?3,
                               exited_at=COALESCE(exited_at, ?4), updated_at=?4
                             WHERE id=?1 AND status IN ('starting', 'live', 'crashed')",
                            params![run_id.as_str(), SUPPLY_HELD_EXIT_REASON, error.as_str(), now],
                        )
                        .await?;
                        run_id
                    }
                    None => {
                        let run_id = ids::mint_child(&job);
                        conn.execute(
                            "INSERT INTO runs (id, job_id, status, error_message, exit_reason, created_at, updated_at, start_mode)
                             VALUES (?1, ?2, 'crashed', ?3, ?4, ?5, ?5, 'fresh')",
                            params![run_id.as_str(), job.as_str(), error.as_str(), SUPPLY_HELD_EXIT_REASON, now],
                        )
                        .await?;
                        run_id
                    }
                };
                conn.execute(
                    "UPDATE turns SET state='cancelled', end_reason='launch_refused', ended_at=?2, updated_at=?2
                     WHERE id=(SELECT current_turn_id FROM jobs WHERE id=?1)
                       AND (state='pending' OR (state='running' AND start_reason='retry'))",
                    params![job.as_str(), now],
                )
                .await?;
                conn.execute(
                    "UPDATE provider_limit_holds SET run_id=COALESCE(run_id, ?2) WHERE id=?1",
                    params![hold_id.as_str(), run_id.as_str()],
                )
                .await?;
                Ok(run_id)
            })
        })
        .await
        .map_err(|error| error.to_string())?;
    log::info!(
        "Held job {job_id} for want of supply ({} hold {}); its start is recorded on run {run_id}",
        hold.kind.as_str(),
        hold.id
    );
    crate::execution::advancement::recompute_job(orch, job_id)?;
    let change = crate::notify::run_db_change_for_id(db, &run_id, "update").await;
    let _ = orch.services.emitter.emit("db-change", change);
    let change = crate::notify::job_db_change_for_id(db, job_id, "update").await;
    let _ = orch.services.emitter.emit("db-change", change);
    Ok(true)
}

/// Retire every active hold that no longer owns what it was armed for. A yield
/// hold owns its provider-yielded head, and once an operator continues past it
/// there is nothing left to resolve. A launch hold owns a pending job, which an
/// operator's start, a cancel, or a resolution moves on. A resume hold owns the
/// head it rests on, which a later continue replaces. A timed hold would learn
/// this when it came due, but a gated hold never comes due.
pub async fn supersede_orphaned_provider_limit_holds(
    db: &LocalDb,
    now: i64,
) -> Result<u64, String> {
    db.execute(
        "UPDATE provider_limit_holds SET state='superseded', updated_at=?1
         WHERE state='active' AND (
           (kind='yield' AND NOT EXISTS (
              SELECT 1 FROM jobs j JOIN turns t ON t.id=j.current_turn_id
              WHERE j.id=provider_limit_holds.job_id AND t.id=provider_limit_holds.turn_id
                AND t.state='yielded' AND t.yield_reason='provider_limit'))
           OR (kind='launch' AND NOT EXISTS (
              SELECT 1 FROM jobs j WHERE j.id=provider_limit_holds.job_id AND j.status='pending'))
           OR (kind='resume' AND NOT EXISTS (
              SELECT 1 FROM jobs j WHERE j.id=provider_limit_holds.job_id
                AND j.current_turn_id=provider_limit_holds.turn_id AND j.status<>'cancelled')))",
        params![now],
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn load_active_provider_limit_holds(
    db: &LocalDb,
) -> Result<Vec<ProviderLimitHold>, String> {
    db.query_all(
        format!("SELECT {COLUMNS} FROM provider_limit_holds WHERE state='active'"),
        (),
        from_row,
    )
    .await
    .map_err(|error| error.to_string())
}

pub async fn cancel_provider_limit_holds_for_job(db: &LocalDb, job_id: &str) -> Result<(), String> {
    db.execute(
        "UPDATE provider_limit_holds SET state='cancelled', updated_at=?1 WHERE job_id=?2 AND state='active'",
        params![chrono::Utc::now().timestamp(), job_id],
    ).await.map(|_| ()).map_err(|error| error.to_string())
}

async fn set_state_conn(
    conn: &crate::storage::TrackedConnection,
    id: &str,
    state: &str,
    now: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE provider_limit_holds SET state=?1, updated_at=?2 WHERE id=?3 AND state='active'",
        params![state, now, id],
    )
    .await?;
    Ok(())
}

/// Whether `job_id` can take no more work: cancelled, on a resolved issue, or
/// owned by a thread that is no longer active.
async fn job_ineligible_conn(
    conn: &crate::storage::TrackedConnection,
    job_id: &str,
) -> DbResult<bool> {
    let mut rows = conn
        .query(
            "SELECT j.status, i.merged_at, i.closed_at, th.status
             FROM jobs j
             LEFT JOIN executions e ON e.id=j.execution_id
             LEFT JOIN issues i ON i.id=COALESCE(j.issue_id,e.issue_id)
             LEFT JOIN threads th ON th.id=j.thread_id
             WHERE j.id=?1",
            (job_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(true);
    };
    Ok(row.text(0)? == "cancelled"
        || row.opt_i64(1)?.is_some()
        || row.opt_i64(2)?.is_some()
        || row.opt_text(3)?.is_some_and(|status| status != "active"))
}

/// Atomically consume a yield hold and publish its sole pending successor.
/// `forced` skips the clock: supply changed, so the hold is asked now.
async fn claim(
    db: &LocalDb,
    id: &str,
    now: i64,
    forced: bool,
) -> Result<Option<(ProviderLimitHold, String, String)>, String> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        Box::pin(async move {
            let Some(hold) = load_conn(conn, &id).await? else { return Ok(None) };
            let ready = if forced { hold.releasable() } else { hold.due_at(now).is_some() };
            if !ready { return Ok(None) }
            let Some(turn_id) = hold.turn_id.clone() else { return Ok(None) };
            let mut rows = conn.query(
                "SELECT t.session_id FROM jobs j JOIN turns t ON t.id=j.current_turn_id
                 WHERE j.id=?1 AND t.id=?2 AND t.state='yielded' AND t.yield_reason='provider_limit'",
                params![hold.job_id.as_str(), turn_id.as_str()],
            ).await?;
            let Some(row) = rows.next().await? else {
                drop(rows);
                set_state_conn(conn, &id, "superseded", now).await?;
                return Ok(None)
            };
            let session_id = row.text(0)?;
            drop(rows);
            if job_ineligible_conn(conn, &hold.job_id).await? {
                set_state_conn(conn, &id, "cancelled", now).await?;
                return Ok(None);
            }
            // Recheck inside the transaction as well as at the outer admission
            // edge: execution replacement can race the scheduler.
            if crate::execution::canonical::fence_for_job_conn(conn, &hold.job_id).await?.is_superseded() {
                set_state_conn(conn, &id, "superseded", now).await?;
                return Ok(None);
            }
            let successor = ids::mint_child(&hold.job_id);
            conn.execute(
                "INSERT INTO turns (id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at)
                 SELECT ?1,?2,?3,COALESCE(MAX(sequence),0)+1,?4,'pending','wait_resolved',?5,?5 FROM turns WHERE session_id=?2",
                params![successor.as_str(), session_id.as_str(), hold.job_id.as_str(), turn_id.as_str(), now],
            ).await?;
            let changed = conn.execute(
                "UPDATE jobs SET current_turn_id=?1, updated_at=?2 WHERE id=?3 AND current_turn_id=?4",
                params![successor.as_str(), now, hold.job_id.as_str(), turn_id.as_str()],
            ).await?;
            if changed != 1 {
                return Err(DbError::internal("provider hold lost its head during claim".to_string()));
            }
            conn.execute(
                "UPDATE provider_limit_holds SET state='resolved', resolved_at=?1, updated_at=?1 WHERE id=?2",
                params![now, id],
            ).await?;
            Ok(Some((hold, successor, session_id)))
        })
    }).await.map_err(|error| error.to_string())
}

/// Consume a launch or resume hold whose node still waits where it was held:
/// a launch hold's job still pending, a resume hold's head still the turn it
/// rests on. A node something else has moved on is superseded instead, and a
/// node that can take no more work is cancelled.
async fn claim_supply_hold(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    let hold = hold.clone();
    db.write(move |conn| {
        let hold = hold.clone();
        Box::pin(async move {
            let Some(current) = load_conn(conn, &hold.id).await? else { return Ok(false) };
            if !current.releasable() { return Ok(false) }
            let waits = match hold.kind {
                HoldKind::Launch => {
                    "SELECT 1 FROM jobs WHERE id=?1 AND status='pending' AND ?2 IS NULL"
                }
                _ => "SELECT 1 FROM jobs WHERE id=?1 AND current_turn_id=?2",
            };
            let mut rows = conn
                .query(waits, params![hold.job_id.as_str(), hold.turn_id.as_deref().filter(|_| hold.kind == HoldKind::Resume)])
                .await?;
            let still = rows.next().await?.is_some();
            drop(rows);
            if !still {
                set_state_conn(conn, &hold.id, "superseded", now).await?;
                return Ok(false);
            }
            if job_ineligible_conn(conn, &hold.job_id).await? {
                set_state_conn(conn, &hold.id, "cancelled", now).await?;
                return Ok(false);
            }
            if crate::execution::canonical::fence_for_job_conn(conn, &hold.job_id).await?.is_superseded() {
                set_state_conn(conn, &hold.id, "superseded", now).await?;
                return Ok(false);
            }
            conn.execute(
                "UPDATE provider_limit_holds SET state='resolved', resolved_at=?1, updated_at=?1 WHERE id=?2 AND state='active'",
                params![now, hold.id.as_str()],
            )
            .await?;
            Ok(true)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Restore only an undriven successor still owned by this claim. Never rewind
/// an operator's newer head or a successor that actually began doing work.
async fn restore_after_launch_failure(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    successor: &str,
    now: i64,
) -> Result<bool, String> {
    let hold = hold.clone();
    let successor = successor.to_string();
    db.write(move |conn| {
        let hold = hold.clone();
        let successor = successor.clone();
        Box::pin(async move {
            let Some(turn_id) = hold.turn_id.clone() else { return Ok(false) };
            let changed = conn.execute(
                "UPDATE turns SET state='cancelled', end_reason='launch_refused', ended_at=?1, updated_at=?1, predecessor_id=NULL
                 WHERE id=?2 AND (state='pending' OR (state='cancelled' AND end_reason='launch_refused'))
                   AND id=(SELECT current_turn_id FROM jobs WHERE id=?3)
                   AND EXISTS (SELECT 1 FROM turns WHERE id=?4 AND state='yielded' AND yield_reason='provider_limit')
                   AND EXISTS (SELECT 1 FROM provider_limit_holds WHERE id=?5 AND state='resolved')",
                params![now, successor.as_str(), hold.job_id.as_str(), turn_id.as_str(), hold.id.as_str()],
            ).await?;
            if changed == 0 { return Ok(false) }
            // Detach the unserved launch from the successor chain so an explicit
            // Continue can still create a successor from the restored head.
            conn.execute(
                "UPDATE jobs SET current_turn_id=?1, updated_at=?2 WHERE id=?3 AND current_turn_id=?4",
                params![turn_id.as_str(), now, hold.job_id.as_str(), successor.as_str()],
            ).await?;
            // A third failed launch is not a supply question any re-check can
            // answer, so the hold stays visibly gated on the operator.
            conn.execute(
                "UPDATE provider_limit_holds SET state='active', resolved_at=NULL,
                   launch_attempts=launch_attempts+1,
                   retry_not_before=CASE WHEN launch_attempts+1 >= 3 THEN retry_not_before ELSE ?1 END,
                   resume_gate=CASE WHEN launch_attempts+1 >= 3 THEN COALESCE(resume_gate, ?4) ELSE resume_gate END,
                   updated_at=?2 WHERE id=?3 AND state='resolved'",
                params![now.saturating_add(300), now, hold.id.as_str(), RESUME_GATE_LAUNCH_FAILED],
            ).await?;
            Ok(true)
        })
    }).await.map_err(|error| error.to_string())
}

/// Cold admission can fail after the resolver returned: queued launches belong
/// to the admission worker, not the original call stack. Recover its pending
/// provider successor before generic failure projection can settle the DAG.
pub(crate) async fn restore_provider_hold_after_admission_failure(
    orch: &Orchestrator,
    db: &LocalDb,
    successor: &str,
    now: i64,
) -> Result<bool, String> {
    let successor_id = successor.to_string();
    let hold = db.read(move |conn| {
        let successor_id = successor_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT h.id FROM provider_limit_holds h
                 JOIN turns t ON t.predecessor_id=h.turn_id AND t.job_id=h.job_id
                 WHERE t.id=?1 AND t.start_reason='wait_resolved' AND h.state='resolved'
                   AND h.kind='yield'
                   AND (t.state='pending' OR (t.state='cancelled' AND t.end_reason='launch_refused'))
                 ORDER BY h.created_at DESC LIMIT 1",
                (successor_id.as_str(),),
            ).await?;
            let id = rows.next().await?.map(|row| row.text(0)).transpose()?;
            drop(rows);
            match id {
                Some(id) => load_conn(conn, &id).await,
                None => Ok(None),
            }
        })
    }).await.map_err(|error| error.to_string())?;
    let Some(hold) = hold else { return Ok(false) };
    if restore_after_launch_failure(db, &hold, successor, now).await? {
        crate::execution::advancement::recompute_job(orch, &hold.job_id)?;
    }
    // A newer head is authoritative too: this stale launch must not fail it.
    Ok(true)
}

/// Record that a hold came due and its work still cannot run: its new boundary
/// (the soonest reset now declared, or none), and that it learned this now, so
/// a hold with no boundary re-checks a full interval later.
async fn rearm(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    at: Option<i64>,
    account: Option<String>,
    reason: Option<String>,
    now: i64,
) -> Result<(), String> {
    log::info!(
        "{} hold {} came due with nothing able to serve it; re-armed to {at:?}",
        hold.kind.as_str(),
        hold.id
    );
    let id = hold.id.clone();
    let supply = hold.kind.is_supply();
    db.write(move |conn| {
        let id = id.clone();
        let account = account.clone();
        let reason = reason.clone();
        Box::pin(async move {
            let source = if !supply {
                None
            } else if at.is_some() {
                Some("supply_reset")
            } else {
                Some("recheck")
            };
            conn.execute(
                "UPDATE provider_limit_holds SET retry_not_before=?1, boundary_account_id=?2,
                   boundary_source=COALESCE(?5, boundary_source),
                   observed_text=COALESCE(?6, observed_text), updated_at=?3
                 WHERE id=?4 AND state='active'",
                params![at, account, now, id.as_str(), source, reason],
            )
            .await?;
            Ok(())
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Whether a hold's work has something to run on now. A boundary is a reading
/// taken when the hold was armed; by the time it comes due, supply may have
/// moved either way. Launching anyway would only spend a launch on a refusal,
/// so a hold whose launch would be refused re-arms to the soonest reset now
/// declared, or to the bounded re-check, and launches nothing.
///
/// A launch hold asks selection, because its launch selects again. A yield or
/// resume hold asks launch routing itself, because its session keeps the
/// provider it chose, so a healthy pin or an allowed API-key fallback launches
/// exactly as a continue would.
async fn hold_can_launch(
    orch: &Orchestrator,
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    if hold.kind == HoldKind::Launch {
        let probe_orch = orch.clone();
        let job = hold.job_id.clone();
        let refusal = tokio::task::spawn_blocking(move || {
            crate::execution::jobs::probe_launch_supply(&probe_orch, &job)
        })
        .await
        .map_err(|error| format!("supply probe for hold {} failed: {error}", hold.id))??;
        return match refusal {
            None => Ok(true),
            Some(refusal) => {
                rearm(
                    db,
                    hold,
                    refusal.retry_at,
                    refusal.retry_account,
                    Some(refusal.reason),
                    now,
                )
                .await?;
                Ok(false)
            }
        };
    }
    use crate::credentials::{RoutedAvailability, RoutedProvider};
    let Some(provider) = RoutedProvider::for_backend(&hold.backend) else {
        return Ok(true);
    };
    let job = hold.job_id.clone();
    let Some((project_id, model, pinned)) = db
        .query_opt(
            "SELECT COALESCE(j.project_id, i.project_id), j.model, s.account_id FROM jobs j
               LEFT JOIN issues i ON i.id = j.issue_id
               LEFT JOIN sessions s ON s.id = j.current_session_id
              WHERE j.id = ?1",
            (job,),
            |row| Ok((row.opt_text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
        )
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(true);
    };
    let snapshot =
        crate::orchestrator::session::snapshot_account_override_for_job(orch, &hold.job_id)?;
    if crate::orchestrator::session::routed_session_can_launch(
        orch,
        provider,
        pinned.as_deref(),
        project_id.as_deref(),
        model.as_deref(),
        snapshot.as_deref(),
    ) {
        return Ok(true);
    }
    let explicit = snapshot.or_else(|| {
        crate::orchestrator::session::routed_project_override(orch, provider, project_id.as_deref())
    });
    let (at, account) = match orch.routed_availability(
        provider,
        project_id.as_deref(),
        explicit.as_deref(),
        model.as_deref(),
        now,
    ) {
        RoutedAvailability::Now(_) | RoutedAvailability::NoCandidates => return Ok(true),
        RoutedAvailability::At(at, _) if at <= now => return Ok(true),
        RoutedAvailability::At(at, account) => (Some(at), Some(account)),
        RoutedAvailability::Unknown => (None, None),
    };
    let account = account.filter(|account| hold.account_id.as_deref() != Some(account.as_str()));
    rearm(db, hold, at, account, None, now).await?;
    Ok(false)
}

/// What asked a hold whether its work can run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Trigger {
    /// Its clock came due: the declared reset, or the bounded re-check.
    Timer,
    /// The accounts changed: one added or signed in, a block cleared, a usage
    /// reading, a credit top-up.
    SupplyChanged,
}

impl Trigger {
    fn label(self, hold: &ProviderLimitHold) -> &'static str {
        match self {
            Self::SupplyChanged => "supply_changed",
            Self::Timer if hold.retry_not_before.is_some() => "reset",
            Self::Timer => "recheck",
        }
    }
}

/// Resolve a hold whose clock has come due, through owned-wait admission, never
/// through an attention push or the model-capacity retry path (both would mint
/// a competing successor).
pub async fn resolve_provider_limit_hold(
    orch: &Orchestrator,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    resolve(orch, hold, now, Trigger::Timer).await
}

/// Ask a hold, whatever its clock says, whether the accounts as they now stand
/// can serve its work, and release it through the same path a reset does when
/// they can. A hold gated on the operator is never released this way.
pub async fn release_on_supply_change(
    orch: &Orchestrator,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    resolve(orch, hold, now, Trigger::SupplyChanged).await
}

async fn resolve(
    orch: &Orchestrator,
    hold: &ProviderLimitHold,
    now: i64,
    trigger: Trigger,
) -> Result<bool, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &hold.job_id)
        .await
        .map_err(|error| error.to_string())?;
    let ready = match trigger {
        Trigger::Timer => hold.due_at(now).is_some(),
        Trigger::SupplyChanged => hold.releasable(),
    };
    if !ready {
        return Ok(false);
    }
    if crate::execution::canonical::job_refusal(&db, &hold.job_id).is_some() {
        let id = hold.id.clone();
        db.write(move |conn| {
            let id = id.clone();
            Box::pin(async move { set_state_conn(conn, &id, "superseded", now).await })
        })
        .await
        .map_err(|error| error.to_string())?;
        return Ok(false);
    }
    if !hold_can_launch(orch, &db, hold, now).await? {
        return Ok(false);
    }
    // Supply can serve it; the fleet cap decides whether it runs now. The
    // admission lock serialises this with every other cap decision, and the
    // release below happens under it, so each released hold counts before
    // the next is decided.
    let guard = match orch
        .admission_lock
        .acquire(
            crate::execution::gates::capacity::AdmissionSite::HoldRelease,
            hold.job_id.clone(),
        )
        .await
    {
        Ok(guard) => guard,
        Err(busy) => {
            log::info!("Hold {} waits for the admission lock: {busy:?}", hold.id);
            wait_for_fleet_slot(&db, hold, now).await?;
            return Ok(false);
        }
    };
    if !held_work_fits(orch, &db, hold).await? {
        wait_for_fleet_slot(&db, hold, now).await?;
        return Ok(false);
    }
    match hold.kind {
        HoldKind::Yield => resolve_yield(orch, &db, hold, now, trigger, guard).await,
        HoldKind::Launch | HoldKind::Resume => {
            resolve_supply_hold(orch, &db, hold, now, trigger, guard).await
        }
    }
}

async fn held_work_fits(
    orch: &Orchestrator,
    db: &LocalDb,
    hold: &ProviderLimitHold,
) -> Result<bool, String> {
    let execution = db
        .query_opt(
            "SELECT execution_id FROM jobs WHERE id = ?1",
            (hold.job_id.clone(),),
            |row| row.opt_text(0),
        )
        .await
        .map_err(|error| error.to_string())?
        .flatten();
    // Work outside an execution (a thread's own session) never counts.
    let Some(execution) = execution else {
        return Ok(true);
    };
    crate::execution::gates::capacity::held_work_fits(
        orch,
        db,
        &execution,
        hold.kind == HoldKind::Launch,
    )
    .await
}

/// Keep a hold whose work supply can serve again held until a fleet slot is
/// free, asking again after [`FLEET_SLOT_RECHECK_SECS`].
async fn wait_for_fleet_slot(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<(), String> {
    log::info!(
        "{} hold {} can be served but its fleet is full; asking again in {FLEET_SLOT_RECHECK_SECS}s",
        hold.kind.as_str(),
        hold.id
    );
    db.execute(
        "UPDATE provider_limit_holds SET retry_not_before=?1, boundary_source='fleet_slot', updated_at=?2
         WHERE id=?3 AND state='active'",
        params![now.saturating_add(FLEET_SLOT_RECHECK_SECS), now, hold.id.as_str()],
    )
    .await
    .map(|_| ())
    .map_err(|error| error.to_string())
}

async fn resolve_yield(
    orch: &Orchestrator,
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
    trigger: Trigger,
    // Held through the continue, which records the live turn that makes the
    // job count, so no other cap decision reads the fleet in between.
    _admission: crate::execution::gates::capacity::AdmissionGuard,
) -> Result<bool, String> {
    let Some((hold, successor, session)) =
        claim(db, &hold.id, now, trigger == Trigger::SupplyChanged).await?
    else {
        return Ok(false);
    };
    let note = if hold.boundary_source == "account_switch" {
        "Continuing the unfinished work on the replacement account."
    } else if trigger == Trigger::SupplyChanged {
        "An account can serve this work again. Continuing the unfinished work."
    } else {
        "The provider usage limit has reset. Continuing the unfinished work."
    };
    if let Some(run_id) = hold.run_id.as_deref() {
        if let Err(error) = crate::messages::transcript::insert_system_message_sync(
            orch,
            run_id,
            Some(&session),
            hold.turn_id.as_deref(),
            note,
            serde_json::json!({
                "provider": hold.backend,
                "kind": "provider_limit_resolved",
                "holdId": hold.id,
                "trigger": trigger.label(&hold),
            }),
        ) {
            log::warn!(
                "Failed to record provider hold resolution {}: {error}",
                hold.id
            );
        }
    }
    let result = continue_job_impl(
        orch,
        &hold.job_id,
        None,
        None,
        Some(ResumeContext {
            preclaimed_successor_turn_id: Some(successor.clone()),
            suppress_user_event: true,
            suppress_self_suspend_note: true,
            unwind_preclaimed_start_as: Some(crate::models::TurnEndReason::LaunchRefused),
            ..Default::default()
        }),
    );
    if let Err(error) = result {
        restore_after_launch_failure(db, &hold, &successor, now).await?;
        crate::execution::advancement::recompute_job(orch, &hold.job_id)?;
        return Err(format!("Provider hold {} resume failed: {error}", hold.id));
    }
    Ok(true)
}

/// Release a launch or resume hold. A launch hold's node becomes placeable
/// and is handed to advancement, so it launches the way any ready node does:
/// through admission and the fleet cap, never straight from this wake, so a
/// burst of releases queues rather than stampedes. A resume continues the node
/// as an operator's Continue would, redelivering what its refusal returned.
async fn resolve_supply_hold(
    orch: &Orchestrator,
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
    trigger: Trigger,
    admission: crate::execution::gates::capacity::AdmissionGuard,
) -> Result<bool, String> {
    if !claim_supply_hold(db, hold, now).await? {
        return Ok(false);
    }
    // A released launch hold's job is pending and unheld, which the fleet
    // already counts, so advancement runs after the lock is let go. A resume
    // counts only once its continue records a live turn, so it keeps the lock
    // through that.
    let admission = (hold.kind == HoldKind::Resume).then_some(admission);
    if let Some(run_id) = hold.run_id.as_deref() {
        let note = match trigger {
            Trigger::SupplyChanged => "An account can serve this node again, so it resumes now.",
            Trigger::Timer if hold.retry_not_before.is_some() => {
                "The soonest usage reset has passed, so this node resumes now."
            }
            Trigger::Timer => "An account can serve this node again, so it resumes now.",
        };
        if let Err(error) = crate::messages::transcript::insert_system_message_sync(
            orch,
            run_id,
            None,
            hold.turn_id.as_deref(),
            note,
            serde_json::json!({
                "provider": hold.backend,
                "kind": "supply_hold_resolved",
                "holdId": hold.id,
                "holdKind": hold.kind.as_str(),
                "trigger": trigger.label(hold),
            }),
        ) {
            log::warn!(
                "Failed to record supply hold resolution {}: {error}",
                hold.id
            );
        }
    }
    let change = crate::notify::job_db_change_for_id(db, &hold.job_id, "update").await;
    let _ = orch.services.emitter.emit("db-change", change);
    match hold.kind {
        HoldKind::Resume => {
            let resumed = continue_job_impl(
                orch,
                &hold.job_id,
                None,
                None,
                Some(ResumeContext {
                    suppress_user_event: true,
                    ..Default::default()
                }),
            )
            .map(|_| true)
            .map_err(|error| format!("Supply hold {} resume failed: {error}", hold.id));
            drop(admission);
            resumed
        }
        _ => {
            let execution = db
                .query_opt(
                    "SELECT execution_id FROM jobs WHERE id = ?1",
                    (hold.job_id.clone(),),
                    |row| row.opt_text(0),
                )
                .await
                .map_err(|error| error.to_string())?
                .flatten();
            let Some(execution) = execution else {
                return Ok(true);
            };
            let ready =
                crate::execution::advancement::advance_execution_with_actions(orch, &execution)
                    .await?;
            if !ready.is_empty() {
                let executor = orch
                    .executor
                    .get()
                    .ok_or_else(|| "no executor is configured".to_string())?;
                executor
                    .execute(
                        orch,
                        crate::effects::types::WorkflowEffect::StartAgentJobs(ready),
                    )
                    .await?;
            }
            Ok(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hold() -> ProviderLimitHold {
        ProviderLimitHold {
            id: "stable-hold".into(),
            job_id: "j".into(),
            run_id: Some("r".into()),
            turn_id: Some("t".into()),
            kind: HoldKind::Yield,
            backend: "claude".into(),
            account_id: None,
            retry_not_before: Some(1000),
            boundary_source: "result_text".into(),
            observed_text: None,
            observed_count: 1,
            launch_attempts: 0,
            state: "active".into(),
            created_at: 1,
            updated_at: 1,
            resolved_at: None,
            resume_gate: None,
            boundary_account_id: None,
            subject: None,
        }
    }

    #[test]
    fn jitter_is_stable_bounded_and_a_gate_is_never_due() {
        let mut hold = hold();
        let due = hold.next_due_at().unwrap();
        assert!((1000..=1120).contains(&due));
        assert_eq!(hold.clone().next_due_at(), Some(due));
        assert_eq!(hold.due_at(due - 1), None);
        assert_eq!(hold.due_at(due), Some(due));
        hold.boundary_source = "account_switch".into();
        assert_eq!(
            hold.next_due_at(),
            Some(1000),
            "an account switch is not jittered"
        );
        hold.resume_gate = Some(RESUME_GATE_TOOL_IN_FLIGHT.into());
        assert_eq!(
            hold.due_at(i64::MAX),
            None,
            "a gated hold is never due, whatever its boundary"
        );
        assert!(!hold.releasable(), "nor released by a supply change");
        hold.resume_gate = Some(RESUME_GATE_LAUNCH_FAILED.into());
        assert_eq!(hold.due_at(i64::MAX), None);
    }

    /// cairn/5378's timer half: a hold that declares no reset is not left
    /// waiting forever on an event that may never come. It asks again a
    /// bounded interval after it last learned anything.
    #[test]
    fn a_hold_with_no_reset_rechecks_on_a_bounded_clock() {
        for kind in [HoldKind::Yield, HoldKind::Launch, HoldKind::Resume] {
            let mut hold = hold();
            hold.kind = kind;
            hold.retry_not_before = None;
            hold.boundary_source = "recheck".into();
            hold.updated_at = 5_000;
            assert_eq!(hold.next_due_at(), Some(5_000 + SUPPLY_RECHECK_SECS));
            assert_eq!(hold.due_at(5_000 + SUPPLY_RECHECK_SECS - 1), None);
            assert!(hold.releasable());
            hold.state = "resolved".into();
            assert_eq!(hold.next_due_at(), None);
            assert!(!hold.releasable());
        }
    }

    const NOW: i64 = 10_000;

    /// Job `a` running on a stored selection that chose `claude/opus`; job `b`
    /// pinned to `claude/opus`; job `r` resting on its head turn `head`.
    async fn seeded(name: &str) -> LocalDb {
        let db = crate::storage::migrated_test_db(name).await;
        db.execute_script(
            r#"INSERT INTO workspaces(id,name,created_at,updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('p','w','P','p','/tmp/p',1,1);
             INSERT INTO issues(id,project_id,number,title,status,created_at,updated_at) VALUES('i','p',1,'I','active',1,1);
             INSERT INTO jobs(id,project_id,issue_id,status,node_name,uri_segment,model,selection,created_at,updated_at)
               VALUES('a','p','i','running','builder','builder','claude/opus','{"requirement":{"source":{"kind":"allotment"}}}',1,1),
                     ('b','p','i','running','reviewer','reviewer','claude/opus','{"requirement":{"source":{"kind":"pinned"}}}',1,1),
                     ('r','p','i','completed','other','other','claude/opus',NULL,1,1);
             INSERT INTO sessions(id,job_id,backend,backend_id,status,sequence,created_at,updated_at)
               VALUES('session','r','claude','native','open',1,1,1);
             INSERT INTO runs(id,project_id,issue_id,job_id,session_id,status,start_mode,created_at,updated_at)
               VALUES('run','p','i','r','session','exited','resume',1,1);
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('head','session','run','r',1,'completed','initial',1,1);
             UPDATE jobs SET current_turn_id='head' WHERE id='r';"#,
        )
        .await
        .unwrap();
        db
    }

    fn refused(job: &str, kind: HoldKind, retry_at: Option<i64>) -> SupplyRefusal {
        SupplyRefusal {
            job_id: job.into(),
            kind,
            backend: "claude".into(),
            run_id: None,
            turn_id: (kind == HoldKind::Resume).then(|| "head".into()),
            refusal: CapacityRefusal {
                subject: "claude/opus".into(),
                reason: "claude/opus cannot run now: every Claude account is out of usage".into(),
                retry_at,
                retry_account: retry_at.map(|_| "acct".into()),
            },
            now: NOW,
        }
    }

    async fn job(db: &LocalDb, id: &str) -> (String, Option<String>, Option<String>) {
        db.query_one(
            "SELECT status, model, selection FROM jobs WHERE id=?1",
            params![id],
            |row| Ok((row.text(0)?, row.opt_text(1)?, row.opt_text(2)?)),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_launch_hold_returns_its_job_to_selection_and_coalesces() {
        let db = seeded("supply-hold-launch").await;
        let hold = arm_supply_hold(&db, refused("a", HoldKind::Launch, Some(NOW + 3600)))
            .await
            .unwrap();
        assert_eq!(hold.kind, HoldKind::Launch);
        assert_eq!(hold.boundary_source, "supply_reset");
        assert_eq!(hold.retry_not_before, Some(NOW + 3600));
        assert_eq!(hold.boundary_account_id.as_deref(), Some("acct"));
        assert_eq!(hold.subject.as_deref(), Some("claude/opus"));
        assert_eq!(hold.run_id, None, "a cold launch has no run yet");
        assert_eq!(
            job(&db, "a").await,
            ("pending".into(), None, None),
            "release selects afresh, so neither the selection nor the model it chose survives"
        );

        let again = arm_supply_hold(&db, refused("a", HoldKind::Launch, None))
            .await
            .unwrap();
        assert_eq!(again.id, hold.id, "one active hold per job");
        assert_eq!(again.observed_count, 2);
        assert_eq!(again.boundary_source, "recheck");
        assert_eq!(again.next_due_at(), Some(NOW + SUPPLY_RECHECK_SECS));
        assert_eq!(
            active_supply_hold(&db, "a").await.unwrap().map(|h| h.id),
            Some(hold.id)
        );
    }

    #[tokio::test]
    async fn a_held_pin_stays_pinned() {
        let db = seeded("supply-hold-pin").await;
        arm_supply_hold(&db, refused("b", HoldKind::Launch, None))
            .await
            .unwrap();
        assert_eq!(
            job(&db, "b").await,
            ("pending".into(), Some("claude/opus".into()), None)
        );
    }

    #[tokio::test]
    async fn a_resume_hold_rests_on_its_head_and_leaves_the_job_alone() {
        let db = seeded("supply-hold-resume").await;
        let mut headless = refused("r", HoldKind::Resume, None);
        headless.turn_id = None;
        assert!(arm_supply_hold(&db, headless).await.is_err());
        assert!(arm_supply_hold(&db, refused("r", HoldKind::Yield, None))
            .await
            .is_err());

        let hold = arm_supply_hold(&db, refused("r", HoldKind::Resume, None))
            .await
            .unwrap();
        assert_eq!(hold.turn_id.as_deref(), Some("head"));
        assert_eq!(
            job(&db, "r").await,
            ("completed".into(), Some("claude/opus".into()), None)
        );
    }

    #[tokio::test]
    async fn each_kind_is_orphaned_by_its_own_rule() {
        let db = seeded("supply-hold-orphans").await;
        let launch = arm_supply_hold(&db, refused("a", HoldKind::Launch, None))
            .await
            .unwrap();
        let resume = arm_supply_hold(&db, refused("r", HoldKind::Resume, None))
            .await
            .unwrap();
        assert_eq!(
            supersede_orphaned_provider_limit_holds(&db, NOW)
                .await
                .unwrap(),
            0,
            "a pending launch and a resting head both still own their holds"
        );

        // An operator starts the held job, and continues past the head.
        db.execute_script(
            "UPDATE jobs SET status='running' WHERE id='a';
             INSERT INTO turns(id,session_id,run_id,job_id,sequence,state,start_reason,created_at,updated_at)
               VALUES('next','session','run','r',2,'running','user',2,2);
             UPDATE jobs SET current_turn_id='next' WHERE id='r';",
        )
        .await
        .unwrap();
        assert_eq!(
            supersede_orphaned_provider_limit_holds(&db, NOW)
                .await
                .unwrap(),
            2
        );
        let active: Vec<String> = load_active_provider_limit_holds(&db)
            .await
            .unwrap()
            .into_iter()
            .map(|hold| hold.id)
            .collect();
        assert!(!active.contains(&launch.id) && !active.contains(&resume.id));
        assert!(active_supply_hold(&db, "a").await.unwrap().is_none());
    }
}
