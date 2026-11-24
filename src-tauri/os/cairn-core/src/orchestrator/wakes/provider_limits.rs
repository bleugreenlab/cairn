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

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::execution::gates::capacity::{fleet_working, held_work_fits, FleetWorking, ThreadFacts};
use crate::execution::jobs::ResumeContext;
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
    /// Durable launch failures, in observation order: `{at,cause,retryable}`.
    pub launch_failures_json: String,
    pub state: String,
    pub created_at: i64,
    pub queue_key: Option<i64>,
    pub updated_at: i64,
    pub resolved_at: Option<i64>,
    /// Why automatic resolution is withheld whatever the boundary says.
    pub resume_gate: Option<String>,
    /// The account whose reset the boundary is, when not this hold's own.
    pub boundary_account_id: Option<String>,
    /// What a launch or resume hold waits for supply of: a `provider/model`,
    /// or an allotment as `[sm, md, lg]`.
    pub subject: Option<String>,
    pub resume_json: Option<String>,
}

/// The limit landed while a tool call had no recorded result. Resuming could run
/// that call a second time, so only an explicit continue resumes the work.
pub const RESUME_GATE_TOOL_IN_FLIGHT: &str = "tool_in_flight";

/// A launch cannot be retried without changing durable session state.
/// Attempt counts alone never establish that a failure is permanent.
pub const RESUME_GATE_LAUNCH_FAILED: &str = "launch_failed";

/// How long a hold with no declared reset waits before asking again whether
/// anything can serve it. A change to the accounts asks sooner.
pub const SUPPLY_RECHECK_SECS: i64 = 900;

/// How soon a hold whose work supply can serve again, but whose fleet is
/// full, asks for a slot again. A supply change asks sooner.
pub const FLEET_SLOT_RECHECK_SECS: i64 = 60;

impl ProviderLimitHold {
    fn launch_backoff_until(&self) -> Option<i64> {
        let failures: Vec<serde_json::Value> =
            serde_json::from_str(&self.launch_failures_json).ok()?;
        if self.state != "active" {
            return None;
        }
        let Some(last) = failures.last() else {
            // Count-only gates from older runners have no history timestamp.
            return (self.launch_attempts > 0)
                .then_some(self.retry_not_before)
                .flatten();
        };
        last.get("retryable")?
            .as_bool()?
            .then(|| last.get("at")?.as_i64()?.checked_add(300))
            .flatten()
    }

    pub fn queue_order(&self) -> i64 {
        self.queue_key
            .unwrap_or_else(|| self.created_at.saturating_mul(1_000_000))
    }

    /// When this hold next asks whether its work can run, or `None` when it
    /// never does by itself (resolved, or gated on the operator).
    ///
    /// A declared reset is jittered by a stable FNV-1a hash, independent of
    /// process hash seeds and restarts, which spreads jobs released by one
    /// shared reset; a switch to another account has no shared instant to
    /// spread, so it is due as declared. With no declared reset the hold
    /// re-checks [`SUPPLY_RECHECK_SECS`] after it last learned anything.
    pub fn next_due_at(&self) -> Option<i64> {
        if self.state != "active" || self.resume_gate.is_some() || self.is_thread_bound() {
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

    /// Whether this hold waits on its thread's bound (focus or open PRs)
    /// rather than on supply or a fleet slot.
    pub fn is_thread_bound(&self) -> bool {
        matches!(
            self.boundary_source.as_str(),
            "thread_focus" | "thread_open_prs"
        )
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

const COLUMNS: &str = "id, job_id, run_id, turn_id, backend, account_id, retry_not_before, boundary_source, observed_text, observed_count, launch_attempts, state, created_at, updated_at, resolved_at, resume_gate, boundary_account_id, kind, subject, queue_key, resume_json, launch_failures_json";

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
        queue_key: row.opt_i64(19)?,
        resume_json: row.opt_text(20)?,
        launch_failures_json: row.text(21)?,
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
    arm_job_hold(db, refused, None, None).await
}

/// Supply refuses a prepared continuation only after its exact input and cause
/// have been captured. Publish that payload with the hold, never in a later write.
pub async fn arm_supply_resume_hold(
    db: &LocalDb,
    refused: SupplyRefusal,
    resume_json: String,
) -> Result<ProviderLimitHold, String> {
    arm_job_hold(db, refused, None, Some(resume_json)).await
}

async fn arm_job_hold(
    db: &LocalDb,
    refused: SupplyRefusal,
    boundary: Option<&'static str>,
    resume_json: Option<String>,
) -> Result<ProviderLimitHold, String> {
    if !refused.kind.is_supply() {
        return Err("a supply hold is a launch or a resume".into());
    }
    if refused.kind == HoldKind::Resume && refused.turn_id.is_none() {
        return Err("a resume hold owns the head it rests on".into());
    }
    db.write(move |conn| {
        let r = refused.clone();
        let resume_json=resume_json.clone();
        Box::pin(async move {
            let source = boundary.unwrap_or(if r.refusal.retry_at.is_some() { "supply_reset" } else { "recheck" });
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
            conn.execute("UPDATE provider_limit_holds SET resume_json=COALESCE(?2,resume_json) WHERE id=?1",params![id.as_str(),resume_json.as_deref()]).await?;
            if boundary.is_some() {
                conn.execute("UPDATE provider_limit_holds SET queue_key=MAX(?2,COALESCE((SELECT MAX(COALESCE(h.queue_key,h.created_at*1000000)) FROM provider_limit_holds h WHERE h.state='active' AND h.boundary_source IN ('thread_focus','thread_open_prs') AND h.id<>?1),0)+1) WHERE id=?1 AND queue_key IS NULL", params![id.as_str(),r.now.saturating_mul(1_000_000)]).await?;
            }
            if r.kind == HoldKind::Launch && boundary.is_none() {
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
            if r.kind == HoldKind::Launch && boundary.is_some() {
                conn.execute("UPDATE jobs SET status='pending',updated_at=?2 WHERE id=?1 AND status IN ('pending','running')", params![r.job_id.as_str(),r.now]).await?;
            }
            load_conn(conn, &id)
                .await?
                .ok_or_else(|| DbError::internal("armed hold disappeared".to_string()))
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Synced hold rows are visible to every runner; only their execution owner
/// may claim them. Refuse before any state transition, not after launch claim.
async fn owns_held_job(orch: &Orchestrator, db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let row=db.query_opt("SELECT e.runner_device_id FROM jobs j LEFT JOIN executions e ON e.id=j.execution_id WHERE j.id=?1",(job_id.to_string(),),|row|row.opt_text(0)).await.map_err(|e|e.to_string())?;
    Ok(row.is_some_and(|owner| {
        owner.is_none_or(|owner| owner == orch.anon_device_manager.device_id())
    }))
}

/// A refused launch admission as a sentence: it is the reason a waiting
/// parent, a node page and a failed run all read.
fn launch_admission_refusal(busy: &crate::execution::gates::capacity::AdmissionBusy) -> String {
    let waited = busy.waited.as_secs();
    match &busy.holder {
        Some(holder) => format!(
            "launch admission was busy: this launch waited {waited} s while another admission \
             ({} of {}) held it for {} s",
            holder.site,
            holder.subject,
            holder.held.as_secs()
        ),
        None => format!(
            "launch admission was busy: this launch waited {waited} s and gave up just as another admission let go"
        ),
    }
}

/// The host's cold-launch seam, shared by DAG dispatch and owner pickup.
/// Hold the returned guard until the pending/running job reserves its slot.
pub async fn admit_thread_launch(
    orch: &Orchestrator,
    job_id: &str,
    cause: crate::execution::jobs::WakeCause,
    focus_admitted: bool,
) -> Result<Option<crate::execution::gates::capacity::AdmissionGuard>, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, job_id)
        .await
        .map_err(|e| e.to_string())?;
    let guard = orch
        .admission_lock
        .acquire(
            crate::execution::gates::capacity::AdmissionSite::HoldRelease,
            job_id.to_string(),
        )
        .await
        .map_err(|busy| launch_admission_refusal(&busy))?;
    if active_supply_hold(&db, job_id).await?.is_some() {
        return Ok(None);
    }
    if cause == crate::execution::jobs::WakeCause::System {
        if let Some((boundary, reason)) =
            thread_job_refusal(orch, &db, job_id, HoldKind::Launch, None, focus_admitted).await?
        {
            if skip_bound_for_operator_start(&db, job_id, boundary).await? {
                return Ok(Some(guard));
            }
            arm_thread_hold(
                &db,
                job_id,
                HoldKind::Launch,
                boundary,
                reason,
                String::new(),
                orch.services.clock.now(),
            )
            .await?;
            return Ok(None);
        }
    }
    Ok(Some(guard))
}

/// Whether a launch the thread's `boundary` would hold is one the operator
/// started, and so is never held (tenet 1456): its execution's release records
/// the operator's Start now (or a start under the operator's grant), and the
/// job has never run. When it is, the bound is added to that record's
/// `skipped`, so the start names every cap that actually held the work.
///
/// The release is read here, at admission, rather than carried on the
/// dispatch: an operator's start reaches this seam through a durable advance
/// and possibly a restart, and every dispatch along the way says `System`.
async fn skip_bound_for_operator_start(
    db: &LocalDb,
    job_id: &str,
    boundary: &'static str,
) -> Result<bool, String> {
    let job = job_id.to_string();
    db.write(move |conn| {
        let job = job.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT e.id, e.release_override_json,
                            EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id)
                       FROM jobs j JOIN executions e ON e.id = j.execution_id
                      WHERE j.id = ?1",
                    params![job.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            let execution = row.text(0)?;
            let release = row.opt_text(1)?;
            let has_run = row.i64(2)? != 0;
            drop(rows);
            let Some(mut record) = release
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
            else {
                return Ok(false);
            };
            let operator_started = record.get("operator").and_then(|v| v.as_bool()) == Some(true)
                || record
                    .get("granted")
                    .is_some_and(|granted| !granted.is_null());
            if !operator_started || has_run {
                return Ok(false);
            }
            let Some(label) = crate::execution::gates::open_prs::skipped_label(boundary) else {
                return Ok(true);
            };
            let skipped = record.as_object_mut().map(|object| {
                object
                    .entry("skipped")
                    .or_insert_with(|| serde_json::json!([]))
            });
            if let Some(serde_json::Value::Array(skipped)) = skipped {
                if !skipped.iter().any(|known| known.as_str() == Some(label)) {
                    skipped.push(serde_json::Value::from(label));
                    conn.execute(
                        "UPDATE executions SET release_override_json = ?2 WHERE id = ?1",
                        params![execution.as_str(), record.to_string()],
                    )
                    .await?;
                }
            }
            Ok(true)
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Check thread policy while the caller holds the admission lock. Returning
/// a boundary rather than arming here lets resolution keep its FIFO identity.
pub async fn thread_job_refusal(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    kind: HoldKind,
    hold_id: Option<&str>,
    focus_admitted: bool,
) -> Result<Option<(&'static str, String)>, String> {
    thread_refusal(orch, db, job_id, kind, hold_id, focus_admitted, None).await
}

/// [`thread_job_refusal`] over the thread's facts as a pass already read them
/// (`known`), when they are this job's thread's; read afresh otherwise.
async fn thread_refusal(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    kind: HoldKind,
    hold_id: Option<&str>,
    focus_admitted: bool,
    known: Option<std::sync::Arc<ThreadFacts>>,
) -> Result<Option<(&'static str, String)>, String> {
    if !owns_held_job(orch, db, job_id).await? {
        return Err("This execution belongs to another runner; resume it there.".into());
    }
    let execution = db
        .query_opt_text(
            "SELECT execution_id FROM jobs WHERE id=?1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|e| e.to_string())?;
    let Some(execution) = execution else {
        return Ok(None);
    };
    if kind == HoldKind::Launch {
        let job = job_id.to_string();
        let bound = orch.get_settings().max_open_prs_per_thread;
        let facts = known.clone();
        let reason = db
            .read(move |conn| {
                let job = job.clone();
                let facts = facts.clone();
                Box::pin(async move {
                    crate::execution::gates::open_prs::launch_refusal_conn(
                        conn,
                        &job,
                        bound,
                        facts.as_deref(),
                    )
                    .await
                })
            })
            .await
            .map_err(|e| e.to_string())?;
        if let Some(reason) = reason {
            return Ok(Some(("thread_open_prs", reason)));
        }
    }
    let focus_fits = if focus_admitted {
        true
    } else if kind == HoldKind::Launch {
        crate::execution::gates::capacity::focus_admits_launch(
            orch, db, &execution, job_id, hold_id, known,
        )
        .await?
    } else {
        crate::execution::gates::capacity::focus_admits(orch, db, &execution, hold_id, known)
            .await?
    };
    if !focus_fits {
        return Ok(Some((
            "thread_focus",
            "Waiting for a thread focus slot; re-entries precede fresh starts.".into(),
        )));
    }
    Ok(None)
}

/// Arm a no-timer thread hold using the same transactional claim and delivery
/// lifecycle as a supply refusal. `subject` may carry a durable continuation.
pub async fn arm_thread_hold(
    db: &LocalDb,
    job_id: &str,
    kind: HoldKind,
    boundary: &'static str,
    reason: String,
    subject: String,
    now: i64,
) -> Result<ProviderLimitHold, String> {
    let (backend, run_id, turn_id) = db.query_opt(
        "SELECT COALESCE(s.backend,'claude'), (SELECT id FROM runs WHERE job_id=j.id ORDER BY created_at DESC LIMIT 1), j.current_turn_id FROM jobs j LEFT JOIN sessions s ON s.id=j.current_session_id WHERE j.id=?1",
        (job_id.to_string(),), |row| Ok((row.text(0)?,row.opt_text(1)?,row.opt_text(2)?)),
    ).await.map_err(|e| e.to_string())?.ok_or_else(|| "held job disappeared".to_string())?;
    arm_job_hold(
        db,
        SupplyRefusal {
            job_id: job_id.to_string(),
            kind,
            backend,
            run_id,
            turn_id: if kind == HoldKind::Resume {
                turn_id
            } else {
                None
            },
            refusal: CapacityRefusal {
                subject: boundary.to_string(),
                reason,
                retry_at: None,
                retry_account: None,
            },
            now,
        },
        Some(boundary),
        (!subject.is_empty()).then_some(subject),
    )
    .await
}

/// A gate pass is also the boot and periodic backstop for no-timer holds.
/// Resolve oldest first before the same pass considers never-started work.
pub async fn resolve_thread_holds(orch: &Orchestrator, now: i64) -> Result<HoldPassReport, String> {
    let mut held = Vec::new();
    for db in orch.db.all_dbs().await {
        recover_undriven_thread_resumes(orch, &db, now).await?;
        supersede_orphaned_provider_limit_holds(&db, now).await?;
        held.extend(
            load_active_provider_limit_holds(&db)
                .await?
                .into_iter()
                .filter(ProviderLimitHold::is_thread_bound)
                .map(|hold| (db.clone(), hold)),
        );
    }
    Ok(decide_held_work(orch, held, now).await)
}

/// Ask every hold not gated on the operator whether the accounts as they now
/// stand can serve its work, and release those they can. A hold that still
/// cannot run re-arms and stays held. Releases go through the hold's own path
/// (a launch hold to advancement and admission), so a burst queues behind the
/// fleet cap rather than launching at once. No jitter: a supply change has no
/// shared instant to spread.
pub async fn release_holds_on_supply_change(orch: &Orchestrator, now: i64) -> HoldPassReport {
    let mut held = Vec::new();
    for db in orch.db.all_dbs().await {
        if let Err(error) = supersede_orphaned_provider_limit_holds(&db, now).await {
            log::error!("Failed to retire orphaned provider limit holds: {error}");
        }
        match load_active_provider_limit_holds(&db).await {
            Ok(loaded) => held.extend(
                loaded
                    .into_iter()
                    .filter(ProviderLimitHold::releasable)
                    .map(|hold| (db.clone(), hold)),
            ),
            Err(error) => log::error!("Failed to load provider limit holds: {error}"),
        }
    }
    decide_held_work(orch, held, now).await
}

/// How long a thread-bound hold refused on unchanged thread facts goes without
/// being decided again. The backstop for anything a thread's fingerprint does
/// not capture (an operator override on the execution, a band hold's job
/// changing state).
pub const THREAD_HOLD_BACKSTOP_SECS: i64 = 60;

/// Thread-bound holds the last pass refused on their thread's bound: the
/// fingerprint of the thread facts that refused each, and when. Process memory
/// only, so a restart decides every hold afresh.
#[derive(Default)]
pub struct HeldWorkMemo(std::sync::Mutex<HashMap<String, (u64, i64)>>);

impl HeldWorkMemo {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (u64, i64)>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// What one pass over held work did: the line that makes its cost one grep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HoldPassReport {
    /// Holds the pass looked at.
    pub considered: usize,
    /// Thread-bound holds not decided again because nothing their thread's
    /// bound reads had changed since they were last refused.
    pub skipped_unchanged: usize,
    /// Fleet counts the pass took: 0 when no hold reached the fleet cap, 1
    /// otherwise, plus one per admission another site made mid-pass.
    pub fleet_snapshots: usize,
    pub released: usize,
}

/// A pass's fleet count, taken under the admission lock and good for as long
/// as every acquisition since was this pass's own: `admissions` is the lock's
/// total acquisition count as of this pass's latest acquisition.
struct FleetSnapshot {
    working: FleetWorking,
    admissions: u64,
}

/// The state one pass carries from hold to hold.
#[derive(Default)]
struct HoldPass {
    fleet: Option<FleetSnapshot>,
    report: HoldPassReport,
}

impl HoldPass {
    /// Note that this pass has just taken the admission lock. If anyone else
    /// took it since the count was taken, they may have admitted work the
    /// count does not hold, so it is dropped and taken again when next needed.
    fn admitted(&mut self, orch: &Orchestrator) {
        let seen = orch.admission_lock.total_acquisitions();
        if let Some(snapshot) = &mut self.fleet {
            if snapshot.admissions + 1 == seen {
                snapshot.admissions = seen;
            } else {
                self.fleet = None;
            }
        }
    }

    /// The fleet as this pass counts it. Call only under the admission lock.
    async fn fleet(&mut self, orch: &Orchestrator) -> Result<&FleetWorking, String> {
        let snapshot = match self.fleet.take() {
            Some(snapshot) => snapshot,
            None => {
                let working = fleet_working(orch).await?;
                self.report.fleet_snapshots += 1;
                FleetSnapshot {
                    working,
                    admissions: orch.admission_lock.total_acquisitions(),
                }
            }
        };
        Ok(&self.fleet.insert(snapshot).working)
    }

    /// A released hold's execution counts from now on, so it counts before the
    /// next hold is decided.
    fn counted(&mut self, execution: Option<String>) {
        if let (Some(snapshot), Some(execution)) = (&mut self.fleet, execution) {
            snapshot.working.entry(execution).or_insert(false);
        }
    }
}

/// A thread as a pass keys it: the address of the database it lives in, and
/// its id.
type ThreadKey = (usize, String);

/// What a pass knows about one hold's thread: its key, its facts, and the
/// fingerprint of everything its bound reads.
type HoldThread = (ThreadKey, std::sync::Arc<ThreadFacts>, u64);

/// Decide `held` as one pass, in release order (queue key, then id): resumed
/// work ahead of never-started work, each release counting before the next
/// decision. The pass reads each thread's bound facts once, counts the fleet
/// once, and does not decide again a thread-bound hold its thread's facts
/// refused last time when those facts have not changed, for at most
/// [`THREAD_HOLD_BACKSTOP_SECS`].
async fn decide_held_work(
    orch: &Orchestrator,
    mut held: Vec<(std::sync::Arc<LocalDb>, ProviderLimitHold)>,
    now: i64,
) -> HoldPassReport {
    let started = std::time::Instant::now();
    held.sort_by(|(_, a), (_, b)| (a.queue_order(), &a.id).cmp(&(b.queue_order(), &b.id)));
    let threads = thread_facts_by_hold(orch, &held)
        .await
        .unwrap_or_else(|error| {
            log::warn!("Held work pass reads each hold's thread afresh: {error}");
            HashMap::new()
        });
    let prior = orch.held_work_memo.lock().clone();
    let mut refused = HashMap::new();
    let mut released_in: HashSet<ThreadKey> = HashSet::new();
    let mut pass = HoldPass::default();
    for (_, hold) in &held {
        pass.report.considered += 1;
        let thread = threads
            .get(&hold.id)
            .filter(|(key, ..)| !released_in.contains(key));
        if let (Some((_, _, fingerprint)), Some((seen, at))) = (thread, prior.get(&hold.id)) {
            if hold.is_thread_bound()
                && fingerprint == seen
                && now.saturating_sub(*at) < THREAD_HOLD_BACKSTOP_SECS
            {
                pass.report.skipped_unchanged += 1;
                refused.insert(hold.id.clone(), (*seen, *at));
                continue;
            }
        }
        let known = thread.map(|(_, facts, _)| facts.clone());
        match resolve(orch, hold, now, Trigger::SupplyChanged, &mut pass, known).await {
            Ok(Decision::ThreadBound) => {
                if let Some((_, _, fingerprint)) = thread {
                    refused.insert(hold.id.clone(), (*fingerprint, now));
                }
            }
            Ok(Decision::Released) => {
                pass.report.released += 1;
                if let Some((key, ..)) = threads.get(&hold.id) {
                    released_in.insert(key.clone());
                }
            }
            Ok(Decision::Kept) => {}
            Err(error) => log::error!("Hold {} could not release: {error}", hold.id),
        }
    }
    *orch.held_work_memo.lock() = refused;
    log::debug!(
        "held work pass holds={} skipped_unchanged={} fleet_snapshots={} released={} duration_ms={}",
        pass.report.considered,
        pass.report.skipped_unchanged,
        pass.report.fleet_snapshots,
        pass.report.released,
        started.elapsed().as_millis()
    );
    pass.report
}

/// Each thread-bound hold's thread, read once per thread: one query per
/// database maps holds to threads, then one read per thread takes its facts.
/// A thread's fingerprint covers what its bound reads: those facts, the two
/// bound settings, and the identity and place of every thread-bound hold on it
/// (the re-entry band). A hold whose issue has no thread has no entry.
async fn thread_facts_by_hold(
    orch: &Orchestrator,
    held: &[(std::sync::Arc<LocalDb>, ProviderLimitHold)],
) -> Result<HashMap<String, HoldThread>, String> {
    use std::hash::{Hash, Hasher};
    let settings = orch.get_settings();
    let bounds = (
        settings.max_open_prs_per_thread,
        settings.max_open_children_per_thread,
    );
    let holds: HashMap<&str, &ProviderLimitHold> = held
        .iter()
        .map(|(_, hold)| (hold.id.as_str(), hold))
        .collect();
    let mut dbs: Vec<&std::sync::Arc<LocalDb>> = Vec::new();
    for (db, hold) in held {
        if hold.is_thread_bound() && !dbs.iter().any(|known| std::sync::Arc::ptr_eq(known, db)) {
            dbs.push(db);
        }
    }
    // Per thread: its holds as (id, open issue), in a stable order.
    let mut members: BTreeMap<ThreadKey, Vec<(String, bool)>> = BTreeMap::new();
    let mut by_thread: HashMap<ThreadKey, std::sync::Arc<LocalDb>> = HashMap::new();
    for db in dbs {
        let address = std::sync::Arc::as_ptr(db) as usize;
        let rows = db
            .query_all(
                "SELECT h.id, i.parent_thread_id, (i.merged_at IS NULL AND i.closed_at IS NULL)
                   FROM provider_limit_holds h JOIN jobs j ON j.id = h.job_id
                   LEFT JOIN executions e ON e.id = j.execution_id
                   JOIN issues i ON i.id = COALESCE(j.issue_id, e.issue_id)
                  WHERE h.state = 'active' AND h.boundary_source IN ('thread_focus', 'thread_open_prs')
                    AND i.parent_thread_id IS NOT NULL",
                (),
                |row| Ok((row.text(0)?, row.text(1)?, row.i64(2)? != 0)),
            )
            .await
            .map_err(|error| format!("failed to map holds to threads: {error}"))?;
        for (hold, thread, open) in rows {
            if !holds.contains_key(hold.as_str()) {
                continue;
            }
            let key = (address, thread);
            by_thread.entry(key.clone()).or_insert_with(|| db.clone());
            members.entry(key).or_default().push((hold, open));
        }
    }
    let mut out = HashMap::new();
    // One observation of this host's work for every thread the pass reads.
    let observer = match members.is_empty() {
        true => None,
        false => Some(crate::execution::jobs::liveness::WorkObserver::read(orch).await?),
    };
    for (key, mut thread_holds) in members {
        let (Some(db), Some(observer)) = (by_thread.get(&key), observer.clone()) else {
            continue;
        };
        let thread_id = key.1.clone();
        let facts = db
            .read(move |conn| {
                let (thread_id, observer) = (thread_id.clone(), observer.clone());
                Box::pin(async move {
                    crate::execution::gates::capacity::thread_facts_conn(
                        conn, &thread_id, &observer,
                    )
                    .await
                })
            })
            .await
            .map_err(|error| format!("failed to read thread {}: {error}", key.1))?;
        thread_holds.sort();
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        facts.hash(&mut hasher);
        bounds.hash(&mut hasher);
        for (id, open) in &thread_holds {
            let hold = holds[id.as_str()];
            (
                id,
                open,
                hold.queue_order(),
                hold.kind.as_str(),
                &hold.boundary_source,
                hold.updated_at,
            )
                .hash(&mut hasher);
        }
        let fingerprint = hasher.finish();
        let facts = std::sync::Arc::new(facts);
        for (id, _) in thread_holds {
            out.insert(id, (key.clone(), facts.clone(), fingerprint));
        }
    }
    Ok(out)
}

const FINISHED_THREAD_WAIT_HOLD: &str = "state='active' AND kind='resume' AND boundary_source IN ('thread_focus','thread_open_prs')
    AND EXISTS (SELECT 1 FROM agent_waits aw WHERE aw.id=json_extract(provider_limit_holds.resume_json,'$.durable_wait')
      AND (aw.state='cancelled' OR (aw.state='resolved' AND NOT EXISTS (
        SELECT 1 FROM turns wt JOIN jobs wj ON wj.current_turn_id=wt.id
         WHERE wt.id=aw.successor_turn_id AND wj.id=provider_limit_holds.job_id
           AND (wt.state='pending' OR (wt.state='cancelled' AND wt.end_reason='launch_refused'))))))";

/// A process can end after claiming a hold but before reserving a successor.
/// The ordinary gate pass recovers that durable obligation, not a separate timer.
async fn recover_undriven_thread_resumes(
    orch: &Orchestrator,
    db: &LocalDb,
    now: i64,
) -> Result<(), String> {
    // Older runners could count successful/no-op wait resolutions as failed
    // launches. Retire those obsolete holds without asking for operator state.
    let finished = db
        .query_opt_i64(
            format!("SELECT 1 FROM provider_limit_holds WHERE {FINISHED_THREAD_WAIT_HOLD} LIMIT 1"),
            (),
        )
        .await
        .map_err(|error| error.to_string())?;
    if finished.is_some() {
        db.execute(
            format!("UPDATE provider_limit_holds SET state='superseded',updated_at=?1 WHERE {FINISHED_THREAD_WAIT_HOLD}"),
            params![now],
        ).await.map_err(|error| error.to_string())?;
    }
    let candidates = db.query_all(
        "SELECT h.id,h.job_id FROM provider_limit_holds h
           JOIN jobs j ON j.id=h.job_id JOIN turns t ON t.id=j.current_turn_id
          WHERE h.state='resolved' AND h.kind='resume' AND h.resume_json IS NOT NULL
            AND h.boundary_source IN ('thread_focus','thread_open_prs')
            AND NOT EXISTS (SELECT 1 FROM agent_waits aw WHERE aw.id=json_extract(h.resume_json,'$.durable_wait')
              AND (aw.state='cancelled' OR (aw.state='resolved' AND NOT (
                aw.successor_turn_id=t.id AND (t.state='pending' OR (t.state='cancelled' AND t.end_reason='launch_refused'))))))
            AND ((t.id=h.turn_id AND t.state NOT IN ('running','pending'))
              OR ((t.id=h.turn_id OR t.predecessor_id=h.turn_id)
                  AND (t.state='pending' OR (t.state='cancelled' AND t.end_reason='launch_refused'))))",
        (), |row| Ok((row.text(0)?,row.text(1)?)),
    ).await.map_err(|error|error.to_string())?;
    for (id, job_id) in candidates {
        if !owns_held_job(orch, db, &job_id).await? {
            continue;
        }
        // Live resolution takes the same admission -> job order. A pass cannot
        // mistake the brief healthy claim-to-successor window for a crash.
        let admission = orch
            .admission_lock
            .acquire(
                crate::execution::gates::capacity::AdmissionSite::HoldRelease,
                job_id.clone(),
            )
            .await
            .map_err(|error| error.to_string())?;
        let task_orch = orch.clone();
        let owning = crate::execution::routing::owning_db_for_job(&orch.db, &job_id)
            .await
            .map_err(|error| error.to_string())?;
        tokio::task::spawn_blocking(move || {
            let mutex = task_orch.job_launch_lock(&job_id);
            let _job = mutex
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            crate::storage::run_db_blocking(|| async move {
                let head = owning
                    .query_opt(
                        "SELECT t.id,t.state,t.run_id,r.started_at FROM jobs j
                       JOIN turns t ON t.id=j.current_turn_id LEFT JOIN runs r ON r.id=t.run_id
                      WHERE j.id=?1",
                        (job_id.clone(),),
                        |row| {
                            Ok((
                                row.text(0)?,
                                row.text(1)?,
                                row.opt_text(2)?,
                                row.opt_i64(3)?,
                            ))
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                let Some((turn, state, run, started)) = head else {
                    return Ok(());
                };
                if state == "pending" {
                    if started.is_some()
                        || run.as_deref().is_some_and(|run| {
                            task_orch.process_state.has_process(run).unwrap_or(false)
                        })
                    {
                        return Ok(());
                    }
                    // Prepared launches live in the runner's own database even
                    // when the job lives on a team replica. Their admission
                    // worker already owns recovery, so leave them untouched.
                    let queued = task_orch
                        .db
                        .local
                        .query_opt_i64(
                            "SELECT 1 FROM prepared_ordinary_launches p
                           JOIN runtime_admission_requests r ON r.request_id=p.request_id
                          WHERE p.job_id=?1 AND p.turn_id=?2
                            AND r.state IN ('queued','leased','starting','active') LIMIT 1",
                            params![job_id, turn],
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    if queued.is_some() {
                        return Ok(());
                    }
                }
                owning
                    .write(move |conn| {
                        let id = id.clone();
                        Box::pin(async move {
                            restore_resume_claim_conn(conn, &id, now, "Runner recovery found an undriven held continuation without a live or queued launch").await?;
                            Ok(())
                        })
                    })
                    .await
                    .map_err(|error| error.to_string())
            })
        })
        .await
        .map_err(|error| error.to_string())??;
        drop(admission);
    }
    Ok(())
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
                conn.on_commit(cairn_db::admission_hint::evidence_changed);
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

/// An active `provider_limit_holds` row that no longer owns what it was armed
/// for. The supersession's read probe and its write both use this one
/// predicate, so they cannot disagree about what is orphaned.
const ORPHANED_HOLD: &str = "state='active' AND (
           EXISTS (SELECT 1 FROM jobs j
             LEFT JOIN executions e ON e.id=j.execution_id
             LEFT JOIN issues i ON i.id=COALESCE(j.issue_id,e.issue_id)
             LEFT JOIN threads th ON th.id=j.thread_id
             WHERE j.id=provider_limit_holds.job_id
               AND (j.status='cancelled' OR i.merged_at IS NOT NULL
                    OR i.closed_at IS NOT NULL OR th.status<>'active'))
           OR (kind='yield' AND NOT EXISTS (
              SELECT 1 FROM jobs j JOIN turns t ON t.id=j.current_turn_id
              WHERE j.id=provider_limit_holds.job_id AND t.id=provider_limit_holds.turn_id
                AND t.state='yielded' AND t.yield_reason='provider_limit'))
           OR (kind='launch' AND NOT EXISTS (
              SELECT 1 FROM jobs j WHERE j.id=provider_limit_holds.job_id AND j.status='pending'))
           OR (kind='resume' AND NOT EXISTS (
              SELECT 1 FROM jobs j WHERE j.id=provider_limit_holds.job_id
                AND j.current_turn_id=provider_limit_holds.turn_id AND j.status<>'cancelled')))";

/// Retire every active hold that no longer owns what it was armed for. A yield
/// hold owns its provider-yielded head, and once an operator continues past it
/// there is nothing left to resolve. A launch hold owns a pending job, which an
/// operator's start, a cancel, or a resolution moves on. A resume hold owns the
/// head it rests on, which a later continue replaces. A timed hold would learn
/// this when it came due, but a gated hold never comes due.
///
/// Every gate pass and scheduler tick asks this of every database, and the
/// answer is almost always none, so it is asked on the read path first and the
/// writer is taken only when something is orphaned. The update re-evaluates
/// the predicate under the write; a hold orphaned after the probe is retired
/// by the next pass.
pub async fn supersede_orphaned_provider_limit_holds(
    db: &LocalDb,
    now: i64,
) -> Result<u64, String> {
    let orphaned = db
        .query_opt_i64(
            format!("SELECT 1 FROM provider_limit_holds WHERE {ORPHANED_HOLD} LIMIT 1"),
            (),
        )
        .await
        .map_err(|error| error.to_string())?;
    if orphaned.is_none() {
        return Ok(0);
    }
    db.execute(
        format!(
            "UPDATE provider_limit_holds SET state='superseded', updated_at=?1 WHERE {ORPHANED_HOLD}"
        ),
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
    override_record: Option<serde_json::Value>,
) -> Result<Option<(ProviderLimitHold, String, String)>, String> {
    let id = id.to_string();
    db.write(move |conn| {
        let id = id.clone();
        let override_record=override_record.clone();
        Box::pin(async move {
            let Some(hold) = load_conn(conn, &id).await? else { return Ok(None) };
            let ready = if forced { hold.releasable() } else { hold.due_at(now).is_some() };
            if !ready || (override_record.is_none() && hold.launch_backoff_until().is_some_and(|at| at > now)) { return Ok(None) }
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
            if let Some(record)=&override_record {
                conn.execute("UPDATE executions SET release_override_json=?1 WHERE id=(SELECT execution_id FROM jobs WHERE id=?2)",params![record.to_string(),hold.job_id.as_str()]).await?;
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
    override_record: Option<serde_json::Value>,
) -> Result<bool, String> {
    let hold = hold.clone();
    db.write(move |conn| {
        let hold = hold.clone();
        let override_record = override_record.clone();
        Box::pin(async move {
            let Some(current) = load_conn(conn, &hold.id).await? else { return Ok(false) };
            if !current.releasable() || (override_record.is_none() && current.launch_backoff_until().is_some_and(|at| at > now)) { return Ok(false) }
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
            if let Some(record) = &override_record {
                conn.execute("UPDATE executions SET release_override_json=?1 WHERE id=(SELECT execution_id FROM jobs WHERE id=?2)",params![record.to_string(),hold.job_id.as_str()]).await?;
                if let Some(wait_id)=hold.resume_json.as_deref().and_then(|json|serde_json::from_str::<serde_json::Value>(json).ok()).and_then(|json|json.get("durable_wait").and_then(|id|id.as_str()).map(str::to_string)) {
                    conn.execute("UPDATE agent_waits SET resume_cause='addressed' WHERE id=?1 AND state='resolving'",params![wait_id]).await?;
                }
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
    error: &str,
) -> Result<bool, String> {
    let hold = hold.clone();
    let successor = successor.to_string();
    let error = error.to_owned();
    db.write(move |conn| {
        let hold = hold.clone();
        let successor = successor.clone();
        let error = error.clone();
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
            conn.on_commit(cairn_db::admission_hint::evidence_changed);
            if changed == 0 { return Ok(false) }
            // Detach the unserved launch from the successor chain so an explicit
            // Continue can still create a successor from the restored head.
            conn.execute(
                "UPDATE jobs SET current_turn_id=?1, updated_at=?2 WHERE id=?3 AND current_turn_id=?4",
                params![turn_id.as_str(), now, hold.job_id.as_str(), successor.as_str()],
            ).await?;
            restore_failed_hold_conn(conn, &hold.id, now, &error).await?;
            Ok(true)
        })
    }).await.map_err(|error| error.to_string())
}

/// Record the actual launch error in the same transaction that restores its
/// owner. Unknown errors remain retryable: repetition is not evidence that a
/// transient condition needs an operator. Only durable terminal session state
/// establishes that another automatic launch cannot succeed.
async fn restore_failed_hold_conn(
    conn: &crate::storage::TrackedConnection,
    hold_id: &str,
    now: i64,
    error: &str,
) -> DbResult<()> {
    let mut rows = conn.query(
        "SELECT s.status,s.terminal_reason,j.current_session_id,s.id FROM provider_limit_holds h
         JOIN jobs j ON j.id=h.job_id LEFT JOIN sessions s ON s.id=j.current_session_id WHERE h.id=?1",
        params![hold_id],
    ).await?;
    let terminal_session = if let Some(row) = rows.next().await? {
        let status = row.opt_text(0)?;
        let reason = row.opt_text(1)?;
        if row.opt_text(2)?.is_some() && row.opt_text(3)?.is_none() {
            Some("the current session is missing".to_string())
        } else if status.as_deref() == Some("failed")
            || (status.as_deref() == Some("closed")
                && reason.as_deref()
                    != Some(crate::orchestrator::lifecycle::ALREADY_TERMINAL_RECONCILED_REASON))
        {
            Some(format!(
                "the current session is {} ({})",
                status.unwrap_or_default(),
                reason.as_deref().unwrap_or("no terminal reason recorded")
            ))
        } else {
            None
        }
    } else {
        None
    };
    drop(rows);
    let permanent = terminal_session.is_some();
    let cause = match terminal_session {
        Some(reason) => format!("{error}; {reason}"),
        None => error.to_string(),
    };
    let failure = serde_json::json!({"at":now,"cause":cause,"retryable":!permanent}).to_string();
    conn.execute(
        "UPDATE provider_limit_holds SET state='active',resolved_at=NULL,
           launch_attempts=launch_attempts+1,retry_not_before=?2,
           resume_gate=CASE WHEN ?3 THEN COALESCE(resume_gate,?4) ELSE resume_gate END,
           launch_failures_json=json_insert(launch_failures_json,'$[#]',json(?5)),updated_at=?6
         WHERE id=?1 AND state='resolved'",
        params![
            hold_id,
            now.saturating_add(300),
            permanent,
            RESUME_GATE_LAUNCH_FAILED,
            failure,
            now
        ],
    )
    .await?;
    Ok(())
}

/// Restore a refused resume while its caller still holds the job launch lock.
/// Only the claimed head or its undriven direct successor may be restored;
/// an addressed continuation or a turn that reached a process is authoritative.
pub(crate) async fn restore_resume_hold_after_launch_failure(
    _orch: &Orchestrator,
    db: &LocalDb,
    hold_id: &str,
    now: i64,
    error: &str,
) -> Result<bool, String> {
    let hold_id = hold_id.to_owned();
    let error = error.to_owned();
    db.write(move |conn| {
        let hold_id = hold_id.clone();
        let error = error.clone();
        Box::pin(async move { restore_resume_claim_conn(conn, &hold_id, now, &error).await })
    })
    .await
    .map_err(|error| error.to_string())
}

async fn restore_resume_claim_conn(
    conn: &crate::storage::TrackedConnection,
    hold_id: &str,
    now: i64,
    error: &str,
) -> DbResult<bool> {
    let Some(hold) = load_conn(conn, hold_id).await? else {
        return Ok(false);
    };
    if hold.kind != HoldKind::Resume || hold.state != "resolved" {
        return Ok(false);
    }
    if job_ineligible_conn(conn, &hold.job_id).await?
        || crate::execution::canonical::fence_for_job_conn(conn, &hold.job_id)
            .await?
            .is_superseded()
    {
        return Ok(false);
    }
    // A subsequent supply refusal owns the retry. Carry the serialized input
    // into that hold rather than creating two owners of the same continuation.
    let mut active = conn
        .query(
            "SELECT id FROM provider_limit_holds WHERE job_id=?1 AND state='active' AND id<>?2",
            params![hold.job_id.as_str(), hold_id],
        )
        .await?;
    let active_id = active.next().await?.map(|row| row.text(0)).transpose()?;
    drop(active);
    if let Some(active_id) = active_id {
        let failure = serde_json::json!({"at":now,"cause":error,"retryable":true}).to_string();
        conn.execute(
            "UPDATE provider_limit_holds SET resume_json=COALESCE(resume_json,?2),
               boundary_source=CASE WHEN ?3 AND COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN ?4 ELSE boundary_source END,
               queue_key=CASE WHEN ?3 AND COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN ?5 ELSE queue_key END,
               created_at=CASE WHEN ?3 AND COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN ?6 ELSE created_at END,
               retry_not_before=CASE WHEN COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN MAX(COALESCE(retry_not_before,0),?7) ELSE retry_not_before END,
               launch_attempts=launch_attempts+CASE WHEN COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN 1 ELSE 0 END,
               launch_failures_json=CASE WHEN COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN json_insert(launch_failures_json,'$[#]',json(?8)) ELSE launch_failures_json END,
               updated_at=CASE WHEN COALESCE(json_extract(resume_json,'$.cause'),'system')<>'addressed' THEN ?9 ELSE updated_at END
             WHERE id=?1",
            params![
                active_id,
                hold.resume_json.as_deref(),
                hold.is_thread_bound(),
                hold.boundary_source.as_str(),
                hold.queue_key
                    .unwrap_or_else(|| hold.created_at.saturating_mul(1_000_000)),
                hold.created_at,
                now.saturating_add(300),
                failure.as_str(),
                now,
            ],
        )
        .await?;
        conn.execute(
            "UPDATE provider_limit_holds SET state='superseded',launch_attempts=launch_attempts+1,
               launch_failures_json=json_insert(launch_failures_json,'$[#]',json(?2)),updated_at=?3 WHERE id=?1 AND state='resolved'",
            params![hold_id,failure,now],
        ).await?;
        return Ok(true);
    }
    let Some(original) = hold.turn_id.as_deref() else {
        return Ok(false);
    };
    let mut heads = conn
        .query(
            "SELECT t.id,t.predecessor_id,t.state,t.end_reason FROM jobs j
           JOIN turns t ON t.id=j.current_turn_id WHERE j.id=?1",
            params![hold.job_id.as_str()],
        )
        .await?;
    let Some(head) = heads.next().await? else {
        return Ok(false);
    };
    let current = head.text(0)?;
    let predecessor = head.opt_text(1)?;
    let state = head.text(2)?;
    let end_reason = head.opt_text(3)?;
    drop(heads);
    let durable_wait = hold
        .resume_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .and_then(|value| {
            value
                .get("durable_wait")
                .and_then(|id| id.as_str())
                .map(str::to_owned)
        });
    if let Some(wait_id) = durable_wait.as_deref() {
        let mut waits = conn
            .query(
                "SELECT state,successor_turn_id FROM agent_waits WHERE id=?1",
                params![wait_id],
            )
            .await?;
        let wait = waits
            .next()
            .await?
            .map(|row| Ok::<_, DbError>((row.text(0)?, row.opt_text(1)?)))
            .transpose()?;
        drop(waits);
        if let Some((wait_state, successor)) = wait {
            if wait_state == "cancelled" {
                return Ok(false);
            }
            if wait_state == "resolved" {
                // Queued cold admission can fail after the wait resolver has
                // finished. Only its still-undriven successor justifies replay;
                // a completed successor or a resolved collision owes no launch.
                if successor.as_deref() != Some(current.as_str())
                    || !(state == "pending"
                        || (state == "cancelled"
                            && end_reason.as_deref() == Some("launch_refused")))
                {
                    return Ok(false);
                }
                conn.execute("UPDATE agent_waits SET state='resolving',resolved_at=NULL WHERE id=?1 AND state='resolved'", params![wait_id]).await?;
            }
        }
    }
    let mut resting_head = original.to_owned();
    if current == original && (state == "running" || (state == "pending" && durable_wait.is_none()))
    {
        return Ok(false);
    }
    if current != original {
        if predecessor.as_deref() != Some(original)
            || !(state == "pending"
                || (state == "cancelled" && end_reason.as_deref() == Some("launch_refused")))
        {
            return Ok(false);
        }
        crate::orchestrator::attention_push::revert_turn_deliveries_conn(
            conn,
            &current,
            crate::orchestrator::attention_push::TurnDelivery::NeverSent,
        )
        .await?;
        conn.execute(
            "UPDATE runs SET status='exited',exit_reason='launch_superseded',exited_at=?2,updated_at=?2
              WHERE id=(SELECT run_id FROM turns WHERE id=?1) AND status='starting' AND started_at IS NULL",
            params![current.as_str(),now],
        ).await?;
        if let Some(wait_id) = durable_wait {
            // The wait owns its one successor durably. Re-drive that same turn
            // and keep its result-delivery stamp; no tool result is appended twice.
            let changed = conn
                .execute(
                    "UPDATE turns SET state='pending',end_reason=NULL,ended_at=NULL,updated_at=?2
                  WHERE id=?1 AND EXISTS (SELECT 1 FROM agent_waits
                    WHERE id=?3 AND state='resolving' AND successor_turn_id=?1)",
                    params![current.as_str(), now, wait_id],
                )
                .await?;
            if changed == 0 {
                return Ok(false);
            }
            resting_head = current;
        } else {
            conn.execute(
                "UPDATE turns SET state='cancelled',end_reason='launch_refused',ended_at=?2,
                   updated_at=?2,predecessor_id=NULL WHERE id=?1",
                params![current.as_str(), now],
            )
            .await?;
            conn.execute(
                "UPDATE jobs SET current_turn_id=?2,updated_at=?3 WHERE id=?1 AND current_turn_id=?4",
                params![hold.job_id.as_str(),original,now,current.as_str()],
            ).await?;
        }
    }
    conn.execute(
        "UPDATE provider_limit_holds SET turn_id=?2 WHERE id=?1 AND state='resolved'",
        params![hold_id, resting_head],
    )
    .await?;
    restore_failed_hold_conn(conn, hold_id, now, error).await?;
    conn.on_commit(cairn_db::admission_hint::evidence_changed);
    Ok(true)
}

/// Cold admission can fail after the resolver returned: queued launches belong
/// to the admission worker, not the original call stack. Recover its pending
/// provider successor before generic failure projection can settle the DAG.
/// Durable-only: the caller holds the job launch lock across this call, then
/// publishes and recomputes after releasing it.
pub(crate) async fn restore_provider_hold_after_admission_failure(
    db: &LocalDb,
    successor: &str,
    now: i64,
    error: &str,
) -> Result<bool, String> {
    let successor_id = successor.to_string();
    let hold = db.read(move |conn| {
        let successor_id = successor_id.clone();
        Box::pin(async move {
            let mut rows = conn.query(
                "SELECT h.id FROM provider_limit_holds h
                 JOIN turns t ON t.job_id=h.job_id AND (t.predecessor_id=h.turn_id OR
                   (h.kind='resume' AND t.id=h.turn_id AND EXISTS (
                     SELECT 1 FROM agent_waits aw WHERE aw.job_id=h.job_id AND aw.successor_turn_id=t.id
                       AND aw.id=json_extract(h.resume_json,'$.durable_wait'))))
                 WHERE t.id=?1 AND h.state='resolved'
                   AND ((h.kind='yield' AND t.start_reason='wait_resolved') OR h.kind='resume')
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
    if hold.kind == HoldKind::Resume {
        // The caller holds the job launch lock, so no successor can launch
        // between this fence and the restore. Restore only while this exact
        // successor is still the job's head; an operator's newer head wins.
        let (hold_id, job_id, successor) =
            (hold.id.clone(), hold.job_id.clone(), successor.to_owned());
        let error = error.to_owned();
        db.write(move |conn| {
            let error = error.clone();
            let (hold_id, job_id, successor) = (hold_id.clone(), job_id.clone(), successor.clone());
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT 1 FROM jobs WHERE id=?1 AND current_turn_id=?2",
                        params![job_id, successor],
                    )
                    .await?;
                let ours = rows.next().await?.is_some();
                drop(rows);
                if ours {
                    restore_resume_claim_conn(conn, &hold_id, now, &error).await?;
                }
                Ok(())
            })
        })
        .await
        .map_err(|error| error.to_string())?;
        return Ok(true);
    }
    restore_after_launch_failure(db, &hold, successor, now, error).await?;
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
    let thread_boundary = hold.is_thread_bound();
    db.write(move |conn| {
        let id = id.clone();
        let account = account.clone();
        let reason = reason.clone();
        Box::pin(async move {
            let source = if !supply || thread_boundary {
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
                params![
                    if thread_boundary { None } else { at },
                    account,
                    now,
                    id.as_str(),
                    source,
                    reason
                ],
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
    Operator,
}

impl Trigger {
    fn label(self, hold: &ProviderLimitHold) -> &'static str {
        match self {
            Self::SupplyChanged => "supply_changed",
            Self::Operator => "start_now",
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
    resolve(
        orch,
        hold,
        now,
        Trigger::Timer,
        &mut HoldPass::default(),
        None,
    )
    .await
    .map(|decision| decision == Decision::Released)
}

/// Ask a hold, whatever its clock says, whether the accounts as they now stand
/// can serve its work, and release it through the same path a reset does when
/// they can. A hold gated on the operator is never released this way.
pub async fn release_on_supply_change(
    orch: &Orchestrator,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<bool, String> {
    resolve(
        orch,
        hold,
        now,
        Trigger::SupplyChanged,
        &mut HoldPass::default(),
        None,
    )
    .await
    .map(|decision| decision == Decision::Released)
}

/// Operator-authorized Start now, with the same hold claim as automatic
/// release and provenance committed atomically with that claim.
pub async fn start_now_thread_hold(
    orch: &Orchestrator,
    hold_id: &str,
    override_record: serde_json::Value,
) -> Result<bool, String> {
    for db in orch.db.all_dbs().await {
        let id = hold_id.to_string();
        let hold = db
            .read(move |conn| {
                let id = id.clone();
                Box::pin(async move { load_conn(conn, &id).await })
            })
            .await
            .map_err(|e| e.to_string())?;
        let Some(hold) = hold else { continue };
        if !owns_held_job(orch, &db, &hold.job_id).await? {
            return Ok(false);
        }
        if !hold.is_thread_bound() || !hold.releasable() {
            return Ok(false);
        }
        let guard = orch
            .admission_lock
            .acquire(
                crate::execution::gates::capacity::AdmissionSite::HoldRelease,
                hold.job_id.clone(),
            )
            .await
            .map_err(|e| format!("start now admission busy: {e:?}"))?;
        if hold.kind == HoldKind::Yield {
            return resolve_yield(
                orch,
                &db,
                &hold,
                orch.services.clock.now(),
                Trigger::Operator,
                guard,
                Some(override_record),
            )
            .await;
        }
        return resolve_supply_hold(
            orch,
            &db,
            &hold,
            orch.services.clock.now(),
            Trigger::Operator,
            guard,
            Some(override_record),
        )
        .await;
    }
    Ok(false)
}

/// What deciding one hold came to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Decision {
    Released,
    /// Refused by its thread's bound, read before anything else was asked.
    ThreadBound,
    /// Still held for any other reason, or not this runner's to decide.
    Kept,
}

/// Decide one hold. A thread-bound hold asks its thread's bound first, from
/// `known` when the pass read its thread: that is the question that refuses it
/// almost every time, and it needs neither a supply probe nor a fleet count.
/// Only work the thread would admit goes on to supply, then to the fleet cap
/// under the admission lock, where the thread's bound is asked again afresh
/// before anything is released.
async fn resolve(
    orch: &Orchestrator,
    hold: &ProviderLimitHold,
    now: i64,
    trigger: Trigger,
    pass: &mut HoldPass,
    known: Option<std::sync::Arc<ThreadFacts>>,
) -> Result<Decision, String> {
    let db = crate::execution::routing::owning_db_for_job(&orch.db, &hold.job_id)
        .await
        .map_err(|error| error.to_string())?;
    if !owns_held_job(orch, &db, &hold.job_id).await? {
        return Ok(Decision::Kept);
    }
    let ready = match trigger {
        Trigger::Timer => hold.due_at(now).is_some(),
        Trigger::SupplyChanged | Trigger::Operator => hold.releasable(),
    };
    if !ready
        || (trigger != Trigger::Operator && hold.launch_backoff_until().is_some_and(|at| at > now))
    {
        return Ok(Decision::Kept);
    }
    if crate::execution::canonical::job_refusal(&db, &hold.job_id).is_some() {
        let id = hold.id.clone();
        db.write(move |conn| {
            let id = id.clone();
            Box::pin(async move { set_state_conn(conn, &id, "superseded", now).await })
        })
        .await
        .map_err(|error| error.to_string())?;
        return Ok(Decision::Kept);
    }
    let addressed = hold
        .resume_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .is_some_and(|v| v.get("cause").and_then(|c| c.as_str()) == Some("addressed"));
    if hold.is_thread_bound() {
        // Refusing reads no count that a refusal could overrun, so it needs no
        // lock; a stale read here only sends work on to the check under it.
        if let Some((boundary, reason)) = thread_refusal(
            orch,
            &db,
            &hold.job_id,
            hold.kind,
            Some(&hold.id),
            addressed,
            known,
        )
        .await?
        {
            keep_thread_bound(&db, hold, boundary, reason, now).await?;
            return Ok(Decision::ThreadBound);
        }
    }
    if !hold_can_launch(orch, &db, hold, now).await? {
        return Ok(Decision::Kept);
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
            return Ok(Decision::Kept);
        }
    };
    pass.admitted(orch);
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
    if let (false, Some(execution)) = (addressed, execution.as_deref()) {
        let behind_its_lane = hold.kind == HoldKind::Launch && !hold.is_thread_bound();
        let working = pass.fleet(orch).await?;
        if !held_work_fits(orch, &db, working, execution, behind_its_lane).await? {
            wait_for_fleet_slot(&db, hold, now).await?;
            return Ok(Decision::Kept);
        }
    }
    if let Some((boundary, reason)) = thread_job_refusal(
        orch,
        &db,
        &hold.job_id,
        hold.kind,
        Some(&hold.id),
        addressed,
    )
    .await?
    {
        keep_thread_bound(&db, hold, boundary, reason, now).await?;
        return Ok(Decision::Kept);
    }
    let released = match hold.kind {
        HoldKind::Yield => resolve_yield(orch, &db, hold, now, trigger, guard, None).await?,
        HoldKind::Launch | HoldKind::Resume => {
            resolve_supply_hold(orch, &db, hold, now, trigger, guard, None).await?
        }
    };
    if !released {
        return Ok(Decision::Kept);
    }
    pass.counted(execution);
    Ok(Decision::Released)
}

/// Record that a hold waits on its thread's bound, and why, giving it a place
/// in the thread's line if it has none. A hold that already says exactly this
/// is left untouched, so a refusal repeated pass after pass writes nothing.
async fn keep_thread_bound(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    boundary: &'static str,
    reason: String,
    now: i64,
) -> Result<(), String> {
    if hold.boundary_source == boundary
        && hold.observed_text.as_deref() == Some(reason.as_str())
        && hold.retry_not_before.is_none()
        && hold.queue_key.is_some()
    {
        return Ok(());
    }
    let id = hold.id.clone();
    db.write(move |conn| {let id=id.clone();let reason=reason.clone();Box::pin(async move {
        conn.execute("UPDATE provider_limit_holds SET boundary_source=?1,retry_not_before=NULL,observed_text=?2,updated_at=?3,queue_key=COALESCE(queue_key,MAX(?5,COALESCE((SELECT MAX(COALESCE(h.queue_key,h.created_at*1000000)) FROM provider_limit_holds h WHERE h.state='active' AND h.boundary_source IN ('thread_focus','thread_open_prs') AND h.id<>?4),0)+1)) WHERE id=?4 AND state='active'",params![boundary,reason,now,id.as_str(),now.saturating_mul(1_000_000)]).await?;
        Ok(())
    })}).await.map_err(|e|e.to_string())
}

/// Keep a hold whose work supply can serve again held until a fleet slot is
/// free, asking again after [`FLEET_SLOT_RECHECK_SECS`].
async fn wait_for_fleet_slot(
    db: &LocalDb,
    hold: &ProviderLimitHold,
    now: i64,
) -> Result<(), String> {
    if hold.is_thread_bound() {
        // Preserve the re-entry band's identity while a fleet slot is busy.
        // The periodic/boot pass, not a per-hold timer, retries this hold.
        return Ok(());
    }
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
    // Handed to the continue, which lets it go once the live turn that makes
    // the job count is recorded, so no other cap decision reads the fleet in
    // between.
    admission: crate::execution::gates::capacity::AdmissionGuard,
    override_record: Option<serde_json::Value>,
) -> Result<bool, String> {
    let Some((hold, successor, session)) = claim(
        db,
        &hold.id,
        now,
        trigger != Trigger::Timer,
        override_record,
    )
    .await?
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
    let result = crate::execution::jobs::continue_job_impl_admitted(
        Some(admission),
        if trigger == Trigger::Operator {
            crate::execution::jobs::WakeCause::Addressed
        } else {
            crate::execution::jobs::WakeCause::System
        },
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
        restore_after_launch_failure(db, &hold, &successor, now, &error).await?;
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
    override_record: Option<serde_json::Value>,
) -> Result<bool, String> {
    if !claim_supply_hold(db, hold, now, override_record).await? {
        return Ok(false);
    }
    // A released launch hold's job is pending and unheld, which the fleet
    // already counts, so advancement runs after the lock is let go. A resume
    // counts only once its continue records a live turn, so the lock is handed
    // to the continue, which lets it go there.
    let admission = (hold.kind == HoldKind::Resume).then_some(admission);
    if let Some(run_id) = hold.run_id.as_deref() {
        let note = match trigger {
            Trigger::SupplyChanged => "An account can serve this node again, so it resumes now.",
            Trigger::Operator => "The operator started this held node now.",
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
            let resumed = if let Some(resume_json) = hold.resume_json.as_deref() {
                let wait_id = serde_json::from_str::<serde_json::Value>(resume_json)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("durable_wait")
                            .and_then(|id| id.as_str())
                            .map(str::to_owned)
                    });
                if let Some(wait_id) = wait_id {
                    crate::mcp::handlers::durable_suspend::resume_focus_wait(
                        orch, db, &wait_id, &hold.id, admission,
                    )
                    .await
                } else {
                    crate::execution::jobs::resume_held_continuation(
                        orch,
                        &hold.job_id,
                        resume_json,
                        &hold.id,
                        if trigger == Trigger::Operator {
                            crate::execution::jobs::WakeCause::Addressed
                        } else {
                            crate::execution::jobs::WakeCause::System
                        },
                        admission,
                    )
                    .map(|_| ())
                }
            } else {
                crate::execution::jobs::continue_job_impl_admitted(
                    admission,
                    if trigger == Trigger::Operator {
                        crate::execution::jobs::WakeCause::Addressed
                    } else {
                        crate::execution::jobs::WakeCause::System
                    },
                    orch,
                    &hold.job_id,
                    None,
                    None,
                    Some(ResumeContext {
                        resolving_hold_id: Some(hold.id.clone()),
                        suppress_user_event: true,
                        ..Default::default()
                    }),
                )
                .map(|_| ())
            };
            if let Err(error) = resumed {
                // Decoding or wait lookup may fail before continuation's in-lock
                // failure guard runs. This fallback restores only an unchanged
                // head; it cannot rewind a concurrent addressed continuation.
                let id = hold.id.clone();
                let failure = error.clone();
                db.write(move |conn| {
                    let id = id.clone();
                    let failure = failure.clone();
                    Box::pin(async move {
                        let mut rows = conn
                            .query(
                                "SELECT 1 FROM provider_limit_holds h JOIN jobs j ON j.id=h.job_id
                              WHERE h.id=?1 AND j.current_turn_id=h.turn_id",
                                params![id.as_str()],
                            )
                            .await?;
                        let unchanged = rows.next().await?.is_some();
                        drop(rows);
                        if unchanged {
                            restore_resume_claim_conn(conn, &id, now, &failure).await?;
                        }
                        Ok(())
                    })
                })
                .await
                .map_err(|restore| {
                    format!("resume failed: {error}; hold restoration failed: {restore}")
                })?;
                return Err(format!(
                    "Held continuation {} could not launch: {error}",
                    hold.id
                ));
            }
            Ok(true)
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
                        crate::effects::types::WorkflowEffect::StartAgentJobs {
                            jobs: ready,
                            cause: if trigger == Trigger::Operator {
                                crate::execution::jobs::WakeCause::Addressed
                            } else {
                                crate::execution::jobs::WakeCause::System
                            },
                            focus_admitted: true,
                        },
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
            launch_failures_json: "[]".into(),
            state: "active".into(),
            created_at: 1,
            updated_at: 1,
            resolved_at: None,
            resume_gate: None,
            boundary_account_id: None,
            subject: None,
            queue_key: None,
            resume_json: None,
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

    async fn restore_resume(db: &LocalDb, id: &str) -> bool {
        let id = id.to_owned();
        db.write(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                restore_resume_claim_conn(conn, &id, NOW + 1, "temporary launch refusal").await
            })
        })
        .await
        .unwrap()
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
        let payload =
            serde_json::json!({"cause":"addressed","message":"retained-inline-input"}).to_string();
        let captured =
            arm_supply_resume_hold(&db, refused("r", HoldKind::Resume, None), payload.clone())
                .await
                .unwrap();
        assert_eq!(captured.id, hold.id);
        assert_eq!(
            captured.resume_json.as_deref(),
            Some(payload.as_str()),
            "input and cause publish in the same arm transaction"
        );
        assert_eq!(hold.turn_id.as_deref(), Some("head"));
        assert_eq!(
            job(&db, "r").await,
            ("completed".into(), Some("claude/opus".into()), None)
        );
        db.execute(
            "UPDATE provider_limit_holds SET state='resolved',resolved_at=?2 WHERE id=?1",
            params![hold.id.as_str(), NOW],
        )
        .await
        .unwrap();
        assert!(
            restore_resume(&db, &hold.id).await,
            "failure before successor leaves a retryable hold"
        );
        assert_eq!(
            active_supply_hold(&db, "r")
                .await
                .unwrap()
                .unwrap()
                .turn_id
                .as_deref(),
            Some("head")
        );

        db.execute_script(
            "INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,end_reason,created_at,updated_at)
               VALUES('unserved','session','r',2,'head','cancelled','user','launch_refused',2,2);
             UPDATE jobs SET current_turn_id='unserved' WHERE id='r';"
        ).await.unwrap();
        db.execute(
            "UPDATE provider_limit_holds SET state='resolved' WHERE id=?1",
            (hold.id.clone(),),
        )
        .await
        .unwrap();
        assert!(restore_resume(&db, &hold.id).await);
        assert_eq!(
            db.query_opt_text("SELECT current_turn_id FROM jobs WHERE id='r'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("head")
        );
        assert!(db
            .query_opt_text("SELECT predecessor_id FROM turns WHERE id='unserved'", ())
            .await
            .unwrap()
            .is_none());

        db.execute_script(
            "INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at)
               VALUES('operator-head','session','r',3,'head','running','user',3,3);
             UPDATE jobs SET current_turn_id='operator-head' WHERE id='r';"
        ).await.unwrap();
        db.execute(
            "UPDATE provider_limit_holds SET state='resolved' WHERE id=?1",
            (hold.id.clone(),),
        )
        .await
        .unwrap();
        assert!(
            !restore_resume(&db, &hold.id).await,
            "never rewind an addressed running head"
        );

        db.execute_script(
            "UPDATE turns SET predecessor_id=NULL WHERE id='operator-head';
             INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,end_reason,created_at,updated_at)
               VALUES('wait-head','session','r',4,'head','cancelled','wait_resolved','launch_refused',4,4);
             UPDATE jobs SET current_turn_id='wait-head' WHERE id='r';
             INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,successor_turn_id,result_stored_at)
               VALUES('owned-wait','r','run','session','head','call','{}','resolving',1,'wait-head',1);"
        ).await.unwrap();
        db.execute(
            "UPDATE provider_limit_holds SET state='resolved',resume_json=?2 WHERE id=?1",
            params![hold.id.as_str(), r#"{"durable_wait":"owned-wait"}"#],
        )
        .await
        .unwrap();
        assert!(restore_resume(&db, &hold.id).await);
        let restored = active_supply_hold(&db, "r").await.unwrap().unwrap();
        assert_eq!(restored.turn_id.as_deref(), Some("wait-head"));
        assert_eq!(
            db.query_opt_text("SELECT state FROM turns WHERE id='wait-head'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("pending")
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT result_stored_at FROM agent_waits WHERE id='owned-wait'",
                ()
            )
            .await
            .unwrap(),
            Some(1)
        );
        assert_eq!(
            restored.created_at, hold.created_at,
            "retry keeps queue age"
        );
        for cause in ["system", "addressed"] {
            db.execute(
                "UPDATE provider_limit_holds SET state='resolved',boundary_source='thread_focus',queue_key=17 WHERE id=?1",
                (hold.id.clone(),),
            ).await.unwrap();
            let input = serde_json::json!({"cause":cause,"message":"newer-input"}).to_string();
            db.execute(
                "INSERT INTO provider_limit_holds(id,job_id,turn_id,kind,backend,boundary_source,retry_not_before,state,created_at,updated_at,resume_json)
                 VALUES('replacement','r','wait-head','resume','claude','recheck',?1,'active',?2,?2,?3)",
                params![NOW+400,NOW+2,input.as_str()],
            ).await.unwrap();
            assert!(restore_resume(&db, &hold.id).await);
            let replacement = active_supply_hold(&db, "r").await.unwrap().unwrap();
            assert_eq!(replacement.resume_json.as_deref(), Some(input.as_str()));
            if cause == "addressed" {
                assert_eq!(replacement.boundary_source, "recheck");
                assert_eq!(replacement.retry_not_before, Some(NOW + 400));
                assert_eq!(replacement.created_at, NOW + 2);
                assert_eq!(replacement.queue_key, None);
            } else {
                assert_eq!(replacement.boundary_source, "thread_focus");
                assert_eq!(replacement.retry_not_before, Some(NOW + 400));
                assert_eq!(replacement.created_at, hold.created_at);
                assert_eq!(replacement.queue_key, Some(17));
                assert_eq!(replacement.launch_backoff_until(), Some(NOW + 301));
                assert!(!claim_supply_hold(&db, &replacement, NOW + 300, None)
                    .await
                    .unwrap());
                assert!(claim_supply_hold(&db, &replacement, NOW + 301, None)
                    .await
                    .unwrap());
            }
            db.execute(
                "DELETE FROM provider_limit_holds WHERE id='replacement'",
                (),
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn finished_waits_never_become_failed_launches() {
        use crate::db::DbState;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;

        for (wait_state, successor) in [
            ("resolved", Some("head")),
            ("resolved", None),
            ("cancelled", None),
        ] {
            let root = tempfile::tempdir().unwrap();
            let db = Arc::new(seeded("finished-focus-wait").await);
            let state = Arc::new(DbState::new(
                db.clone(),
                Arc::new(SearchIndex::open_or_create(root.path().join("search")).unwrap()),
            ));
            let orch = Orchestrator::builder(
                state,
                Arc::new(TestServicesBuilder::new().build()),
                root.path().to_path_buf(),
            )
            .build();
            let held = arm_supply_resume_hold(
                &db,
                refused("r", HoldKind::Resume, None),
                r#"{"durable_wait":"wait"}"#.into(),
            )
            .await
            .unwrap();
            db.execute(
                "INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,successor_turn_id,result_stored_at)
                 VALUES('wait','r','run','session','head','call','{}',?1,1,?2,1)",
                params![wait_state,successor],
            ).await.unwrap();
            db.execute(
                "UPDATE provider_limit_holds SET state='resolved',boundary_source='thread_focus',queue_key=17 WHERE id=?1",
                params![held.id.as_str()],
            ).await.unwrap();
            for _ in 0..4 {
                assert!(!restore_resume(&db, &held.id).await);
                recover_undriven_thread_resumes(&orch, &db, NOW)
                    .await
                    .unwrap();
            }
            let row = db.query_one(
                "SELECT state,launch_attempts,launch_failures_json FROM provider_limit_holds WHERE id=?1",
                params![held.id.as_str()],
                |row| Ok((row.text(0)?,row.i64(1)?,row.text(2)?)),
            ).await.unwrap();
            assert_eq!(
                row,
                ("resolved".into(), 0, "[]".into()),
                "{wait_state} {successor:?}"
            );
            // Repair an obsolete attempt-count gate without operator-written state.
            db.execute("UPDATE provider_limit_holds SET state='active',resume_gate='launch_failed',launch_attempts=3 WHERE id=?1", params![held.id.as_str()]).await.unwrap();
            recover_undriven_thread_resumes(&orch, &db, NOW)
                .await
                .unwrap();
            assert_eq!(
                db.query_opt_text(
                    "SELECT state FROM provider_limit_holds WHERE id=?1",
                    params![held.id.as_str()]
                )
                .await
                .unwrap()
                .as_deref(),
                Some("superseded")
            );
        }
    }

    #[tokio::test]
    async fn transient_failures_keep_the_queue_and_history_without_a_terminal_gate() {
        let db = seeded("transient-focus-failures").await;
        let held = arm_supply_resume_hold(
            &db,
            refused("r", HoldKind::Resume, None),
            r#"{"cause":"system","message":"kept input"}"#.into(),
        )
        .await
        .unwrap();
        db.execute("UPDATE provider_limit_holds SET boundary_source='thread_focus',queue_key=17 WHERE id=?1",params![held.id.as_str()]).await.unwrap();
        for (index, cause) in [
            "launch contention",
            "capacity unavailable",
            "runner restarted",
        ]
        .into_iter()
        .enumerate()
        {
            let now = NOW + (index as i64) * 300;
            let hold = active_supply_hold(&db, "r").await.unwrap().unwrap();
            assert!(claim_supply_hold(&db, &hold, now, None).await.unwrap());
            let id = held.id.clone();
            db.write(move |conn| {
                let id = id.clone();
                Box::pin(async move { restore_resume_claim_conn(conn, &id, now, cause).await })
            })
            .await
            .unwrap();
            let restored = active_supply_hold(&db, "r").await.unwrap().unwrap();
            assert!(restored.releasable());
            assert_eq!(restored.resume_gate, None);
            assert_eq!(restored.launch_attempts, index as i64 + 1);
            assert_eq!(restored.retry_not_before, Some(now + 300));
            assert_eq!(restored.launch_backoff_until(), Some(now + 300));
            assert!(
                !claim_supply_hold(&db, &hold, now + 299, None)
                    .await
                    .unwrap(),
                "a stale release observation cannot bypass the durable backoff"
            );
            assert_eq!(restored.queue_key, Some(17));
            assert_eq!(restored.created_at, held.created_at);
            assert_eq!(restored.resume_json, held.resume_json);
            let failures: Vec<serde_json::Value> =
                serde_json::from_str(&restored.launch_failures_json).unwrap();
            assert_eq!(failures.len(), index + 1);
            assert_eq!(failures.last().unwrap()["cause"], cause);
            assert_eq!(failures.last().unwrap()["retryable"], true);
        }
        let hold = active_supply_hold(&db, "r").await.unwrap().unwrap();
        assert!(claim_supply_hold(&db, &hold, NOW + 900, None)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn a_terminal_session_gates_with_its_actual_cause() {
        let db = seeded("terminal-hold-session").await;
        let held = arm_supply_hold(&db, refused("r", HoldKind::Resume, None))
            .await
            .unwrap();
        db.execute_script("UPDATE jobs SET current_session_id='session' WHERE id='r'; UPDATE sessions SET status='closed',terminal_reason='node_removed' WHERE id='session';").await.unwrap();
        assert!(claim_supply_hold(&db, &held, NOW, None).await.unwrap());
        assert!(restore_resume(&db, &held.id).await);
        let restored = active_supply_hold(&db, "r").await.unwrap().unwrap();
        assert_eq!(
            restored.resume_gate.as_deref(),
            Some(RESUME_GATE_LAUNCH_FAILED)
        );
        let failures: Vec<serde_json::Value> =
            serde_json::from_str(&restored.launch_failures_json).unwrap();
        assert_eq!(failures[0]["retryable"], false);
        assert!(failures[0]["cause"]
            .as_str()
            .unwrap()
            .contains("closed (node_removed)"));
    }

    #[tokio::test]
    async fn a_resolved_wait_with_an_unlaunched_successor_remains_retryable() {
        let db = seeded("resolved-wait-cold-refusal").await;
        let held = arm_supply_resume_hold(
            &db,
            refused("r", HoldKind::Resume, None),
            r#"{"durable_wait":"wait"}"#.into(),
        )
        .await
        .unwrap();
        db.execute_script(
            "INSERT INTO turns(id,session_id,job_id,sequence,predecessor_id,state,start_reason,created_at,updated_at)
               VALUES('cold','session','r',2,'head','pending','wait_resolved',2,2);
             UPDATE jobs SET current_turn_id='cold' WHERE id='r';
             INSERT INTO agent_waits(id,job_id,run_id,session_id,predecessor_turn_id,tool_use_id,condition_json,state,created_at,successor_turn_id,result_stored_at)
               VALUES('wait','r','run','session','head','call','{}','resolved',1,'cold',1);",
        ).await.unwrap();
        db.execute("UPDATE provider_limit_holds SET state='resolved',boundary_source='thread_focus',queue_key=17 WHERE id=?1",params![held.id.as_str()]).await.unwrap();
        assert!(restore_provider_hold_after_admission_failure(
            &db,
            "cold",
            NOW,
            "session memory temporarily unavailable"
        )
        .await
        .unwrap());
        let restored = active_supply_hold(&db, "r").await.unwrap().unwrap();
        assert_eq!(restored.turn_id.as_deref(), Some("cold"));
        assert_eq!(restored.resume_gate, None);
        assert_eq!(restored.retry_not_before, Some(NOW + 300));
        assert_eq!(
            db.query_opt_text("SELECT state FROM agent_waits WHERE id='wait'", ())
                .await
                .unwrap()
                .as_deref(),
            Some("resolving")
        );
        assert_eq!(
            db.query_opt_i64(
                "SELECT result_stored_at FROM agent_waits WHERE id='wait'",
                ()
            )
            .await
            .unwrap(),
            Some(1)
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
