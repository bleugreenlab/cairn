//! Message delivery routing.
//!
//! Channel messages (project/issue) are stored in DB and pulled by the hook
//! handler using a per-process cursor — no push needed.
//!
//! Direct messages ride the attention push queue as `direct:` pushes
//! (CAIRN-1900): [`queue_system_direct`]/[`enqueue_direct_push`] persist the
//! message and `nudge_job_for_urgency` rouses an idle recipient. A busy
//! (mid-turn) recipient drains the push at its next event boundary via the
//! queued-direct injection paths (Claude `additionalContext` hook +
//! tool-result augmentation) — see CAIRN-1196.

pub(crate) use cairn_attention::direct::{
    direct_wake_for_urgency, insert_system_direct_push_conn,
    insert_system_direct_push_with_key_conn,
};

use crate::execution::routing::{owning_db_for_job, owning_db_for_run};
use crate::messages::queued::DeliveryUrgency;
use crate::orchestrator::Orchestrator;
use crate::storage::{run_db_blocking, DbError, LocalDb, RowExt};
use cairn_db::turso::params;

/// Append a message to a first-class thread and route the same user wake used by
/// issue-channel delivery to its live session. Threads with no session yet
/// acquire one before the message is made visible.
///
/// This is the one funnel every prompt to a thread arrives through — the
/// resource append, desktop and operator delivery, and follow-channel routing —
/// so it is where a closed thread refuses. The refusal is taken before anything
/// is written: a prompt to a closed thread leaves no message row, no session, no
/// wake, and no push, rather than a half-delivered message nobody will read.
pub async fn append_thread_message(
    orch: &Orchestrator,
    db: &LocalDb,
    thread_id: &str,
    sender_run_id: Option<&str>,
    sender_name: &str,
    content: &str,
) -> Result<String, String> {
    if content.is_empty() {
        return Err("Message content cannot be empty".into());
    }
    if !crate::threads::thread_is_active(db, thread_id).await? {
        return Err(format!(
            "Thread is closed and cannot receive messages: {thread_id}. Reopen it to resume the conversation."
        ));
    }
    let job_id = crate::threads::ensure_thread_session(db, thread_id).await?;
    let message = crate::messages::db::insert_message(
        db,
        &crate::models::ChannelType::Thread,
        Some(thread_id),
        sender_run_id,
        sender_name,
        None,
        content,
    )?;
    use crate::orchestrator::wakes::{WakeDelivery, WakeEvent, WakeSource};
    let (source, detail_uri) = match sender_run_id {
        Some(sender_run_id) => (
            WakeSource::Peer {
                reference: Some(sender_run_id.to_string()),
            },
            None,
        ),
        // A user wake marks its turn as the operator's own, which a message
        // nobody verified must not do.
        None if sender_name == crate::messages::render::UNVERIFIED_LOCAL_SENDER => {
            (WakeSource::Peer { reference: None }, None)
        }
        None => (WakeSource::User, Some(message.id.clone())),
    };
    crate::orchestrator::wakes::route_wake(
        orch,
        WakeEvent {
            source,
            fact_kind: "message".to_string(),
            detail_uri,
            delivery: WakeDelivery::Targeted {
                subscriber_job_id: job_id,
                sender_name: Some(sender_name.to_string()),
                message: content.to_string(),
            },
            urgency: DeliveryUrgency::Steer,
            muted: crate::orchestrator::wakes::MutedDelivery::Drop,
        },
    )
    .await?;
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table":"messages", "action":"insert", "threadId":thread_id}),
    );
    let thread_change = crate::notify::thread_db_change_for_id(db, thread_id, "update").await;
    let _ = orch.services.emitter.emit("db-change", thread_change);
    Ok(message.id)
}

/// Look up the most recent run id for a job. Used by callers that have a job
/// id (PR conflict resolution, wake delivery, etc.) and need to address the
/// recipient by run id for direct-message delivery.
pub fn latest_run_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { latest_run_for_job_async(db, &job_id).await })
        .ok()
        .flatten()
}

async fn latest_run_for_job_async(db: &LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id FROM runs WHERE job_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            crate::storage::next_text(&mut rows, 0).await
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Does this job currently have a pending or running turn? Shared between
/// the message handler's active-turn check and the queue-on-active fallback
/// path in conflict resolution (CAIRN-1196).
pub(crate) async fn head_turn_active(db: &LocalDb, job_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(*) FROM turns
                     WHERE job_id = ?1 AND state IN ('pending', 'running')",
                    params![job_id.as_str()],
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::Row("missing turn count".to_string()))?;
            Ok(row.i64(0)? > 0)
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Whether a job's current session refuses every further turn (closed by a
/// resolution or a node removal, or failed). Nothing queued for such a job can
/// be delivered by a resume, so the delivery paths below do not try: the push
/// stays undelivered, and is still there if the issue reopens.
fn session_refuses_turns_sync(db: &LocalDb, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        Ok::<_, String>(crate::execution::jobs::current_session_refuses_turns(db, &job_id).await)
    })
    .unwrap_or(false)
}

/// Why nothing queued for a job can reach it right now.
///
/// The one answer to "can this job take a turn": the launch funnel asks it
/// before writing anything, the idle flush consults it before deciding to
/// resume, and the pending list consults it before showing anything as
/// pending, so the composer can never promise a delivery the flush would
/// decline and the funnel would refuse. Every refusal is reversible (reopening
/// the issue or the thread, or lifting a cancel), which is why callers filter
/// on it rather than retiring the rows it covers.
///
/// The variants are checked in declaration order and the first that holds is
/// the answer. That order is what lets the funnel name a specific next step: a
/// cancelled job whose session a resolution closed reports
/// [`Self::SessionRefusesTurns`], whose session carries the reason, rather
/// than the bare [`Self::Cancelled`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryRefusal {
    /// The job is a closed thread's session.
    DormantThread,
    /// The job's execution was superseded by another attempt on its issue.
    Superseded,
    /// The job's current session was closed by a resolution or a node removal,
    /// or failed. The session's `terminal_reason` names why.
    SessionRefusesTurns,
    /// The job is cancelled while its current session still reads open.
    /// Cancelled to Running is not a transition; a lifted cancel passes
    /// through Pending first, so a job still reading Cancelled cannot take a
    /// turn now.
    Cancelled,
}

impl DeliveryRefusal {
    pub fn describe(self) -> &'static str {
        match self {
            Self::DormantThread => "its thread is closed",
            Self::Superseded => "its execution was superseded",
            Self::Cancelled => "it is cancelled",
            Self::SessionRefusesTurns => "its session is closed",
        }
    }
}

/// Whether anything queued for `job_id` can be delivered, or why not.
///
/// A failed or complete job is deliberately NOT refused here: both resume on a
/// reply or a retry with the same job id, so what they hold is still pending.
/// An unreadable lineage is an `Err`, left to the caller to fail closed on.
pub(crate) async fn delivery_refusal(
    db: &LocalDb,
    job_id: &str,
) -> Result<Option<DeliveryRefusal>, String> {
    if crate::threads::is_dormant_thread_session(db, job_id).await {
        return Ok(Some(DeliveryRefusal::DormantThread));
    }
    let fence = crate::execution::canonical::fence_for_job(db, job_id)
        .await
        .map_err(|error| format!("failed to read canonical lineage for job {job_id}: {error}"))?;
    if fence.is_superseded() {
        return Ok(Some(DeliveryRefusal::Superseded));
    }
    if crate::execution::jobs::current_session_refuses_turns(db, job_id).await {
        return Ok(Some(DeliveryRefusal::SessionRefusesTurns));
    }
    let status = db
        .query_opt_text(
            "SELECT status FROM jobs WHERE id = ?1",
            (job_id.to_string(),),
        )
        .await
        .map_err(|error| format!("failed to read status of job {job_id}: {error}"))?;
    if status.as_deref() == Some("cancelled") {
        return Ok(Some(DeliveryRefusal::Cancelled));
    }
    Ok(None)
}

/// Sync wrapper for [`head_turn_active`] for use from non-async callers
/// (e.g. PR conflict resolution).
pub(crate) fn head_turn_active_sync(db: &LocalDb, job_id: &str) -> bool {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { head_turn_active(db, &job_id).await }).unwrap_or(false)
}

/// These yielded turns have an owner for their resumption: the agent's own
/// pending work, or a provider-capacity hold whose reset time owns it. An
/// external push does not resolve that wait, with one exception: an escalated
/// direct message ends a pure `waitFor` park through the park's own resolution
/// path (see [`nudge_for_direct_message`]). Explicit operator continuation
/// remains authoritative.
///
/// A sub-agent task/batch wait or a dependency wait yields `dependency_wait`,
/// the agent's own open question/prompt yields `user_input`, its own pending
/// permission yields `permission`, and a durable `waitFor` or `run` park yields
/// `wait`. Verify against the writers (`mcp/handlers/planning.rs`,
/// `mcp/handlers/permission.rs`, `mcp/handlers/durable_suspend.rs`,
/// `execution/delegation/runtime/resume.rs`) before extending.
/// `safety_pause` is deliberately absent: it has no live writer, and a
/// historical turn carrying it must not hold messages forever.
fn is_self_suspend_reason(reason: &str) -> bool {
    matches!(
        reason,
        "user_input" | "permission" | "dependency_wait" | "wait" | "provider_limit"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectQueueDisposition {
    QueuedOrPresent,
    Undeliverable,
}

pub(crate) fn queue_system_direct_once_confirmed(
    orch: &Orchestrator,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    idempotency_key: &str,
) -> Result<DirectQueueDisposition, String> {
    queue_system_direct_impl(
        orch,
        recipient_run_id,
        content,
        urgency,
        Some(idempotency_key),
    )
}

/// What a job's head turn says its agent is doing right now.
///
/// Head turn = `jobs.current_turn_id` when present, mirroring the canonical
/// head-turn query in `jobs::queries::node_status_indicators`. The fallback is
/// only for legacy rows without the pointer; turn sequence is session-local and
/// restarts after cold-resume reseed rotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeadTurn {
    /// No turn yet, or the head turn finished (`complete`, `failed`,
    /// `interrupted`, `cancelled`). The agent is at rest and takes another turn
    /// only when something hands it one.
    Idle,
    /// The head turn is still live: `running`, `pending`, or `yielded` for a
    /// reason that is not the agent's own pending work (the host holds the turn,
    /// so the agent is still not idle).
    Live,
    /// The head turn `yielded` waiting on the agent's OWN pending work — a
    /// sub-agent task/batch or dependency wait, its own open question or prompt,
    /// its own pending permission, or a durable `waitFor` / `run` suspension.
    ///
    /// Self-suspended is not idle: the agent resumes only when its own pending
    /// work resolves (the task returns, the question is answered, the permission
    /// is decided, the wait fires), draining anything queued for it then. Two
    /// consequences follow. It must NOT be woken by an external attention push,
    /// even a `wake`/`interrupt` one (`docs/attention-redesign.md` — "Wakeable vs
    /// self-suspended"; CAIRN-1876). The one exception is an escalated direct
    /// message to a pure `waitFor` park, which ends the wait through its owner's
    /// resolution path rather than resuming around it
    /// ([`crate::mcp::handlers::durable_suspend::supersede_timed_waits`]). And
    /// background work must NOT be scheduled
    /// against it as though its turn had ended — notably the turn-end project
    /// check cadence, whose heavy lanes would otherwise compete with the very
    /// tests the agent is suspended waiting on (CAIRN-3154).
    SelfSuspended,
}

impl HeadTurn {
    /// Why this job is mid-work rather than idle, phrased for a log line, or
    /// `None` when it is genuinely idle. One accessor over the whole
    /// classification, so a caller that only asks "is a turn in progress" cannot
    /// drift from one that asks which kind.
    pub(crate) fn mid_work_reason(self) -> Option<&'static str> {
        match self {
            HeadTurn::Idle => None,
            HeadTurn::Live => Some("its head turn is still live (the agent already resumed)"),
            HeadTurn::SelfSuspended => Some("it is self-suspended on its own pending work"),
        }
    }

    /// Is the agent suspended on its own pending work, specifically?
    pub(crate) fn is_self_suspended(self) -> bool {
        matches!(self, HeadTurn::SelfSuspended)
    }
}

/// Classify a job's head turn. See [`HeadTurn`] for what each answer means and
/// which callers depend on the distinction.
///
/// A yielded head turn reliably means self-suspended: a normally-idle agent's
/// head turn is `complete` (the warm transition completes the turn — see
/// `orchestrator::lifecycle::transition_to_warm_state`), and a stopped agent's is
/// `interrupted`/`cancelled`; only the durable-wait yields land a `yielded` head
/// turn. The reason check guards against any future non-self-suspend yield.
async fn head_turn(db: &LocalDb, job_id: &str) -> Result<HeadTurn, String> {
    Ok(head_turn_row(db, job_id)
        .await?
        .map_or(HeadTurn::Idle, |row| row.classify()))
}

/// The job's head turn as stored: the one row every head-turn question reads.
struct HeadTurnRow {
    id: String,
    state: String,
    yield_reason: Option<String>,
}

impl HeadTurnRow {
    fn classify(&self) -> HeadTurn {
        match self.state.as_str() {
            "yielded" if self.self_suspend_reason().is_some() => HeadTurn::SelfSuspended,
            "yielded" | "running" | "pending" => HeadTurn::Live,
            _ => HeadTurn::Idle,
        }
    }

    /// Why the head turn is self-suspended, or `None` when it is not.
    fn self_suspend_reason(&self) -> Option<crate::models::TurnYieldReason> {
        if self.state != "yielded" {
            return None;
        }
        self.yield_reason
            .as_deref()
            .filter(|reason| is_self_suspend_reason(reason))?
            .parse()
            .ok()
    }
}

async fn head_turn_row(db: &LocalDb, job_id: &str) -> Result<Option<HeadTurnRow>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT t.id, t.state, t.yield_reason
                     FROM jobs j
                     LEFT JOIN turns current ON current.id = j.current_turn_id
                     JOIN turns t ON t.id = COALESCE(
                         current.id,
                         (SELECT fallback.id
                            FROM turns fallback
                           WHERE fallback.job_id = j.id
                           ORDER BY fallback.created_at DESC, fallback.sequence DESC
                           LIMIT 1)
                     )
                     WHERE j.id = ?1
                     LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            Ok(Some(HeadTurnRow {
                id: row.text(0)?,
                state: row.text(1)?,
                yield_reason: row.opt_text(2)?,
            }))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// What holds a self-suspended job, and what releases it: the one description
/// both a sender's reply and the node summary give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hold {
    /// Parked on its own durable calls (`waitFor`, `run`, MCP).
    Wait {
        /// Each parked call's label, such as `waitFor: duration` or `run: cargo test`.
        labels: Vec<String>,
        /// Whether an escalated message would end these waits now.
        supersedable: bool,
    },
    /// Waiting on its own sub-agent tasks, or on a dependency when none is open.
    Tasks { pending: i64 },
    /// Waiting for the operator to answer its question.
    Question,
    /// Waiting for the operator to decide its permission request.
    Permission,
    /// Held for a provider usage limit until `retry_not_before` (unix seconds).
    ProviderLimit { retry_not_before: Option<i64> },
}

impl Hold {
    /// One clause naming what holds the job and what releases it, written to
    /// follow "It has not started a turn: ".
    pub fn render(&self) -> String {
        match self {
            Hold::Wait {
                labels,
                supersedable: true,
            } => format!(
                "it is parked on its own {}; it reads this when that wait resolves or the operator resumes it, and a message sent with escalate:true ends the wait and wakes it now",
                Self::calls(labels)
            ),
            Hold::Wait { labels, .. } => format!(
                "it is parked on its own {}, which is work in flight; it reads this when that call resolves or the operator resumes it",
                Self::calls(labels)
            ),
            Hold::Tasks { pending } if *pending > 0 => format!(
                "it is waiting on {pending} sub-agent task{} of its own; it reads this when they return or the operator resumes it",
                if *pending == 1 { "" } else { "s" }
            ),
            Hold::Tasks { .. } => {
                "it is waiting on its own dependencies; it reads this when they resolve or the operator resumes it".into()
            }
            Hold::Question => {
                "it is waiting for the operator to answer its question; it reads this once the question is answered".into()
            }
            Hold::Permission => {
                "it is waiting for the operator to decide its permission request; it reads this once the request is decided".into()
            }
            Hold::ProviderLimit {
                retry_not_before: Some(at),
            } => format!(
                "it is held for a provider usage limit until {}; it reads this when the limit resets or the operator resumes it sooner",
                Self::time(*at)
            ),
            Hold::ProviderLimit {
                retry_not_before: None,
            } => {
                "it is held for a provider usage limit with no known reset time; it reads this when the limit clears or the operator resumes it".into()
            }
        }
    }

    /// The short form a node summary's title carries beside its status icon.
    pub fn qualifier(&self) -> String {
        match self {
            Hold::Wait { labels, .. } => match labels.as_slice() {
                [one] => format!("waiting: {one}"),
                [] => "waiting: its own call".into(),
                many => format!("waiting: {} calls", many.len()),
            },
            Hold::Tasks { pending } if *pending > 0 => format!(
                "waiting: {pending} task{}",
                if *pending == 1 { "" } else { "s" }
            ),
            Hold::Tasks { .. } => "waiting: dependencies".into(),
            Hold::Question => "waiting: operator answer".into(),
            Hold::Permission => "waiting: permission decision".into(),
            Hold::ProviderLimit {
                retry_not_before: Some(at),
            } => format!("held: provider limit until {}", Self::time(*at)),
            Hold::ProviderLimit { .. } => "held: provider limit".into(),
        }
    }

    fn calls(labels: &[String]) -> String {
        if labels.is_empty() {
            return "durable call".into();
        }
        labels
            .iter()
            .map(|label| format!("`{label}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn time(unix_seconds: i64) -> String {
        crate::clock::stamp(unix_seconds).unwrap_or_else(|| unix_seconds.to_string())
    }
}

/// What holds this job from taking a turn, or `None` when nothing of its own
/// does (it is idle, mid-turn, or its read failed).
pub(crate) async fn describe_hold(db: &LocalDb, job_id: &str) -> Option<Hold> {
    use crate::models::TurnYieldReason;
    let row = head_turn_row(db, job_id).await.ok()??;
    Some(match row.self_suspend_reason()? {
        TurnYieldReason::Wait => {
            let waits =
                crate::mcp::handlers::durable_suspend::parked_waits_of_turn(db, job_id, &row.id)
                    .await
                    .unwrap_or_default();
            Hold::Wait {
                labels: waits.labels,
                supersedable: waits.supersedable,
            }
        }
        TurnYieldReason::DependencyWait => Hold::Tasks {
            pending: db
                .query_one(
                    "SELECT COUNT(*) FROM delegated_packets
                     WHERE parent_job_id = ?1
                       AND status NOT IN ('completed', 'failed', 'cancelled')",
                    params![job_id],
                    |row| row.i64(0),
                )
                .await
                .unwrap_or(0),
        },
        TurnYieldReason::UserInput => Hold::Question,
        TurnYieldReason::Permission => Hold::Permission,
        TurnYieldReason::ProviderLimit => Hold::ProviderLimit {
            retry_not_before: db
                .query_opt(
                    "SELECT retry_not_before FROM provider_limit_holds
                     WHERE job_id = ?1 AND state = 'active'
                     ORDER BY created_at DESC LIMIT 1",
                    params![job_id],
                    |row| row.opt_i64(0),
                )
                .await
                .ok()
                .flatten()
                .flatten(),
        },
        TurnYieldReason::SafetyPause => return None,
    })
}

/// Why a job is self-suspended and on which turn, for the one caller that acts
/// on the reason rather than only on the fact.
struct SelfSuspension {
    reason: crate::models::TurnYieldReason,
    turn_id: String,
}

fn self_suspension_sync(db: &LocalDb, job_id: &str) -> Option<SelfSuspension> {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { head_turn_row(db, &job_id).await })
        .ok()
        .flatten()
        .and_then(|row| {
            Some(SelfSuspension {
                reason: row.self_suspend_reason()?,
                turn_id: row.id,
            })
        })
}

/// [`head_turn`] for the non-async flush-on-idle resume gate. Fails open to
/// [`HeadTurn::Idle`] so a read error never silently suppresses a caller's
/// normal behaviour.
fn head_turn_sync(db: &LocalDb, job_id: &str) -> HeadTurn {
    let job_id = job_id.to_string();
    run_db_blocking(move || async move { head_turn(db, &job_id).await }).unwrap_or(HeadTurn::Idle)
}

/// [`head_turn`] for a job id, routed to the job's owning database, from an
/// async caller. Fails open to [`HeadTurn::Idle`] for the same reason as
/// [`head_turn_sync`]: a read error must not invent work that is not happening.
///
/// Used by the checks-settlement predicate, which asks whether a node is still
/// working before it will believe that node's lanes have stopped.
pub(crate) async fn head_turn_for_job_async(orch: &Orchestrator, job_id: &str) -> HeadTurn {
    let Ok(db) = crate::execution::routing::owning_db_for_job(&orch.db, job_id).await else {
        return HeadTurn::Idle;
    };
    head_turn(&db, job_id).await.unwrap_or(HeadTurn::Idle)
}

/// [`head_turn`] for a job id, routed to the job's owning database, for the
/// synchronous turn-end hooks in `orchestrator::lifecycle`. Fails open to
/// [`HeadTurn::Idle`] for the same reason as [`head_turn_sync`].
pub(crate) fn head_turn_for_job(orch: &Orchestrator, job_id: &str) -> HeadTurn {
    let dbs = orch.db.clone();
    let job_id = job_id.to_string();
    run_db_blocking(move || async move {
        let db = crate::execution::routing::owning_db_for_job(&dbs, &job_id)
            .await
            .map_err(|e| e.to_string())?;
        head_turn(&db, &job_id).await
    })
    .unwrap_or(HeadTurn::Idle)
}

/// The recipient job's node URI, used as a `direct:` push's wake-card link. The
/// message body resolves from the messages row at drain, so this is only the UI
/// link; `None` when the job can't be resolved (the caller falls back). The
/// query itself lives in [`crate::jobs::queries::home_uri_for_job`].
pub(crate) async fn node_uri_for_job(db: &LocalDb, job_id: &str) -> Option<String> {
    crate::jobs::queries::home_uri_for_job(db, job_id)
        .await
        .ok()
        .flatten()
}

async fn node_uri_for_job_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> Result<Option<String>, DbError> {
    crate::jobs::queries::home_uri_for_job_conn(conn, job_id).await
}

/// The id of the one system direct a given idempotency key may ever insert.
pub(crate) fn system_direct_once_message_id(idempotency_key: &str) -> String {
    format!("reconcile:{idempotency_key}")
}

pub(crate) async fn insert_system_direct_push_once_conn(
    conn: &cairn_db::storage::TrackedConnection,
    recipient_job_id: &str,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    idempotency_key: &str,
) -> Result<(String, crate::orchestrator::attention_push::Wake, bool), DbError> {
    use crate::orchestrator::attention_push::Boundary;
    let now = chrono::Utc::now().timestamp();
    let message_id = system_direct_once_message_id(idempotency_key);
    let inserted = conn
        .execute(
            "INSERT OR IGNORE INTO messages (
            id, channel_type, channel_id, sender_run_id, sender_name,
            recipient_run_id, content, created_at, urgency
         ) VALUES (?1, 'direct', NULL, NULL, 'system', ?2, ?3, ?4, ?5)",
            params![
                message_id.as_str(),
                recipient_run_id,
                content,
                now,
                urgency.as_str()
            ],
        )
        .await?;
    let wake = direct_wake_for_urgency(urgency);
    if inserted == 0 {
        return Ok((message_id, wake, false));
    }
    let content_ref = node_uri_for_job_conn(conn, recipient_job_id)
        .await?
        .unwrap_or_else(|| recipient_job_id.to_string());
    let push_id = uuid::Uuid::new_v4().to_string();
    let key = crate::orchestrator::attention_push::direct_push_key(&message_id, None);
    conn.execute(
        "INSERT INTO attention_pushes
           (id, recipient, content_ref, wake, boundary, key, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            push_id.as_str(),
            recipient_job_id,
            content_ref.as_str(),
            wake.as_str(),
            Boundary::Event.as_str(),
            key.as_str(),
            now
        ],
    )
    .await?;
    Ok((message_id, wake, true))
}

/// Create a `direct:{message_id}` attention push so a system-originated direct
/// message rides the push queue (CAIRN-1900) rather than the retired
/// `delivered_at` pull path — without this the raw `messages` row is never
/// drained. `recipient_job_id` is the push recipient; the wake-card link is the
/// recipient's node URI when resolvable (the body resolves from the messages row
/// at drain). The wake level follows the urgency: `Interrupt` for interrupt,
/// `Passive` for a passive (ride-along) urgency, else `Wake`. Returns the
/// effective wake so the caller decides whether to nudge.
pub(crate) fn enqueue_direct_push(
    orch: &Orchestrator,
    recipient_job_id: &str,
    message_id: &str,
    urgency: DeliveryUrgency,
) -> Result<crate::orchestrator::attention_push::Wake, String> {
    enqueue_direct_push_with_operator_trigger(orch, recipient_job_id, message_id, urgency, None)
}

pub(crate) fn enqueue_direct_push_with_operator_trigger(
    orch: &Orchestrator,
    recipient_job_id: &str,
    message_id: &str,
    urgency: DeliveryUrgency,
    operator_message_id: Option<&str>,
) -> Result<crate::orchestrator::attention_push::Wake, String> {
    use crate::orchestrator::attention_push::Boundary;
    let requested = direct_wake_for_urgency(urgency);
    let owning = run_db_blocking({
        let dbs = orch.db.clone();
        let recipient_job_id = recipient_job_id.to_string();
        move || async move {
            owning_db_for_job(&dbs, &recipient_job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .map_err(|error| {
        log::warn!(
            "enqueue_direct_push: failed to route recipient job {recipient_job_id}: {error}"
        );
        error
    })?;
    let recipient_job_id = recipient_job_id.to_string();
    let message_id = message_id.to_string();
    let operator_message_id = operator_message_id.map(str::to_string);
    run_db_blocking(move || async move {
        let content_ref = node_uri_for_job(&owning, &recipient_job_id)
            .await
            .unwrap_or_else(|| recipient_job_id.clone());
        let (_, effective) = crate::orchestrator::attention_push::push(
            &owning,
            &recipient_job_id,
            &content_ref,
            requested,
            Boundary::Event,
            &crate::orchestrator::attention_push::direct_push_key(
                &message_id,
                operator_message_id.as_deref(),
            ),
        )
        .await
        .map_err(|e| e.to_string())?;
        Ok(effective)
    })
}

/// Persist a system-originated direct message addressed to `recipient_run_id` and
/// queue it on the attention push queue (CAIRN-1900). Used by orchestrator-internal
/// callers (base-branch advance notices, PR conflict resolution, etc.). The
/// `urgency` sets the wake level, which governs only whether an *idle* recipient
/// is roused: a waking urgency (`Steer`/`Queue`/`Interrupt`) nudges an idle
/// recipient to resume, while a `Passive` note never wakes an idle recipient. The
/// wake level does not gate delivery on a *busy* recipient — a recipient already
/// mid-turn drains the push at its next event boundary regardless of wake level,
/// so a passive note rides along inline on the active turn while never having
/// woken an idle agent.
pub(crate) fn queue_system_direct(
    orch: &Orchestrator,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
) -> Result<(), String> {
    queue_system_direct_impl(orch, recipient_run_id, content, urgency, None).map(|_| ())
}

pub(crate) fn queue_system_direct_once(
    orch: &Orchestrator,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    idempotency_key: &str,
) -> Result<(), String> {
    queue_system_direct_impl(
        orch,
        recipient_run_id,
        content,
        urgency,
        Some(idempotency_key),
    )
    .map(|_| ())
}

fn queue_system_direct_impl(
    orch: &Orchestrator,
    recipient_run_id: &str,
    content: &str,
    urgency: DeliveryUrgency,
    idempotency_key: Option<&str>,
) -> Result<DirectQueueDisposition, String> {
    let db = match run_db_blocking({
        let dbs = orch.db.clone();
        let run = recipient_run_id.to_string();
        move || async move {
            owning_db_for_run(&dbs, &run)
                .await
                .map_err(|e| e.to_string())
        }
    }) {
        Ok(db) => db,
        Err(error) => {
            log::warn!(
                "queue_system_direct: failed to route recipient run {recipient_run_id}: {error}; direct message undeliverable"
            );
            return Ok(DirectQueueDisposition::Undeliverable);
        }
    };
    let run = recipient_run_id.to_string();
    let job_id = run_db_blocking({
        let db = db.clone();
        move || async move {
            Ok::<_, String>(crate::messages::side_channel::job_id_for_run(&db, &run).await)
        }
    })
    .ok()
    .flatten();
    let Some(job_id) = job_id else {
        log::warn!(
            "queue_system_direct: no job for run {recipient_run_id}; direct message undeliverable"
        );
        return Ok(DirectQueueDisposition::Undeliverable);
    };

    let (message_id, effective, inserted) = run_db_blocking({
        let db = db.clone();
        let job_id = job_id.clone();
        let run_id = recipient_run_id.to_string();
        let content = content.to_string();
        let idempotency_key = idempotency_key.map(str::to_string);
        move || async move {
            db.write(|conn| {
                let job_id = job_id.clone();
                let run_id = run_id.clone();
                let content = content.clone();
                let idempotency_key = idempotency_key.clone();
                Box::pin(async move {
                    if let Some(key) = idempotency_key.as_deref() {
                        insert_system_direct_push_once_conn(
                            conn, &job_id, &run_id, &content, urgency, key,
                        )
                        .await
                    } else {
                        insert_system_direct_push_conn(conn, &job_id, &run_id, &content, urgency)
                            .await
                            .map(|(id, wake)| (id, wake, true))
                    }
                })
            })
            .await
            .map_err(|e| e.to_string())
        }
    })?;

    if !inserted {
        log::debug!("System direct {message_id} was already queued; skipping duplicate delivery");
        return Ok(DirectQueueDisposition::QueuedOrPresent);
    }
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": "messages", "action": "insert"}),
    );

    if effective.wakes_idle() {
        if let Err(e) = nudge_job_for_urgency(orch, &job_id, urgency) {
            log::warn!("queue_system_direct: nudge failed for job {job_id}: {e}");
        }
    }

    log::debug!("Queued system direct message {} for delivery", message_id);
    Ok(DirectQueueDisposition::QueuedOrPresent)
}

/// Everything still queued for an idle run that justifies a single resume.
///
/// Directs no longer appear here — they ride the attention push queue as
/// `direct:` pushes (CAIRN-1900), drained and stamped by `continue_job_impl` and
/// surfaced to this gate through the `waking_push` check. The flush carries no
/// prompt: `continue_job_impl` claims/injects notices, queued messages, and
/// pushes itself and supplies its own default lead-in.
struct PendingFlush {
    job_id: String,
    /// Count of detached run-batch results pending for the job, claimed and
    /// injected by `continue_job_impl` with every other side-channel notice;
    /// counted only to decide that they justify a resume.
    notice_count: usize,
    /// Count of queued user messages (CAIRN-1309) pending for the job, claimed
    /// and injected by `continue_job_impl`; kept only so a stranded queued
    /// message still triggers a resume.
    queued_count: usize,
}

/// Peek — without stamping — the directs addressed to `run_id` and the count of
/// child side-channel notices queued for its job, and build the directs-only
/// resume prompt.
///
/// Returns `None` when nothing is pending. That `None` is the guard that keeps
/// flush-on-idle from spuriously resuming a healthy idle run: a resume only
/// happens when there is genuinely stuck content. Peeking (rather than the
/// stamping `claim_*` reads) lets the caller stamp the directs only after the
/// resume succeeds, so a failed resume leaves them pending for the next prompt
/// boundary.
///
/// Side-channel notices intentionally do **not** go into the prompt: every
/// `continue_job_impl` resume claims and injects pending notices for the job,
/// so the flush only needs their presence to decide whether to resume. Building
/// them in here as well would deliver each notice twice.
fn collect_pending_for_flush(db: &LocalDb, run_id: &str) -> Result<PendingFlush, DeclinedFlush> {
    let run_id_owned = run_id.to_string();
    let job_id = run_db_blocking(move || async move {
        Ok(crate::messages::side_channel::job_id_for_run(db, &run_id_owned).await)
    })
    .ok()
    .flatten()
    .ok_or(DeclinedFlush {
        job_id: None,
        decline: FlushDecline::NoJob,
    })?;
    let declined = |decline| DeclinedFlush {
        job_id: Some(job_id.clone()),
        decline,
    };

    // A recipient no turn can reach is not resumed by anything queued for it —
    // pushes, queued user messages, or notices. Taken here, at the one gate the
    // idle flush consults, so every queued reason to resume is covered by one
    // check rather than each mechanism carrying its own, and rather than relying
    // on the launch funnel to refuse after the decision was made. An unreadable
    // lineage declines too: a missed refusal could put two agents on one issue.
    let refusal = {
        let job_id = job_id.clone();
        run_db_blocking(move || async move { delivery_refusal(db, &job_id).await })
    };
    match refusal {
        Ok(None) => {}
        Ok(Some(refusal)) => {
            log::debug!(
                "flush-on-idle: job {} declined resume ({})",
                &job_id[..job_id.len().min(8)],
                refusal.describe()
            );
            return Err(declined(FlushDecline::Refused(refusal)));
        }
        Err(error) => {
            log::debug!(
                "flush-on-idle: job {} declined resume (deliverability unknown): {error}",
                &job_id[..job_id.len().min(8)]
            );
            return Err(declined(FlushDecline::Unknown(error)));
        }
    }

    let queued_count =
        crate::messages::queued::peek_waking_pending_count_for_job(db, &job_id).unwrap_or(0);
    // CAIRN-1876: an agent self-suspended on its OWN pending work (a sub-agent
    // task/batch, its own open question/prompt, its own pending permission, a
    // dependency wait, or a durable `waitFor`/`run` park) is NOT idle and must NOT
    // be resumed here by an external attention push — even a `wake`/`interrupt`
    // one. The push stays queued (undelivered) and rides along on the agent's
    // own-work-resolution resume, which drains the queue then. Direct messages
    // are `direct:` pushes and are held the same way. The one exception is taken
    // before this gate, in `nudge_for_direct_message`: an escalated direct to a
    // pure `waitFor` park ends the wait through the park's own resolution path,
    // never by resuming around it. A queued operator message is counted above
    // without this check, because the operator stays authoritative.
    // (docs/attention-redesign.md — "Wakeable vs self-suspended".)
    let self_suspended = head_turn_sync(db, &job_id).is_self_suspended();

    // Passive side-channel notices do not wake an idle job; they ride along
    // with the next agent-bound payload. The one exception is a detached run
    // batch's result: the agent is waiting for it, and nothing else will
    // carry it. A self-suspended job still gets it on its own resume.
    let notice_count = if self_suspended {
        0
    } else {
        let job_id = job_id.clone();
        run_db_blocking(move || async move {
            Ok(crate::messages::side_channel::pending_run_batch_notice_count(db, &job_id).await)
        })
        .unwrap_or(0)
    };

    // CAIRN-1881/1900: a pending rousing (`wake`/`interrupt`) push for this job is
    // a reason to resume an *idle* agent — but never a self-suspended one.
    // `passive` pushes ride along on a resume that happens for another reason but
    // never wake an idle agent. Direct messages are now `direct:` pushes, so they
    // are covered here too. This site only decides whether to resume; the render +
    // atomic stamp happen in `continue_job_impl`.
    let waking_push = !self_suspended && {
        let job_id = job_id.clone();
        run_db_blocking(move || async move {
            Ok(
                crate::orchestrator::attention_push::has_pending_waking_live(db, &job_id)
                    .await
                    .unwrap_or(false),
            )
        })
        .unwrap_or(false)
    };

    if notice_count == 0 && queued_count == 0 && !waking_push {
        // CAIRN-2410: a decline is silent by default, which is exactly what turned
        // a lost review wake into a multi-hour mystery. If the recipient DID carry
        // undelivered rousing pushes but they all lazy-resolved dead (or it is
        // self-suspended on its own work), name the job, the reason, and the push
        // keys so the next incident of this class is a one-grep diagnosis.
        let job_for_log = job_id.clone();
        let keys = run_db_blocking(move || async move {
            Ok(
                crate::orchestrator::attention_push::pending_waking_keys(db, &job_for_log)
                    .await
                    .unwrap_or_default(),
            )
        })
        .unwrap_or_default();
        if !keys.is_empty() {
            log::debug!(
                "flush-on-idle: job {} declined resume ({}): {} pending rousing push(es) not delivered: {}",
                &job_id[..job_id.len().min(8)],
                if self_suspended {
                    "self-suspended on own work"
                } else {
                    "all pushes lazy-resolve dead"
                },
                keys.len(),
                keys.join(", "),
            );
        }
        return Err(declined(if self_suspended {
            FlushDecline::SelfSuspended
        } else {
            FlushDecline::NothingPending
        }));
    }

    Ok(PendingFlush {
        job_id,
        notice_count,
        queued_count,
    })
}

/// Why the idle flush did not resume a run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FlushDecline {
    /// The run resolves to no job.
    NoJob,
    /// No turn can reach the job (see [`DeliveryRefusal`]).
    Refused(DeliveryRefusal),
    /// Whether the job can take a turn could not be read, so the flush failed closed.
    Unknown(String),
    /// The job is suspended on its own pending work, which holds what is queued.
    SelfSuspended,
    /// Nothing queued for the job is a reason to resume it.
    NothingPending,
}

impl FlushDecline {
    /// The decline as the idle-flush timing event records it.
    fn reason(&self) -> &'static str {
        match self {
            Self::NoJob => "no_job",
            Self::Refused(DeliveryRefusal::DormantThread) => "refused_dormant_thread",
            Self::Refused(DeliveryRefusal::Superseded) => "refused_superseded",
            Self::Refused(DeliveryRefusal::Cancelled) => "refused_cancelled",
            Self::Refused(DeliveryRefusal::SessionRefusesTurns) => "refused_session_closed",
            Self::Unknown(_) => "deliverability_unknown",
            Self::SelfSuspended => "self_suspended",
            Self::NothingPending => "nothing_pending",
        }
    }
}

/// A decline, with the job it was about when the run resolved to one.
#[derive(Debug)]
struct DeclinedFlush {
    job_id: Option<String>,
    decline: FlushDecline,
}

/// What the idle flush did.
enum FlushOutcome {
    Resumed,
    Declined(FlushDecline),
}

/// Resume a run that has just gone idle to deliver anything still queued for it:
/// child side-channel notices, queued user follow-ups, a deliverable attention
/// briefing, or a rousing attention push — direct messages are now `direct:`
/// pushes (CAIRN-1900). Called from the turn-end sites (warm transition and run
/// finalize), it closes the gap where a recipient's last turn goes idle with no
/// further tool boundary to drain queued content.
///
/// `collect_pending_for_flush` is the guard: it returns `None` for a healthy idle
/// run with nothing stuck and skips a self-suspended agent, so an external push
/// never resumes it. `continue_job_impl` claims, injects, renders, and atomically
/// stamps every pending item (notices, queued messages, pushes); this function
/// only decides whether to resume and carries no prompt of its own.
pub fn flush_pending_directs_on_idle(orch: &Orchestrator, run_id: &str) -> Result<(), String> {
    flush_on_idle(orch, run_id).map(|_| ())
}

/// [`flush_pending_directs_on_idle`], reporting whether it resumed the run and,
/// when it did not, why.
fn flush_on_idle(orch: &Orchestrator, run_id: &str) -> Result<FlushOutcome, String> {
    let timing_started = std::time::Instant::now();
    let db = match run_db_blocking({
        let dbs = orch.db.clone();
        let run_id = run_id.to_string();
        move || async move {
            owning_db_for_run(&dbs, &run_id)
                .await
                .map_err(|e| e.to_string())
        }
    }) {
        Ok(db) => db,
        Err(error) => {
            log::warn!("flush-on-idle: failed to route run {run_id}: {error}");
            return Err(format!(
                "flush-on-idle could not route run {run_id}: {error}"
            ));
        }
    };
    let pending = match collect_pending_for_flush(&db, run_id) {
        Ok(pending) => pending,
        Err(DeclinedFlush { job_id, decline }) => {
            let mut event = crate::resume_timing::ResumeTimingEvent::new("idle_flush_decision")
                .elapsed(timing_started);
            event.run_id = Some(run_id);
            event.job_id = job_id.as_deref();
            event.mode = Some("skip");
            event.reason = Some(decline.reason());
            event.emit();
            return Ok(FlushOutcome::Declined(decline));
        }
    };

    let mut event =
        crate::resume_timing::ResumeTimingEvent::new("idle_flush_decision").elapsed(timing_started);
    event.run_id = Some(run_id);
    event.job_id = Some(&pending.job_id);
    event.mode = Some("resume");
    event.count = Some(pending.queued_count);
    event.emit();

    let short = &pending.job_id[..pending.job_id.len().min(8)];
    let handoff_started = std::time::Instant::now();
    match crate::execution::jobs::continue_job_impl(orch, &pending.job_id, None, None, None) {
        // `continue_job_impl` returns once the process is spawned, before the
        // provider's first handshake, so `Ok` is a dispatch, not a confirmed
        // launch. A start that fails afterwards settles on the run itself.
        Ok(run) => log::info!(
            "flush-on-idle: dispatched continuation for job {} (run {}; {} side-channel notice(s), {} queued user message(s)); launch outcome settles on the run",
            short,
            &run.id[..run.id.len().min(8)],
            pending.notice_count,
            pending.queued_count
        ),
        Err(e) => return Err(format!("flush-on-idle failed to resume job {short}: {e}")),
    }
    let mut event = crate::resume_timing::ResumeTimingEvent::new("continue_job_handoff")
        .elapsed(handoff_started);
    event.run_id = Some(run_id);
    event.job_id = Some(&pending.job_id);
    event.emit();
    Ok(FlushOutcome::Resumed)
}

/// Which direct message is waking a job, so a wait it ends can say so.
#[derive(Debug, Clone)]
pub(crate) struct DirectMessageRef {
    /// The sender's node URI, or its name when it has none.
    pub(crate) from: String,
    pub(crate) message_id: String,
}

/// What a nudge did with the content already queued for a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NudgeOutcome {
    /// The urgency does not wake anyone (passive); the content rides along.
    NotWaking,
    /// No turn can reach the job.
    Refused(DeliveryRefusal),
    /// The job's head turn was active when the content was queued, so it was
    /// left queued rather than delivered. Either that turn drains it at a later
    /// boundary, or, once the turn has ended, the turn-end flush (or whatever
    /// continues the job) does: the content is persisted before the head is read
    /// and every turn end is made durable before its flush peeks, so one of the
    /// two always sees it. Nothing here knows the turn will reach another
    /// boundary, so this is not a confirmed delivery.
    QueuedForActiveTurn,
    /// The job's active turn was stopped, and it resumes with the content.
    Interrupted,
    /// A continuation was dispatched; it takes the content as it starts.
    Resumed,
    /// An escalated direct message ended the job's `waitFor` park, and it is
    /// resuming through that wait's resolution.
    SupersededWait { labels: Vec<String> },
    /// The job is suspended on its own work, which holds the content.
    Held(Hold),
    /// The job was not resumed for some other reason, named in the sentence.
    Declined(String),
}

/// Nudge a job after a pending row has been durably persisted according to the
/// delivery urgency ladder. Passive never wakes. Queue/steer wake idle jobs.
/// Interrupt on an active recipient uses the same semantics as manually queueing
/// a message and pressing Stop: stop the active turn and let the normal idle
/// flush claim the already-persisted row and resume exactly once. Idle interrupt
/// recipients still resume immediately. A self-suspended recipient is held, and
/// the outcome says what holds it.
pub fn nudge_job_for_urgency(
    orch: &Orchestrator,
    job_id: &str,
    urgency: DeliveryUrgency,
) -> Result<NudgeOutcome, String> {
    nudge(orch, job_id, urgency, None)
}

/// [`nudge_job_for_urgency`] for an agent's direct message, which adds one
/// rung: an escalated (`interrupt`) message to a job parked only on its own
/// `waitFor` calls ends those waits and resumes it with the message. A
/// `waitFor` only observes something, so ending it loses nothing. Every other
/// self-suspension still holds, and operator input never comes through here.
pub(crate) fn nudge_for_direct_message(
    orch: &Orchestrator,
    job_id: &str,
    urgency: DeliveryUrgency,
    message: &DirectMessageRef,
) -> Result<NudgeOutcome, String> {
    nudge(orch, job_id, urgency, Some(message))
}

fn nudge(
    orch: &Orchestrator,
    job_id: &str,
    urgency: DeliveryUrgency,
    message: Option<&DirectMessageRef>,
) -> Result<NudgeOutcome, String> {
    let mut timing_event = crate::resume_timing::ResumeTimingEvent::new("queue_nudge");
    timing_event.job_id = Some(job_id);
    timing_event.mode = Some(urgency.as_str());
    timing_event.emit();
    if !urgency.wakes_idle() {
        return Ok(NudgeOutcome::NotWaking);
    }

    let db = run_db_blocking({
        let dbs = orch.db.clone();
        let job_id = job_id.to_string();
        move || async move {
            owning_db_for_job(&dbs, &job_id)
                .await
                .map_err(|e| e.to_string())
        }
    })
    .map_err(|error| format!("failed to route job {job_id}: {error}"))?;

    // A post-resolution push (a turn-end check verdict, a base-advance failure)
    // arriving for a job whose session the resolution closed is left queued,
    // not delivered: there is no turn to deliver it into, and attempting one
    // is what wrote merged builders back to Running.
    if session_refuses_turns_sync(&db, job_id) {
        log::debug!(
            "queue_nudge: not nudging job {} (its session is closed); the push stays queued",
            &job_id[..job_id.len().min(8)]
        );
        return Ok(NudgeOutcome::Refused(DeliveryRefusal::SessionRefusesTurns));
    }

    let live_run_id = crate::orchestrator::lifecycle::live_run_id_for_job(orch, job_id);
    let head_active = head_turn_active_sync(&db, job_id);
    if head_active && urgency != DeliveryUrgency::Interrupt {
        return Ok(NudgeOutcome::QueuedForActiveTurn);
    }

    if urgency == DeliveryUrgency::Interrupt && head_active {
        let run_id =
            live_run_id.ok_or_else(|| "no live process holds the active turn".to_string())?;
        crate::orchestrator::lifecycle::stop_session(orch, &run_id)?;

        // The backend abort acknowledgement remains the fast path, but it is not
        // the owner: stop_session has already made the turn terminal and the run
        // idle synchronously, so this idempotent flush guarantees the queued row
        // is claimed even when the backend never acknowledges the interrupt.
        // A concurrent acknowledgement-driven flush either wins the claim or
        // observes no pending rows; continue_job_impl also joins an in-flight
        // launch, so neither ordering can create a duplicate successor.
        flush_pending_directs_on_idle(orch, &run_id)?;
        return Ok(NudgeOutcome::Interrupted);
    }

    if let (Some(message), DeliveryUrgency::Interrupt) = (message, urgency) {
        if let Some(SelfSuspension {
            reason: crate::models::TurnYieldReason::Wait,
            turn_id,
        }) = self_suspension_sync(&db, job_id)
        {
            if let Some(labels) = crate::mcp::handlers::durable_suspend::supersede_timed_waits(
                orch, &db, job_id, &turn_id, message,
            )? {
                log::info!(
                    "queue_nudge: escalated direct {} ended job {}'s parked {}",
                    message.message_id,
                    &job_id[..job_id.len().min(8)],
                    labels.join(", ")
                );
                return Ok(NudgeOutcome::SupersededWait { labels });
            }
        }
    }

    let flushed = if let Some(run_id) = live_run_id.or_else(|| latest_run_for_job(&db, job_id)) {
        flush_on_idle(orch, &run_id)?
    } else {
        crate::execution::jobs::continue_job_impl(orch, job_id, None, None, None)
            .map_err(|error| format!("could not resume queued delivery: {error}"))?;
        FlushOutcome::Resumed
    };
    Ok(match flushed {
        FlushOutcome::Resumed => NudgeOutcome::Resumed,
        FlushOutcome::Declined(FlushDecline::Refused(refusal)) => NudgeOutcome::Refused(refusal),
        FlushOutcome::Declined(FlushDecline::SelfSuspended) => {
            let job_id = job_id.to_string();
            match run_db_blocking(move || async move { Ok(describe_hold(&db, &job_id).await) }) {
                Ok(Some(hold)) => NudgeOutcome::Held(hold),
                _ => NudgeOutcome::Declined("it is suspended on its own pending work".into()),
            }
        }
        FlushOutcome::Declined(FlushDecline::Unknown(error)) => NudgeOutcome::Declined(format!(
            "whether it can take a turn could not be read ({error})"
        )),
        FlushOutcome::Declined(FlushDecline::NothingPending) => {
            NudgeOutcome::Declined("nothing queued for it is deliverable now".into())
        }
        FlushOutcome::Declined(FlushDecline::NoJob) => {
            NudgeOutcome::Declined("its run does not resolve to a job".into())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_process::process::RunHandle;
    use crate::db::DbState;
    use crate::services::testing::{RecordingProcessSpawner, TestServicesBuilder};
    use crate::storage::SearchIndex;
    use cairn_db::turso::params;
    use std::sync::Arc;
    use std::sync::Mutex;

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("delivery-flush.db").await
    }

    /// Delivering to a thread rouses its session, so the orchestrator needs a
    /// spawner that accepts the launch; these tests assert what is persisted,
    /// not process mechanics.
    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        let config_dir = tempfile::tempdir().unwrap().keep();
        let db_state = Arc::new(DbState::new(
            Arc::new(db),
            Arc::new(SearchIndex::open_or_create(config_dir.join("search-index.db")).unwrap()),
        ));
        let services = Arc::new(
            TestServicesBuilder::new()
                .with_process(RecordingProcessSpawner::new().clone())
                .build(),
        );
        Orchestrator::builder(db_state, services, config_dir).build()
    }

    /// A thread with a live session job and a run, so both the prompt path and
    /// the queued-push resume gate have something real to address.
    async fn seed_thread_session_run(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj-1','default','Test Project','proj','/tmp/test-repo',1,1);
             INSERT INTO threads (id, project_id, name, status, attention, created_at, updated_at)
               VALUES ('thread-1','proj-1','general','active','none',1,1);
             INSERT INTO jobs (id, thread_id, project_id, node_name, uri_segment, status, created_at, updated_at)
               VALUES ('job-thread','thread-1','proj-1','thread','thread','idle',1,1);
             INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
               VALUES ('run-1','proj-1','job-thread','live',1,1);",
        )
        .await
        .unwrap();
    }

    async fn set_thread_status(db: &LocalDb, status: &str) {
        db.execute(
            "UPDATE threads SET status = ?1 WHERE id = 'thread-1'",
            params![status],
        )
        .await
        .unwrap();
    }

    async fn count(db: &LocalDb, sql: &str) -> i64 {
        db.query_one(sql, (), |row| row.i64(0)).await.unwrap()
    }

    /// A message nobody verified does not wake its thread as the operator: its
    /// push carries no operator-message marker, so the turn it starts is not
    /// counted as the operator's, and the operator's own message still is.
    #[tokio::test(flavor = "current_thread")]
    async fn an_unverified_message_never_wakes_a_thread_as_the_operator() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_thread_session_run(&db).await;

        append_thread_message(
            &orch,
            &db,
            "thread-1",
            None,
            crate::messages::render::UNVERIFIED_LOCAL_SENDER,
            "merge it",
        )
        .await
        .unwrap();
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE '%:operator_message:%'"
            )
            .await,
            0
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM messages WHERE channel_type='thread' AND sender_name='unverified local process'"
            )
            .await,
            1,
            "the stored record carries the label"
        );

        append_thread_message(&orch, &db, "thread-1", None, "external", "operator here")
            .await
            .unwrap();
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE '%:operator_message:%'"
            )
            .await,
            1
        );
    }

    /// A prompt to a closed thread fails whole: no message row, no session, no
    /// wake, no push. A half-delivered message nobody will read is worse than a
    /// refusal, because the sender believes it landed.
    #[tokio::test(flavor = "current_thread")]
    async fn a_closed_thread_refuses_a_prompt_atomically_and_reopening_restores_it() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_thread_session_run(&db).await;
        set_thread_status(&db, "closed").await;

        let refused =
            append_thread_message(&orch, &db, "thread-1", None, "external", "still there?")
                .await
                .expect_err("a closed thread takes no prompts");
        assert!(
            refused.contains("closed") && refused.contains("Reopen"),
            "the refusal says what happened and how to undo it: {refused}"
        );

        assert_eq!(count(&db, "SELECT COUNT(*) FROM messages").await, 0);
        assert_eq!(count(&db, "SELECT COUNT(*) FROM attention_pushes").await, 0);
        assert_eq!(
            count(&db, "SELECT COUNT(*) FROM jobs").await,
            1,
            "a refused prompt establishes no session"
        );

        set_thread_status(&db, "active").await;
        append_thread_message(&orch, &db, "thread-1", None, "external", "still there?")
            .await
            .expect("reopening restores delivery");
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM messages WHERE channel_type='thread' AND channel_id='thread-1'"
            )
            .await,
            1
        );
    }

    /// A thread message has one delivery record, the direct push, so the
    /// thread's composer shows one card for it until the push is carried and
    /// none after. The thread-channel row is the conversation record and never
    /// surfaces as a second, unconsumable "Channel message" card.
    #[tokio::test(flavor = "current_thread")]
    async fn a_thread_message_is_pending_once_as_its_push_until_delivered() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_thread_session_run(&db).await;
        db.execute_script(
            "INSERT INTO sessions (id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES ('sess-thread','job-thread','claude','open',1,1,1);
             UPDATE jobs SET current_session_id='sess-thread' WHERE id='job-thread';",
        )
        .await
        .unwrap();
        // Parked on its own sub-agent work, the thread takes no turn for the
        // message yet, so its push stays queued rather than being carried by an
        // inline resume.
        set_head_turn(&db, "job-thread", "yielded", Some("dependency_wait")).await;

        append_thread_message(&orch, &db, "thread-1", None, "external", "are you there?")
            .await
            .expect("an active thread takes the prompt");
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM messages WHERE channel_type='thread' AND channel_id='thread-1'"
            )
            .await,
            1,
            "the conversation record exists past the session's empty cursor"
        );

        // Read the cards as the composer receives them.
        let pending = crate::messages::pending::list_pending_deliveries(&db, "job-thread")
            .await
            .unwrap();
        let cards = serde_json::to_value(&pending).unwrap();
        assert_eq!(pending.len(), 1, "{cards}");
        assert_eq!(cards[0]["source"], "push", "{cards}");
        assert!(
            cards[0]["headline"]
                .as_str()
                .is_some_and(|headline| headline.starts_with("Message from")),
            "{cards}"
        );

        let push_id = cards[0]["id"].as_str().unwrap().to_string();
        crate::orchestrator::attention_push::stamp_delivered(&db, &[push_id], "ev-carrier")
            .await
            .unwrap();
        let after = crate::messages::pending::list_pending_deliveries(&db, "job-thread")
            .await
            .unwrap();
        assert!(after.is_empty(), "{after:?}");
    }

    /// A push queued before closure stays on the books and stays undelivered.
    /// Suspending delivery rather than deleting the row is what makes the
    /// transition reversible: reopening restores the resume without anything
    /// having to be reconstructed.
    #[tokio::test(flavor = "current_thread")]
    async fn a_queued_push_cannot_resume_a_closed_thread_until_it_reopens() {
        let db = migrated_db().await;
        seed_thread_session_run(&db).await;
        seed_waking_push(&db, "job-thread").await;
        set_head_turn(&db, "job-thread", "complete", None).await;

        assert!(
            collect_pending_for_flush(&db, "run-1").is_ok(),
            "an active thread resumes for a pending rousing push"
        );

        set_thread_status(&db, "closed").await;
        assert!(
            collect_pending_for_flush(&db, "run-1").is_err(),
            "a closed thread is not resumed by anything queued for it"
        );
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM attention_pushes WHERE delivered_event_id IS NULL"
            )
            .await,
            1,
            "the push is suspended, not discarded"
        );

        set_thread_status(&db, "active").await;
        assert!(
            collect_pending_for_flush(&db, "run-1").is_ok(),
            "reopening makes the same push deliverable again"
        );
    }

    /// Seed the minimal project/issue/execution/job/run graph so
    /// `job_id_for_run` resolves `run-1` -> `job-1`. FK constraints are enforced
    /// in the lib-test harness, so the parent rows are required.
    async fn seed_run(db: &LocalDb, run_id: &str, job_id: &str) {
        let run_id = run_id.to_string();
        let job_id = job_id.to_string();
        db.write(|conn| {
            let run_id = run_id.clone();
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('proj-1', 'default', 'Test Project', 'proj', '/tmp/test-repo', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
                     VALUES ('issue-1', 'proj-1', 42, 'test issue', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('exec-1', 'recipe-default', 'issue-1', 'proj-1', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, recipe_node_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
                     VALUES (?1, 'exec-1', 'builder', 'issue-1', 'proj-1', 'builder', 'builder', 'running', 1, 1)",
                    params![job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO runs (id, project_id, job_id, status, created_at, updated_at)
                     VALUES (?1, 'proj-1', ?2, 'live', 1, 1)",
                    params![run_id.as_str(), job_id.as_str()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    // Use the live recorder so the fixture has a current delivery timestamp.
    // An epoch-timestamp notice is outside the delivery horizon and cannot
    // witness whether the idle flush preserves an otherwise deliverable row.
    async fn seed_side_channel_notice(db: &LocalDb, job_id: &str, child_uri: &str, content: &str) {
        let rendered =
            format!("[Side-channel] the user messaged your child {child_uri}:\n{content}");
        crate::orchestrator::wakes::record_live_comment_side_channel_message(
            db, job_id, child_uri, &rendered,
        )
        .await
        .unwrap();
    }

    /// Replace the job's head turn with one of the given `state` / `yield_reason`
    /// so `head_turn` reads a single, deterministic head turn.
    async fn set_head_turn(db: &LocalDb, job_id: &str, state: &str, reason: Option<&str>) {
        let job_id = job_id.to_string();
        let state = state.to_string();
        let reason = reason.map(|r| r.to_string());
        db.write(move |conn| {
            let job_id = job_id.clone();
            let state = state.clone();
            let reason = reason.clone();
            Box::pin(async move {
                conn.execute(
                    "DELETE FROM turns WHERE job_id = ?1",
                    params![job_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, yield_reason, start_reason, created_at, updated_at)
                     VALUES ('t-head', 'sess', 'run-1', ?1, 1, ?2, ?3, 'initial', 1, 1)",
                    params![job_id.as_str(), state.as_str(), reason.as_deref()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    /// Seed a live rousing (`wake`) push addressed to `recipient` (the job id).
    /// The `resolved:` prefix is informational, so `lazy_resolve_live` keeps it
    /// live regardless of referent state — it is a pending rousing push for the
    /// gate to weigh.
    /// A rousing `resolved:` push for the fixture's issue.
    ///
    /// The issue is resolved first because the two are one fact: a `resolved:`
    /// push exists only because an issue reached a terminal status, and the
    /// drain resolves the push against that status. Seeding the push over an
    /// unresolved issue describes a state the system cannot produce, and reads
    /// as a notice whose subject was reopened before delivery.
    async fn seed_waking_push(db: &LocalDb, recipient: &str) {
        db.execute(
            "UPDATE issues SET status = 'merged', merged_at = 1 WHERE id = 'issue-1'",
            (),
        )
        .await
        .unwrap();
        crate::orchestrator::attention_push::push(
            db,
            recipient,
            "cairn://p/proj/42",
            crate::orchestrator::attention_push::Wake::Wake,
            crate::orchestrator::attention_push::Boundary::Event,
            "resolved:cairn://p/proj/42",
        )
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn head_turn_classifies_state_and_yield_reason() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;

        // No head turn at all -> a fresh job is idle.
        assert_eq!(head_turn(&db, "job-1").await.unwrap(), HeadTurn::Idle);

        // Every durable-wait yield is a self-suspend: an own task/dependency
        // wait, an own question, an own permission, and a `waitFor` / long `run`.
        for reason in [
            "dependency_wait",
            "user_input",
            "permission",
            "wait",
            "provider_limit",
        ] {
            set_head_turn(&db, "job-1", "yielded", Some(reason)).await;
            assert_eq!(
                head_turn(&db, "job-1").await.unwrap(),
                HeadTurn::SelfSuspended,
                "yielded/{reason} must read as self-suspended"
            );
        }

        // A yield for some future non-self-suspend reason is not self-suspended,
        // but the host holds the turn — so it is live, not idle. A historical
        // `safety_pause` turn reads the same way: nothing writes it now, and it
        // must not hold messages.
        for reason in ["host_takeover", "safety_pause"] {
            set_head_turn(&db, "job-1", "yielded", Some(reason)).await;
            assert_eq!(head_turn(&db, "job-1").await.unwrap(), HeadTurn::Live);
            assert!(!head_turn(&db, "job-1").await.unwrap().is_self_suspended());
        }

        // A completed head turn is the normal idle case.
        set_head_turn(&db, "job-1", "complete", None).await;
        assert_eq!(head_turn(&db, "job-1").await.unwrap(), HeadTurn::Idle);

        // A stopped (interrupted) head turn is idle/terminal, not self-suspended.
        set_head_turn(&db, "job-1", "interrupted", None).await;
        assert_eq!(head_turn(&db, "job-1").await.unwrap(), HeadTurn::Idle);

        // A running or pending head turn is active work, not idle.
        for state in ["running", "pending"] {
            set_head_turn(&db, "job-1", state, None).await;
            let head = head_turn(&db, "job-1").await.unwrap();
            assert_eq!(head, HeadTurn::Live, "{state} must read as live");
            assert!(!head.is_self_suspended());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_does_not_resume_self_suspended_agent_with_waking_push() {
        // The core CAIRN-1876 rule: an agent suspended waiting on its own
        // sub-agent task (yielded/dependency_wait) is NOT resumed by a pending
        // rousing push, and the push stays queued to ride along later.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "yielded", Some("dependency_wait")).await;

        assert!(
            collect_pending_for_flush(&db, "run-1").is_err(),
            "a self-suspended agent must not be resumed by a pending wake push"
        );

        // Ride-along: the push remains undelivered, queued for the
        // own-work-resolution drain.
        let still_pending = crate::orchestrator::attention_push::list_pending(&db, "job-1")
            .await
            .unwrap();
        assert_eq!(
            still_pending.len(),
            1,
            "the gated wake push stays queued to ride along on the next resume"
        );
    }

    /// The Codex repeat brake that wrote `safety_pause` is gone, and 1,135
    /// directs once sat behind turns it parked. A head still carrying it is
    /// resumed for a pending push like any agent that is not self-suspended.
    #[tokio::test(flavor = "current_thread")]
    async fn a_historical_safety_pause_head_no_longer_holds_a_waking_push() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "yielded", Some("safety_pause")).await;

        let pending = collect_pending_for_flush(&db, "run-1")
            .expect("a safety-paused head must not hold the push");
        assert_eq!(pending.job_id, "job-1");
        assert_eq!(describe_hold(&db, "job-1").await, None);
    }

    async fn seed_provider_limit_hold(db: &LocalDb, job_id: &str, retry_not_before: i64) {
        db.execute(
            "INSERT INTO provider_limit_holds
               (id, job_id, run_id, turn_id, backend, retry_not_before, boundary_source, created_at, updated_at)
             VALUES ('hold-1', ?1, 'run-1', 't-head', 'claude', ?2, 'rate_limit_event', 1, 1)",
            params![job_id, retry_not_before],
        )
        .await
        .unwrap();
    }

    fn escalation() -> DirectMessageRef {
        DirectMessageRef {
            from: "cairn://p/proj/42/1/coordinator".into(),
            message_id: "msg-1".into(),
        }
    }

    /// Only a pure `waitFor` park yields to an escalated message. Every other
    /// self-suspension holds it, and says what holds it and what releases it.
    #[tokio::test(flavor = "current_thread")]
    async fn an_escalated_direct_holds_for_every_other_self_suspension_and_names_its_release() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_run(&db, "run-1", "job-1").await;
        db.execute(
            "INSERT INTO delegated_packets (execution_id, id, seq, parent_job_id, status, spec, created_at)
             VALUES ('exec-1', 'packet-1', 1, 'job-1', 'running', '{}', 1),
                    ('exec-1', 'packet-2', 2, 'job-1', 'completed', '{}', 1)",
            (),
        )
        .await
        .unwrap();
        let reset = chrono::Utc::now().timestamp() + 3_600;

        for (reason, expected, release) in [
            (
                "dependency_wait",
                Hold::Tasks { pending: 1 },
                "when they return",
            ),
            (
                "user_input",
                Hold::Question,
                "once the question is answered",
            ),
            (
                "permission",
                Hold::Permission,
                "once the request is decided",
            ),
            (
                "provider_limit",
                Hold::ProviderLimit {
                    retry_not_before: Some(reset),
                },
                "when the limit resets",
            ),
        ] {
            set_head_turn(&db, "job-1", "yielded", Some(reason)).await;
            if reason == "provider_limit" {
                seed_provider_limit_hold(&db, "job-1", reset).await;
            }
            let outcome =
                nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Interrupt, &escalation())
                    .unwrap();
            assert_eq!(outcome, NudgeOutcome::Held(expected.clone()), "{reason}");
            let said = expected.render();
            assert!(said.contains(release), "{reason}: {said}");
        }

        let at = crate::clock::stamp(reset).unwrap_or_else(|| reset.to_string());
        let limit = Hold::ProviderLimit {
            retry_not_before: Some(reset),
        };
        assert!(limit.render().contains(&at), "{}", limit.render());
        assert_eq!(
            limit.qualifier(),
            format!("held: provider limit until {at}")
        );
        assert_eq!(Hold::Tasks { pending: 1 }.qualifier(), "waiting: 1 task");
    }

    /// An escalate that cannot stop the recipient's turn is an error the sender
    /// sees, not a warn line in the host log under a bare "Sent".
    #[tokio::test(flavor = "current_thread")]
    async fn an_escalate_to_a_turn_with_no_stoppable_run_reports_the_failure() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_run(&db, "run-1", "job-1").await;
        set_head_turn(&db, "job-1", "running", None).await;
        // The turn reads running, but no run is starting or live to stop.
        db.execute("UPDATE runs SET status = 'complete' WHERE id = 'run-1'", ())
            .await
            .unwrap();

        let error =
            nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Interrupt, &escalation())
                .expect_err("a running turn with no stoppable run cannot be interrupted");
        assert!(
            error.contains("no live process holds the active turn"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_plain_direct_to_a_live_turn_is_left_queued_for_it() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_run(&db, "run-1", "job-1").await;
        set_head_turn(&db, "job-1", "running", None).await;

        assert_eq!(
            nudge_for_direct_message(&orch, "job-1", DeliveryUrgency::Steer, &escalation())
                .unwrap(),
            NudgeOutcome::QueuedForActiveTurn
        );
    }

    /// cairn/6189: a direct queued just before the recipient's turn ends must
    /// not be stranded by the "active turn" answer. The push stays pending, and
    /// the turn-end flush that follows the settlement finds it and resumes.
    #[tokio::test(flavor = "current_thread")]
    async fn a_direct_queued_against_an_ending_turn_is_left_for_the_turn_end_flush() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_run(&db, "run-1", "job-1").await;
        set_head_turn(&db, "job-1", "running", None).await;
        seed_waking_push(&db, "job-1").await;

        assert_eq!(
            nudge_job_for_urgency(&orch, "job-1", DeliveryUrgency::Steer).unwrap(),
            NudgeOutcome::QueuedForActiveTurn
        );
        // The turn the nudge saw ends without reaching another boundary.
        db.execute(
            "UPDATE turns SET state = 'interrupted', end_reason = 'provider_failure', ended_at = 2
              WHERE id = 't-head'",
            (),
        )
        .await
        .unwrap();
        let pending = collect_pending_for_flush(&db, "run-1")
            .expect("the turn-end flush must see the push the nudge left queued");
        assert_eq!(pending.job_id, "job-1");
        assert_eq!(
            crate::orchestrator::attention_push::list_pending(&db, "job-1")
                .await
                .unwrap()
                .len(),
            1,
            "nothing sealed it into the ended turn"
        );
    }

    /// The other order: the turn has already ended and its flush found nothing
    /// when the direct arrives. The nudge must not answer "active turn" and
    /// leave it; it takes the idle path itself.
    #[tokio::test(flavor = "current_thread")]
    async fn a_direct_arriving_after_the_turn_end_flush_takes_the_idle_path() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_run(&db, "run-1", "job-1").await;
        set_head_turn(&db, "job-1", "interrupted", None).await;
        assert_eq!(
            collect_pending_for_flush(&db, "run-1")
                .err()
                .unwrap()
                .decline,
            FlushDecline::NothingPending
        );
        seed_waking_push(&db, "job-1").await;

        let outcome = nudge_job_for_urgency(&orch, "job-1", DeliveryUrgency::Steer);
        assert!(
            !matches!(outcome, Ok(NudgeOutcome::QueuedForActiveTurn)),
            "{outcome:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_nudge_to_an_idle_recipient_reports_the_turn_it_started() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_thread_session_run(&db).await;
        db.execute_script(
            "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES('sess-1','job-thread','claude','open',1,1,1);
             UPDATE jobs SET current_session_id='sess-1' WHERE id='job-thread';
             UPDATE runs SET session_id='sess-1' WHERE id='run-1';
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, started_at, updated_at)
               VALUES('turn-1','sess-1','run-1','job-thread',1,'complete','initial',1,1,1);
             UPDATE jobs SET current_turn_id='turn-1' WHERE id='job-thread';",
        )
        .await
        .unwrap();
        crate::messages::queued::enqueue_async(
            &db,
            "job-thread",
            "are you there?",
            DeliveryUrgency::Queue,
        )
        .await
        .unwrap();

        assert_eq!(
            nudge_job_for_urgency(&orch, "job-thread", DeliveryUrgency::Queue).unwrap(),
            NudgeOutcome::Resumed
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn provider_limit_hold_defers_external_waking_push() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "yielded", Some("provider_limit")).await;
        assert!(collect_pending_for_flush(&db, "run-1").is_err());
        assert_eq!(
            crate::orchestrator::attention_push::list_pending(&db, "job-1")
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_resumes_idle_agent_with_waking_push() {
        // The contrast: a genuinely idle agent (head turn completed, nothing of
        // its own pending) with the same pending wake push IS resumed.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "complete", None).await;

        let pending = collect_pending_for_flush(&db, "run-1")
            .expect("an idle agent with a pending wake push must resume");
        assert_eq!(pending.job_id, "job-1");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_resumes_idle_agent_with_review_backed_by_confirmed_create_pr() {
        // CAIRN-2410: the coordinator's pending review push is backed ONLY by a
        // confirmed create-pr artifact with no merge_requests row (the pre-open
        // window). That is precisely the state the drain/wake liveness predicate
        // used to read as dead, silently declining the resume. It must now
        // resume, since `has_pending_waking_live` -> `lazy_resolve_live` treats the
        // confirmed create-pr/no-MR-row window as live.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        set_head_turn(&db, "job-1", "complete", None).await;
        // A confirmed create-pr artifact for the issue, and NO merge_requests row.
        db.execute_script(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-pr','job-1','create-pr',1,'{}',1,'create-pr',1,1,1);",
        )
        .await
        .unwrap();
        // A pending rousing review push whose referent is that issue.
        crate::orchestrator::attention_push::push(
            &db,
            "job-1",
            "cairn://p/proj/42",
            crate::orchestrator::attention_push::Wake::Wake,
            crate::orchestrator::attention_push::Boundary::Event,
            "review:cairn://p/proj/42",
        )
        .await
        .unwrap();

        let pending = collect_pending_for_flush(&db, "run-1").expect(
            "an idle coordinator whose review is backed by a confirmed create-pr must resume",
        );
        assert_eq!(pending.job_id, "job-1");
    }

    /// A cancelled job is not resumed even while a stray session of its stays
    /// open: Cancelled to Running is not a transition, so the flush declines
    /// rather than leaving the launch funnel to refuse.
    #[tokio::test(flavor = "current_thread")]
    async fn flush_declines_a_cancelled_job_with_an_open_session() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_waking_push(&db, "job-1").await;
        set_head_turn(&db, "job-1", "complete", None).await;
        db.execute_script(
            "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES('sess-1','job-1','claude','open',1,1,1);
             UPDATE jobs SET current_session_id='sess-1' WHERE id='job-1';",
        )
        .await
        .unwrap();
        assert!(
            collect_pending_for_flush(&db, "run-1").is_ok(),
            "the fixture resumes before the cancel"
        );

        db.execute("UPDATE jobs SET status='cancelled' WHERE id='job-1'", ())
            .await
            .unwrap();

        assert!(collect_pending_for_flush(&db, "run-1").is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_returns_none_when_nothing_queued() {
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        assert!(collect_pending_for_flush(&db, "run-1").is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_returns_none_for_unknown_run() {
        let db = migrated_db().await;
        // No run row -> job can't be resolved -> nothing to flush, no panic.
        assert!(collect_pending_for_flush(&db, "ghost").is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_does_not_resume_for_passive_notice_only() {
        // Passive side-channel notices do not wake idle jobs by themselves. They
        // remain pending until the next agent-bound payload/resume.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        seed_side_channel_notice(
            &db,
            "job-1",
            "cairn://p/p/9/1/child",
            "user pinged the child",
        )
        .await;

        assert_eq!(
            crate::orchestrator::wakes::peek_pending_live_side_channel_for_job(&db, "job-1")
                .await
                .unwrap()
                .len(),
            1,
            "the fixture must be deliverable before testing that the flush leaves it alone"
        );
        assert!(
            collect_pending_for_flush(&db, "run-1").is_err(),
            "passive side-channel notices must not trigger flush-on-idle resumes"
        );

        // Peek did not stamp -> the notice is still claimable when a later
        // agent-bound payload/resume happens.
        let still_pending =
            crate::messages::side_channel::claim_pending_side_channel_for_job_async(&db, "job-1")
                .await
                .unwrap();
        assert_eq!(still_pending.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn collect_pending_resumes_for_stranded_queued_message_but_keeps_prompt_empty() {
        // A queued user message (CAIRN-1309) with no directs still triggers a
        // resume (Some), but the prompt stays None: continue_job_impl claims and
        // injects queued messages itself, so the flush must not build them in.
        let db = migrated_db().await;
        seed_run(&db, "run-1", "job-1").await;
        crate::messages::queued::enqueue_async(
            &db,
            "job-1",
            "please also add a test",
            crate::messages::queued::Delivery::Queue,
        )
        .await
        .unwrap();

        let pending = collect_pending_for_flush(&db, "run-1")
            .expect("a queued user message should trigger a resume");
        assert_eq!(pending.job_id, "job-1");
        assert_eq!(pending.queued_count, 1);

        // Peek did not stamp -> the message is still claimable by the resume.
        let still_pending = crate::messages::queued::claim_all_for_job_async(&db, "job-1")
            .await
            .unwrap();
        assert_eq!(still_pending.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stop_target_prefers_process_holding_run_over_newest_row() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_run(&db, "run-process", "job-1").await;
        db.execute(
            "INSERT INTO runs(id, project_id, job_id, status, created_at, updated_at) VALUES('run-newest','proj-1','job-1','live',2,2)",
            (),
        )
        .await
        .unwrap();
        let handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(
                cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
            ))),
            Some("sess-process".to_string()),
            Some("job-1".to_string()),
        );
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-process".to_string(), handle);

        assert_eq!(
            crate::orchestrator::lifecycle::live_run_id_for_job(&orch, "job-1").as_deref(),
            Some("run-process")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn interrupt_owns_flush_without_backend_ack_and_delivers_once_in_same_session() {
        let orch = test_orchestrator(migrated_db().await);
        let db = orch.db.local.clone();
        seed_thread_session_run(&db).await;
        db.execute_script(
            "INSERT INTO sessions(id, job_id, backend, status, sequence, created_at, updated_at)
               VALUES('sess-1','job-thread','claude','open',1,1,1);
             UPDATE jobs SET current_session_id='sess-1' WHERE id='job-thread';
             UPDATE runs SET session_id='sess-1' WHERE id='run-1';
             INSERT INTO turns(id, session_id, run_id, job_id, sequence, state, start_reason, created_at, started_at, updated_at)
               VALUES('turn-1','sess-1','run-1','job-thread',1,'running','initial',1,1,1);
             UPDATE jobs SET current_turn_id='turn-1' WHERE id='job-thread';",
        )
        .await
        .unwrap();
        let mut handle = RunHandle::new(
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(Some(
                cairn_models::session::claude::ClaudeControl::boxed(Box::new(Vec::<u8>::new())),
            ))),
            Some("sess-1".to_string()),
            Some("job-thread".to_string()),
        );
        handle.begin_turn("turn-1");
        orch.process_state
            .processes
            .lock()
            .unwrap()
            .register("run-1".to_string(), handle);

        crate::messages::queued::enqueue_async(
            &db,
            "job-thread",
            "interrupt now",
            DeliveryUrgency::Interrupt,
        )
        .await
        .unwrap();

        nudge_job_for_urgency(&orch, "job-thread", DeliveryUrgency::Interrupt)
            .expect("synchronous interrupt flush should resume without an abort acknowledgement");
        let delivered = count(
            &db,
            "SELECT COUNT(*) FROM queued_messages WHERE job_id='job-thread' AND delivered_at IS NOT NULL",
        )
        .await;
        assert_eq!(delivered, 1);
        assert_eq!(
            db.query_one(
                "SELECT current_session_id FROM jobs WHERE id='job-thread'",
                (),
                |row| row.text(0)
            )
            .await
            .unwrap(),
            "sess-1"
        );
        let event_count = count(
            &db,
            "SELECT COUNT(*) FROM events WHERE json_extract(data, '$.queued_message_id') IS NOT NULL",
        )
        .await;
        nudge_job_for_urgency(&orch, "job-thread", DeliveryUrgency::Interrupt).unwrap();
        assert_eq!(
            count(
                &db,
                "SELECT COUNT(*) FROM events WHERE json_extract(data, '$.queued_message_id') IS NOT NULL"
            )
            .await,
            event_count
        );
    }

    /// A cancelled job whose session is closed reports the closed session, so
    /// the launch funnel can answer with the session's reason; only a cancelled
    /// job whose session still reads open reports the bare cancel.
    #[tokio::test]
    async fn a_closed_session_outranks_the_bare_cancelled_refusal() {
        let db = crate::storage::migrated_test_db("delivery-refusal-order.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES ('proj-1','default','P','proj','/tmp/p',1,1);
             INSERT INTO jobs(id, project_id, status, current_session_id, created_at, updated_at)
               VALUES ('closed-cancelled','proj-1','cancelled','s-closed',1,1),
                      ('open-cancelled','proj-1','cancelled','s-open',1,1),
                      ('open-complete','proj-1','complete','s-live',1,1);
             INSERT INTO sessions(id, job_id, backend, status, terminal_reason, sequence, created_at, updated_at)
               VALUES ('s-closed','closed-cancelled','claude','closed','issue_merged',1,1,1),
                      ('s-open','open-cancelled','claude','open',NULL,1,1,1),
                      ('s-live','open-complete','claude','open',NULL,1,1,1);",
        )
        .await
        .unwrap();

        for (job, expected) in [
            (
                "closed-cancelled",
                Some(DeliveryRefusal::SessionRefusesTurns),
            ),
            ("open-cancelled", Some(DeliveryRefusal::Cancelled)),
            ("open-complete", None),
        ] {
            assert_eq!(delivery_refusal(&db, job).await.unwrap(), expected, "{job}");
        }
    }
}
