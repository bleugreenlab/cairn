//! Every write to the `tenets` table.
//!
//! Each transition validates against the row it reads inside the same write
//! transaction and refuses BEFORE it writes anything, so a refusal commits an
//! empty transaction and a concurrent writer is caught by the transaction's
//! own conflict check rather than by a second read outside it.

use cairn_common::identity::PrincipalRef;
use cairn_db::turso::{params, Connection};

use super::read::row_conn;
use super::{
    emit_changed, successor_refusal, AnswerSource, TenetOrigin, TenetRow, TenetStatus, Verdict,
    MAX_RATIONALE_CHARS, MAX_STATEMENT_CHARS, OVER_LENGTH_REFUSAL, RATIONALE_OVER_LENGTH_REFUSAL,
};
use crate::execution::gates::sweep::notify_gate_evidence_changed;
use crate::services::EventEmitter;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// What a thread proposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    /// How the project does things, as the thread proposes it: one sentence.
    pub statement: String,
    /// Why, the alternatives ruled out, and where it was argued.
    pub rationale: Option<String>,
    /// An approved tenet this one replaces once it is itself approved.
    pub supersedes: Option<i64>,
    pub origin: TenetOrigin,
}

/// Who proposes: a thread, through its own session or a task it delegated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposer {
    pub project_id: String,
    /// The proposing thread's id, from `jobs.thread_id` of the proposing run.
    pub thread_id: String,
    /// The run that proposed, as its own agent principal.
    pub principal: PrincipalRef,
}

impl Proposer {
    /// The proposer behind an authenticated agent run, or the refusal for a
    /// run that acts for no thread.
    ///
    /// Lineage is `jobs.thread_id`, which a delegated task inherits from the
    /// thread that spawned it, so a thread's sub-agent proposes as its thread.
    /// A builder or coordinator working an issue carries no thread id even
    /// when a thread filed the issue: it is not the thread.
    pub(crate) async fn for_agent(
        db: &LocalDb,
        agent: &crate::mcp::handlers::run_context::AuthenticatedAgent,
        principal: PrincipalRef,
    ) -> Result<Self, String> {
        let job_id = agent.job_id.clone();
        let lineage = db
            .query_opt(
                "SELECT j.thread_id, t.project_id
                 FROM jobs j JOIN threads t ON t.id = j.thread_id
                 WHERE j.id = ?1",
                (job_id,),
                |row| Ok((row.text(0)?, row.text(1)?)),
            )
            .await
            .map_err(|error| format!("Failed to resolve the proposing thread: {error}"))?;
        let Some((thread_id, project_id)) = lineage else {
            return Err(NON_THREAD_REFUSAL.to_string());
        };
        Ok(Self {
            project_id,
            thread_id,
            principal,
        })
    }
}

pub const NON_THREAD_REFUSAL: &str = "only a thread proposes tenets: this run acts for no thread. Ask the thread that owns this work to propose it, or put the question in your issue or PR for the operator.";

/// A tenet row as a caller builds it, before it has an id or a number. The
/// shape every insert takes, including the ruling importer's.
#[derive(Debug, Clone)]
pub struct NewTenet {
    pub project_id: String,
    pub proposing_thread_id: String,
    pub slug: Option<String>,
    pub statement: String,
    pub rationale: Option<String>,
    pub status: TenetStatus,
    pub verdict: Option<Verdict>,
    pub comment: Option<String>,
    pub answered_by: Option<PrincipalRef>,
    pub answer_source: Option<AnswerSource>,
    pub answered_at: Option<i64>,
    pub supersedes: Option<i64>,
    pub origin: TenetOrigin,
    pub legacy: Option<serde_json::Value>,
    pub proposer: PrincipalRef,
    pub now: i64,
}

/// Take the project's next tenet number. Must run in the same write
/// transaction as the INSERT that uses it, which is what keeps numbers unique
/// and gapless under concurrent proposals.
pub async fn allocate_number_conn(conn: &Connection, project_id: &str, now: i64) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "SELECT next_tenet_number FROM projects WHERE id = ?1",
            params![project_id],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| DbError::Row(format!("project not found: {project_id}")))?;
    let number = row.opt_i64(0)?.unwrap_or(1);
    conn.execute(
        "UPDATE projects SET next_tenet_number = ?1, updated_at = ?2 WHERE id = ?3",
        params![number + 1, now, project_id],
    )
    .await?;
    Ok(number)
}

/// Insert `tenet` under the project's next number, returning its id and
/// number. The caller owns the transaction.
pub async fn insert_conn(conn: &Connection, tenet: &NewTenet) -> DbResult<(String, i64)> {
    let number = allocate_number_conn(conn, &tenet.project_id, tenet.now).await?;
    let id = cairn_common::ids::mint_child(&tenet.project_id);
    conn.execute(
        "INSERT INTO tenets (
            id, project_id, number, proposing_thread_id, slug, statement, status, verdict,
            comment, answered_by_json, answer_source, answered_at, supersedes, origin,
            legacy_json, proposer_principal_json, created_at, updated_at, rationale
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
                   ?17, ?17, ?18)",
        params![
            id.as_str(),
            tenet.project_id.as_str(),
            number,
            tenet.proposing_thread_id.as_str(),
            tenet.slug.as_deref(),
            tenet.statement.as_str(),
            tenet.status.as_str(),
            tenet.verdict.map(Verdict::as_str),
            tenet.comment.as_deref(),
            tenet.answered_by.as_ref().map(encode).transpose()?,
            tenet.answer_source.map(AnswerSource::as_str),
            tenet.answered_at,
            tenet.supersedes,
            tenet.origin.as_str(),
            tenet.legacy.as_ref().map(encode).transpose()?,
            encode(&tenet.proposer)?,
            tenet.now,
            tenet.rationale.as_deref()
        ],
    )
    .await?;
    Ok((id, number))
}

/// A stored JSON column, with an encoding failure reported as the row error
/// it is.
fn encode<T: serde::Serialize>(value: &T) -> DbResult<String> {
    serde_json::to_string(value).map_err(|error| DbError::Row(error.to_string()))
}

/// A proposal's text as it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Validated {
    pub statement: String,
    pub rationale: Option<String>,
}

/// The proposal's statement and rationale, trimmed (a blank rationale is
/// none), or the refusal naming what is wrong with them.
pub(super) fn validate(proposal: &Proposal) -> Result<Validated, String> {
    let statement = proposal.statement.trim().to_string();
    if statement.is_empty() {
        return Err("a proposal needs a non-empty statement".into());
    }
    let chars = statement.chars().count();
    if chars > MAX_STATEMENT_CHARS {
        return Err(format!(
            "{OVER_LENGTH_REFUSAL} (this statement is {chars} characters; it may be at most {MAX_STATEMENT_CHARS})"
        ));
    }
    let rationale = proposal
        .rationale
        .as_deref()
        .map(str::trim)
        .filter(|rationale| !rationale.is_empty())
        .map(str::to_string);
    if let Some(rationale) = &rationale {
        let chars = rationale.chars().count();
        if chars > MAX_RATIONALE_CHARS {
            return Err(format!(
                "{RATIONALE_OVER_LENGTH_REFUSAL} (this rationale is {chars} characters)"
            ));
        }
    }
    if proposal.origin == TenetOrigin::MigratedRuling {
        return Err("migrated rulings are imported, never proposed".into());
    }
    Ok(Validated {
        statement,
        rationale,
    })
}

fn not_found(number: i64) -> String {
    format!("no tenet #{number} in this project")
}

/// Why `supersedes: m` cannot be honored, or `None` when #m is an approved
/// tenet still in force.
fn supersede_refusal(target: Option<&TenetRow>, number: i64) -> Option<String> {
    match target {
        None => Some(format!(
            "supersedes: {number} names no tenet in this project"
        )),
        Some(row) if row.status == TenetStatus::Approved => None,
        Some(row) if row.status == TenetStatus::Superseded => Some(format!(
            "tenet #{number} is already superseded; supersede the tenet that replaced it instead"
        )),
        Some(row) => Some(format!(
            "tenet #{number} is {}; only an approved tenet can be superseded",
            row.status.as_str()
        )),
    }
}

/// Propose a tenet for the operator's verdict.
pub async fn propose(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    now: i64,
    proposer: &Proposer,
    proposal: Proposal,
) -> Result<TenetRow, String> {
    let Validated {
        statement,
        rationale,
    } = validate(&proposal)?;
    let new = NewTenet {
        project_id: proposer.project_id.clone(),
        proposing_thread_id: proposer.thread_id.clone(),
        slug: None,
        statement,
        rationale,
        status: TenetStatus::Proposed,
        verdict: None,
        comment: None,
        answered_by: None,
        answer_source: None,
        answered_at: None,
        supersedes: proposal.supersedes,
        origin: proposal.origin,
        legacy: None,
        proposer: proposer.principal.clone(),
        now,
    };
    let outcome = db
        .write(|conn| {
            let new = new.clone();
            Box::pin(async move {
                if let Some(number) = new.supersedes {
                    let target = row_conn(conn, &new.project_id, number).await?;
                    if let Some(refusal) = supersede_refusal(target.as_ref(), number) {
                        return Ok(Err(refusal));
                    }
                }
                let (_, number) = insert_conn(conn, &new).await?;
                Ok(row_conn(conn, &new.project_id, number)
                    .await?
                    .ok_or_else(|| "the proposed tenet was not found after insert".to_string()))
            })
        })
        .await
        .map_err(|error| format!("Failed to propose tenet: {error}"))??;
    emit_changed(emitter, "insert");
    Ok(outcome)
}

/// Withdraw an unanswered proposal. Only the proposing thread's lineage may.
pub async fn withdraw(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    now: i64,
    project_id: &str,
    number: i64,
    withdrawing_thread_id: &str,
) -> Result<TenetRow, String> {
    let project_id = project_id.to_string();
    let withdrawing_thread_id = withdrawing_thread_id.to_string();
    let outcome = db
        .write(|conn| {
            let project_id = project_id.clone();
            let withdrawing_thread_id = withdrawing_thread_id.clone();
            Box::pin(async move {
                let Some(row) = row_conn(conn, &project_id, number).await? else {
                    return Ok(Err(not_found(number)));
                };
                if row.proposing_thread_id != withdrawing_thread_id {
                    return Ok(Err(format!(
                        "only the thread that proposed tenet #{number} can withdraw it"
                    )));
                }
                match row.status {
                    TenetStatus::Proposed => {}
                    TenetStatus::Withdrawn => {
                        return Ok(Err(format!("tenet #{number} is already withdrawn")))
                    }
                    _ => return Ok(Err(successor_refusal(number))),
                }
                conn.execute(
                    "UPDATE tenets SET status = 'withdrawn', updated_at = ?1 WHERE id = ?2",
                    params![now, row.id.as_str()],
                )
                .await?;
                Ok(row_conn(conn, &project_id, number)
                    .await?
                    .ok_or_else(|| not_found(number)))
            })
        })
        .await
        .map_err(|error| format!("Failed to withdraw tenet: {error}"))??;
    emit_changed(emitter, "update");
    notify_gate_evidence_changed();
    Ok(outcome)
}

/// The operator's verdict on a proposal.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    pub verdict: Verdict,
    pub comment: Option<String>,
    /// The answering operator: a signed-in Human principal, or the desktop's
    /// Machine device identity when no one is signed in.
    pub principal: PrincipalRef,
}

/// A recorded verdict and what it changed.
#[derive(Debug, Clone, PartialEq)]
pub struct Answered {
    pub tenet: TenetRow,
    /// The tenet this approval superseded, when it named one.
    pub superseded: Option<i64>,
    /// The proposing thread's session, when the verdict message reached it
    /// with an urgency that wants an idle session roused (a decline).
    /// [`super::answer_and_wake`] rouses it.
    pub wake: Option<String>,
}

/// Record the operator's verdict on proposal `number`.
///
/// `yes` or `your_call` approves; `no` declines. Approving a tenet that
/// names `supersedes: m` marks #m superseded in the same transaction. The
/// verdict is delivered in that transaction too (`super::verdict`): one
/// message to the proposing thread and, on approval, a comment on every issue
/// the tenet gates. A caller holding an orchestrator answers through
/// [`super::answer_and_wake`], which also rouses an idle proposer.
pub async fn answer(
    db: &LocalDb,
    emitter: &dyn EventEmitter,
    now: i64,
    project_id: &str,
    number: i64,
    answer: Answer,
) -> Result<Answered, String> {
    let source = AnswerSource::for_answerer(&answer.principal)?;
    let answered_by = serde_json::to_string(&answer.principal).map_err(|e| e.to_string())?;
    let comment = answer
        .comment
        .as_deref()
        .map(str::trim)
        .filter(|comment| !comment.is_empty())
        .map(str::to_string);
    let project_id = project_id.to_string();
    let verdict = answer.verdict;
    let outcome = db
        .write(|conn| {
            let project_id = project_id.clone();
            let answered_by = answered_by.clone();
            let comment = comment.clone();
            Box::pin(async move {
                let Some(row) = row_conn(conn, &project_id, number).await? else {
                    return Ok(Err(not_found(number)));
                };
                match row.status {
                    TenetStatus::Proposed => {}
                    TenetStatus::Withdrawn => {
                        return Ok(Err(format!(
                            "tenet #{number} was withdrawn by its thread; there is nothing to answer"
                        )))
                    }
                    _ => {
                        return Ok(Err(format!(
                            "tenet #{number} is already {}; a verdict is given once",
                            row.status.as_str()
                        )))
                    }
                }
                let status = verdict.status();
                let superseded = match (status, row.supersedes) {
                    (TenetStatus::Approved, Some(target)) => {
                        let target_row = row_conn(conn, &project_id, target).await?;
                        if let Some(refusal) = supersede_refusal(target_row.as_ref(), target) {
                            return Ok(Err(format!(
                                "tenet #{number} supersedes #{target}, but approving it now would supersede nothing: {refusal}. Answer no, or ask the thread to withdraw it and propose again"
                            )));
                        }
                        conn.execute(
                            "UPDATE tenets SET status = 'superseded', updated_at = ?1
                             WHERE project_id = ?2 AND number = ?3 AND status = 'approved'",
                            params![now, project_id.as_str(), target],
                        )
                        .await?;
                        Some(target)
                    }
                    _ => None,
                };
                conn.execute(
                    "UPDATE tenets
                     SET status = ?1, verdict = ?2, comment = ?3, answered_by_json = ?4,
                         answer_source = ?5, answered_at = ?6, updated_at = ?6
                     WHERE id = ?7 AND status = 'proposed'",
                    params![
                        status.as_str(),
                        verdict.as_str(),
                        comment.as_deref(),
                        answered_by.as_str(),
                        source.as_str(),
                        now,
                        row.id.as_str()
                    ],
                )
                .await?;
                let tenet = row_conn(conn, &project_id, number)
                    .await?
                    .ok_or_else(|| DbError::Row(not_found(number)))?;
                let delivered =
                    super::verdict::deliver_conn(conn, &tenet, superseded, now).await?;
                Ok(Ok((
                    Answered {
                        tenet,
                        superseded,
                        wake: delivered.wake,
                    },
                    delivered.comments,
                )))
            })
        })
        .await
        .map_err(|error| format!("Failed to record the verdict: {error}"))??;
    let (outcome, comments) = outcome;
    emit_changed(emitter, "update");
    if comments > 0 {
        if let Err(error) = emitter.emit(
            "db-change",
            serde_json::json!({"table": "comments", "action": "insert"}),
        ) {
            tracing::warn!(%error, "failed to emit comments db-change notification");
        }
    }
    notify_gate_evidence_changed();
    Ok(outcome)
}

/// What deleting thread `thread_id` does to its tenets, inside the delete's
/// own transaction.
///
/// A tenet with a verdict is durable project history: what the operator
/// decided outlives the thread that asked. So a thread holding any approved,
/// declined, or superseded tenet is not deletable, and the refusal says to
/// close it instead, which keeps its session dormant and its tenets in force.
/// Its unanswered and withdrawn proposals carry no decision; they are deleted
/// with it. The caller announces the removal after its transaction commits.
pub(crate) async fn release_deleted_thread_conn(
    conn: &Connection,
    thread_id: &str,
) -> DbResult<ReleasedTenets> {
    let mut rows = conn
        .query(
            "SELECT number, status FROM tenets WHERE proposing_thread_id = ?1 ORDER BY number",
            params![thread_id],
        )
        .await?;
    let mut held = Vec::new();
    let mut released = ReleasedTenets::default();
    while let Some(row) = rows.next().await? {
        let number = row.i64(0)?;
        let status = TenetStatus::parse(&row.text(1)?);
        if status.is_some_and(TenetStatus::has_verdict) {
            held.push(format!("#{number}"));
        }
        if status == Some(TenetStatus::Proposed) {
            released.pending += 1;
        }
        released.removed += 1;
    }
    drop(rows);
    if !held.is_empty() {
        return Err(DbError::Row(format!(
            "this thread's tenets {} carry the operator's verdicts and are project history; close the thread instead of deleting it",
            held.join(", ")
        )));
    }
    conn.execute(
        "DELETE FROM tenets WHERE proposing_thread_id = ?1",
        params![thread_id],
    )
    .await?;
    Ok(released)
}

/// What a thread deletion took from the tenets table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReleasedTenets {
    /// Every row removed, unanswered and withdrawn alike.
    pub removed: usize,
    /// The unanswered ones, each of which a gate may have named.
    pub pending: usize,
}

impl ReleasedTenets {
    /// Announce the removal, after the deleting transaction has committed.
    pub(crate) fn announce(self, emitter: &dyn EventEmitter) {
        if self.removed > 0 {
            emit_changed(emitter, "delete");
        }
        if self.pending > 0 {
            // A gate naming one of those proposals now names nothing.
            notify_gate_evidence_changed();
        }
    }
}
