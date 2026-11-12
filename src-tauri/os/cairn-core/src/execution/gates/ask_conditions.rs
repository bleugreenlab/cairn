//! Conditions on a pending ask: an approver's deferral of a gated artifact
//! (`patch {defer: {until: [...]}}`), resolved by the gate sweep's own pass.
//!
//! ## One engine
//!
//! A condition is a row in `ask_conditions` keyed by `(ask_kind, ask_id)`, its
//! `until_json` clauses written in the `after:` vocabulary and resolved through
//! the same [`GateVocabulary`] a held execution's gates are. The sweep calls
//! [`sweep_ask_conditions`] at the end of every pass; there is no second loop,
//! no timer and no reminder. This change writes only artifact deferrals
//! (`effect = 'resurface'`); a question's `moot_when` (cairn/5519) is another
//! kind of row in the same table, through the same pass, with its own effect.
//!
//! ## Latch, then deliver, exactly once
//!
//! Each open row is latched once, by a guarded update, as one of:
//! - `cleared`: the ask it deferred is no longer pending (confirmed, revised
//!   into a new version, its job ended, its issue resolved). Nobody is woken.
//! - `satisfied`: every clause is true.
//! - `foreclosed`: some clause can never become true, so under AND semantics
//!   the whole condition never will.
//! - `superseded`: the approver deferred again (written by [`defer`]).
//!
//! A satisfied or foreclosed row then owes one re-wake to the ask's approver,
//! the same parent-axis coordinator a foreclosed `after:` gate wakes. The
//! re-wake's message row, whose id is derived from the condition, is the
//! delivery record, exactly as for a foreclosure, so dismissing the push does
//! not re-arm it. When the approver is the operator (no coordinating session),
//! nothing is sent: the notification center re-surfaces the entry as a new
//! occurrence instead.

use cairn_db::turso::params;
use serde_json::Value;

use super::authoring::{canonical_predicates, gate_vocabulary, parse_after};
use super::vocabulary::{GateContext, GateOutcome, GateVocabulary};
use crate::execution::approvals::{
    pending_approval_for_job_conn, pending_approvals_conn, render_until, ApprovalScope,
    PendingApproval, ASK_KIND_ARTIFACT,
};
use crate::orchestrator::Orchestrator;
use crate::routes::TriggerClause;
use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// The one effect artifact deferrals run when they fire.
pub const EFFECT_RESURFACE: &str = "resurface";

/// The facts an approver may defer on, for refusals and the affordance.
pub fn deferrable_facts() -> Vec<&'static str> {
    gate_vocabulary()
        .fact_names()
        .into_iter()
        .filter(|fact| !super::capacity::CAPACITY_FACTS.contains(fact))
        .collect()
}

/// What one pass did with ask conditions.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AskSweepReport {
    /// Conditions latched this pass, as `(condition id, fired_kind)`.
    pub fired: Vec<(String, &'static str)>,
    /// Conditions whose re-wake this pass delivered, or handed to the operator.
    pub delivered: Vec<String>,
}

// ---------------------------------------------------------------------------
// The write
// ---------------------------------------------------------------------------

/// The `until` value of an artifact patch's `defer` key, after refusing the
/// shapes a deferral cannot take: `defer` beside `confirmed` or `action`
/// (those act now, a deferral waits on a cause), a non-object, or a field
/// other than `until`.
pub fn defer_until(payload: &Value) -> Result<Option<&Value>, String> {
    for other in ["confirmed", "action"] {
        if payload.get(other).is_some() {
            return Err(format!(
                "payload.defer and payload.{other} are mutually exclusive"
            ));
        }
    }
    let defer = payload
        .get("defer")
        .and_then(Value::as_object)
        .ok_or_else(|| "payload.defer must be an object: {until:[gate clause, ...]}".to_string())?;
    if let Some(unknown) = defer.keys().find(|key| key.as_str() != "until") {
        return Err(format!(
            "payload.defer.{unknown} is not a deferral field; the only one is until"
        ));
    }
    Ok(defer.get("until"))
}

/// Defer the pending approval on `job_id` until every clause of `until` holds.
///
/// `artifact_name` is the name the write addressed (`/plan`), checked against
/// the pending version so a deferral never lands on a different artifact than
/// the writer meant. Returns the reply line.
pub async fn defer(
    orch: &Orchestrator,
    db: &LocalDb,
    job_id: &str,
    artifact_name: Option<&str>,
    until: Option<&Value>,
    created_by: Option<String>,
    dry_run: bool,
) -> Result<String, String> {
    let key = "payload.defer.until";
    let clauses = parse_after(until, key)?;
    if clauses.is_empty() {
        return Err(format!(
            "{key} is empty: a deferral names what it waits for. e.g. \
             {{defer:{{until:[{{fact:\"issue_resolution\",issue:\"cairn://p/PROJECT/N\",resolution:\"merged\"}}]}}}}"
        ));
    }
    let predicates = canonical_predicates(&clauses, key)?;
    let clauses: Vec<TriggerClause> = predicates
        .iter()
        .filter_map(|json| serde_json::from_str(json).ok())
        .collect();

    let job = job_id.to_string();
    let approval = db
        .read(move |conn| {
            let job = job.clone();
            Box::pin(async move { pending_approval_for_job_conn(conn, &job).await })
        })
        .await
        .map_err(|error| error.to_string())?;
    let Some(approval) = approval else {
        return Err(
            "nothing to defer: this node has no gated artifact waiting on its approver \
             (it is confirmed, the node ended, or its issue resolved)"
                .to_string(),
        );
    };
    if let Some(name) = artifact_name {
        if approval.output_name.as_deref() != Some(name) && name != "artifact" {
            return Err(format!(
                "{name} is not what waits on its approver here; the pending artifact is {}. \
                 Defer that address instead.",
                approval.artifact_uri
            ));
        }
    }

    let vocabulary = gate_vocabulary();
    let ctx = GateContext::new(orch, orch.services.clock.now());
    let mut all_true = true;
    for clause in &clauses {
        match vocabulary
            .resolve(&ctx, clause)
            .await
            .map_err(|error| error.to_string())?
        {
            GateOutcome::Satisfied => {}
            _ => {
                all_true = false;
                if let Some(reason) = vocabulary
                    .foreclosed(&ctx, clause)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    return Err(format!(
                        "`{}` can never come true: {reason}. Confirm or continue the artifact \
                         now, or defer on something that can still happen.",
                        vocabulary.render(clause)
                    ));
                }
            }
        }
    }
    let rendered = render_until(&vocabulary, &clauses);
    if all_true {
        return Err(format!(
            "`{rendered}` is already true: confirm or continue {} now rather than deferring it",
            approval.artifact_uri
        ));
    }
    if dry_run {
        return Ok(format!(
            "Would defer {} until {rendered}",
            approval.artifact_uri
        ));
    }

    let now = orch.services.clock.now();
    let until_json = format!("[{}]", predicates.join(","));
    let condition_id = uuid::Uuid::new_v4().to_string();
    let row = (
        condition_id.clone(),
        approval.artifact_id.clone(),
        approval.job_id.clone(),
        approval.issue_id.clone(),
        until_json,
        created_by,
    );
    let project_id = db
        .write(move |conn| {
            let (id, ask_id, job_id, issue_id, until_json, created_by) = row.clone();
            Box::pin(async move {
                conn.execute(
                    "UPDATE ask_conditions
                        SET fired_at = ?1, fired_kind = 'superseded',
                            fired_reason = 'deferred again', delivered_at = ?1
                      WHERE ask_kind = ?2 AND ask_id = ?3 AND fired_at IS NULL",
                    params![now, ASK_KIND_ARTIFACT, ask_id.as_str()],
                )
                .await?;
                conn.execute(
                    "INSERT INTO ask_conditions
                        (id, ask_kind, ask_id, job_id, issue_id, effect, until_json, created_by, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        id.as_str(),
                        ASK_KIND_ARTIFACT,
                        ask_id.as_str(),
                        job_id.as_str(),
                        issue_id.as_str(),
                        EFFECT_RESURFACE,
                        until_json.as_str(),
                        created_by.as_deref(),
                        now
                    ],
                )
                .await?;
                crate::storage::query_opt_text_conn(
                    conn,
                    "SELECT project_id FROM jobs WHERE id = ?1",
                    params![job_id.as_str()],
                )
                .await
            })
        })
        .await
        .map_err(|error| format!("failed to record the deferral: {error}"))?;
    announce(orch, project_id.as_deref(), "insert");
    // A clock deferral moves the sweep's next due instant.
    super::sweep::notify_gate_evidence_changed();
    Ok(format!(
        "Deferred {} until {rendered}; the approver is woken once when that comes true or can no \
         longer come true",
        approval.artifact_uri
    ))
}

fn announce(orch: &Orchestrator, project_id: Option<&str>, action: &str) {
    let _ = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({
            "table": "ask_conditions",
            "action": action,
            "projectId": project_id,
        }),
    );
}

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

/// Every condition whose effect has not run: open ones to evaluate, and fired
/// ones still owed a delivery.
const OWED_SQL: &str = "SELECT c.id, c.ask_kind, c.ask_id, c.job_id, c.issue_id, c.until_json,
        c.fired_at, c.fired_kind, c.fired_reason, j.project_id
   FROM ask_conditions c
   JOIN jobs j ON j.id = c.job_id
  WHERE c.delivered_at IS NULL
  ORDER BY c.created_at ASC, c.id ASC";

#[derive(Debug, Clone)]
struct OwedCondition {
    id: String,
    ask_kind: String,
    ask_id: String,
    job_id: String,
    issue_id: Option<String>,
    until: Vec<TriggerClause>,
    fired_kind: Option<String>,
    fired_reason: Option<String>,
    project_id: Option<String>,
}

async fn list_owed(db: &LocalDb) -> DbResult<Vec<OwedCondition>> {
    db.query_all(OWED_SQL, (), |row| {
        Ok(OwedCondition {
            id: row.text(0)?,
            ask_kind: row.text(1)?,
            ask_id: row.text(2)?,
            job_id: row.text(3)?,
            issue_id: row.opt_text(4)?,
            until: crate::execution::approvals::parse_until(&row.text(5)?),
            fired_kind: row.opt_i64(6)?.and(row.opt_text(7)?),
            fired_reason: row.opt_text(8)?,
            project_id: row.opt_text(9)?,
        })
    })
    .await
}

/// Resolve every open condition and deliver every owed one, in every database.
/// Failures are logged per database, so one unreadable condition never holds
/// up the execution releases of the pass it rides in.
pub(crate) async fn sweep_ask_conditions(
    orch: &Orchestrator,
    vocabulary: &GateVocabulary,
    now: i64,
) -> AskSweepReport {
    let mut report = AskSweepReport::default();
    for db in orch.db.all_dbs().await {
        if let Err(error) = sweep_db(orch, &db, vocabulary, now, &mut report).await {
            log::error!("ask-condition sweep failed for a database: {error}");
        }
    }
    report
}

async fn sweep_db(
    orch: &Orchestrator,
    db: &LocalDb,
    vocabulary: &GateVocabulary,
    now: i64,
    report: &mut AskSweepReport,
) -> Result<(), String> {
    let owed = list_owed(db).await.map_err(|error| error.to_string())?;
    if owed.is_empty() {
        return Ok(());
    }
    let job_ids: Vec<String> = owed.iter().map(|row| row.job_id.clone()).collect();
    let pending: Vec<PendingApproval> = db
        .read(move |conn| {
            let scope = ApprovalScope::Jobs(job_ids.clone());
            Box::pin(async move { pending_approvals_conn(conn, &scope).await })
        })
        .await
        .map_err(|error| error.to_string())?;
    let pending_for = |row: &OwedCondition| {
        pending
            .iter()
            .find(|approval| approval.job_id == row.job_id && approval.artifact_id == row.ask_id)
    };
    let ctx = GateContext::new(orch, now);

    for mut row in owed {
        if row.ask_kind != ASK_KIND_ARTIFACT {
            // Another kind's effect is its own module's; this pass leaves it.
            continue;
        }
        let approval = pending_for(&row);
        if row.fired_kind.is_none() {
            let fired = match approval {
                None => Some((
                    "cleared",
                    "the deferred artifact is no longer waiting".to_string(),
                )),
                Some(_) => evaluate(vocabulary, &ctx, &row.until).await?,
            };
            let Some((kind, reason)) = fired else {
                continue;
            };
            if !latch(db, &row.id, kind, &reason, now).await? {
                continue;
            }
            report.fired.push((row.id.clone(), kind));
            announce(orch, row.project_id.as_deref(), "update");
            row.fired_kind = Some(kind.to_string());
            row.fired_reason = Some(reason);
            if kind == "cleared" {
                report.delivered.push(row.id.clone());
                continue;
            }
        }
        match approval {
            Some(approval) => {
                if deliver(orch, db, &row, approval, vocabulary, now).await? {
                    report.delivered.push(row.id.clone());
                }
            }
            // Fired, then answered before the wake went out: nothing to say.
            None => {
                stamp_delivered(db, &row.id, now).await?;
                report.delivered.push(row.id.clone());
            }
        }
    }
    Ok(())
}

/// `Some((kind, reason))` when the condition fires now.
async fn evaluate(
    vocabulary: &GateVocabulary,
    ctx: &GateContext<'_>,
    until: &[TriggerClause],
) -> Result<Option<(&'static str, String)>, String> {
    if until.is_empty() {
        return Ok(Some((
            "cleared",
            "the deferral names no clause".to_string(),
        )));
    }
    let mut all_true = true;
    for clause in until {
        match vocabulary
            .resolve(ctx, clause)
            .await
            .map_err(|error| error.to_string())?
        {
            GateOutcome::Satisfied => {}
            _ => {
                all_true = false;
                if let Some(reason) = vocabulary
                    .foreclosed(ctx, clause)
                    .await
                    .map_err(|error| error.to_string())?
                {
                    return Ok(Some(("foreclosed", reason)));
                }
            }
        }
    }
    Ok(all_true.then(|| ("satisfied", render_until(vocabulary, until))))
}

async fn latch(
    db: &LocalDb,
    id: &str,
    kind: &'static str,
    reason: &str,
    now: i64,
) -> Result<bool, String> {
    // A cleared condition owes nothing, so it is delivered as it latches.
    let delivered = (kind == "cleared").then_some(now);
    db.execute(
        "UPDATE ask_conditions
            SET fired_at = ?1, fired_kind = ?2, fired_reason = ?3, delivered_at = ?4
          WHERE id = ?5 AND fired_at IS NULL",
        params![now, kind, reason, delivered, id],
    )
    .await
    .map(|changed| changed > 0)
    .map_err(|error| format!("failed to latch ask condition: {error}"))
}

async fn stamp_delivered(db: &LocalDb, id: &str, now: i64) -> Result<(), String> {
    db.execute(
        "UPDATE ask_conditions SET delivered_at = ?1 WHERE id = ?2 AND delivered_at IS NULL",
        params![now, id],
    )
    .await
    .map(|_| ())
    .map_err(|error| format!("failed to stamp ask condition delivered: {error}"))
}

/// Deliver one fired condition's re-wake, unless it was already delivered.
/// Returns whether this call finished the delivery (sent it, or handed it to
/// the operator's surface); `false` leaves it owed for the next pass.
async fn deliver(
    orch: &Orchestrator,
    db: &LocalDb,
    row: &OwedCondition,
    approval: &PendingApproval,
    vocabulary: &GateVocabulary,
    now: i64,
) -> Result<bool, String> {
    let Some(issue_id) = row.issue_id.clone() else {
        stamp_delivered(db, &row.id, now).await?;
        return Ok(true);
    };
    let content = resurface_message(row, approval, vocabulary);
    let condition_id = row.id.clone();
    let outcome = db
        .write(move |conn| {
            let condition_id = condition_id.clone();
            let issue_id = issue_id.clone();
            let content = content.clone();
            Box::pin(async move {
                let message_id = super::sweep::wake_message_id("ask-resurface", &condition_id);
                if super::sweep::wake_already_sent_conn(conn, &message_id).await? {
                    stamp_delivered_conn(conn, &condition_id, now).await?;
                    return Ok(Delivery::Done(None));
                }
                let Some(recipient) =
                    crate::orchestrator::wakes::coordinating_job_for_child(conn, &issue_id).await?
                else {
                    // No coordinating session: the operator approves, and the
                    // notification center re-surfaces the entry. Nothing is sent.
                    stamp_delivered_conn(conn, &condition_id, now).await?;
                    return Ok(Delivery::Done(None));
                };
                let Some(run) = super::sweep::recipient_run_conn(conn, &recipient).await? else {
                    return Ok(Delivery::Owed);
                };
                super::sweep::send_wake_conn(
                    conn,
                    &recipient,
                    &run,
                    &content,
                    &format!("ask-condition:{condition_id}"),
                    &message_id,
                )
                .await?;
                stamp_delivered_conn(conn, &condition_id, now).await?;
                Ok(Delivery::Done(Some(recipient)))
            })
        })
        .await
        .map_err(|error| format!("failed to deliver an ask re-wake: {error}"))?;
    match outcome {
        Delivery::Owed => {
            log::warn!(
                "deferral {} fired, but its approver has no run to address yet; the wake stays owed",
                row.id
            );
            Ok(false)
        }
        Delivery::Done(recipient) => {
            if let Some(recipient) = recipient {
                super::sweep::nudge_woken(orch, &recipient, "deferred approval");
            }
            announce(orch, row.project_id.as_deref(), "update");
            Ok(true)
        }
    }
}

enum Delivery {
    Owed,
    Done(Option<String>),
}

async fn stamp_delivered_conn(
    conn: &crate::storage::TrackedConnection,
    id: &str,
    now: i64,
) -> DbResult<()> {
    conn.execute(
        "UPDATE ask_conditions SET delivered_at = ?1 WHERE id = ?2 AND delivered_at IS NULL",
        params![now, id],
    )
    .await
    .map(|_| ())
    .map_err(DbError::from)
}

/// The re-wake: which artifact, what it was deferred until, what fired, and
/// the exits as writes the approver can send as-is.
fn resurface_message(
    row: &OwedCondition,
    approval: &PendingApproval,
    vocabulary: &GateVocabulary,
) -> String {
    let uri = &approval.artifact_uri;
    let until = render_until(vocabulary, &row.until);
    let fact = match row.fired_kind.as_deref() {
        Some("foreclosed") => format!(
            "that can never come true: {}",
            row.fired_reason.as_deref().unwrap_or("its cause is gone")
        ),
        _ => "that is now true".to_string(),
    };
    let messages = format!("{}/messages", approval.node_uri);
    format!(
        "The {what} you deferred at {uri} is waiting on you again. It was deferred until \
         {until}, and {fact}.\n\n\
         Decide it now. Either:\n\
         - confirm it: write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{confirmed:true}}}}]}})\n\
         - defer it again on a new cause: write({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{defer:{{until:[{{fact:\"issue_resolution\",issue:\"cairn://p/PROJECT/N\",resolution:\"merged\"}}]}}}}}}]}})\n\
         - or continue it with feedback: write({{changes:[{{target:\"{messages}\",mode:\"append\",payload:{{content:\"...\"}}}}]}})",
        what = approval.what(),
    )
}

/// The soonest instant after `now` an open condition's clause could become
/// true on its own (only `clock` answers), for the sweep loop's sleep bound.
pub(crate) async fn next_condition_due_after(orch: &Orchestrator, now: i64) -> Option<i64> {
    let vocabulary = gate_vocabulary();
    let mut soonest: Option<i64> = None;
    for db in orch.db.all_dbs().await {
        let rows = db
            .query_all(
                "SELECT until_json FROM ask_conditions WHERE delivered_at IS NULL AND fired_at IS NULL",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap_or_else(|error| {
                log::error!("failed to read open ask conditions for the sweep loop's bound: {error}");
                Vec::new()
            });
        for until in rows {
            for clause in crate::execution::approvals::parse_until(&until) {
                if let Some(due) = vocabulary.next_due_at(&clause).filter(|due| *due > now) {
                    soonest = Some(soonest.map_or(due, |current| current.min(due)));
                }
            }
        }
    }
    soonest
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::approvals::pending_approvals;
    use crate::execution::gates::sweep::sweep_at;
    use crate::execution::gates::sweep::tests::{count, env, now, resolve_issue, Env, NOW};
    use serde_json::json;

    const PLAN_URI: &str = "cairn://p/prj/2/1/planner/plan";
    const WAKES: &str = "SELECT COUNT(*) FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = 'run-coord')";
    const PUSHES: &str = "SELECT COUNT(*) FROM attention_pushes WHERE key LIKE 'ask-condition:%'";

    /// A planner on issue 2 (a child of issue 9, whose coordinator approves it)
    /// whose plan is pending, its job stored `running`.
    async fn pending_plan(env: &Env, issue: &str, number: i32) {
        env.db
            .execute_script(format!(
                "INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
                   VALUES('e-{issue}', 'recipe', '{issue}', 'p', 'running', 1, 1);
                 INSERT INTO jobs(id, execution_id, project_id, issue_id, node_name, status, uri_segment, created_at, updated_at)
                   VALUES('planner-{issue}', 'e-{issue}', 'p', '{issue}', 'Planner', 'running', 'planner', 1, 1);
                 INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
                   VALUES('plan-{issue}', 'planner-{issue}', 'plan', '{{}}', 1, 'plan', 0, 100, 100);"
            ))
            .await
            .unwrap();
        let _ = number;
    }

    async fn defer_on(env: &Env, issue: &str, until: Value) -> Result<String, String> {
        defer(
            &env.orch,
            &env.db,
            &format!("planner-{issue}"),
            Some("plan"),
            Some(&until),
            Some("test".into()),
            false,
        )
        .await
    }

    fn merged(number: i32) -> Value {
        json!([{"fact": "issue_resolution", "issue": format!("cairn://p/prj/{number}"), "resolution": "merged"}])
    }

    async fn wake_messages(env: &Env) -> Vec<String> {
        env.db
            .query_all(
                "SELECT content FROM messages WHERE id IN (SELECT message_id FROM message_recipients WHERE recipient_run_id = 'run-coord')",
                (),
                |row| row.text(0),
            )
            .await
            .unwrap()
    }

    async fn fired_kinds(env: &Env) -> Vec<Option<String>> {
        env.db
            .query_all(
                "SELECT fired_kind FROM ask_conditions ORDER BY fired_at IS NULL, created_at, id",
                (),
                |row| row.opt_text(0),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_deferral_until_an_issue_merges_rewakes_its_approver_exactly_once() {
        let env = env("ask-conditions-merge.db").await;
        pending_plan(&env, "i2", 2).await;
        let reply = defer_on(&env, "i2", merged(1)).await.unwrap();
        assert!(
            reply.contains(PLAN_URI) && reply.contains("issue cairn://p/prj/1 merged"),
            "{reply}"
        );

        let before = sweep_at(&env.orch, now()).await.unwrap();
        assert!(before.asks.fired.is_empty(), "{before:?}");
        assert_eq!(count(&env, WAKES).await, 0, "nothing before the merge");

        resolve_issue(&env, 1, "merged").await;
        let fired = sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(fired.asks.fired.len(), 1, "{fired:?}");
        assert_eq!(fired.asks.fired[0].1, "satisfied");
        let messages = wake_messages(&env).await;
        assert_eq!(messages.len(), 1, "{messages:?}");
        for expected in [
            PLAN_URI,
            "deferred until issue cairn://p/prj/1 merged",
            "that is now true",
            "payload:{confirmed:true}",
            "cairn://p/prj/2/1/planner/messages",
        ] {
            assert!(
                messages[0].contains(expected),
                "missing {expected:?} in {}",
                messages[0]
            );
        }
        assert_eq!(count(&env, PUSHES).await, 1);

        let again = sweep_at(&env.orch, now()).await.unwrap();
        assert!(again.asks.fired.is_empty() && again.asks.delivered.is_empty());
        assert_eq!(count(&env, WAKES).await, 1, "exactly once");
    }

    #[tokio::test]
    async fn a_deferral_that_can_never_come_true_rewakes_once_with_the_reason() {
        let env = env("ask-conditions-foreclose.db").await;
        pending_plan(&env, "i2", 2).await;
        defer_on(&env, "i2", merged(1)).await.unwrap();
        resolve_issue(&env, 1, "closed").await;

        sweep_at(&env.orch, now()).await.unwrap();
        sweep_at(&env.orch, now()).await.unwrap();
        let messages = wake_messages(&env).await;
        assert_eq!(messages.len(), 1, "{messages:?}");
        assert!(
            messages[0].contains("can never come true"),
            "{}",
            messages[0]
        );
        assert!(messages[0].contains("is closed"), "{}", messages[0]);
        assert_eq!(
            fired_kinds(&env).await,
            vec![Some("foreclosed".to_string())]
        );
    }

    #[tokio::test]
    async fn a_deferral_clears_silently_when_the_plan_is_confirmed_or_revised() {
        for (case, sql) in [
            ("confirmed", "UPDATE artifacts SET confirmed = 1 WHERE id = 'plan-i2'"),
            (
                "revised",
                "INSERT INTO artifacts(id, job_id, artifact_type, data, version, output_name, confirmed, created_at, updated_at)
                 VALUES('plan-i2-v2', 'planner-i2', 'plan', '{}', 2, 'plan', 0, 200, 200)",
            ),
        ] {
            let env = env(&format!("ask-conditions-clear-{case}.db")).await;
            pending_plan(&env, "i2", 2).await;
            defer_on(&env, "i2", merged(1)).await.unwrap();
            env.db.execute(sql, ()).await.unwrap();
            sweep_at(&env.orch, now()).await.unwrap();
            resolve_issue(&env, 1, "merged").await;
            sweep_at(&env.orch, now()).await.unwrap();
            assert_eq!(fired_kinds(&env).await, vec![Some("cleared".to_string())], "{case}");
            assert_eq!(count(&env, WAKES).await, 0, "{case}: nobody is woken");
        }
    }

    #[tokio::test]
    async fn an_operator_approved_deferral_resurfaces_in_the_center_not_by_message() {
        let env = env("ask-conditions-operator.db").await;
        env.db
            .execute_script(
                "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                   VALUES('i7', 'p', 7, 'Top level', 'active', 'active', 'none', 1, 1);",
            )
            .await
            .unwrap();
        pending_plan(&env, "i7", 7).await;
        defer_on(&env, "i7", merged(1)).await.unwrap();
        resolve_issue(&env, 1, "merged").await;
        sweep_at(&env.orch, now()).await.unwrap();

        assert_eq!(count(&env, "SELECT COUNT(*) FROM messages").await, 0);
        assert_eq!(
            count(
                &env,
                "SELECT COUNT(*) FROM ask_conditions WHERE delivered_at IS NOT NULL"
            )
            .await,
            1
        );
        let approval = pending_approvals(&env.db, ApprovalScope::Issues(vec!["i7".into()]))
            .await
            .unwrap()
            .remove(0);
        assert!(approval.deferral.is_none());
        let condition = env
            .db
            .query_opt_text("SELECT id FROM ask_conditions", ())
            .await
            .unwrap();
        assert_eq!(
            approval.resurfaced_by, condition,
            "a new occurrence for the center"
        );
    }

    #[tokio::test]
    async fn dismissing_a_resurface_push_does_not_resend_it() {
        use crate::messages::pending::{dismiss_pending_delivery, PendingDeliverySource};
        let env = env("ask-conditions-dismiss.db").await;
        pending_plan(&env, "i2", 2).await;
        defer_on(&env, "i2", merged(1)).await.unwrap();
        resolve_issue(&env, 1, "merged").await;
        sweep_at(&env.orch, now()).await.unwrap();
        let push_id = env
            .db
            .query_opt_text(
                "SELECT id FROM attention_pushes WHERE key LIKE 'ask-condition:%'",
                (),
            )
            .await
            .unwrap()
            .unwrap();
        dismiss_pending_delivery(&env.db, "coord", &push_id, PendingDeliverySource::Push)
            .await
            .unwrap();
        assert_eq!(count(&env, PUSHES).await, 0);
        sweep_at(&env.orch, now()).await.unwrap();
        sweep_at(&env.orch, now()).await.unwrap();
        assert_eq!(count(&env, PUSHES).await, 0, "no replacement push");
        assert_eq!(count(&env, WAKES).await, 1, "no duplicate message");
    }

    #[tokio::test]
    async fn a_clock_deferral_fires_on_time() {
        let env = env("ask-conditions-clock.db").await;
        pending_plan(&env, "i2", 2).await;
        let due = NOW + 100;
        let at = chrono::DateTime::from_timestamp(due, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();
        defer_on(&env, "i2", json!([{"fact": "clock", "notBefore": at}]))
            .await
            .unwrap();
        assert_eq!(
            crate::execution::gates::sweep::next_gate_due_after(&env.orch, NOW).await,
            Some(due),
            "the sweep loop sleeps until the deferral is due"
        );
        sweep_at(&env.orch, due - 1).await.unwrap();
        assert_eq!(count(&env, WAKES).await, 0);
        sweep_at(&env.orch, due).await.unwrap();
        assert_eq!(count(&env, WAKES).await, 1);
    }

    #[tokio::test]
    async fn defer_refuses_what_it_cannot_honour_and_re_deferring_supersedes() {
        let env = env("ask-conditions-refusals.db").await;
        pending_plan(&env, "i2", 2).await;

        let unknown = defer_on(
            &env,
            "i2",
            json!([{"fact": "issue_resolutoin", "issue": "cairn://p/prj/1"}]),
        )
        .await
        .unwrap_err();
        assert!(unknown.contains("unknown gate fact"), "{unknown}");
        let malformed = defer_on(
            &env,
            "i2",
            json!([{"fact": "issue_resolution", "issue": "cairn://p/prj/1", "resolution": "done"}]),
        )
        .await
        .unwrap_err();
        assert!(malformed.contains("Corrected:"), "{malformed}");
        let empty = defer_on(&env, "i2", json!([])).await.unwrap_err();
        assert!(empty.contains("is empty"), "{empty}");

        resolve_issue(&env, 3, "merged").await;
        let already = defer_on(&env, "i2", merged(3)).await.unwrap_err();
        assert!(already.contains("already true"), "{already}");
        resolve_issue(&env, 4, "closed").await;
        let never = defer_on(&env, "i2", merged(4)).await.unwrap_err();
        assert!(never.contains("can never come true"), "{never}");

        defer_on(&env, "i2", merged(1)).await.unwrap();
        defer_on(&env, "i2", merged(5)).await.unwrap();
        assert_eq!(
            fired_kinds(&env).await,
            vec![Some("superseded".to_string()), None],
            "deferring again replaces the open deferral"
        );
        let approval = pending_approvals(&env.db, ApprovalScope::Issues(vec!["i2".into()]))
            .await
            .unwrap()
            .remove(0);
        assert_eq!(
            approval.deferral.unwrap().until_rendered,
            "issue cairn://p/prj/5 merged"
        );

        env.db
            .execute("UPDATE artifacts SET confirmed = 1", ())
            .await
            .unwrap();
        let settled = defer_on(&env, "i2", merged(1)).await.unwrap_err();
        assert!(settled.contains("nothing to defer"), "{settled}");
    }

    /// Age and stale are marks, never wakes (cairn/5708): an approval left
    /// undeferred far past a week is swept like any other and nobody hears.
    #[tokio::test]
    async fn an_undeferred_stale_approval_wakes_nobody() {
        let env = env("ask-conditions-stale.db").await;
        pending_plan(&env, "i2", 2).await;
        let later = NOW + 30 * 86_400;
        sweep_at(&env.orch, later).await.unwrap();
        sweep_at(&env.orch, later + 86_400).await.unwrap();
        assert_eq!(count(&env, "SELECT COUNT(*) FROM messages").await, 0);
        assert_eq!(
            count(&env, "SELECT COUNT(*) FROM attention_pushes").await,
            0
        );
        let approval = pending_approvals(&env.db, ApprovalScope::Issues(vec!["i2".into()]))
            .await
            .unwrap()
            .remove(0);
        assert!(approval.is_stale(later));
    }

    #[test]
    fn the_defer_payload_refuses_acting_now_beside_it() {
        for other in ["confirmed", "action"] {
            let error = defer_until(&json!({"defer": {"until": []}, other: true})).unwrap_err();
            assert!(error.contains("mutually exclusive"), "{error}");
        }
        assert!(defer_until(&json!({"defer": "after 4526"})).is_err());
        assert!(defer_until(&json!({"defer": {"after": []}}))
            .unwrap_err()
            .contains("only one is until"));
        assert_eq!(
            defer_until(&json!({"defer": {"until": [1]}})).unwrap(),
            Some(&json!([1]))
        );
    }

    /// The pass reads its owed rows through the partial index, never by
    /// scanning every condition ever written.
    #[tokio::test]
    async fn the_owed_listing_reads_through_its_partial_index() {
        let env = env("ask-conditions-plan.db").await;
        let plan = crate::execution::approvals::tests::query_plan(
            &env.db,
            OWED_SQL.to_string(),
            Vec::new(),
        )
        .await;
        assert!(plan.contains("idx_ask_conditions_owed"), "{plan}");
        assert!(!plan.contains("SCAN j"), "{plan}");
    }
}
